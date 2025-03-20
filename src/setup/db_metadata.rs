use std::{collections::HashMap, sync::LazyLock};

use anyhow::Result;
use axum::async_trait;
use axum::http::StatusCode;
use log::warn;
use sqlx::PgPool;

use crate::utils::db::WriteAction;
use crate::{get_lib_context, service::error::ApiError};

use super::{ResourceSetupStatusCheck, SetupChangeType, StateChange};

const SETUP_METADATA_TABLE_NAME: &str = "cocoindex_setup_metadata";
pub const FLOW_VERSION_RESOURCE_TYPE: &str = "__FlowVersion";

#[derive(sqlx::FromRow, Debug)]
pub struct SetupMetadataRecord {
    pub flow_name: String,
    // e.g. "Flow", "SourceTracking", "Target:{TargetType}"
    pub resource_type: String,
    pub key: serde_json::Value,
    pub state: Option<serde_json::Value>,
    pub staging_changes: sqlx::types::Json<Vec<StateChange<serde_json::Value>>>,
}

pub fn parse_flow_version(state: &Option<serde_json::Value>) -> Option<u64> {
    match state {
        Some(serde_json::Value::Number(n)) => n.as_u64(),
        _ => None,
    }
}

/// Returns None if metadata table doesn't exist.
pub async fn read_setup_metadata(pool: &PgPool) -> Result<Option<Vec<SetupMetadataRecord>>> {
    let mut db_conn = pool.acquire().await?;
    let query_str = format!(
        "SELECT flow_name, resource_type, key, state, staging_changes FROM {SETUP_METADATA_TABLE_NAME}",
    );
    let metadata = sqlx::query_as(&query_str).fetch_all(&mut *db_conn).await;
    let result = match metadata {
        Ok(metadata) => Some(metadata),
        Err(err) => {
            let exists: Option<bool> = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = $1)",
            )
            .bind(SETUP_METADATA_TABLE_NAME)
            .fetch_one(&mut *db_conn)
            .await?;
            if !exists.unwrap_or(false) {
                None
            } else {
                return Err(err.into());
            }
        }
    };
    Ok(result)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourceTypeKey {
    pub resource_type: String,
    pub key: serde_json::Value,
}

impl ResourceTypeKey {
    pub fn new(resource_type: String, key: serde_json::Value) -> Self {
        Self { resource_type, key }
    }
}

static VERSION_RESOURCE_TYPE_ID: LazyLock<ResourceTypeKey> = LazyLock::new(|| ResourceTypeKey {
    resource_type: FLOW_VERSION_RESOURCE_TYPE.to_string(),
    key: serde_json::Value::Null,
});

async fn read_metadata_records_for_flow(
    flow_name: &str,
    db_executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
) -> Result<HashMap<ResourceTypeKey, SetupMetadataRecord>> {
    let query_str = format!(
        "SELECT flow_name, resource_type, key, state, staging_changes FROM {SETUP_METADATA_TABLE_NAME} WHERE flow_name = $1",
    );
    let metadata: Vec<SetupMetadataRecord> = sqlx::query_as(&query_str)
        .bind(flow_name)
        .fetch_all(db_executor)
        .await?;
    let result = metadata
        .into_iter()
        .map(|m| {
            (
                ResourceTypeKey {
                    resource_type: m.resource_type.clone(),
                    key: m.key.clone(),
                },
                m,
            )
        })
        .collect();
    Ok(result)
}

async fn read_state(
    flow_name: &str,
    type_id: &ResourceTypeKey,
    db_executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
) -> Result<Option<serde_json::Value>> {
    let query_str = format!(
        "SELECT state FROM {SETUP_METADATA_TABLE_NAME} WHERE flow_name = $1 AND resource_type = $2 AND key = $3",
    );
    let state: Option<serde_json::Value> = sqlx::query_scalar(&query_str)
        .bind(flow_name)
        .bind(&type_id.resource_type)
        .bind(&type_id.key)
        .fetch_optional(db_executor)
        .await?;
    Ok(state)
}

async fn upsert_staging_changes(
    flow_name: &str,
    type_id: &ResourceTypeKey,
    staging_changes: Vec<StateChange<serde_json::Value>>,
    db_executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    action: WriteAction,
) -> Result<()> {
    let query_str = match action {
        WriteAction::Insert => format!(
            "INSERT INTO {SETUP_METADATA_TABLE_NAME} (flow_name, resource_type, key, staging_changes) VALUES ($1, $2, $3, $4)",
        ),
        WriteAction::Update => format!(
            "UPDATE {SETUP_METADATA_TABLE_NAME} SET staging_changes = $4 WHERE flow_name = $1 AND resource_type = $2 AND key = $3",
        ),
    };
    sqlx::query(&query_str)
        .bind(flow_name)
        .bind(&type_id.resource_type)
        .bind(&type_id.key)
        .bind(sqlx::types::Json(staging_changes))
        .execute(db_executor)
        .await?;
    Ok(())
}

async fn upsert_state(
    flow_name: &str,
    type_id: &ResourceTypeKey,
    state: serde_json::Value,
    action: WriteAction,
    db_executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
) -> Result<()> {
    let query_str = match action {
        WriteAction::Insert => format!(
            "INSERT INTO {SETUP_METADATA_TABLE_NAME} (flow_name, resource_type, key, state, staging_changes) VALUES ($1, $2, $3, $4, $5)",
        ),
        WriteAction::Update => format!(
            "UPDATE {SETUP_METADATA_TABLE_NAME} SET state = $4, staging_changes = $5 WHERE flow_name = $1 AND resource_type = $2 AND key = $3",
        ),
    };
    sqlx::query(&query_str)
        .bind(flow_name)
        .bind(&type_id.resource_type)
        .bind(&type_id.key)
        .bind(sqlx::types::Json(state))
        .bind(sqlx::types::Json(Vec::<serde_json::Value>::new()))
        .execute(db_executor)
        .await?;
    Ok(())
}

async fn delete_state(
    flow_name: &str,
    type_id: &ResourceTypeKey,
    db_executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
) -> Result<()> {
    let query_str = format!(
        "DELETE FROM {SETUP_METADATA_TABLE_NAME} WHERE flow_name = $1 AND resource_type = $2 AND key = $3",
    );
    sqlx::query(&query_str)
        .bind(flow_name)
        .bind(&type_id.resource_type)
        .bind(&type_id.key)
        .execute(db_executor)
        .await?;
    Ok(())
}

pub async fn stage_changes_for_flow(
    flow_name: &str,
    seen_metadata_version: Option<u64>,
    state_updates: &HashMap<ResourceTypeKey, Option<serde_json::Value>>,
    pool: &PgPool,
) -> Result<u64> {
    let mut txn = pool.begin().await?;
    let mut existing_records = read_metadata_records_for_flow(flow_name, &mut *txn).await?;
    let latest_metadata_version = existing_records
        .get(&VERSION_RESOURCE_TYPE_ID)
        .and_then(|m| parse_flow_version(&m.state));
    if seen_metadata_version < latest_metadata_version {
        return Err(ApiError::new(
            "seen newer version in the metadata table",
            StatusCode::CONFLICT,
        ))?;
    }
    let new_metadata_version = seen_metadata_version.unwrap_or_default() + 1;
    upsert_state(
        flow_name,
        &VERSION_RESOURCE_TYPE_ID,
        serde_json::Value::Number(new_metadata_version.into()),
        if latest_metadata_version.is_some() {
            WriteAction::Update
        } else {
            WriteAction::Insert
        },
        &mut *txn,
    )
    .await?;

    for (type_id, desired_state) in state_updates {
        let existing = existing_records.remove(type_id);
        let change = match desired_state {
            Some(desired_state) => StateChange::Upsert(desired_state.clone()),
            None => StateChange::Delete,
        };
        match existing {
            Some(existing) => {
                if existing.staging_changes.0.iter().all(|c| c != &change) {
                    let mut staging_changes = existing.staging_changes.0;
                    staging_changes.push(change);
                    upsert_staging_changes(
                        flow_name,
                        type_id,
                        staging_changes,
                        &mut *txn,
                        WriteAction::Update,
                    )
                    .await?;
                }
            }
            None => {
                if desired_state.is_some() {
                    upsert_staging_changes(
                        flow_name,
                        type_id,
                        vec![change],
                        &mut *txn,
                        WriteAction::Insert,
                    )
                    .await?;
                }
            }
        }
    }
    txn.commit().await?;
    Ok(new_metadata_version)
}

pub async fn commit_changes_for_flow(
    flow_name: &str,
    curr_metadata_version: u64,
    state_updates: HashMap<ResourceTypeKey, Option<serde_json::Value>>,
    delete_version: bool,
    pool: &PgPool,
) -> Result<()> {
    let mut txn = pool.begin().await?;
    let latest_metadata_version =
        parse_flow_version(&read_state(flow_name, &VERSION_RESOURCE_TYPE_ID, &mut *txn).await?);
    if latest_metadata_version != Some(curr_metadata_version) {
        return Err(ApiError::new(
            "seen newer version in the metadata table",
            StatusCode::CONFLICT,
        ))?;
    }
    for (type_id, desired_state) in state_updates {
        match desired_state {
            Some(desired_state) => {
                upsert_state(
                    flow_name,
                    &type_id,
                    desired_state,
                    WriteAction::Update,
                    &mut *txn,
                )
                .await?;
            }
            None => {
                delete_state(flow_name, &type_id, &mut *txn).await?;
            }
        }
    }
    if delete_version {
        delete_state(flow_name, &VERSION_RESOURCE_TYPE_ID, &mut *txn).await?;
    }
    txn.commit().await?;
    Ok(())
}

#[derive(Debug)]
pub struct MetadataTableSetup {
    pub metadata_table_missing: bool,
}

#[async_trait]
impl ResourceSetupStatusCheck for MetadataTableSetup {
    type Key = ();
    type State = ();

    fn key(&self) -> &Self::Key {
        &()
    }

    fn desired_state(&self) -> Option<&Self::State> {
        warn!("Metadata table is unexpected to have a state");
        None
    }

    fn describe_resource(&self) -> String {
        "CocoIndex Metadata Table".to_string()
    }

    fn describe_changes(&self) -> Vec<String> {
        if self.metadata_table_missing {
            vec![format!(
                "Create the cocoindex metadata table {SETUP_METADATA_TABLE_NAME}"
            )]
        } else {
            vec![]
        }
    }

    fn change_type(&self) -> SetupChangeType {
        if self.metadata_table_missing {
            SetupChangeType::Create
        } else {
            SetupChangeType::NoChange
        }
    }

    async fn apply_change(&self) -> Result<()> {
        if !self.metadata_table_missing {
            return Ok(());
        }
        let pool = &get_lib_context()
            .ok_or_else(|| anyhow::anyhow!("Library not initialized"))?
            .pool;
        let query_str = format!(
            "CREATE TABLE IF NOT EXISTS {SETUP_METADATA_TABLE_NAME} (
                flow_name TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                key JSONB NOT NULL,
                state JSONB,
                staging_changes JSONB NOT NULL,

                PRIMARY KEY (flow_name, resource_type, key)
            )
        ",
        );
        sqlx::query(&query_str).execute(pool).await?;
        Ok(())
    }
}
