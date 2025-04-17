use crate::prelude::*;

use crate::setup::{CombinedState, ResourceSetupInfo, ResourceSetupStatusCheck, SetupChangeType};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

pub fn default_tracking_table_name(flow_name: &str) -> String {
    let sanitized_name = flow_name
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect::<String>();
    format!("{}__cocoindex_tracking", sanitized_name)
}

pub const CURRENT_TRACKING_TABLE_VERSION: i32 = 1;

async fn upgrade_tracking_table(
    pool: &PgPool,
    table_name: &str,
    existing_version_id: i32,
    target_version_id: i32,
) -> Result<()> {
    if existing_version_id < 1 && target_version_id >= 1 {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {table_name} (
                source_id INTEGER NOT NULL,
                source_key JSONB NOT NULL,
            
                -- Update in the precommit phase: after evaluation done, before really applying the changes to the target storage.
                max_process_ordinal BIGINT NOT NULL,
                staging_target_keys JSONB NOT NULL,
                memoization_info JSONB,
            
                -- Update after applying the changes to the target storage.
                processed_source_ordinal BIGINT,
                process_logic_fingerprint BYTEA,
                process_ordinal BIGINT,
                process_time_micros BIGINT,
                target_keys JSONB,
            
                PRIMARY KEY (source_id, source_key)
            );",
        );
        sqlx::query(&query).execute(pool).await?;
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TrackingTableSetupState {
    pub table_name: String,
    pub version_id: i32,
}

#[derive(Debug)]
pub struct TrackingTableSetupStatusCheck {
    pub desired_state: Option<TrackingTableSetupState>,

    pub legacy_table_names: Vec<String>,

    pub min_existing_version_id: Option<i32>,
    pub source_ids_to_delete: Vec<i32>,
}

impl TrackingTableSetupStatusCheck {
    pub fn new(
        desired: Option<&TrackingTableSetupState>,
        existing: &CombinedState<TrackingTableSetupState>,
        source_ids_to_delete: Vec<i32>,
    ) -> Self {
        Self {
            desired_state: desired.cloned(),
            legacy_table_names: existing
                .legacy_values(desired, |v| &v.table_name)
                .into_iter()
                .cloned()
                .collect(),
            min_existing_version_id: existing
                .always_exists()
                .then(|| existing.possible_versions().map(|v| v.version_id).min())
                .flatten(),
            source_ids_to_delete,
        }
    }

    pub fn into_setup_info(
        self,
    ) -> ResourceSetupInfo<(), TrackingTableSetupState, TrackingTableSetupStatusCheck> {
        ResourceSetupInfo {
            key: (),
            state: self.desired_state.clone(),
            description: "Tracking Table".to_string(),
            status_check: Some(self),
        }
    }
}

#[async_trait]
impl ResourceSetupStatusCheck for TrackingTableSetupStatusCheck {
    fn describe_changes(&self) -> Vec<String> {
        let mut changes: Vec<String> = vec![];
        if self.desired_state.is_some() && !self.legacy_table_names.is_empty() {
            changes.push(format!(
                "Rename legacy tracking tables: {}. ",
                self.legacy_table_names.join(", ")
            ));
        }
        match (self.min_existing_version_id, &self.desired_state) {
            (None, Some(state)) => {
                changes.push(format!("Create the tracking table: {}. ", state.table_name))
            }
            (Some(min_version_id), Some(desired)) => {
                if min_version_id < desired.version_id {
                    changes.push("Update the tracking table. ".into());
                }
            }
            (Some(_), None) => changes.push(format!(
                "Drop existing tracking table: {}. ",
                self.legacy_table_names.join(", ")
            )),
            (None, None) => (),
        }
        if !self.source_ids_to_delete.is_empty() {
            changes.push(format!(
                "Delete source IDs: {}. ",
                self.source_ids_to_delete
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            ));
        }
        changes
    }

    fn change_type(&self) -> SetupChangeType {
        match (self.min_existing_version_id, &self.desired_state) {
            (None, Some(_)) => SetupChangeType::Create,
            (Some(min_version_id), Some(desired)) => {
                if min_version_id == desired.version_id && self.legacy_table_names.is_empty() {
                    SetupChangeType::NoChange
                } else if min_version_id < desired.version_id {
                    SetupChangeType::Update
                } else {
                    SetupChangeType::Invalid
                }
            }
            (Some(_), None) => SetupChangeType::Delete,
            (None, None) => SetupChangeType::NoChange,
        }
    }

    async fn apply_change(&self) -> Result<()> {
        let pool = &get_lib_context()?.pool;
        if let Some(desired) = &self.desired_state {
            for lagacy_name in self.legacy_table_names.iter() {
                let query = format!(
                    "ALTER TABLE IF EXISTS {} RENAME TO {}",
                    lagacy_name, desired.table_name
                );
                sqlx::query(&query).execute(pool).await?;
            }

            if self.min_existing_version_id != Some(desired.version_id) {
                upgrade_tracking_table(
                    pool,
                    &desired.table_name,
                    self.min_existing_version_id.unwrap_or(0),
                    desired.version_id,
                )
                .await?;
            }
        } else {
            for lagacy_name in self.legacy_table_names.iter() {
                let query = format!("DROP TABLE IF EXISTS {}", lagacy_name);
                sqlx::query(&query).execute(pool).await?;
            }
            return Ok(());
        }
        Ok(())
    }
}
