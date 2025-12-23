use crate::prelude::*;

use crate::execution::db_tracking::{
    SourceLastProcessedInfo, SourceTrackingInfoForCommit, SourceTrackingInfoForPrecommit,
    SourceTrackingInfoForProcessing, TrackedSourceKeyMetadata, TrackedTargetKeyForSource,
};
use crate::execution::db_tracking_setup::{TrackingTableSetupChange, TrackingTableSetupState};
use crate::execution::memoization::StoredMemoizationInfo;
use crate::persistence::{InternalPersistence, InternalPersistenceTxn};
use crate::setup::db_metadata::{parse_flow_version, ResourceTypeKey, SetupMetadataRecord, StateUpdateInfo, FLOW_VERSION_RESOURCE_TYPE};
use async_trait::async_trait;
use axum::http::StatusCode;
use blake2::Digest;
use serde::{Deserialize, Serialize};
use surrealdb::Surreal;
use surrealdb::engine::remote::ws::{Client, Ws};
use surrealdb::opt::auth::Root;
use utils::db::WriteAction;

const SETUP_TABLE: &str = "cocoindex_setup_metadata";
const TRACKING_TABLE: &str = "cocoindex_tracking";
const SOURCE_STATE_TABLE: &str = "cocoindex_source_state";

fn sanitize_id_part(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
        .collect()
}

fn canonical_json(v: &serde_json::Value, out: &mut String) {
    match v {
        serde_json::Value::Null => out.push_str("null"),
        serde_json::Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        serde_json::Value::Number(n) => out.push_str(&n.to_string()),
        serde_json::Value::String(s) => out.push_str(&serde_json::to_string(s).unwrap()),
        serde_json::Value::Array(a) => {
            out.push('[');
            for (i, item) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canonical_json(item, out);
            }
            out.push(']');
        }
        serde_json::Value::Object(m) => {
            out.push('{');
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(k).unwrap());
                out.push(':');
                canonical_json(&m[*k].clone(), out);
            }
            out.push('}');
        }
    }
}

fn stable_hash_json(v: &serde_json::Value) -> String {
    let mut s = String::new();
    canonical_json(v, &mut s);
    let mut hasher = blake2::Blake2b512::new();
    hasher.update(s.as_bytes());
    hex::encode(hasher.finalize())
}

fn setup_record_id(flow_name: &str, resource_type: &str, key: &serde_json::Value) -> String {
    format!(
        "{}__{}__{}",
        sanitize_id_part(flow_name),
        sanitize_id_part(resource_type),
        stable_hash_json(key)
    )
}

fn tracking_record_id(flow_table: &str, source_id: i32, source_key: &serde_json::Value) -> String {
    format!(
        "{}__{}__{}",
        sanitize_id_part(flow_table),
        source_id,
        stable_hash_json(source_key)
    )
}

fn source_state_record_id(flow_table: &str, source_id: i32, key: &serde_json::Value) -> String {
    format!(
        "{}__{}__{}",
        sanitize_id_part(flow_table),
        source_id,
        stable_hash_json(key)
    )
}

#[derive(Clone)]
pub struct SurrealWsPersistence {
    db: Surreal<Client>,
}

impl SurrealWsPersistence {
    pub async fn connect(
        endpoint: &str,
        ns: &str,
        db: &str,
        user: Option<&str>,
        password: Option<&str>,
    ) -> Result<Self> {
        let sdb: Surreal<Client> = Surreal::init();
        sdb.connect::<Ws>(endpoint).await?;
        if let Some(user) = user {
            sdb.signin(Root {
                username: user,
                password: password.unwrap_or_default(),
            })
            .await?;
        }
        sdb.use_ns(ns).use_db(db).await?;
        Ok(Self { db: sdb })
    }
}

#[derive(Clone)]
struct SurrealTxn {
    db: Surreal<Client>,
}

#[derive(Debug, Deserialize)]
struct SurrealSetupRow {
    flow_name: String,
    resource_type: String,
    key: serde_json::Value,
    state: Option<serde_json::Value>,
    staging_changes: Option<Vec<crate::setup::StateChange<serde_json::Value>>>,
}

#[async_trait]
impl InternalPersistence for SurrealWsPersistence {
    async fn read_setup_metadata(&self) -> Result<Option<Vec<SetupMetadataRecord>>> {
        let mut res = self
            .db
            .query(format!(
                "SELECT flow_name, resource_type, key, state, staging_changes FROM {SETUP_TABLE};"
            ))
            .await?;
        let rows: Vec<SurrealSetupRow> = res.take(0)?;
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(
            rows.into_iter()
                .map(|r| SetupMetadataRecord {
                    flow_name: r.flow_name,
                    resource_type: r.resource_type,
                    key: r.key,
                    state: r.state,
                    staging_changes: sqlx::types::Json(r.staging_changes.unwrap_or_default()),
                })
                .collect(),
        ))
    }

    async fn stage_changes_for_flow(
        &self,
        flow_name: &str,
        seen_metadata_version: Option<u64>,
        resource_update_info: &HashMap<ResourceTypeKey, StateUpdateInfo>,
    ) -> Result<u64> {
        // Read all setup records for this flow
        let mut res = self
            .db
            .query(format!(
                "SELECT flow_name, resource_type, key, state, staging_changes FROM {SETUP_TABLE} WHERE flow_name = $flow;"
            ))
            .bind(("flow", flow_name))
            .await?;
        let rows: Vec<SurrealSetupRow> = res.take(0)?;
        let mut existing: HashMap<ResourceTypeKey, SurrealSetupRow> = rows
            .into_iter()
            .map(|r| {
                (
                    ResourceTypeKey {
                        resource_type: r.resource_type.clone(),
                        key: r.key.clone(),
                    },
                    r,
                )
            })
            .collect();

        let version_key = ResourceTypeKey {
            resource_type: FLOW_VERSION_RESOURCE_TYPE.to_string(),
            key: serde_json::Value::Null,
        };
        let latest_version = existing
            .get(&version_key)
            .and_then(|r| parse_flow_version(&r.state));

        if seen_metadata_version < latest_version {
            return Err(ApiError::new(
                "seen newer version in the metadata table",
                StatusCode::CONFLICT,
            ))?;
        }

        let new_version = seen_metadata_version.unwrap_or_default() + 1;
        let version_id = setup_record_id(flow_name, FLOW_VERSION_RESOURCE_TYPE, &serde_json::Value::Null);
        let q = self
            .db
            .query(format!(
                "UPDATE {SETUP_TABLE}:{version_id} MERGE {{ flow_name: $flow, resource_type: $rtype, key: $key, state: $state, staging_changes: [] }};"
            ))
            .bind(("flow", flow_name))
            .bind(("rtype", FLOW_VERSION_RESOURCE_TYPE))
            .bind(("key", serde_json::Value::Null))
            .bind(("state", serde_json::Value::Number(new_version.into())));
        q.await?;

        for (type_id, update_info) in resource_update_info {
            let change = match &update_info.desired_state {
                Some(desired) => crate::setup::StateChange::Upsert(desired.clone()),
                None => crate::setup::StateChange::Delete,
            };

            let mut new_staging: Vec<crate::setup::StateChange<serde_json::Value>> = Vec::new();

            if let Some(legacy_key) = &update_info.legacy_key {
                if let Some(legacy_row) = existing.remove(legacy_key) {
                    if let Some(staging) = legacy_row.staging_changes {
                        new_staging.extend(staging);
                    }
                    let legacy_id = setup_record_id(flow_name, &legacy_key.resource_type, &legacy_key.key);
                    self.db
                        .query(format!("DELETE {SETUP_TABLE}:{legacy_id};"))
                        .await?;
                }
            }

            let existing_row = existing.remove(type_id);
            let existing_staging = existing_row
                .and_then(|r| r.staging_changes)
                .unwrap_or_default();

            let mut merged = existing_staging;
            if !merged.iter().any(|c| c == &change) {
                if update_info.desired_state.is_some() {
                    merged.push(change);
                }
            }
            merged.extend(new_staging);

            if merged.is_empty() {
                continue;
            }

            let rid = setup_record_id(flow_name, &type_id.resource_type, &type_id.key);
            self.db
                .query(format!(
                    "UPDATE {SETUP_TABLE}:{rid} MERGE {{ flow_name: $flow, resource_type: $rtype, key: $key, staging_changes: $staging }};"
                ))
                .bind(("flow", flow_name))
                .bind(("rtype", type_id.resource_type.as_str()))
                .bind(("key", type_id.key.clone()))
                .bind(("staging", merged))
                .await?;
        }

        Ok(new_version)
    }

    async fn commit_changes_for_flow(
        &self,
        flow_name: &str,
        curr_metadata_version: u64,
        state_updates: &HashMap<ResourceTypeKey, StateUpdateInfo>,
        delete_version: bool,
    ) -> Result<()> {
        let version_id = setup_record_id(flow_name, FLOW_VERSION_RESOURCE_TYPE, &serde_json::Value::Null);
        #[derive(Deserialize)]
        struct VersionRow {
            state: Option<serde_json::Value>,
        }
        let mut res = self
            .db
            .query(format!("SELECT state FROM {SETUP_TABLE}:{version_id};"))
            .await?;
        let rows: Vec<VersionRow> = res.take(0)?;
        let state = rows.into_iter().next().and_then(|r| r.state);
        let latest_version = parse_flow_version(&state);
        if latest_version != Some(curr_metadata_version) {
            return Err(ApiError::new(
                "seen newer version in the metadata table",
                StatusCode::CONFLICT,
            ))?;
        }

        for (type_id, update_info) in state_updates {
            let rid = setup_record_id(flow_name, &type_id.resource_type, &type_id.key);
            match &update_info.desired_state {
                Some(desired_state) => {
                    self.db
                        .query(format!(
                            "UPDATE {SETUP_TABLE}:{rid} MERGE {{ flow_name: $flow, resource_type: $rtype, key: $key, state: $state, staging_changes: [] }};"
                        ))
                        .bind(("flow", flow_name))
                        .bind(("rtype", type_id.resource_type.as_str()))
                        .bind(("key", type_id.key.clone()))
                        .bind(("state", desired_state.clone()))
                        .await?;
                }
                None => {
                    self.db
                        .query(format!("DELETE {SETUP_TABLE}:{rid};"))
                        .await?;
                }
            }
        }

        if delete_version {
            self.db
                .query(format!("DELETE {SETUP_TABLE}:{version_id};"))
                .await?;
        }

        Ok(())
    }

    async fn apply_metadata_table_setup(&self, _metadata_table_missing: bool) -> Result<()> {
        // SurrealDB is schema-less by default; we lazily create records as needed.
        Ok(())
    }

    async fn apply_tracking_table_setup_change(&self, _change: &TrackingTableSetupChange) -> Result<()> {
        // No per-flow DDL required in SurrealDB.
        Ok(())
    }

    async fn begin_txn(&self) -> Result<Box<dyn InternalPersistenceTxn>> {
        Ok(Box::new(SurrealTxn { db: self.db.clone() }))
    }

    async fn list_tracked_source_key_metadata(
        &self,
        source_id: i32,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Vec<TrackedSourceKeyMetadata>> {
        #[derive(Debug, Deserialize)]
        struct Row {
            source_key: serde_json::Value,
            processed_source_ordinal: Option<i64>,
            processed_source_fp: Option<Vec<u8>>,
            process_logic_fingerprint: Option<Vec<u8>>,
            max_process_ordinal: Option<i64>,
            process_ordinal: Option<i64>,
        }

        let mut res = self
            .db
            .query(format!(
                "SELECT source_key, processed_source_ordinal, processed_source_fp, process_logic_fingerprint, max_process_ordinal, process_ordinal FROM {TRACKING_TABLE} WHERE flow_table = $flow_table AND source_id = $source_id;"
            ))
            .bind(("flow_table", db_setup.table_name.as_str()))
            .bind(("source_id", source_id))
            .await?;
        let rows: Vec<Row> = res.take(0)?;
        Ok(rows
            .into_iter()
            .map(|r| TrackedSourceKeyMetadata {
                source_key: r.source_key,
                processed_source_ordinal: r.processed_source_ordinal,
                processed_source_fp: r.processed_source_fp,
                process_logic_fingerprint: r.process_logic_fingerprint,
                max_process_ordinal: r.max_process_ordinal,
                process_ordinal: r.process_ordinal,
            })
            .collect())
    }

    async fn read_source_last_processed_info(
        &self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Option<SourceLastProcessedInfo>> {
        #[derive(Deserialize)]
        struct Row {
            processed_source_ordinal: Option<i64>,
            process_logic_fingerprint: Option<Vec<u8>>,
            process_time_micros: Option<i64>,
        }
        let id = tracking_record_id(&db_setup.table_name, source_id, source_key_json);
        let mut res = self
            .db
            .query(format!(
                "SELECT processed_source_ordinal, process_logic_fingerprint, process_time_micros FROM {TRACKING_TABLE}:{id};"
            ))
            .await?;
        let rows: Vec<Row> = res.take(0)?;
        Ok(rows.into_iter().next().map(|r| SourceLastProcessedInfo {
            processed_source_ordinal: r.processed_source_ordinal,
            process_logic_fingerprint: r.process_logic_fingerprint,
            process_time_micros: r.process_time_micros,
        }))
    }
}

#[async_trait]
impl InternalPersistenceTxn for SurrealTxn {
    async fn commit(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    async fn read_source_tracking_info_for_processing(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Option<SourceTrackingInfoForProcessing>> {
        let id = tracking_record_id(&db_setup.table_name, source_id, source_key_json);
        let mut res = self
            .db
            .query(format!(
                "SELECT memoization_info, processed_source_ordinal, processed_source_fp, process_logic_fingerprint, max_process_ordinal, process_ordinal FROM {TRACKING_TABLE}:{id};"
            ))
            .await?;
        let rows: Vec<SourceTrackingInfoForProcessing> = res.take(0)?;
        Ok(rows.into_iter().next())
    }

    async fn read_source_tracking_info_for_precommit(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Option<SourceTrackingInfoForPrecommit>> {
        let id = tracking_record_id(&db_setup.table_name, source_id, source_key_json);
        let mut res = self
            .db
            .query(format!(
                "SELECT max_process_ordinal, staging_target_keys, processed_source_ordinal, processed_source_fp, process_logic_fingerprint, process_ordinal, target_keys FROM {TRACKING_TABLE}:{id};"
            ))
            .await?;
        let rows: Vec<SourceTrackingInfoForPrecommit> = res.take(0)?;
        Ok(rows.into_iter().next())
    }

    async fn precommit_source_tracking_info(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        max_process_ordinal: i64,
        staging_target_keys: TrackedTargetKeyForSource,
        memoization_info: Option<&StoredMemoizationInfo>,
        db_setup: &TrackingTableSetupState,
        _action: WriteAction,
    ) -> Result<()> {
        let id = tracking_record_id(&db_setup.table_name, source_id, source_key_json);
        let memo = memoization_info.map(serde_json::to_value).transpose()?;
        self.db
            .query(format!(
                "UPDATE {TRACKING_TABLE}:{id} MERGE {{ flow_table: $flow_table, source_id: $source_id, source_key: $source_key, max_process_ordinal: $max_ord, staging_target_keys: $staging, memoization_info: $memo }};"
            ))
            .bind(("flow_table", db_setup.table_name.as_str()))
            .bind(("source_id", source_id))
            .bind(("source_key", source_key_json.clone()))
            .bind(("max_ord", max_process_ordinal))
            .bind(("staging", staging_target_keys))
            .bind(("memo", memo.unwrap_or(serde_json::Value::Null)))
            .await?;
        Ok(())
    }

    async fn touch_max_process_ordinal(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        process_ordinal: i64,
        db_setup: &TrackingTableSetupState,
    ) -> Result<()> {
        let id = tracking_record_id(&db_setup.table_name, source_id, source_key_json);
        #[derive(Deserialize)]
        struct OrdRow {
            max_process_ordinal: Option<i64>,
        }
        let mut res = self
            .db
            .query(format!("SELECT max_process_ordinal FROM {TRACKING_TABLE}:{id};"))
            .await?;
        let rows: Vec<OrdRow> = res.take(0)?;
        let existing = rows.into_iter().next().and_then(|r| r.max_process_ordinal).unwrap_or(0);
        let new_max = (existing + 1).max(process_ordinal);
        self.db
            .query(format!(
                "UPDATE {TRACKING_TABLE}:{id} MERGE {{ flow_table: $flow_table, source_id: $source_id, source_key: $source_key, max_process_ordinal: $max_ord, staging_target_keys: [] }};"
            ))
            .bind(("flow_table", db_setup.table_name.as_str()))
            .bind(("source_id", source_id))
            .bind(("source_key", source_key_json.clone()))
            .bind(("max_ord", new_max))
            .await?;
        Ok(())
    }

    async fn read_source_tracking_info_for_commit(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Option<SourceTrackingInfoForCommit>> {
        let id = tracking_record_id(&db_setup.table_name, source_id, source_key_json);
        let mut res = self
            .db
            .query(format!("SELECT staging_target_keys, process_ordinal FROM {TRACKING_TABLE}:{id};"))
            .await?;
        let rows: Vec<SourceTrackingInfoForCommit> = res.take(0)?;
        Ok(rows.into_iter().next())
    }

    async fn commit_source_tracking_info(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        staging_target_keys: TrackedTargetKeyForSource,
        processed_source_ordinal: Option<i64>,
        processed_source_fp: Option<Vec<u8>>,
        logic_fingerprint: &[u8],
        process_ordinal: i64,
        process_time_micros: i64,
        target_keys: TrackedTargetKeyForSource,
        db_setup: &TrackingTableSetupState,
        action: WriteAction,
    ) -> Result<()> {
        let id = tracking_record_id(&db_setup.table_name, source_id, source_key_json);
        #[derive(Serialize)]
        struct Merge<'a> {
            flow_table: &'a str,
            source_id: i32,
            source_key: serde_json::Value,
            staging_target_keys: TrackedTargetKeyForSource,
            processed_source_ordinal: Option<i64>,
            processed_source_fp: Option<Vec<u8>>,
            process_logic_fingerprint: &'a [u8],
            process_ordinal: i64,
            process_time_micros: i64,
            target_keys: TrackedTargetKeyForSource,
            #[serde(skip_serializing_if = "Option::is_none")]
            max_process_ordinal: Option<i64>,
        }
        let merge = Merge {
            flow_table: db_setup.table_name.as_str(),
            source_id,
            source_key: source_key_json.clone(),
            staging_target_keys,
            processed_source_ordinal,
            processed_source_fp,
            process_logic_fingerprint: logic_fingerprint,
            process_ordinal,
            process_time_micros,
            target_keys,
            max_process_ordinal: matches!(action, WriteAction::Insert).then_some(process_ordinal + 1),
        };
        self.db
            .query(format!("UPDATE {TRACKING_TABLE}:{id} MERGE $merge;"))
            .bind(("merge", merge))
            .await?;
        Ok(())
    }

    async fn delete_source_tracking_info(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<()> {
        let id = tracking_record_id(&db_setup.table_name, source_id, source_key_json);
        self.db.query(format!("DELETE {TRACKING_TABLE}:{id};")).await?;
        Ok(())
    }

    async fn update_source_tracking_ordinal(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        processed_source_ordinal: Option<i64>,
        db_setup: &TrackingTableSetupState,
    ) -> Result<()> {
        let id = tracking_record_id(&db_setup.table_name, source_id, source_key_json);
        self.db
            .query(format!(
                "UPDATE {TRACKING_TABLE}:{id} MERGE {{ processed_source_ordinal: $ord }};"
            ))
            .bind(("ord", processed_source_ordinal))
            .await?;
        Ok(())
    }

    async fn read_source_state(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Option<serde_json::Value>> {
        let Some(flow_table) = db_setup.source_state_table_name.as_ref() else {
            bail!("Source state table not enabled for this flow");
        };
        let id = source_state_record_id(flow_table, source_id, source_key_json);
        #[derive(Deserialize)]
        struct Row {
            value: serde_json::Value,
        }
        let mut res = self
            .db
            .query(format!("SELECT value FROM {SOURCE_STATE_TABLE}:{id};"))
            .await?;
        let rows: Vec<Row> = res.take(0)?;
        Ok(rows.into_iter().next().map(|r| r.value))
    }

    async fn upsert_source_state(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        state: serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<()> {
        let Some(flow_table) = db_setup.source_state_table_name.as_ref() else {
            bail!("Source state table not enabled for this flow");
        };
        let id = source_state_record_id(flow_table, source_id, source_key_json);
        self.db
            .query(format!(
                "UPDATE {SOURCE_STATE_TABLE}:{id} MERGE {{ flow_table: $flow_table, source_id: $source_id, key: $key, value: $value }};"
            ))
            .bind(("flow_table", flow_table.as_str()))
            .bind(("source_id", source_id))
            .bind(("key", source_key_json.clone()))
            .bind(("value", state))
            .await?;
        Ok(())
    }
}


