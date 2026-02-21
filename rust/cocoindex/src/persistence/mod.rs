use crate::prelude::*;

use crate::execution::db_tracking::{
    SourceLastProcessedInfo, SourceTrackingInfoForCommit, SourceTrackingInfoForPrecommit,
    SourceTrackingInfoForProcessing, TrackedSourceKeyMetadata, TrackedTargetKeyForSource,
};
use crate::execution::db_tracking_setup::TrackingTableSetupChange;
use crate::execution::db_tracking_setup::TrackingTableSetupState;
use crate::setup::db_metadata::{ResourceTypeKey, SetupMetadataRecord, StateUpdateInfo};
use async_trait::async_trait;
use utils::db::WriteAction;

pub mod postgres;
pub mod surreal;
pub mod surrealdb_pool;

#[async_trait]
pub trait InternalPersistence: Send + Sync {
    /// Returns None if the backend has no persisted setup metadata yet.
    async fn read_setup_metadata(&self) -> Result<Option<Vec<SetupMetadataRecord>>>;

    async fn stage_changes_for_flow(
        &self,
        flow_name: &str,
        seen_metadata_version: Option<u64>,
        resource_update_info: &HashMap<ResourceTypeKey, StateUpdateInfo>,
    ) -> Result<u64>;

    async fn commit_changes_for_flow(
        &self,
        flow_name: &str,
        curr_metadata_version: u64,
        state_updates: &HashMap<ResourceTypeKey, StateUpdateInfo>,
        delete_version: bool,
    ) -> Result<()>;

    /// Apply the global metadata-table setup change.
    async fn apply_metadata_table_setup(&self, metadata_table_missing: bool) -> Result<()>;

    /// Apply tracking-table setup changes for a flow (DDL/cleanup for SQL backends; no-op for schema-less backends).
    async fn apply_tracking_table_setup_change(
        &self,
        change: &TrackingTableSetupChange,
    ) -> Result<()>;

    async fn begin_txn(&self) -> Result<Box<dyn InternalPersistenceTxn>>;

    /// Convenience: list tracked keys for a source (used on startup to warm in-memory state).
    async fn list_tracked_source_key_metadata(
        &self,
        source_id: i32,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Vec<TrackedSourceKeyMetadata>>;

    async fn read_source_last_processed_info(
        &self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Option<SourceLastProcessedInfo>>;
}

#[async_trait]
pub trait InternalPersistenceTxn: Send {
    async fn commit(self: Box<Self>) -> Result<()>;
    async fn rollback(self: Box<Self>) -> Result<()>;

    async fn read_source_tracking_info_for_processing(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Option<SourceTrackingInfoForProcessing>>;

    async fn read_source_tracking_info_for_precommit(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Option<SourceTrackingInfoForPrecommit>>;

    async fn precommit_source_tracking_info(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        max_process_ordinal: i64,
        staging_target_keys: TrackedTargetKeyForSource,
        memoization_info: Option<&crate::execution::memoization::StoredMemoizationInfo>,
        db_setup: &TrackingTableSetupState,
        action: WriteAction,
    ) -> Result<()>;

    async fn touch_max_process_ordinal(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        process_ordinal: i64,
        db_setup: &TrackingTableSetupState,
    ) -> Result<()>;

    async fn read_source_tracking_info_for_commit(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Option<SourceTrackingInfoForCommit>>;

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
    ) -> Result<()>;

    async fn delete_source_tracking_info(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<()>;

    async fn update_source_tracking_ordinal(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        processed_source_ordinal: Option<i64>,
        db_setup: &TrackingTableSetupState,
    ) -> Result<()>;

    async fn read_source_state(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Option<serde_json::Value>>;

    async fn upsert_source_state(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        state: serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<()>;
}
