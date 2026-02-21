use crate::prelude::*;

use crate::execution::db_tracking;
use crate::execution::db_tracking_setup::TrackingTableSetupChange;
use crate::execution::db_tracking_setup::TrackingTableSetupState;
use crate::persistence::{InternalPersistence, InternalPersistenceTxn};
use crate::setup::db_metadata;
use crate::setup::db_metadata::{ResourceTypeKey, SetupMetadataRecord, StateUpdateInfo};
use async_trait::async_trait;
use sqlx::PgPool;
use utils::db::WriteAction;

pub struct PostgresPersistence {
    pool: PgPool,
}

impl PostgresPersistence {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

pub struct PostgresPersistenceTxn {
    conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
    finished: bool,
}

#[async_trait]
impl InternalPersistence for PostgresPersistence {
    async fn read_setup_metadata(&self) -> Result<Option<Vec<SetupMetadataRecord>>> {
        db_metadata::read_setup_metadata(&self.pool).await
    }

    async fn stage_changes_for_flow(
        &self,
        flow_name: &str,
        seen_metadata_version: Option<u64>,
        resource_update_info: &HashMap<ResourceTypeKey, StateUpdateInfo>,
    ) -> Result<u64> {
        db_metadata::stage_changes_for_flow(
            flow_name,
            seen_metadata_version,
            resource_update_info,
            &self.pool,
        )
        .await
    }

    async fn commit_changes_for_flow(
        &self,
        flow_name: &str,
        curr_metadata_version: u64,
        state_updates: &HashMap<ResourceTypeKey, StateUpdateInfo>,
        delete_version: bool,
    ) -> Result<()> {
        db_metadata::commit_changes_for_flow(
            flow_name,
            curr_metadata_version,
            state_updates,
            delete_version,
            &self.pool,
        )
        .await
    }

    async fn apply_metadata_table_setup(&self, metadata_table_missing: bool) -> Result<()> {
        if !metadata_table_missing {
            return Ok(());
        }
        // Reuse existing logic by instantiating the setup-change type and applying it using the pool.
        // (This will be refactored further when Surreal implementation lands.)
        let query_str = format!(
            "CREATE TABLE IF NOT EXISTS cocoindex_setup_metadata (
                flow_name TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                key JSONB NOT NULL,
                state JSONB,
                staging_changes JSONB NOT NULL,

                PRIMARY KEY (flow_name, resource_type, key)
            )"
        );
        sqlx::query(&query_str).execute(&self.pool).await?;
        Ok(())
    }

    async fn apply_tracking_table_setup_change(
        &self,
        change: &TrackingTableSetupChange,
    ) -> Result<()> {
        // Delegate to the existing SQL implementation for Postgres by temporarily using its method body logic:
        // call the module-level apply via its existing public API in db_tracking_setup.
        // The current code lives on the type itself and grabs the builtin pool from LibContext; so we replicate here
        // to keep the refactor scoped.
        crate::execution::db_tracking_setup::apply_change_with_pool(change, &self.pool).await
    }

    async fn begin_txn(&self) -> Result<Box<dyn InternalPersistenceTxn>> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query("BEGIN").execute(&mut *conn).await?;
        Ok(Box::new(PostgresPersistenceTxn {
            conn,
            finished: false,
        }))
    }

    async fn list_tracked_source_key_metadata(
        &self,
        source_id: i32,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Vec<db_tracking::TrackedSourceKeyMetadata>> {
        let mut list_state = db_tracking::ListTrackedSourceKeyMetadataState::new();
        let mut stream = list_state.list(source_id, db_setup, &self.pool);
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item?);
        }
        Ok(out)
    }

    async fn read_source_last_processed_info(
        &self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Option<db_tracking::SourceLastProcessedInfo>> {
        db_tracking::read_source_last_processed_info(
            source_id,
            source_key_json,
            db_setup,
            &self.pool,
        )
        .await
    }
}

#[async_trait]
impl InternalPersistenceTxn for PostgresPersistenceTxn {
    async fn commit(self: Box<Self>) -> Result<()> {
        let mut this = *self;
        if !this.finished {
            sqlx::query("COMMIT").execute(&mut *this.conn).await?;
            this.finished = true;
        }
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        let mut this = *self;
        if !this.finished {
            sqlx::query("ROLLBACK").execute(&mut *this.conn).await?;
            this.finished = true;
        }
        Ok(())
    }

    async fn read_source_tracking_info_for_processing(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Option<db_tracking::SourceTrackingInfoForProcessing>> {
        db_tracking::read_source_tracking_info_for_processing(
            source_id,
            source_key_json,
            db_setup,
            &mut *self.conn,
        )
        .await
    }

    async fn read_source_tracking_info_for_precommit(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Option<db_tracking::SourceTrackingInfoForPrecommit>> {
        db_tracking::read_source_tracking_info_for_precommit(
            source_id,
            source_key_json,
            db_setup,
            &mut *self.conn,
        )
        .await
    }

    async fn precommit_source_tracking_info(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        max_process_ordinal: i64,
        staging_target_keys: db_tracking::TrackedTargetKeyForSource,
        memoization_info: Option<&crate::execution::memoization::StoredMemoizationInfo>,
        db_setup: &TrackingTableSetupState,
        action: WriteAction,
    ) -> Result<()> {
        db_tracking::precommit_source_tracking_info(
            source_id,
            source_key_json,
            max_process_ordinal,
            staging_target_keys,
            memoization_info,
            db_setup,
            &mut *self.conn,
            action,
        )
        .await
    }

    async fn touch_max_process_ordinal(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        process_ordinal: i64,
        db_setup: &TrackingTableSetupState,
    ) -> Result<()> {
        db_tracking::touch_max_process_ordinal(
            source_id,
            source_key_json,
            process_ordinal,
            db_setup,
            &mut *self.conn,
        )
        .await
    }

    async fn read_source_tracking_info_for_commit(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Option<db_tracking::SourceTrackingInfoForCommit>> {
        db_tracking::read_source_tracking_info_for_commit(
            source_id,
            source_key_json,
            db_setup,
            &mut *self.conn,
        )
        .await
    }

    async fn commit_source_tracking_info(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        staging_target_keys: db_tracking::TrackedTargetKeyForSource,
        processed_source_ordinal: Option<i64>,
        processed_source_fp: Option<Vec<u8>>,
        logic_fingerprint: &[u8],
        process_ordinal: i64,
        process_time_micros: i64,
        target_keys: db_tracking::TrackedTargetKeyForSource,
        db_setup: &TrackingTableSetupState,
        action: WriteAction,
    ) -> Result<()> {
        db_tracking::commit_source_tracking_info(
            source_id,
            source_key_json,
            staging_target_keys,
            processed_source_ordinal,
            processed_source_fp,
            logic_fingerprint,
            process_ordinal,
            process_time_micros,
            target_keys,
            db_setup,
            &mut *self.conn,
            action,
        )
        .await
    }

    async fn delete_source_tracking_info(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<()> {
        db_tracking::delete_source_tracking_info(
            source_id,
            source_key_json,
            db_setup,
            &mut *self.conn,
        )
        .await
    }

    async fn update_source_tracking_ordinal(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        processed_source_ordinal: Option<i64>,
        db_setup: &TrackingTableSetupState,
    ) -> Result<()> {
        db_tracking::update_source_tracking_ordinal(
            source_id,
            source_key_json,
            processed_source_ordinal,
            db_setup,
            &mut *self.conn,
        )
        .await
    }

    async fn read_source_state(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<Option<serde_json::Value>> {
        db_tracking::read_source_state(source_id, source_key_json, db_setup, &mut *self.conn).await
    }

    async fn upsert_source_state(
        &mut self,
        source_id: i32,
        source_key_json: &serde_json::Value,
        state: serde_json::Value,
        db_setup: &TrackingTableSetupState,
    ) -> Result<()> {
        db_tracking::upsert_source_state(
            source_id,
            source_key_json,
            state,
            db_setup,
            &mut *self.conn,
        )
        .await
    }
}
