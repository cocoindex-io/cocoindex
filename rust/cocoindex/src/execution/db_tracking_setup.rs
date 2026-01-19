use crate::prelude::*;

use crate::execution::db_tracking;
use crate::setup::{CombinedState, ResourceSetupChange, ResourceSetupInfo, SetupChangeType};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

pub fn default_tracking_table_name(flow_name: &str) -> String {
    format!(
        "{}__cocoindex_tracking",
        utils::db::sanitize_identifier(flow_name)
    )
}

pub fn default_source_state_table_name(flow_name: &str) -> String {
    format!(
        "{}__cocoindex_srcstate",
        utils::db::sanitize_identifier(flow_name)
    )
}

pub const CURRENT_TRACKING_TABLE_VERSION: i32 = 1;

async fn upgrade_tracking_table(
    pool: &PgPool,
    desired_state: &TrackingTableSetupState,
    existing_version_id: i32,
) -> Result<()> {
    if existing_version_id < 1 && desired_state.version_id >= 1 {
        let table_name = &desired_state.table_name;
        let opt_fast_fingerprint_column = desired_state
            .has_fast_fingerprint_column
            .then(|| "processed_source_fp BYTEA,")
            .unwrap_or("");
        let query =  format!(
            "CREATE TABLE IF NOT EXISTS {table_name} (
                source_id INTEGER NOT NULL,
                source_key JSONB NOT NULL,

                -- Update in the precommit phase: after evaluation done, before really applying the changes to the target storage.
                max_process_ordinal BIGINT NOT NULL,
                staging_target_keys JSONB NOT NULL,
                memoization_info JSONB,

                -- Update after applying the changes to the target storage.
                processed_source_ordinal BIGINT,
                {opt_fast_fingerprint_column}
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

async fn create_source_state_table(pool: &PgPool, table_name: &str) -> Result<()> {
    let query = format!(
        "CREATE TABLE IF NOT EXISTS {table_name} (
            source_id INTEGER NOT NULL,
            key JSONB NOT NULL,
            value JSONB NOT NULL,

            PRIMARY KEY (source_id, key)
        )"
    );
    sqlx::query(&query).execute(pool).await?;
    Ok(())
}

async fn delete_source_states_for_sources(
    pool: &PgPool,
    table_name: &str,
    source_ids: &Vec<i32>,
) -> Result<()> {
    let query = format!("DELETE FROM {} WHERE source_id = ANY($1)", table_name,);
    sqlx::query(&query).bind(source_ids).execute(pool).await?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TrackingTableSetupState {
    pub table_name: String,
    pub version_id: i32,
    #[serde(default)]
    pub source_state_table_name: Option<String>,
    #[serde(default)]
    pub has_fast_fingerprint_column: bool,
}

#[derive(Debug)]
pub struct TrackingTableSetupChange {
    pub desired_state: Option<TrackingTableSetupState>,

    pub min_existing_version_id: Option<i32>,
    pub legacy_tracking_table_names: BTreeSet<String>,

    pub source_state_table_always_exists: bool,
    pub legacy_source_state_table_names: BTreeSet<String>,

    pub source_names_need_state_cleanup: BTreeMap<i32, BTreeSet<String>>,

    /// Target information for cleanup (target_id -> target_kind, key_schema)
    pub desired_targets: Option<BTreeMap<i32, (String, Box<[schema::ValueType]>)>>,

    /// Export contexts for targets (target_id -> export_context)
    pub export_contexts: Option<Arc<BTreeMap<i32, Arc<dyn Any + Send + Sync>>>>,

    has_state_change: bool,
}

impl TrackingTableSetupChange {
    pub fn new(
        desired: Option<&TrackingTableSetupState>,
        existing: &CombinedState<TrackingTableSetupState>,
        source_names_need_state_cleanup: BTreeMap<i32, BTreeSet<String>>,
        desired_targets: Option<BTreeMap<i32, (String, Box<[schema::ValueType]>)>>,
    ) -> Option<Self> {
        let legacy_tracking_table_names = existing
            .legacy_values(desired, |v| &v.table_name)
            .into_iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let legacy_source_state_table_names = existing
            .legacy_values(desired, |v| &v.source_state_table_name)
            .into_iter()
            .filter_map(|v| v.clone())
            .collect::<BTreeSet<_>>();
        let min_existing_version_id = existing
            .always_exists()
            .then(|| existing.possible_versions().map(|v| v.version_id).min())
            .flatten();
        if desired.is_some() || min_existing_version_id.is_some() {
            Some(Self {
                desired_state: desired.cloned(),
                legacy_tracking_table_names,
                source_state_table_always_exists: existing.always_exists()
                    && existing
                        .possible_versions()
                        .all(|v| v.source_state_table_name.is_some()),
                legacy_source_state_table_names,
                min_existing_version_id,
                source_names_need_state_cleanup,
                desired_targets,
                export_contexts: None,
                has_state_change: existing.has_state_diff(desired, |v| v),
            })
        } else {
            None
        }
    }

    /// Attach export contexts for targets (called after targets are built)
    pub fn attach_export_contexts(&mut self, contexts: BTreeMap<i32, Arc<dyn Any + Send + Sync>>) {
        self.export_contexts = Some(Arc::new(contexts));
    }

    pub fn into_setup_info(
        self,
    ) -> ResourceSetupInfo<(), TrackingTableSetupState, TrackingTableSetupChange> {
        ResourceSetupInfo {
            key: (),
            state: self.desired_state.clone(),
            has_tracked_state_change: self.has_state_change,
            description: "Internal Storage for Tracking".to_string(),
            setup_change: Some(self),
            legacy_key: None,
        }
    }
}

impl ResourceSetupChange for TrackingTableSetupChange {
    fn describe_changes(&self) -> Vec<setup::ChangeDescription> {
        let mut changes: Vec<setup::ChangeDescription> = vec![];
        if self.desired_state.is_some() && !self.legacy_tracking_table_names.is_empty() {
            changes.push(setup::ChangeDescription::Action(format!(
                "Rename legacy tracking tables: {}. ",
                self.legacy_tracking_table_names.iter().join(", ")
            )));
        }
        match (self.min_existing_version_id, &self.desired_state) {
            (None, Some(state)) => {
                changes.push(setup::ChangeDescription::Action(format!(
                    "Create the tracking table: {}. ",
                    state.table_name
                )));
            }
            (Some(min_version_id), Some(desired)) => {
                if min_version_id < desired.version_id {
                    changes.push(setup::ChangeDescription::Action(
                        "Update the tracking table. ".into(),
                    ));
                }
            }
            (Some(_), None) => changes.push(setup::ChangeDescription::Action(format!(
                "Drop existing tracking table: {}. ",
                self.legacy_tracking_table_names.iter().join(", ")
            ))),
            (None, None) => (),
        }

        let source_state_table_name = self
            .desired_state
            .as_ref()
            .and_then(|v| v.source_state_table_name.as_ref());
        if let Some(source_state_table_name) = source_state_table_name {
            if !self.legacy_source_state_table_names.is_empty() {
                changes.push(setup::ChangeDescription::Action(format!(
                    "Rename legacy source state tables: {}. ",
                    self.legacy_source_state_table_names.iter().join(", ")
                )));
            }
            if !self.source_state_table_always_exists {
                changes.push(setup::ChangeDescription::Action(format!(
                    "Create the source state table: {}. ",
                    source_state_table_name
                )));
            }
        } else if !self.source_state_table_always_exists
            && !self.legacy_source_state_table_names.is_empty()
        {
            changes.push(setup::ChangeDescription::Action(format!(
                "Drop existing source state table: {}. ",
                self.legacy_source_state_table_names.iter().join(", ")
            )));
        }

        if !self.source_names_need_state_cleanup.is_empty() {
            changes.push(setup::ChangeDescription::Action(format!(
                "Clean up {} legacy source(s) including tracking metadata, target data, and source states: {}. ",
                self.source_names_need_state_cleanup.len(),
                self.source_names_need_state_cleanup
                    .values()
                    .flatten()
                    .dedup()
                    .join(", ")
            )));
        }
        changes
    }

    fn change_type(&self) -> SetupChangeType {
        match (self.min_existing_version_id, &self.desired_state) {
            (None, Some(_)) => SetupChangeType::Create,
            (Some(min_version_id), Some(desired)) => {
                let source_state_table_up_to_date = self.legacy_source_state_table_names.is_empty()
                    && self.source_names_need_state_cleanup.is_empty()
                    && (self.source_state_table_always_exists
                        || desired.source_state_table_name.is_none());

                if min_version_id == desired.version_id
                    && self.legacy_tracking_table_names.is_empty()
                    && source_state_table_up_to_date
                {
                    SetupChangeType::NoChange
                } else if min_version_id < desired.version_id || !source_state_table_up_to_date {
                    SetupChangeType::Update
                } else {
                    SetupChangeType::Invalid
                }
            }
            (Some(_), None) => SetupChangeType::Delete,
            (None, None) => SetupChangeType::NoChange,
        }
    }
}

impl TrackingTableSetupChange {
    pub async fn apply_change(&self) -> Result<()> {
        let lib_context = get_lib_context().await?;
        let pool = lib_context.require_builtin_db_pool()?;
        if let Some(desired) = &self.desired_state {
            for lagacy_name in self.legacy_tracking_table_names.iter() {
                let query = format!(
                    "ALTER TABLE IF EXISTS {} RENAME TO {}",
                    lagacy_name, desired.table_name
                );
                sqlx::query(&query).execute(pool).await?;
            }

            if self.min_existing_version_id != Some(desired.version_id) {
                upgrade_tracking_table(pool, desired, self.min_existing_version_id.unwrap_or(0))
                    .await?;
            }
        } else {
            for lagacy_name in self.legacy_tracking_table_names.iter() {
                let query = format!("DROP TABLE IF EXISTS {lagacy_name}");
                sqlx::query(&query).execute(pool).await?;
            }
        }

        let source_state_table_name = self
            .desired_state
            .as_ref()
            .and_then(|v| v.source_state_table_name.as_ref());
        if let Some(source_state_table_name) = source_state_table_name {
            for lagacy_name in self.legacy_source_state_table_names.iter() {
                let query = format!(
                    "ALTER TABLE IF EXISTS {lagacy_name} RENAME TO {source_state_table_name}"
                );
                sqlx::query(&query).execute(pool).await?;
            }
            if !self.source_state_table_always_exists {
                create_source_state_table(pool, source_state_table_name).await?;
            }

            // Clean up tracking metadata and target data for stale sources
            if !self.source_names_need_state_cleanup.is_empty() {
                self.cleanup_stale_sources(pool).await?;
            }

            if !self.source_names_need_state_cleanup.is_empty() {
                delete_source_states_for_sources(
                    pool,
                    source_state_table_name,
                    &self
                        .source_names_need_state_cleanup
                        .keys()
                        .map(|v| *v)
                        .collect::<Vec<_>>(),
                )
                .await?;
            }
        } else {
            for lagacy_name in self.legacy_source_state_table_names.iter() {
                let query = format!("DROP TABLE IF EXISTS {lagacy_name}");
                sqlx::query(&query).execute(pool).await?;
            }
        }
        Ok(())
    }

    /// Clean up tracking metadata and target data for stale sources
    async fn cleanup_stale_sources(&self, pool: &PgPool) -> Result<()> {
        // Early return if flow is being dropped
        let Some(desired) = &self.desired_state else {
            tracing::info!("Skipping stale source cleanup: flow is being dropped");
            return Ok(());
        };

        let source_ids: Vec<i32> = self
            .source_names_need_state_cleanup
            .keys()
            .copied()
            .collect();

        tracing::info!(
            "Cleaning up tracking metadata and target data for {} stale source(s): {:?}",
            source_ids.len(),
            source_ids
        );

        // Stream tracking entries instead of loading all at once
        let entries_stream = db_tracking::read_tracking_entries_for_sources_stream(
            source_ids.clone(),
            desired.clone(),
            pool.clone(),
        );

        // Process with limited parallelism
        const MAX_CONCURRENT_ROW_CLEANUPS: usize = 10;
        self.cleanup_tracking_entries_streaming(
            Box::pin(entries_stream),
            pool,
            MAX_CONCURRENT_ROW_CLEANUPS,
        )
        .await?;

        // Delete tracking metadata
        let rows_deleted =
            db_tracking::delete_tracking_entries_for_sources(&source_ids, desired, pool).await?;

        tracing::info!(
            "Deleted {} tracking entries for stale sources",
            rows_deleted
        );

        Ok(())
    }

    /// Stream tracking entries and clean up target data with limited parallelism
    async fn cleanup_tracking_entries_streaming(
        &self,
        mut entries_stream: std::pin::Pin<
            Box<dyn Stream<Item = Result<db_tracking::SourceTrackingEntryForCleanup>> + Send>,
        >,
        _pool: &PgPool,
        max_concurrent: usize,
    ) -> Result<()> {
        use tokio::sync::Semaphore;
        use tokio::task::JoinSet;

        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        let mut join_set = JoinSet::new();
        let mut total_processed = 0u64;
        let mut total_deleted = 0u64;

        // Stream entries one by one
        while let Some(entry) = entries_stream.try_next().await? {
            total_processed += 1;

            let permit = semaphore.clone().acquire_owned().await?;

            let desired_targets = self.desired_targets.clone();
            let export_contexts = self.export_contexts.clone();

            join_set.spawn(async move {
                let result = Self::cleanup_single_tracking_entry(
                    entry,
                    desired_targets.as_ref(),
                    export_contexts.as_ref(),
                )
                .await;
                drop(permit);
                result
            });
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(deleted_count)) => total_deleted += deleted_count,
                Ok(Err(e)) => return Err(e),
                Err(e) if !e.is_cancelled() => {
                    return Err(internal_error!("Task panicked: {:?}", e));
                }
                _ => {}
            }
        }

        tracing::info!(
            "Processed {} tracking entries, deleted {} target rows",
            total_processed,
            total_deleted
        );

        Ok(())
    }

    /// Process a single tracking entry and delete its target data
    async fn cleanup_single_tracking_entry(
        entry: db_tracking::SourceTrackingEntryForCleanup,
        desired_targets: Option<&BTreeMap<i32, (String, Box<[schema::ValueType]>)>>,
        export_contexts: Option<&Arc<BTreeMap<i32, Arc<dyn Any + Send + Sync>>>>,
    ) -> Result<u64> {
        let Some(desired_targets) = desired_targets else {
            return Ok(0);
        };

        let Some(target_keys) = entry.target_keys else {
            return Ok(0);
        };

        let mut total_deleted = 0u64;

        // One tracking entry can have keys for multiple targets (fanout)
        for (target_id, tracked_keys) in target_keys {
            // Case 1 vs Case 2: Is this target still in the flow?
            let (target_kind, key_schema) = match desired_targets.get(&target_id) {
                Some((kind, schema)) => (kind, schema),
                None => {
                    // Target dropped, skip cleanup
                    tracing::debug!("Skipping target_id {}: target no longer exists", target_id);
                    continue;
                }
            };

            // Parse tracked keys
            let mut parsed_keys = Vec::new();
            for tracked_key_info in tracked_keys {
                match Self::parse_key_from_json_static(&tracked_key_info.key, key_schema) {
                    Ok(key) => parsed_keys.push(key),
                    Err(e) => {
                        tracing::warn!(
                            "Failed to parse key for target_id {}: {}. Skipping key.",
                            target_id,
                            e
                        );
                    }
                }
            }

            if parsed_keys.is_empty() {
                continue;
            }

            // Get export context for this target
            let export_context = export_contexts
                .and_then(|ctxs| ctxs.get(&target_id))
                .ok_or_else(|| internal_error!("No export context for target {}", target_id))?;

            // Delete via target factory
            let factory = crate::ops::get_target_factory(target_kind)?;

            let deletes: Vec<interface::ExportTargetDeleteEntry> = parsed_keys
                .into_iter()
                .map(|key| interface::ExportTargetDeleteEntry {
                    key,
                    additional_key: serde_json::Value::Null,
                })
                .collect();

            let delete_count = deletes.len() as u64;

            factory
                .apply_mutation(vec![interface::ExportTargetMutationWithContext {
                    mutation: interface::ExportTargetMutation {
                        upserts: vec![],
                        deletes,
                    },
                    export_context: &**export_context,
                }])
                .await?;

            total_deleted += delete_count;
        }

        Ok(total_deleted)
    }

    /// Parse a key from JSON using the key schema
    fn parse_key_from_json_static(
        key_json: &serde_json::Value,
        key_schema: &[schema::ValueType],
    ) -> Result<value::KeyValue> {
        let field_schemas: Vec<schema::FieldSchema> = key_schema
            .iter()
            .enumerate()
            .map(|(i, value_type)| schema::FieldSchema {
                name: format!("_key_{}", i),
                value_type: schema::EnrichedValueType {
                    typ: value_type.clone(),
                    nullable: false,
                    attrs: Arc::new(BTreeMap::new()),
                },
                description: None,
            })
            .collect();

        value::KeyValue::from_json(key_json.clone(), &field_schemas)
            .with_context(|| format!("Failed to parse key: {}", key_json))
    }
}
