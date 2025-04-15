use crate::{ops::interface::SourceValueChange, prelude::*};

use sqlx::PgPool;
use std::collections::{hash_map, HashMap};
use tokio::{sync::Semaphore, task::JoinSet};

use super::{
    db_tracking,
    row_indexer::{self, SkippedOr, SourceVersion, SourceVersionKind},
    stats,
};
struct SourceRowIndexingState {
    source_version: SourceVersion,
    processing_sem: Arc<Semaphore>,
    touched_generation: usize,
}

impl Default for SourceRowIndexingState {
    fn default() -> Self {
        Self {
            source_version: SourceVersion::default(),
            processing_sem: Arc::new(Semaphore::new(1)),
            touched_generation: 0,
        }
    }
}

struct SourceIndexingState {
    rows: HashMap<value::KeyValue, SourceRowIndexingState>,
    scan_generation: usize,
}
pub struct SourceIndexingContext {
    flow: Arc<builder::AnalyzedFlow>,
    source_idx: usize,
    state: Mutex<SourceIndexingState>,
}

impl SourceIndexingContext {
    pub async fn load(
        flow: Arc<builder::AnalyzedFlow>,
        source_idx: usize,
        pool: &PgPool,
    ) -> Result<Self> {
        let plan = flow.get_execution_plan().await?;
        let import_op = &plan.import_ops[source_idx];
        let mut list_state = db_tracking::ListTrackedSourceKeyMetadataState::new();
        let mut rows = HashMap::new();
        let mut key_metadata_stream =
            list_state.list(import_op.source_id, &plan.tracking_table_setup, pool);
        let scan_generation = 0;
        while let Some(key_metadata) = key_metadata_stream.next().await {
            let key_metadata = key_metadata?;
            let source_key = value::Value::<value::ScopeValue>::from_json(
                key_metadata.source_key,
                &import_op.primary_key_type,
            )?
            .into_key()?;
            rows.insert(
                source_key,
                SourceRowIndexingState {
                    source_version: SourceVersion::from_stored(
                        key_metadata.processed_source_ordinal,
                        &key_metadata.process_logic_fingerprint,
                        plan.logic_fingerprint,
                    ),
                    processing_sem: Arc::new(Semaphore::new(1)),
                    touched_generation: scan_generation,
                },
            );
        }
        Ok(Self {
            flow,
            source_idx,
            state: Mutex::new(SourceIndexingState {
                rows,
                scan_generation,
            }),
        })
    }

    async fn process_source_key(
        self: Arc<Self>,
        key: value::KeyValue,
        source_version: SourceVersion,
        value: Option<value::FieldValues>,
        update_stats: Arc<stats::UpdateStats>,
        processing_sem: Arc<Semaphore>,
        pool: PgPool,
    ) {
        let process = async {
            let permit = processing_sem.acquire().await?;
            let plan = self.flow.get_execution_plan().await?;
            let import_op = &plan.import_ops[self.source_idx];
            let source_value = if source_version.kind == row_indexer::SourceVersionKind::Deleted {
                None
            } else if let Some(value) = value {
                Some(value)
            } else {
                // Even if the source version kind is not Deleted, the source value might be gone one polling.
                // In this case, we still use the current source version even if it's already stale - actually this version skew
                // also happens for update cases and there's no way to keep them always in sync for many sources.
                //
                // We only need source version <= actual version for value.
                import_op.executor.get_value(&key).await?
            };
            let schema = &self.flow.data_schema;
            let result = row_indexer::update_source_row(
                &plan,
                import_op,
                schema,
                &key,
                source_value,
                &source_version,
                &pool,
                &update_stats,
            )
            .await?;
            let target_source_version = if let SkippedOr::Skipped(existing_source_version) = result
            {
                Some(existing_source_version)
            } else if source_version.kind == row_indexer::SourceVersionKind::Deleted {
                Some(source_version)
            } else {
                None
            };
            if let Some(target_source_version) = target_source_version {
                let mut state = self.state.lock().unwrap();
                let scan_generation = state.scan_generation;
                let entry = state.rows.entry(key.clone());
                match entry {
                    hash_map::Entry::Occupied(mut entry) => {
                        if !entry
                            .get()
                            .source_version
                            .should_skip(&target_source_version, None)
                        {
                            if target_source_version.kind == row_indexer::SourceVersionKind::Deleted
                            {
                                entry.remove();
                            } else {
                                let mut_entry = entry.get_mut();
                                mut_entry.source_version = target_source_version;
                                mut_entry.touched_generation = scan_generation;
                            }
                        }
                    }
                    hash_map::Entry::Vacant(entry) => {
                        entry.insert(SourceRowIndexingState {
                            source_version: target_source_version,
                            touched_generation: scan_generation,
                            ..Default::default()
                        });
                    }
                }
            }
            drop(permit);
            anyhow::Ok(())
        };
        if let Err(e) = process.await {
            update_stats.num_errors.inc(1);
            error!("{:?}", e.context("Error in processing a source row"));
        }
    }

    fn process_source_key_if_newer(
        self: &Arc<Self>,
        key: value::KeyValue,
        source_version: SourceVersion,
        value: Option<value::FieldValues>,
        update_stats: &Arc<stats::UpdateStats>,
        pool: &PgPool,
    ) -> Option<impl Future<Output = ()> + Send + 'static> {
        let processing_sem = {
            let mut state = self.state.lock().unwrap();
            let scan_generation = state.scan_generation;
            let row_state = state.rows.entry(key.clone()).or_default();
            row_state.touched_generation = scan_generation;
            if row_state
                .source_version
                .should_skip(&source_version, Some(update_stats))
            {
                return None;
            }
            row_state.source_version = source_version.clone();
            row_state.processing_sem.clone()
        };
        Some(self.clone().process_source_key(
            key,
            source_version,
            value,
            update_stats.clone(),
            processing_sem,
            pool.clone(),
        ))
    }

    pub async fn update(
        self: &Arc<Self>,
        pool: &PgPool,
        update_stats: &Arc<stats::UpdateStats>,
    ) -> Result<()> {
        let plan = self.flow.get_execution_plan().await?;
        let import_op = &plan.import_ops[self.source_idx];
        let mut rows_stream = import_op
            .executor
            .list(interface::SourceExecutorListOptions {
                include_ordinal: true,
            });
        let mut join_set = JoinSet::new();
        let scan_generation = {
            let mut state = self.state.lock().unwrap();
            state.scan_generation += 1;
            state.scan_generation
        };
        while let Some(row) = rows_stream.next().await {
            for row in row? {
                self.process_source_key_if_newer(
                    row.key,
                    SourceVersion::from_current(row.ordinal),
                    None,
                    update_stats,
                    pool,
                )
                .map(|fut| join_set.spawn(fut));
            }
        }
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                if !e.is_cancelled() {
                    error!("{:?}", e);
                }
            }
        }

        let deleted_key_versions = {
            let mut deleted_key_versions = Vec::new();
            let mut state = self.state.lock().unwrap();
            for (key, row_state) in state.rows.iter_mut() {
                if row_state.touched_generation < scan_generation {
                    deleted_key_versions.push((
                        key.clone(),
                        row_state.source_version.for_deletion(),
                        row_state.processing_sem.clone(),
                    ));
                }
            }
            deleted_key_versions
        };
        for (key, source_version, processing_sem) in deleted_key_versions {
            join_set.spawn(self.clone().process_source_key(
                key,
                source_version,
                None,
                update_stats.clone(),
                processing_sem,
                pool.clone(),
            ));
        }
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                if !e.is_cancelled() {
                    error!("{:?}", e);
                }
            }
        }

        Ok(())
    }

    pub fn process_change(
        self: &Arc<Self>,
        change: interface::SourceChange,
        pool: &PgPool,
        update_stats: &Arc<stats::UpdateStats>,
    ) -> Option<impl Future<Output = ()> + Send + 'static> {
        let (source_version_kind, value) = match change.value {
            SourceValueChange::Upsert(value) => (SourceVersionKind::CurrentLogic, value),
            SourceValueChange::Delete => (SourceVersionKind::Deleted, None),
        };
        self.process_source_key_if_newer(
            change.key,
            SourceVersion {
                ordinal: change.ordinal,
                kind: source_version_kind,
            },
            value,
            update_stats,
            pool,
        )
    }
}
