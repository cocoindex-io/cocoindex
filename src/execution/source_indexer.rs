use crate::{
    prelude::*,
    service::error::{SharedError, SharedResult, SharedResultExt},
};

use futures::future::Ready;
use sqlx::PgPool;
use std::collections::{HashMap, hash_map};
use tokio::{sync::Semaphore, task::JoinSet};

use super::{
    db_tracking,
    evaluator::SourceRowEvaluationContext,
    row_indexer::{self, SkippedOr, SourceVersion},
    stats,
};

use crate::ops::interface;
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
    pending_update: Mutex<Option<Shared<BoxFuture<'static, SharedResult<()>>>>>,
    update_sem: Semaphore,
    state: Mutex<SourceIndexingState>,
    setup_execution_ctx: Arc<exec_ctx::FlowSetupExecutionContext>,
}

pub const NO_ACK: Option<fn() -> Ready<Result<()>>> = None;

impl SourceIndexingContext {
    pub async fn load(
        flow: Arc<builder::AnalyzedFlow>,
        source_idx: usize,
        setup_execution_ctx: Arc<exec_ctx::FlowSetupExecutionContext>,
        pool: &PgPool,
    ) -> Result<Self> {
        let plan = flow.get_execution_plan().await?;
        let import_op = &plan.import_ops[source_idx];
        let mut list_state = db_tracking::ListTrackedSourceKeyMetadataState::new();
        let mut rows = HashMap::new();
        let scan_generation = 0;
        {
            let mut key_metadata_stream = list_state.list(
                setup_execution_ctx.import_ops[source_idx].source_id,
                &setup_execution_ctx.setup_state.tracking_table,
                pool,
            );
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
        }
        Ok(Self {
            flow,
            source_idx,
            state: Mutex::new(SourceIndexingState {
                rows,
                scan_generation,
            }),
            pending_update: Mutex::new(None),
            update_sem: Semaphore::new(1),
            setup_execution_ctx,
        })
    }

    pub async fn process_source_key<
        AckFut: Future<Output = Result<()>> + Send + 'static,
        AckFn: FnOnce() -> AckFut,
    >(
        self: Arc<Self>,
        key: value::KeyValue,
        source_data: Option<interface::SourceData>,
        update_stats: Arc<stats::UpdateStats>,
        _concur_permit: concur_control::CombinedConcurrencyControllerPermit,
        ack_fn: Option<AckFn>,
        pool: PgPool,
    ) {
        let process = async {
            let plan = self.flow.get_execution_plan().await?;
            let import_op = &plan.import_ops[self.source_idx];
            let schema = &self.flow.data_schema;
            let source_data = match source_data {
                Some(source_data) => source_data,
                None => import_op
                    .executor
                    .get_value(
                        &key,
                        &interface::SourceExecutorGetOptions {
                            include_value: true,
                            include_ordinal: true,
                        },
                    )
                    .await?
                    .try_into()?,
            };

            let source_version = SourceVersion::from_current_data(&source_data);
            let processing_sem = {
                let mut state = self.state.lock().unwrap();
                let touched_generation = state.scan_generation;
                match state.rows.entry(key.clone()) {
                    hash_map::Entry::Occupied(mut entry) => {
                        if entry
                            .get()
                            .source_version
                            .should_skip(&source_version, Some(update_stats.as_ref()))
                        {
                            return anyhow::Ok(());
                        }
                        let sem = entry.get().processing_sem.clone();
                        if source_version.kind == row_indexer::SourceVersionKind::NonExistence {
                            entry.remove();
                        } else {
                            entry.get_mut().source_version = source_version.clone();
                        }
                        sem
                    }
                    hash_map::Entry::Vacant(entry) => {
                        if source_version.kind == row_indexer::SourceVersionKind::NonExistence {
                            update_stats.num_deletions.inc(1);
                            return anyhow::Ok(());
                        }
                        let new_entry = SourceRowIndexingState {
                            source_version: source_version.clone(),
                            touched_generation,
                            ..Default::default()
                        };
                        let sem = new_entry.processing_sem.clone();
                        entry.insert(new_entry);
                        sem
                    }
                }
            };

            {
                let _processing_permit = processing_sem.acquire().await?;
                let result = row_indexer::update_source_row(
                    &SourceRowEvaluationContext {
                        plan: &plan,
                        import_op,
                        schema,
                        key: &key,
                        import_op_idx: self.source_idx,
                    },
                    &self.setup_execution_ctx,
                    source_data.value,
                    &source_version,
                    &pool,
                    &update_stats,
                )
                .await?;
                let target_source_version =
                    if let SkippedOr::Skipped(existing_source_version) = result {
                        Some(existing_source_version)
                    } else if source_version.kind == row_indexer::SourceVersionKind::NonExistence {
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
                                if target_source_version.kind
                                    == row_indexer::SourceVersionKind::NonExistence
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
                            if target_source_version.kind
                                != row_indexer::SourceVersionKind::NonExistence
                            {
                                entry.insert(SourceRowIndexingState {
                                    source_version: target_source_version,
                                    touched_generation: scan_generation,
                                    ..Default::default()
                                });
                            }
                        }
                    }
                }
            }
            if let Some(ack_fn) = ack_fn {
                ack_fn().await?;
            }
            anyhow::Ok(())
        };
        if let Err(e) = process.await {
            update_stats.num_errors.inc(1);
            error!(
                "{:?}",
                e.context(format!(
                    "Error in processing row from source `{source}` with key: {key}",
                    source = self.flow.flow_instance.import_ops[self.source_idx].name
                ))
            );
        }
    }

    pub async fn update(
        self: &Arc<Self>,
        pool: &PgPool,
        update_stats: &Arc<stats::UpdateStats>,
    ) -> Result<()> {
        let pending_update_fut = {
            let mut pending_update = self.pending_update.lock().unwrap();
            if let Some(pending_update_fut) = &*pending_update {
                pending_update_fut.clone()
            } else {
                let slf = self.clone();
                let pool = pool.clone();
                let update_stats = update_stats.clone();
                let task = tokio::spawn(async move {
                    {
                        let _permit = slf.update_sem.acquire().await?;
                        {
                            let mut pending_update = slf.pending_update.lock().unwrap();
                            *pending_update = None;
                        }
                        slf.update_once(&pool, &update_stats).await?;
                    }
                    anyhow::Ok(())
                });
                let pending_update_fut = async move {
                    task.await
                        .map_err(SharedError::from)?
                        .map_err(SharedError::new)
                }
                .boxed()
                .shared();
                *pending_update = Some(pending_update_fut.clone());
                pending_update_fut
            }
        };
        pending_update_fut.await.std_result()?;
        Ok(())
    }

    async fn update_once(
        self: &Arc<Self>,
        pool: &PgPool,
        update_stats: &Arc<stats::UpdateStats>,
    ) -> Result<()> {
        let plan = self.flow.get_execution_plan().await?;
        let import_op = &plan.import_ops[self.source_idx];
        let mut rows_stream = import_op
            .executor
            .list(&interface::SourceExecutorListOptions {
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
                let source_version = SourceVersion::from_current_with_ordinal(
                    row.ordinal
                        .ok_or_else(|| anyhow::anyhow!("ordinal is not available"))?,
                );
                {
                    let mut state = self.state.lock().unwrap();
                    let scan_generation = state.scan_generation;
                    let row_state = state.rows.entry(row.key.clone()).or_default();
                    row_state.touched_generation = scan_generation;
                    if row_state
                        .source_version
                        .should_skip(&source_version, Some(update_stats.as_ref()))
                    {
                        continue;
                    }
                }
                let concur_permit = import_op
                    .concurrency_controller
                    .acquire(concur_control::BYTES_UNKNOWN_YET)
                    .await?;
                join_set.spawn(self.clone().process_source_key(
                    row.key,
                    None,
                    update_stats.clone(),
                    concur_permit,
                    NO_ACK,
                    pool.clone(),
                ));
            }
        }
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                if !e.is_cancelled() {
                    error!("{e:?}");
                }
            }
        }

        let deleted_key_versions = {
            let mut deleted_key_versions = Vec::new();
            let state = self.state.lock().unwrap();
            for (key, row_state) in state.rows.iter() {
                if row_state.touched_generation < scan_generation {
                    deleted_key_versions.push((key.clone(), row_state.source_version.ordinal));
                }
            }
            deleted_key_versions
        };
        for (key, source_ordinal) in deleted_key_versions {
            // If the source ordinal is unavailable, call without source ordinal so that another polling will be triggered to avoid out-of-order.
            let source_data = source_ordinal
                .is_available()
                .then(|| interface::SourceData {
                    value: interface::SourceValue::NonExistence,
                    ordinal: source_ordinal,
                });
            let concur_permit = import_op.concurrency_controller.acquire(Some(|| 0)).await?;
            join_set.spawn(self.clone().process_source_key(
                key,
                source_data,
                update_stats.clone(),
                concur_permit,
                NO_ACK,
                pool.clone(),
            ));
        }
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                if !e.is_cancelled() {
                    error!("{e:?}");
                }
            }
        }

        Ok(())
    }
}
