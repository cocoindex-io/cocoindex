use crate::{
    execution::row_indexer::ContentHashBasedCollapsingBaseline,
    prelude::*,
    service::error::{SharedError, SharedResult, SharedResultExt},
};

use futures::future::Ready;
use sqlx::PgPool;
use std::collections::{HashMap, hash_map};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};

use super::{
    db_tracking,
    evaluator::SourceRowEvaluationContext,
    row_indexer::{self, SkippedOr, SourceVersion},
    stats,
};

use crate::ops::interface;
struct SourceRowIndexingState {
    source_version: SourceVersion,
    content_version_fp: Option<Vec<u8>>,
    processing_sem: Arc<Semaphore>,
    touched_generation: usize,
}

impl Default for SourceRowIndexingState {
    fn default() -> Self {
        Self {
            source_version: SourceVersion::default(),
            content_version_fp: None,
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

struct LocalSourceRowStateOperator<'a> {
    key: &'a value::KeyValue,
    indexing_state: &'a Mutex<SourceIndexingState>,
    update_stats: &'a Arc<stats::UpdateStats>,

    processing_sem: Option<Arc<Semaphore>>,
    processing_sem_permit: Option<OwnedSemaphorePermit>,
    last_source_version: Option<SourceVersion>,
}

enum RowStateAdvanceOutcome {
    Skipped,
    Advanced {
        prev_source_version: Option<SourceVersion>,
        prev_content_version_fp: Option<Vec<u8>>,
    },
    Noop,
}

impl<'a> LocalSourceRowStateOperator<'a> {
    fn new(
        key: &'a value::KeyValue,
        indexing_state: &'a Mutex<SourceIndexingState>,
        update_stats: &'a Arc<stats::UpdateStats>,
    ) -> Self {
        Self {
            key,
            indexing_state,
            update_stats,
            processing_sem: None,
            processing_sem_permit: None,
            last_source_version: None,
        }
    }
    async fn advance(
        &mut self,
        source_version: SourceVersion,
        content_version_fp: Option<&Vec<u8>>,
    ) -> Result<RowStateAdvanceOutcome> {
        let (sem, outcome) = {
            let mut state = self.indexing_state.lock().unwrap();
            let touched_generation = state.scan_generation;

            if self.last_source_version == Some(source_version) {
                return Ok(RowStateAdvanceOutcome::Noop);
            }
            self.last_source_version = Some(source_version);

            match state.rows.entry(self.key.clone()) {
                hash_map::Entry::Occupied(mut entry) => {
                    if entry
                        .get()
                        .source_version
                        .should_skip(&source_version, Some(self.update_stats.as_ref()))
                    {
                        return Ok(RowStateAdvanceOutcome::Skipped);
                    }
                    let entry_sem = &entry.get().processing_sem;
                    let sem = if self
                        .processing_sem
                        .as_ref()
                        .is_none_or(|sem| !Arc::ptr_eq(sem, &entry_sem))
                    {
                        Some(entry_sem.clone())
                    } else {
                        None
                    };

                    let entry_mut = entry.get_mut();
                    let outcome = RowStateAdvanceOutcome::Advanced {
                        prev_source_version: Some(std::mem::take(&mut entry_mut.source_version)),
                        prev_content_version_fp: entry_mut.content_version_fp.take(),
                    };
                    if source_version.kind == row_indexer::SourceVersionKind::NonExistence {
                        entry.remove();
                    } else {
                        entry_mut.source_version = source_version;
                        entry_mut.content_version_fp = content_version_fp.cloned();
                    }
                    (sem, outcome)
                }
                hash_map::Entry::Vacant(entry) => {
                    if source_version.kind == row_indexer::SourceVersionKind::NonExistence {
                        self.update_stats.num_no_change.inc(1);
                        return Ok(RowStateAdvanceOutcome::Skipped);
                    }
                    let new_entry = SourceRowIndexingState {
                        source_version,
                        content_version_fp: content_version_fp.cloned(),
                        touched_generation,
                        ..Default::default()
                    };
                    let sem = new_entry.processing_sem.clone();
                    entry.insert(new_entry);
                    (
                        Some(sem),
                        RowStateAdvanceOutcome::Advanced {
                            prev_source_version: None,
                            prev_content_version_fp: None,
                        },
                    )
                }
            }
        };
        if let Some(sem) = sem {
            self.processing_sem_permit = Some(sem.clone().acquire_owned().await?);
            self.processing_sem = Some(sem);
        }
        Ok(outcome)
    }
}

pub struct ProcessSourceRowInput {
    pub key: value::KeyValue,
    /// `key_aux_info` is not available for deletions. It must be provided if `data.value` is `None`.
    pub key_aux_info: Option<serde_json::Value>,
    pub data: interface::PartialSourceRowData,
}

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
                let source_pk = value::KeyValue::from_json(
                    key_metadata.source_key,
                    &import_op.primary_key_schema,
                )?;
                rows.insert(
                    source_pk,
                    SourceRowIndexingState {
                        source_version: SourceVersion::from_stored(
                            key_metadata.processed_source_ordinal,
                            &key_metadata.process_logic_fingerprint,
                            plan.logic_fingerprint,
                        ),
                        content_version_fp: key_metadata.processed_source_fp,
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

    pub async fn process_source_row<
        AckFut: Future<Output = Result<()>> + Send + 'static,
        AckFn: FnOnce() -> AckFut,
    >(
        self: Arc<Self>,
        row_input: ProcessSourceRowInput,
        update_stats: Arc<stats::UpdateStats>,
        _concur_permit: concur_control::CombinedConcurrencyControllerPermit,
        ack_fn: Option<AckFn>,
        pool: PgPool,
    ) {
        let process = async {
            let plan = self.flow.get_execution_plan().await?;
            let import_op = &plan.import_ops[self.source_idx];
            let schema = &self.flow.data_schema;

            let eval_ctx = SourceRowEvaluationContext {
                plan: &plan,
                import_op,
                schema,
                key: &row_input.key,
                import_op_idx: self.source_idx,
            };
            let mut row_indexer = row_indexer::RowIndexer::new(
                &eval_ctx,
                &self.setup_execution_ctx,
                &pool,
                &update_stats,
            )?;

            let mut row_state_operator =
                LocalSourceRowStateOperator::new(&row_input.key, &self.state, &update_stats);

            let source_data = row_input.data;
            if let Some(ordinal) = source_data.ordinal
                && let Some(content_version_fp) = &source_data.content_version_fp
            {
                let version = SourceVersion::from_current_with_ordinal(ordinal);
                match row_state_operator
                    .advance(version, Some(content_version_fp))
                    .await?
                {
                    RowStateAdvanceOutcome::Skipped => {
                        return anyhow::Ok(());
                    }
                    RowStateAdvanceOutcome::Advanced {
                        prev_source_version: Some(prev_source_version),
                        prev_content_version_fp: Some(prev_content_version_fp),
                    } => {
                        // Fast path optimization: may collapse the row based on source version fingerprint.
                        // Still need to update the tracking table as the processed ordinal advanced.
                        if row_indexer
                            .try_collapse(
                                &version,
                                content_version_fp.as_slice(),
                                &prev_source_version,
                                ContentHashBasedCollapsingBaseline::ProcessedSourceFingerprint(
                                    &prev_content_version_fp,
                                ),
                            )
                            .await?
                            .is_some()
                        {
                            return Ok(());
                        }
                    }
                    _ => {}
                }
            }

            let (ordinal, content_version_fp, value) =
                match (source_data.ordinal, source_data.value) {
                    (Some(ordinal), Some(value)) => {
                        (ordinal, source_data.content_version_fp, value)
                    }
                    _ => {
                        let data = import_op
                        .executor
                        .get_value(
                            &row_input.key,
                            row_input.key_aux_info.as_ref().ok_or_else(|| {
                                anyhow::anyhow!(
                                    "`key_aux_info` must be provided when there's no `source_data`"
                                )
                            })?,
                            &interface::SourceExecutorReadOptions {
                                include_value: true,
                                include_ordinal: true,
                                include_content_version_fp: true,
                            },
                        )
                        .await?;
                        (
                            data.ordinal
                                .ok_or_else(|| anyhow::anyhow!("ordinal is not available"))?,
                            data.content_version_fp,
                            data.value
                                .ok_or_else(|| anyhow::anyhow!("value is not available"))?,
                        )
                    }
                };

            let source_version = SourceVersion::from_current_data(ordinal, &value);
            if let RowStateAdvanceOutcome::Skipped = row_state_operator
                .advance(source_version, content_version_fp.as_ref())
                .await?
            {
                return Ok(());
            }

            let result = row_indexer
                .update_source_row(&source_version, value, content_version_fp.clone())
                .await?;
            if let SkippedOr::Skipped(version, fp) = result {
                row_state_operator.advance(version, fp.as_ref()).await?;
            }
            Ok(())
        };
        let process_and_ack = async {
            process.await?;
            if let Some(ack_fn) = ack_fn {
                ack_fn().await?;
            }
            anyhow::Ok(())
        };
        if let Err(e) = process_and_ack.await {
            update_stats.num_errors.inc(1);
            error!(
                "{:?}",
                e.context(format!(
                    "Error in processing row from source `{source}` with key: {key}",
                    source = self.flow.flow_instance.import_ops[self.source_idx].name,
                    key = row_input.key,
                ))
            );
        }
    }

    pub async fn update(
        self: &Arc<Self>,
        pool: &PgPool,
        update_stats: &Arc<stats::UpdateStats>,
        expect_little_diff: bool,
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
                        slf.update_once(&pool, &update_stats, expect_little_diff)
                            .await?;
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
        expect_little_diff: bool,
    ) -> Result<()> {
        let plan = self.flow.get_execution_plan().await?;
        let import_op = &plan.import_ops[self.source_idx];
        let read_options = interface::SourceExecutorReadOptions {
            include_ordinal: true,
            include_content_version_fp: true,
            // When only a little diff is expected and the source provides ordinal, we don't fetch values during `list()` by default,
            // as there's a high chance that we don't need the values at all
            include_value: !(expect_little_diff && import_op.executor.provides_ordinal()),
        };
        let rows_stream = import_op.executor.list(&read_options).await?;
        self.update_with_stream(import_op, rows_stream, pool, update_stats)
            .await
    }

    async fn update_with_stream(
        self: &Arc<Self>,
        import_op: &plan::AnalyzedImportOp,
        mut rows_stream: BoxStream<'_, Result<Vec<interface::PartialSourceRow>>>,
        pool: &PgPool,
        update_stats: &Arc<stats::UpdateStats>,
    ) -> Result<()> {
        let mut join_set = JoinSet::new();
        let scan_generation = {
            let mut state = self.state.lock().unwrap();
            state.scan_generation += 1;
            state.scan_generation
        };
        while let Some(row) = rows_stream.next().await {
            for row in row? {
                let source_version = SourceVersion::from_current_with_ordinal(
                    row.data
                        .ordinal
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
                join_set.spawn(self.clone().process_source_row(
                    ProcessSourceRowInput {
                        key: row.key,
                        key_aux_info: Some(row.key_aux_info),
                        data: row.data,
                    },
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
            let concur_permit = import_op.concurrency_controller.acquire(Some(|| 0)).await?;
            join_set.spawn(self.clone().process_source_row(
                ProcessSourceRowInput {
                    key,
                    key_aux_info: None,
                    data: interface::PartialSourceRowData {
                        ordinal: Some(source_ordinal),
                        content_version_fp: None,
                        value: Some(interface::SourceValue::NonExistence),
                    },
                },
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
