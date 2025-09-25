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

#[derive(Default)]
struct SourceRowVersionState {
    source_version: SourceVersion,
    content_version_fp: Option<Vec<u8>>,
}
struct SourceRowIndexingState {
    version_state: SourceRowVersionState,
    processing_sem: Arc<Semaphore>,
    touched_generation: usize,
}

impl Default for SourceRowIndexingState {
    fn default() -> Self {
        Self {
            version_state: SourceRowVersionState {
                source_version: SourceVersion::default(),
                content_version_fp: None,
            },
            processing_sem: Arc::new(Semaphore::new(1)),
            touched_generation: 0,
        }
    }
}

struct SourceIndexingState {
    rows: HashMap<value::KeyValue, SourceRowIndexingState>,
    scan_generation: usize,

    // Set of rows to retry.
    // It's for sources that we don't proactively scan all input rows during refresh.
    // We need to maintain a list of row keys failed in last processing, to retry them later.
    // It's `None` if we don't need this mechanism for failure retry.
    rows_to_retry: Option<HashSet<value::KeyValue>>,
}

pub struct SourceIndexingContext {
    flow: Arc<builder::AnalyzedFlow>,
    source_idx: usize,
    pending_update: Mutex<Option<Shared<BoxFuture<'static, SharedResult<()>>>>>,
    update_sem: Semaphore,
    state: Mutex<SourceIndexingState>,
    setup_execution_ctx: Arc<exec_ctx::FlowSetupExecutionContext>,
    needs_to_track_rows_to_retry: bool,
}

pub const NO_ACK: Option<fn() -> Ready<Result<()>>> = None;

struct LocalSourceRowStateOperator<'a> {
    key: &'a value::KeyValue,
    indexing_state: &'a Mutex<SourceIndexingState>,
    update_stats: &'a Arc<stats::UpdateStats>,

    processing_sem: Option<Arc<Semaphore>>,
    processing_sem_permit: Option<OwnedSemaphorePermit>,
    last_source_version: Option<SourceVersion>,

    // `None` means no advance yet.
    // `Some(None)` means the state before advance is `None`.
    // `Some(Some(version_state))` means the state before advance is `Some(version_state)`.
    prev_version_state: Option<Option<SourceRowVersionState>>,

    to_remove_entry_on_success: bool,
}

enum RowStateAdvanceOutcome {
    Skipped,
    Advanced {
        prev_version_state: Option<SourceRowVersionState>,
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
            prev_version_state: None,
            to_remove_entry_on_success: false,
        }
    }
    async fn advance(
        &mut self,
        source_version: SourceVersion,
        content_version_fp: Option<&Vec<u8>>,
        force_reload: bool,
    ) -> Result<RowStateAdvanceOutcome> {
        let (sem, outcome) = {
            let mut state = self.indexing_state.lock().unwrap();
            let touched_generation = state.scan_generation;

            if let Some(rows_to_retry) = &mut state.rows_to_retry {
                rows_to_retry.remove(self.key);
            }

            if self.last_source_version == Some(source_version) {
                return Ok(RowStateAdvanceOutcome::Noop);
            }
            self.last_source_version = Some(source_version);

            match state.rows.entry(self.key.clone()) {
                hash_map::Entry::Occupied(mut entry) => {
                    if !force_reload
                        && entry
                            .get()
                            .version_state
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
                        prev_version_state: Some(std::mem::take(&mut entry_mut.version_state)),
                    };
                    if source_version.kind == row_indexer::SourceVersionKind::NonExistence {
                        self.to_remove_entry_on_success = true;
                    }
                    let prev_version_state = std::mem::replace(
                        &mut entry_mut.version_state,
                        SourceRowVersionState {
                            source_version,
                            content_version_fp: content_version_fp.cloned(),
                        },
                    );
                    if self.prev_version_state.is_none() {
                        self.prev_version_state = Some(Some(prev_version_state));
                    }
                    (sem, outcome)
                }
                hash_map::Entry::Vacant(entry) => {
                    if source_version.kind == row_indexer::SourceVersionKind::NonExistence {
                        self.update_stats.num_no_change.inc(1);
                        return Ok(RowStateAdvanceOutcome::Skipped);
                    }
                    let new_entry = SourceRowIndexingState {
                        version_state: SourceRowVersionState {
                            source_version,
                            content_version_fp: content_version_fp.cloned(),
                        },
                        touched_generation,
                        ..Default::default()
                    };
                    let sem = new_entry.processing_sem.clone();
                    entry.insert(new_entry);
                    if self.prev_version_state.is_none() {
                        self.prev_version_state = Some(None);
                    }
                    (
                        Some(sem),
                        RowStateAdvanceOutcome::Advanced {
                            prev_version_state: None,
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

    fn commit(self) {
        if self.to_remove_entry_on_success {
            self.indexing_state.lock().unwrap().rows.remove(self.key);
        }
    }

    fn rollback(self) {
        let Some(prev_version_state) = self.prev_version_state else {
            return;
        };
        let mut indexing_state = self.indexing_state.lock().unwrap();
        if let Some(prev_version_state) = prev_version_state {
            if let Some(entry) = indexing_state.rows.get_mut(self.key) {
                entry.version_state = prev_version_state;
            }
        } else {
            indexing_state.rows.remove(self.key);
        }
        if let Some(rows_to_retry) = &mut indexing_state.rows_to_retry {
            rows_to_retry.insert(self.key.clone());
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UpdateMode {
    #[default]
    Normal,
    ReexportTargets,
}

pub struct UpdateOptions {
    pub expect_little_diff: bool,
    pub mode: UpdateMode,
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
        let mut rows_to_retry: Option<HashSet<value::KeyValue>> = None;
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
                if let Some(rows_to_retry) = &mut rows_to_retry {
                    if key_metadata.max_process_ordinal > key_metadata.process_ordinal {
                        rows_to_retry.insert(source_pk.clone());
                    }
                }
                rows.insert(
                    source_pk,
                    SourceRowIndexingState {
                        version_state: SourceRowVersionState {
                            source_version: SourceVersion::from_stored(
                                key_metadata.processed_source_ordinal,
                                &key_metadata.process_logic_fingerprint,
                                plan.logic_fingerprint,
                            ),
                            content_version_fp: key_metadata.processed_source_fp,
                        },
                        processing_sem: Arc::new(Semaphore::new(1)),
                        touched_generation: scan_generation,
                    },
                );
            }
        }
        Ok(Self {
            flow,
            source_idx,
            needs_to_track_rows_to_retry: rows_to_retry.is_some(),
            state: Mutex::new(SourceIndexingState {
                rows,
                scan_generation,
                rows_to_retry,
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
        mode: UpdateMode,
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
            let process_time = chrono::Utc::now();
            let row_indexer = row_indexer::RowIndexer::new(
                &eval_ctx,
                &self.setup_execution_ctx,
                mode,
                process_time,
                &update_stats,
                &pool,
            )?;

            let source_data = row_input.data;
            let mut row_state_operator =
                LocalSourceRowStateOperator::new(&row_input.key, &self.state, &update_stats);
            let mut ordinal_touched = false;
            let result = {
                let row_state_operator = &mut row_state_operator;
                let row_key = &row_input.key;
                async move {
                    if let Some(ordinal) = source_data.ordinal
                        && let Some(content_version_fp) = &source_data.content_version_fp
                    {
                        let version = SourceVersion::from_current_with_ordinal(ordinal);
                        match row_state_operator
                            .advance(
                                version,
                                Some(content_version_fp),
                                /*force_reload=*/ mode == UpdateMode::ReexportTargets,
                            )
                            .await?
                        {
                            RowStateAdvanceOutcome::Skipped => {
                                return anyhow::Ok(());
                            }
                            RowStateAdvanceOutcome::Advanced {
                                prev_version_state: Some(prev_version_state),
                            } => {
                                // Fast path optimization: may collapse the row based on source version fingerprint.
                                // Still need to update the tracking table as the processed ordinal advanced.
                                if let Some(prev_content_version_fp) =
                            &prev_version_state.content_version_fp
                            && mode == UpdateMode::Normal
                            && row_indexer
                                .try_collapse(
                                    &version,
                                    content_version_fp.as_slice(),
                                    &prev_version_state.source_version,
                                    ContentHashBasedCollapsingBaseline::ProcessedSourceFingerprint(
                                        prev_content_version_fp,
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
                            row_key,
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
                                    data.ordinal.ok_or_else(|| {
                                        anyhow::anyhow!("ordinal is not available")
                                    })?,
                                    data.content_version_fp,
                                    data.value
                                        .ok_or_else(|| anyhow::anyhow!("value is not available"))?,
                                )
                            }
                        };

                    let source_version = SourceVersion::from_current_data(ordinal, &value);
                    if let RowStateAdvanceOutcome::Skipped = row_state_operator
                        .advance(
                            source_version,
                            content_version_fp.as_ref(),
                            /*force_reload=*/ mode == UpdateMode::ReexportTargets,
                        )
                        .await?
                    {
                        return Ok(());
                    }

                    let result = row_indexer
                        .update_source_row(
                            &source_version,
                            value,
                            content_version_fp.clone(),
                            &mut ordinal_touched,
                        )
                        .await?;
                    if let SkippedOr::Skipped(version, fp) = result {
                        row_state_operator
                            .advance(version, fp.as_ref(), /*force_reload=*/ false)
                            .await?;
                    }
                    Ok(())
                }
            }
            .await;
            if result.is_ok() {
                row_state_operator.commit();
            } else {
                row_state_operator.rollback();
                if !ordinal_touched && self.needs_to_track_rows_to_retry {
                    let source_key_json = serde_json::to_value(&row_input.key)?;
                    db_tracking::touch_max_process_ordinal(
                        self.setup_execution_ctx.import_ops[self.source_idx].source_id,
                        &source_key_json,
                        row_indexer::RowIndexer::process_ordinal_from_time(process_time),
                        &self.setup_execution_ctx.setup_state.tracking_table,
                        &pool,
                    )
                    .await?;
                }
            }
            result
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
                    "Error in processing row from flow `{flow}` source `{source}` with key: {key}",
                    flow = self.flow.flow_instance.name,
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
        update_options: UpdateOptions,
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
                        slf.update_once(&pool, &update_stats, &update_options)
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
        pending_update_fut.await.anyhow_result()?;
        Ok(())
    }

    async fn update_once(
        self: &Arc<Self>,
        pool: &PgPool,
        update_stats: &Arc<stats::UpdateStats>,
        update_options: &UpdateOptions,
    ) -> Result<()> {
        let plan = self.flow.get_execution_plan().await?;
        let import_op = &plan.import_ops[self.source_idx];
        let read_options = interface::SourceExecutorReadOptions {
            include_ordinal: true,
            include_content_version_fp: true,
            // When only a little diff is expected and the source provides ordinal, we don't fetch values during `list()` by default,
            // as there's a high chance that we don't need the values at all
            include_value: !(update_options.expect_little_diff
                && import_op.executor.provides_ordinal()),
        };
        let rows_stream = import_op.executor.list(&read_options).await?;
        self.update_with_stream(import_op, rows_stream, pool, update_stats, update_options)
            .await
    }

    async fn update_with_stream(
        self: &Arc<Self>,
        import_op: &plan::AnalyzedImportOp,
        mut rows_stream: BoxStream<'_, Result<Vec<interface::PartialSourceRow>>>,
        pool: &PgPool,
        update_stats: &Arc<stats::UpdateStats>,
        update_options: &UpdateOptions,
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
                    if update_options.mode == UpdateMode::Normal
                        && row_state
                            .version_state
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
                    update_options.mode,
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
                    deleted_key_versions
                        .push((key.clone(), row_state.version_state.source_version.ordinal));
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
                update_options.mode,
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
