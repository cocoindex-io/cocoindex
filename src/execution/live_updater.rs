use crate::{
    execution::{
        source_indexer::{ProcessSourceRowInput, SourceIndexingContext},
        stats::UpdateStats,
    },
    prelude::*,
};

use super::stats;
use futures::future::try_join_all;
// Progress bar functionality temporarily disabled due to compilation issues
use sqlx::PgPool;
use tokio::{sync::watch, task::JoinSet, time::MissedTickBehavior};
use serde::{Serialize, Deserialize};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use futures::future::BoxFuture;

pub struct FlowLiveUpdaterUpdates {
    pub active_sources: Vec<String>,
    pub updated_sources: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct FlowLiveUpdaterStatus {
    pub active_source_idx: BTreeSet<usize>,
    pub source_updates_num: Vec<usize>,
    // CLI feature fields
    pub source_interval_enabled: Vec<bool>,
    pub source_change_capture_enabled: Vec<bool>,
}

struct UpdateReceiveState {
    status_rx: watch::Receiver<FlowLiveUpdaterStatus>,
    last_num_source_updates: Vec<usize>,
    is_done: bool,
}

pub struct FlowLiveUpdater {
    flow_ctx: Arc<FlowContext>,
    join_set: Mutex<Option<JoinSet<Result<()>>>>,
    stats_per_task: Vec<Arc<stats::UpdateStats>>,
    pub operation_in_process_stats: Arc<stats::OperationInProcessStats>,
    recv_state: tokio::sync::Mutex<UpdateReceiveState>,
    num_remaining_tasks_rx: watch::Receiver<usize>,

    _status_tx: watch::Sender<FlowLiveUpdaterStatus>,
    _num_remaining_tasks_tx: watch::Sender<usize>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FlowLiveUpdaterOptions {
    pub live_mode: bool,
    pub reexport_targets: bool,
    pub print_stats: bool,
}

const REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

struct SharedAckFn<AckAsyncFn: AsyncFnOnce() -> Result<()>> {
    count: usize,
    ack_fn: Option<AckAsyncFn>,
}

impl<AckAsyncFn: AsyncFnOnce() -> Result<()>> SharedAckFn<AckAsyncFn> {
    fn new(count: usize, ack_fn: AckAsyncFn) -> Self {
        Self {
            count,
            ack_fn: Some(ack_fn),
        }
    }

    async fn ack(v: &Mutex<Self>) -> Result<()> {
        let ack_fn = {
            let mut v = v.lock().unwrap();
            v.count -= 1;
            if v.count > 0 { None } else { v.ack_fn.take() }
        };
        if let Some(ack_fn) = ack_fn {
            ack_fn().await?;
        }
        Ok(())
    }
}

struct SourceUpdateTask {
    source_idx: usize,
    flow: Arc<builder::AnalyzedFlow>,
    plan: Arc<plan::ExecutionPlan>,
    execution_ctx: Arc<tokio::sync::OwnedRwLockReadGuard<crate::lib_context::FlowExecutionContext>>,
    source_update_stats: Arc<stats::UpdateStats>,
    operation_in_process_stats: Arc<stats::OperationInProcessStats>,
    pool: PgPool,
    options: FlowLiveUpdaterOptions,

    status_tx: watch::Sender<FlowLiveUpdaterStatus>,
    num_remaining_tasks_tx: watch::Sender<usize>,
}

impl Drop for SourceUpdateTask {
    fn drop(&mut self) {
        self.status_tx.send_modify(|update| {
            update.active_source_idx.remove(&self.source_idx);
        });
        self.num_remaining_tasks_tx.send_modify(|update| {
            *update -= 1;
        });
    }
}

impl SourceUpdateTask {
    async fn run(self) -> Result<()> {
        let source_indexing_context = self
            .execution_ctx
            .get_source_indexing_context(&self.flow, self.source_idx, &self.pool)
            .await?;
        let initial_update_options = super::source_indexer::UpdateOptions {
            expect_little_diff: false,
            mode: if self.options.reexport_targets {
                super::source_indexer::UpdateMode::ReexportTargets
            } else {
                super::source_indexer::UpdateMode::Normal
            },
        };

        if !self.options.live_mode {
            return self.update_one_pass(
                source_indexing_context,
                "batch update",
                initial_update_options,
            ).await;
        }

        let mut futs: Vec<BoxFuture<'_, Result<()>>> = Vec::new();
        let source_idx = self.source_idx;
        let import_op = self.import_op();
        let task = &self;

        // Change streams
        if let Some(change_stream) = import_op.executor.change_stream().await? {
            let change_stream_stats = Arc::new(stats::UpdateStats::default());
            futs.push({
                let change_stream_stats = change_stream_stats.clone();
                let pool = self.pool.clone();
                let status_tx = self.status_tx.clone();
                let operation_in_process_stats = self.operation_in_process_stats.clone();
                async move {
                    let mut change_stream = change_stream;
                    let retry_options = retryable::RetryOptions {
                        retry_timeout: None,
                        initial_backoff: std::time::Duration::from_secs(5),
                        max_backoff: std::time::Duration::from_secs(60),
                    };
                    loop {
                        let change_stream = tokio::sync::Mutex::new(&mut change_stream);
                        let change_msg = retryable::run(
                            || async {
                                let mut change_stream = change_stream.lock().await;
                                change_stream.next()
                                    .await
                                    .transpose()
                                    .map_err(retryable::Error::always_retryable)
                            },
                            &retry_options,
                        )
                        .await
                        .map_err(Into::<anyhow::Error>::into)?;
                        let change_msg = match change_msg {
                            Some(change_msg) => change_msg,
                            None => break,
                        };

                        let update_stats = Arc::new(stats::UpdateStats::default());
                        let ack_fn = {
                            let status_tx = status_tx.clone();
                            let update_stats = update_stats.clone();
                            let change_stream_stats = change_stream_stats.clone();
                            async move || {
                                if update_stats.has_any_change() {
                                    status_tx.send_modify(|update| {
                                        update.source_updates_num[source_idx] += 1;
                                    });
                                    change_stream_stats.merge(&update_stats);
                                }
                                if let Some(ack_fn) = change_msg.ack_fn {
                                    ack_fn().await
                                } else { Ok(()) }
                            }
                        };
                        let shared_ack_fn = Arc::new(Mutex::new(SharedAckFn::new(
                            change_msg.changes.iter().len(),
                            ack_fn,
                        )));
                        for change in change_msg.changes {
                            let shared_ack_fn = shared_ack_fn.clone();
                            let concur_permit = import_op
                                .concurrency_controller
                                .acquire(concur_control::BYTES_UNKNOWN_YET)
                                .await?;
                            tokio::spawn(
                                source_indexing_context.clone().process_source_row(
                                    ProcessSourceRowInput {
                                        key: change.key,
                                        key_aux_info: Some(change.key_aux_info),
                                        data: change.data,
                                    },
                                    super::source_indexer::UpdateMode::Normal,
                                    update_stats.clone(),
                                    Some(operation_in_process_stats.clone()),
                                    concur_permit,
                                    Some(move || async move {
                                        SharedAckFn::ack(&shared_ack_fn).await
                                    }),
                                    pool.clone(),
                                ),
                            );
                        }
                    }
                    Ok(())
                }.boxed()
            });

            // Report stats periodically
            futs.push({
                async move {
                    let mut interval = tokio::time::interval(REPORT_INTERVAL);
                    let mut last_change_stream_stats: UpdateStats =
                        change_stream_stats.as_ref().clone();
                    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    interval.tick().await;
                    loop {
                        interval.tick().await;
                        let curr_change_stream_stats = change_stream_stats.as_ref().clone();
                        let delta = curr_change_stream_stats.delta(&last_change_stream_stats);
                        if delta.has_any_change() {
                            task.report_stats(&delta, "change stream");
                            last_change_stream_stats = curr_change_stream_stats;
                        }
                    }
                }.boxed()
            });
        }

        // Main update loop
        futs.push({
            async move {
                let refresh_interval = import_op.refresh_options.refresh_interval;
                task.update_with_pass_with_error_logging(
                    source_indexing_context,
                    if refresh_interval.is_some() { "initial interval update" } else { "batch update" },
                    initial_update_options,
                ).await;

                if let Some(refresh_interval) = refresh_interval {
                    let mut interval = tokio::time::interval(refresh_interval);
                    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    interval.tick().await;
                    loop {
                        interval.tick().await;
                        task.update_with_pass_with_error_logging(
                            source_indexing_context,
                            "interval update",
                            super::source_indexer::UpdateOptions {
                                expect_little_diff: true,
                                mode: super::source_indexer::UpdateMode::Normal,
                            },
                        ).await;
                    }
                }
                Ok(())
            }.boxed()
        });

        try_join_all(futs).await?;
        Ok(())
    }

    fn report_stats(&self, stats: &stats::UpdateStats, update_title: &str) {
        self.source_update_stats.merge(stats);
        if self.options.print_stats {
            println!("{}.{} ({update_title}): {}", self.flow.flow_instance.name, self.import_op().name, stats);
        } else {
            trace!("{}.{} ({update_title}): {}", self.flow.flow_instance.name, self.import_op().name, stats);
        }
    }

    async fn update_one_pass(
        &self,
        source_indexing_context: &Arc<SourceIndexingContext>,
        update_title: &str,
        update_options: super::source_indexer::UpdateOptions,
    ) -> Result<()> {
        let update_stats = Arc::new(stats::UpdateStats::default());

        // Spawn periodic stats reporting task if print_stats is enabled
        let reporting_handle = if self.options.print_stats {
            let update_stats_clone = update_stats.clone();
            let update_title_owned = update_title.to_string();
            let flow_name = self.flow.flow_instance.name.clone();
            let import_op_name = self.import_op().name.clone();

            let report_task = async move {
                let mut interval = tokio::time::interval(REPORT_INTERVAL);
                interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                interval.tick().await; // Skip first tick

                loop {
                    interval.tick().await;
                    let current_stats = update_stats_clone.as_ref().clone();
                    if current_stats.has_any_change() {
                        // Show cumulative stats (always show latest total, not delta)
                        println!(
                            "{}.{} ({update_title_owned}): {}",
                            flow_name, import_op_name, current_stats
                        );
                    }
                }
            };
            Some(tokio::spawn(report_task))
        } else {
            None
        };

        // Run the actual update
        let update_result = source_indexing_context
            .update(&self.pool, &update_stats, update_options)
            .await
            .with_context(|| {
                format!(
                    "Error in processing flow `{}` source `{}` ({update_title})",
                    self.flow.flow_instance.name,
                    self.import_op().name
                )
            });

        // Cancel the reporting task if it was spawned
        if let Some(handle) = reporting_handle {
            handle.abort();
        }

        // Check update result
        update_result?;
        if update_stats.has_any_change() {
            self.status_tx.send_modify(|update| {
                update.source_updates_num[self.source_idx] += 1;
            });
        }

        // Report final stats
        self.report_stats(&update_stats, update_title);
        Ok(())
    }

    async fn update_with_pass_with_error_logging(
        &self,
        source_indexing_context: &Arc<SourceIndexingContext>,
        update_title: &str,
        update_options: super::source_indexer::UpdateOptions,
    ) {
        if let Err(err) = self.update_one_pass(source_indexing_context, update_title, update_options).await {
            error!("{:?}", err);
        }
    }

    fn import_op(&self) -> &plan::AnalyzedImportOp {
        &self.plan.import_ops[self.source_idx]
    }
}

impl FlowLiveUpdater {
    pub async fn start(
        flow_ctx: Arc<FlowContext>,
        pool: &PgPool,
        options: FlowLiveUpdaterOptions,
    ) -> Result<Self> {
        let plan = flow_ctx.flow.get_execution_plan().await?;
        let execution_ctx = Arc::new(flow_ctx.use_owned_execution_ctx().await?);

        let num_sources = plan.import_ops.len();
        // Check change streams for each source
        let mut source_change_capture_enabled = Vec::new();
        for op in &plan.import_ops {
            let has_change_stream = op.executor.change_stream().await?.is_some();
            source_change_capture_enabled.push(has_change_stream);
        }

        let (status_tx, status_rx) = watch::channel(FlowLiveUpdaterStatus {
            active_source_idx: BTreeSet::from_iter(0..num_sources),
            source_updates_num: vec![0; num_sources],
            source_interval_enabled: plan.import_ops.iter().map(|op| op.refresh_options.refresh_interval.is_some()).collect(),
            source_change_capture_enabled,
        });

        let (num_remaining_tasks_tx, num_remaining_tasks_rx) = watch::channel(num_sources);

        let mut join_set = JoinSet::new();
        let mut stats_per_task = Vec::new();
        let operation_in_process_stats = Arc::new(stats::OperationInProcessStats::default());

        for source_idx in 0..num_sources {
            let source_update_stats = Arc::new(stats::UpdateStats::default());
            let source_update_task = SourceUpdateTask {
                source_idx,
                flow: flow_ctx.flow.clone(),
                plan: plan.clone(),
                execution_ctx: execution_ctx.clone(),
                source_update_stats: source_update_stats.clone(),
                operation_in_process_stats: operation_in_process_stats.clone(),
                pool: pool.clone(),
                options: options.clone(),
                status_tx: status_tx.clone(),
                num_remaining_tasks_tx: num_remaining_tasks_tx.clone(),
            };
            join_set.spawn(source_update_task.run());
            stats_per_task.push(source_update_stats);
        }

        Ok(Self {
            flow_ctx,
            join_set: Mutex::new(Some(join_set)),
            stats_per_task,
            operation_in_process_stats,
            recv_state: tokio::sync::Mutex::new(UpdateReceiveState {
                status_rx,
                last_num_source_updates: vec![0; num_sources],
                is_done: false,
            }),
            num_remaining_tasks_rx,
            _status_tx: status_tx,
            _num_remaining_tasks_tx: num_remaining_tasks_tx,
        })
    }

    pub async fn next_status_updates(&self) -> Result<FlowLiveUpdaterUpdates> {
        let mut recv_state = self.recv_state.lock().await;
        let recv_state = &mut *recv_state;

        if recv_state.is_done {
            return Ok(FlowLiveUpdaterUpdates {
                active_sources: vec![],
                updated_sources: vec![],
            });
        }

        recv_state.status_rx.changed().await?;
        let status = recv_state.status_rx.borrow_and_update();
        let updates = FlowLiveUpdaterUpdates {
            active_sources: status.active_source_idx.iter().map(|idx| self.flow_ctx.flow.flow_instance.import_ops[*idx].name.clone()).collect(),
            updated_sources: status.source_updates_num.iter().enumerate().filter_map(|(idx, num_updates)| {
                if num_updates > &recv_state.last_num_source_updates[idx] {
                    Some(self.flow_ctx.flow.flow_instance.import_ops[idx].name.clone())
                } else { None }
            }).collect(),
        };
        recv_state.last_num_source_updates = status.source_updates_num.clone();
        if status.active_source_idx.is_empty() {
            recv_state.is_done = true;
        }
        Ok(updates)
    }

    // --- CLI printing ---
    pub fn print_cli_status(&self, updates: &FlowLiveUpdaterUpdates) {
        let recv_state = self.recv_state.blocking_lock();
        let status = recv_state.status_rx.borrow();
        for (idx, import_op) in self.flow_ctx.flow.flow_instance.import_ops.iter().enumerate() {
            println!(
                "{} | interval={} | change_capture={}",
                import_op.name,
                status.source_interval_enabled[idx],
                status.source_change_capture_enabled[idx]
            );
        }
        println!("Updated sources: {:?}", updates.updated_sources);
    }

    pub async fn next_status_updates_cli(&self) -> Result<()> {
        let updates = self.next_status_updates().await?;
        self.print_cli_status(&updates);
        Ok(())
    }

    pub async fn wait(&self) -> Result<()> {
        self.num_remaining_tasks_rx.clone().wait_for(|v| *v == 0).await?;

        let Some(mut join_set) = self.join_set.lock().unwrap().take() else { return Ok(()); };
        while let Some(task_result) = join_set.join_next().await {
            match task_result {
                Ok(Ok(_)) => {}
                Ok(Err(err)) => return Err(err),
                Err(err) if err.is_cancelled() => {}
                Err(err) => return Err(err.into()),
            }
        }
        Ok(())
    }

    pub fn abort(&self) {
        if let Some(join_set) = &mut *self.join_set.lock().unwrap() {
            join_set.abort_all();
        }
    }

    pub fn index_update_info(&self) -> stats::IndexUpdateInfo {
        // Return an empty IndexUpdateInfo for now
        // This method is used by the Python bindings
        stats::IndexUpdateInfo {
            sources: Vec::new(),
        }
    }
}
