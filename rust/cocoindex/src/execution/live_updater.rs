use crate::{
    execution::source_indexer::{ProcessSourceRowInput, SourceIndexingContext},
    prelude::*,
};

use super::stats;
use futures::future::try_join_all;
use indicatif::{MultiProgress, ProgressBar, ProgressFinish};
use sqlx::PgPool;
use std::fmt::Write;
use tokio::{sync::watch, task::JoinSet, time::MissedTickBehavior};
use tracing::Level;

pub struct FlowLiveUpdaterUpdates {
    pub active_sources: Vec<String>,
    pub updated_sources: Vec<String>,
}
struct FlowLiveUpdaterStatus {
    pub active_source_idx: BTreeSet<usize>,
    pub source_updates_num: Vec<usize>,
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
    /// Global tracking of in-process rows per operation
    pub operation_in_process_stats: Arc<stats::OperationInProcessStats>,
    recv_state: tokio::sync::Mutex<UpdateReceiveState>,
    num_remaining_tasks_rx: watch::Receiver<usize>,

    // Hold tx to avoid dropping the sender.
    _status_tx: watch::Sender<FlowLiveUpdaterStatus>,
    _num_remaining_tasks_tx: watch::Sender<usize>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FlowLiveUpdaterOptions {
    /// If true, the updater will keep refreshing the index.
    /// Otherwise, it will only apply changes from the source up to the current time.
    pub live_mode: bool,

    /// If true, the updater will reexport the targets even if there's no change.
    pub reexport_targets: bool,

    /// If true, the updater will reprocess everything and invalidate existing caches.
    pub full_reprocess: bool,

    /// If true, stats will be printed to the console.
    pub print_stats: bool,
}

const PROGRESS_BAR_REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
const TRACE_REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

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
    multi_progress_bar: MultiProgress,
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
    fn maybe_new_progress_bar(&self) -> Result<Option<ProgressBar>> {
        if !self.options.print_stats || self.multi_progress_bar.is_hidden() {
            return Ok(None);
        }
        let style =
            indicatif::ProgressStyle::default_spinner().template("{spinner}{spinner} {msg}")?;
        let pb = ProgressBar::new_spinner().with_finish(ProgressFinish::AndClear);
        pb.set_style(style);
        Ok(Some(pb))
    }

    #[instrument(name = "source_update_task.run", skip_all, fields(flow_name = %self.flow.flow_instance.name, source_name = %self.import_op().name))]
    async fn run(self) -> Result<()> {
        let source_indexing_context = self
            .execution_ctx
            .get_source_indexing_context(&self.flow, self.source_idx, &self.pool)
            .await?;
        let initial_update_options = super::source_indexer::UpdateOptions {
            expect_little_diff: false,
            mode: if self.options.full_reprocess {
                super::source_indexer::UpdateMode::FullReprocess
            } else if self.options.reexport_targets {
                super::source_indexer::UpdateMode::ReexportTargets
            } else {
                super::source_indexer::UpdateMode::Normal
            },
        };

        let interval_progress_bar = self
            .maybe_new_progress_bar()?
            .map(|pb| self.multi_progress_bar.add(pb));
        if !self.options.live_mode {
            return self
                .update_one_pass(
                    source_indexing_context,
                    "batch update",
                    initial_update_options,
                    interval_progress_bar.as_ref(),
                )
                .await;
        }

        let mut futs: Vec<BoxFuture<'_, Result<()>>> = Vec::new();
        let source_idx = self.source_idx;
        let import_op = self.import_op();
        let task = &self;

        // Deal with change streams.
        if let Some(change_stream) = import_op.executor.change_stream().await? {
            let stats = Arc::new(stats::UpdateStats::default());
            let stats_to_report = stats.clone();

            let status_tx = self.status_tx.clone();
            let operation_in_process_stats = self.operation_in_process_stats.clone();
            let progress_bar = self
                .maybe_new_progress_bar()?
                .zip(interval_progress_bar.as_ref())
                .map(|(pb, interval_progress_bar)| {
                    self.multi_progress_bar
                        .insert_after(interval_progress_bar, pb)
                });
            let process_change_stream = async move {
                let mut change_stream = change_stream;
                let retry_options = retryable::RetryOptions {
                    retry_timeout: None,
                    initial_backoff: std::time::Duration::from_secs(5),
                    max_backoff: std::time::Duration::from_secs(60),
                };
                loop {
                    // Workaround as AsyncFnMut isn't mature yet.
                    // Should be changed to use AsyncFnMut once it is.
                    let change_stream = tokio::sync::Mutex::new(&mut change_stream);
                    let change_msg = retryable::run(
                        || async {
                            let mut change_stream = change_stream.lock().await;
                            change_stream
                                .next()
                                .await
                                .transpose()
                                .map_err(retryable::Error::retryable)
                        },
                        &retry_options,
                    )
                    .await
                    .map_err(Error::from)
                    .with_context(|| {
                        format!(
                            "Error in getting change message for flow `{}` source `{}`",
                            task.flow.flow_instance.name, import_op.name
                        )
                    });
                    let change_msg = match change_msg {
                        Ok(Some(change_msg)) => change_msg,
                        Ok(None) => break,
                        Err(err) => {
                            error!("{:?}", err);
                            continue;
                        }
                    };

                    let update_stats = Arc::new(stats::UpdateStats::default());
                    let ack_fn = {
                        let status_tx = status_tx.clone();
                        let update_stats = update_stats.clone();
                        let change_stream_stats = stats.clone();
                        async move || {
                            if update_stats.has_any_change() {
                                status_tx.send_modify(|update| {
                                    update.source_updates_num[source_idx] += 1;
                                });
                                change_stream_stats.merge(&update_stats);
                            }
                            if let Some(ack_fn) = change_msg.ack_fn {
                                ack_fn().await
                            } else {
                                Ok(())
                            }
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
                        tokio::spawn(source_indexing_context.clone().process_source_row(
                            ProcessSourceRowInput {
                                key: change.key,
                                key_aux_info: Some(change.key_aux_info),
                                data: change.data,
                            },
                            super::source_indexer::UpdateMode::Normal,
                            update_stats.clone(),
                            Some(operation_in_process_stats.clone()),
                            concur_permit,
                            Some(move || async move { SharedAckFn::ack(&shared_ack_fn).await }),
                        ));
                    }
                }
                Ok(())
            };

            let slf = &self;
            futs.push(
                async move {
                    slf.run_with_progress_report(
                        process_change_stream,
                        &stats_to_report,
                        "change stream",
                        None,
                        progress_bar.as_ref(),
                    )
                    .await
                }
                .boxed(),
            );
        }

        // The main update loop.
        futs.push({
            async move {
                let refresh_interval = import_op.refresh_options.refresh_interval;

                task.update_one_pass_with_error_logging(
                    source_indexing_context,
                    if refresh_interval.is_some() {
                        "initial interval update"
                    } else {
                        "batch update"
                    },
                    initial_update_options,
                    interval_progress_bar.as_ref(),
                )
                .await;

                let Some(refresh_interval) = refresh_interval else {
                    return Ok(());
                };

                let mut interval = tokio::time::interval(refresh_interval);
                interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

                // tokio::time::interval ticks immediately once; consume it so the first loop waits.
                interval.tick().await;

                loop {
                    if let Some(progress_bar) = interval_progress_bar.as_ref() {
                        progress_bar.set_message(format!(
                            "{}.{}: Waiting for next interval update...",
                            task.flow.flow_instance.name,
                            task.import_op().name
                        ));
                        progress_bar.tick();
                    }

                    // Wait for the next scheduled update tick
                    interval.tick().await;

                    let mut update_fut = Box::pin(task.update_one_pass_with_error_logging(
                        source_indexing_context,
                        "interval update",
                        super::source_indexer::UpdateOptions {
                            expect_little_diff: true,
                            mode: super::source_indexer::UpdateMode::Normal,
                        },
                        interval_progress_bar.as_ref(),
                    ));

                    tokio::select! {
                        biased;

                        _ = update_fut.as_mut() => {
                            // finished within refresh_interval, no warning
                        }

                        _ = tokio::time::sleep(refresh_interval) => {
                            // overrun: warn once for this pass, then wait for the pass to finish
                            warn!(
                                flow_name = %task.flow.flow_instance.name,
                                source_name = %task.import_op().name,
                                update_title = "interval update",
                                refresh_interval_secs = refresh_interval.as_secs_f64(),
                                "Live update pass exceeded refresh_interval; interval updates will lag behind"
                            );
                            update_fut.as_mut().await;
                        }
                    }
                }
    }
    .boxed()
        });

        try_join_all(futs).await?;
        Ok(())
    }

    fn stats_message(
        &self,
        stats: &stats::UpdateStats,
        update_title: &str,
        start_time: Option<std::time::Instant>,
    ) -> String {
        self.source_update_stats.merge(stats);
        let mut message = format!(
            "{}.{} ({update_title}):{stats}",
            self.flow.flow_instance.name,
            self.import_op().name
        );
        if let Some(start_time) = start_time {
            write!(
                &mut message,
                " [elapsed: {:.3}s]",
                start_time.elapsed().as_secs_f64()
            )
            .expect("Failed to write to message");
        }
        message
    }

    fn report_stats(
        &self,
        stats: &stats::UpdateStats,
        update_title: &str,
        start_time: Option<std::time::Instant>,
        prefix: &str,
    ) {
        if start_time.is_none() && !stats.has_any_change() {
            return;
        }
        if self.options.print_stats {
            println!(
                "{prefix}{message}",
                message = self.stats_message(stats, update_title, start_time)
            );
        } else {
            trace!(
                "{prefix}{message}",
                message = self.stats_message(stats, update_title, start_time)
            );
        }
    }

    fn stats_report_enabled(&self) -> bool {
        self.options.print_stats || tracing::event_enabled!(Level::TRACE)
    }

    async fn run_with_progress_report(
        &self,
        fut: impl Future<Output = Result<()>>,
        stats: &stats::UpdateStats,
        update_title: &str,
        start_time: Option<std::time::Instant>,
        progress_bar: Option<&ProgressBar>,
    ) -> Result<()> {
        let interval = if progress_bar.is_some() {
            PROGRESS_BAR_REPORT_INTERVAL
        } else if self.stats_report_enabled() {
            TRACE_REPORT_INTERVAL
        } else {
            return fut.await;
        };
        let mut pinned_fut = Box::pin(fut);
        let mut interval = tokio::time::interval(interval);

        // Use this to skip the first tick if there's no progress bar.
        let mut report_ready = false;
        loop {
            tokio::select! {
                res = &mut pinned_fut => {
                    return res;
                }
                _ = interval.tick() => {
                    if let Some(progress_bar) = progress_bar {
                        progress_bar.set_message(
                            self.stats_message(stats, update_title, start_time));
                        progress_bar.tick();
                    } else if report_ready {
                        self.report_stats(stats, update_title, start_time, "⏳ ");
                    } else {
                        report_ready = true;
                    }
                }
            }
        }
    }

    async fn update_one_pass(
        &self,
        source_indexing_context: &Arc<SourceIndexingContext>,
        update_title: &str,
        update_options: super::source_indexer::UpdateOptions,
        progress_bar: Option<&ProgressBar>,
    ) -> Result<()> {
        let start_time = std::time::Instant::now();
        let update_stats = Arc::new(stats::UpdateStats::default());

        let update_fut = source_indexing_context.update(&update_stats, update_options);

        self.run_with_progress_report(
            update_fut,
            &update_stats,
            update_title,
            Some(start_time),
            progress_bar,
        )
        .await
        .with_context(|| {
            format!(
                "Error in processing flow `{}` source `{}` ({update_title})",
                self.flow.flow_instance.name,
                self.import_op().name
            )
        })?;

        if update_stats.has_any_change() {
            self.status_tx.send_modify(|update| {
                update.source_updates_num[self.source_idx] += 1;
            });
        }

        // Report final stats
        if let Some(progress_bar) = progress_bar {
            progress_bar.set_message("");
        }
        self.multi_progress_bar
            .suspend(|| self.report_stats(&update_stats, update_title, Some(start_time), "✅ "));
        Ok(())
    }

    async fn update_one_pass_with_error_logging(
        &self,
        source_indexing_context: &Arc<SourceIndexingContext>,
        update_title: &str,
        update_options: super::source_indexer::UpdateOptions,
        progress_bar: Option<&ProgressBar>,
    ) {
        let result = self
            .update_one_pass(
                source_indexing_context,
                update_title,
                update_options,
                progress_bar,
            )
            .await;

        if let Err(err) = result {
            error!("{:?}", err);
        }
    }

    fn import_op(&self) -> &plan::AnalyzedImportOp {
        &self.plan.import_ops[self.source_idx]
    }
}

impl FlowLiveUpdater {
    #[instrument(name = "flow_live_updater.start", skip_all, fields(flow_name = %flow_ctx.flow_name()))]
    pub async fn start(
        flow_ctx: Arc<FlowContext>,
        pool: &PgPool,
        multi_progress_bar: &LazyLock<MultiProgress>,
        options: FlowLiveUpdaterOptions,
    ) -> Result<Self> {
        let plan = flow_ctx.flow.get_execution_plan().await?;
        let execution_ctx = Arc::new(flow_ctx.use_owned_execution_ctx().await?);

        let (status_tx, status_rx) = watch::channel(FlowLiveUpdaterStatus {
            active_source_idx: BTreeSet::from_iter(0..plan.import_ops.len()),
            source_updates_num: vec![0; plan.import_ops.len()],
        });

        let (num_remaining_tasks_tx, num_remaining_tasks_rx) =
            watch::channel(plan.import_ops.len());

        let mut join_set = JoinSet::new();
        let mut stats_per_task = Vec::new();
        let operation_in_process_stats = Arc::new(stats::OperationInProcessStats::default());

        for source_idx in 0..plan.import_ops.len() {
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
                multi_progress_bar: (*multi_progress_bar).clone(),
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
                last_num_source_updates: vec![0; plan.import_ops.len()],
                is_done: false,
            }),
            num_remaining_tasks_rx,

            _status_tx: status_tx,
            _num_remaining_tasks_tx: num_remaining_tasks_tx,
        })
    }

    pub async fn wait(&self) -> Result<()> {
        {
            let mut rx = self.num_remaining_tasks_rx.clone();
            rx.wait_for(|v| *v == 0).await?;
        }

        let Some(mut join_set) = self.join_set.lock().unwrap().take() else {
            return Ok(());
        };
        while let Some(task_result) = join_set.join_next().await {
            match task_result {
                Ok(Ok(_)) => {}
                Ok(Err(err)) => {
                    return Err(err);
                }
                Err(err) if err.is_cancelled() => {}
                Err(err) => {
                    return Err(err.into());
                }
            }
        }
        Ok(())
    }

    pub fn abort(&self) {
        let mut join_set = self.join_set.lock().unwrap();
        if let Some(join_set) = &mut *join_set {
            join_set.abort_all();
        }
    }

    pub fn index_update_info(&self) -> stats::IndexUpdateInfo {
        stats::IndexUpdateInfo {
            sources: std::iter::zip(
                self.flow_ctx.flow.flow_instance.import_ops.iter(),
                self.stats_per_task.iter(),
            )
            .map(|(import_op, stats)| stats::SourceUpdateInfo {
                source_name: import_op.name.clone(),
                stats: stats.as_ref().clone(),
            })
            .collect(),
        }
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
            active_sources: status
                .active_source_idx
                .iter()
                .map(|idx| {
                    self.flow_ctx.flow.flow_instance.import_ops[*idx]
                        .name
                        .clone()
                })
                .collect(),
            updated_sources: status
                .source_updates_num
                .iter()
                .enumerate()
                .filter_map(|(idx, num_updates)| {
                    if num_updates > &recv_state.last_num_source_updates[idx] {
                        Some(
                            self.flow_ctx.flow.flow_instance.import_ops[idx]
                                .name
                                .clone(),
                        )
                    } else {
                        None
                    }
                })
                .collect(),
        };
        recv_state.last_num_source_updates = status.source_updates_num.clone();
        if status.active_source_idx.is_empty() {
            recv_state.is_done = true;
        }
        Ok(updates)
    }
}
