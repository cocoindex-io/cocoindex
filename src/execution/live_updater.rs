use crate::{
    execution::{source_indexer::ProcessSourceRowInput, stats::UpdateStats},
    prelude::*,
};

use super::stats;
use futures::future::try_join_all;
use sqlx::PgPool;
use tokio::{sync::watch, task::JoinSet, time::MissedTickBehavior};

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

    /// If true, stats will be printed to the console.
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
        let source_idx = self.source_idx;
        let source_context = self
            .execution_ctx
            .get_source_indexing_context(&self.flow, source_idx, &self.pool)
            .await?;

        let import_op = &self.plan.import_ops[source_idx];

        let report_stats = |stats: &stats::UpdateStats, kind: &str| {
            self.source_update_stats.merge(stats);
            if self.options.print_stats {
                println!(
                    "{}.{} ({kind}): {}",
                    self.flow.flow_instance.name, import_op.name, stats
                );
            } else {
                trace!(
                    "{}.{} ({kind}): {}",
                    self.flow.flow_instance.name, import_op.name, stats
                );
            }
        };

        let mut futs: Vec<BoxFuture<'_, Result<()>>> = Vec::new();

        // Deal with change streams.
        if self.options.live_mode {
            if let Some(change_stream) = import_op.executor.change_stream().await? {
                let change_stream_stats = Arc::new(stats::UpdateStats::default());
                futs.push(
                    {
                        let change_stream_stats = change_stream_stats.clone();
                        let pool = self.pool.clone();
                        let status_tx = self.status_tx.clone();
                        async move {
                            let mut change_stream = change_stream;
                            let retry_options = retryable::RetryOptions {
                                max_retries: None,
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
                                            .map_err(retryable::Error::always_retryable)
                                    },
                                    &retry_options,
                                )
                                .await?;
                                let change_msg = if let Some(change_msg) = change_msg {
                                    change_msg
                                } else {
                                    break;
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
                                    tokio::spawn(source_context.clone().process_source_row(
                                        ProcessSourceRowInput {
                                            key: change.key,
                                            key_aux_info: Some(change.key_aux_info),
                                            data: change.data,
                                        },
                                        super::source_indexer::UpdateMode::Normal,
                                        update_stats.clone(),
                                        concur_permit,
                                        Some(move || async move {
                                            SharedAckFn::ack(&shared_ack_fn).await
                                        }),
                                        pool.clone(),
                                    ));
                                }
                            }
                            Ok(())
                        }
                    }
                    .boxed(),
                );

                futs.push(
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
                                report_stats(&delta, "change stream");
                                last_change_stream_stats = curr_change_stream_stats;
                            }
                        }
                    }
                    .boxed(),
                );
            }
        }

        // The main update loop.
        futs.push({
            let status_tx = self.status_tx.clone();
            let pool = self.pool.clone();
            let live_mode = self.options.live_mode;
            async move {
                let update_stats = Arc::new(stats::UpdateStats::default());
                source_context
                    .update(
                        &pool,
                        &update_stats,
                        super::source_indexer::UpdateOptions {
                            expect_little_diff: false,
                            mode: if self.options.reexport_targets {
                                super::source_indexer::UpdateMode::ReexportTargets
                            } else {
                                super::source_indexer::UpdateMode::Normal
                            },
                        },
                    )
                    .await?;
                if update_stats.has_any_change() {
                    status_tx.send_modify(|update| {
                        update.source_updates_num[source_idx] += 1;
                    });
                }
                report_stats(&update_stats, "batch update");

                if let (true, Some(refresh_interval)) =
                    (live_mode, import_op.refresh_options.refresh_interval)
                {
                    let mut interval = tokio::time::interval(refresh_interval);
                    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    interval.tick().await;
                    loop {
                        interval.tick().await;

                        let update_stats = Arc::new(stats::UpdateStats::default());
                        source_context
                            .update(
                                &pool,
                                &update_stats,
                                super::source_indexer::UpdateOptions {
                                    expect_little_diff: true,
                                    mode: super::source_indexer::UpdateMode::Normal,
                                },
                            )
                            .await?;
                        if update_stats.has_any_change() {
                            status_tx.send_modify(|update| {
                                update.source_updates_num[source_idx] += 1;
                            });
                        }
                        report_stats(&update_stats, "interval refresh");
                    }
                }
                Ok(())
            }
            .boxed()
        });

        let join_result = try_join_all(futs).await;
        if let Err(err) = join_result {
            error!("Error in source `{}`: {:?}", import_op.name, err);
            return Err(err);
        }
        Ok(())
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

        let (status_tx, status_rx) = watch::channel(FlowLiveUpdaterStatus {
            active_source_idx: BTreeSet::from_iter(0..plan.import_ops.len()),
            source_updates_num: vec![0; plan.import_ops.len()],
        });

        let (num_remaining_tasks_tx, num_remaining_tasks_rx) =
            watch::channel(plan.import_ops.len());

        let mut join_set = JoinSet::new();
        let mut stats_per_task = Vec::new();

        for source_idx in 0..plan.import_ops.len() {
            let source_update_stats = Arc::new(stats::UpdateStats::default());
            let source_update_task = SourceUpdateTask {
                source_idx,
                flow: flow_ctx.flow.clone(),
                plan: plan.clone(),
                execution_ctx: execution_ctx.clone(),
                source_update_stats: source_update_stats.clone(),
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
