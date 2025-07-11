use crate::prelude::*;

use super::stats;
use futures::future::try_join_all;
use sqlx::PgPool;
use tokio::{task::JoinSet, time::MissedTickBehavior};

pub struct FlowLiveUpdater {
    flow_ctx: Arc<FlowContext>,
    tasks: JoinSet<Result<()>>,
    sources_update_stats: Vec<Arc<stats::UpdateStats>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FlowLiveUpdaterOptions {
    /// If true, the updater will keep refreshing the index.
    /// Otherwise, it will only apply changes from the source up to the current time.
    pub live_mode: bool,

    /// If true, stats will be printed to the console.
    pub print_stats: bool,
}

const REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

struct SharedAckFn {
    count: usize,
    ack_fn: Option<Box<dyn FnOnce() -> BoxFuture<'static, Result<()>> + Send + Sync>>,
}

impl SharedAckFn {
    fn new(
        count: usize,
        ack_fn: Box<dyn FnOnce() -> BoxFuture<'static, Result<()>> + Send + Sync>,
    ) -> Self {
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

async fn update_source(
    flow: Arc<builder::AnalyzedFlow>,
    plan: Arc<plan::ExecutionPlan>,
    execution_ctx: Arc<tokio::sync::OwnedRwLockReadGuard<crate::lib_context::FlowExecutionContext>>,
    source_update_stats: Arc<stats::UpdateStats>,
    source_idx: usize,
    pool: PgPool,
    options: FlowLiveUpdaterOptions,
) -> Result<()> {
    let source_context = execution_ctx
        .get_source_indexing_context(&flow, source_idx, &pool)
        .await?;

    let import_op = &plan.import_ops[source_idx];

    let report_stats = |stats: &stats::UpdateStats, kind: &str| {
        source_update_stats.merge(stats);
        if options.print_stats {
            println!(
                "{}.{} ({kind}): {}",
                flow.flow_instance.name, import_op.name, stats
            );
        } else {
            trace!(
                "{}.{} ({kind}): {}",
                flow.flow_instance.name, import_op.name, stats
            );
        }
    };

    let mut futs: Vec<BoxFuture<'_, Result<()>>> = Vec::new();

    // Deal with change streams.
    if options.live_mode {
        if let Some(change_stream) = import_op.executor.change_stream().await? {
            let change_stream_stats = Arc::new(stats::UpdateStats::default());
            futs.push(
                {
                    let change_stream_stats = change_stream_stats.clone();
                    let pool = pool.clone();
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
                            let ack_fn = change_msg.ack_fn.map(|ack_fn| {
                                Arc::new(Mutex::new(SharedAckFn::new(
                                    change_msg.changes.iter().len(),
                                    ack_fn,
                                )))
                            });
                            for change in change_msg.changes {
                                let ack_fn = ack_fn.clone();
                                let concur_permit = import_op
                                    .concurrency_controller
                                    .acquire(concur_control::BYTES_UNKNOWN_YET)
                                    .await?;
                                tokio::spawn(source_context.clone().process_source_key(
                                    change.key,
                                    change.data,
                                    change_stream_stats.clone(),
                                    concur_permit,
                                    ack_fn.map(|ack_fn| {
                                        move || async move { SharedAckFn::ack(&ack_fn).await }
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
                    let mut last_change_stream_stats = change_stream_stats.as_ref().clone();
                    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    interval.tick().await;
                    loop {
                        interval.tick().await;
                        let curr_change_stream_stats = change_stream_stats.as_ref().clone();
                        let delta = curr_change_stream_stats.delta(&last_change_stream_stats);
                        if !delta.has_any_change() {
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
    futs.push(
        async move {
            let update_stats = Arc::new(stats::UpdateStats::default());
            source_context.update(&pool, &update_stats).await?;
            report_stats(&update_stats, "batch update");

            if let (true, Some(refresh_interval)) = (
                options.live_mode,
                import_op.refresh_options.refresh_interval,
            ) {
                let mut interval = tokio::time::interval(refresh_interval);
                interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                interval.tick().await;
                loop {
                    interval.tick().await;

                    let update_stats = Arc::new(stats::UpdateStats::default());
                    source_context.update(&pool, &update_stats).await?;
                    report_stats(&update_stats, "interval refresh");
                }
            }
            Ok(())
        }
        .boxed(),
    );

    try_join_all(futs).await?;
    Ok(())
}

impl FlowLiveUpdater {
    pub async fn start(
        flow_ctx: Arc<FlowContext>,
        pool: &PgPool,
        options: FlowLiveUpdaterOptions,
    ) -> Result<Self> {
        let plan = flow_ctx.flow.get_execution_plan().await?;
        let execution_ctx = Arc::new(flow_ctx.use_owned_execution_ctx().await?);

        let mut tasks = JoinSet::new();
        let sources_update_stats = (0..plan.import_ops.len())
            .map(|source_idx| {
                let source_update_stats = Arc::new(stats::UpdateStats::default());
                tasks.spawn(update_source(
                    flow_ctx.flow.clone(),
                    plan.clone(),
                    execution_ctx.clone(),
                    source_update_stats.clone(),
                    source_idx,
                    pool.clone(),
                    options.clone(),
                ));
                source_update_stats
            })
            .collect();
        Ok(Self {
            flow_ctx,
            tasks,
            sources_update_stats,
        })
    }

    pub async fn wait(&mut self) -> Result<()> {
        while let Some(result) = self.tasks.join_next().await {
            match result {
                Err(e) if !e.is_cancelled() => {
                    error!("A background task in FlowLiveUpdater failed to join: {e:?}");
                }
                Ok(Err(e)) => {
                    error!("Error reported by a source update task during live update: {e:?}");
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn abort(&mut self) {
        self.tasks.abort_all();
    }

    pub fn index_update_info(&self) -> stats::IndexUpdateInfo {
        stats::IndexUpdateInfo {
            sources: std::iter::zip(
                self.flow_ctx.flow.flow_instance.import_ops.iter(),
                self.sources_update_stats.iter(),
            )
            .map(|(import_op, stats)| stats::SourceUpdateInfo {
                source_name: import_op.name.clone(),
                stats: (**stats).clone(),
            })
            .collect(),
        }
    }
}
