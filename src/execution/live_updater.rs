use std::time::Instant;

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

struct StatsReportState {
    last_report_time: Option<Instant>,
    last_stats: stats::UpdateStats,
}

const MIN_REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

async fn update_source(
    flow_ctx: Arc<FlowContext>,
    plan: Arc<plan::ExecutionPlan>,
    source_update_stats: Arc<stats::UpdateStats>,
    source_idx: usize,
    pool: PgPool,
    options: FlowLiveUpdaterOptions,
) -> Result<()> {
    let source_context = flow_ctx
        .get_source_indexing_context(source_idx, &pool)
        .await?;

    let import_op = &plan.import_ops[source_idx];

    let stats_report_state = Mutex::new(StatsReportState {
        last_report_time: None,
        last_stats: source_update_stats.as_ref().clone(),
    });
    let report_stats = || {
        let new_stats = source_update_stats.as_ref().clone();
        let now = Instant::now();
        let delta = {
            let mut state = stats_report_state.lock().unwrap();
            if let Some(last_report_time) = state.last_report_time {
                if now.duration_since(last_report_time) < MIN_REPORT_INTERVAL {
                    return;
                }
            }
            let delta = new_stats.delta(&state.last_stats);
            if delta.is_zero() {
                return;
            }
            state.last_stats = new_stats;
            state.last_report_time = Some(now);
            delta
        };
        if options.print_stats {
            println!(
                "{}.{}: {}",
                flow_ctx.flow.flow_instance.name, import_op.name, delta
            );
        } else {
            trace!(
                "{}.{}: {}",
                flow_ctx.flow.flow_instance.name,
                import_op.name,
                delta
            );
        }
    };

    let mut futs: Vec<BoxFuture<'_, Result<()>>> = Vec::new();

    // Deal with change streams.
    if let (true, Some(change_stream)) = (options.live_mode, import_op.executor.change_stream()) {
        let pool = pool.clone();
        let source_update_stats = source_update_stats.clone();
        futs.push(
            async move {
                let mut change_stream = change_stream;
                while let Some(change) = change_stream.next().await {
                    source_context
                        .process_change(change, &pool, &source_update_stats)
                        .map(tokio::spawn);
                }
                Ok(())
            }
            .boxed(),
        );
        futs.push(
            async move {
                let mut interval = tokio::time::interval(REPORT_INTERVAL);
                interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                interval.tick().await;
                loop {
                    interval.tick().await;
                    report_stats();
                }
            }
            .boxed(),
        );
    }

    // The main update loop.
    let source_update_stats = source_update_stats.clone();
    futs.push(
        async move {
            source_context.update(&pool, &source_update_stats).await?;
            report_stats();

            if let (true, Some(refresh_interval)) = (
                options.live_mode,
                import_op.refresh_options.refresh_interval,
            ) {
                let mut interval = tokio::time::interval(refresh_interval);
                interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                interval.tick().await;
                loop {
                    interval.tick().await;
                    source_context.update(&pool, &source_update_stats).await?;
                    report_stats();
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

        let mut tasks = JoinSet::new();
        let sources_update_stats = (0..plan.import_ops.len())
            .map(|source_idx| {
                let source_update_stats = Arc::new(stats::UpdateStats::default());
                tasks.spawn(update_source(
                    flow_ctx.clone(),
                    plan.clone(),
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
            if let Err(e) = (|| anyhow::Ok(result??))() {
                error!("{:?}", e.context("Error in applying changes from a source"));
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
                stats: (&**stats).clone(),
            })
            .collect(),
        }
    }
}
