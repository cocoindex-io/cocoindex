use std::time::Instant;

use crate::prelude::*;

use super::stats;
use sqlx::PgPool;
use tokio::task::JoinSet;

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
    let maybe_print_stats = |stats: &stats::UpdateStats| {
        if options.print_stats {
            println!(
                "{}.{}: {}",
                flow_ctx.flow.flow_instance.name, import_op.name, stats
            );
        } else {
            trace!(
                "{}.{}: {}",
                flow_ctx.flow.flow_instance.name,
                import_op.name,
                stats
            );
        }
    };

    let mut update_start = Instant::now();
    source_context.update(&pool, &source_update_stats).await?;
    maybe_print_stats(&source_update_stats);

    if let (true, Some(refresh_interval)) = (
        options.live_mode,
        import_op.refresh_options.refresh_interval,
    ) {
        let mut last_stats = source_update_stats.as_ref().clone();
        loop {
            let elapsed = update_start.elapsed();
            if elapsed < refresh_interval {
                tokio::time::sleep(refresh_interval - elapsed).await;
            }
            update_start = Instant::now();
            source_context.update(&pool, &source_update_stats).await?;

            let this_stats = source_update_stats.as_ref().clone();
            maybe_print_stats(&this_stats.delta(&last_stats));
            last_stats = this_stats;
        }
    }
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
