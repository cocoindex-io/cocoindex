use std::time::Instant;

use crate::prelude::*;

use super::stats;
use sqlx::PgPool;
use tokio::task::JoinSet;

pub struct FlowSynchronizer {
    flow_ctx: Arc<FlowContext>,
    tasks: JoinSet<Result<()>>,
    sources_update_stats: Vec<Arc<stats::UpdateStats>>,
}

pub struct FlowSynchronizerOptions {
    pub keep_refreshed: bool,
}

async fn sync_source(
    flow_ctx: Arc<FlowContext>,
    plan: Arc<plan::ExecutionPlan>,
    source_update_stats: Arc<stats::UpdateStats>,
    source_idx: usize,
    pool: PgPool,
    keep_refreshed: bool,
) -> Result<()> {
    let source_context = flow_ctx
        .get_source_indexing_context(source_idx, &pool)
        .await?;

    let mut update_start = Instant::now();
    source_context.update(&pool, &source_update_stats).await?;

    let import_op = &plan.import_ops[source_idx];
    if let (true, Some(refresh_interval)) =
        (keep_refreshed, import_op.refresh_options.refresh_interval)
    {
        loop {
            let elapsed = update_start.elapsed();
            if elapsed < refresh_interval {
                tokio::time::sleep(refresh_interval - elapsed).await;
            }
            update_start = Instant::now();
            source_context.update(&pool, &source_update_stats).await?;
        }
    }
    Ok(())
}

impl FlowSynchronizer {
    pub async fn start(
        flow_ctx: Arc<FlowContext>,
        pool: &PgPool,
        options: &FlowSynchronizerOptions,
    ) -> Result<Self> {
        let plan = flow_ctx.flow.get_execution_plan().await?;

        let mut tasks = JoinSet::new();
        let sources_update_stats = (0..plan.import_ops.len())
            .map(|source_idx| {
                let source_update_stats = Arc::new(stats::UpdateStats::default());
                tasks.spawn(sync_source(
                    flow_ctx.clone(),
                    plan.clone(),
                    source_update_stats.clone(),
                    source_idx,
                    pool.clone(),
                    options.keep_refreshed,
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

    pub async fn join(&mut self) -> Result<()> {
        while let Some(result) = self.tasks.join_next().await {
            if let Err(e) = (|| anyhow::Ok(result??))() {
                error!("{:?}", e.context("Error in synchronizing a source"));
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
