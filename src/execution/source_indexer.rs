use crate::prelude::*;

use super::{db_tracking, row_indexer, stats};
use futures::future::{join, join_all, try_join_all};
use sqlx::PgPool;

async fn update_source(
    source_name: &str,
    plan: &plan::ExecutionPlan,
    source_op: &plan::AnalyzedSourceOp,
    schema: &schema::DataSchema,
    pool: &PgPool,
) -> Result<stats::SourceUpdateInfo> {
    let existing_keys_json = db_tracking::list_source_tracking_keys(
        source_op.source_id,
        &plan.tracking_table_setup,
        pool,
    )
    .await?;

    let mut keys = Vec::new();
    let mut rows_stream = source_op
        .executor
        .list(interface::SourceExecutorListOptions {
            include_ordinal: false,
        });
    while let Some(rows) = rows_stream.next().await {
        keys.extend(rows?.into_iter().map(|row| row.key));
    }

    let stats = stats::UpdateStats::default();
    let upsert_futs = join_all(keys.iter().map(|key| {
        row_indexer::update_source_row_with_err_handling(
            plan, source_op, schema, key, false, pool, &stats,
        )
    }));
    let deleted_keys = existing_keys_json
        .into_iter()
        .map(|existing_key_json| {
            value::Value::<value::ScopeValue>::from_json(
                existing_key_json.source_key,
                &source_op.primary_key_type,
            )?
            .as_key()
        })
        .filter_ok(|existing_key| !keys.contains(existing_key))
        .collect::<Result<Vec<_>>>()?;
    let delete_futs = join_all(deleted_keys.iter().map(|key| {
        row_indexer::update_source_row_with_err_handling(
            plan, source_op, schema, key, true, pool, &stats,
        )
    }));
    join(upsert_futs, delete_futs).await;

    Ok(stats::SourceUpdateInfo {
        source_name: source_name.to_string(),
        stats,
    })
}

pub async fn update(
    plan: &plan::ExecutionPlan,
    schema: &schema::DataSchema,
    pool: &PgPool,
) -> Result<stats::IndexUpdateInfo> {
    let source_update_stats = try_join_all(
        plan.source_ops
            .iter()
            .map(|source_op| async move {
                update_source(source_op.name.as_str(), plan, source_op, schema, pool).await
            })
            .collect::<Vec<_>>(),
    )
    .await?;
    Ok(stats::IndexUpdateInfo {
        sources: source_update_stats,
    })
}
