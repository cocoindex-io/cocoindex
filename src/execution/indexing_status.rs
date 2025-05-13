use crate::prelude::*;

use super::db_tracking;
use super::evaluator;
use futures::try_join;

#[derive(Debug, Serialize)]
pub struct SourceRowLastProcessedInfo {
    pub source_ordinal: interface::Ordinal,
    pub processing_time: Option<chrono::DateTime<chrono::Utc>>,
    pub is_logic_current: bool,
}

#[derive(Debug, Serialize)]
pub struct SourceRowInfo {
    pub ordinal: interface::Ordinal,
}

#[derive(Debug, Serialize)]
pub struct SourceRowIndexingStatus {
    pub last_processed: Option<SourceRowLastProcessedInfo>,
    pub current: Option<SourceRowInfo>,
}

pub async fn get_source_row_indexing_status(
    src_eval_ctx: &evaluator::SourceRowEvaluationContext<'_>,
    pool: &sqlx::PgPool,
) -> Result<SourceRowIndexingStatus> {
    let source_key_json = serde_json::to_value(src_eval_ctx.key)?;
    let last_processed_fut = db_tracking::read_source_last_processed_info(
        src_eval_ctx.import_op.source_id,
        &source_key_json,
        &src_eval_ctx.plan.tracking_table_setup,
        pool,
    );
    let current_fut = src_eval_ctx.import_op.executor.get_value(
        &src_eval_ctx.key,
        &interface::SourceExecutorGetOptions {
            include_value: false,
            include_ordinal: true,
        },
    );
    let (last_processed, current) = try_join!(last_processed_fut, current_fut)?;

    let last_processed = last_processed.map(|l| SourceRowLastProcessedInfo {
        source_ordinal: interface::Ordinal(l.processed_source_ordinal),
        processing_time: l
            .process_time_micros
            .map(chrono::DateTime::<chrono::Utc>::from_timestamp_micros)
            .flatten(),
        is_logic_current: Some(src_eval_ctx.plan.logic_fingerprint.0.as_slice())
            == l.process_logic_fingerprint.as_ref().map(|b| b.as_slice()),
    });
    let current = SourceRowInfo {
        ordinal: current
            .ordinal
            .ok_or(anyhow::anyhow!("Ordinal is unavailable for the source"))?,
    };
    Ok(SourceRowIndexingStatus {
        last_processed,
        current: Some(current),
    })
}
