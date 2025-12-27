use crate::prelude::*;

use super::db_tracking;
use super::evaluator;
use futures::try_join;
use utils::fingerprint::{Fingerprint, Fingerprinter};

pub struct SourceLogicFingerprint {
    pub current: Fingerprint,
    pub legacy: Vec<Fingerprint>,
}

impl SourceLogicFingerprint {
    pub fn new(
        exec_plan: &plan::ExecutionPlan,
        source_idx: usize,
        export_exec_ctx: &[exec_ctx::ExportOpExecutionContext],
        legacy: Vec<Fingerprint>,
    ) -> Result<Self> {
        let import_op = &exec_plan.import_ops[source_idx];
        let mut fp = Fingerprinter::default();
        if exec_plan.export_ops.len() != export_exec_ctx.len() {
            internal_bail!("`export_ops` count does not match `export_exec_ctx` count");
        }
        for (export_op, export_op_exec_ctx) in
            std::iter::zip(exec_plan.export_ops.iter(), export_exec_ctx.iter())
        {
            if export_op.def_fp.source_op_names.contains(&import_op.name) {
                fp = fp.with(&(
                    &export_op.def_fp.fingerprint,
                    &export_op_exec_ctx.target_id,
                    &export_op_exec_ctx.schema_version_id,
                ))?;
            }
        }
        Ok(Self {
            current: fp.into_fingerprint(),
            legacy,
        })
    }

    pub fn matches(&self, other: impl AsRef<[u8]>) -> bool {
        self.current.as_slice() == other.as_ref()
            || self.legacy.iter().any(|fp| fp.as_slice() == other.as_ref())
    }
}

#[derive(Debug, Serialize)]
pub struct SourceRowLastProcessedInfo {
    pub source_ordinal: interface::Ordinal,
    pub processing_time: Option<chrono::DateTime<chrono::Utc>>,
    pub is_logic_current: bool,
}

#[derive(Debug, Serialize)]
pub struct SourceRowInfo {
    pub ordinal: Option<interface::Ordinal>,
}

#[derive(Debug, Serialize)]
pub struct SourceRowIndexingStatus {
    pub last_processed: Option<SourceRowLastProcessedInfo>,
    pub current: Option<SourceRowInfo>,
}

pub async fn get_source_row_indexing_status(
    src_eval_ctx: &evaluator::SourceRowEvaluationContext<'_>,
    key_aux_info: &serde_json::Value,
    setup_execution_ctx: &exec_ctx::FlowSetupExecutionContext,
    pool: &sqlx::PgPool,
) -> Result<SourceRowIndexingStatus> {
    let source_key_json = serde_json::to_value(src_eval_ctx.key)?;
    let last_processed_fut = db_tracking::read_source_last_processed_info(
        setup_execution_ctx.import_ops[src_eval_ctx.import_op_idx].source_id,
        &source_key_json,
        &setup_execution_ctx.setup_state.tracking_table,
        pool,
    );
    let current_fut = src_eval_ctx.import_op.executor.get_value(
        src_eval_ctx.key,
        key_aux_info,
        &interface::SourceExecutorReadOptions {
            include_value: false,
            include_ordinal: true,
            include_content_version_fp: false,
        },
    );
    let (last_processed, current) = try_join!(last_processed_fut, current_fut)?;

    let last_processed = last_processed.map(|l| SourceRowLastProcessedInfo {
        source_ordinal: interface::Ordinal(l.processed_source_ordinal),
        processing_time: l
            .process_time_micros
            .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_micros),
        is_logic_current: l
            .process_logic_fingerprint
            .as_ref()
            .map_or(false, |fp| src_eval_ctx.source_logic_fp.matches(fp)),
    });
    let current = SourceRowInfo {
        ordinal: current.ordinal,
    };
    Ok(SourceRowIndexingStatus {
        last_processed,
        current: Some(current),
    })
}
