use crate::prelude::*;

use crate::execution::db_tracking_setup;
use crate::ops::interface::*;
use crate::utils::fingerprint::{Fingerprint, Fingerprinter};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AnalyzedLocalFieldReference {
    /// Must be non-empty.
    pub fields_idx: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AnalyzedFieldReference {
    pub local: AnalyzedLocalFieldReference,
    /// How many levels up the scope the field is at.
    /// 0 means the current scope.
    #[serde(skip_serializing_if = "u32_is_zero")]
    pub scope_up_level: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AnalyzedLocalCollectorReference {
    pub collector_idx: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AnalyzedCollectorReference {
    pub local: AnalyzedLocalCollectorReference,
    /// How many levels up the scope the field is at.
    /// 0 means the current scope.
    #[serde(skip_serializing_if = "u32_is_zero")]
    pub scope_up_level: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct AnalyzedStructMapping {
    pub fields: Vec<AnalyzedValueMapping>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind")]
pub enum AnalyzedValueMapping {
    Constant { value: value::Value },
    Field(AnalyzedFieldReference),
    Struct(AnalyzedStructMapping),
}

#[derive(Debug, Clone)]
pub struct AnalyzedOpOutput {
    pub field_idx: u32,
}

pub struct AnalyzedImportOp {
    pub name: String,
    pub source_id: i32,
    pub executor: Box<dyn SourceExecutor>,
    pub output: AnalyzedOpOutput,
    pub primary_key_type: schema::ValueType,
    pub refresh_options: spec::SourceRefreshOptions,
}

pub struct AnalyzedFunctionExecInfo {
    pub enable_cache: bool,
    pub behavior_version: Option<u32>,

    /// Fingerprinter of the function's behavior.
    pub fingerprinter: Fingerprinter,
    /// To deserialize cached value.
    pub output_type: schema::ValueType,
}

pub struct AnalyzedTransformOp {
    pub name: String,
    pub inputs: Vec<AnalyzedValueMapping>,
    pub function_exec_info: AnalyzedFunctionExecInfo,
    pub executor: Box<dyn SimpleFunctionExecutor>,
    pub output: AnalyzedOpOutput,
}

pub struct AnalyzedForEachOp {
    pub name: String,
    pub local_field_ref: AnalyzedLocalFieldReference,
    pub op_scope: AnalyzedOpScope,
}

pub struct AnalyzedCollectOp {
    pub name: String,
    pub has_auto_uuid_field: bool,
    pub input: AnalyzedStructMapping,
    pub collector_ref: AnalyzedCollectorReference,
    /// Fingerprinter of the collector's schema. Used to decide when to reuse auto-generated UUIDs.
    pub fingerprinter: Fingerprinter,
}

pub enum AnalyzedPrimaryKeyDef {
    Fields(Vec<usize>),
}

pub struct AnalyzedExportOp {
    pub name: String,
    pub target_id: i32,
    pub input: AnalyzedLocalCollectorReference,
    pub export_target_factory: Arc<dyn ExportTargetFactory + Send + Sync>,
    pub export_context: Arc<dyn Any + Send + Sync>,
    pub primary_key_def: AnalyzedPrimaryKeyDef,
    pub primary_key_type: schema::ValueType,
    /// idx for value fields - excluding the primary key field.
    pub value_fields: Vec<u32>,
    /// If true, value is never changed on the same primary key.
    /// This is guaranteed if the primary key contains auto-generated UUIDs.
    pub value_stable: bool,
}

pub struct AnalyzedExportTargetOpGroup {
    pub target_factory: Arc<dyn ExportTargetFactory + Send + Sync>,
    pub op_idx: Vec<usize>,
}

pub enum AnalyzedReactiveOp {
    Transform(AnalyzedTransformOp),
    ForEach(AnalyzedForEachOp),
    Collect(AnalyzedCollectOp),
}

pub struct AnalyzedOpScope {
    pub reactive_ops: Vec<AnalyzedReactiveOp>,
    pub collector_len: usize,
}

pub struct ExecutionPlan {
    pub tracking_table_setup: db_tracking_setup::TrackingTableSetupState,
    pub logic_fingerprint: Fingerprint,

    pub import_ops: Vec<AnalyzedImportOp>,
    pub op_scope: AnalyzedOpScope,
    pub export_ops: Vec<AnalyzedExportOp>,
    pub export_op_groups: Vec<AnalyzedExportTargetOpGroup>,
}

pub struct TransientExecutionPlan {
    pub input_fields: Vec<AnalyzedOpOutput>,
    pub op_scope: AnalyzedOpScope,
    pub output_value: AnalyzedValueMapping,
}

fn u32_is_zero(v: &u32) -> bool {
    *v == 0
}
