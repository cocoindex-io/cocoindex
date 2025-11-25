use crate::base::schema::FieldSchema;
use crate::base::spec::FieldName;
use crate::prelude::*;

use crate::ops::interface::*;
use std::time::Duration;
use utils::fingerprint::{Fingerprint, Fingerprinter};

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

/// Tracks which affects value of the field, to detect changes of logic.
#[derive(Debug, Clone)]
pub struct FieldDefFingerprint {
    /// Name of sources that affect value of the field.
    pub source_op_names: HashSet<String>,
    /// Fingerprint of the logic that affects value of the field.
    pub fingerprint: Fingerprint,
}

impl Default for FieldDefFingerprint {
    fn default() -> Self {
        Self {
            source_op_names: HashSet::new(),
            fingerprint: Fingerprinter::default().into_fingerprint(),
        }
    }
}

pub struct AnalyzedImportOp {
    pub name: String,
    pub executor: Box<dyn SourceExecutor>,
    pub output: AnalyzedOpOutput,
    pub primary_key_schema: Box<[FieldSchema]>,
    pub refresh_options: spec::SourceRefreshOptions,

    pub concurrency_controller: concur_control::CombinedConcurrencyController,
}

pub struct AnalyzedFunctionExecInfo {
    pub enable_cache: bool,
    pub timeout: Option<Duration>,
    pub behavior_version: Option<u32>,

    /// Fingerprinter of the function's behavior.
    pub fingerprinter: Fingerprinter,
    /// To deserialize cached value.
    pub output_type: schema::ValueType,
}

pub struct AnalyzedTransformOp {
    pub name: String,
    pub op_kind: String,
    pub inputs: Vec<AnalyzedValueMapping>,
    pub function_exec_info: AnalyzedFunctionExecInfo,
    pub executor: Box<dyn SimpleFunctionExecutor>,
    pub output: AnalyzedOpOutput,
}

pub struct AnalyzedForEachOp {
    pub name: String,
    pub local_field_ref: AnalyzedLocalFieldReference,
    pub op_scope: AnalyzedOpScope,
    pub concurrency_controller: concur_control::ConcurrencyController,
}

pub struct AnalyzedCollectOp {
    pub name: String,
    pub has_auto_uuid_field: bool,
    pub input: AnalyzedStructMapping,
    pub input_field_names: Vec<FieldName>,
    pub collector_schema: Arc<schema::CollectorSchema>,
    pub collector_ref: AnalyzedCollectorReference,
    /// Pre-computed mapping from collector field index to input field index.
    pub field_index_mapping: Vec<Option<usize>>,
    /// Fingerprinter of the collector's schema. Used to decide when to reuse auto-generated UUIDs.
    pub fingerprinter: Fingerprinter,
}

pub enum AnalyzedPrimaryKeyDef {
    Fields(Vec<usize>),
}

pub struct AnalyzedExportOp {
    pub name: String,
    pub input: AnalyzedLocalCollectorReference,
    pub export_target_factory: Arc<dyn TargetFactory + Send + Sync>,
    pub export_context: Arc<dyn Any + Send + Sync>,
    pub primary_key_def: AnalyzedPrimaryKeyDef,
    pub primary_key_schema: Box<[FieldSchema]>,
    /// idx for value fields - excluding the primary key field.
    pub value_fields: Vec<u32>,
    /// If true, value is never changed on the same primary key.
    /// This is guaranteed if the primary key contains auto-generated UUIDs.
    pub value_stable: bool,
    /// Fingerprinter of the output value.
    pub output_value_fingerprinter: Fingerprinter,
    pub def_fp: FieldDefFingerprint,
}

pub struct AnalyzedExportTargetOpGroup {
    pub target_factory: Arc<dyn TargetFactory + Send + Sync>,
    pub target_kind: String,
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
    pub scope_qualifier: String,
}

pub struct ExecutionPlan {
    pub legacy_fingerprint: Vec<Fingerprint>,
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
