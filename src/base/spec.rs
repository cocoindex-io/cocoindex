use std::ops::Deref;

use serde::{Deserialize, Serialize};

use super::schema::{EnrichedValueType, FieldSchema};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum SpecString {
    /// The value comes from the environment variable.
    Env(String),
    /// The value is defined by the literal string.
    #[serde(untagged)]
    Literal(String),
}

pub type ScopeName = String;

/// Used to identify a data field within a flow.
/// Within a flow, in each specific scope, each field name must be unique.
/// - A field is defined by `outputs` of an operation. There must be exactly one definition for each field.
/// - A field can be used as an input for multiple operations.
pub type FieldName = String;

pub const ROOT_SCOPE_NAME: &str = "_root";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub struct FieldPath(pub Vec<FieldName>);

impl Deref for FieldPath {
    type Target = Vec<FieldName>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for FieldPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            write!(f, "*")
        } else {
            write!(f, "{}", self.join("."))
        }
    }
}

/// Used to identify an input or output argument for an operator.
/// Useful to identify different inputs/outputs of the same operation. Usually omitted for operations with the same purpose of input/output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct OpArgName(pub Option<String>);

impl std::fmt::Display for OpArgName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(arg_name) = &self.0 {
            write!(f, "${}", arg_name)
        } else {
            write!(f, "?")
        }
    }
}

impl OpArgName {
    pub fn is_unnamed(&self) -> bool {
        self.0.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NamedSpec<T> {
    pub name: String,

    #[serde(flatten)]
    pub spec: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMapping {
    /// If unspecified, means the current scope.
    /// "_root" refers to the top-level scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<ScopeName>,

    pub field_path: FieldPath,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstantMapping {
    pub schema: EnrichedValueType,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionMapping {
    pub field: FieldMapping,
    pub scope_name: ScopeName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructMapping {
    pub fields: Vec<NamedSpec<ValueMapping>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ValueMapping {
    Constant(ConstantMapping),
    Field(FieldMapping),
    Struct(StructMapping),
    // TODO: Add support for collections
}

impl ValueMapping {
    pub fn is_entire_scope(&self) -> bool {
        match self {
            ValueMapping::Field(FieldMapping {
                scope: None,
                field_path,
            }) => field_path.is_empty(),
            _ => false,
        }
    }
}

impl std::fmt::Display for ValueMapping {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueMapping::Constant(v) => write!(
                f,
                "{}",
                serde_json::to_string(&v.value)
                    .unwrap_or_else(|_| "#(invalid json value)".to_string())
            ),
            ValueMapping::Field(v) => {
                write!(
                    f,
                    "{}.{}",
                    v.scope.as_ref().map(|s| s.as_str()).unwrap_or(""),
                    v.field_path
                )
            }
            ValueMapping::Struct(v) => write!(
                f,
                "Struct({})",
                v.fields
                    .iter()
                    .map(|f| format!("{}={}", f.name, f.spec))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpArgBinding {
    #[serde(default, skip_serializing_if = "OpArgName::is_unnamed")]
    pub arg_name: OpArgName,

    #[serde(flatten)]
    pub value: ValueMapping,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpSpec {
    pub kind: String,
    #[serde(flatten, default)]
    pub spec: serde_json::Map<String, serde_json::Value>,
}

/// Transform data using a given operator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformOpSpec {
    pub inputs: Vec<OpArgBinding>,
    pub op: OpSpec,
}

/// Apply reactive operations to each row of the input field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForEachOpSpec {
    /// Mapping that provides a collection of rows to apply reactive operations to.
    pub field_path: FieldPath,
    pub op_scope: ReactiveOpScope,
}

/// Emit data to a given collector at the given scope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectOpSpec {
    pub input: StructMapping,
    pub scope_name: ScopeName,
    pub collector_name: FieldName,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum VectorSimilarityMetric {
    CosineSimilarity,
    L2Distance,
    InnerProduct,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VectorIndexDef {
    pub field_name: FieldName,
    pub metric: VectorSimilarityMetric,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_key_fields: Option<Vec<FieldName>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub vector_index_defs: Vec<VectorIndexDef>,
}

/// Store data to a given sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportOpSpec {
    pub collector_name: FieldName,
    pub target: OpSpec,
    pub index_options: IndexOptions,
}

/// A reactive operation reacts on given input values.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action")]
pub enum ReactiveOpSpec {
    Transform(TransformOpSpec),
    ForEach(ForEachOpSpec),
    Collect(CollectOpSpec),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReactiveOpScope {
    pub name: ScopeName,
    pub ops: Vec<NamedSpec<ReactiveOpSpec>>,
    // TODO: Suport collectors
}

/// A flow defines the rule to sync data from given sources to given sinks with given transformations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowInstanceSpec {
    /// Name of the flow instance.
    pub name: String,

    #[serde(default = "Vec::new", skip_serializing_if = "Vec::is_empty")]
    pub source_ops: Vec<NamedSpec<OpSpec>>,

    #[serde(default = "Vec::new", skip_serializing_if = "Vec::is_empty")]
    pub reactive_ops: Vec<NamedSpec<ReactiveOpSpec>>,

    #[serde(default = "Vec::new", skip_serializing_if = "Vec::is_empty")]
    pub export_ops: Vec<NamedSpec<ExportOpSpec>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransientFlowSpec {
    pub name: String,
    pub input_fields: Vec<FieldSchema>,
    pub reactive_ops: Vec<NamedSpec<ReactiveOpSpec>>,
    pub output_value: ValueMapping,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleSemanticsQueryHandlerSpec {
    pub name: String,
    pub flow_instance_name: String,
    pub export_target_name: String,
    pub query_transform_flow: TransientFlowSpec,
    pub default_similarity_metric: VectorSimilarityMetric,
}
