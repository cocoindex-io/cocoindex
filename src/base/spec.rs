use crate::prelude::*;

use super::schema::{EnrichedValueType, FieldSchema, ValueType};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

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

impl fmt::Display for FieldPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

impl fmt::Display for OpArgName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

impl<T: fmt::Display> fmt::Display for NamedSpec<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name, self.spec)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMapping {
    /// If unspecified, means the current scope.
    /// "_root" refers to the top-level scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<ScopeName>,

    pub field_path: FieldPath,
}

impl fmt::Display for FieldMapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scope = self.scope.as_deref().unwrap_or("");
        write!(
            f,
            "{}{}",
            if scope.is_empty() {
                "".to_string()
            } else {
                format!("{}.", scope)
            },
            self.field_path
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstantMapping {
    pub schema: EnrichedValueType,
    pub value: serde_json::Value,
}

impl fmt::Display for ConstantMapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let schema = format_value_type(&self.schema);
        let value = serde_json::to_string(&self.value).unwrap_or("#serde_error".to_string());
        write!(f, "Constant({}: {})", value, schema)
    }
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

impl fmt::Display for StructMapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fields = self
            .fields
            .iter()
            .map(|field| format!("{}={}", field.name, field.spec))
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "[{}]", fields)
    }
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
                write!(f, "{}.{}", v.scope.as_deref().unwrap_or(""), v.field_path)
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

impl fmt::Display for OpArgBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.arg_name.is_unnamed() {
            write!(f, "{}", self.value)
        } else {
            write!(f, "{}={}", self.arg_name, self.value)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpSpec {
    pub kind: String,
    #[serde(flatten, default)]
    pub spec: serde_json::Map<String, serde_json::Value>,
}

impl fmt::Display for OpSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let spec_str = serde_json::to_string_pretty(&self.spec)
            .map(|s| {
                let lines: Vec<&str> = s.lines().take(50).collect();
                if lines.len() < s.lines().count() {
                    lines
                        .into_iter()
                        .chain(["..."])
                        .collect::<Vec<_>>()
                        .join("\n  ")
                } else {
                    lines.join("\n  ")
                }
            })
            .unwrap_or("#serde_error".to_string());
        write!(f, "OpSpec: kind={}, spec={}", self.kind, spec_str)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SourceRefreshOptions {
    pub refresh_interval: Option<std::time::Duration>,
}

impl fmt::Display for SourceRefreshOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let refresh = self
            .refresh_interval
            .map(|d| format!("{:?}", d))
            .unwrap_or("None".to_string());
        write!(f, "{}", refresh)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportOpSpec {
    pub source: OpSpec,

    #[serde(default)]
    pub refresh_options: SourceRefreshOptions,
}

impl fmt::Display for ImportOpSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Import: source={}, refresh={}",
            self.source, self.refresh_options
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformOpSpec {
    pub inputs: Vec<OpArgBinding>,
    pub op: OpSpec,
}

impl fmt::Display for TransformOpSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inputs = self
            .inputs
            .iter()
            .map(|input| input.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "Transform: op={}, inputs=[{}]", self.op, inputs)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForEachOpSpec {
    /// Mapping that provides a table to apply reactive operations to.
    pub field_path: FieldPath,
    pub op_scope: ReactiveOpScope,
}

impl fmt::Display for ForEachOpSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ForEach: field={}", self.field_path)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectOpSpec {
    /// Field values to be collected.
    pub input: StructMapping,
    /// Scope for the collector.
    pub scope_name: ScopeName,
    /// Name of the collector.
    pub collector_name: FieldName,
    /// If specified, the collector will have an automatically generated UUID field with the given name.
    /// The uuid will remain stable when collected input values remain unchanged.
    pub auto_uuid_field: Option<FieldName>,
}

impl fmt::Display for CollectOpSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Collect: scope={}, collector={}, input={}, uuid_field={:?}",
            self.scope_name, self.collector_name, self.input, self.auto_uuid_field
        )
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum VectorSimilarityMetric {
    CosineSimilarity,
    L2Distance,
    InnerProduct,
}

impl fmt::Display for VectorSimilarityMetric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VectorSimilarityMetric::CosineSimilarity => write!(f, "Cosine"),
            VectorSimilarityMetric::L2Distance => write!(f, "L2"),
            VectorSimilarityMetric::InnerProduct => write!(f, "InnerProduct"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VectorIndexDef {
    pub field_name: FieldName,
    pub metric: VectorSimilarityMetric,
}

impl fmt::Display for VectorIndexDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.field_name, self.metric)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_key_fields: Option<Vec<FieldName>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub vector_indexes: Vec<VectorIndexDef>,
}

impl fmt::Display for IndexOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let primary_keys = self
            .primary_key_fields
            .as_ref()
            .map(|p| p.join(", "))
            .unwrap_or_default();
        let vector_indexes = self
            .vector_indexes
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            f,
            "IndexOptions: primary_keys=[{}], vector_indexes=[{}]",
            primary_keys, vector_indexes
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportOpSpec {
    pub collector_name: FieldName,
    pub target: OpSpec,
    pub index_options: IndexOptions,
    pub setup_by_user: bool,
}

impl fmt::Display for ExportOpSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Export: collector={}, target={}, {}, setup_by_user={}",
            self.collector_name, self.target, self.index_options, self.setup_by_user
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action")]
pub enum ReactiveOpSpec {
    Transform(TransformOpSpec),
    ForEach(ForEachOpSpec),
    Collect(CollectOpSpec),
}

impl fmt::Display for ReactiveOpSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReactiveOpSpec::Transform(t) => write!(f, "{}", t),
            ReactiveOpSpec::ForEach(fe) => write!(f, "{}", fe),
            ReactiveOpSpec::Collect(c) => write!(f, "{}", c),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReactiveOpScope {
    pub name: ScopeName,
    pub ops: Vec<NamedSpec<ReactiveOpSpec>>,
    // TODO: Suport collectors
}

impl fmt::Display for ReactiveOpScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Scope: name={}", self.name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowInstanceSpec {
    /// Name of the flow instance.
    pub name: String,

    #[serde(default = "Vec::new", skip_serializing_if = "Vec::is_empty")]
    pub import_ops: Vec<NamedSpec<ImportOpSpec>>,

    #[serde(default = "Vec::new", skip_serializing_if = "Vec::is_empty")]
    pub reactive_ops: Vec<NamedSpec<ReactiveOpSpec>>,

    #[serde(default = "Vec::new", skip_serializing_if = "Vec::is_empty")]
    pub export_ops: Vec<NamedSpec<ExportOpSpec>>,

    #[serde(default = "Vec::new", skip_serializing_if = "Vec::is_empty")]
    pub declarations: Vec<OpSpec>,
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

pub struct AuthEntryReference<T> {
    pub key: String,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> fmt::Debug for AuthEntryReference<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AuthEntryReference({})", self.key)
    }
}

impl<T> fmt::Display for AuthEntryReference<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AuthEntryReference({})", self.key)
    }
}

impl<T> Clone for AuthEntryReference<T> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct UntypedAuthEntryReference<T> {
    key: T,
}

impl<T> Serialize for AuthEntryReference<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        UntypedAuthEntryReference { key: &self.key }.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for AuthEntryReference<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let untyped_ref = UntypedAuthEntryReference::<String>::deserialize(deserializer)?;
        Ok(AuthEntryReference {
            key: untyped_ref.key,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<T> PartialEq for AuthEntryReference<T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<T> Eq for AuthEntryReference<T> {}

impl<T> std::hash::Hash for AuthEntryReference<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

// Helper function to format EnrichedValueType
fn format_value_type(value_type: &EnrichedValueType) -> String {
    let mut typ = match &value_type.typ {
        ValueType::Basic(basic) => format!("{}", basic),
        ValueType::Table(t) => format!("{}", t.kind),
        ValueType::Struct(s) => format!("{}", s),
    };
    if value_type.nullable {
        typ.push('?');
    }
    typ
}
