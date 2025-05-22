use crate::prelude::*;

use crate::ops::sdk::AuthEntryReference;

#[derive(Debug, Deserialize)]
pub struct TargetFieldMapping {
    pub source: spec::FieldName,

    /// Field name for the node in the Knowledge Graph.
    /// If unspecified, it's the same as `field_name`.
    #[serde(default)]
    pub target: Option<spec::FieldName>,
}

impl TargetFieldMapping {
    pub fn get_target(&self) -> &spec::FieldName {
        self.target.as_ref().unwrap_or(&self.source)
    }
}

#[derive(Debug, Deserialize)]
pub struct NodeFromFieldsSpec {
    pub label: String,
    pub fields: Vec<TargetFieldMapping>,
}

#[derive(Debug, Deserialize)]
pub struct NodesSpec {
    pub label: String,
}

#[derive(Debug, Deserialize)]
pub struct RelationshipsSpec {
    pub rel_type: String,
    pub source: NodeFromFieldsSpec,
    pub target: NodeFromFieldsSpec,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind")]
pub enum GraphElementMapping {
    Relationship(RelationshipsSpec),
    Node(NodesSpec),
}

#[derive(Debug, Deserialize)]
pub struct GraphDeclaration {
    pub nodes_label: String,

    #[serde(flatten)]
    pub index_options: spec::IndexOptions,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub enum ElementType {
    Node(String),
    Relationship(String),
}

impl ElementType {
    pub fn label(&self) -> &str {
        match self {
            ElementType::Node(label) => label,
            ElementType::Relationship(label) => label,
        }
    }

    pub fn from_mapping_spec(spec: &GraphElementMapping) -> Self {
        match spec {
            GraphElementMapping::Relationship(spec) => {
                ElementType::Relationship(spec.rel_type.clone())
            }
            GraphElementMapping::Node(spec) => ElementType::Node(spec.label.clone()),
        }
    }

    pub fn matcher(&self, var_name: &str) -> String {
        match self {
            ElementType::Relationship(label) => format!("()-[{var_name}:{label}]->()"),
            ElementType::Node(label) => format!("({var_name}:{label})"),
        }
    }
}

impl std::fmt::Display for ElementType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ElementType::Node(label) => write!(f, "Node(label:{label})"),
            ElementType::Relationship(rel_type) => write!(f, "Relationship(type:{rel_type})"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Derivative)]
#[derivative(PartialEq(bound = ""), Eq(bound = ""), Hash(bound = ""))]
pub struct GraphElement<AuthEntry> {
    #[serde(bound = "")]
    pub connection: AuthEntryReference<AuthEntry>,
    pub typ: ElementType,
}
