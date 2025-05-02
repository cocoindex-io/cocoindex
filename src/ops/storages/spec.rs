use crate::prelude::*;

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
