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
pub struct NodeReferenceMapping {
    pub label: String,
    pub fields: Vec<TargetFieldMapping>,
}

#[derive(Debug, Deserialize)]
pub struct NodeStorageSpec {
    #[serde(flatten)]
    pub index_options: spec::IndexOptions,
}

#[derive(Debug, Deserialize)]
pub struct NodeMapping {
    pub label: String,
}

#[derive(Debug, Deserialize)]
pub struct RelationshipMapping {
    pub rel_type: String,
    pub source: NodeReferenceMapping,
    pub target: NodeReferenceMapping,
    pub nodes_storage_spec: Option<BTreeMap<String, NodeStorageSpec>>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind")]
pub enum GraphElementMapping {
    Relationship(RelationshipMapping),
    Node(NodeMapping),
}
