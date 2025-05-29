use crate::prelude::*;

use crate::ops::sdk::{AuthEntryReference, FieldSchema};

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

#[derive(Debug, Serialize, Deserialize, Derivative)]
#[derivative(
    Clone(bound = ""),
    PartialEq(bound = ""),
    Eq(bound = ""),
    Hash(bound = "")
)]
pub struct GraphElementType<AuthEntry> {
    #[serde(bound = "")]
    pub connection: AuthEntryReference<AuthEntry>,
    pub typ: ElementType,
}

impl<AuthEntry> std::fmt::Display for GraphElementType<AuthEntry> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.connection.key, self.typ)
    }
}

pub struct GraphElementSchema {
    pub elem_type: ElementType,
    pub key_fields: Vec<schema::FieldSchema>,
    pub value_fields: Vec<schema::FieldSchema>,
}

pub struct GraphElementInputFieldsIdx {
    pub key: Vec<usize>,
    pub value: Vec<usize>,
}

impl GraphElementInputFieldsIdx {
    pub fn extract_key(&self, fields: &[value::Value]) -> Result<value::KeyValue> {
        value::KeyValue::from_values(self.key.iter().map(|idx| &fields[*idx]))
    }
}

pub struct AnalyzedGraphElementFieldMapping {
    pub schema: Arc<GraphElementSchema>,
    pub fields_input_idx: GraphElementInputFieldsIdx,
}

impl AnalyzedGraphElementFieldMapping {
    pub fn has_value_fields(&self) -> bool {
        !self.fields_input_idx.value.is_empty()
    }
}

pub struct AnalyzedRelationshipInfo {
    pub source: AnalyzedGraphElementFieldMapping,
    pub target: AnalyzedGraphElementFieldMapping,
}

pub struct AnalyzedDataCollection {
    pub schema: Arc<GraphElementSchema>,
    pub value_fields_input_idx: Vec<usize>,

    pub rel: Option<AnalyzedRelationshipInfo>,
}

impl AnalyzedDataCollection {
    pub fn dependent_node_labels(&self) -> IndexSet<&str> {
        let mut dependent_node_labels = IndexSet::new();
        if let Some(rel) = &self.rel {
            dependent_node_labels.insert(rel.source.schema.elem_type.label());
            dependent_node_labels.insert(rel.target.schema.elem_type.label());
        }
        dependent_node_labels
    }
}

struct GraphElementSchemaBuilder {
    elem_type: ElementType,
    key_fields: Vec<FieldSchema>,
    value_fields: Vec<FieldSchema>,
}

impl GraphElementSchemaBuilder {
    fn new(elem_type: ElementType) -> Self {
        Self {
            elem_type,
            key_fields: vec![],
            value_fields: vec![],
        }
    }

    fn merge_fields(
        elem_type: &ElementType,
        kind: &str,
        existing_fields: &mut Vec<FieldSchema>,
        fields: Vec<(usize, schema::FieldSchema)>,
    ) -> Result<Vec<usize>> {
        if fields.is_empty() {
            return Ok(vec![]);
        }
        let result: Vec<usize> = if existing_fields.is_empty() {
            let fields_idx: Vec<usize> = fields.iter().map(|(idx, _)| *idx).collect();
            existing_fields.extend(fields.into_iter().map(|(_, f)| f));
            fields_idx
        } else {
            if existing_fields.len() != fields.len() {
                bail!(
                    "{elem_type} {kind} fields number mismatch: {} vs {}",
                    existing_fields.len(),
                    fields.len()
                );
            }
            let mut fields_map: HashMap<_, _> = fields
                .into_iter()
                .map(|(idx, schema)| (schema.name, (idx, schema.value_type)))
                .collect();
            // Follow the order of existing fields
            existing_fields
                .iter()
                .map(|existing_field| {
                    let (idx, typ) = fields_map.remove(&existing_field.name).ok_or_else(|| {
                        anyhow!(
                            "{elem_type} {kind} field `{}` not found in some collector",
                            existing_field.name
                        )
                    })?;
                    if typ != existing_field.value_type {
                        bail!(
                            "{elem_type} {kind} field `{}` type mismatch: {} vs {}",
                            existing_field.name,
                            typ,
                            existing_field.value_type
                        )
                    }
                    Ok(idx)
                })
                .collect::<Result<Vec<_>>>()?
        };
        Ok(result)
    }

    fn merge(
        &mut self,
        key_fields: Vec<(usize, schema::FieldSchema)>,
        value_fields: Vec<(usize, schema::FieldSchema)>,
    ) -> Result<GraphElementInputFieldsIdx> {
        let key_fields_idx =
            Self::merge_fields(&self.elem_type, "key", &mut self.key_fields, key_fields)?;
        let value_fields_idx = Self::merge_fields(
            &self.elem_type,
            "value",
            &mut self.value_fields,
            value_fields,
        )?;
        Ok(GraphElementInputFieldsIdx {
            key: key_fields_idx,
            value: value_fields_idx,
        })
    }

    fn build_schema(self) -> Result<GraphElementSchema> {
        if self.key_fields.is_empty() {
            bail!(
                "No key fields specified for Node label `{}`",
                self.elem_type
            );
        }
        Ok(GraphElementSchema {
            elem_type: self.elem_type,
            key_fields: self.key_fields,
            value_fields: self.value_fields,
        })
    }
}
struct DependentNodeLabelAnalyzer<'a, AuthEntry> {
    graph_elem_type: GraphElementType<AuthEntry>,
    fields: IndexMap<spec::FieldName, (usize, schema::EnrichedValueType)>,
    remaining_fields: HashMap<&'a str, &'a TargetFieldMapping>,
    primary_key_fields: &'a [String],
}

impl<'a, AuthEntry> DependentNodeLabelAnalyzer<'a, AuthEntry> {
    fn new(
        conn: &'a spec::AuthEntryReference<AuthEntry>,
        rel_end_spec: &'a NodeFromFieldsSpec,
        primary_key_fields_map: &'a HashMap<&'a GraphElementType<AuthEntry>, &'a [String]>,
    ) -> Result<Self> {
        let graph_elem_type = GraphElementType {
            connection: conn.clone(),
            typ: ElementType::Node(rel_end_spec.label.clone()),
        };
        let primary_key_fields = primary_key_fields_map
            .get(&graph_elem_type)
            .ok_or_else(invariance_violation)?;
        Ok(Self {
            graph_elem_type,
            fields: IndexMap::new(),
            remaining_fields: rel_end_spec
                .fields
                .iter()
                .map(|f| (f.source.as_str(), f))
                .collect(),
            primary_key_fields,
        })
    }

    fn process_field(&mut self, field_idx: usize, field_schema: &schema::FieldSchema) -> bool {
        let field_mapping = match self.remaining_fields.remove(field_schema.name.as_str()) {
            Some(field_mapping) => field_mapping,
            None => return false,
        };
        self.fields.insert(
            field_mapping.get_target().clone(),
            (field_idx, field_schema.value_type.clone()),
        );
        true
    }

    fn build(
        self,
        schema_builders: &mut HashMap<GraphElementType<AuthEntry>, GraphElementSchemaBuilder>,
    ) -> Result<(GraphElementType<AuthEntry>, GraphElementInputFieldsIdx)> {
        if !self.remaining_fields.is_empty() {
            anyhow::bail!(
                "Fields not mapped for {}: {}",
                self.graph_elem_type,
                self.remaining_fields.keys().join(", ")
            );
        }

        let (mut key_fields, value_fields): (Vec<_>, Vec<_>) = self
            .fields
            .into_iter()
            .map(|(field_name, (idx, typ))| (idx, FieldSchema::new(field_name, typ)))
            .partition(|(_, f)| self.primary_key_fields.contains(&f.name));
        if key_fields.len() != self.primary_key_fields.len() {
            bail!(
                "Primary key fields number mismatch: {} vs {}",
                key_fields.iter().map(|(_, f)| &f.name).join(", "),
                self.primary_key_fields.iter().join(", ")
            );
        }
        key_fields.sort_by_key(|(_, f)| {
            self.primary_key_fields
                .iter()
                .position(|k| k == &f.name)
                .unwrap()
        });

        let fields_idx = schema_builders
            .entry(self.graph_elem_type.clone())
            .or_insert_with(|| GraphElementSchemaBuilder::new(self.graph_elem_type.typ.clone()))
            .merge(key_fields, value_fields)?;
        Ok((self.graph_elem_type, fields_idx))
    }
}

pub struct DataCollectionGraphMappingInput<'a, AuthEntry> {
    pub auth_ref: &'a spec::AuthEntryReference<AuthEntry>,
    pub mapping: &'a GraphElementMapping,
    pub index_options: &'a spec::IndexOptions,

    pub key_fields_schema: Vec<FieldSchema>,
    pub value_fields_schema: Vec<FieldSchema>,
}

pub fn analyze_graph_mappings<'a, AuthEntry: 'a>(
    data_coll_inputs: impl Iterator<Item = DataCollectionGraphMappingInput<'a, AuthEntry>>,
    declarations: impl Iterator<
        Item = (
            &'a spec::AuthEntryReference<AuthEntry>,
            &'a GraphDeclaration,
        ),
    >,
) -> Result<(Vec<AnalyzedDataCollection>, Vec<Arc<GraphElementSchema>>)> {
    let data_coll_inputs: Vec<_> = data_coll_inputs.collect();
    let decls: Vec<_> = declarations.collect();

    // 1a. Prepare graph element types
    let graph_elem_types = data_coll_inputs
        .iter()
        .map(|d| GraphElementType {
            connection: d.auth_ref.clone(),
            typ: ElementType::from_mapping_spec(d.mapping),
        })
        .collect::<Vec<_>>();
    let decl_graph_elem_types = decls
        .iter()
        .map(|(auth_ref, decl)| GraphElementType {
            connection: (*auth_ref).clone(),
            typ: ElementType::Node(decl.nodes_label.clone()),
        })
        .collect::<Vec<_>>();

    // 1b. Prepare primary key fields map
    let primary_key_fields_map: HashMap<&GraphElementType<AuthEntry>, &[spec::FieldName]> =
        std::iter::zip(data_coll_inputs.iter(), graph_elem_types.iter())
            .map(|(data_coll_input, graph_elem_type)| {
                (
                    graph_elem_type,
                    data_coll_input.index_options.primary_key_fields(),
                )
            })
            .chain(
                std::iter::zip(decl_graph_elem_types.iter(), decls.iter()).map(
                    |(graph_elem_type, (_, decl))| {
                        (graph_elem_type, decl.index_options.primary_key_fields())
                    },
                ),
            )
            .map(|(graph_elem_type, primary_key_fields)| {
                Ok((
                    graph_elem_type,
                    primary_key_fields.with_context(|| {
                        format!("Primary key fields are not set for {graph_elem_type}")
                    })?,
                ))
            })
            .collect::<Result<_>>()?;

    // 2. Analyze data collection graph mappings and build target schema
    let mut node_schema_builders =
        HashMap::<GraphElementType<AuthEntry>, GraphElementSchemaBuilder>::new();
    struct RelationshipProcessedInfo<AuthEntry> {
        rel_schema: GraphElementSchema,
        source_typ: GraphElementType<AuthEntry>,
        source_fields_idx: GraphElementInputFieldsIdx,
        target_typ: GraphElementType<AuthEntry>,
        target_fields_idx: GraphElementInputFieldsIdx,
    }
    struct DataCollectionProcessedInfo<AuthEntry> {
        value_input_fields_idx: Vec<usize>,
        rel_specific: Option<RelationshipProcessedInfo<AuthEntry>>,
    }
    let data_collection_processed_info = std::iter::zip(data_coll_inputs, graph_elem_types.iter())
        .map(|(data_coll_input, graph_elem_type)| -> Result<_> {
            let processed_info = match data_coll_input.mapping {
                GraphElementMapping::Node(_) => {
                    let input_fields_idx = node_schema_builders
                        .entry(graph_elem_type.clone())
                        .or_insert_with_key(|graph_elem| {
                            GraphElementSchemaBuilder::new(graph_elem.typ.clone())
                        })
                        .merge(
                            data_coll_input
                                .key_fields_schema
                                .into_iter()
                                .enumerate()
                                .collect(),
                            data_coll_input
                                .value_fields_schema
                                .into_iter()
                                .enumerate()
                                .collect(),
                        )?;

                    if !(0..input_fields_idx.key.len())
                        .into_iter()
                        .eq(input_fields_idx.key.into_iter())
                    {
                        return Err(invariance_violation());
                    }
                    DataCollectionProcessedInfo {
                        value_input_fields_idx: input_fields_idx.value,
                        rel_specific: None,
                    }
                }
                GraphElementMapping::Relationship(rel_spec) => {
                    let mut src_analyzer = DependentNodeLabelAnalyzer::new(
                        data_coll_input.auth_ref,
                        &rel_spec.source,
                        &primary_key_fields_map,
                    )?;
                    let mut tgt_analyzer = DependentNodeLabelAnalyzer::new(
                        data_coll_input.auth_ref,
                        &rel_spec.target,
                        &primary_key_fields_map,
                    )?;

                    let mut value_fields_schema = vec![];
                    let mut value_input_fields_idx = vec![];
                    for (field_idx, field_schema) in
                        data_coll_input.value_fields_schema.into_iter().enumerate()
                    {
                        if !src_analyzer.process_field(field_idx, &field_schema)
                            && !tgt_analyzer.process_field(field_idx, &field_schema)
                        {
                            value_fields_schema.push(field_schema.clone());
                            value_input_fields_idx.push(field_idx);
                        }
                    }

                    let rel_schema = GraphElementSchema {
                        elem_type: graph_elem_type.typ.clone(),
                        key_fields: data_coll_input.key_fields_schema,
                        value_fields: value_fields_schema,
                    };
                    let (source_typ, source_fields_idx) =
                        src_analyzer.build(&mut node_schema_builders)?;
                    let (target_typ, target_fields_idx) =
                        tgt_analyzer.build(&mut node_schema_builders)?;
                    DataCollectionProcessedInfo {
                        value_input_fields_idx,
                        rel_specific: Some(RelationshipProcessedInfo {
                            rel_schema,
                            source_typ,
                            source_fields_idx,
                            target_typ,
                            target_fields_idx,
                        }),
                    }
                }
            };
            Ok(processed_info)
        })
        .collect::<Result<Vec<_>>>()?;

    let node_schemas: HashMap<GraphElementType<AuthEntry>, Arc<GraphElementSchema>> =
        node_schema_builders
            .into_iter()
            .map(|(graph_elem_type, schema_builder)| {
                Ok((graph_elem_type, Arc::new(schema_builder.build_schema()?)))
            })
            .collect::<Result<_>>()?;

    // 3. Build output
    let analyzed_data_colls: Vec<AnalyzedDataCollection> =
        std::iter::zip(data_collection_processed_info, graph_elem_types.iter())
            .map(|(processed_info, graph_elem_type)| {
                let result = match processed_info.rel_specific {
                    // Node
                    None => AnalyzedDataCollection {
                        schema: node_schemas
                            .get(graph_elem_type)
                            .ok_or_else(invariance_violation)?
                            .clone(),
                        value_fields_input_idx: processed_info.value_input_fields_idx,
                        rel: None,
                    },
                    // Relationship
                    Some(rel_info) => AnalyzedDataCollection {
                        schema: Arc::new(rel_info.rel_schema),
                        value_fields_input_idx: processed_info.value_input_fields_idx,
                        rel: Some(AnalyzedRelationshipInfo {
                            source: AnalyzedGraphElementFieldMapping {
                                schema: node_schemas
                                    .get(&rel_info.source_typ)
                                    .ok_or_else(invariance_violation)?
                                    .clone(),
                                fields_input_idx: rel_info.source_fields_idx,
                            },
                            target: AnalyzedGraphElementFieldMapping {
                                schema: node_schemas
                                    .get(&rel_info.target_typ)
                                    .ok_or_else(invariance_violation)?
                                    .clone(),
                                fields_input_idx: rel_info.target_fields_idx,
                            },
                        }),
                    },
                };
                Ok(result)
            })
            .collect::<Result<_>>()?;
    let decl_schemas: Vec<Arc<GraphElementSchema>> = decl_graph_elem_types
        .iter()
        .map(|graph_elem_type| {
            Ok(node_schemas
                .get(graph_elem_type)
                .ok_or_else(invariance_violation)?
                .clone())
        })
        .collect::<Result<_>>()?;
    Ok((analyzed_data_colls, decl_schemas))
}
