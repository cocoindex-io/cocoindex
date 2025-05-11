use crate::prelude::*;

use super::spec::{GraphDeclaration, GraphElementMapping, NodeFromFieldsSpec, TargetFieldMapping};
use crate::setup::components::{self, apply_component_changes, State};
use crate::setup::{ResourceSetupStatus, SetupChangeType};
use crate::{ops::sdk::*, setup::CombinedState};

use indoc::formatdoc;
use neo4rs::{BoltType, ConfigBuilder, Graph};
use std::fmt::Write;
use tokio::sync::OnceCell;

const DEFAULT_DB: &str = "neo4j";

#[derive(Debug, Deserialize, Clone)]
pub struct ConnectionSpec {
    uri: String,
    user: String,
    password: String,
    db: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Spec {
    connection: spec::AuthEntryReference<ConnectionSpec>,
    mapping: GraphElementMapping,
}

#[derive(Debug, Deserialize)]
pub struct Declaration {
    connection: spec::AuthEntryReference<ConnectionSpec>,
    #[serde(flatten)]
    decl: GraphDeclaration,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct GraphKey {
    uri: String,
    db: String,
}

impl GraphKey {
    fn from_spec(spec: &ConnectionSpec) -> Self {
        Self {
            uri: spec.uri.clone(),
            db: spec.db.clone().unwrap_or_else(|| DEFAULT_DB.to_string()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
enum ElementType {
    Node(String),
    Relationship(String),
}

impl ElementType {
    fn label(&self) -> &str {
        match self {
            ElementType::Node(label) => label,
            ElementType::Relationship(label) => label,
        }
    }

    fn from_mapping_spec(spec: &GraphElementMapping) -> Self {
        match spec {
            GraphElementMapping::Relationship(spec) => {
                ElementType::Relationship(spec.rel_type.clone())
            }
            GraphElementMapping::Node(spec) => ElementType::Node(spec.label.clone()),
        }
    }

    fn matcher(&self, var_name: &str) -> String {
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GraphElement {
    connection: AuthEntryReference<ConnectionSpec>,
    typ: ElementType,
}

impl GraphElement {
    fn from_spec(spec: &Spec) -> Self {
        Self {
            connection: spec.connection.clone(),
            typ: ElementType::from_mapping_spec(&spec.mapping),
        }
    }
}

impl retriable::IsRetryable for neo4rs::Error {
    fn is_retryable(&self) -> bool {
        match self {
            neo4rs::Error::ConnectionError => true,
            neo4rs::Error::Neo4j(e) => e.kind() == neo4rs::Neo4jErrorKind::Transient,
            _ => false,
        }
    }
}

#[derive(Default)]
pub struct GraphPool {
    graphs: Mutex<HashMap<GraphKey, Arc<OnceCell<Arc<Graph>>>>>,
}

impl GraphPool {
    pub async fn get_graph(&self, spec: &ConnectionSpec) -> Result<Arc<Graph>> {
        let graph_key = GraphKey::from_spec(spec);
        let cell = {
            let mut graphs = self.graphs.lock().unwrap();
            graphs.entry(graph_key).or_default().clone()
        };
        let graph = cell
            .get_or_try_init(|| async {
                let mut config_builder = ConfigBuilder::default()
                    .uri(spec.uri.clone())
                    .user(spec.user.clone())
                    .password(spec.password.clone());
                if let Some(db) = &spec.db {
                    config_builder = config_builder.db(db.clone());
                }
                anyhow::Ok(Arc::new(Graph::connect(config_builder.build()?).await?))
            })
            .await?;
        Ok(graph.clone())
    }
}

#[derive(Debug, Clone)]
struct AnalyzedGraphFieldMapping {
    field_idx: usize,
    field_name: String,
    value_type: schema::ValueType,
}

struct AnalyzedNodeLabelInfo {
    key_fields: Vec<AnalyzedGraphFieldMapping>,
    value_fields: Vec<AnalyzedGraphFieldMapping>,
}

pub struct ExportContext {
    connection_ref: AuthEntryReference<ConnectionSpec>,
    graph: Arc<Graph>,

    create_order: u8,

    delete_cypher: String,
    insert_cypher: String,
    delete_before_upsert: bool,

    key_field_params: Vec<String>,
    key_fields: Vec<FieldSchema>,
    value_fields: Vec<AnalyzedGraphFieldMapping>,

    src_fields: Option<AnalyzedNodeLabelInfo>,
    src_key_field_params: Vec<String>,

    tgt_fields: Option<AnalyzedNodeLabelInfo>,
    tgt_key_field_params: Vec<String>,
}

fn json_value_to_bolt_value(value: &serde_json::Value) -> Result<BoltType> {
    let bolt_value = match value {
        serde_json::Value::Null => BoltType::Null(neo4rs::BoltNull),
        serde_json::Value::Bool(v) => BoltType::Boolean(neo4rs::BoltBoolean::new(*v)),
        serde_json::Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                BoltType::Integer(neo4rs::BoltInteger::new(i))
            } else if let Some(f) = v.as_f64() {
                BoltType::Float(neo4rs::BoltFloat::new(f))
            } else {
                anyhow::bail!("Unsupported JSON number: {}", v)
            }
        }
        serde_json::Value::String(v) => BoltType::String(neo4rs::BoltString::new(v)),
        serde_json::Value::Array(v) => BoltType::List(neo4rs::BoltList {
            value: v
                .iter()
                .map(json_value_to_bolt_value)
                .collect::<Result<_>>()?,
        }),
        serde_json::Value::Object(v) => BoltType::Map(neo4rs::BoltMap {
            value: v
                .into_iter()
                .map(|(k, v)| Ok((neo4rs::BoltString::new(k), json_value_to_bolt_value(v)?)))
                .collect::<Result<_>>()?,
        }),
    };
    Ok(bolt_value)
}

fn key_to_bolt(key: &KeyValue, schema: &schema::ValueType) -> Result<BoltType> {
    value_to_bolt(&key.into(), schema)
}

fn field_values_to_bolt<'a>(
    field_values: impl IntoIterator<Item = &'a value::Value>,
    schema: impl IntoIterator<Item = &'a schema::FieldSchema>,
) -> Result<BoltType> {
    let bolt_value = BoltType::Map(neo4rs::BoltMap {
        value: std::iter::zip(schema, field_values)
            .map(|(schema, value)| {
                Ok((
                    neo4rs::BoltString::new(&schema.name),
                    value_to_bolt(value, &schema.value_type.typ)?,
                ))
            })
            .collect::<Result<_>>()?,
    });
    Ok(bolt_value)
}

fn mapped_field_values_to_bolt<'a>(
    field_values: impl IntoIterator<Item = &'a value::Value>,
    field_mappings: impl IntoIterator<Item = &'a AnalyzedGraphFieldMapping>,
) -> Result<BoltType> {
    let bolt_value = BoltType::Map(neo4rs::BoltMap {
        value: std::iter::zip(field_mappings, field_values)
            .map(|(mapping, value)| {
                Ok((
                    neo4rs::BoltString::new(&mapping.field_name),
                    value_to_bolt(value, &mapping.value_type)?,
                ))
            })
            .collect::<Result<_>>()?,
    });
    Ok(bolt_value)
}

fn basic_value_to_bolt(value: &BasicValue, schema: &BasicValueType) -> Result<BoltType> {
    let bolt_value = match value {
        BasicValue::Bytes(v) => {
            BoltType::Bytes(neo4rs::BoltBytes::new(bytes::Bytes::from_owner(v.clone())))
        }
        BasicValue::Str(v) => BoltType::String(neo4rs::BoltString::new(v)),
        BasicValue::Bool(v) => BoltType::Boolean(neo4rs::BoltBoolean::new(*v)),
        BasicValue::Int64(v) => BoltType::Integer(neo4rs::BoltInteger::new(*v)),
        BasicValue::Float64(v) => BoltType::Float(neo4rs::BoltFloat::new(*v)),
        BasicValue::Float32(v) => BoltType::Float(neo4rs::BoltFloat::new(*v as f64)),
        BasicValue::Range(v) => BoltType::List(neo4rs::BoltList {
            value: [
                BoltType::Integer(neo4rs::BoltInteger::new(v.start as i64)),
                BoltType::Integer(neo4rs::BoltInteger::new(v.end as i64)),
            ]
            .into(),
        }),
        BasicValue::Uuid(v) => BoltType::String(neo4rs::BoltString::new(&v.to_string())),
        BasicValue::Date(v) => BoltType::Date(neo4rs::BoltDate::from(*v)),
        BasicValue::Time(v) => BoltType::LocalTime(neo4rs::BoltLocalTime::from(*v)),
        BasicValue::LocalDateTime(v) => {
            BoltType::LocalDateTime(neo4rs::BoltLocalDateTime::from(*v))
        }
        BasicValue::OffsetDateTime(v) => BoltType::DateTime(neo4rs::BoltDateTime::from(*v)),
        BasicValue::Vector(v) => match schema {
            BasicValueType::Vector(t) => BoltType::List(neo4rs::BoltList {
                value: v
                    .iter()
                    .map(|v| basic_value_to_bolt(v, &t.element_type))
                    .collect::<Result<_>>()?,
            }),
            _ => anyhow::bail!("Non-vector type got vector value: {}", schema),
        },
        BasicValue::Json(v) => json_value_to_bolt_value(v)?,
    };
    Ok(bolt_value)
}

fn value_to_bolt(value: &Value, schema: &schema::ValueType) -> Result<BoltType> {
    let bolt_value = match value {
        Value::Null => BoltType::Null(neo4rs::BoltNull),
        Value::Basic(v) => match schema {
            ValueType::Basic(t) => basic_value_to_bolt(v, t)?,
            _ => anyhow::bail!("Non-basic type got basic value: {}", schema),
        },
        Value::Struct(v) => match schema {
            ValueType::Struct(t) => field_values_to_bolt(v.fields.iter(), t.fields.iter())?,
            _ => anyhow::bail!("Non-struct type got struct value: {}", schema),
        },
        Value::UTable(v) | Value::LTable(v) => match schema {
            ValueType::Table(t) => BoltType::List(neo4rs::BoltList {
                value: v
                    .iter()
                    .map(|v| field_values_to_bolt(v.0.fields.iter(), t.row.fields.iter()))
                    .collect::<Result<_>>()?,
            }),
            _ => anyhow::bail!("Non-table type got table value: {}", schema),
        },
        Value::KTable(v) => match schema {
            ValueType::Table(t) => BoltType::List(neo4rs::BoltList {
                value: v
                    .iter()
                    .map(|(k, v)| {
                        field_values_to_bolt(
                            std::iter::once(&Into::<value::Value>::into(k.clone()))
                                .chain(v.0.fields.iter()),
                            t.row.fields.iter(),
                        )
                    })
                    .collect::<Result<_>>()?,
            }),
            _ => anyhow::bail!("Non-table type got table value: {}", schema),
        },
    };
    Ok(bolt_value)
}

const CORE_KEY_PARAM_PREFIX: &str = "key";
const CORE_PROPS_PARAM: &str = "props";
const SRC_KEY_PARAM_PREFIX: &str = "source_key";
const SRC_PROPS_PARAM: &str = "source_props";
const TGT_KEY_PARAM_PREFIX: &str = "target_key";
const TGT_PROPS_PARAM: &str = "target_props";
const CORE_ELEMENT_MATCHER_VAR: &str = "e";
const SELF_CONTAINED_TAG_FIELD_NAME: &str = "__self_contained";

impl ExportContext {
    fn build_key_field_params_n_literal<'a>(
        param_prefix: &str,
        key_fields: impl Iterator<Item = &'a spec::FieldName>,
    ) -> (Vec<String>, String) {
        let (params, items): (Vec<String>, Vec<String>) = key_fields
            .into_iter()
            .enumerate()
            .map(|(i, name)| {
                let param = format!("{}_{}", param_prefix, i);
                let item = format!("{}: ${}", name, param);
                (param, item)
            })
            .unzip();
        (params, format!("{{{}}}", items.into_iter().join(", ")))
    }

    fn new(
        graph: Arc<Graph>,
        spec: Spec,
        key_fields: Vec<FieldSchema>,
        value_fields: Vec<AnalyzedGraphFieldMapping>,
        end_node_fields: Option<(AnalyzedNodeLabelInfo, AnalyzedNodeLabelInfo)>,
    ) -> Result<Self> {
        let (key_field_params, key_fields_literal) = Self::build_key_field_params_n_literal(
            CORE_KEY_PARAM_PREFIX,
            key_fields.iter().map(|f| &f.name),
        );
        let result = match spec.mapping {
            GraphElementMapping::Node(node_spec) => {
                let delete_cypher = formatdoc! {"
                    OPTIONAL MATCH (old_node:{label} {key_fields_literal})
                    WITH old_node
                    SET old_node.{SELF_CONTAINED_TAG_FIELD_NAME} = NULL
                    WITH old_node
                    WHERE NOT (old_node)--()
                    DELETE old_node
                    FINISH
                    ",
                    label = node_spec.label,
                };

                let insert_cypher = formatdoc! {"
                    MERGE (new_node:{label} {key_fields_literal})
                    SET new_node.{SELF_CONTAINED_TAG_FIELD_NAME} = TRUE{optional_set_props}
                    FINISH
                    ",
                    label = node_spec.label,
                    optional_set_props = if value_fields.is_empty() {
                        "".to_string()
                    } else {
                        format!(", new_node += ${CORE_PROPS_PARAM}\n")
                    },
                };

                Self {
                    connection_ref: spec.connection,
                    graph,
                    create_order: 0,
                    delete_cypher,
                    insert_cypher,
                    delete_before_upsert: false,
                    key_field_params,
                    key_fields,
                    value_fields,
                    src_key_field_params: vec![],
                    src_fields: None,
                    tgt_key_field_params: vec![],
                    tgt_fields: None,
                }
            }
            GraphElementMapping::Relationship(rel_spec) => {
                let delete_cypher = formatdoc! {"
                    OPTIONAL MATCH (old_src)-[old_rel:{rel_type} {key_fields_literal}]->(old_tgt)

                    DELETE old_rel

                    WITH collect(old_src) + collect(old_tgt) AS nodes_to_check
                    UNWIND nodes_to_check AS node
                    WITH DISTINCT node
                    WHERE NOT COALESCE(node.{SELF_CONTAINED_TAG_FIELD_NAME}, FALSE)
                      AND COUNT{{ (node)--() }} = 0
                    DELETE node

                    FINISH
                    ",
                    rel_type = rel_spec.rel_type,
                };

                let (src_fields, tgt_fields) = match end_node_fields {
                    Some((src_fields, tgt_fields)) => (src_fields, tgt_fields),
                    None => anyhow::bail!("Relationship spec requires source / target fields"),
                };
                let (src_key_field_params, src_key_fields_literal) =
                    Self::build_key_field_params_n_literal(
                        SRC_KEY_PARAM_PREFIX,
                        src_fields.key_fields.iter().map(|f| &f.field_name),
                    );
                let (tgt_key_field_params, tgt_key_fields_literal) =
                    Self::build_key_field_params_n_literal(
                        TGT_KEY_PARAM_PREFIX,
                        tgt_fields.key_fields.iter().map(|f| &f.field_name),
                    );

                let insert_cypher = formatdoc! {"
                    MERGE (new_src:{src_node_label} {src_key_fields_literal})
                    {optional_set_src_props}

                    MERGE (new_tgt:{tgt_node_label} {tgt_key_fields_literal})
                    {optional_set_tgt_props}

                    MERGE (new_src)-[new_rel:{rel_type} {key_fields_literal}]->(new_tgt)
                    {optional_set_rel_props}

                    FINISH
                    ",
                    src_node_label = rel_spec.source.label,
                    optional_set_src_props = if src_fields.value_fields.is_empty() {
                        "".to_string()
                    } else {
                        format!("SET new_src += ${SRC_PROPS_PARAM}\n")
                    },
                    tgt_node_label = rel_spec.target.label,
                    optional_set_tgt_props = if tgt_fields.value_fields.is_empty() {
                        "".to_string()
                    } else {
                        format!("SET new_tgt += ${TGT_PROPS_PARAM}\n")
                    },
                    rel_type = rel_spec.rel_type,
                    optional_set_rel_props = if value_fields.is_empty() {
                        "".to_string()
                    } else {
                        format!("SET new_rel += ${CORE_PROPS_PARAM}\n")
                    },
                };
                Self {
                    connection_ref: spec.connection,
                    graph,
                    create_order: 1,
                    delete_cypher,
                    insert_cypher,
                    delete_before_upsert: true,
                    key_field_params,
                    key_fields,
                    value_fields,
                    src_key_field_params,
                    src_fields: Some(src_fields),
                    tgt_key_field_params,
                    tgt_fields: Some(tgt_fields),
                }
            }
        };
        Ok(result)
    }

    fn bind_key_field_params<'a>(
        query: neo4rs::Query,
        params: &[String],
        type_val: impl Iterator<Item = (&'a schema::ValueType, &'a value::Value)>,
    ) -> Result<neo4rs::Query> {
        let mut query = query;
        for (i, (typ, val)) in type_val.enumerate() {
            query = query.param(&params[i], value_to_bolt(val, typ)?);
        }
        Ok(query)
    }

    fn bind_rel_key_field_params(
        &self,
        query: neo4rs::Query,
        val: &KeyValue,
    ) -> Result<neo4rs::Query> {
        let mut query = query;
        for (i, val) in val.fields_iter(self.key_fields.len())?.enumerate() {
            query = query.param(
                &self.key_field_params[i],
                key_to_bolt(val, &self.key_fields[i].value_type.typ)?,
            );
        }
        Ok(query)
    }

    fn add_upsert_queries(
        &self,
        upsert: &ExportTargetUpsertEntry,
        queries: &mut Vec<neo4rs::Query>,
    ) -> Result<()> {
        if self.delete_before_upsert {
            queries.push(
                self.bind_rel_key_field_params(neo4rs::query(&self.delete_cypher), &upsert.key)?,
            );
        }

        let value = &upsert.value;
        let mut insert_cypher =
            self.bind_rel_key_field_params(neo4rs::query(&self.insert_cypher), &upsert.key)?;

        if let Some(src_fields) = &self.src_fields {
            insert_cypher = Self::bind_key_field_params(
                insert_cypher,
                &self.src_key_field_params,
                src_fields
                    .key_fields
                    .iter()
                    .map(|f| (&f.value_type, &value.fields[f.field_idx])),
            )?;

            if !src_fields.value_fields.is_empty() {
                insert_cypher = insert_cypher.param(
                    SRC_PROPS_PARAM,
                    mapped_field_values_to_bolt(
                        src_fields
                            .value_fields
                            .iter()
                            .map(|f| &value.fields[f.field_idx]),
                        src_fields.value_fields.iter(),
                    )?,
                );
            }
        }

        if let Some(tgt_fields) = &self.tgt_fields {
            insert_cypher = Self::bind_key_field_params(
                insert_cypher,
                &self.tgt_key_field_params,
                tgt_fields
                    .key_fields
                    .iter()
                    .map(|f| (&f.value_type, &value.fields[f.field_idx])),
            )?;

            if !tgt_fields.value_fields.is_empty() {
                insert_cypher = insert_cypher.param(
                    TGT_PROPS_PARAM,
                    mapped_field_values_to_bolt(
                        tgt_fields
                            .value_fields
                            .iter()
                            .map(|f| &value.fields[f.field_idx]),
                        tgt_fields.value_fields.iter(),
                    )?,
                );
            }
        }

        if !self.value_fields.is_empty() {
            insert_cypher = insert_cypher.param(
                CORE_PROPS_PARAM,
                mapped_field_values_to_bolt(
                    self.value_fields.iter().map(|f| &value.fields[f.field_idx]),
                    self.value_fields.iter(),
                )?,
            );
        }
        queries.push(insert_cypher);
        Ok(())
    }

    fn add_delete_queries(
        &self,
        delete_key: &value::KeyValue,
        queries: &mut Vec<neo4rs::Query>,
    ) -> Result<()> {
        queries
            .push(self.bind_rel_key_field_params(neo4rs::query(&self.delete_cypher), delete_key)?);
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SetupState {
    key_field_names: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    dependent_node_labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    sub_components: Vec<ComponentState>,
}

impl SetupState {
    fn new(
        object_label: &ElementType,
        key_field_names: Vec<String>,
        index_options: &IndexOptions,
        field_types: &HashMap<String, schema::ValueType>,
        dependent_node_labels: Option<Vec<String>>,
    ) -> Result<Self> {
        let mut sub_components = vec![];
        sub_components.push(ComponentState {
            object_label: object_label.clone(),
            index_def: IndexDef::KeyConstraint {
                field_names: key_field_names.clone(),
            },
        });
        for index_def in index_options.vector_indexes.iter() {
            sub_components.push(ComponentState {
                object_label: object_label.clone(),
                index_def: IndexDef::from_vector_index_def(
                    index_def,
                    field_types.get(&index_def.field_name).ok_or_else(|| {
                        api_error!(
                            "Unknown field name for vector index: {}",
                            index_def.field_name
                        )
                    })?,
                )?,
            });
        }
        Ok(Self {
            key_field_names,
            dependent_node_labels: dependent_node_labels.unwrap_or_default(),
            sub_components,
        })
    }

    fn check_compatible(&self, existing: &Self) -> SetupStateCompatibility {
        if self.key_field_names == existing.key_field_names {
            SetupStateCompatibility::Compatible
        } else {
            SetupStateCompatibility::NotCompatible
        }
    }
}

impl IntoIterator for SetupState {
    type Item = ComponentState;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.sub_components.into_iter()
    }
}
#[derive(Debug, Default)]
struct DataClearAction {
    dependent_node_labels: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ComponentKind {
    KeyConstraint,
    VectorIndex,
}

impl ComponentKind {
    fn describe(&self) -> &str {
        match self {
            ComponentKind::KeyConstraint => "KEY CONSTRAINT",
            ComponentKind::VectorIndex => "VECTOR INDEX",
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ComponentKey {
    kind: ComponentKind,
    name: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
enum IndexDef {
    KeyConstraint {
        field_names: Vec<String>,
    },
    VectorIndex {
        field_name: String,
        metric: spec::VectorSimilarityMetric,
        vector_size: usize,
    },
}

impl IndexDef {
    fn from_vector_index_def(
        index_def: &spec::VectorIndexDef,
        field_typ: &schema::ValueType,
    ) -> Result<Self> {
        Ok(Self::VectorIndex {
            field_name: index_def.field_name.clone(),
            vector_size: (match field_typ {
                schema::ValueType::Basic(schema::BasicValueType::Vector(schema)) => {
                    schema.dimension
                }
                _ => None,
            })
            .ok_or_else(|| {
                api_error!("Vector index field must be a vector with fixed dimension")
            })?,
            metric: index_def.metric,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct ComponentState {
    object_label: ElementType,
    index_def: IndexDef,
}

impl components::State<ComponentKey> for ComponentState {
    fn key(&self) -> ComponentKey {
        let prefix = match &self.object_label {
            ElementType::Relationship(_) => "r",
            ElementType::Node(_) => "n",
        };
        let label = self.object_label.label();
        match &self.index_def {
            IndexDef::KeyConstraint { .. } => ComponentKey {
                kind: ComponentKind::KeyConstraint,
                name: format!("{prefix}__{label}__key"),
            },
            IndexDef::VectorIndex {
                field_name, metric, ..
            } => ComponentKey {
                kind: ComponentKind::VectorIndex,
                name: format!("{prefix}__{label}__{field_name}__{metric}__vidx"),
            },
        }
    }
}

pub struct SetupComponentOperator {
    graph_pool: Arc<GraphPool>,
    conn_spec: ConnectionSpec,
}

#[async_trait]
impl components::SetupOperator for SetupComponentOperator {
    type Key = ComponentKey;
    type State = ComponentState;
    type SetupState = SetupState;

    fn describe_key(&self, key: &Self::Key) -> String {
        format!("{} {}", key.kind.describe(), key.name)
    }

    fn describe_state(&self, state: &Self::State) -> String {
        let key_desc = self.describe_key(&state.key());
        let label = state.object_label.label();
        match &state.index_def {
            IndexDef::KeyConstraint { field_names } => {
                format!("{key_desc} ON {label} (key: {})", field_names.join(", "))
            }
            IndexDef::VectorIndex {
                field_name,
                metric,
                vector_size,
            } => {
                format!("{key_desc} ON {label} (field_name: {field_name}, vector_size: {vector_size}, metric: {metric})",)
            }
        }
    }

    fn is_up_to_date(&self, current: &ComponentState, desired: &ComponentState) -> bool {
        current == desired
    }

    async fn create(&self, state: &ComponentState) -> Result<()> {
        let graph = self.graph_pool.get_graph(&self.conn_spec).await?;
        let key = state.key();
        let qualifier = CORE_ELEMENT_MATCHER_VAR;
        let matcher = state.object_label.matcher(qualifier);
        let query = neo4rs::query(&match &state.index_def {
            IndexDef::KeyConstraint { field_names } => {
                let key_type = match &state.object_label {
                    ElementType::Node(_) => "NODE",
                    ElementType::Relationship(_) => "RELATIONSHIP",
                };
                format!(
                    "CREATE CONSTRAINT {name} IF NOT EXISTS FOR {matcher} REQUIRE {field_names} IS {key_type} KEY",
                    name=key.name,
                    field_names=build_composite_field_names(qualifier, &field_names),
                )
            }
            IndexDef::VectorIndex {
                field_name,
                metric,
                vector_size,
            } => {
                formatdoc! {"
                    CREATE VECTOR INDEX {name} IF NOT EXISTS
                    FOR {matcher} ON {qualifier}.{field_name}
                    OPTIONS {{
                        indexConfig: {{
                            `vector.dimensions`: {vector_size},
                            `vector.similarity_function`: '{metric}'
                        }}
                    }}",
                    name = key.name,
                }
            }
        });
        Ok(graph.run(query).await?)
    }

    async fn delete(&self, key: &ComponentKey) -> Result<()> {
        let graph = self.graph_pool.get_graph(&self.conn_spec).await?;
        let query = neo4rs::query(&format!(
            "DROP {kind} {name} IF EXISTS",
            kind = match key.kind {
                ComponentKind::KeyConstraint => "CONSTRAINT",
                ComponentKind::VectorIndex => "INDEX",
            },
            name = key.name,
        ));
        Ok(graph.run(query).await?)
    }
}

fn build_composite_field_names(qualifier: &str, field_names: &[String]) -> String {
    let strs = field_names
        .iter()
        .map(|name| format!("{qualifier}.{name}"))
        .join(", ");
    if field_names.len() == 1 {
        strs
    } else {
        format!("({})", strs)
    }
}
#[derive(Debug)]
pub struct GraphElementDataSetupStatus {
    key: GraphElement,
    conn_spec: ConnectionSpec,
    data_clear: Option<DataClearAction>,
    change_type: SetupChangeType,
}

impl GraphElementDataSetupStatus {
    fn new(
        key: GraphElement,
        conn_spec: ConnectionSpec,
        desired_state: Option<&SetupState>,
        existing: &CombinedState<SetupState>,
    ) -> Self {
        let mut data_clear: Option<DataClearAction> = None;
        for v in existing.possible_versions() {
            if desired_state.as_ref().is_none_or(|desired| {
                desired.check_compatible(v) == SetupStateCompatibility::NotCompatible
            }) {
                data_clear
                    .get_or_insert_default()
                    .dependent_node_labels
                    .extend(v.dependent_node_labels.iter().cloned());
            }
        }

        let change_type = match (desired_state, existing.possible_versions().next()) {
            (Some(_), Some(_)) => {
                if data_clear.is_none() {
                    SetupChangeType::NoChange
                } else {
                    SetupChangeType::Update
                }
            }
            (Some(_), None) => SetupChangeType::Create,
            (None, Some(_)) => SetupChangeType::Delete,
            (None, None) => SetupChangeType::NoChange,
        };

        Self {
            key,
            conn_spec,
            data_clear,
            change_type,
        }
    }
}

impl ResourceSetupStatus for GraphElementDataSetupStatus {
    fn describe_changes(&self) -> Vec<String> {
        let mut result = vec![];
        if let Some(data_clear) = &self.data_clear {
            let mut desc = "Clear data".to_string();
            if !data_clear.dependent_node_labels.is_empty() {
                write!(
                    &mut desc,
                    "; dependents {}",
                    data_clear
                        .dependent_node_labels
                        .iter()
                        .map(|l| format!("{}", ElementType::Node(l.clone())))
                        .join(", ")
                )
                .unwrap();
            }
            result.push(desc);
        }
        result
    }

    fn change_type(&self) -> SetupChangeType {
        self.change_type
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

async fn clear_graph_element_data(
    graph: &Graph,
    key: &GraphElement,
    is_self_contained: bool,
) -> Result<()> {
    let var_name = CORE_ELEMENT_MATCHER_VAR;
    let matcher = key.typ.matcher(var_name);
    let query_string = match key.typ {
        ElementType::Node(_) => {
            let optional_reset_self_contained = if is_self_contained {
                formatdoc! {"
                    WITH {var_name}
                    SET {var_name}.{SELF_CONTAINED_TAG_FIELD_NAME} = NULL
                "}
            } else {
                "".to_string()
            };
            formatdoc! {"
            CALL {{
                MATCH {matcher}
                {optional_reset_self_contained}
                WITH {var_name} WHERE NOT ({var_name})--() DELETE {var_name}
            }} IN TRANSACTIONS
            "}
        }
        ElementType::Relationship(_) => {
            formatdoc! {"
            CALL {{
                MATCH {matcher} WITH {var_name} DELETE {var_name}
            }} IN TRANSACTIONS
            "}
        }
    };
    let delete_query = neo4rs::query(&query_string);
    graph.run(delete_query).await?;
    Ok(())
}

/// Factory for Neo4j relationships
pub struct Factory {
    graph_pool: Arc<GraphPool>,
}

impl Factory {
    pub fn new() -> Self {
        Self {
            graph_pool: Arc::default(),
        }
    }
}

struct DependentNodeLabelAnalyzer<'a> {
    label_name: &'a str,
    fields: IndexMap<&'a str, AnalyzedGraphFieldMapping>,
    remaining_fields: HashMap<&'a str, &'a TargetFieldMapping>,
    primary_key_fields: &'a Vec<String>,
}

impl<'a> DependentNodeLabelAnalyzer<'a> {
    fn new(
        rel_end_spec: &'a NodeFromFieldsSpec,
        index_options_map: &'a HashMap<String, IndexOptions>,
    ) -> Result<Self> {
        Ok(Self {
            label_name: rel_end_spec.label.as_str(),
            fields: IndexMap::new(),
            remaining_fields: rel_end_spec
                .fields
                .iter()
                .map(|f| (f.source.as_str(), f))
                .collect(),
            primary_key_fields: index_options_map
                .get(&rel_end_spec.label)
                .and_then(|o| o.primary_key_fields.as_ref())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "No key fields specified for Node label `{}`",
                        rel_end_spec.label
                    )
                })?,
        })
    }

    fn process_field(&mut self, field_idx: usize, field_schema: &FieldSchema) -> bool {
        let field_mapping = match self.remaining_fields.remove(field_schema.name.as_str()) {
            Some(field_mapping) => field_mapping,
            None => return false,
        };
        self.fields.insert(
            field_mapping.get_target().as_str(),
            AnalyzedGraphFieldMapping {
                field_idx,
                field_name: field_mapping.get_target().clone(),
                value_type: field_schema.value_type.typ.clone(),
            },
        );
        true
    }

    fn build(
        self,
        label_value_field_types: &mut HashMap<String, HashMap<String, schema::ValueType>>,
    ) -> Result<AnalyzedNodeLabelInfo> {
        if !self.remaining_fields.is_empty() {
            anyhow::bail!(
                "Fields not mapped for  Node label `{}`: {}",
                self.label_name,
                self.remaining_fields.keys().join(", ")
            );
        }
        let mut fields = self.fields;
        let key_fields = self
            .primary_key_fields
            .iter()
            .map(|key_field| {
                let e = fields.shift_remove(key_field.as_str()).ok_or_else(|| {
                    anyhow!(
                        "Key field `{}` not mapped in Node label `{}`",
                        key_field,
                        self.label_name
                    )
                })?;
                Ok(e)
            })
            .collect::<Result<Vec<_>>>()?;
        label_value_field_types
            .entry(self.label_name.to_string())
            .or_insert_with(HashMap::new)
            .extend(
                fields
                    .values()
                    .map(|f| (f.field_name.clone(), f.value_type.clone())),
            );
        Ok(AnalyzedNodeLabelInfo {
            key_fields,
            value_fields: fields.into_values().collect(),
        })
    }
}

#[async_trait]
impl StorageFactoryBase for Factory {
    type Spec = Spec;
    type DeclarationSpec = Declaration;
    type SetupState = SetupState;
    type SetupStatus = (
        GraphElementDataSetupStatus,
        components::SetupStatus<SetupComponentOperator>,
    );
    type Key = GraphElement;
    type ExportContext = ExportContext;

    fn name(&self) -> &str {
        "Neo4j"
    }

    fn build(
        self: Arc<Self>,
        data_collections: Vec<TypedExportDataCollectionSpec<Self>>,
        declarations: Vec<Declaration>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        Vec<TypedExportDataCollectionBuildOutput<Self>>,
        Vec<(GraphElement, SetupState)>,
    )> {
        let node_labels_index_options = data_collections
            .iter()
            .filter_map(|d| match &d.spec.mapping {
                GraphElementMapping::Node(n) => Some((n.label.clone(), d.index_options.clone())),
                _ => None,
            })
            .chain(
                declarations
                    .iter()
                    .map(|n| (n.decl.nodes_label.clone(), n.decl.index_options.clone())),
            )
            .collect::<HashMap<String, IndexOptions>>();
        let mut label_value_field_types =
            HashMap::<String, HashMap<String, schema::ValueType>>::new();
        let data_coll_output = data_collections
            .into_iter()
            .map(|d| {
                let setup_key = GraphElement::from_spec(&d.spec);

                let (value_fields_info, rel_end_label_info, dependent_node_labels) = match &d
                    .spec
                    .mapping
                {
                    GraphElementMapping::Node(_) => (
                        d.value_fields_schema
                            .iter()
                            .enumerate()
                            .map(|(field_idx, field_schema)| AnalyzedGraphFieldMapping {
                                field_idx,
                                field_name: field_schema.name.clone(),
                                value_type: field_schema.value_type.typ.clone(),
                            })
                            .collect(),
                        None,
                        None,
                    ),
                    GraphElementMapping::Relationship(rel_spec) => {
                        let mut src_label_analyzer = DependentNodeLabelAnalyzer::new(
                            &rel_spec.source,
                            &node_labels_index_options,
                        )?;
                        let mut tgt_label_analyzer = DependentNodeLabelAnalyzer::new(
                            &rel_spec.target,
                            &node_labels_index_options,
                        )?;
                        let mut value_fields_info = vec![];
                        for (field_idx, field_schema) in d.value_fields_schema.iter().enumerate() {
                            if !src_label_analyzer.process_field(field_idx, field_schema)
                                && !tgt_label_analyzer.process_field(field_idx, field_schema)
                            {
                                value_fields_info.push(AnalyzedGraphFieldMapping {
                                    field_idx,
                                    field_name: field_schema.name.clone(),
                                    value_type: field_schema.value_type.typ.clone(),
                                });
                            }
                        }
                        let src_label_info =
                            src_label_analyzer.build(&mut label_value_field_types)?;
                        let tgt_label_info =
                            tgt_label_analyzer.build(&mut label_value_field_types)?;
                        let dependent_node_labels: Vec<String> = IndexSet::<&String>::from_iter([
                            &rel_spec.source.label,
                            &rel_spec.target.label,
                        ])
                        .into_iter()
                        .cloned()
                        .collect();
                        (
                            value_fields_info,
                            Some((src_label_info, tgt_label_info)),
                            Some(dependent_node_labels),
                        )
                    }
                };

                let value_field_types = value_fields_info
                    .iter()
                    .map(|f| (f.field_name.clone(), f.value_type.clone()))
                    .collect::<HashMap<String, schema::ValueType>>();
                let desired_setup_state = SetupState::new(
                    &setup_key.typ,
                    d.key_fields_schema.iter().map(|f| f.name.clone()).collect(),
                    &d.index_options,
                    &value_field_types,
                    dependent_node_labels,
                )?;

                let conn_spec = context
                    .auth_registry
                    .get::<ConnectionSpec>(&d.spec.connection)?;
                let factory = self.clone();
                let executors = async move {
                    let graph = factory.graph_pool.get_graph(&conn_spec).await?;
                    let executor = Arc::new(ExportContext::new(
                        graph,
                        d.spec,
                        d.key_fields_schema,
                        value_fields_info,
                        rel_end_label_info,
                    )?);
                    Ok(TypedExportTargetExecutors {
                        export_context: executor,
                        query_target: None,
                    })
                }
                .boxed();
                Ok(TypedExportDataCollectionBuildOutput {
                    executors,
                    setup_key,
                    desired_setup_state,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let decl_output = declarations
            .into_iter()
            .map(|d| {
                let label_value_field_types = &label_value_field_types;
                let setup_key = GraphElement {
                    connection: d.connection.clone(),
                    typ: ElementType::Node(d.decl.nodes_label.clone()),
                };
                let primary_key_fields = d
                    .decl
                    .index_options
                    .primary_key_fields
                    .as_ref()
                    .ok_or_else(|| {
                        api_error!(
                            "No primary key fields specified for node label `{}`",
                            &d.decl.nodes_label
                        )
                    })?
                    .iter()
                    .map(|f| f.clone())
                    .collect();
                let setup_state = SetupState::new(
                    &setup_key.typ,
                    primary_key_fields,
                    &d.decl.index_options,
                    label_value_field_types
                        .get(&d.decl.nodes_label)
                        .ok_or_else(|| {
                            api_error!(
                                "Data for nodes with label `{}` not provided",
                                d.decl.nodes_label
                            )
                        })?,
                    None,
                )?;
                Ok((setup_key, setup_state))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((data_coll_output, decl_output))
    }

    async fn check_setup_status(
        &self,
        key: GraphElement,
        desired: Option<SetupState>,
        existing: CombinedState<SetupState>,
        auth_registry: &Arc<AuthRegistry>,
    ) -> Result<Self::SetupStatus> {
        let conn_spec = auth_registry.get::<ConnectionSpec>(&key.connection)?;
        let data_status =
            GraphElementDataSetupStatus::new(key, conn_spec.clone(), desired.as_ref(), &existing);
        let components = components::SetupStatus::create(
            SetupComponentOperator {
                graph_pool: self.graph_pool.clone(),
                conn_spec,
            },
            desired,
            existing,
        )?;
        Ok((data_status, components))
    }

    fn check_state_compatibility(
        &self,
        desired: &SetupState,
        existing: &SetupState,
    ) -> Result<SetupStateCompatibility> {
        Ok(desired.check_compatible(existing))
    }

    fn describe_resource(&self, key: &GraphElement) -> Result<String> {
        Ok(format!("Neo4j {}", key.typ))
    }

    async fn apply_mutation(
        &self,
        mutations: Vec<ExportTargetMutationWithContext<'async_trait, ExportContext>>,
    ) -> Result<()> {
        let mut muts_by_graph = HashMap::new();
        for mut_with_ctx in mutations.iter() {
            muts_by_graph
                .entry(&mut_with_ctx.export_context.connection_ref)
                .or_insert_with(Vec::new)
                .push(mut_with_ctx);
        }
        for muts in muts_by_graph.values_mut() {
            muts.sort_by_key(|m| m.export_context.create_order);
            let graph = &muts[0].export_context.graph;
            retriable::run(
                || async {
                    let mut queries = vec![];
                    for mut_with_ctx in muts.iter() {
                        let export_ctx = &mut_with_ctx.export_context;
                        for upsert in mut_with_ctx.mutation.upserts.iter() {
                            export_ctx.add_upsert_queries(upsert, &mut queries)?;
                        }
                    }
                    for mut_with_ctx in muts.iter().rev() {
                        let export_ctx = &mut_with_ctx.export_context;
                        for delete_key in mut_with_ctx.mutation.delete_keys.iter() {
                            export_ctx.add_delete_queries(delete_key, &mut queries)?;
                        }
                    }
                    let mut txn = graph.start_txn().await?;
                    txn.run_queries(queries).await?;
                    txn.commit().await?;
                    retriable::Ok(())
                },
                retriable::RunOptions::default(),
            )
            .await
            .map_err(Into::<anyhow::Error>::into)?
        }
        Ok(())
    }

    async fn apply_setup_changes(
        &self,
        changes: Vec<&'async_trait Self::SetupStatus>,
    ) -> Result<()> {
        let (data_statuses, components): (Vec<_>, Vec<_>) =
            changes.into_iter().map(|c| (&c.0, &c.1)).unzip();

        // Relationships first, then nodes, as relationships need to be deleted before nodes they referenced.
        let mut relationship_types = IndexMap::<&GraphElement, &ConnectionSpec>::new();
        let mut node_labels = IndexMap::<&GraphElement, &ConnectionSpec>::new();
        let mut dependent_node_labels = IndexMap::<GraphElement, &ConnectionSpec>::new();
        for data_status in data_statuses.iter() {
            if let Some(data_clear) = &data_status.data_clear {
                match &data_status.key.typ {
                    ElementType::Relationship(_) => {
                        relationship_types.insert(&data_status.key, &data_status.conn_spec);
                        for label in &data_clear.dependent_node_labels {
                            dependent_node_labels.insert(
                                GraphElement {
                                    connection: data_status.key.connection.clone(),
                                    typ: ElementType::Node(label.clone()),
                                },
                                &data_status.conn_spec,
                            );
                        }
                    }
                    ElementType::Node(_) => {
                        node_labels.insert(&data_status.key, &data_status.conn_spec);
                    }
                }
            }
        }

        // Relationships have no dependency, so can be cleared first.
        for (rel_type, conn_spec) in relationship_types.iter() {
            let graph = self.graph_pool.get_graph(conn_spec).await?;
            clear_graph_element_data(&graph, rel_type, true).await?;
        }
        // Clear standalone nodes, which is simpler than dependent nodes.
        for (node_label, conn_spec) in node_labels.iter() {
            let graph = self.graph_pool.get_graph(conn_spec).await?;
            clear_graph_element_data(&graph, node_label, true).await?;
        }
        // Clear dependent nodes if they're not covered by standalone nodes.
        for (node_label, conn_spec) in dependent_node_labels.iter() {
            if !node_labels.contains_key(node_label) {
                let graph = self.graph_pool.get_graph(conn_spec).await?;
                clear_graph_element_data(&graph, node_label, false).await?;
            }
        }

        apply_component_changes(components).await?;
        Ok(())
    }
}
