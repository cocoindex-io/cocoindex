use crate::prelude::*;
use crate::setup::components::{self, State};
use crate::setup::{ResourceSetupStatusCheck, SetupChangeType};
use crate::{ops::sdk::*, setup::CombinedState};

use neo4rs::{BoltType, ConfigBuilder, Graph};
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
pub struct FieldMapping {
    field_name: FieldName,

    /// Field name for the node in the Knowledge Graph.
    /// If unspecified, it's the same as `field_name`.
    #[serde(default)]
    node_field_name: Option<FieldName>,
}

impl FieldMapping {
    fn get_node_field_name(&self) -> &FieldName {
        self.node_field_name.as_ref().unwrap_or(&self.field_name)
    }
}

#[derive(Debug, Deserialize)]
pub struct RelationshipEndSpec {
    label: String,
    fields: Vec<FieldMapping>,
}

#[derive(Debug, Deserialize)]
pub struct RelationshipNodeSpec {
    #[serde(flatten)]
    index_options: spec::IndexOptions,
}

#[derive(Debug, Deserialize)]
pub struct RelationshipSpec {
    connection: AuthEntryReference,
    rel_type: String,
    source: RelationshipEndSpec,
    target: RelationshipEndSpec,
    nodes: BTreeMap<String, RelationshipNodeSpec>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GraphRelationship {
    connection: AuthEntryReference,
    relationship: String,
}

impl GraphRelationship {
    fn from_spec(spec: &RelationshipSpec) -> Self {
        Self {
            connection: spec.connection.clone(),
            relationship: spec.rel_type.clone(),
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
pub struct RelationshipExportContext {
    connection_ref: AuthEntryReference,
    graph: Arc<Graph>,

    delete_cypher: String,
    insert_cypher: String,

    key_field_params: Vec<String>,
    key_fields: Vec<FieldSchema>,
    value_fields: Vec<AnalyzedGraphFieldMapping>,

    src_key_field_params: Vec<String>,
    src_fields: AnalyzedNodeLabelInfo,

    tgt_key_field_params: Vec<String>,
    tgt_fields: AnalyzedNodeLabelInfo,
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
        Value::Collection(v) | Value::List(v) => match schema {
            ValueType::Collection(t) => BoltType::List(neo4rs::BoltList {
                value: v
                    .iter()
                    .map(|v| field_values_to_bolt(v.0.fields.iter(), t.row.fields.iter()))
                    .collect::<Result<_>>()?,
            }),
            _ => anyhow::bail!("Non-collection type got collection value: {}", schema),
        },
        Value::Table(v) => match schema {
            ValueType::Collection(t) => BoltType::List(neo4rs::BoltList {
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

const REL_KEY_PARAM_PREFIX: &str = "rel_key";
const REL_PROPS_PARAM: &str = "rel_props";
const SRC_KEY_PARAM_PREFIX: &str = "source_key";
const SRC_PROPS_PARAM: &str = "source_props";
const TGT_KEY_PARAM_PREFIX: &str = "target_key";
const TGT_PROPS_PARAM: &str = "target_props";

impl RelationshipExportContext {
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
        spec: RelationshipSpec,
        key_fields: Vec<FieldSchema>,
        value_fields: Vec<AnalyzedGraphFieldMapping>,
        src_fields: AnalyzedNodeLabelInfo,
        tgt_fields: AnalyzedNodeLabelInfo,
    ) -> Result<Self> {
        let (key_field_params, key_fields_literal) = Self::build_key_field_params_n_literal(
            REL_KEY_PARAM_PREFIX,
            key_fields.iter().map(|f| &f.name),
        );
        let (src_key_field_params, src_key_fields_literal) = Self::build_key_field_params_n_literal(
            SRC_KEY_PARAM_PREFIX,
            src_fields.key_fields.iter().map(|f| &f.field_name),
        );
        let (tgt_key_field_params, tgt_key_fields_literal) = Self::build_key_field_params_n_literal(
            TGT_KEY_PARAM_PREFIX,
            tgt_fields.key_fields.iter().map(|f| &f.field_name),
        );

        let delete_cypher = format!(
            r#"
OPTIONAL MATCH (old_src)-[old_rel:{rel_type} {key_fields_literal}]->(old_tgt)

DELETE old_rel

WITH old_src, old_tgt
CALL {{
  WITH old_src
  OPTIONAL MATCH (old_src)-[r]-()
  WITH old_src, count(r) AS rels
  WHERE rels = 0
  DELETE old_src
  RETURN 0 AS _1
}}

CALL {{
  WITH old_tgt
  OPTIONAL MATCH (old_tgt)-[r]-()
  WITH old_tgt, count(r) AS rels
  WHERE rels = 0
  DELETE old_tgt
  RETURN 0 AS _2
}}            

FINISH
            "#,
            rel_type = spec.rel_type,
        );

        let insert_cypher = format!(
            r#"
MERGE (new_src:{src_node_label} {src_key_fields_literal})
{optional_set_src_props}

MERGE (new_tgt:{tgt_node_label} {tgt_key_fields_literal})
{optional_set_tgt_props}

MERGE (new_src)-[new_rel:{rel_type} {key_fields_literal}]->(new_tgt)
{optional_set_rel_props}

FINISH
            "#,
            src_node_label = spec.source.label,
            optional_set_src_props = if src_fields.value_fields.is_empty() {
                "".to_string()
            } else {
                format!("SET new_src += ${SRC_PROPS_PARAM}\n")
            },
            tgt_node_label = spec.target.label,
            optional_set_tgt_props = if tgt_fields.value_fields.is_empty() {
                "".to_string()
            } else {
                format!("SET new_tgt += ${TGT_PROPS_PARAM}\n")
            },
            rel_type = spec.rel_type,
            optional_set_rel_props = if value_fields.is_empty() {
                "".to_string()
            } else {
                format!("SET new_rel += ${REL_PROPS_PARAM}\n")
            },
        );
        Ok(Self {
            connection_ref: spec.connection,
            graph,
            delete_cypher,
            insert_cypher,
            key_field_params,
            key_fields,
            value_fields,
            src_key_field_params,
            src_fields,
            tgt_key_field_params,
            tgt_fields,
        })
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
        queries
            .push(self.bind_rel_key_field_params(neo4rs::query(&self.delete_cypher), &upsert.key)?);

        let value = &upsert.value;
        let mut insert_cypher =
            self.bind_rel_key_field_params(neo4rs::query(&self.insert_cypher), &upsert.key)?;
        insert_cypher = Self::bind_key_field_params(
            insert_cypher,
            &self.src_key_field_params,
            self.src_fields
                .key_fields
                .iter()
                .map(|f| (&f.value_type, &value.fields[f.field_idx])),
        )?;
        insert_cypher = Self::bind_key_field_params(
            insert_cypher,
            &self.tgt_key_field_params,
            self.tgt_fields
                .key_fields
                .iter()
                .map(|f| (&f.value_type, &value.fields[f.field_idx])),
        )?;

        if !self.src_fields.value_fields.is_empty() {
            insert_cypher = insert_cypher.param(
                SRC_PROPS_PARAM,
                mapped_field_values_to_bolt(
                    self.src_fields
                        .value_fields
                        .iter()
                        .map(|f| &value.fields[f.field_idx]),
                    self.src_fields.value_fields.iter(),
                )?,
            );
        }
        if !self.tgt_fields.value_fields.is_empty() {
            insert_cypher = insert_cypher.param(
                TGT_PROPS_PARAM,
                mapped_field_values_to_bolt(
                    self.tgt_fields
                        .value_fields
                        .iter()
                        .map(|f| &value.fields[f.field_idx]),
                    self.tgt_fields.value_fields.iter(),
                )?,
            );
        }
        if !self.value_fields.is_empty() {
            insert_cypher = insert_cypher.param(
                REL_PROPS_PARAM,
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
pub struct RelationshipSetupState {
    key_field_names: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    node_labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    sub_components: Vec<ComponentState>,
}

impl RelationshipSetupState {
    fn new(
        spec: &RelationshipSpec,
        key_field_names: Vec<String>,
        index_options: &IndexOptions,
        rel_value_fields_info: &[AnalyzedGraphFieldMapping],
        src_label_info: &AnalyzedNodeLabelInfo,
        tgt_label_info: &AnalyzedNodeLabelInfo,
    ) -> Result<Self> {
        let mut sub_components = vec![];
        sub_components.push(ComponentState {
            object_label: ObjectLabel::Relationship(spec.rel_type.clone()),
            index_def: IndexDef::KeyConstraint {
                field_names: key_field_names.clone(),
            },
        });
        for index_def in index_options.vector_indexes.iter() {
            sub_components.push(ComponentState {
                object_label: ObjectLabel::Relationship(spec.rel_type.clone()),
                index_def: IndexDef::from_vector_index_def(
                    index_def,
                    &rel_value_fields_info
                        .iter()
                        .find(|f| f.field_name == index_def.field_name)
                        .ok_or_else(|| {
                            api_error!(
                                "Unknown field name for vector index: {}",
                                index_def.field_name
                            )
                        })?
                        .value_type,
                )?,
            });
        }
        for (label, node) in spec.nodes.iter() {
            sub_components.push(ComponentState {
                object_label: ObjectLabel::Node(label.clone()),
                index_def: IndexDef::KeyConstraint {
                    field_names: key_field_names.clone(),
                },
            });
            for index_def in &node.index_options.vector_indexes {
                sub_components.push(ComponentState {
                    object_label: ObjectLabel::Node(label.clone()),
                    index_def: IndexDef::from_vector_index_def(
                        index_def,
                        [src_label_info, tgt_label_info]
                            .into_iter()
                            .flat_map(|v| v.key_fields.iter().chain(v.value_fields.iter()))
                            .find(|f| f.field_name == index_def.field_name)
                            .map(|f| &f.value_type)
                            .ok_or_else(|| {
                                api_error!(
                                    "Unknown field name for vector index: {}",
                                    index_def.field_name
                                )
                            })?,
                    )?,
                });
            }
        }
        Ok(Self {
            key_field_names,
            node_labels: spec.nodes.keys().cloned().collect(),
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

impl IntoIterator for RelationshipSetupState {
    type Item = ComponentState;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.sub_components.into_iter()
    }
}
#[derive(Debug)]
struct DataClearAction {
    rel_type: String,
    node_labels: Vec<String>,
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
struct ComponentKey {
    kind: ComponentKind,
    name: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
enum ObjectLabel {
    Node(String),
    Relationship(String),
}

impl ObjectLabel {
    fn label(&self) -> &str {
        match self {
            ObjectLabel::Node(label) => label,
            ObjectLabel::Relationship(label) => label,
        }
    }
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
    object_label: ObjectLabel,
    index_def: IndexDef,
}

impl components::State<ComponentKey> for ComponentState {
    fn key(&self) -> ComponentKey {
        let prefix = match &self.object_label {
            ObjectLabel::Relationship(_) => "r",
            ObjectLabel::Node(_) => "n",
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

struct SetupComponentOperator {
    graph_pool: Arc<GraphPool>,
    conn_spec: ConnectionSpec,
}

#[async_trait]
impl components::Operator for SetupComponentOperator {
    type Key = ComponentKey;
    type State = ComponentState;
    type SetupState = RelationshipSetupState;

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
        let (matcher, qualifier) = match &state.object_label {
            ObjectLabel::Relationship(label) => (format!("()-[r:{label}]->()"), "r"),
            ObjectLabel::Node(label) => (format!("(n:{label})"), "n"),
        };
        let query = neo4rs::query(&match &state.index_def {
            IndexDef::KeyConstraint { field_names } => {
                format!(
                    "CREATE CONSTRAINT {name} IF NOT EXISTS FOR {matcher} REQUIRE {field_names} IS UNIQUE",
                    name=key.name,
                    field_names=build_composite_field_names(qualifier, &field_names),
                )
            }
            IndexDef::VectorIndex {
                field_name,
                metric,
                vector_size,
            } => {
                format!(
                    r#"CREATE VECTOR INDEX {name} IF NOT EXISTS FOR {matcher} ON {qualifier}.{field_name} OPTIONS
                       {{ indexConfig: {{`vector.dimensions`: {vector_size}, `vector.similarity_function`: '{metric}'}}}}"#,
                    name = key.name,
                )
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
#[derive(Derivative)]
#[derivative(Debug)]
struct SetupStatusCheck {
    #[derivative(Debug = "ignore")]
    graph_pool: Arc<GraphPool>,
    conn_spec: ConnectionSpec,
    data_clear: Option<DataClearAction>,
    change_type: SetupChangeType,
}

impl SetupStatusCheck {
    fn new(
        key: GraphRelationship,
        graph_pool: Arc<GraphPool>,
        conn_spec: ConnectionSpec,
        desired_state: Option<&RelationshipSetupState>,
        existing: &CombinedState<RelationshipSetupState>,
    ) -> Self {
        let data_clear = existing
            .current
            .as_ref()
            .filter(|existing_current| {
                desired_state.as_ref().is_none_or(|desired| {
                    desired.check_compatible(existing_current)
                        == SetupStateCompatibility::NotCompatible
                })
            })
            .map(|existing_current| DataClearAction {
                rel_type: key.relationship.clone(),
                node_labels: existing_current.node_labels.clone(),
            });

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
            graph_pool,
            conn_spec,
            data_clear,
            change_type,
        }
    }
}

#[async_trait]
impl ResourceSetupStatusCheck for SetupStatusCheck {
    fn describe_changes(&self) -> Vec<String> {
        let mut result = vec![];
        if let Some(data_clear) = &self.data_clear {
            result.push(format!(
                "Clear data for relationship {}; nodes {}",
                data_clear.rel_type,
                data_clear.node_labels.iter().join(", "),
            ));
        }
        result
    }

    fn change_type(&self) -> SetupChangeType {
        self.change_type
    }

    async fn apply_change(&self) -> Result<()> {
        let graph = self.graph_pool.get_graph(&self.conn_spec).await?;
        if let Some(data_clear) = &self.data_clear {
            let delete_rel_query = neo4rs::query(&format!(
                r#"
                    CALL {{
                      MATCH ()-[r:{rel_type}]->()
                      WITH r
                      DELETE r
                    }} IN TRANSACTIONS
                "#,
                rel_type = data_clear.rel_type
            ));
            graph.run(delete_rel_query).await?;

            for node_label in &data_clear.node_labels {
                let delete_node_query = neo4rs::query(&format!(
                    r#"
                        CALL {{
                          MATCH (n:{node_label})
                          WHERE NOT (n)--()
                          DELETE n
                        }} IN TRANSACTIONS
                    "#,
                    node_label = node_label
                ));
                graph.run(delete_node_query).await?;
            }
        }
        Ok(())
    }
}
/// Factory for Neo4j relationships
pub struct RelationshipFactory {
    graph_pool: Arc<GraphPool>,
}

impl RelationshipFactory {
    pub fn new(graph_pool: Arc<GraphPool>) -> Self {
        Self { graph_pool }
    }
}

struct NodeLabelAnalyzer<'a> {
    label_name: &'a str,
    fields: IndexMap<&'a str, AnalyzedGraphFieldMapping>,
    remaining_fields: HashMap<&'a str, &'a FieldMapping>,
    index_options: &'a IndexOptions,
}

impl<'a> NodeLabelAnalyzer<'a> {
    fn new(rel_spec: &'a RelationshipSpec, rel_end_spec: &'a RelationshipEndSpec) -> Result<Self> {
        let node_spec = rel_spec.nodes.get(&rel_end_spec.label).ok_or_else(|| {
            anyhow!(
                "Node label `{}` not found in relationship spec",
                rel_end_spec.label
            )
        })?;
        Ok(Self {
            label_name: rel_end_spec.label.as_str(),
            fields: IndexMap::new(),
            remaining_fields: rel_end_spec
                .fields
                .iter()
                .map(|f| (f.field_name.as_str(), f))
                .collect(),
            index_options: &node_spec.index_options,
        })
    }

    fn process_field(&mut self, field_idx: usize, field_schema: &FieldSchema) -> bool {
        let field_info = match self.remaining_fields.remove(field_schema.name.as_str()) {
            Some(field_info) => field_info,
            None => return false,
        };
        self.fields.insert(
            field_info.get_node_field_name().as_str(),
            AnalyzedGraphFieldMapping {
                field_idx,
                field_name: field_info.get_node_field_name().clone(),
                value_type: field_schema.value_type.typ.clone(),
            },
        );
        true
    }

    fn build(self) -> Result<AnalyzedNodeLabelInfo> {
        if !self.remaining_fields.is_empty() {
            anyhow::bail!(
                "Fields not mapped for  Node label `{}`: {}",
                self.label_name,
                self.remaining_fields.keys().join(", ")
            );
        }
        let mut fields = self.fields;
        let mut key_fields = vec![];
        for key_field in self
            .index_options
            .primary_key_fields
            .iter()
            .flat_map(|f| f.iter())
        {
            let e = fields.shift_remove(key_field.as_str()).ok_or_else(|| {
                anyhow!(
                    "Key field `{}` not mapped in Node label `{}`",
                    key_field,
                    self.label_name
                )
            })?;
            key_fields.push(e);
        }
        if key_fields.is_empty() {
            anyhow::bail!(
                "No key fields specified for Node label `{}`",
                self.label_name
            );
        }
        Ok(AnalyzedNodeLabelInfo {
            key_fields,
            value_fields: fields.into_values().collect(),
        })
    }
}

#[async_trait]
impl StorageFactoryBase for RelationshipFactory {
    type Spec = RelationshipSpec;
    type SetupState = RelationshipSetupState;
    type Key = GraphRelationship;
    type ExportContext = RelationshipExportContext;

    fn name(&self) -> &str {
        "Neo4jRelationship"
    }

    fn build(
        self: Arc<Self>,
        _name: String,
        spec: RelationshipSpec,
        key_fields_schema: Vec<FieldSchema>,
        value_fields_schema: Vec<FieldSchema>,
        index_options: IndexOptions,
        context: Arc<FlowInstanceContext>,
    ) -> Result<TypedExportTargetBuildOutput<Self>> {
        let setup_key = GraphRelationship::from_spec(&spec);

        let mut src_label_analyzer = NodeLabelAnalyzer::new(&spec, &spec.source)?;
        let mut tgt_label_analyzer = NodeLabelAnalyzer::new(&spec, &spec.target)?;
        let mut rel_value_fields_info = vec![];
        for (field_idx, field_schema) in value_fields_schema.iter().enumerate() {
            if !src_label_analyzer.process_field(field_idx, field_schema)
                && !tgt_label_analyzer.process_field(field_idx, field_schema)
            {
                rel_value_fields_info.push(AnalyzedGraphFieldMapping {
                    field_idx,
                    field_name: field_schema.name.clone(),
                    value_type: field_schema.value_type.typ.clone(),
                });
            }
        }
        let src_label_info = src_label_analyzer.build()?;
        let tgt_label_info = tgt_label_analyzer.build()?;

        let desired_setup_state = RelationshipSetupState::new(
            &spec,
            key_fields_schema.iter().map(|f| f.name.clone()).collect(),
            &index_options,
            &rel_value_fields_info,
            &src_label_info,
            &tgt_label_info,
        )?;

        let conn_spec = context
            .auth_registry
            .get::<ConnectionSpec>(&spec.connection)?;
        let executors = async move {
            let graph = self.graph_pool.get_graph(&conn_spec).await?;
            let executor = Arc::new(RelationshipExportContext::new(
                graph,
                spec,
                key_fields_schema,
                rel_value_fields_info,
                src_label_info,
                tgt_label_info,
            )?);
            Ok(TypedExportTargetExecutors {
                export_context: executor,
                query_target: None,
            })
        }
        .boxed();
        Ok(TypedExportTargetBuildOutput {
            executors,
            setup_key,
            desired_setup_state,
        })
    }

    fn check_setup_status(
        &self,
        key: GraphRelationship,
        desired: Option<RelationshipSetupState>,
        existing: CombinedState<RelationshipSetupState>,
        auth_registry: &Arc<AuthRegistry>,
    ) -> Result<impl ResourceSetupStatusCheck + 'static> {
        let conn_spec = auth_registry.get::<ConnectionSpec>(&key.connection)?;
        let base = SetupStatusCheck::new(
            key,
            self.graph_pool.clone(),
            conn_spec.clone(),
            desired.as_ref(),
            &existing,
        );
        let comp = components::StatusCheck::create(
            SetupComponentOperator {
                graph_pool: self.graph_pool.clone(),
                conn_spec: conn_spec.clone(),
            },
            desired,
            existing,
        )?;
        Ok(components::combine_status_checks(base, comp))
    }

    fn check_state_compatibility(
        &self,
        desired: &RelationshipSetupState,
        existing: &RelationshipSetupState,
    ) -> Result<SetupStateCompatibility> {
        Ok(desired.check_compatible(existing))
    }

    fn describe_resource(&self, key: &GraphRelationship) -> Result<String> {
        Ok(format!("Neo4j relationship {}", key.relationship))
    }

    async fn apply_mutation(
        &self,
        mutations: Vec<ExportTargetMutationWithContext<'async_trait, RelationshipExportContext>>,
    ) -> Result<()> {
        let mut muts_by_graph = HashMap::new();
        for mut_with_ctx in mutations.iter() {
            muts_by_graph
                .entry(&mut_with_ctx.export_context.connection_ref)
                .or_insert_with(Vec::new)
                .push(mut_with_ctx);
        }
        for muts in muts_by_graph.values() {
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
                    for mut_with_ctx in muts.iter() {
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
}
