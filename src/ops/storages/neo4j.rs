use crate::prelude::*;
use crate::setup::{ResourceSetupStatusCheck, SetupChangeType};
use crate::{ops::sdk::*, setup::CombinedState};

use neo4rs::{BoltType, ConfigBuilder, Graph};
use tokio::sync::OnceCell;

const DEFAULT_DB: &str = "neo4j";

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
pub struct RelationshipEndSpec {
    label: String,
    fields: Vec<FieldMapping>,
}

#[derive(Debug, Deserialize)]
pub struct RelationshipNodeSpec {
    #[serde(default)]
    key_field_name: String,
}

#[derive(Debug, Deserialize)]
pub struct RelationshipSpec {
    connection: AuthEntryReference,
    rel_type: String,
    source: RelationshipEndSpec,
    target: RelationshipEndSpec,
    nodes: BTreeMap<String, RelationshipNodeSpec>,
}

impl RelationshipSpec {
    fn get_src_label_info(&self) -> Result<&RelationshipNodeSpec> {
        Ok(self
            .nodes
            .get(self.source.label.as_str())
            .ok_or_else(|| api_error!("Source label `{}` not found", self.source.label))?)
    }

    fn get_tgt_label_info(&self) -> Result<&RelationshipNodeSpec> {
        Ok(self
            .nodes
            .get(self.target.label.as_str())
            .ok_or_else(|| api_error!("Target label `{}` not found", self.target.label))?)
    }
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
    field_schema: FieldSchema,
}

struct AnalyzedGraphFields {
    key_field: AnalyzedGraphFieldMapping,
    value_fields: Vec<AnalyzedGraphFieldMapping>,
}
struct RelationshipStorageExecutor {
    graph: Arc<Graph>,
    delete_cypher: String,
    insert_cypher: String,

    key_field: FieldSchema,
    value_fields: Vec<AnalyzedGraphFieldMapping>,

    src_fields: AnalyzedGraphFields,
    tgt_fields: AnalyzedGraphFields,
}

fn json_value_to_bolt_value(value: &serde_json::Value) -> Result<BoltType> {
    let bolt_value = match value {
        serde_json::Value::Null => BoltType::Null(neo4rs::BoltNull::default()),
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
                .into_iter()
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

fn basic_value_to_bolt(value: &BasicValue, schema: &BasicValueType) -> Result<BoltType> {
    let bolt_value = match value {
        BasicValue::Bytes(v) => {
            BoltType::Bytes(neo4rs::BoltBytes::new(bytes::Bytes::from_owner(v.clone())))
        }
        BasicValue::Str(v) => BoltType::String(neo4rs::BoltString::new(&v)),
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
                    .into_iter()
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
        Value::Null => BoltType::Null(neo4rs::BoltNull::default()),
        Value::Basic(v) => match schema {
            ValueType::Basic(t) => basic_value_to_bolt(v, &t)?,
            _ => anyhow::bail!("Non-basic type got basic value: {}", schema),
        },
        Value::Struct(v) => match schema {
            ValueType::Struct(t) => field_values_to_bolt(v.fields.iter(), t.fields.iter())?,
            _ => anyhow::bail!("Non-struct type got struct value: {}", schema),
        },
        Value::Collection(v) | Value::List(v) => match schema {
            ValueType::Collection(t) => BoltType::List(neo4rs::BoltList {
                value: v
                    .into_iter()
                    .map(|v| field_values_to_bolt(v.0.fields.iter(), t.row.fields.iter()))
                    .collect::<Result<_>>()?,
            }),
            _ => anyhow::bail!("Non-collection type got collection value: {}", schema),
        },
        Value::Table(v) => match schema {
            ValueType::Collection(t) => BoltType::List(neo4rs::BoltList {
                value: v
                    .into_iter()
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

const REL_ID_PARAM: &str = "rel_id";
const REL_PROPS_PARAM: &str = "rel_props";
const SRC_ID_PARAM: &str = "source_id";
const SRC_PROPS_PARAM: &str = "source_props";
const TGT_ID_PARAM: &str = "target_id";
const TGT_PROPS_PARAM: &str = "target_props";

impl RelationshipStorageExecutor {
    fn new(
        graph: Arc<Graph>,
        spec: RelationshipSpec,
        key_field: FieldSchema,
        value_fields: Vec<AnalyzedGraphFieldMapping>,
        src_fields: AnalyzedGraphFields,
        tgt_fields: AnalyzedGraphFields,
    ) -> Result<Self> {
        let delete_cypher = format!(
            r#"
OPTIONAL MATCH (old_src)-[old_rel:{rel_type} {{{rel_key_field_name}: ${REL_ID_PARAM}}}]->(old_tgt)

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
            rel_key_field_name = key_field.name,
        );

        let insert_cypher = format!(
            r#"
MERGE (new_src:{src_node_label} {{{src_node_key_field_name}: ${SRC_ID_PARAM}}})
{optional_set_src_props}

MERGE (new_tgt:{tgt_node_label} {{{tgt_node_key_field_name}: ${TGT_ID_PARAM}}})
{optional_set_tgt_props}

MERGE (new_src)-[new_rel:{rel_type} {{{rel_key_field_name}: ${REL_ID_PARAM}}}]->(new_tgt)
{optional_set_rel_props}

FINISH
            "#,
            src_node_label = spec.source.label,
            src_node_key_field_name = spec.get_src_label_info()?.key_field_name,
            optional_set_src_props = if src_fields.value_fields.is_empty() {
                "".to_string()
            } else {
                format!("SET new_src += ${SRC_PROPS_PARAM}\n")
            },
            tgt_node_label = spec.target.label,
            tgt_node_key_field_name = spec.get_tgt_label_info()?.key_field_name,
            optional_set_tgt_props = if tgt_fields.value_fields.is_empty() {
                "".to_string()
            } else {
                format!("SET new_tgt += ${TGT_PROPS_PARAM}\n")
            },
            rel_type = spec.rel_type,
            rel_key_field_name = key_field.name,
            optional_set_rel_props = if value_fields.is_empty() {
                "".to_string()
            } else {
                format!("SET new_rel += ${REL_PROPS_PARAM}\n")
            },
        );
        Ok(Self {
            graph,
            delete_cypher,
            insert_cypher,
            key_field,
            value_fields,
            src_fields,
            tgt_fields,
        })
    }

    fn build_queries_to_apply_mutation(
        &self,
        mutation: &ExportTargetMutation,
    ) -> Result<Vec<neo4rs::Query>> {
        let mut queries = vec![];
        for upsert in mutation.upserts.iter() {
            let rel_id_bolt = key_to_bolt(&upsert.key, &self.key_field.value_type.typ)?;
            queries
                .push(neo4rs::query(&self.delete_cypher).param(REL_ID_PARAM, rel_id_bolt.clone()));

            let value = &upsert.value;
            let mut insert_cypher = neo4rs::query(&self.insert_cypher)
                .param(REL_ID_PARAM, rel_id_bolt)
                .param(
                    SRC_ID_PARAM,
                    value_to_bolt(
                        &value.fields[self.src_fields.key_field.field_idx],
                        &self.src_fields.key_field.field_schema.value_type.typ,
                    )?,
                )
                .param(
                    TGT_ID_PARAM,
                    value_to_bolt(
                        &value.fields[self.tgt_fields.key_field.field_idx],
                        &self.tgt_fields.key_field.field_schema.value_type.typ,
                    )?,
                );
            if !self.src_fields.value_fields.is_empty() {
                insert_cypher = insert_cypher.param(
                    SRC_PROPS_PARAM,
                    field_values_to_bolt(
                        self.src_fields
                            .value_fields
                            .iter()
                            .map(|f| &value.fields[f.field_idx]),
                        self.src_fields.value_fields.iter().map(|f| &f.field_schema),
                    )?,
                );
            }
            if !self.tgt_fields.value_fields.is_empty() {
                insert_cypher = insert_cypher.param(
                    TGT_PROPS_PARAM,
                    field_values_to_bolt(
                        self.tgt_fields
                            .value_fields
                            .iter()
                            .map(|f| &value.fields[f.field_idx]),
                        self.tgt_fields.value_fields.iter().map(|f| &f.field_schema),
                    )?,
                );
            }
            if !self.value_fields.is_empty() {
                insert_cypher = insert_cypher.param(
                    REL_PROPS_PARAM,
                    field_values_to_bolt(
                        self.value_fields.iter().map(|f| &value.fields[f.field_idx]),
                        self.value_fields.iter().map(|f| &f.field_schema),
                    )?,
                );
            }
            queries.push(insert_cypher);
        }
        for delete_key in mutation.delete_keys.iter() {
            queries.push(neo4rs::query(&self.delete_cypher).param(
                REL_ID_PARAM,
                key_to_bolt(delete_key, &self.key_field.value_type.typ)?,
            ));
        }
        Ok(queries)
    }
}

#[async_trait]
impl ExportTargetExecutor for RelationshipStorageExecutor {
    async fn apply_mutation(&self, mutation: ExportTargetMutation) -> Result<()> {
        retriable::run(
            || async {
                let queries = self.build_queries_to_apply_mutation(&mutation)?;
                let mut txn = self.graph.start_txn().await?;
                txn.run_queries(queries.clone()).await?;
                txn.commit().await?;
                retriable::Ok(())
            },
            retriable::RunOptions::default(),
        )
        .await
        .map_err(Into::<anyhow::Error>::into)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeLabelSetupState {
    key_field_name: String,
    key_constraint_name: String,
}

impl NodeLabelSetupState {
    fn from_spec(label: &str, spec: &RelationshipNodeSpec) -> Self {
        let key_constraint_name = format!("n__{}__{}", label, spec.key_field_name);
        Self {
            key_field_name: spec.key_field_name.clone(),
            key_constraint_name,
        }
    }

    fn is_compatible(&self, other: &Self) -> bool {
        self.key_field_name == other.key_field_name
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipSetupState {
    key_field_name: String,
    key_constraint_name: String,
    #[serde(default)]
    nodes: BTreeMap<String, NodeLabelSetupState>,
}

impl RelationshipSetupState {
    fn from_spec(spec: &RelationshipSpec, key_field_name: String) -> Self {
        Self {
            key_field_name,
            key_constraint_name: format!("r__{}__key", spec.rel_type),
            nodes: spec
                .nodes
                .iter()
                .map(|(label, node)| (label.clone(), NodeLabelSetupState::from_spec(label, node)))
                .collect(),
        }
    }

    fn check_compatible(&self, existing: &Self) -> SetupStateCompatibility {
        if self.key_field_name != existing.key_field_name {
            SetupStateCompatibility::NotCompatible
        } else if existing.nodes.iter().any(|(label, existing_node)| {
            !self
                .nodes
                .get(label)
                .map_or(false, |node| node.is_compatible(existing_node))
        }) {
            // If any node's key field change of some node label gone, we have to clear relationship.
            SetupStateCompatibility::NotCompatible
        } else {
            SetupStateCompatibility::Compatible
        }
    }
}

#[derive(Debug)]
struct DataClearAction {
    rel_type: String,
    node_labels: IndexSet<String>,
}

#[derive(Debug)]
struct KeyConstraint {
    label: String,
    field_name: String,
}

impl KeyConstraint {
    fn new(label: String, state: &NodeLabelSetupState) -> Self {
        Self {
            label: label,
            field_name: state.key_field_name.clone(),
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
struct SetupStatusCheck {
    #[derivative(Debug = "ignore")]
    graph_pool: Arc<GraphPool>,
    conn_spec: ConnectionSpec,

    data_clear: Option<DataClearAction>,
    rel_constraint_to_delete: IndexSet<String>,
    rel_constraint_to_create: IndexMap<String, KeyConstraint>,
    node_constraint_to_delete: IndexSet<String>,
    node_constraint_to_create: IndexMap<String, KeyConstraint>,

    change_type: SetupChangeType,
}

impl SetupStatusCheck {
    fn new(
        key: GraphRelationship,
        graph_pool: Arc<GraphPool>,
        conn_spec: ConnectionSpec,
        desired_state: Option<RelationshipSetupState>,
        existing: CombinedState<RelationshipSetupState>,
    ) -> Self {
        let data_clear = existing
            .current
            .as_ref()
            .filter(|existing_current| {
                desired_state.as_ref().map_or(true, |desired| {
                    desired.check_compatible(existing_current)
                        == SetupStateCompatibility::NotCompatible
                })
            })
            .map(|existing_current| DataClearAction {
                rel_type: key.relationship.clone(),
                node_labels: existing_current.nodes.keys().cloned().collect(),
            });

        let mut old_rel_constraints = IndexSet::new();
        let mut old_node_constraints = IndexSet::new();
        for existing_version in existing.possible_versions() {
            old_rel_constraints.insert(existing_version.key_constraint_name.clone());
            for (_, node) in existing_version.nodes.iter() {
                old_node_constraints.insert(node.key_constraint_name.clone());
            }
        }

        let mut rel_constraint_to_create = IndexMap::new();
        let mut node_constraint_to_create = IndexMap::new();
        if let Some(desired_state) = desired_state {
            let rel_constraint = KeyConstraint {
                label: key.relationship.clone(),
                field_name: desired_state.key_field_name.clone(),
            };
            old_rel_constraints.swap_remove(&desired_state.key_constraint_name);
            if !existing
                .current
                .as_ref()
                .map(|c| rel_constraint.field_name == c.key_field_name)
                .unwrap_or(false)
            {
                rel_constraint_to_create.insert(desired_state.key_constraint_name, rel_constraint);
            }

            for (label, node) in desired_state.nodes.iter() {
                old_node_constraints.swap_remove(&node.key_constraint_name);
                if !existing
                    .current
                    .as_ref()
                    .map(|c| {
                        c.nodes
                            .get(label)
                            .map_or(false, |existing_node| node.is_compatible(existing_node))
                    })
                    .unwrap_or(false)
                {
                    node_constraint_to_create.insert(
                        node.key_constraint_name.clone(),
                        KeyConstraint::new(label.clone(), node),
                    );
                }
            }
        }

        let rel_constraint_to_delete = old_rel_constraints;
        let node_constraint_to_delete = old_node_constraints;

        let change_type = if data_clear.is_none()
            && rel_constraint_to_delete.is_empty()
            && rel_constraint_to_create.is_empty()
            && node_constraint_to_delete.is_empty()
            && node_constraint_to_create.is_empty()
        {
            SetupChangeType::NoChange
        } else if data_clear.is_none()
            && rel_constraint_to_delete.is_empty()
            && node_constraint_to_delete.is_empty()
        {
            SetupChangeType::Create
        } else if rel_constraint_to_create.is_empty() && node_constraint_to_create.is_empty() {
            SetupChangeType::Delete
        } else {
            SetupChangeType::Update
        };

        Self {
            graph_pool,
            conn_spec,
            data_clear,
            rel_constraint_to_delete,
            rel_constraint_to_create,
            node_constraint_to_delete,
            node_constraint_to_create,
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
        for name in &self.rel_constraint_to_delete {
            result.push(format!("Delete relationship constraint {}", name));
        }
        for (name, rel_constraint) in self.rel_constraint_to_create.iter() {
            result.push(format!(
                "Create KEY CONSTRAINT {} ON RELATIONSHIP {} (key: {})",
                name, rel_constraint.label, rel_constraint.field_name,
            ));
        }
        for name in &self.node_constraint_to_delete {
            result.push(format!("Delete node constraint {}", name));
        }
        for (name, node_constraint) in self.node_constraint_to_create.iter() {
            result.push(format!(
                "Create KEY CONSTRAINT {} ON NODE {} (key: {})",
                name, node_constraint.label, node_constraint.field_name,
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

        for name in
            (self.rel_constraint_to_delete.iter()).chain(self.node_constraint_to_delete.iter())
        {
            graph
                .run(neo4rs::query(&format!("DROP CONSTRAINT {name}")))
                .await?;
        }

        for (name, constraint) in self.node_constraint_to_create.iter() {
            graph
                .run(neo4rs::query(&format!(
                    "CREATE CONSTRAINT {name} IF NOT EXISTS FOR (n:{label}) REQUIRE n.{field_name} IS UNIQUE",
                    label = constraint.label,
                    field_name = constraint.field_name
                )))
                .await?;
        }

        for (name, constraint) in self.rel_constraint_to_create.iter() {
            graph
                .run(neo4rs::query(&format!(
                    "CREATE CONSTRAINT {name} IF NOT EXISTS FOR ()-[e:{label}]-() REQUIRE e.{field_name} IS UNIQUE",
                    label = constraint.label,
                    field_name = constraint.field_name
                )))
                .await?;
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

impl StorageFactoryBase for RelationshipFactory {
    type Spec = RelationshipSpec;
    type SetupState = RelationshipSetupState;
    type Key = GraphRelationship;

    fn name(&self) -> &str {
        "Neo4jRelationship"
    }

    fn build(
        self: Arc<Self>,
        _name: String,
        spec: RelationshipSpec,
        key_fields_schema: Vec<FieldSchema>,
        value_fields_schema: Vec<FieldSchema>,
        _storage_options: IndexOptions,
        context: Arc<FlowInstanceContext>,
    ) -> Result<ExportTargetBuildOutput<Self>> {
        let setup_key = GraphRelationship::from_spec(&spec);
        let key_field_schema = {
            if key_fields_schema.len() != 1 {
                anyhow::bail!("Neo4j only supports a single key field");
            }
            key_fields_schema.into_iter().next().unwrap()
        };
        let desired_setup_state =
            RelationshipSetupState::from_spec(&spec, key_field_schema.name.clone());

        let mut rel_value_fields_info = vec![];
        let mut src_key_field_info = None;
        let mut src_value_fields_info = vec![];
        let mut tgt_key_field_info = None;
        let mut tgt_value_fields_info = vec![];

        let mut field_name_to_src_field_info = spec
            .source
            .fields
            .iter()
            .map(|field| (field.field_name.as_str(), field))
            .collect::<HashMap<_, _>>();
        let mut field_name_to_tgt_field_info = spec
            .target
            .fields
            .iter()
            .map(|field| (field.field_name.as_str(), field))
            .collect::<HashMap<_, _>>();

        let src_label_info = spec.get_src_label_info()?;
        let tgt_label_info = spec.get_tgt_label_info()?;
        for (field_idx, field_schema) in value_fields_schema.into_iter().enumerate() {
            let src_field_info = field_name_to_src_field_info.remove(field_schema.name.as_str());
            let tgt_field_info = field_name_to_tgt_field_info.remove(field_schema.name.as_str());
            let field_mapping = AnalyzedGraphFieldMapping {
                field_idx,
                field_schema,
            };
            if let Some(src_field_info) = src_field_info {
                let node_field_name = src_field_info
                    .node_field_name
                    .as_ref()
                    .unwrap_or(&src_field_info.field_name);
                if &src_label_info.key_field_name == node_field_name {
                    src_key_field_info = Some(field_mapping.clone());
                } else {
                    src_value_fields_info.push(field_mapping.clone());
                }
            }
            if let Some(tgt_field_info) = tgt_field_info {
                let node_field_name = tgt_field_info
                    .node_field_name
                    .as_ref()
                    .unwrap_or(&tgt_field_info.field_name);
                if &tgt_label_info.key_field_name == node_field_name {
                    tgt_key_field_info = Some(field_mapping.clone());
                } else {
                    tgt_value_fields_info.push(field_mapping.clone());
                }
            }
            if src_field_info.is_none() && tgt_field_info.is_none() {
                rel_value_fields_info.push(field_mapping);
            }
        }
        if !field_name_to_src_field_info.is_empty() {
            anyhow::bail!(
                "Source field not found: {}",
                field_name_to_src_field_info.keys().join(", ")
            );
        }
        if !field_name_to_tgt_field_info.is_empty() {
            anyhow::bail!(
                "Target field not found: {}",
                field_name_to_tgt_field_info.keys().join(", ")
            );
        }
        let src_key_field_info = src_key_field_info.ok_or_else(|| {
            anyhow::anyhow!(
                "Source key field not found: {}",
                src_label_info.key_field_name
            )
        })?;
        let tgt_key_field_info = tgt_key_field_info.ok_or_else(|| {
            anyhow::anyhow!(
                "Target key field not found: {}",
                tgt_label_info.key_field_name
            )
        })?;
        let conn_spec = context
            .auth_registry
            .get::<ConnectionSpec>(&spec.connection)?;
        let executor = async move {
            let graph = self.graph_pool.get_graph(&conn_spec).await?;
            let executor = Arc::new(RelationshipStorageExecutor::new(
                graph,
                spec,
                key_field_schema,
                rel_value_fields_info,
                AnalyzedGraphFields {
                    key_field: src_key_field_info,
                    value_fields: src_value_fields_info,
                },
                AnalyzedGraphFields {
                    key_field: tgt_key_field_info,
                    value_fields: tgt_value_fields_info,
                },
            )?);
            Ok((executor as Arc<dyn ExportTargetExecutor>, None))
        }
        .boxed();
        Ok(ExportTargetBuildOutput {
            executor,
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
        Ok(SetupStatusCheck::new(
            key,
            self.graph_pool.clone(),
            conn_spec,
            desired,
            existing,
        ))
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
}
