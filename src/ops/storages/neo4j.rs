use crate::prelude::*;
use crate::setup::{ResourceSetupStatusCheck, SetupChangeType};
use crate::{ops::sdk::*, setup::CombinedState};

use neo4rs::{BoltType, ConfigBuilder, Graph};
use tokio::sync::OnceCell;

const DEFAULT_DB: &str = "neo4j";

#[derive(Debug, Deserialize)]
pub struct Neo4jConnectionSpec {
    uri: String,
    user: String,
    password: String,
    db: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RelationshipEndSpec {
    field_name: String,
    label: String,
}

const DEFAULT_KEY_FIELD_NAME: &str = "value";

#[derive(Debug, Deserialize)]
pub struct RelationshipNodeSpec {
    key_field_name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RelationshipSpec {
    connection: AuthEntryReference,
    relationship: String,
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
    fn from_spec(spec: &Neo4jConnectionSpec) -> Self {
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
            relationship: spec.relationship.clone(),
        }
    }
}

#[derive(Default)]
pub struct GraphPool {
    graphs: Mutex<HashMap<GraphKey, Arc<OnceCell<Arc<Graph>>>>>,
}

impl GraphPool {
    pub async fn get_graph(&self, spec: &Neo4jConnectionSpec) -> Result<Arc<Graph>> {
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

struct RelationshipFieldInfo {
    field_idx: usize,
    field_schema: FieldSchema,
}

struct RelationshipStorageExecutor {
    graph: Arc<Graph>,
    delete_cypher: String,
    insert_cypher: String,

    key_field: FieldSchema,
    value_fields: Vec<RelationshipFieldInfo>,

    src_key_field: RelationshipFieldInfo,
    tgt_key_field: RelationshipFieldInfo,
}

fn json_value_to_bolt_value(value: serde_json::Value) -> Result<BoltType> {
    let bolt_value = match value {
        serde_json::Value::Null => BoltType::Null(neo4rs::BoltNull::default()),
        serde_json::Value::Bool(v) => BoltType::Boolean(neo4rs::BoltBoolean::new(v)),
        serde_json::Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                BoltType::Integer(neo4rs::BoltInteger::new(i))
            } else if let Some(f) = v.as_f64() {
                BoltType::Float(neo4rs::BoltFloat::new(f))
            } else {
                anyhow::bail!("Unsupported JSON number: {}", v)
            }
        }
        serde_json::Value::String(v) => BoltType::String(v.into()),
        serde_json::Value::Array(v) => BoltType::List(neo4rs::BoltList {
            value: v
                .into_iter()
                .map(json_value_to_bolt_value)
                .collect::<Result<_>>()?,
        }),
        serde_json::Value::Object(v) => BoltType::Map(neo4rs::BoltMap {
            value: v
                .into_iter()
                .map(|(k, v)| Ok((k.into(), json_value_to_bolt_value(v)?)))
                .collect::<Result<_>>()?,
        }),
    };
    Ok(bolt_value)
}

fn key_to_bolt(key: KeyValue, schema: &schema::ValueType) -> Result<BoltType> {
    value_to_bolt(key.into(), schema)
}

fn field_values_to_bolt<'a>(
    field_values: impl IntoIterator<Item = value::Value>,
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

fn basic_value_to_bolt(value: BasicValue, schema: &BasicValueType) -> Result<BoltType> {
    let bolt_value = match value {
        BasicValue::Bytes(v) => {
            BoltType::Bytes(neo4rs::BoltBytes::new(bytes::Bytes::from_owner(v)))
        }
        BasicValue::Str(v) => BoltType::String(neo4rs::BoltString::new(&v)),
        BasicValue::Bool(v) => BoltType::Boolean(neo4rs::BoltBoolean::new(v)),
        BasicValue::Int64(v) => BoltType::Integer(neo4rs::BoltInteger::new(v)),
        BasicValue::Float64(v) => BoltType::Float(neo4rs::BoltFloat::new(v)),
        BasicValue::Float32(v) => BoltType::Float(neo4rs::BoltFloat::new(v as f64)),
        BasicValue::Range(v) => BoltType::List(neo4rs::BoltList {
            value: [
                BoltType::Integer(neo4rs::BoltInteger::new(v.start as i64)),
                BoltType::Integer(neo4rs::BoltInteger::new(v.end as i64)),
            ]
            .into(),
        }),
        BasicValue::Uuid(v) => BoltType::String(neo4rs::BoltString::new(&v.to_string())),
        BasicValue::Date(v) => BoltType::Date(neo4rs::BoltDate::from(v)),
        BasicValue::Time(v) => BoltType::LocalTime(neo4rs::BoltLocalTime::from(v)),
        BasicValue::LocalDateTime(v) => BoltType::LocalDateTime(neo4rs::BoltLocalDateTime::from(v)),
        BasicValue::OffsetDateTime(v) => BoltType::DateTime(neo4rs::BoltDateTime::from(v)),
        BasicValue::Vector(v) => match schema {
            BasicValueType::Vector(t) => BoltType::List(neo4rs::BoltList {
                value: v
                    .into_iter()
                    .map(|v| basic_value_to_bolt(v.clone(), &t.element_type))
                    .collect::<Result<_>>()?,
            }),
            _ => anyhow::bail!("Non-vector type got vector value: {}", schema),
        },
        BasicValue::Json(v) => json_value_to_bolt_value(Arc::unwrap_or_clone(v))?,
    };
    Ok(bolt_value)
}

fn value_to_bolt(value: Value, schema: &schema::ValueType) -> Result<BoltType> {
    let bolt_value = match value {
        Value::Null => BoltType::Null(neo4rs::BoltNull::default()),
        Value::Basic(v) => match schema {
            ValueType::Basic(t) => basic_value_to_bolt(v, &t)?,
            _ => anyhow::bail!("Non-basic type got basic value: {}", schema),
        },
        Value::Struct(v) => match schema {
            ValueType::Struct(t) => field_values_to_bolt(v.fields.into_iter(), t.fields.iter())?,
            _ => anyhow::bail!("Non-struct type got struct value: {}", schema),
        },
        Value::Collection(v) | Value::List(v) => match schema {
            ValueType::Collection(t) => BoltType::List(neo4rs::BoltList {
                value: v
                    .into_iter()
                    .map(|v| field_values_to_bolt(v.0.fields, t.row.fields.iter()))
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
                            std::iter::once(k.into()).chain(v.0.fields),
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
const SRC_ID_PARAM: &str = "source_id";
const TGT_ID_PARAM: &str = "target_id";
const REL_PROPS_PARAM: &str = "rel_props";

impl RelationshipStorageExecutor {
    fn new(
        graph: Arc<Graph>,
        spec: RelationshipSpec,
        key_field: FieldSchema,
        value_fields: Vec<RelationshipFieldInfo>,
        src_key_field: RelationshipFieldInfo,
        tgt_key_field: RelationshipFieldInfo,
    ) -> Self {
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
            rel_type = spec.relationship,
            rel_key_field_name = key_field.name,
        );

        let optional_set_rel_props = if value_fields.is_empty() {
            "".to_string()
        } else {
            format!("SET new_rel += ${REL_PROPS_PARAM}\n")
        };
        let insert_cypher = format!(
            r#"
MERGE (new_src:{src_node_label} {{{src_node_key_field_name}: ${SRC_ID_PARAM}}})
MERGE (new_tgt:{tgt_node_label} {{{tgt_node_key_field_name}: ${TGT_ID_PARAM}}})
MERGE (new_src)-[new_rel:{rel_type} {{{rel_key_field_name}: ${REL_ID_PARAM}}}]->(new_tgt)
{optional_set_rel_props}

FINISH
            "#,
            src_node_label = spec.source.label,
            src_node_key_field_name = spec
                .nodes
                .get(&spec.source.label)
                .and_then(|node| node.key_field_name.as_ref().map(|n| n.as_str()))
                .unwrap_or_else(|| DEFAULT_KEY_FIELD_NAME),
            tgt_node_label = spec.target.label,
            tgt_node_key_field_name = spec
                .nodes
                .get(&spec.target.label)
                .and_then(|node| node.key_field_name.as_ref().map(|n| n.as_str()))
                .unwrap_or_else(|| DEFAULT_KEY_FIELD_NAME),
            rel_type = spec.relationship,
            rel_key_field_name = key_field.name,
        );
        Self {
            graph,
            delete_cypher,
            insert_cypher,
            key_field,
            value_fields,
            src_key_field,
            tgt_key_field,
        }
    }
}

#[async_trait]
impl ExportTargetExecutor for RelationshipStorageExecutor {
    async fn apply_mutation(&self, mutation: ExportTargetMutation) -> Result<()> {
        let mut queries = vec![];
        for upsert in mutation.upserts {
            let rel_id_bolt = key_to_bolt(upsert.key, &self.key_field.value_type.typ)?;
            queries
                .push(neo4rs::query(&self.delete_cypher).param(REL_ID_PARAM, rel_id_bolt.clone()));

            let mut value = upsert.value;
            let mut insert_cypher = neo4rs::query(&self.insert_cypher)
                .param(REL_ID_PARAM, rel_id_bolt)
                .param(
                    SRC_ID_PARAM,
                    value_to_bolt(
                        std::mem::take(&mut value.fields[self.src_key_field.field_idx]),
                        &self.src_key_field.field_schema.value_type.typ,
                    )?,
                )
                .param(
                    TGT_ID_PARAM,
                    value_to_bolt(
                        std::mem::take(&mut value.fields[self.tgt_key_field.field_idx]),
                        &self.tgt_key_field.field_schema.value_type.typ,
                    )?,
                );
            if !self.value_fields.is_empty() {
                insert_cypher = insert_cypher.param(
                    REL_PROPS_PARAM,
                    field_values_to_bolt(
                        self.value_fields
                            .iter()
                            .map(|f| std::mem::take(&mut value.fields[f.field_idx])),
                        self.value_fields.iter().map(|f| &f.field_schema),
                    )?,
                );
            }
            queries.push(insert_cypher);
        }
        for delete_key in mutation.delete_keys {
            queries.push(neo4rs::query(&self.delete_cypher).param(
                REL_ID_PARAM,
                key_to_bolt(delete_key, &self.key_field.value_type.typ)?,
            ));
        }

        let mut txn = self.graph.start_txn().await?;
        txn.run_queries(queries).await?;
        txn.commit().await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeLabelSetupState {
    key_field_name: String,
    key_constraint_name: String,
}

impl NodeLabelSetupState {
    fn from_spec(label: &str, spec: &RelationshipNodeSpec) -> Self {
        let key_field_name = spec
            .key_field_name
            .to_owned()
            .unwrap_or_else(|| DEFAULT_KEY_FIELD_NAME.to_string());
        let key_constraint_name = format!("n__{}__{}", label, key_field_name);
        Self {
            key_field_name,
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
            key_constraint_name: format!("r__{}__key", spec.relationship),
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
    conn_spec: Neo4jConnectionSpec,

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
        conn_spec: Neo4jConnectionSpec,
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
        let mut src_field_info = None;
        let mut tgt_field_info = None;
        let mut rel_value_fields_info = vec![];
        for (field_idx, field_schema) in value_fields_schema.into_iter().enumerate() {
            let field_info = RelationshipFieldInfo {
                field_idx,
                field_schema,
            };
            if field_info.field_schema.name == spec.source.field_name {
                src_field_info = Some(field_info);
            } else if field_info.field_schema.name == spec.target.field_name {
                tgt_field_info = Some(field_info);
            } else {
                rel_value_fields_info.push(field_info);
            }
        }
        let src_field_info = src_field_info.ok_or_else(|| {
            anyhow::anyhow!("Source key field {} not found", spec.source.field_name)
        })?;
        let tgt_field_info = tgt_field_info.ok_or_else(|| {
            anyhow::anyhow!("Target key field {} not found", spec.target.field_name)
        })?;
        let conn_spec = context
            .auth_registry
            .get::<Neo4jConnectionSpec>(&spec.connection)?;
        let executor = async move {
            let graph = self.graph_pool.get_graph(&conn_spec).await?;
            let executor = Arc::new(RelationshipStorageExecutor::new(
                graph,
                spec,
                key_field_schema,
                rel_value_fields_info,
                src_field_info,
                tgt_field_info,
            ));
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
        let conn_spec = auth_registry.get::<Neo4jConnectionSpec>(&key.connection)?;
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
