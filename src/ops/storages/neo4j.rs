use std::convert::Infallible;

use crate::prelude::*;
use crate::setup::ResourceSetupStatusCheck;
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
pub struct NodeSpec {
    field_name: String,
    label: String,
}

#[derive(Debug, Deserialize)]
pub struct RelationshipSpec {
    connection: Neo4jConnectionSpec,
    relationship_label: String,
    source_node: NodeSpec,
    target_node: NodeSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct GraphKey {
    uri: String,
    user: String,
    db: String,
}

impl GraphKey {
    fn from_spec(spec: &Neo4jConnectionSpec) -> Self {
        Self {
            uri: spec.uri.clone(),
            user: spec.user.clone(),
            db: spec.db.clone().unwrap_or_else(|| DEFAULT_DB.to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GraphRelationship {
    graph: GraphKey,
    relationship: String,
}

impl GraphRelationship {
    fn from_spec(spec: &RelationshipSpec) -> Self {
        Self {
            graph: GraphKey::from_spec(&spec.connection),
            relationship: spec.relationship_label.clone(),
        }
    }
}

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
OPTIONAL MATCH (old_src)-[old_rel:{rel_label} {{{rel_key_field_name}: ${REL_ID_PARAM}}}]->(old_tgt)

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
            "#,
            rel_label = spec.relationship_label,
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
MERGE (new_src)-[new_rel:{rel_label} {{id: ${REL_ID_PARAM}}}]->(new_tgt)
{optional_set_rel_props}
            "#,
            src_node_label = spec.source_node.label,
            src_node_key_field_name = spec.source_node.field_name,
            tgt_node_label = spec.target_node.label,
            tgt_node_key_field_name = spec.target_node.field_name,
            rel_label = spec.relationship_label,
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
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipSetupState {
    key_field_name: String,
}

/// Factory for Neo4j relationships
pub struct RelationshipFactory {
    graph_pool: Arc<GraphPool>,
}

impl StorageFactoryBase for RelationshipFactory {
    type Spec = RelationshipSpec;
    type SetupState = RelationshipSetupState;
    type Key = GraphRelationship;

    fn name(&self) -> &str {
        "Neo4j"
    }

    fn build(
        self: Arc<Self>,
        _name: String,
        spec: RelationshipSpec,
        key_fields_schema: Vec<FieldSchema>,
        value_fields_schema: Vec<FieldSchema>,
        _storage_options: IndexOptions,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<ExportTargetBuildOutput<Self>> {
        let setup_key = GraphRelationship::from_spec(&spec);
        let key_field_schema = {
            if key_fields_schema.len() != 1 {
                anyhow::bail!("Neo4j only supports a single key field");
            }
            key_fields_schema.into_iter().next().unwrap()
        };
        let desired_setup_state = RelationshipSetupState {
            key_field_name: key_field_schema.name.clone(),
        };

        let mut src_field_info = None;
        let mut tgt_field_info = None;
        let mut rel_value_fields_info = vec![];
        for (field_idx, field_schema) in value_fields_schema.into_iter().enumerate() {
            let field_info = RelationshipFieldInfo {
                field_idx,
                field_schema,
            };
            if field_info.field_schema.name == spec.source_node.field_name {
                src_field_info = Some(field_info);
            } else if field_info.field_schema.name == spec.target_node.field_name {
                tgt_field_info = Some(field_info);
            } else {
                rel_value_fields_info.push(field_info);
            }
        }
        let src_field_info = src_field_info.ok_or_else(|| {
            anyhow::anyhow!("Source key field {} not found", spec.source_node.field_name)
        })?;
        let tgt_field_info = tgt_field_info.ok_or_else(|| {
            anyhow::anyhow!("Target key field {} not found", spec.target_node.field_name)
        })?;
        let executor = async move {
            let graph = self.graph_pool.get_graph(&spec.connection).await?;
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
        _key: GraphRelationship,
        _desired: Option<RelationshipSetupState>,
        _existing: CombinedState<RelationshipSetupState>,
    ) -> Result<impl ResourceSetupStatusCheck<GraphRelationship, RelationshipSetupState> + 'static>
    {
        Err(anyhow::anyhow!("Not supported")) as Result<Infallible, _>
    }

    fn check_state_compatibility(
        &self,
        desired: &RelationshipSetupState,
        existing: &RelationshipSetupState,
    ) -> Result<SetupStateCompatibility> {
        let compatibility = if desired.key_field_name == existing.key_field_name {
            SetupStateCompatibility::Compatible
        } else {
            SetupStateCompatibility::NotCompatible
        };
        Ok(compatibility)
    }
}
