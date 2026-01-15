use crate::prelude::*;

use super::shared::property_graph::*;

use crate::setup::components::{self, State, apply_component_changes};
use crate::setup::{ResourceSetupChange, SetupChangeType};
use crate::{ops::sdk::*, setup::CombinedState};

use falkordb::{FalkorAsyncClient, FalkorClientBuilder, FalkorConnectionInfo, FalkorValue};
use indoc::formatdoc;
use std::fmt::Write;
use tokio::sync::OnceCell;

const DEFAULT_GRAPH: &str = "default";

#[derive(Debug, Deserialize, Clone)]
pub struct ConnectionSpec {
    /// FalkorDB connection URI (e.g., "falkor://localhost:6379" or "redis://localhost:6379")
    uri: String,
    /// Graph name to use (defaults to "default")
    graph: Option<String>,
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

type FalkorDBGraphElement = GraphElementType<ConnectionSpec>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct GraphKey {
    uri: String,
    graph: String,
}

impl GraphKey {
    fn from_spec(spec: &ConnectionSpec) -> Self {
        Self {
            uri: spec.uri.clone(),
            graph: spec
                .graph
                .clone()
                .unwrap_or_else(|| DEFAULT_GRAPH.to_string()),
        }
    }
}

struct ClientWithGraph {
    client: FalkorAsyncClient,
    graph_name: String,
}

#[derive(Default)]
pub struct GraphPool {
    clients: Mutex<HashMap<GraphKey, Arc<OnceCell<Arc<ClientWithGraph>>>>>,
}

impl GraphPool {
    async fn get_client(&self, spec: &ConnectionSpec) -> Result<Arc<ClientWithGraph>> {
        let graph_key = GraphKey::from_spec(spec);
        let cell = {
            let mut clients = self.clients.lock().unwrap();
            clients.entry(graph_key.clone()).or_default().clone()
        };
        let client = cell
            .get_or_try_init(|| async {
                let connection_info: FalkorConnectionInfo = spec
                    .uri
                    .as_str()
                    .try_into()
                    .map_err(|e| api_error!("Invalid FalkorDB connection URI: {}", e))?;

                let client = FalkorClientBuilder::new_async()
                    .with_connection_info(connection_info)
                    .build()
                    .await
                    .map_err(|e| api_error!("Failed to connect to FalkorDB: {}", e))?;

                Ok::<_, Error>(Arc::new(ClientWithGraph {
                    client,
                    graph_name: graph_key.graph.clone(),
                }))
            })
            .await?;
        Ok(client.clone())
    }

    async fn get_client_for_key(
        &self,
        key: &FalkorDBGraphElement,
        auth_registry: &AuthRegistry,
    ) -> Result<Arc<ClientWithGraph>> {
        let spec = auth_registry.get::<ConnectionSpec>(&key.connection)?;
        self.get_client(&spec).await
    }
}

pub struct ExportContext {
    connection_ref: AuthEntryReference<ConnectionSpec>,
    client: Arc<ClientWithGraph>,

    create_order: u8,

    delete_cypher: String,
    insert_cypher: String,
    delete_before_upsert: bool,

    analyzed_data_coll: AnalyzedDataCollection,

    key_field_params: Vec<String>,
    src_key_field_params: Vec<String>,
    tgt_key_field_params: Vec<String>,
}

/// Convert FalkorValue to Cypher literal string representation
fn falkor_value_to_cypher_literal(value: &FalkorValue) -> String {
    match value {
        FalkorValue::None => "null".to_string(),
        FalkorValue::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        FalkorValue::I64(i) => i.to_string(),
        FalkorValue::F64(f) => {
            if f.is_nan() {
                "0.0/0.0".to_string() // NaN representation
            } else if f.is_infinite() {
                if *f > 0.0 { "1.0/0.0" } else { "-1.0/0.0" }.to_string()
            } else {
                f.to_string()
            }
        }
        FalkorValue::String(s) => {
            // Escape quotes and backslashes
            let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
            format!("'{}'", escaped)
        }
        FalkorValue::Array(arr) => {
            let items: Vec<String> = arr.iter().map(falkor_value_to_cypher_literal).collect();
            format!("[{}]", items.join(", "))
        }
        FalkorValue::Map(map) => {
            let items: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}: {}", k, falkor_value_to_cypher_literal(v)))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
        FalkorValue::Vec32(v) => {
            // Vector of f32 values for vector search
            let items: Vec<String> = v.values.iter().map(|f| f.to_string()).collect();
            format!("[{}]", items.join(", "))
        }
        // These types are typically only returned from queries, not used as input
        FalkorValue::Node(_)
        | FalkorValue::Edge(_)
        | FalkorValue::Path(_)
        | FalkorValue::Point(_)
        | FalkorValue::Unparseable(_) => {
            "null".to_string() // Fallback for complex types
        }
    }
}

/// Interpolate parameters into a Cypher query
fn interpolate_cypher_params(query: &str, params: &HashMap<String, FalkorValue>) -> String {
    let mut result = query.to_string();
    for (key, value) in params {
        let placeholder = format!("${}", key);
        let literal = falkor_value_to_cypher_literal(value);
        result = result.replace(&placeholder, &literal);
    }
    result
}

fn value_to_falkor(value: &Value, schema: &schema::ValueType) -> Result<FalkorValue> {
    let fv = match value {
        Value::Null => FalkorValue::None,
        Value::Basic(v) => match schema {
            ValueType::Basic(t) => basic_value_to_falkor(v, t)?,
            _ => internal_bail!("Non-basic type got basic value: {}", schema),
        },
        Value::Struct(v) => match schema {
            ValueType::Struct(t) => struct_to_falkor(&v.fields, &t.fields)?,
            _ => internal_bail!("Non-struct type got struct value: {}", schema),
        },
        Value::UTable(v) | Value::LTable(v) => match schema {
            ValueType::Table(t) => FalkorValue::Array(
                v.iter()
                    .map(|v| struct_to_falkor(&v.0.fields, &t.row.fields))
                    .collect::<Result<_>>()?,
            ),
            _ => internal_bail!("Non-table type got table value: {}", schema),
        },
        Value::KTable(v) => match schema {
            ValueType::Table(t) => FalkorValue::Array(
                v.iter()
                    .map(|(k, v)| {
                        let mut fields: Vec<value::Value> = k.to_values().to_vec();
                        fields.extend(v.0.fields.iter().cloned());
                        struct_to_falkor(&fields, &t.row.fields)
                    })
                    .collect::<Result<_>>()?,
            ),
            _ => internal_bail!("Non-table type got table value: {}", schema),
        },
    };
    Ok(fv)
}

fn basic_value_to_falkor(value: &BasicValue, schema: &BasicValueType) -> Result<FalkorValue> {
    let fv = match value {
        BasicValue::Bytes(v) => FalkorValue::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            v,
        )),
        BasicValue::Str(v) => FalkorValue::String(v.to_string()),
        BasicValue::Bool(v) => FalkorValue::Bool(*v),
        BasicValue::Int64(v) => FalkorValue::I64(*v),
        BasicValue::Float64(v) => FalkorValue::F64(*v),
        BasicValue::Float32(v) => FalkorValue::F64(*v as f64),
        BasicValue::Range(v) => FalkorValue::Array(vec![
            FalkorValue::I64(v.start as i64),
            FalkorValue::I64(v.end as i64),
        ]),
        BasicValue::Uuid(v) => FalkorValue::String(v.to_string()),
        BasicValue::Date(v) => FalkorValue::String(v.to_string()),
        BasicValue::Time(v) => FalkorValue::String(v.to_string()),
        BasicValue::LocalDateTime(v) => FalkorValue::String(v.to_string()),
        BasicValue::OffsetDateTime(v) => FalkorValue::String(v.to_string()),
        BasicValue::TimeDelta(v) => FalkorValue::I64(v.num_milliseconds()),
        BasicValue::Vector(v) => match schema {
            BasicValueType::Vector(t) => FalkorValue::Array(
                v.iter()
                    .map(|v| basic_value_to_falkor(v, &t.element_type))
                    .collect::<Result<_>>()?,
            ),
            _ => internal_bail!("Non-vector type got vector value: {}", schema),
        },
        BasicValue::Json(v) => json_value_to_falkor(v)?,
        BasicValue::UnionVariant { tag_id, value } => match schema {
            BasicValueType::Union(s) => {
                let typ = s
                    .types
                    .get(*tag_id)
                    .ok_or_else(|| internal_error!("Invalid `tag_id`: {}", tag_id))?;
                basic_value_to_falkor(value, typ)?
            }
            _ => internal_bail!("Non-union type got union value: {}", schema),
        },
    };
    Ok(fv)
}

fn json_value_to_falkor(value: &serde_json::Value) -> Result<FalkorValue> {
    let fv = match value {
        serde_json::Value::Null => FalkorValue::None,
        serde_json::Value::Bool(v) => FalkorValue::Bool(*v),
        serde_json::Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                FalkorValue::I64(i)
            } else if let Some(f) = v.as_f64() {
                FalkorValue::F64(f)
            } else {
                client_bail!("Unsupported JSON number: {}", v)
            }
        }
        serde_json::Value::String(v) => FalkorValue::String(v.clone()),
        serde_json::Value::Array(v) => {
            FalkorValue::Array(v.iter().map(json_value_to_falkor).collect::<Result<_>>()?)
        }
        serde_json::Value::Object(v) => FalkorValue::Map(
            v.iter()
                .map(|(k, v)| Ok((k.clone(), json_value_to_falkor(v)?)))
                .collect::<Result<_>>()?,
        ),
    };
    Ok(fv)
}

fn struct_to_falkor(
    fields: &[value::Value],
    schema: &[schema::FieldSchema],
) -> Result<FalkorValue> {
    let map: HashMap<String, FalkorValue> = std::iter::zip(schema, fields)
        .map(|(s, v)| Ok((s.name.clone(), value_to_falkor(v, &s.value_type.typ)?)))
        .collect::<Result<_>>()?;
    Ok(FalkorValue::Map(map))
}

fn key_to_falkor(key: &KeyPart, schema: &schema::ValueType) -> Result<FalkorValue> {
    value_to_falkor(&key.into(), schema)
}

#[allow(dead_code)]
fn field_values_to_map(
    field_values: &[value::Value],
    schema: &[schema::FieldSchema],
) -> Result<HashMap<String, FalkorValue>> {
    std::iter::zip(schema, field_values)
        .map(|(s, v)| Ok((s.name.clone(), value_to_falkor(v, &s.value_type.typ)?)))
        .collect::<Result<_>>()
}

fn mapped_field_values_to_map(
    fields_schema: &[schema::FieldSchema],
    fields_input_idx: &[usize],
    field_values: &FieldValues,
) -> Result<HashMap<String, FalkorValue>> {
    std::iter::zip(fields_schema.iter(), fields_input_idx.iter())
        .map(|(schema, field_idx)| {
            Ok((
                schema.name.clone(),
                value_to_falkor(&field_values.fields[*field_idx], &schema.value_type.typ)?,
            ))
        })
        .collect::<Result<_>>()
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
                let param = format!("{param_prefix}_{i}");
                let item = format!("{name}: ${param}");
                (param, item)
            })
            .unzip();
        (params, format!("{{{}}}", items.into_iter().join(", ")))
    }

    fn new(
        client: Arc<ClientWithGraph>,
        spec: Spec,
        analyzed_data_coll: AnalyzedDataCollection,
    ) -> Result<Self> {
        let (key_field_params, key_fields_literal) = Self::build_key_field_params_n_literal(
            CORE_KEY_PARAM_PREFIX,
            analyzed_data_coll.schema.key_fields.iter().map(|f| &f.name),
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
                    ",
                    label = node_spec.label,
                };

                let insert_cypher = formatdoc! {"
                    MERGE (new_node:{label} {key_fields_literal})
                    SET new_node.{SELF_CONTAINED_TAG_FIELD_NAME} = true{optional_set_props}
                    ",
                    label = node_spec.label,
                    optional_set_props = if !analyzed_data_coll.value_fields_input_idx.is_empty() {
                        format!(", new_node += ${CORE_PROPS_PARAM}\n")
                    } else {
                        "".to_string()
                    },
                };

                Self {
                    connection_ref: spec.connection,
                    client,
                    create_order: 0,
                    delete_cypher,
                    insert_cypher,
                    delete_before_upsert: false,
                    analyzed_data_coll,
                    key_field_params,
                    src_key_field_params: vec![],
                    tgt_key_field_params: vec![],
                }
            }
            GraphElementMapping::Relationship(rel_spec) => {
                let analyzed_rel = analyzed_data_coll
                    .rel
                    .as_ref()
                    .ok_or_else(|| internal_error!("Expected relationship info"))?;
                let analyzed_src = &analyzed_rel.source;
                let analyzed_tgt = &analyzed_rel.target;

                let (src_key_field_params, src_key_fields_literal) =
                    Self::build_key_field_params_n_literal(
                        SRC_KEY_PARAM_PREFIX,
                        analyzed_src.schema.key_fields.iter().map(|f| &f.name),
                    );
                let (tgt_key_field_params, tgt_key_fields_literal) =
                    Self::build_key_field_params_n_literal(
                        TGT_KEY_PARAM_PREFIX,
                        analyzed_tgt.schema.key_fields.iter().map(|f| &f.name),
                    );

                // For delete, we only match on the relationship key, not source/target
                // This allows deletion when we only have the relationship ID
                let delete_cypher = formatdoc! {"
                    OPTIONAL MATCH (old_src)-[old_rel:{rel_type} {key_fields_literal}]->(old_tgt)
                    DELETE old_rel
                    WITH old_src, old_tgt
                    WHERE old_src.{SELF_CONTAINED_TAG_FIELD_NAME} IS NULL AND NOT (old_src)--()
                    DELETE old_src
                    WITH old_tgt
                    WHERE old_tgt.{SELF_CONTAINED_TAG_FIELD_NAME} IS NULL AND NOT (old_tgt)--()
                    DELETE old_tgt
                    ",
                    rel_type = rel_spec.rel_type,
                };

                let insert_cypher = formatdoc! {"
                    MERGE (new_src:{src_node_label} {src_key_fields_literal})
                    {optional_set_src_props}
                    MERGE (new_tgt:{tgt_node_label} {tgt_key_fields_literal})
                    {optional_set_tgt_props}
                    MERGE (new_src)-[new_rel:{rel_type} {key_fields_literal}]->(new_tgt)
                    {optional_set_rel_props}
                    ",
                    src_node_label = rel_spec.source.label,
                    optional_set_src_props = if analyzed_src.has_value_fields() {
                        format!("SET new_src += ${SRC_PROPS_PARAM}\n")
                    } else {
                        "".to_string()
                    },
                    tgt_node_label = rel_spec.target.label,
                    optional_set_tgt_props = if analyzed_tgt.has_value_fields() {
                        format!("SET new_tgt += ${TGT_PROPS_PARAM}\n")
                    } else {
                        "".to_string()
                    },
                    rel_type = rel_spec.rel_type,
                    optional_set_rel_props = if !analyzed_data_coll.value_fields_input_idx.is_empty() {
                        format!("SET new_rel += ${CORE_PROPS_PARAM}\n")
                    } else {
                        "".to_string()
                    },
                };

                Self {
                    connection_ref: spec.connection,
                    client,
                    create_order: 1,
                    delete_cypher,
                    insert_cypher,
                    delete_before_upsert: true,
                    analyzed_data_coll,
                    key_field_params,
                    src_key_field_params,
                    tgt_key_field_params,
                }
            }
        };
        Ok(result)
    }

    fn build_params(
        &self,
        upsert: &ExportTargetUpsertEntry,
    ) -> Result<HashMap<String, FalkorValue>> {
        let mut params = HashMap::new();
        let value = &upsert.value;

        // Bind key field params
        for (i, val) in upsert.key.iter().enumerate() {
            params.insert(
                self.key_field_params[i].clone(),
                key_to_falkor(
                    val,
                    &self.analyzed_data_coll.schema.key_fields[i].value_type.typ,
                )?,
            );
        }

        // Bind relationship-specific params
        if let Some(analyzed_rel) = &self.analyzed_data_coll.rel {
            // Source key params
            for (i, field_idx) in analyzed_rel.source.fields_input_idx.key.iter().enumerate() {
                params.insert(
                    self.src_key_field_params[i].clone(),
                    value_to_falkor(
                        &value.fields[*field_idx],
                        &analyzed_rel.source.schema.key_fields[i].value_type.typ,
                    )?,
                );
            }
            // Source props
            if analyzed_rel.source.has_value_fields() {
                params.insert(
                    SRC_PROPS_PARAM.to_string(),
                    FalkorValue::Map(mapped_field_values_to_map(
                        &analyzed_rel.source.schema.value_fields,
                        &analyzed_rel.source.fields_input_idx.value,
                        value,
                    )?),
                );
            }

            // Target key params
            for (i, field_idx) in analyzed_rel.target.fields_input_idx.key.iter().enumerate() {
                params.insert(
                    self.tgt_key_field_params[i].clone(),
                    value_to_falkor(
                        &value.fields[*field_idx],
                        &analyzed_rel.target.schema.key_fields[i].value_type.typ,
                    )?,
                );
            }
            // Target props
            if analyzed_rel.target.has_value_fields() {
                params.insert(
                    TGT_PROPS_PARAM.to_string(),
                    FalkorValue::Map(mapped_field_values_to_map(
                        &analyzed_rel.target.schema.value_fields,
                        &analyzed_rel.target.fields_input_idx.value,
                        value,
                    )?),
                );
            }
        }

        // Core props
        if !self.analyzed_data_coll.value_fields_input_idx.is_empty() {
            params.insert(
                CORE_PROPS_PARAM.to_string(),
                FalkorValue::Map(mapped_field_values_to_map(
                    &self.analyzed_data_coll.schema.value_fields,
                    &self.analyzed_data_coll.value_fields_input_idx,
                    value,
                )?),
            );
        }

        Ok(params)
    }

    fn build_delete_params(
        &self,
        delete_key: &value::KeyValue,
    ) -> Result<HashMap<String, FalkorValue>> {
        let mut params = HashMap::new();
        for (i, val) in delete_key.iter().enumerate() {
            params.insert(
                self.key_field_params[i].clone(),
                key_to_falkor(
                    val,
                    &self.analyzed_data_coll.schema.key_fields[i].value_type.typ,
                )?,
            );
        }
        Ok(params)
    }

    async fn execute_query(
        &self,
        cypher: &str,
        params: HashMap<String, FalkorValue>,
    ) -> Result<()> {
        let mut graph = self.client.client.select_graph(&self.client.graph_name);

        // FalkorDB's with_params only accepts HashMap<String, String>, so we
        // interpolate the FalkorValue parameters directly into the Cypher query
        let interpolated_query = interpolate_cypher_params(cypher, &params);

        // Debug: Log the query being executed
        tracing::debug!("Executing FalkorDB query: {}", interpolated_query);

        graph
            .query(&interpolated_query)
            .execute()
            .await
            .map_err(|e| {
                api_error!(
                    "FalkorDB query failed: {}. Query was: {}",
                    e,
                    interpolated_query
                )
            })?;

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
        schema: &GraphElementSchema,
        index_options: &IndexOptions,
        dependent_node_labels: Vec<String>,
    ) -> Result<Self> {
        let key_field_names: Vec<String> =
            schema.key_fields.iter().map(|f| f.name.clone()).collect();
        let mut sub_components = vec![];
        sub_components.push(ComponentState {
            object_label: schema.elem_type.clone(),
            index_def: IndexDef::KeyConstraint {
                field_names: key_field_names.clone(),
            },
        });
        let value_field_types = schema
            .value_fields
            .iter()
            .map(|f| (f.name.as_str(), &f.value_type.typ))
            .collect::<HashMap<_, _>>();
        if !index_options.fts_indexes.is_empty() {
            // FalkorDB supports full-text search
            for fts_def in index_options.fts_indexes.iter() {
                sub_components.push(ComponentState {
                    object_label: schema.elem_type.clone(),
                    index_def: IndexDef::FullTextIndex {
                        field_names: vec![fts_def.field_name.clone()],
                    },
                });
            }
        }
        for index_def in index_options.vector_indexes.iter() {
            sub_components.push(ComponentState {
                object_label: schema.elem_type.clone(),
                index_def: IndexDef::from_vector_index_def(
                    index_def,
                    value_field_types
                        .get(index_def.field_name.as_str())
                        .ok_or_else(|| {
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
            dependent_node_labels,
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
    FullTextIndex,
}

impl ComponentKind {
    fn describe(&self) -> &str {
        match self {
            ComponentKind::KeyConstraint => "CONSTRAINT",
            ComponentKind::VectorIndex => "VECTOR INDEX",
            ComponentKind::FullTextIndex => "FULLTEXT INDEX",
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
        method: Option<spec::VectorIndexMethod>,
    },
    FullTextIndex {
        field_names: Vec<String>,
    },
}

impl IndexDef {
    fn from_vector_index_def(
        index_def: &spec::VectorIndexDef,
        field_typ: &schema::ValueType,
    ) -> Result<Self> {
        let method = index_def.method.clone();
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
            method,
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
            IndexDef::FullTextIndex { .. } => ComponentKey {
                kind: ComponentKind::FullTextIndex,
                name: format!("{prefix}__{label}__fts"),
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
    type Context = ();

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
                method,
            } => {
                let method_str = method
                    .as_ref()
                    .map(|m| format!(", method: {}", m))
                    .unwrap_or_default();
                format!(
                    "{key_desc} ON {label} (field_name: {field_name}, vector_size: {vector_size}, metric: {metric}{method_str})",
                )
            }
            IndexDef::FullTextIndex { field_names } => {
                format!("{key_desc} ON {label} (fields: {})", field_names.join(", "))
            }
        }
    }

    fn is_up_to_date(&self, current: &ComponentState, desired: &ComponentState) -> bool {
        current == desired
    }

    async fn create(&self, state: &ComponentState, _context: &Self::Context) -> Result<()> {
        tracing::info!("🔧 CREATE called for component: {:?}", state);
        let client = self.graph_pool.get_client(&self.conn_spec).await?;
        let _key = state.key();
        let label = state.object_label.label();

        let is_node = matches!(state.object_label, ElementType::Node(_));
        let entity_type = if is_node { "NODE" } else { "RELATIONSHIP" };

        match &state.index_def {
            IndexDef::KeyConstraint { field_names } => {
                tracing::info!(
                    "📋 Creating KeyConstraint for {}: fields={:?}",
                    label,
                    field_names
                );
                // FalkorDB requires creating an index first, then a unique constraint
                let fields = field_names.iter().map(|f| format!("e.{f}")).join(", ");

                // Step 1: Create the index
                let index_cypher = if is_node {
                    format!("CREATE INDEX FOR (e:{label}) ON ({fields})")
                } else {
                    format!("CREATE INDEX FOR ()-[e:{label}]-() ON ({fields})")
                };

                tracing::info!("📝 Executing INDEX query: {}", index_cypher);
                let mut graph = client.client.select_graph(&client.graph_name);
                graph
                    .query(&index_cypher)
                    .execute()
                    .await
                    .map_err(|e| api_error!("Failed to create index: {}", e))?;
                tracing::info!("✅ Index created successfully");

                // Step 2: Create the unique constraint using Redis command format
                // Format: GRAPH.CONSTRAINT CREATE graph_name UNIQUE NODE/RELATIONSHIP label PROPERTIES count prop1 prop2...
                let field_list = field_names.join(" ");
                let constraint_cmd = format!(
                    "GRAPH.CONSTRAINT CREATE {} UNIQUE {} {} PROPERTIES {} {}",
                    client.graph_name,
                    entity_type,
                    label,
                    field_names.len(),
                    field_list
                );

                tracing::info!("📝 Attempting CONSTRAINT query: {}", constraint_cmd);
                // Execute as a raw Redis command via the client
                // Note: This may fail if the Rust client doesn't support raw commands
                // In that case, the index alone will provide performance but not uniqueness enforcement
                let constraint_result = graph.query(&constraint_cmd).execute().await;
                if let Err(e) = constraint_result {
                    tracing::warn!(
                        "Failed to create unique constraint (index created successfully): {}. \
                         Key fields will be indexed but uniqueness is not enforced.",
                        e
                    );
                } else {
                    tracing::info!("✅ Constraint created successfully");
                }

                Ok(())
            }
            IndexDef::VectorIndex {
                field_name,
                metric,
                vector_size,
                ..
            } => {
                tracing::info!(
                    "📊 Creating VectorIndex for {}: field={}, metric={:?}, dim={}",
                    label,
                    field_name,
                    metric,
                    vector_size
                );
                let similarity = match metric {
                    spec::VectorSimilarityMetric::CosineSimilarity => "cosine",
                    spec::VectorSimilarityMetric::L2Distance => "L2",
                    spec::VectorSimilarityMetric::InnerProduct => "IP",
                };
                let cypher = format!(
                    "CREATE VECTOR INDEX FOR (e:{label}) ON (e.{field_name}) OPTIONS {{dimension: {vector_size}, similarityFunction: '{similarity}'}}"
                );

                tracing::info!("📝 Executing VECTOR INDEX query: {}", cypher);
                let mut graph = client.client.select_graph(&client.graph_name);
                graph
                    .query(&cypher)
                    .execute()
                    .await
                    .map_err(|e| api_error!("Failed to create vector index: {}", e))?;

                tracing::info!("✅ Vector index created successfully");
                Ok(())
            }
            IndexDef::FullTextIndex { field_names } => {
                tracing::info!(
                    "🔍 Creating FullTextIndex for {}: fields={:?}",
                    label,
                    field_names
                );
                let fields = field_names.iter().map(|f| format!("e.{f}")).join(", ");
                let cypher = format!("CALL db.idx.fulltext.createNodeIndex('{label}', {fields})");

                tracing::info!("📝 Executing FULLTEXT INDEX query: {}", cypher);
                let mut graph = client.client.select_graph(&client.graph_name);
                graph
                    .query(&cypher)
                    .execute()
                    .await
                    .map_err(|e| api_error!("Failed to create fulltext index: {}", e))?;

                tracing::info!("✅ Fulltext index created successfully");
                Ok(())
            }
        }
    }

    async fn delete(&self, key: &ComponentKey, _context: &Self::Context) -> Result<()> {
        let client = self.graph_pool.get_client(&self.conn_spec).await?;
        let mut graph = client.client.select_graph(&client.graph_name);

        match key.kind {
            ComponentKind::KeyConstraint => {
                // Try to drop constraint first (may not exist)
                let constraint_cmd =
                    format!("GRAPH.CONSTRAINT DROP {} {}", client.graph_name, key.name);
                let _ = graph.query(&constraint_cmd).execute().await;

                // Then drop the index
                let index_drop = format!("DROP INDEX {}", key.name);
                let _ = graph.query(&index_drop).execute().await;
            }
            ComponentKind::VectorIndex => {
                let cypher = format!("DROP INDEX {}", key.name);
                let _ = graph.query(&cypher).execute().await;
            }
            ComponentKind::FullTextIndex => {
                let cypher = format!("CALL db.idx.fulltext.drop('{}')", key.name);
                let _ = graph.query(&cypher).execute().await;
            }
        }

        Ok(())
    }
}

#[allow(dead_code)]
fn build_composite_field_names(qualifier: &str, field_names: &[String]) -> String {
    let strs = field_names
        .iter()
        .map(|name| format!("{qualifier}.{name}"))
        .join(", ");
    if field_names.len() == 1 {
        strs
    } else {
        format!("({strs})")
    }
}

#[derive(Debug)]
pub struct GraphElementDataSetupChange {
    data_clear: Option<DataClearAction>,
    change_type: SetupChangeType,
}

impl GraphElementDataSetupChange {
    fn new(desired_state: Option<&SetupState>, existing: &CombinedState<SetupState>) -> Self {
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
            data_clear,
            change_type,
        }
    }
}

impl ResourceSetupChange for GraphElementDataSetupChange {
    fn describe_changes(&self) -> Vec<setup::ChangeDescription> {
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
            result.push(setup::ChangeDescription::Action(desc));
        }
        result
    }

    fn change_type(&self) -> SetupChangeType {
        self.change_type
    }
}

async fn clear_graph_element_data(
    client: &ClientWithGraph,
    key: &FalkorDBGraphElement,
    is_self_contained: bool,
) -> Result<()> {
    let var_name = CORE_ELEMENT_MATCHER_VAR;
    let matcher = key.typ.matcher(var_name);
    let cypher = match &key.typ {
        ElementType::Node(_) => {
            if is_self_contained {
                formatdoc! {"
                    MATCH {matcher}
                    SET {var_name}.{SELF_CONTAINED_TAG_FIELD_NAME} = NULL
                    WITH {var_name} WHERE NOT ({var_name})--()
                    DELETE {var_name}
                "}
            } else {
                formatdoc! {"
                    MATCH {matcher}
                    WHERE NOT ({var_name})--()
                    DELETE {var_name}
                "}
            }
        }
        ElementType::Relationship(_) => {
            formatdoc! {"
                MATCH {matcher}
                DELETE {var_name}
            "}
        }
    };

    let mut graph = client.client.select_graph(&client.graph_name);
    graph
        .query(&cypher)
        .execute()
        .await
        .map_err(|e| api_error!("Failed to clear graph element data: {}", e))?;

    Ok(())
}

/// Factory for FalkorDB graph database
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

#[async_trait]
impl TargetFactoryBase for Factory {
    type Spec = Spec;
    type DeclarationSpec = Declaration;
    type SetupState = SetupState;
    type SetupChange = (
        GraphElementDataSetupChange,
        components::SetupChange<SetupComponentOperator>,
    );
    type SetupKey = FalkorDBGraphElement;
    type ExportContext = ExportContext;

    fn name(&self) -> &str {
        "FalkorDB"
    }

    async fn build(
        self: Arc<Self>,
        data_collections: Vec<TypedExportDataCollectionSpec<Self>>,
        declarations: Vec<Declaration>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        Vec<TypedExportDataCollectionBuildOutput<Self>>,
        Vec<(FalkorDBGraphElement, SetupState)>,
    )> {
        let (analyzed_data_colls, declared_graph_elements) = analyze_graph_mappings(
            data_collections
                .iter()
                .map(|d| DataCollectionGraphMappingInput {
                    auth_ref: &d.spec.connection,
                    mapping: &d.spec.mapping,
                    index_options: &d.index_options,
                    key_fields_schema: d.key_fields_schema.clone(),
                    value_fields_schema: d.value_fields_schema.clone(),
                }),
            declarations.iter().map(|d| (&d.connection, &d.decl)),
        )?;
        let data_coll_output = std::iter::zip(data_collections, analyzed_data_colls)
            .map(|(data_coll, analyzed)| {
                let setup_key = FalkorDBGraphElement {
                    connection: data_coll.spec.connection.clone(),
                    typ: analyzed.schema.elem_type.clone(),
                };
                let desired_setup_state = SetupState::new(
                    &analyzed.schema,
                    &data_coll.index_options,
                    analyzed
                        .dependent_node_labels()
                        .into_iter()
                        .map(|s| s.to_string())
                        .collect(),
                )?;

                let conn_spec = context
                    .auth_registry
                    .get::<ConnectionSpec>(&data_coll.spec.connection)?;
                let factory = self.clone();
                let export_context = async move {
                    Ok(Arc::new(ExportContext::new(
                        factory.graph_pool.get_client(&conn_spec).await?,
                        data_coll.spec,
                        analyzed,
                    )?))
                }
                .boxed();

                Ok(TypedExportDataCollectionBuildOutput {
                    export_context,
                    setup_key,
                    desired_setup_state,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let decl_output = std::iter::zip(declarations, declared_graph_elements)
            .map(|(decl, graph_elem_schema)| {
                let setup_state =
                    SetupState::new(&graph_elem_schema, &decl.decl.index_options, vec![])?;
                let setup_key = GraphElementType {
                    connection: decl.connection,
                    typ: graph_elem_schema.elem_type.clone(),
                };
                Ok((setup_key, setup_state))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((data_coll_output, decl_output))
    }

    async fn diff_setup_states(
        &self,
        key: FalkorDBGraphElement,
        desired: Option<SetupState>,
        existing: CombinedState<SetupState>,
        flow_instance_ctx: Arc<FlowInstanceContext>,
    ) -> Result<Self::SetupChange> {
        let conn_spec = flow_instance_ctx
            .auth_registry
            .get::<ConnectionSpec>(&key.connection)?;
        let data_status = GraphElementDataSetupChange::new(desired.as_ref(), &existing);
        let components = components::SetupChange::create(
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

    fn describe_resource(&self, key: &FalkorDBGraphElement) -> Result<String> {
        Ok(format!("FalkorDB {}", key.typ))
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

            for mut_with_ctx in muts.iter() {
                let export_ctx = &mut_with_ctx.export_context;

                // Execute upserts
                for upsert in mut_with_ctx.mutation.upserts.iter() {
                    if export_ctx.delete_before_upsert {
                        let delete_params = export_ctx.build_delete_params(&upsert.key)?;
                        export_ctx
                            .execute_query(&export_ctx.delete_cypher, delete_params)
                            .await?;
                    }

                    let params = export_ctx.build_params(upsert)?;
                    export_ctx
                        .execute_query(&export_ctx.insert_cypher, params)
                        .await?;
                }
            }

            // Execute deletes in reverse order
            for mut_with_ctx in muts.iter().rev() {
                let export_ctx = &mut_with_ctx.export_context;
                for deletion in mut_with_ctx.mutation.deletes.iter() {
                    let params = export_ctx.build_delete_params(&deletion.key)?;
                    export_ctx
                        .execute_query(&export_ctx.delete_cypher, params)
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn apply_setup_changes(
        &self,
        changes: Vec<TypedResourceSetupChangeItem<'async_trait, Self>>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<()> {
        // Relationships first, then nodes, as relationships need to be deleted before nodes they referenced.
        let mut relationship_types = IndexSet::<&FalkorDBGraphElement>::new();
        let mut node_labels = IndexSet::<&FalkorDBGraphElement>::new();
        let mut dependent_node_labels = IndexSet::<FalkorDBGraphElement>::new();

        let mut components = vec![];
        for change in changes.iter() {
            if let Some(data_clear) = &change.setup_change.0.data_clear {
                match &change.key.typ {
                    ElementType::Relationship(_) => {
                        relationship_types.insert(&change.key);
                        for label in &data_clear.dependent_node_labels {
                            dependent_node_labels.insert(FalkorDBGraphElement {
                                connection: change.key.connection.clone(),
                                typ: ElementType::Node(label.clone()),
                            });
                        }
                    }
                    ElementType::Node(_) => {
                        node_labels.insert(&change.key);
                    }
                }
            }
            components.push(&change.setup_change.1);
        }

        // Relationships have no dependency, so can be cleared first.
        for rel_type in relationship_types.into_iter() {
            let client = self
                .graph_pool
                .get_client_for_key(rel_type, &context.auth_registry)
                .await?;
            clear_graph_element_data(&client, rel_type, true).await?;
        }
        // Clear standalone nodes, which is simpler than dependent nodes.
        for node_label in node_labels.iter() {
            let client = self
                .graph_pool
                .get_client_for_key(node_label, &context.auth_registry)
                .await?;
            clear_graph_element_data(&client, node_label, true).await?;
        }
        // Clear dependent nodes if they're not covered by standalone nodes.
        for node_label in dependent_node_labels.iter() {
            if !node_labels.contains(node_label) {
                let client = self
                    .graph_pool
                    .get_client_for_key(node_label, &context.auth_registry)
                    .await?;
                clear_graph_element_data(&client, node_label, false).await?;
            }
        }

        apply_component_changes(components, &()).await?;
        Ok(())
    }
}
