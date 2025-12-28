use super::shared::property_graph::*;
use crate::ops::registry::ExecutorFactoryRegistry;
use crate::persistence::surrealdb_pool::{SurrealDBPool, get_surrealdb_pool};
use crate::prelude::*;
use crate::settings::SurrealDBConnectionSpec;
use crate::{ops::sdk::*, setup::CombinedState};
use async_trait::async_trait;
use blake2::Digest;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

////////////////////////////////////////////////////////////
// Public Types
////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct Spec {
    connection: spec::AuthEntryReference<SurrealDBConnectionSpec>,
    mapping: Option<GraphElementMapping>,
    table_name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Declaration {
    connection: spec::AuthEntryReference<SurrealDBConnectionSpec>,
    #[serde(flatten)]
    decl: GraphDeclaration,
}

////////////////////////////////////////////////////////////
// Setup State
////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ExtendedVectorIndexDef {
    #[serde(flatten)]
    index_def: VectorIndexDef,
    vector_size: usize,
    // TODO: review this
    // #[serde(default)]
    field_type: String, // "F32" | "F64"
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct SetupState {
    // TODO: understand this. Do we need them?
    // key_field_names: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rel_endpoints: Option<(String, String)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    dependent_node_labels: Vec<String>,

    vector_indexes: BTreeMap<String, ExtendedVectorIndexDef>,
}

impl SetupState {
    fn new(
        // TODO: do we need this?
        _table_id: &SurrealTableId,
        // TODO: understand key_fields_schema. Do we need them?
        _key_fields_schema: &[FieldSchema],
        value_fields_schema: &[FieldSchema],
        index_options: &IndexOptions,
        // column_options: &HashMap<String, ColumnOptions>,
        graph_analysis: Option<&AnalyzedDataCollection>,
    ) -> Result<Self> {
        if !index_options.fts_indexes.is_empty() {
            api_bail!("FTS indexes are not supported for SurrealDB target yet");
        }

        let mut vector_indexes = BTreeMap::new();
        for index_def in index_options.vector_indexes.iter() {
            let field_typ = value_fields_schema
                .iter()
                .find(|f| f.name == index_def.field_name.as_str())
                .ok_or(anyhow!("field not found"))?;
            let (dim, field_type) = parse_vector_field(&field_typ.value_type.typ)?;

            // Only HNSW is supported
            if let Some(method) = &index_def.method {
                if matches!(method, spec::VectorIndexMethod::IvfFlat { .. }) {
                    api_bail!("IvfFlat vector index method is not supported for SurrealDB target");
                }
            }

            // Validate metric mapping
            let _ = metric_to_surreal(index_def.metric)?;

            vector_indexes.insert(
                // to_vector_index_name(&table_id.table_name, &index_def),
                index_def.field_name.clone(),
                ExtendedVectorIndexDef {
                    index_def: index_def.clone(),
                    vector_size: dim,
                    field_type: field_type.to_string(),
                },
            );
        }

        let rel_endpoints = match graph_analysis.as_ref() {
            Some(analyzed) => analyzed.rel.as_ref().map(|rel| {
                (
                    rel.source.schema.elem_type.label().to_string(),
                    rel.target.schema.elem_type.label().to_string(),
                )
            }),
            None => None,
        };
        let dependent_node_labels = match graph_analysis.as_ref() {
            Some(analyzed) => analyzed
                .dependent_node_labels()
                .into_iter()
                .map(|s| s.to_string())
                .collect(),
            None => Vec::new(),
        };
        Ok(Self {
            // key_field_names: key_fields_schema.iter().map(|f| f.name.clone()).collect(),
            rel_endpoints,
            dependent_node_labels,
            vector_indexes,
        })
    }
}

#[derive(Debug)]
pub enum SetupAction {
    NoChange,
    Upsert(SetupState),
    Delete,
}

#[derive(Debug)]
pub struct SetupChange {
    existing: bool,
    action: SetupAction,
}

impl setup::ResourceSetupChange for SetupChange {
    fn describe_changes(&self) -> Vec<setup::ChangeDescription> {
        match &self.action {
            SetupAction::NoChange => vec![],
            SetupAction::Delete => {
                vec![setup::ChangeDescription::Action("Remove table".to_string())]
            }
            SetupAction::Upsert(state) => {
                let mut changes = vec![setup::ChangeDescription::Action(if self.existing {
                    "Update table".to_string()
                } else {
                    "Create table".to_string()
                })];
                if !state.vector_indexes.is_empty() {
                    changes.push(setup::ChangeDescription::Note(format!(
                        "Vector indexes: {}",
                        state
                            .vector_indexes
                            .values()
                            .map(|v| format!(
                                "{}[{}] {} {}",
                                v.index_def.field_name,
                                v.vector_size,
                                v.index_def.metric,
                                v.index_def
                                    .method
                                    .as_ref()
                                    .map(|m| m.to_string())
                                    .unwrap_or_else(|| "Hnsw".to_string())
                            ))
                            .collect::<Vec<_>>()
                            .join("; ")
                    )));
                }
                changes
            }
        }
    }

    fn change_type(&self) -> setup::SetupChangeType {
        match (&self.action, self.existing) {
            (SetupAction::NoChange, _) => setup::SetupChangeType::NoChange,
            (SetupAction::Delete, true) => setup::SetupChangeType::Delete,
            (SetupAction::Delete, false) => setup::SetupChangeType::NoChange,
            (SetupAction::Upsert(_), false) => setup::SetupChangeType::Create,
            (SetupAction::Upsert(_), true) => setup::SetupChangeType::Update,
        }
    }
}

////////////////////////////////////////////////////////////
// Export Context + Mutation Helpers
////////////////////////////////////////////////////////////

pub struct ExportContext {
    db_ref: spec::AuthEntryReference<SurrealDBConnectionSpec>,
    db_pool: SurrealDBPool,
    // key_fields_schema: Box<[(FieldSchema, Option<ColumnOptions>)]>,
    table_name: String,
    value_fields_schema: Vec<FieldSchema>,
    analyzed_data_coll: Option<AnalyzedDataCollection>,
}

fn sanitize_ident(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn canonical_json(v: &serde_json::Value, out: &mut String) {
    match v {
        serde_json::Value::Null => out.push_str("null"),
        serde_json::Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        serde_json::Value::Number(n) => out.push_str(&n.to_string()),
        serde_json::Value::String(s) => out.push_str(&serde_json::to_string(s).unwrap()),
        serde_json::Value::Array(a) => {
            out.push('[');
            for (i, item) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canonical_json(item, out);
            }
            out.push(']');
        }
        serde_json::Value::Object(m) => {
            out.push('{');
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(k).unwrap());
                out.push(':');
                canonical_json(&m[*k].clone(), out);
            }
            out.push('}');
        }
    }
}

fn stable_hash_json(v: &serde_json::Value) -> String {
    let mut s = String::new();
    canonical_json(v, &mut s);
    let mut hasher = blake2::Blake2b512::new();
    hasher.update(s.as_bytes());
    hex::encode(hasher.finalize())
}

fn record_id(table: &str, key: &KeyValue, additional_key: &serde_json::Value) -> String {
    let v = serde_json::json!({
        "k": key,
        "a": additional_key,
    });
    format!("{}__{}", sanitize_ident(table), stable_hash_json(&v))
}

fn parse_vector_field(field_type: &schema::ValueType) -> Result<(usize, &'static str)> {
    match field_type {
        schema::ValueType::Basic(schema::BasicValueType::Vector(vs)) => {
            let dim = vs.dimension.ok_or_else(|| {
                api_error!("Vector index field must be a vector with fixed dimension")
            })?;
            let elem = match &*vs.element_type {
                schema::BasicValueType::Float32 => "F32",
                schema::BasicValueType::Float64 => "F64",
                schema::BasicValueType::Int64 => "F32",
                t => api_bail!("Unsupported vector element type for SurrealDB: {t}"),
            };
            Ok((dim, elem))
        }
        t => api_bail!("Vector index field must be a vector type, got: {t}"),
    }
}

fn metric_to_surreal(metric: spec::VectorSimilarityMetric) -> Result<&'static str> {
    Ok(match metric {
        spec::VectorSimilarityMetric::CosineSimilarity => "COSINE",
        spec::VectorSimilarityMetric::L2Distance => "EUCLIDEAN",
        spec::VectorSimilarityMetric::InnerProduct => {
            api_bail!("InnerProduct vector metric is not supported for SurrealDB target yet")
        }
    })
}

fn method_to_surreal_params(method: &Option<spec::VectorIndexMethod>) -> Result<String> {
    Ok(match method {
        None => "".to_string(),
        Some(spec::VectorIndexMethod::Hnsw { m, ef_construction }) => {
            let mut parts = vec![];
            if let Some(efc) = ef_construction {
                parts.push(format!("EFC {efc}"));
            }
            if let Some(m) = m {
                parts.push(format!("M {m}"));
            }
            if parts.is_empty() {
                "".to_string()
            } else {
                format!(" {}", parts.join(" "))
            }
        }
        Some(spec::VectorIndexMethod::IvfFlat { .. }) => {
            api_bail!("IvfFlat vector index method is not supported for SurrealDB target")
        }
    })
}

fn build_table_setup_surql(table: &str, state: &SetupState) -> Result<String> {
    let table = sanitize_ident(table);
    let mut out = String::new();

    match &state.rel_endpoints {
        None => {
            out.push_str(&format!("DEFINE TABLE OVERWRITE {table} SCHEMALESS;\n"));
        }
        Some((src, tgt)) => {
            out.push_str(&format!(
                "DEFINE TABLE OVERWRITE {table} TYPE RELATION IN {} OUT {} SCHEMALESS;\n",
                sanitize_ident(src),
                sanitize_ident(tgt)
            ));

            // Unique index for relations
            let idx_name = format!("__{table}__unique__idx");
            out.push_str(&format!(
                "DEFINE INDEX OVERWRITE {idx_name} ON {table} FIELDS {}, {} UNIQUE;\n",
                sanitize_ident(src),
                sanitize_ident(tgt)
            ));
        }
    }

    // Vector indexes (HNSW)
    for (key, v) in state.vector_indexes.iter() {
        let field = sanitize_ident(&key);
        let idx_name = to_vector_index_name(&table, &v.index_def);
        let dist = metric_to_surreal(v.index_def.metric)?;
        let method_params = method_to_surreal_params(&v.index_def.method)?;
        out.push_str(&format!(
            "DEFINE FIELD OVERWRITE {field} ON {table} TYPE array<float>;\n"
        ));
        out.push_str(&format!(
            "DEFINE INDEX OVERWRITE {idx_name} ON {table} FIELDS {field} HNSW DIMENSION {} DIST {dist} TYPE {}{method_params};\n",
            v.vector_size, v.field_type
        ));
    }

    Ok(out)
}

fn to_vector_index_name(table_name: &str, vector_index_def: &spec::VectorIndexDef) -> String {
    format!(
        "__{table_name}__{}__{}__hnsw__idx",
        vector_index_def.field_name, vector_index_def.metric
    )
}

fn encode_value_as_json(
    schema: &schema::ValueType,
    value: &value::Value,
) -> Result<serde_json::Value> {
    Ok(serde_json::to_value(TypedValue {
        t: schema,
        v: value,
    })?)
}

fn encode_key_fields_as_json(
    key_fields: &[schema::FieldSchema],
    key: &KeyValue,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    if key_fields.len() != key.len() {
        bail!(
            "Key fields number mismatch: {} vs {}",
            key_fields.len(),
            key.len()
        );
    }
    let mut obj = serde_json::Map::new();
    for (field_schema, key_part) in std::iter::zip(key_fields.iter(), key.iter()) {
        let v = value::Value::from(key_part);
        obj.insert(
            field_schema.name.clone(),
            encode_value_as_json(&field_schema.value_type.typ, &v)?,
        );
    }
    Ok(obj)
}

fn encode_mapped_fields_as_json(
    fields_schema: &[schema::FieldSchema],
    fields_input_idx: &[usize],
    field_values: &FieldValues,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    let mut obj = serde_json::Map::new();
    for (field_schema, idx) in std::iter::zip(fields_schema.iter(), fields_input_idx.iter()) {
        let v = &field_values.fields[*idx];
        obj.insert(
            field_schema.name.clone(),
            encode_value_as_json(&field_schema.value_type.typ, v)?,
        );
    }
    Ok(obj)
}

async fn run_surql(db: &Surreal<Any>, query: String) -> Result<()> {
    if query.trim().is_empty() {
        return Ok(());
    }
    db.query(query).await?;
    Ok(())
}

////////////////////////////////////////////////////////////
// Factory implementation
////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct SurrealTableId {
    connection: spec::AuthEntryReference<SurrealDBConnectionSpec>,
    table_name: String,
    // #[serde(skip_serializing_if = "Option::is_none")]
    mapping: Option<ElementType>,
    // TODO: add optional schema
    // #[serde(skip_serializing_if = "Option::is_none")]
    // schema: Option<String>,
}

impl std::fmt::Display for SurrealTableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO: add optional schema
        // if let Some(schema) = &self.schema {
        //     write!(f, "{}.{}", schema, self.table_name)?;
        // } else {
        //     write!(f, "{}", self.table_name)?;
        // }
        write!(f, "{}", self.table_name)?;
        Ok(())
    }
}

struct TargetFactory;

#[async_trait]
impl TargetFactoryBase for TargetFactory {
    type Spec = Spec;
    type DeclarationSpec = Declaration;
    type SetupState = SetupState;
    type SetupChange = SetupChange;

    type SetupKey = SurrealTableId;
    type ExportContext = ExportContext;

    fn name(&self) -> &str {
        "SurrealDB"
    }

    async fn build(
        self: Arc<Self>,
        data_collections: Vec<TypedExportDataCollectionSpec<Self>>,
        declarations: Vec<Declaration>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        Vec<TypedExportDataCollectionBuildOutput<Self>>,
        Vec<(SurrealTableId, SetupState)>,
    )> {
        let mut graph_collections = vec![];
        let mut other_collections = vec![];
        for collection in data_collections.iter() {
            match collection.spec.mapping {
                Some(_) => {
                    graph_collections.push(collection);
                }
                None => {
                    other_collections.push(collection);
                }
            }
        }

        let (analyzed_data_colls, declared_graph_elements) = analyze_graph_mappings(
            graph_collections.iter().filter_map(|d| {
                if let Some(mapping) = d.spec.mapping.as_ref() {
                    let x = DataCollectionGraphMappingInput {
                        auth_ref: &d.spec.connection,
                        mapping: mapping,
                        index_options: &d.index_options,
                        key_fields_schema: d.key_fields_schema.clone(),
                        value_fields_schema: d.value_fields_schema.clone(),
                    };
                    Some(x)
                } else {
                    None
                }
            }),
            declarations.iter().map(|d| (&d.connection, &d.decl)),
        )?;

        let part1 = std::iter::zip(
            graph_collections,
            analyzed_data_colls.into_iter().map(|x| Some(x)),
        );
        let part2 = other_collections.into_iter().map(|x| (x, None));

        let data_coll_outputs: Vec<TypedExportDataCollectionBuildOutput<Self>> = part1
            .chain(part2)
            .map(|(d, graph_analysis)| {
                let setup_key = SurrealTableId {
                    connection: d.spec.connection.clone(),
                    table_name: d.spec.table_name.clone().unwrap_or_else(|| {
                        utils::db::sanitize_identifier(&format!(
                            "{}__{}",
                            context.flow_instance_name, d.name
                        ))
                    }),
                    mapping: d
                        .spec
                        .mapping
                        .as_ref()
                        .map(|x| ElementType::from_mapping_spec(&x)),
                };
                let desired_setup_state = SetupState::new(
                    &setup_key,
                    &d.key_fields_schema,
                    &d.value_fields_schema,
                    &d.index_options,
                    graph_analysis.as_ref(),
                )?;

                let db_ref = d.spec.connection.clone();
                let ctx = context.clone();
                // TODO: how come table name is not specified?
                let table_name = d
                    .spec
                    .table_name
                    .clone()
                    .ok_or(anyhow!("Table name not specified"))?;
                let value_fields_schema = d.value_fields_schema.clone();
                let export_context = async move {
                    let dbconf = ctx.auth_registry.get::<SurrealDBConnectionSpec>(&db_ref)?;
                    Ok(Arc::new(ExportContext {
                        db_ref,
                        db_pool: SurrealDBPool::new(dbconf),
                        table_name,
                        value_fields_schema,
                        analyzed_data_coll: graph_analysis,
                    }))
                }
                .boxed();

                Ok(TypedExportDataCollectionBuildOutput {
                    export_context,
                    setup_key,
                    desired_setup_state,
                })
            })
            .collect::<Result<_>>()?;

        // Declarations create setup states for extra node labels.
        let decl_output = std::iter::zip(declarations, declared_graph_elements)
            .map(|(decl, graph_elem_schema)| {
                let setup_key = SurrealTableId {
                    connection: decl.connection,
                    table_name: graph_elem_schema.elem_type.label().to_string(),
                    mapping: Some(graph_elem_schema.elem_type.clone()),
                };
                let setup_state = SetupState::new(
                    &setup_key,
                    &graph_elem_schema.key_fields,
                    &graph_elem_schema.value_fields,
                    &decl.decl.index_options,
                    None,
                )?;
                Ok((setup_key, setup_state))
            })
            .collect::<Result<_>>()?;

        Ok((data_coll_outputs, decl_output))
    }

    async fn diff_setup_states(
        &self,
        _key: SurrealTableId,
        desired_state: Option<SetupState>,
        existing_states: CombinedState<SetupState>,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<SetupChange> {
        let existing = existing_states.current.is_some()
            || !existing_states.staging.is_empty()
            || existing_states.legacy_state_key.is_some();
        let desired_is_same = desired_state
            .as_ref()
            .is_some_and(|d| existing_states.always_exists_and(|s| s == d));
        if desired_is_same || (!existing && desired_state.is_none()) {
            return Ok(SetupChange {
                existing,
                action: SetupAction::NoChange,
            });
        }
        Ok(SetupChange {
            existing,
            action: match desired_state {
                None => SetupAction::Delete,
                Some(state) => SetupAction::Upsert(state),
            },
        })
    }

    fn check_state_compatibility(
        &self,
        desired_state: &SetupState,
        existing_state: &SetupState,
    ) -> Result<SetupStateCompatibility> {
        // if desired_state.key_field_names == existing_state.key_field_names
        //     && desired_state.rel_endpoints == existing_state.rel_endpoints {
        if desired_state.rel_endpoints == existing_state.rel_endpoints {
            Ok(SetupStateCompatibility::Compatible)
        } else {
            Ok(SetupStateCompatibility::NotCompatible)
        }
    }

    fn describe_resource(&self, key: &SurrealTableId) -> Result<String> {
        Ok(format!("SurrealDB TABLE {}", key))
    }

    fn extract_additional_key(
        &self,
        _key: &KeyValue,
        value: &FieldValues,
        export_context: &ExportContext,
    ) -> Result<serde_json::Value> {
        let additional_key = match &export_context.analyzed_data_coll {
            Some(x) => match &x.rel {
                Some(rel_info) => serde_json::to_value((
                    (rel_info.source.fields_input_idx).extract_key(&value.fields)?,
                    (rel_info.target.fields_input_idx).extract_key(&value.fields)?,
                ))?,
                None => serde_json::Value::Null,
            },
            None => serde_json::Value::Null,
        };
        Ok(additional_key)
    }

    async fn apply_setup_changes(
        &self,
        changes: Vec<TypedResourceSetupChangeItem<'async_trait, Self>>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<()> {
        // Apply in dependency order: nodes before relationships; deletes in reverse.
        let mut changes_sorted = changes;
        changes_sorted.sort_by_key(|i| match &i.key.mapping {
            None => 0u8,
            Some(ElementType::Node(_)) => 1u8,
            Some(ElementType::Relationship(_)) => 2u8,
        });

        for change in changes_sorted.iter() {
            let db =
                get_surrealdb_pool(Some(&change.key.connection), &context.auth_registry).await?;
            let conn = db.get_db().await?;

            // TODO: implement apply_change in SetupChange
            // change.setup_change.apply_change(&db, &change.key).await?;

            match &change.setup_change.action {
                SetupAction::NoChange => {}
                SetupAction::Delete => {
                    // Best-effort teardown.
                    let table = sanitize_ident(&change.key.table_name);
                    let q = format!("REMOVE TABLE {table};\n");
                    let _ = conn.query(q).await;
                }
                SetupAction::Upsert(state) => {
                    let q = build_table_setup_surql(&change.key.table_name, state)?;
                    run_surql(&conn, q).await?;
                }
            }
        }
        Ok(())
    }

    async fn apply_mutation(
        &self,
        mutations: Vec<ExportTargetMutationWithContext<'async_trait, Self::ExportContext>>,
    ) -> Result<()> {
        let mut mutations_by_conn: IndexMap<
            spec::AuthEntryReference<SurrealDBConnectionSpec>,
            Vec<_>,
        > = IndexMap::new();
        for mutation in mutations.into_iter() {
            mutations_by_conn
                .entry(mutation.export_context.db_ref.clone())
                .or_default()
                .push(mutation);
        }

        for mutations in mutations_by_conn.into_values() {
            let db = &mutations[0].export_context.db_pool.get_db().await?;

            // Apply node-only mutations first, then relationship mutations (which also upsert nodes).
            let (mut rel_mutations, node_mutations): (Vec<_>, Vec<_>) = mutations
                .into_iter()
                .partition(|m| match &m.export_context.analyzed_data_coll {
                    Some(analyzed) => analyzed.rel.is_some(),
                    None => false,
                });

            // Mutations for non-graph records
            for m in node_mutations.iter() {
                if m.export_context.analyzed_data_coll.is_none() {
                    // ---------------------------------------------------------
                    // TODO: next ----------------------------------------------
                    // ---------------------------------------------------------
                    let table_name = m.export_context.table_name.clone();
                    for upsert in m.mutation.upserts.iter() {
                        let mut content = encode_key_fields_as_json(
                            &m.export_context.value_fields_schema,
                            &upsert.key,
                        )?;
                        // Value fields are in upsert.value.fields (value-fields only). For nodes, schema.value_fields aligns.
                        for (field_schema, value) in std::iter::zip(
                            m.export_context.value_fields_schema.iter(),
                            upsert.value.fields.iter(),
                        ) {
                            content.insert(
                                field_schema.name.clone(),
                                encode_value_as_json(&field_schema.value_type.typ, value)?,
                            );
                        }
                        let rid = record_id(&table_name, &upsert.key, &upsert.additional_key);
                        db.query(format!(
                            "UPSERT {}:{} MERGE $content;",
                            sanitize_ident(&table_name),
                            rid
                        ))
                        .bind(("content", serde_json::Value::Object(content)))
                        .await?;
                    }
                    for del in m.mutation.deletes.iter() {
                        let rid = record_id(&table_name, &del.key, &del.additional_key);
                        db.query(format!("DELETE {}:{};", sanitize_ident(&table_name), rid))
                            .await?;
                    }
                }
            }

            for m in node_mutations.into_iter() {
                if let Some(analyzed) = &m.export_context.analyzed_data_coll {
                    let schema = &analyzed.schema;
                    let table = schema.elem_type.label();
                    for upsert in m.mutation.upserts.iter() {
                        let mut content =
                            encode_key_fields_as_json(&schema.key_fields, &upsert.key)?;
                        // Value fields are in upsert.value.fields (value-fields only). For nodes, schema.value_fields aligns.
                        for (field_schema, value) in
                            std::iter::zip(schema.value_fields.iter(), upsert.value.fields.iter())
                        {
                            content.insert(
                                field_schema.name.clone(),
                                encode_value_as_json(&field_schema.value_type.typ, value)?,
                            );
                        }
                        let rid = record_id(table, &upsert.key, &upsert.additional_key);
                        db.query(format!(
                            "UPSERT {}:{} MERGE $content;",
                            sanitize_ident(table),
                            rid
                        ))
                        .bind(("content", serde_json::Value::Object(content)))
                        .await?;
                    }
                    for del in m.mutation.deletes.iter() {
                        let rid = record_id(table, &del.key, &del.additional_key);
                        db.query(format!("DELETE {}:{};", sanitize_ident(table), rid))
                            .await?;
                    }
                };
            }

            // Relationship mutations: upsert dependent nodes then upsert edges.
            for m in rel_mutations.iter_mut() {
                if let Some(analyzed) = &m.export_context.analyzed_data_coll {
                    let schema = analyzed.schema.clone();
                    let rel_info = analyzed.rel.as_ref().ok_or_else(invariance_violation)?;
                    let edge_table = schema.elem_type.label();
                    let src_table = rel_info.source.schema.elem_type.label();
                    let tgt_table = rel_info.target.schema.elem_type.label();

                    for upsert in m.mutation.upserts.iter() {
                        // Decode additional_key = (src_key, tgt_key)
                        let mut additional_keys = match upsert.additional_key.clone() {
                            serde_json::Value::Array(keys) => keys,
                            _ => return Err(invariance_violation()),
                        };
                        if additional_keys.len() != 2 {
                            api_bail!(
                                "Expected additional key with 2 fields, got {}",
                                upsert.additional_key
                            );
                        }
                        let src_key = KeyValue::from_json(
                            additional_keys[0].take(),
                            &rel_info.source.schema.key_fields,
                        )?;
                        let tgt_key = KeyValue::from_json(
                            additional_keys[1].take(),
                            &rel_info.target.schema.key_fields,
                        )?;

                        // Upsert source node (from mapped fields in relationship row).
                        let mut src_content = encode_key_fields_as_json(
                            &rel_info.source.schema.key_fields,
                            &src_key,
                        )?;
                        let src_values = encode_mapped_fields_as_json(
                            &rel_info.source.schema.value_fields,
                            &rel_info.source.fields_input_idx.value,
                            &upsert.value,
                        )?;
                        src_content.extend(src_values);
                        let src_rid = record_id(src_table, &src_key, &serde_json::Value::Null);
                        db.query(format!(
                            "UPSERT {}:{} MERGE $content;",
                            sanitize_ident(src_table),
                            src_rid
                        ))
                        .bind(("content", serde_json::Value::Object(src_content)))
                        .await?;

                        // Upsert target node.
                        let mut tgt_content = encode_key_fields_as_json(
                            &rel_info.target.schema.key_fields,
                            &tgt_key,
                        )?;
                        let tgt_values = encode_mapped_fields_as_json(
                            &rel_info.target.schema.value_fields,
                            &rel_info.target.fields_input_idx.value,
                            &upsert.value,
                        )?;
                        tgt_content.extend(tgt_values);
                        let tgt_rid = record_id(tgt_table, &tgt_key, &serde_json::Value::Null);
                        db.query(format!(
                            "UPSERT {}:{} MERGE $content;",
                            sanitize_ident(tgt_table),
                            tgt_rid
                        ))
                        .bind(("content", serde_json::Value::Object(tgt_content)))
                        .await?;

                        // Upsert edge record with in/out links and relationship properties.
                        let mut edge_props =
                            encode_key_fields_as_json(&schema.key_fields, &upsert.key)?;
                        let rel_props = encode_mapped_fields_as_json(
                            &schema.value_fields,
                            &analyzed.value_fields_input_idx,
                            &upsert.value,
                        )?;
                        edge_props.extend(rel_props);

                        let edge_rid = record_id(edge_table, &upsert.key, &upsert.additional_key);

                        let in_thing = surrealdb::sql::Thing::from((
                            sanitize_ident(src_table),
                            src_rid.clone(),
                        ));
                        let out_thing = surrealdb::sql::Thing::from((
                            sanitize_ident(tgt_table),
                            tgt_rid.clone(),
                        ));

                        // First set in/out as Things, then merge props.
                        db.query(format!(
                        "UPSERT {}:{} MERGE {{ in: $in, out: $out }}; UPSERT {}:{} MERGE $props;",
                        sanitize_ident(edge_table),
                        edge_rid,
                        sanitize_ident(edge_table),
                        edge_rid,
                    ))
                    .bind(("in", in_thing))
                    .bind(("out", out_thing))
                    .bind(("props", serde_json::Value::Object(edge_props)))
                    .await?;
                    }

                    for del in m.mutation.deletes.iter() {
                        let edge_rid = record_id(edge_table, &del.key, &del.additional_key);
                        db.query(format!(
                            "DELETE {}:{};",
                            sanitize_ident(edge_table),
                            edge_rid
                        ))
                        .await?;
                    }
                }
            }
        }
        Ok(())
    }
}

pub fn register(registry: &mut ExecutorFactoryRegistry) -> Result<()> {
    TargetFactory.register(registry)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_table_setup_sql_node_with_hnsw() -> Result<()> {
        let mut vector_indexes = BTreeMap::new();
        vector_indexes.insert(
            "embedding".to_string(),
            ExtendedVectorIndexDef {
                index_def: VectorIndexDef {
                    field_name: "embedding".to_string(),
                    metric: spec::VectorSimilarityMetric::CosineSimilarity,
                    method: Some(spec::VectorIndexMethod::Hnsw {
                        m: Some(12),
                        ef_construction: Some(150),
                    }),
                },
                vector_size: 384,
                field_type: "F32".to_string(),
            },
        );
        let state = SetupState {
            // key_field_names: vec!["id".to_string()],
            // key_field_names: vec![],
            rel_endpoints: None,
            dependent_node_labels: vec![],
            vector_indexes,
        };
        let sql = build_table_setup_surql("Document", &state)?;
        assert_eq!(
            sql,
            r#"DEFINE TABLE OVERWRITE Document SCHEMALESS;
DEFINE FIELD OVERWRITE embedding ON Document TYPE array<float>;
DEFINE INDEX OVERWRITE __Document__embedding__Cosine__hnsw__idx ON Document FIELDS embedding HNSW DIMENSION 384 DIST COSINE TYPE F32 EFC 150 M 12;
"#
        );
        Ok(())
    }

    #[test]
    fn test_build_table_setup_sql_relation() -> Result<()> {
        let vector_indexes = BTreeMap::new();
        let state = SetupState {
            // key_field_names: vec!["rel_id".to_string()],
            // key_field_names: vec![],
            rel_endpoints: Some(("Person".to_string(), "Place".to_string())),
            dependent_node_labels: vec![],
            vector_indexes,
        };
        let sql = build_table_setup_surql("MENTION", &state)?;
        assert_eq!(
            sql,
            r#"DEFINE TABLE OVERWRITE MENTION TYPE RELATION IN Person OUT Place SCHEMALESS;
DEFINE INDEX OVERWRITE __MENTION__unique__idx ON MENTION FIELDS Person, Place UNIQUE;
"#
        );
        assert!(sql.contains(
            "DEFINE TABLE OVERWRITE MENTION TYPE RELATION IN Person OUT Place SCHEMALESS;"
        ));
        assert!(sql.contains(
            "DEFINE INDEX OVERWRITE __MENTION__unique__idx ON MENTION FIELDS Person, Place UNIQUE;"
        ));
        Ok(())
    }

    #[test]
    fn test_record_id_is_deterministic() {
        let k1 = KeyValue::from(vec![KeyPart::Str("a".into())]);
        let ak = serde_json::json!([1, 2, 3]);
        let id1 = record_id("Document", &k1, &ak);
        let id2 = record_id("Document", &k1, &ak);
        assert_eq!(id1, id2);

        let k2 = KeyValue::from(vec![KeyPart::Str("b".into())]);
        let id3 = record_id("Document", &k2, &ak);
        assert_ne!(id1, id3);
    }
}
