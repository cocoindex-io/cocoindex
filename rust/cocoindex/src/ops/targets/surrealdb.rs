use crate::prelude::*;

use super::shared::property_graph::*;
use crate::ops::registry::ExecutorFactoryRegistry;
use crate::{ops::sdk::*, setup::CombinedState};
use async_trait::async_trait;
use blake2::Digest;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use surrealdb::Surreal;
use surrealdb::engine::remote::ws::{Client, Ws};
use surrealdb::opt::auth::Root;
use tokio::sync::OnceCell;

////////////////////////////////////////////////////////////
// Public Types
////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, Clone)]
pub struct ConnectionSpec {
    /// Websocket RPC endpoint, e.g. `ws://localhost:8000/rpc`.
    endpoint: String,
    /// Namespace to use.
    namespace: String,
    /// Database to use.
    database: String,
    /// Root username.
    username: String,
    /// Root password.
    password: String,
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

////////////////////////////////////////////////////////////
// Connection Pool
////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct DbKey {
    endpoint: String,
    namespace: String,
    database: String,
    username: String,
}

impl DbKey {
    fn from_spec(spec: &ConnectionSpec) -> Self {
        Self {
            endpoint: spec.endpoint.clone(),
            namespace: spec.namespace.clone(),
            database: spec.database.clone(),
            username: spec.username.clone(),
        }
    }
}

#[derive(Default)]
struct DbPool {
    dbs: Mutex<HashMap<DbKey, Arc<OnceCell<Arc<Surreal<Client>>>>>>,
}

impl DbPool {
    async fn get_db(&self, spec: &ConnectionSpec) -> Result<Arc<Surreal<Client>>> {
        let key = DbKey::from_spec(spec);
        let cell = {
            let mut dbs = self.dbs.lock().unwrap();
            dbs.entry(key).or_default().clone()
        };
        let db = cell
            .get_or_try_init(|| async {
                let sdb: Surreal<Client> = Surreal::init();
                sdb.connect::<Ws>(&spec.endpoint).await?;
                sdb.signin(Root {
                    username: &spec.username,
                    password: &spec.password,
                })
                .await?;
                sdb.use_ns(&spec.namespace).use_db(&spec.database).await?;
                anyhow::Ok(Arc::new(sdb))
            })
            .await?;
        Ok(db.clone())
    }

}

////////////////////////////////////////////////////////////
// Setup State
////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct VectorIndexState {
    field_name: String,
    metric: spec::VectorSimilarityMetric,
    vector_size: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    method: Option<spec::VectorIndexMethod>,
    #[serde(default)]
    elem_type: String, // "F32" | "F64"
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct SetupState {
    key_field_names: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rel_endpoints: Option<(String, String)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    dependent_node_labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    vector_indexes: Vec<VectorIndexState>,
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
            SetupAction::Delete => vec![setup::ChangeDescription::Action("Remove table".to_string())],
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
                            .iter()
                            .map(|v| format!(
                                "{}[{}] {} {}",
                                v.field_name,
                                v.vector_size,
                                v.metric,
                                v.method
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
    conn_ref: spec::AuthEntryReference<ConnectionSpec>,
    db: Arc<Surreal<Client>>,
    analyzed_data_coll: AnalyzedDataCollection,
}

fn sanitize_ident(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
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
            let dim = vs
                .dimension
                .ok_or_else(|| api_error!("Vector index field must be a vector with fixed dimension"))?;
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

fn build_table_setup_sql(table: &str, state: &SetupState) -> Result<String> {
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
        }
    }

    // Primary key unique index
    if !state.key_field_names.is_empty() {
        let idx_name = format!("__{table}__pk__idx");
        let fields = state
            .key_field_names
            .iter()
            .map(|f| sanitize_ident(f))
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!(
            "DEFINE INDEX OVERWRITE {idx_name} ON TABLE {table} FIELDS {fields} UNIQUE;\n"
        ));
    }

    // Vector indexes (HNSW)
    for v in state.vector_indexes.iter() {
        let field = sanitize_ident(&v.field_name);
        let idx_name = format!(
            "__{table}__{}__{}__hnsw__idx",
            sanitize_ident(&v.field_name),
            v.metric
        );
        let dist = metric_to_surreal(v.metric)?;
        let method_params = method_to_surreal_params(&v.method)?;
        out.push_str(&format!(
            "DEFINE FIELD OVERWRITE {field} ON TABLE {table} TYPE array<float>;\n"
        ));
        out.push_str(&format!(
            "DEFINE INDEX OVERWRITE {idx_name} ON TABLE {table} FIELDS {field} HNSW DIMENSION {} DIST {dist} TYPE {}{method_params};\n",
            v.vector_size, v.elem_type
        ));
    }

    Ok(out)
}

fn encode_value_as_json(schema: &schema::ValueType, value: &value::Value) -> Result<serde_json::Value> {
    Ok(serde_json::to_value(TypedValue { t: schema, v: value })?)
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

async fn run_surql(db: &Surreal<Client>, query: String) -> Result<()> {
    if query.trim().is_empty() {
        return Ok(());
    }
    db.query(query).await?;
    Ok(())
}

////////////////////////////////////////////////////////////
// Factory implementation
////////////////////////////////////////////////////////////

type SurrealGraphElement = GraphElementType<ConnectionSpec>;

#[derive(Default)]
pub struct Factory {
    db_pool: DbPool,
}

#[async_trait]
impl TargetFactoryBase for Factory {
    type Spec = Spec;
    type DeclarationSpec = Declaration;
    type SetupState = SetupState;
    type SetupChange = SetupChange;

    type SetupKey = SurrealGraphElement;
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
        Vec<(SurrealGraphElement, SetupState)>,
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

        let data_coll_outputs: Vec<TypedExportDataCollectionBuildOutput<Self>> =
            std::iter::zip(data_collections, analyzed_data_colls.into_iter())
                .map(|(data_coll, analyzed)| {
                    if !data_coll.index_options.fts_indexes.is_empty() {
                        api_bail!("FTS indexes are not supported for SurrealDB target yet");
                    }
                    let value_field_types = analyzed
                        .schema
                        .value_fields
                        .iter()
                        .map(|f| (f.name.as_str(), &f.value_type.typ))
                        .collect::<HashMap<_, _>>();

                    let mut vector_indexes = vec![];
                    for index_def in data_coll.index_options.vector_indexes.iter() {
                        let field_typ = value_field_types.get(index_def.field_name.as_str()).ok_or_else(|| {
                            api_error!("Unknown field name for vector index: {}", index_def.field_name)
                        })?;
                        let (dim, elem_type) = parse_vector_field(field_typ)?;
                        // Only HNSW is supported
                        if let Some(method) = &index_def.method {
                            if matches!(method, spec::VectorIndexMethod::IvfFlat { .. }) {
                                api_bail!("IvfFlat vector index method is not supported for SurrealDB target");
                            }
                        }
                        // Validate metric mapping
                        let _ = metric_to_surreal(index_def.metric)?;
                        vector_indexes.push(VectorIndexState {
                            field_name: index_def.field_name.clone(),
                            metric: index_def.metric,
                            vector_size: dim,
                            method: index_def.method.clone(),
                            elem_type: elem_type.to_string(),
                        });
                    }

                    let desired_setup_state = SetupState {
                        key_field_names: analyzed.schema.key_fields.iter().map(|f| f.name.clone()).collect(),
                        rel_endpoints: analyzed.rel.as_ref().map(|rel| {
                            (
                                rel.source.schema.elem_type.label().to_string(),
                                rel.target.schema.elem_type.label().to_string(),
                            )
                        }),
                        dependent_node_labels: analyzed
                            .dependent_node_labels()
                            .into_iter()
                            .map(|s| s.to_string())
                            .collect(),
                        vector_indexes,
                    };
                    let setup_key = SurrealGraphElement {
                        connection: data_coll.spec.connection.clone(),
                        typ: analyzed.schema.elem_type.clone(),
                    };

                    let pool = self.clone();
                    let conn_ref = data_coll.spec.connection.clone();
                    let ctx = context.clone();
                    let export_context = async move {
                        let conn_spec = ctx.auth_registry.get::<ConnectionSpec>(&conn_ref)?;
                        let db = pool.db_pool.get_db(&conn_spec).await?;
                        Ok(Arc::new(ExportContext {
                            conn_ref,
                            db,
                            analyzed_data_coll: analyzed,
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
                if !decl.decl.index_options.fts_indexes.is_empty() {
                    api_bail!("FTS indexes are not supported for SurrealDB target yet");
                }
                let value_field_types = graph_elem_schema
                    .value_fields
                    .iter()
                    .map(|f| (f.name.as_str(), &f.value_type.typ))
                    .collect::<HashMap<_, _>>();
                let mut vector_indexes = vec![];
                for index_def in decl.decl.index_options.vector_indexes.iter() {
                    let field_typ = value_field_types.get(index_def.field_name.as_str()).ok_or_else(|| {
                        api_error!("Unknown field name for vector index: {}", index_def.field_name)
                    })?;
                    let (dim, elem_type) = parse_vector_field(field_typ)?;
                    if let Some(method) = &index_def.method {
                        if matches!(method, spec::VectorIndexMethod::IvfFlat { .. }) {
                            api_bail!("IvfFlat vector index method is not supported for SurrealDB target");
                        }
                    }
                    let _ = metric_to_surreal(index_def.metric)?;
                    vector_indexes.push(VectorIndexState {
                        field_name: index_def.field_name.clone(),
                        metric: index_def.metric,
                        vector_size: dim,
                        method: index_def.method.clone(),
                        elem_type: elem_type.to_string(),
                    });
                }

                let setup_state = SetupState {
                    key_field_names: graph_elem_schema.key_fields.iter().map(|f| f.name.clone()).collect(),
                    rel_endpoints: None,
                    dependent_node_labels: vec![],
                    vector_indexes,
                };
                let setup_key = GraphElementType {
                    connection: decl.connection,
                    typ: graph_elem_schema.elem_type.clone(),
                };
                Ok((setup_key, setup_state))
            })
            .collect::<Result<_>>()?;

        Ok((data_coll_outputs, decl_output))
    }

    async fn diff_setup_states(
        &self,
        _key: SurrealGraphElement,
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
        if desired_state.key_field_names == existing_state.key_field_names
            && desired_state.rel_endpoints == existing_state.rel_endpoints
        {
            Ok(SetupStateCompatibility::Compatible)
        } else {
            Ok(SetupStateCompatibility::NotCompatible)
        }
    }

    fn describe_resource(&self, key: &SurrealGraphElement) -> Result<String> {
        Ok(format!("SurrealDB TABLE {}", key.typ.label()))
    }

    fn extract_additional_key(
        &self,
        _key: &KeyValue,
        value: &FieldValues,
        export_context: &ExportContext,
    ) -> Result<serde_json::Value> {
        let additional_key = if let Some(rel_info) = &export_context.analyzed_data_coll.rel {
            serde_json::to_value((
                (rel_info.source.fields_input_idx).extract_key(&value.fields)?,
                (rel_info.target.fields_input_idx).extract_key(&value.fields)?,
            ))?
        } else {
            serde_json::Value::Null
        };
        Ok(additional_key)
    }

    async fn apply_setup_changes(
        &self,
        setup_change: Vec<TypedResourceSetupChangeItem<'async_trait, Self>>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<()> {
        // Apply in dependency order: nodes before relationships; deletes in reverse.
        let mut items = setup_change;
        items.sort_by_key(|i| match &i.key.typ {
            ElementType::Node(_) => 0u8,
            ElementType::Relationship(_) => 1u8,
        });

        for item in items.iter() {
            let conn_spec = context
                .auth_registry
                .get::<ConnectionSpec>(&item.key.connection)?;
            let db = self.db_pool.get_db(&conn_spec).await?;
            let table = item.key.typ.label();
            match &item.setup_change.action {
                SetupAction::NoChange => {}
                SetupAction::Delete => {
                    // Best-effort teardown.
                    let table = sanitize_ident(table);
                    let q = format!("REMOVE TABLE {table};\n");
                    let _ = db.query(q).await;
                }
                SetupAction::Upsert(state) => {
                    let q = build_table_setup_sql(table, state)?;
                    run_surql(&db, q).await?;
                }
            }
        }
        Ok(())
    }

    async fn apply_mutation(
        &self,
        mutations: Vec<ExportTargetMutationWithContext<'async_trait, Self::ExportContext>>,
    ) -> Result<()> {
        let mut mutations_by_conn: IndexMap<spec::AuthEntryReference<ConnectionSpec>, Vec<_>> =
            IndexMap::new();
        for mutation in mutations.into_iter() {
            mutations_by_conn
                .entry(mutation.export_context.conn_ref.clone())
                .or_default()
                .push(mutation);
        }

        for mutations in mutations_by_conn.into_values() {
            let db = &mutations[0].export_context.db;

            // Apply node-only mutations first, then relationship mutations (which also upsert nodes).
            let (mut rel_mutations, node_mutations): (Vec<_>, Vec<_>) = mutations
                .into_iter()
                .partition(|m| m.export_context.analyzed_data_coll.rel.is_some());

            for m in node_mutations.into_iter() {
                let schema = &m.export_context.analyzed_data_coll.schema;
                let table = schema.elem_type.label();
                for upsert in m.mutation.upserts.iter() {
                    let mut content = encode_key_fields_as_json(&schema.key_fields, &upsert.key)?;
                    // Value fields are in upsert.value.fields (value-fields only). For nodes, schema.value_fields aligns.
                    for (field_schema, value) in std::iter::zip(
                        schema.value_fields.iter(),
                        upsert.value.fields.iter(),
                    ) {
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
                    db.query(format!(
                        "DELETE {}:{};",
                        sanitize_ident(table),
                        rid
                    ))
                    .await?;
                }
            }

            // Relationship mutations: upsert dependent nodes then upsert edges.
            for m in rel_mutations.iter_mut() {
                let schema = &m.export_context.analyzed_data_coll.schema;
                let rel_info = m
                    .export_context
                    .analyzed_data_coll
                    .rel
                    .as_ref()
                    .ok_or_else(invariance_violation)?;
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
                    let mut src_content =
                        encode_key_fields_as_json(&rel_info.source.schema.key_fields, &src_key)?;
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
                    let mut tgt_content =
                        encode_key_fields_as_json(&rel_info.target.schema.key_fields, &tgt_key)?;
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
                    let mut edge_props = encode_key_fields_as_json(&schema.key_fields, &upsert.key)?;
                    let rel_props = encode_mapped_fields_as_json(
                        &schema.value_fields,
                        &m.export_context.analyzed_data_coll.value_fields_input_idx,
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
        Ok(())
    }
}

pub fn register(registry: &mut ExecutorFactoryRegistry) -> Result<()> {
    Factory::default().register(registry)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_table_setup_sql_node_with_hnsw() -> Result<()> {
        let state = SetupState {
            key_field_names: vec!["id".to_string()],
            rel_endpoints: None,
            dependent_node_labels: vec![],
            vector_indexes: vec![VectorIndexState {
                field_name: "embedding".to_string(),
                metric: spec::VectorSimilarityMetric::CosineSimilarity,
                vector_size: 384,
                method: Some(spec::VectorIndexMethod::Hnsw {
                    m: Some(12),
                    ef_construction: Some(150),
                }),
                elem_type: "F32".to_string(),
            }],
        };
        let sql = build_table_setup_sql("Document", &state)?;
        assert!(sql.contains("DEFINE TABLE OVERWRITE Document SCHEMALESS;"));
        assert!(sql.contains(
            "DEFINE INDEX OVERWRITE __Document__pk__idx ON TABLE Document FIELDS id UNIQUE;"
        ));
        assert!(sql.contains("DEFINE FIELD OVERWRITE embedding ON TABLE Document TYPE array<float>;"));
        assert!(sql.contains("HNSW DIMENSION 384 DIST COSINE TYPE F32"));
        assert!(sql.contains("EFC 150"));
        assert!(sql.contains("M 12"));
        Ok(())
    }

    #[test]
    fn test_build_table_setup_sql_relation() -> Result<()> {
        let state = SetupState {
            key_field_names: vec!["rel_id".to_string()],
            rel_endpoints: Some(("Person".to_string(), "Place".to_string())),
            dependent_node_labels: vec![],
            vector_indexes: vec![],
        };
        let sql = build_table_setup_sql("MENTION", &state)?;
        assert!(sql.contains(
            "DEFINE TABLE OVERWRITE MENTION TYPE RELATION IN Person OUT Place SCHEMALESS;"
        ));
        assert!(sql.contains(
            "DEFINE INDEX OVERWRITE __MENTION__pk__idx ON TABLE MENTION FIELDS rel_id UNIQUE;"
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


