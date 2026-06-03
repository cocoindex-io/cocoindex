//! Turbopuffer vector-store target connector.
//!
//! Namespace targets reconcile declared rows against the previous run: changed
//! rows are upserted, unchanged rows are skipped, and orphaned rows are deleted.
//! System-managed namespaces are cleared when the vector schema changes.
//!
//! Turbopuffer is a hosted service; this talks to its v2 HTTP API via `reqwest`
//! (no native crate). Namespaces are created implicitly on first write.

use std::{collections::BTreeMap, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue, json};

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::resources::schema::{VectorElementType, VectorSchema, VectorSchemaProvider};
use crate::statediff::{
    DiffAction, ManagedBy, ManagedTargetOptions, MutualTrackingRecord, diff,
    resolve_system_transition,
};
use crate::target_state::{
    ChildTargetDef, StableKey, TargetAction, TargetActionSink, TargetChildInvalidation,
    TargetHandler, TargetReconcileOutput, TargetState, TargetStateProvider, declare_target_state,
    declare_target_state_with_child, mount_target, register_root_target_states_provider,
};

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

struct ConnInner {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    state_id: String,
}

/// A Turbopuffer connection. Clone-cheap (shared client).
#[derive(Clone)]
pub struct TurbopufferConnection {
    inner: Arc<ConnInner>,
}

impl TurbopufferConnection {
    /// Connect to a Turbopuffer region (e.g. `gcp-us-central1`) with an API key.
    pub fn new(region: &str, api_key: &str) -> Self {
        Self::build(
            format!("https://{region}.turbopuffer.com"),
            api_key.to_string(),
            format!("region:{region}"),
        )
    }

    fn build(base_url: String, api_key: String, state_id: String) -> Self {
        Self {
            inner: Arc::new(ConnInner {
                http: reqwest::Client::new(),
                base_url: base_url.trim_end_matches('/').to_string(),
                api_key,
                state_id,
            }),
        }
    }

    /// Override the base URL (default `https://{region}.turbopuffer.com`). Mainly
    /// for pointing at a mock server in tests.
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        let inner = Arc::get_mut(&mut self.inner)
            .expect("with_base_url must be called before the connection is shared");
        inner.base_url = base_url;
        self
    }

    /// Stable identity for use as a `ContextKey` state id / memo dep.
    pub fn state_id(&self) -> &str {
        &self.inner.state_id
    }

    async fn write(&self, namespace: &str, body: JsonValue) -> Result<()> {
        let url = format!("{}/v2/namespaces/{namespace}", self.inner.base_url);
        self.inner
            .http
            .post(&url)
            .bearer_auth(&self.inner.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::engine(format!("turbopuffer write: {e}")))?
            .error_for_status()
            .map_err(|e| Error::engine(format!("turbopuffer write failed: {e}")))?;
        Ok(())
    }

    /// Delete a namespace and all its rows (idempotent — a missing namespace is
    /// treated as already gone). Explicit teardown convenience, e.g. for
    /// tests/examples; not part of reconciliation.
    pub async fn delete_namespace(&self, namespace: &str) -> Result<()> {
        let url = format!("{}/v2/namespaces/{namespace}", self.inner.base_url);
        let resp = self
            .inner
            .http
            .delete(&url)
            .bearer_auth(&self.inner.api_key)
            .send()
            .await
            .map_err(|e| Error::engine(format!("turbopuffer delete namespace: {e}")))?;
        // A missing namespace (404) is fine — it was already gone.
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        resp.error_for_status()
            .map_err(|e| Error::engine(format!("turbopuffer delete namespace failed: {e}")))?;
        Ok(())
    }

    async fn query_raw(&self, namespace: &str, body: JsonValue) -> Result<JsonValue> {
        let url = format!("{}/v2/namespaces/{namespace}/query", self.inner.base_url);
        let resp = self
            .inner
            .http
            .post(&url)
            .bearer_auth(&self.inner.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::engine(format!("turbopuffer query: {e}")))?
            .error_for_status()
            .map_err(|e| Error::engine(format!("turbopuffer query failed: {e}")))?;
        resp.json()
            .await
            .map_err(|e| Error::engine(format!("turbopuffer query parse: {e}")))
    }
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// Distance metric applied across the namespace's vector column.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DistanceMetric {
    CosineDistance,
    EuclideanSquared,
}

impl DistanceMetric {
    fn as_str(self) -> &'static str {
        match self {
            DistanceMetric::CosineDistance => "cosine_distance",
            DistanceMetric::EuclideanSquared => "euclidean_squared",
        }
    }
}

/// Vector definition for a Turbopuffer namespace field.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorDef {
    pub schema: VectorSchema,
}

impl VectorDef {
    pub fn new(schema: VectorSchema) -> Result<Self> {
        vector_type_str(&schema)?;
        Ok(Self { schema })
    }

    pub fn f32(size: usize) -> Result<Self> {
        Self::new(VectorSchema::f32(size))
    }

    pub async fn from_provider(provider: &(impl VectorSchemaProvider + ?Sized)) -> Result<Self> {
        Self::new(provider.vector_schema().await?)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum VectorFields {
    Single(VectorDef),
    Named(BTreeMap<String, VectorDef>),
}

/// Schema for a Turbopuffer namespace.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceSchema {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    vectors: Option<VectorFields>,
    /// Kept for compatibility with the original single-vector Rust schema.
    pub vector_size: usize,
    pub distance: DistanceMetric,
}

impl NamespaceSchema {
    /// Create a single unnamed f32 vector schema under Turbopuffer's `vector`
    /// field. This is the common path and matches the older Rust API.
    pub fn new(vector_size: usize, distance: DistanceMetric) -> Self {
        Self {
            vectors: Some(VectorFields::Single(VectorDef {
                schema: VectorSchema::f32(vector_size),
            })),
            vector_size,
            distance,
        }
    }

    /// Create a single unnamed vector schema from an explicit vector schema.
    pub fn from_vector_schema(schema: VectorSchema, distance: DistanceMetric) -> Result<Self> {
        Ok(Self {
            vectors: Some(VectorFields::Single(VectorDef::new(schema)?)),
            vector_size: schema.size,
            distance,
        })
    }

    /// Create a single unnamed vector schema from a provider, such as an
    /// embedder.
    pub async fn from_vector_provider(
        provider: &(impl VectorSchemaProvider + ?Sized),
        distance: DistanceMetric,
    ) -> Result<Self> {
        let def = VectorDef::from_provider(provider).await?;
        Ok(Self {
            vector_size: def.schema.size,
            vectors: Some(VectorFields::Single(def)),
            distance,
        })
    }

    /// Create a named-vector schema.
    pub fn named<I, K>(vectors: I, distance: DistanceMetric) -> Result<Self>
    where
        I: IntoIterator<Item = (K, VectorDef)>,
        K: Into<String>,
    {
        let mut resolved = BTreeMap::new();
        for (name, def) in vectors {
            let name = name.into();
            validate_vector_field_name(&name)?;
            if resolved.insert(name.clone(), def).is_some() {
                return Err(Error::engine(format!(
                    "duplicate turbopuffer vector field name: {name:?}"
                )));
            }
        }
        if resolved.is_empty() {
            return Err(Error::engine(
                "named turbopuffer vector schema must declare at least one vector field",
            ));
        }
        let vector_size = resolved
            .values()
            .next()
            .map(|def| def.schema.size)
            .unwrap_or(0);
        Ok(Self {
            vectors: Some(VectorFields::Named(resolved)),
            vector_size,
            distance,
        })
    }

    /// The `schema` payload Turbopuffer's write API expects.
    fn write_schema(&self) -> Result<JsonValue> {
        let mut fields = Map::new();
        match self.vector_fields() {
            VectorFields::Single(def) => {
                fields.insert(
                    DEFAULT_VECTOR_FIELD.into(),
                    vector_schema_entry(&def.schema)?,
                );
            }
            VectorFields::Named(vectors) => {
                for (name, def) in vectors {
                    fields.insert(name, vector_schema_entry(&def.schema)?);
                }
            }
        }
        Ok(JsonValue::Object(fields))
    }

    fn vector_field_names(&self) -> Vec<String> {
        match self.vector_fields() {
            VectorFields::Single(_) => vec![DEFAULT_VECTOR_FIELD.to_string()],
            VectorFields::Named(vectors) => vectors.into_keys().collect(),
        }
    }

    fn vector_fields(&self) -> VectorFields {
        self.vectors.clone().unwrap_or_else(|| {
            VectorFields::Single(VectorDef {
                schema: VectorSchema::f32(self.vector_size),
            })
        })
    }
}

const DEFAULT_VECTOR_FIELD: &str = "vector";

fn validate_vector_field_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::engine(
            "turbopuffer vector field name must not be empty",
        ));
    }
    if name == "id" {
        return Err(Error::engine(
            "turbopuffer vector field name \"id\" is reserved",
        ));
    }
    Ok(())
}

fn vector_type_str(schema: &VectorSchema) -> Result<String> {
    if schema.size == 0 {
        return Err(Error::engine(
            "turbopuffer vector size must be greater than zero",
        ));
    }
    let suffix = match schema.element_type {
        VectorElementType::Float32 => "f32",
        VectorElementType::Float16 => "f16",
    };
    Ok(format!("[{}]{suffix}", schema.size))
}

fn vector_schema_entry(schema: &VectorSchema) -> Result<JsonValue> {
    Ok(json!({ "type": vector_type_str(schema)?, "ann": true }))
}

// ---------------------------------------------------------------------------
// Public target API
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
enum RowVector {
    Single(Vec<f32>),
    Named(BTreeMap<String, Vec<f32>>),
}

/// A row declared into a namespace: id + vector(s) + attribute fields.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Row {
    id: String,
    vector: RowVector,
    attributes: Map<String, JsonValue>,
}

impl Row {
    /// The wire shape Turbopuffer expects: `{id, vector, ...attributes}` or
    /// `{id, text_vector, image_vector, ...attributes}`.
    fn to_upsert(&self, schema: &NamespaceSchema) -> Result<JsonValue> {
        let mut obj = Map::new();
        obj.insert("id".into(), JsonValue::from(self.id.clone()));

        let vector_fields = schema.vector_fields();
        let reserved = match (&vector_fields, &self.vector) {
            (VectorFields::Single(_), RowVector::Single(vector)) => {
                obj.insert(DEFAULT_VECTOR_FIELD.into(), json!(vector));
                schema.vector_field_names()
            }
            (VectorFields::Single(_), RowVector::Named(_)) => {
                return Err(Error::engine(format!(
                    "turbopuffer row {:?}: schema declares a single unnamed vector but row uses named vectors",
                    self.id
                )));
            }
            (VectorFields::Named(vectors), RowVector::Named(row_vectors)) => {
                let missing: Vec<_> = vectors
                    .keys()
                    .filter(|name| !row_vectors.contains_key(*name))
                    .cloned()
                    .collect();
                if !missing.is_empty() {
                    return Err(Error::engine(format!(
                        "turbopuffer row {:?}: missing vector fields {:?}",
                        self.id, missing
                    )));
                }
                for (name, vector) in row_vectors {
                    if !vectors.contains_key(name) {
                        return Err(Error::engine(format!(
                            "turbopuffer row {:?}: unexpected vector field {:?}",
                            self.id, name
                        )));
                    }
                    obj.insert(name.clone(), json!(vector));
                }
                schema.vector_field_names()
            }
            (VectorFields::Named(vectors), RowVector::Single(_)) => {
                let names: Vec<_> = vectors.keys().cloned().collect();
                return Err(Error::engine(format!(
                    "turbopuffer row {:?}: schema declares named vectors {:?} but row uses a single vector",
                    self.id, names
                )));
            }
        };

        if self.attributes.contains_key("id") {
            return Err(Error::engine(format!(
                "turbopuffer row {:?}: attribute name \"id\" is reserved",
                self.id
            )));
        }
        for (k, v) in &self.attributes {
            if reserved.iter().any(|field| field == k) {
                return Err(Error::engine(format!(
                    "turbopuffer row {:?}: attribute name {:?} is reserved",
                    self.id, k
                )));
            }
            obj.insert(k.clone(), v.clone());
        }
        Ok(JsonValue::Object(obj))
    }
}

/// A declarative Turbopuffer namespace target. See the [module docs](self).
#[derive(Clone)]
pub struct NamespaceTarget {
    namespace: Arc<str>,
    schema: NamespaceSchema,
    rows: TargetStateProvider<Row>,
}

/// Mount a declarative Turbopuffer namespace target. Declared rows are upserted;
/// orphaned rows are deleted; changing the vector schema clears the namespace.
pub async fn mount_namespace_target(
    ctx: &Ctx,
    conn: &TurbopufferConnection,
    namespace: impl Into<String>,
    schema: NamespaceSchema,
) -> Result<NamespaceTarget> {
    mount_namespace_target_with_options(
        ctx,
        conn,
        namespace,
        schema,
        ManagedTargetOptions::default(),
    )
    .await
}

pub async fn mount_namespace_target_with_options(
    ctx: &Ctx,
    conn: &TurbopufferConnection,
    namespace: impl Into<String>,
    schema: NamespaceSchema,
    options: ManagedTargetOptions,
) -> Result<NamespaceTarget> {
    let ts = namespace_target_with_options(ctx, conn, namespace, schema, options)?;
    let name = ts.value().namespace.clone();
    let schema = ts.value().schema.clone();
    let rows = mount_target::<NamespaceSpec, Row>(ctx, ts).await?;
    Ok(NamespaceTarget {
        namespace: Arc::from(name),
        schema,
        rows,
    })
}

/// Build a composable [`TargetState`] for a Turbopuffer namespace (the spec
/// constructor). Pass it to the generic
/// [`mount_target`](crate::target_state::mount_target) /
/// [`declare_target_state_with_child`](crate::target_state::declare_target_state_with_child),
/// or use [`declare_namespace_target`]/[`mount_namespace_target`].
pub fn namespace_target(
    ctx: &Ctx,
    conn: &TurbopufferConnection,
    namespace: impl Into<String>,
    schema: NamespaceSchema,
) -> Result<TargetState<NamespaceSpec>> {
    namespace_target_with_options(
        ctx,
        conn,
        namespace,
        schema,
        ManagedTargetOptions::default(),
    )
}

/// [`namespace_target`] with explicit [`ManagedTargetOptions`].
pub fn namespace_target_with_options(
    ctx: &Ctx,
    conn: &TurbopufferConnection,
    namespace: impl Into<String>,
    schema: NamespaceSchema,
    options: ManagedTargetOptions,
) -> Result<TargetState<NamespaceSpec>> {
    let namespace = namespace.into();
    validate_namespace(&namespace)?;
    let provider = register_root_target_states_provider(
        ctx,
        format!(
            "cocoindex/turbopuffer/namespace/{}/{}",
            conn.state_id(),
            namespace
        ),
        NamespaceHandler::new(conn.clone()),
    )?;
    Ok(provider.target_state(
        "default",
        NamespaceSpec {
            namespace,
            schema,
            managed_by: options.managed_by,
        },
    ))
}

/// Declare a Turbopuffer namespace target in the **current** component and
/// return a pending handle. The row child provider resolves when this component
/// commits; use [`mount_namespace_target`] when rows must be declared
/// immediately.
pub fn declare_namespace_target(
    ctx: &Ctx,
    conn: &TurbopufferConnection,
    namespace: impl Into<String>,
    schema: NamespaceSchema,
) -> Result<NamespaceTarget> {
    declare_namespace_target_with_options(
        ctx,
        conn,
        namespace,
        schema,
        ManagedTargetOptions::default(),
    )
}

/// [`declare_namespace_target`] with explicit [`ManagedTargetOptions`].
pub fn declare_namespace_target_with_options(
    ctx: &Ctx,
    conn: &TurbopufferConnection,
    namespace: impl Into<String>,
    schema: NamespaceSchema,
    options: ManagedTargetOptions,
) -> Result<NamespaceTarget> {
    let ts = namespace_target_with_options(ctx, conn, namespace, schema, options)?;
    let name = ts.value().namespace.clone();
    let schema = ts.value().schema.clone();
    let rows = declare_target_state_with_child::<NamespaceSpec, Row>(ctx, ts)?;
    Ok(NamespaceTarget {
        namespace: Arc::from(name),
        schema,
        rows,
    })
}

impl NamespaceTarget {
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Declare a row (id + vector + attributes) to upsert into the namespace.
    pub fn declare_row(
        &self,
        ctx: &Ctx,
        id: impl Into<String>,
        vector: Vec<f32>,
        attributes: Map<String, JsonValue>,
    ) -> Result<()> {
        let id = id.into();
        let row = Row {
            id: id.clone(),
            vector: RowVector::Single(vector),
            attributes,
        };
        row.to_upsert(&self.schema)?;
        declare_target_state(ctx, self.rows.target_state(id, row))
    }

    /// Declare a row for a named-vector namespace.
    pub fn declare_named_row<I, K>(
        &self,
        ctx: &Ctx,
        id: impl Into<String>,
        vectors: I,
        attributes: Map<String, JsonValue>,
    ) -> Result<()>
    where
        I: IntoIterator<Item = (K, Vec<f32>)>,
        K: Into<String>,
    {
        let id = id.into();
        let row = Row {
            id: id.clone(),
            vector: RowVector::Named(
                vectors
                    .into_iter()
                    .map(|(name, vector)| (name.into(), vector))
                    .collect(),
            ),
            attributes,
        };
        row.to_upsert(&self.schema)?;
        declare_target_state(ctx, self.rows.target_state(id, row))
    }
}

fn validate_namespace(name: &str) -> Result<()> {
    if !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        Ok(())
    } else {
        Err(Error::engine(format!(
            "invalid turbopuffer namespace name: {name:?}"
        )))
    }
}

// ---------------------------------------------------------------------------
// Namespace handler (container)
// ---------------------------------------------------------------------------

/// Spec for a Turbopuffer namespace (the declared container value). Public so
/// [`namespace_target`] can return a composable [`TargetState`]; fields are
/// private.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NamespaceSpec {
    namespace: String,
    schema: NamespaceSchema,
    managed_by: ManagedBy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NamespaceAction {
    namespace: String,
    schema: NamespaceSchema,
    clear: bool,
    managed_by: ManagedBy,
}

struct NamespaceHandler {
    sink: TargetActionSink<NamespaceAction>,
}

impl NamespaceHandler {
    fn new(conn: TurbopufferConnection) -> Self {
        Self {
            sink: namespace_sink(conn),
        }
    }
}

impl TargetHandler<NamespaceSpec> for NamespaceHandler {
    type TrackingRecord = MutualTrackingRecord<NamespaceSpec>;
    type Action = NamespaceAction;

    fn reconcile(
        &self,
        _key: StableKey,
        desired: Option<NamespaceSpec>,
        prev: Vec<MutualTrackingRecord<NamespaceSpec>>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<NamespaceAction, Self::TrackingRecord>>> {
        match desired {
            // Always emit when declared, so the sink fulfills the row child.
            Some(spec) => {
                let prev_is_empty = prev.is_empty();
                let tracking_record = MutualTrackingRecord::new(spec.clone(), spec.managed_by);
                let resolved = resolve_system_transition(
                    Some(tracking_record.clone()),
                    prev,
                    prev_may_be_missing,
                );
                let main_action = diff(resolved.as_ref());
                let changed = matches!(main_action, Some(DiffAction::Replace));
                let action = NamespaceAction {
                    namespace: spec.namespace.clone(),
                    schema: spec.schema.clone(),
                    clear: changed,
                    managed_by: spec.managed_by,
                };
                let target_action = if prev_is_empty {
                    TargetAction::Create(action)
                } else {
                    TargetAction::Update(action)
                };
                Ok(Some(TargetReconcileOutput {
                    action: target_action,
                    sink: self.sink.clone(),
                    tracking_record: Some(tracking_record),
                    child_invalidation: changed.then_some(TargetChildInvalidation::Destructive),
                }))
            }
            None => {
                let resolved = resolve_system_transition(None, prev.clone(), prev_may_be_missing);
                if resolved.is_none() {
                    return Ok(None);
                };
                let Some(prev_spec) = prev
                    .into_iter()
                    .find(|p| p.managed_by.is_system())
                    .map(|p| p.tracking_record)
                else {
                    return Ok(None);
                };
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Delete(NamespaceAction {
                        namespace: prev_spec.namespace,
                        schema: prev_spec.schema,
                        clear: true,
                        managed_by: ManagedBy::System,
                    }),
                    sink: self.sink.clone(),
                    tracking_record: None,
                    child_invalidation: Some(TargetChildInvalidation::Destructive),
                }))
            }
        }
    }
}

fn namespace_sink(conn: TurbopufferConnection) -> TargetActionSink<NamespaceAction> {
    TargetActionSink::from_async_fn_with_children(
        move |actions: Vec<TargetAction<NamespaceAction>>| {
            let conn = conn.clone();
            async move {
                let mut out: Vec<Option<ChildTargetDef>> = Vec::with_capacity(actions.len());
                for action in actions {
                    match action {
                        TargetAction::Create(a) | TargetAction::Update(a) => {
                            // Namespaces are created implicitly on write; clear on
                            // schema change.
                            if a.clear && a.managed_by.is_system() {
                                conn.delete_namespace(&a.namespace).await?;
                            }
                            out.push(Some(ChildTargetDef::new::<Row, _>(RowHandler::new(
                                conn.clone(),
                                a.namespace,
                                a.schema,
                            ))));
                        }
                        TargetAction::Delete(a) => {
                            if a.managed_by.is_system() {
                                conn.delete_namespace(&a.namespace).await?;
                            }
                            out.push(None);
                        }
                    }
                }
                Ok(out)
            }
        },
    )
}

// ---------------------------------------------------------------------------
// Row handler (child)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RowAction {
    id: String,
    row: Option<Row>,
}

struct RowHandler {
    sink: TargetActionSink<RowAction>,
}

impl RowHandler {
    fn new(conn: TurbopufferConnection, namespace: String, schema: NamespaceSchema) -> Self {
        Self {
            sink: row_sink(conn, namespace, schema),
        }
    }
}

impl TargetHandler<Row> for RowHandler {
    type TrackingRecord = String;
    type Action = RowAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<Row>,
        prev: Vec<String>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<RowAction, String>>> {
        let id = row_id(&key)?;
        match desired {
            Some(row) => {
                let fp = row_fingerprint(&row);
                let unchanged =
                    !prev_may_be_missing && !prev.is_empty() && prev.iter().all(|p| *p == fp);
                if unchanged {
                    return Ok(None);
                }
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Update(RowAction { id, row: Some(row) }),
                    sink: self.sink.clone(),
                    tracking_record: Some(fp),
                    child_invalidation: None,
                }))
            }
            None => {
                if prev.is_empty() && !prev_may_be_missing {
                    return Ok(None);
                }
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Delete(RowAction { id, row: None }),
                    sink: self.sink.clone(),
                    tracking_record: None,
                    child_invalidation: None,
                }))
            }
        }
    }
}

fn row_id(key: &StableKey) -> Result<String> {
    match key {
        StableKey::Str(s) | StableKey::Symbol(s) => Ok(s.to_string()),
        StableKey::Int(i) => Ok(i.to_string()),
        other => Err(Error::engine(format!(
            "unsupported turbopuffer row key: {other:?}"
        ))),
    }
}

fn row_fingerprint(row: &Row) -> String {
    let fp = cocoindex_utils::fingerprint::Fingerprint::from(&(&row.vector, &row.attributes))
        .expect("fingerprint row");
    format!("{fp:?}")
}

fn row_sink(
    conn: TurbopufferConnection,
    namespace: String,
    schema: NamespaceSchema,
) -> TargetActionSink<RowAction> {
    TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<RowAction>>| {
        let conn = conn.clone();
        let namespace = namespace.clone();
        let schema = schema.clone();
        async move {
            let mut upserts: Vec<JsonValue> = Vec::new();
            let mut deletes: Vec<JsonValue> = Vec::new();
            for action in actions {
                match action {
                    TargetAction::Create(a) | TargetAction::Update(a) => {
                        if let Some(row) = a.row {
                            upserts.push(row.to_upsert(&schema)?);
                        }
                    }
                    TargetAction::Delete(a) => deletes.push(JsonValue::from(a.id)),
                }
            }
            if upserts.is_empty() && deletes.is_empty() {
                return Ok(());
            }
            let mut body = Map::new();
            body.insert(
                "distance_metric".into(),
                JsonValue::from(schema.distance.as_str()),
            );
            body.insert("schema".into(), schema.write_schema()?);
            if !upserts.is_empty() {
                body.insert("upsert_rows".into(), JsonValue::Array(upserts));
            }
            if !deletes.is_empty() {
                body.insert("deletes".into(), JsonValue::Array(deletes));
            }
            conn.write(&namespace, JsonValue::Object(body)).await
        }
    })
}

// ---------------------------------------------------------------------------
// Query helper (convenience for examples)
// ---------------------------------------------------------------------------

/// One vector-search hit: its distance and attribute fields.
pub struct TurbopufferHit {
    pub distance: f64,
    pub attributes: Map<String, JsonValue>,
}

/// Run a vector similarity search and return the top-`k` hits (distance + attrs).
pub async fn vector_search(
    conn: &TurbopufferConnection,
    namespace: &str,
    query: Vec<f32>,
    top_k: usize,
) -> Result<Vec<TurbopufferHit>> {
    vector_search_by_field(conn, namespace, DEFAULT_VECTOR_FIELD, query, top_k).await
}

/// Run a vector similarity search against a named vector field.
pub async fn vector_search_by_field(
    conn: &TurbopufferConnection,
    namespace: &str,
    field: &str,
    query: Vec<f32>,
    top_k: usize,
) -> Result<Vec<TurbopufferHit>> {
    validate_vector_field_name(field)?;
    let body = json!({
        "rank_by": [field, "ANN", query],
        "top_k": top_k,
        "include_attributes": true,
    });
    let resp = conn.query_raw(namespace, body).await?;
    let mut hits = Vec::new();
    if let Some(rows) = resp.get("rows").and_then(|v| v.as_array()) {
        for row in rows {
            let Some(obj) = row.as_object() else { continue };
            let distance = obj.get("$dist").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let attributes: Map<String, JsonValue> = obj
                .iter()
                .filter(|(k, _)| k.as_str() != "$dist" && k.as_str() != "id" && k.as_str() != field)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            hits.push(TurbopufferHit {
                distance,
                attributes,
            });
        }
    }
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distance_metric_strings() {
        assert_eq!(DistanceMetric::CosineDistance.as_str(), "cosine_distance");
        assert_eq!(
            DistanceMetric::EuclideanSquared.as_str(),
            "euclidean_squared"
        );
    }

    #[test]
    fn write_schema_shape() {
        let s = NamespaceSchema::new(384, DistanceMetric::CosineDistance);
        assert_eq!(
            s.write_schema().unwrap(),
            json!({ "vector": { "type": "[384]f32", "ann": true } })
        );
    }

    #[test]
    fn namespace_schema_reads_legacy_single_vector_state() {
        let schema: NamespaceSchema = serde_json::from_value(json!({
            "vector_size": 384,
            "distance": "CosineDistance"
        }))
        .unwrap();
        assert_eq!(
            schema.write_schema().unwrap(),
            json!({ "vector": { "type": "[384]f32", "ann": true } })
        );
    }

    #[test]
    fn write_schema_named_and_f16() {
        let schema = NamespaceSchema::named(
            [
                (
                    "text",
                    VectorDef::new(VectorSchema {
                        element_type: VectorElementType::Float32,
                        size: 4,
                    })
                    .unwrap(),
                ),
                (
                    "image",
                    VectorDef::new(VectorSchema {
                        element_type: VectorElementType::Float16,
                        size: 16,
                    })
                    .unwrap(),
                ),
            ],
            DistanceMetric::EuclideanSquared,
        )
        .unwrap();
        assert_eq!(
            schema.write_schema().unwrap(),
            json!({
                "image": { "type": "[16]f16", "ann": true },
                "text": { "type": "[4]f32", "ann": true },
            })
        );
    }

    #[test]
    fn row_to_upsert_flattens_attributes() {
        let mut attrs = Map::new();
        attrs.insert("text".into(), JsonValue::from("hi"));
        let row = Row {
            id: "x".into(),
            vector: RowVector::Single(vec![0.1, 0.2]),
            attributes: attrs,
        };
        let schema = NamespaceSchema::new(2, DistanceMetric::CosineDistance);
        let up = row.to_upsert(&schema).unwrap();
        assert_eq!(up["id"], "x");
        assert_eq!(up["text"], "hi");
        assert_eq!(up["vector"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn row_to_upsert_rejects_reserved_single_attrs() {
        let schema = NamespaceSchema::new(3, DistanceMetric::CosineDistance);
        for reserved in ["id", "vector"] {
            let mut attrs = Map::new();
            attrs.insert(reserved.into(), JsonValue::from("shadow"));
            let row = Row {
                id: "x".into(),
                vector: RowVector::Single(vec![1.0, 2.0, 3.0]),
                attributes: attrs,
            };
            assert!(row.to_upsert(&schema).is_err());
        }
    }

    #[test]
    fn named_row_to_upsert() {
        let schema = NamespaceSchema::named(
            [
                ("text", VectorDef::f32(3).unwrap()),
                ("image", VectorDef::f32(2).unwrap()),
            ],
            DistanceMetric::CosineDistance,
        )
        .unwrap();
        let mut attrs = Map::new();
        attrs.insert("title".into(), JsonValue::from("T"));
        let row = Row {
            id: "a".into(),
            vector: RowVector::Named(BTreeMap::from([
                ("text".to_string(), vec![1.0, 2.0, 3.0]),
                ("image".to_string(), vec![0.5, 0.5]),
            ])),
            attributes: attrs,
        };
        assert_eq!(
            row.to_upsert(&schema).unwrap(),
            json!({
                "id": "a",
                "text": [1.0, 2.0, 3.0],
                "image": [0.5, 0.5],
                "title": "T",
            })
        );
    }

    #[test]
    fn named_row_rejects_missing_and_unexpected_fields() {
        let schema = NamespaceSchema::named(
            [
                ("text", VectorDef::f32(3).unwrap()),
                ("image", VectorDef::f32(2).unwrap()),
            ],
            DistanceMetric::CosineDistance,
        )
        .unwrap();
        let missing = Row {
            id: "a".into(),
            vector: RowVector::Named(BTreeMap::from([("text".to_string(), vec![1.0, 2.0, 3.0])])),
            attributes: Map::new(),
        };
        assert!(missing.to_upsert(&schema).is_err());

        let unexpected = Row {
            id: "a".into(),
            vector: RowVector::Named(BTreeMap::from([
                ("text".to_string(), vec![1.0, 2.0, 3.0]),
                ("image".to_string(), vec![0.5, 0.5]),
                ("extra".to_string(), vec![0.0]),
            ])),
            attributes: Map::new(),
        };
        assert!(unexpected.to_upsert(&schema).is_err());
    }

    #[test]
    fn named_row_rejects_non_dict_and_attribute_collision() {
        let schema = NamespaceSchema::named(
            [("text", VectorDef::f32(3).unwrap())],
            DistanceMetric::CosineDistance,
        )
        .unwrap();
        let single = Row {
            id: "a".into(),
            vector: RowVector::Single(vec![1.0, 2.0, 3.0]),
            attributes: Map::new(),
        };
        assert!(single.to_upsert(&schema).is_err());

        let mut attrs = Map::new();
        attrs.insert("text".into(), JsonValue::from("shadow"));
        let collision = Row {
            id: "a".into(),
            vector: RowVector::Named(BTreeMap::from([("text".to_string(), vec![1.0, 2.0, 3.0])])),
            attributes: attrs,
        };
        assert!(collision.to_upsert(&schema).is_err());
    }

    #[test]
    fn named_schema_rejects_reserved_and_empty_fields() {
        assert!(
            NamespaceSchema::named(
                [("id", VectorDef::f32(3).unwrap())],
                DistanceMetric::CosineDistance
            )
            .is_err()
        );
        let empty: Vec<(String, VectorDef)> = Vec::new();
        assert!(NamespaceSchema::named(empty, DistanceMetric::CosineDistance).is_err());
    }

    #[test]
    fn vector_def_rejects_zero_size() {
        assert!(VectorDef::f32(0).is_err());
        assert!(
            NamespaceSchema::from_vector_schema(
                VectorSchema {
                    element_type: VectorElementType::Float16,
                    size: 0,
                },
                DistanceMetric::CosineDistance,
            )
            .is_err()
        );
    }

    struct StaticVectorSchemaProvider(VectorSchema);

    #[async_trait::async_trait]
    impl VectorSchemaProvider for StaticVectorSchemaProvider {
        async fn vector_schema(&self) -> Result<VectorSchema> {
            Ok(self.0)
        }
    }

    #[tokio::test]
    async fn namespace_schema_from_provider() {
        let provider = StaticVectorSchemaProvider(VectorSchema {
            element_type: VectorElementType::Float16,
            size: 128,
        });
        let schema =
            NamespaceSchema::from_vector_provider(&provider, DistanceMetric::EuclideanSquared)
                .await
                .unwrap();
        assert_eq!(
            schema.write_schema().unwrap(),
            json!({ "vector": { "type": "[128]f16", "ann": true } })
        );
    }

    #[test]
    fn row_fingerprint_changes_with_content() {
        let row = Row {
            id: "x".into(),
            vector: RowVector::Single(vec![0.1, 0.2]),
            attributes: Map::new(),
        };
        let mut row2 = row.clone();
        row2.vector = RowVector::Single(vec![0.1, 0.3]);
        assert_eq!(row_fingerprint(&row), row_fingerprint(&row.clone()));
        assert_ne!(row_fingerprint(&row), row_fingerprint(&row2));
    }

    #[test]
    fn namespace_validation() {
        assert!(validate_namespace("TextEmbedding").is_ok());
        assert!(validate_namespace("ns-1_test.v2").is_ok());
        assert!(validate_namespace("").is_err());
        assert!(validate_namespace("bad name").is_err());
        assert!(validate_namespace("../escape").is_err());
    }
}
