//! Turbopuffer vector-store target connector — the Rust analogue of Python's
//! `cocoindex.connectors.turbopuffer` target.
//!
//! A declarative, two-level managed target built **on the public target-state
//! facade** ([`crate::target_state`]): a *namespace* (cleared/rebuilt to match
//! the declared vector schema) containing *rows* you
//! [`declare_row`](NamespaceTarget::declare_row). Reconciliation upserts changed
//! rows, skips unchanged ones (fingerprint tracking), deletes orphaned rows, and
//! clears the namespace when the vector schema changes.
//!
//! Turbopuffer is a hosted service; this talks to its v2 HTTP API via `reqwest`
//! (no native crate). Namespaces are created implicitly on first write.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue, json};

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::statediff::{
    DiffAction, ManagedBy, ManagedTargetOptions, MutualTrackingRecord, diff,
    resolve_system_transition,
};
use crate::target_state::{
    ChildTargetDef, StableKey, TargetAction, TargetActionSink, TargetChildInvalidation,
    TargetHandler, TargetReconcileOutput, TargetStateProvider, mount_target,
    register_root_target_states_provider,
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

/// Schema for a Turbopuffer namespace: a single (unnamed) `vector` field.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceSchema {
    pub vector_size: usize,
    pub distance: DistanceMetric,
}

impl NamespaceSchema {
    pub fn new(vector_size: usize, distance: DistanceMetric) -> Self {
        Self {
            vector_size,
            distance,
        }
    }

    /// The `schema` payload turbopuffer's write API expects (`[N]f32` ann field).
    fn write_schema(&self) -> JsonValue {
        json!({ "vector": { "type": format!("[{}]f32", self.vector_size), "ann": true } })
    }
}

// ---------------------------------------------------------------------------
// Public target API
// ---------------------------------------------------------------------------

/// A row declared into a namespace: id + vector + attribute fields.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Row {
    id: String,
    vector: Vec<f32>,
    attributes: Map<String, JsonValue>,
}

impl Row {
    /// The wire shape turbopuffer expects: `{id, vector, ...attributes}`.
    fn to_upsert(&self) -> JsonValue {
        let mut obj = Map::new();
        obj.insert("id".into(), JsonValue::from(self.id.clone()));
        obj.insert("vector".into(), json!(self.vector));
        for (k, v) in &self.attributes {
            obj.insert(k.clone(), v.clone());
        }
        JsonValue::Object(obj)
    }
}

/// A declarative Turbopuffer namespace target. See the [module docs](self).
#[derive(Clone)]
pub struct NamespaceTarget {
    namespace: Arc<str>,
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
    let spec = NamespaceSpec {
        namespace: namespace.clone(),
        schema,
        managed_by: options.managed_by,
    };
    let rows: TargetStateProvider<Row> =
        mount_target(ctx, provider.target_state("default", spec)).await?;
    Ok(NamespaceTarget {
        namespace: Arc::from(namespace),
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
            vector,
            attributes,
        };
        crate::target_state::declare_target_state(ctx, self.rows.target_state(id, row))
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct NamespaceSpec {
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
                            upserts.push(row.to_upsert());
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
            body.insert("schema".into(), schema.write_schema());
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
    let body = json!({
        "rank_by": ["vector", "ANN", query],
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
                .filter(|(k, _)| {
                    k.as_str() != "$dist" && k.as_str() != "id" && k.as_str() != "vector"
                })
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
            s.write_schema(),
            json!({ "vector": { "type": "[384]f32", "ann": true } })
        );
    }

    #[test]
    fn row_to_upsert_flattens_attributes() {
        let mut attrs = Map::new();
        attrs.insert("text".into(), JsonValue::from("hi"));
        let row = Row {
            id: "x".into(),
            vector: vec![0.1, 0.2],
            attributes: attrs,
        };
        let up = row.to_upsert();
        assert_eq!(up["id"], "x");
        assert_eq!(up["text"], "hi");
        assert_eq!(up["vector"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn row_fingerprint_changes_with_content() {
        let row = Row {
            id: "x".into(),
            vector: vec![0.1, 0.2],
            attributes: Map::new(),
        };
        let mut row2 = row.clone();
        row2.vector = vec![0.1, 0.3];
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
