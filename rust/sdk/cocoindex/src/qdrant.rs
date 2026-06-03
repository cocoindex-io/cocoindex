//! Qdrant vector-store target connector — the Rust analogue of Python's
//! `cocoindex.connectors.qdrant` target.
//!
//! A declarative, two-level managed target built **on the public target-state
//! facade** ([`crate::target_state`]): a *collection* (created/dropped to match
//! the declared vector schema) containing *points* you
//! [`declare_point`](CollectionTarget::declare_point). Reconciliation upserts
//! changed points, skips unchanged ones (fingerprint tracking), deletes orphaned
//! points, and recreates the collection (invalidating its points) when the
//! vector schema changes.
//!
//! Uses the native Rust `qdrant-client` (gRPC).

use std::sync::Arc;

use qdrant_client::Payload;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{
    CreateCollectionBuilder, DeletePointsBuilder, Distance as QdrantDistance, PointStruct,
    PointsIdsList, QueryPointsBuilder, UpsertPointsBuilder, VectorParamsBuilder, value::Kind,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

use crate::ctx::Ctx;
use crate::error::{Error, Result};
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

/// A Qdrant connection. Clone-cheap (the underlying client is shared).
#[derive(Clone)]
pub struct QdrantConnection {
    client: Arc<Qdrant>,
    state_id: Arc<str>,
}

impl QdrantConnection {
    /// Connect to a Qdrant server (gRPC URL, e.g. `http://localhost:6334`).
    pub async fn connect(url: &str) -> Result<Self> {
        let client = Qdrant::from_url(url)
            .build()
            .map_err(|e| Error::engine(format!("qdrant connect {url:?}: {e}")))?;
        Ok(Self {
            client: Arc::new(client),
            state_id: Arc::from(url),
        })
    }

    /// Stable identity (the URL) for use as a `ContextKey` state id / memo dep.
    pub fn state_id(&self) -> &str {
        &self.state_id
    }

    /// The underlying `qdrant_client::Qdrant` (e.g. for queries).
    pub fn client(&self) -> &Qdrant {
        &self.client
    }
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// Vector distance metric.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Distance {
    Cosine,
    Dot,
    Euclid,
}

impl Distance {
    fn to_qdrant(self) -> QdrantDistance {
        match self {
            Distance::Cosine => QdrantDistance::Cosine,
            Distance::Dot => QdrantDistance::Dot,
            Distance::Euclid => QdrantDistance::Euclid,
        }
    }
}

/// Schema for a Qdrant collection: a single (unnamed) vector field.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CollectionSchema {
    pub vector_size: u64,
    pub distance: Distance,
}

impl CollectionSchema {
    pub fn new(vector_size: u64, distance: Distance) -> Self {
        Self {
            vector_size,
            distance,
        }
    }
}

// ---------------------------------------------------------------------------
// Public target API
// ---------------------------------------------------------------------------

/// A point declared into a collection: an id, its vector, and a JSON payload.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Point {
    id: u64,
    vector: Vec<f32>,
    payload: Map<String, JsonValue>,
}

/// A declarative Qdrant collection target. See the [module docs](self).
#[derive(Clone)]
pub struct CollectionTarget {
    collection_name: Arc<str>,
    points: TargetStateProvider<Point>,
}

/// Mount a declarative Qdrant collection target. The collection is created to
/// match `schema`; declared points are upserted; orphaned points are deleted;
/// changing the vector schema recreates the collection.
pub async fn mount_collection_target(
    ctx: &Ctx,
    conn: &QdrantConnection,
    collection_name: impl Into<String>,
    schema: CollectionSchema,
) -> Result<CollectionTarget> {
    mount_collection_target_with_options(
        ctx,
        conn,
        collection_name,
        schema,
        ManagedTargetOptions::default(),
    )
    .await
}

pub async fn mount_collection_target_with_options(
    ctx: &Ctx,
    conn: &QdrantConnection,
    collection_name: impl Into<String>,
    schema: CollectionSchema,
    options: ManagedTargetOptions,
) -> Result<CollectionTarget> {
    let ts = collection_target_with_options(ctx, conn, collection_name, schema, options)?;
    let name = ts.value().collection_name.clone();
    let points = mount_target::<CollectionSpec, Point>(ctx, ts).await?;
    Ok(CollectionTarget {
        collection_name: Arc::from(name),
        points,
    })
}

/// Build a composable [`TargetState`] for a Qdrant collection (the spec
/// constructor). Pass it to the generic
/// [`mount_target`](crate::target_state::mount_target) /
/// [`declare_target_state_with_child`](crate::target_state::declare_target_state_with_child),
/// or use [`declare_collection_target`]/[`mount_collection_target`].
pub fn collection_target(
    ctx: &Ctx,
    conn: &QdrantConnection,
    collection_name: impl Into<String>,
    schema: CollectionSchema,
) -> Result<TargetState<CollectionSpec>> {
    collection_target_with_options(
        ctx,
        conn,
        collection_name,
        schema,
        ManagedTargetOptions::default(),
    )
}

/// [`collection_target`] with explicit [`ManagedTargetOptions`].
pub fn collection_target_with_options(
    ctx: &Ctx,
    conn: &QdrantConnection,
    collection_name: impl Into<String>,
    schema: CollectionSchema,
    options: ManagedTargetOptions,
) -> Result<TargetState<CollectionSpec>> {
    let collection_name = collection_name.into();
    let provider = register_root_target_states_provider(
        ctx,
        format!(
            "cocoindex/qdrant/collection/{}/{}",
            conn.state_id(),
            collection_name
        ),
        CollectionHandler::new(conn.client.clone()),
    )?;
    Ok(provider.target_state(
        "default",
        CollectionSpec {
            collection_name,
            schema,
            managed_by: options.managed_by,
        },
    ))
}

/// Declare a Qdrant collection target in the **current** component and return a
/// pending handle. The point child provider resolves when this component
/// commits; use [`mount_collection_target`] when points must be declared
/// immediately.
pub fn declare_collection_target(
    ctx: &Ctx,
    conn: &QdrantConnection,
    collection_name: impl Into<String>,
    schema: CollectionSchema,
) -> Result<CollectionTarget> {
    declare_collection_target_with_options(
        ctx,
        conn,
        collection_name,
        schema,
        ManagedTargetOptions::default(),
    )
}

/// [`declare_collection_target`] with explicit [`ManagedTargetOptions`].
pub fn declare_collection_target_with_options(
    ctx: &Ctx,
    conn: &QdrantConnection,
    collection_name: impl Into<String>,
    schema: CollectionSchema,
    options: ManagedTargetOptions,
) -> Result<CollectionTarget> {
    let ts = collection_target_with_options(ctx, conn, collection_name, schema, options)?;
    let name = ts.value().collection_name.clone();
    let points = declare_target_state_with_child::<CollectionSpec, Point>(ctx, ts)?;
    Ok(CollectionTarget {
        collection_name: Arc::from(name),
        points,
    })
}

impl CollectionTarget {
    pub fn collection_name(&self) -> &str {
        &self.collection_name
    }

    /// Declare a point (id + vector + JSON payload) to upsert into the collection.
    pub fn declare_point(
        &self,
        ctx: &Ctx,
        id: u64,
        vector: Vec<f32>,
        payload: Map<String, JsonValue>,
    ) -> Result<()> {
        let point = Point {
            id,
            vector,
            payload,
        };
        declare_target_state(ctx, self.points.target_state(id.to_string(), point))
    }
}

// ---------------------------------------------------------------------------
// Collection handler (container)
// ---------------------------------------------------------------------------

/// Spec for a Qdrant collection (the declared container value). Public so
/// [`collection_target`] can return a composable [`TargetState`]; fields are
/// private.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CollectionSpec {
    collection_name: String,
    schema: CollectionSchema,
    managed_by: ManagedBy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CollectionAction {
    name: String,
    schema: CollectionSchema,
    recreate: bool,
    drop: bool,
    managed_by: ManagedBy,
}

struct CollectionHandler {
    sink: TargetActionSink<CollectionAction>,
}

impl CollectionHandler {
    fn new(client: Arc<Qdrant>) -> Self {
        Self {
            sink: collection_sink(client),
        }
    }
}

impl TargetHandler<CollectionSpec> for CollectionHandler {
    type TrackingRecord = MutualTrackingRecord<CollectionSpec>;
    type Action = CollectionAction;

    fn reconcile(
        &self,
        _key: StableKey,
        desired: Option<CollectionSpec>,
        prev: Vec<MutualTrackingRecord<CollectionSpec>>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<CollectionAction, Self::TrackingRecord>>> {
        match desired {
            // Always emit when the collection is declared, so the sink runs and
            // fulfills the point child provider.
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
                let action = CollectionAction {
                    name: spec.collection_name.clone(),
                    schema: spec.schema.clone(),
                    recreate: changed,
                    drop: false,
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
                    action: TargetAction::Delete(CollectionAction {
                        name: prev_spec.collection_name,
                        schema: prev_spec.schema,
                        recreate: false,
                        drop: true,
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

fn collection_sink(client: Arc<Qdrant>) -> TargetActionSink<CollectionAction> {
    TargetActionSink::from_async_fn_with_children(
        move |actions: Vec<TargetAction<CollectionAction>>| {
            let client = client.clone();
            async move {
                let mut out: Vec<Option<ChildTargetDef>> = Vec::with_capacity(actions.len());
                for action in actions {
                    match action {
                        TargetAction::Create(a) | TargetAction::Update(a) => {
                            ensure_collection(&client, &a).await?;
                            out.push(Some(ChildTargetDef::new::<Point, _>(PointHandler::new(
                                client.clone(),
                                a.name,
                            ))));
                        }
                        TargetAction::Delete(a) => {
                            drop_collection(&client, &a.name).await?;
                            out.push(None);
                        }
                    }
                }
                Ok(out)
            }
        },
    )
}

async fn ensure_collection(client: &Qdrant, action: &CollectionAction) -> Result<()> {
    if action.managed_by.is_user() {
        return Ok(());
    }
    let exists = client
        .collection_exists(&action.name)
        .await
        .map_err(|e| Error::engine(format!("qdrant collection_exists: {e}")))?;
    if exists && action.recreate {
        drop_collection(client, &action.name).await?;
    } else if exists {
        return Ok(());
    }
    client
        .create_collection(
            CreateCollectionBuilder::new(action.name.clone()).vectors_config(
                VectorParamsBuilder::new(
                    action.schema.vector_size,
                    action.schema.distance.to_qdrant(),
                ),
            ),
        )
        .await
        .map_err(|e| Error::engine(format!("qdrant create_collection {:?}: {e}", action.name)))?;
    Ok(())
}

async fn drop_collection(client: &Qdrant, name: &str) -> Result<()> {
    let exists = client
        .collection_exists(name)
        .await
        .map_err(|e| Error::engine(format!("qdrant collection_exists: {e}")))?;
    if !exists {
        return Ok(());
    }
    client
        .delete_collection(name)
        .await
        .map_err(|e| Error::engine(format!("qdrant delete_collection {name:?}: {e}")))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Point handler (child)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PointAction {
    id: u64,
    point: Option<Point>,
}

struct PointHandler {
    sink: TargetActionSink<PointAction>,
}

impl PointHandler {
    fn new(client: Arc<Qdrant>, collection_name: String) -> Self {
        Self {
            sink: point_sink(client, collection_name),
        }
    }
}

impl TargetHandler<Point> for PointHandler {
    type TrackingRecord = String;
    type Action = PointAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<Point>,
        prev: Vec<String>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<PointAction, String>>> {
        let id = point_id(&key)?;
        match desired {
            Some(point) => {
                let fp = point_fingerprint(&point);
                let unchanged =
                    !prev_may_be_missing && !prev.is_empty() && prev.iter().all(|p| *p == fp);
                if unchanged {
                    return Ok(None);
                }
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Update(PointAction {
                        id,
                        point: Some(point),
                    }),
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
                    action: TargetAction::Delete(PointAction { id, point: None }),
                    sink: self.sink.clone(),
                    tracking_record: None,
                    child_invalidation: None,
                }))
            }
        }
    }
}

fn point_id(key: &StableKey) -> Result<u64> {
    match key {
        StableKey::Str(s) => s
            .parse::<u64>()
            .map_err(|_| Error::engine(format!("invalid qdrant point id: {s:?}"))),
        StableKey::Int(i) => {
            u64::try_from(*i).map_err(|_| Error::engine("negative qdrant point id"))
        }
        other => Err(Error::engine(format!(
            "unsupported qdrant point key: {other:?}"
        ))),
    }
}

/// Content fingerprint of a point (vector + payload), as a hex string tracking
/// record so unchanged points are skipped.
fn point_fingerprint(point: &Point) -> String {
    let fp = cocoindex_utils::fingerprint::Fingerprint::from(&(&point.vector, &point.payload))
        .expect("fingerprint point");
    format!("{fp:?}")
}

fn point_sink(client: Arc<Qdrant>, collection_name: String) -> TargetActionSink<PointAction> {
    TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<PointAction>>| {
        let client = client.clone();
        let collection_name = collection_name.clone();
        async move {
            let mut upserts: Vec<PointStruct> = Vec::new();
            let mut deletes: Vec<qdrant_client::qdrant::PointId> = Vec::new();
            for action in actions {
                match action {
                    TargetAction::Create(a) | TargetAction::Update(a) => {
                        if let Some(point) = a.point {
                            let payload: Payload = point.payload.into();
                            upserts.push(PointStruct::new(point.id, point.vector, payload));
                        }
                    }
                    TargetAction::Delete(a) => deletes.push(a.id.into()),
                }
            }
            if !upserts.is_empty() {
                client
                    .upsert_points(UpsertPointsBuilder::new(collection_name.clone(), upserts))
                    .await
                    .map_err(|e| Error::engine(format!("qdrant upsert_points: {e}")))?;
            }
            if !deletes.is_empty() {
                client
                    .delete_points(
                        DeletePointsBuilder::new(collection_name.clone())
                            .points(PointsIdsList { ids: deletes }),
                    )
                    .await
                    .map_err(|e| Error::engine(format!("qdrant delete_points: {e}")))?;
            }
            Ok(())
        }
    })
}

// ---------------------------------------------------------------------------
// Query helper (convenience for examples)
// ---------------------------------------------------------------------------

/// One vector-search hit: its similarity score and JSON payload.
pub struct QdrantHit {
    pub score: f32,
    pub payload: Map<String, JsonValue>,
}

/// Run a vector similarity search and return the top-`k` hits (score + payload).
pub async fn vector_search(
    conn: &QdrantConnection,
    collection_name: &str,
    query: Vec<f32>,
    top_k: u64,
) -> Result<Vec<QdrantHit>> {
    let response = conn
        .client
        .query(
            QueryPointsBuilder::new(collection_name)
                .query(query)
                .limit(top_k)
                .with_payload(true),
        )
        .await
        .map_err(|e| Error::engine(format!("qdrant query: {e}")))?;
    Ok(response
        .result
        .into_iter()
        .map(|p| QdrantHit {
            score: p.score,
            payload: p
                .payload
                .into_iter()
                .map(|(k, v)| (k, qdrant_value_to_json(v)))
                .collect(),
        })
        .collect())
}

fn qdrant_value_to_json(value: qdrant_client::qdrant::Value) -> JsonValue {
    match value.kind {
        Some(Kind::StringValue(s)) => JsonValue::from(s),
        Some(Kind::IntegerValue(i)) => JsonValue::from(i),
        Some(Kind::DoubleValue(d)) => JsonValue::from(d),
        Some(Kind::BoolValue(b)) => JsonValue::from(b),
        _ => JsonValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_id_parses_string_and_int_keys() {
        assert_eq!(point_id(&StableKey::Str(Arc::from("42"))).unwrap(), 42);
        assert_eq!(point_id(&StableKey::Int(7)).unwrap(), 7);
        assert!(point_id(&StableKey::Str(Arc::from("x"))).is_err());
        assert!(point_id(&StableKey::Int(-1)).is_err());
    }

    #[test]
    fn point_fingerprint_changes_with_content() {
        let mut payload = Map::new();
        payload.insert("text".into(), JsonValue::from("hello"));
        let p1 = Point {
            id: 1,
            vector: vec![0.1, 0.2],
            payload: payload.clone(),
        };
        let mut p2 = p1.clone();
        p2.vector = vec![0.1, 0.3];
        let mut p3 = p1.clone();
        p3.payload.insert("text".into(), JsonValue::from("world"));
        assert_eq!(point_fingerprint(&p1), point_fingerprint(&p1.clone()));
        assert_ne!(point_fingerprint(&p1), point_fingerprint(&p2));
        assert_ne!(point_fingerprint(&p1), point_fingerprint(&p3));
    }

    #[test]
    fn distance_maps_to_qdrant() {
        assert_eq!(Distance::Cosine.to_qdrant(), QdrantDistance::Cosine);
        assert_eq!(Distance::Dot.to_qdrant(), QdrantDistance::Dot);
        assert_eq!(Distance::Euclid.to_qdrant(), QdrantDistance::Euclid);
    }
}
