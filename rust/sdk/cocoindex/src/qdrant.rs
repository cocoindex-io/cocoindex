//! Qdrant vector-store target connector.
//!
//! Collection targets reconcile declared points against the previous run:
//! changed points are upserted, unchanged points are skipped, and orphaned
//! points are deleted. System-managed collections are recreated when the vector
//! schema changes.
//!
//! Uses the native Rust `qdrant-client` (gRPC).

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use qdrant_client::Payload;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{
    CreateCollectionBuilder, Datatype as QdrantDatatype, DeletePointsBuilder,
    Distance as QdrantDistance, MultiVectorComparator, MultiVectorConfigBuilder, NamedVectors,
    PointStruct, PointsIdsList, QueryPointsBuilder, UpsertPointsBuilder, Vector, VectorInput,
    VectorParams, VectorParamsBuilder, Vectors, VectorsConfigBuilder, value::Kind,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::resources::schema::{
    MultiVectorSchema, MultiVectorSchemaProvider, VectorElementType, VectorSchema,
    VectorSchemaProvider,
};
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

/// Multivector comparator.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum MultivectorComparator {
    MaxSim,
}

impl MultivectorComparator {
    fn to_qdrant(self) -> MultiVectorComparator {
        match self {
            MultivectorComparator::MaxSim => MultiVectorComparator::MaxSim,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum QdrantVectorSchema {
    Dense(VectorSchema),
    Multi(MultiVectorSchema),
}

/// Vector definition for one Qdrant vector field.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct QdrantVectorDef {
    schema: QdrantVectorSchema,
    pub distance: Distance,
    pub multivector_comparator: MultivectorComparator,
}

impl QdrantVectorDef {
    pub fn new(schema: VectorSchema, distance: Distance) -> Result<Self> {
        validate_vector_size(schema.size)?;
        Ok(Self {
            schema: QdrantVectorSchema::Dense(schema),
            distance,
            multivector_comparator: MultivectorComparator::MaxSim,
        })
    }

    pub fn f32(size: u64, distance: Distance) -> Result<Self> {
        let size = usize::try_from(size)
            .map_err(|_| Error::engine("qdrant vector size does not fit usize"))?;
        Self::new(VectorSchema::f32(size), distance)
    }

    pub async fn from_vector_provider(
        provider: &(impl VectorSchemaProvider + ?Sized),
        distance: Distance,
    ) -> Result<Self> {
        Self::new(provider.vector_schema().await?, distance)
    }

    pub fn multivector(schema: MultiVectorSchema, distance: Distance) -> Result<Self> {
        validate_vector_size(schema.vector_schema.size)?;
        Ok(Self {
            schema: QdrantVectorSchema::Multi(schema),
            distance,
            multivector_comparator: MultivectorComparator::MaxSim,
        })
    }

    pub async fn from_multivector_provider(
        provider: &(impl MultiVectorSchemaProvider + ?Sized),
        distance: Distance,
    ) -> Result<Self> {
        Self::multivector(provider.multi_vector_schema().await?, distance)
    }

    fn is_multivector(&self) -> bool {
        matches!(self.schema, QdrantVectorSchema::Multi(_))
    }

    fn vector_size(&self) -> u64 {
        match &self.schema {
            QdrantVectorSchema::Dense(schema) => schema.size as u64,
            QdrantVectorSchema::Multi(schema) => schema.vector_schema.size as u64,
        }
    }

    fn element_type(&self) -> VectorElementType {
        match &self.schema {
            QdrantVectorSchema::Dense(schema) => schema.element_type,
            QdrantVectorSchema::Multi(schema) => schema.vector_schema.element_type,
        }
    }

    fn to_params(&self) -> VectorParams {
        let mut params = VectorParamsBuilder::new(self.vector_size(), self.distance.to_qdrant());
        // Map the SDK element type to Qdrant's stored datatype. Float32 is
        // Qdrant's default, so only f16 needs an explicit datatype. NOTE: the
        // Python connector currently does *not* forward the schema dtype to
        // `VectorParams(datatype=...)` — a latent Python bug that silently stores
        // an f16 schema as f32. Rust honors the requested f16; Python should be
        // fixed to match (tracked in the parity review).
        if self.element_type() == VectorElementType::Float16 {
            params = params.datatype(QdrantDatatype::Float16);
        }
        if self.is_multivector() {
            params = params.multivector_config(MultiVectorConfigBuilder::new(
                self.multivector_comparator.to_qdrant(),
            ));
        }
        params.into()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum VectorFields {
    Single(QdrantVectorDef),
    Named(BTreeMap<String, QdrantVectorDef>),
}

/// Schema for a Qdrant collection.
///
/// `new` and `multivector` keep the simple single-vector API. Use
/// [`CollectionSchema::named`] for Python-style named vectors.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CollectionSchema {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    vectors: Option<VectorFields>,
    /// Kept for compatibility with the simple single-vector constructors.
    pub vector_size: u64,
    pub distance: Distance,
    /// Kept for compatibility with the simple single-vector constructors.
    #[serde(default)]
    pub multivector: bool,
}

impl CollectionSchema {
    /// A single dense vector per point.
    pub fn new(vector_size: u64, distance: Distance) -> Self {
        Self {
            vectors: Some(VectorFields::Single(QdrantVectorDef {
                schema: QdrantVectorSchema::Dense(VectorSchema::f32(vector_size as usize)),
                distance,
                multivector_comparator: MultivectorComparator::MaxSim,
            })),
            vector_size,
            distance,
            multivector: false,
        }
    }

    /// A multi-vector (late-interaction) collection: each point holds a list of
    /// `vector_size`-dimensional vectors, scored with MAX_SIM.
    pub fn multivector(vector_size: u64, distance: Distance) -> Self {
        Self {
            vectors: Some(VectorFields::Single(QdrantVectorDef {
                schema: QdrantVectorSchema::Multi(MultiVectorSchema {
                    vector_schema: VectorSchema::f32(vector_size as usize),
                }),
                distance,
                multivector_comparator: MultivectorComparator::MaxSim,
            })),
            vector_size,
            distance,
            multivector: true,
        }
    }

    pub fn from_vector_schema(schema: VectorSchema, distance: Distance) -> Result<Self> {
        let def = QdrantVectorDef::new(schema, distance)?;
        Ok(Self {
            vector_size: def.vector_size(),
            distance,
            multivector: false,
            vectors: Some(VectorFields::Single(def)),
        })
    }

    pub async fn from_vector_provider(
        provider: &(impl VectorSchemaProvider + ?Sized),
        distance: Distance,
    ) -> Result<Self> {
        Self::from_vector_schema(provider.vector_schema().await?, distance)
    }

    pub fn from_multivector_schema(schema: MultiVectorSchema, distance: Distance) -> Result<Self> {
        let def = QdrantVectorDef::multivector(schema, distance)?;
        Ok(Self {
            vector_size: def.vector_size(),
            distance,
            multivector: true,
            vectors: Some(VectorFields::Single(def)),
        })
    }

    pub async fn from_multivector_provider(
        provider: &(impl MultiVectorSchemaProvider + ?Sized),
        distance: Distance,
    ) -> Result<Self> {
        Self::from_multivector_schema(provider.multi_vector_schema().await?, distance)
    }

    pub fn named<I, K>(vectors: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, QdrantVectorDef)>,
        K: Into<String>,
    {
        let mut resolved = BTreeMap::new();
        for (name, def) in vectors {
            let name = name.into();
            validate_vector_name(&name)?;
            if resolved.insert(name.clone(), def).is_some() {
                return Err(Error::engine(format!(
                    "duplicate qdrant vector name: {name:?}"
                )));
            }
        }
        if resolved.is_empty() {
            return Err(Error::engine(
                "named qdrant schema must declare at least one vector field",
            ));
        }
        let first = resolved.values().next().expect("non-empty").clone();
        Ok(Self {
            vector_size: first.vector_size(),
            distance: first.distance,
            multivector: first.is_multivector(),
            vectors: Some(VectorFields::Named(resolved)),
        })
    }

    fn vectors_config(&self) -> VectorsConfigBuilder {
        let mut builder = VectorsConfigBuilder::default();
        match self.vector_fields() {
            VectorFields::Single(def) => {
                builder.add_vector_params(def.to_params());
            }
            VectorFields::Named(vectors) => {
                for (name, def) in vectors {
                    builder.add_named_vector_params(name, def.to_params());
                }
            }
        }
        builder
    }

    fn vector_fields(&self) -> VectorFields {
        self.vectors.clone().unwrap_or_else(|| {
            let schema = if self.multivector {
                QdrantVectorSchema::Multi(MultiVectorSchema {
                    vector_schema: VectorSchema::f32(self.vector_size as usize),
                })
            } else {
                QdrantVectorSchema::Dense(VectorSchema::f32(self.vector_size as usize))
            };
            VectorFields::Single(QdrantVectorDef {
                schema,
                distance: self.distance,
                multivector_comparator: MultivectorComparator::MaxSim,
            })
        })
    }
}

fn validate_vector_size(size: usize) -> Result<()> {
    if size == 0 {
        return Err(Error::engine(
            "qdrant vector size must be greater than zero",
        ));
    }
    Ok(())
}

fn validate_vector_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::engine("qdrant vector name must not be empty"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Public target API
// ---------------------------------------------------------------------------

/// A point's vector data: a single dense vector, or a list of vectors for a
/// multi-vector (MAX_SIM) collection.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
enum PointVector {
    Single(Vec<f32>),
    Multi(Vec<Vec<f32>>),
    Named(BTreeMap<String, NamedPointVector>),
}

/// Vector data for one named Qdrant vector field.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum NamedPointVector {
    Single(Vec<f32>),
    Multi(Vec<Vec<f32>>),
}

impl NamedPointVector {
    fn is_multivector(&self) -> bool {
        matches!(self, NamedPointVector::Multi(_))
    }

    fn into_vector(self) -> Vector {
        match self {
            NamedPointVector::Single(v) => Vector::new_dense(v),
            NamedPointVector::Multi(v) => Vector::new_multi(v),
        }
    }
}

/// A point declared into a collection: an id, its vector(s), and a JSON payload.
/// A Qdrant point id — an unsigned integer or a UUID string (Qdrant's two
/// supported id types). Constructible from `u64`, `String`, or `&str`, so
/// `declare_point(ctx, 7u64, ..)` and `declare_point(ctx, "uuid", ..)` both work.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum QdrantPointId {
    Num(u64),
    Uuid(String),
}

impl From<u64> for QdrantPointId {
    fn from(v: u64) -> Self {
        Self::Num(v)
    }
}
impl From<String> for QdrantPointId {
    fn from(v: String) -> Self {
        Self::Uuid(v)
    }
}
impl From<&str> for QdrantPointId {
    fn from(v: &str) -> Self {
        Self::Uuid(v.to_string())
    }
}

impl QdrantPointId {
    /// The stable target-state key (a string for both variants, preserving the
    /// full `u64` range without an `i64` round-trip).
    fn stable_key(&self) -> StableKey {
        match self {
            Self::Num(n) => StableKey::Str(Arc::from(n.to_string())),
            Self::Uuid(s) => StableKey::Str(Arc::from(s.as_str())),
        }
    }

    fn to_qdrant(&self) -> qdrant_client::qdrant::PointId {
        match self {
            Self::Num(n) => (*n).into(),
            Self::Uuid(s) => s.clone().into(),
        }
    }
}

impl std::fmt::Display for QdrantPointId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Num(n) => write!(f, "{n}"),
            Self::Uuid(s) => write!(f, "{s}"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Point {
    id: QdrantPointId,
    vector: PointVector,
    payload: Map<String, JsonValue>,
}

impl Point {
    fn to_qdrant_vectors(&self, schema: &CollectionSchema) -> Result<Vectors> {
        let vector_fields = schema.vector_fields();
        match (&vector_fields, &self.vector) {
            (VectorFields::Single(def), PointVector::Single(v)) => {
                if def.is_multivector() {
                    return Err(Error::engine(format!(
                        "qdrant point {}: collection expects a multivector",
                        self.id
                    )));
                }
                Ok(Vector::new_dense(v.clone()).into())
            }
            (VectorFields::Single(def), PointVector::Multi(v)) => {
                if !def.is_multivector() {
                    return Err(Error::engine(format!(
                        "qdrant point {}: collection expects a single vector",
                        self.id
                    )));
                }
                Ok(Vector::new_multi(v.clone()).into())
            }
            (VectorFields::Single(_), PointVector::Named(_)) => Err(Error::engine(format!(
                "qdrant point {}: collection uses an unnamed vector but point has named vectors",
                self.id
            ))),
            (VectorFields::Named(vectors), PointVector::Named(point_vectors)) => {
                let missing: Vec<_> = vectors
                    .keys()
                    .filter(|name| !point_vectors.contains_key(*name))
                    .cloned()
                    .collect();
                if !missing.is_empty() {
                    return Err(Error::engine(format!(
                        "qdrant point {}: missing vector fields {:?}",
                        self.id, missing
                    )));
                }

                let mut named = HashMap::new();
                for (name, point_vector) in point_vectors {
                    let Some(def) = vectors.get(name) else {
                        return Err(Error::engine(format!(
                            "qdrant point {}: unexpected vector field {:?}",
                            self.id, name
                        )));
                    };
                    if def.is_multivector() != point_vector.is_multivector() {
                        return Err(Error::engine(format!(
                            "qdrant point {}: vector field {:?} has the wrong vector shape",
                            self.id, name
                        )));
                    }
                    named.insert(name.clone(), point_vector.clone().into_vector());
                }
                Ok(NamedVectors { vectors: named }.into())
            }
            (VectorFields::Named(vectors), PointVector::Single(_))
            | (VectorFields::Named(vectors), PointVector::Multi(_)) => {
                let names: Vec<_> = vectors.keys().cloned().collect();
                Err(Error::engine(format!(
                    "qdrant point {}: collection declares named vectors {:?}",
                    self.id, names
                )))
            }
        }
    }
}

/// A declarative Qdrant collection target. See the [module docs](self).
#[derive(Clone)]
pub struct CollectionTarget {
    collection_name: Arc<str>,
    schema: CollectionSchema,
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
    let schema = ts.value().schema.clone();
    let points = mount_target::<CollectionSpec, Point>(ctx, ts).await?;
    Ok(CollectionTarget {
        collection_name: Arc::from(name),
        schema,
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
    let schema = ts.value().schema.clone();
    let points = declare_target_state_with_child::<CollectionSpec, Point>(ctx, ts)?;
    Ok(CollectionTarget {
        collection_name: Arc::from(name),
        schema,
        points,
    })
}

impl CollectionTarget {
    pub fn collection_name(&self) -> &str {
        &self.collection_name
    }

    /// Declare a single-vector point (id + vector + JSON payload) to upsert.
    pub fn declare_point(
        &self,
        ctx: &Ctx,
        id: impl Into<QdrantPointId>,
        vector: Vec<f32>,
        payload: Map<String, JsonValue>,
    ) -> Result<()> {
        let id = id.into();
        let key = id.stable_key();
        let point = Point {
            id,
            vector: PointVector::Single(vector),
            payload,
        };
        point.to_qdrant_vectors(&self.schema)?;
        declare_target_state(ctx, self.points.target_state(key, point))
    }

    /// Declare a multi-vector point (a list of equal-length vectors) for a
    /// collection created with [`CollectionSchema::multivector`]. Scored with
    /// MAX_SIM at query time.
    pub fn declare_multivector_point(
        &self,
        ctx: &Ctx,
        id: impl Into<QdrantPointId>,
        vectors: Vec<Vec<f32>>,
        payload: Map<String, JsonValue>,
    ) -> Result<()> {
        let id = id.into();
        let key = id.stable_key();
        let point = Point {
            id,
            vector: PointVector::Multi(vectors),
            payload,
        };
        point.to_qdrant_vectors(&self.schema)?;
        declare_target_state(ctx, self.points.target_state(key, point))
    }

    /// Declare a point for a named-vector collection. Each map value may be a
    /// single vector or a multivector, depending on the field schema.
    pub fn declare_named_vectors_point<I, K>(
        &self,
        ctx: &Ctx,
        id: impl Into<QdrantPointId>,
        vectors: I,
        payload: Map<String, JsonValue>,
    ) -> Result<()>
    where
        I: IntoIterator<Item = (K, NamedPointVector)>,
        K: Into<String>,
    {
        let id = id.into();
        let key = id.stable_key();
        let point = Point {
            id,
            vector: PointVector::Named(
                vectors
                    .into_iter()
                    .map(|(name, vector)| (name.into(), vector))
                    .collect(),
            ),
            payload,
        };
        point.to_qdrant_vectors(&self.schema)?;
        declare_target_state(ctx, self.points.target_state(key, point))
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
                                a.schema,
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
            CreateCollectionBuilder::new(action.name.clone())
                .vectors_config(action.schema.vectors_config()),
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
    id: QdrantPointId,
    point: Option<Point>,
}

struct PointHandler {
    sink: TargetActionSink<PointAction>,
}

impl PointHandler {
    fn new(client: Arc<Qdrant>, collection_name: String, schema: CollectionSchema) -> Self {
        Self {
            sink: point_sink(client, collection_name, schema),
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

fn point_id(key: &StableKey) -> Result<QdrantPointId> {
    match key {
        // A numeric string is a `u64` id; any other string is treated as a UUID
        // id (Qdrant validates the UUID format at upsert time).
        StableKey::Str(s) => Ok(match s.parse::<u64>() {
            Ok(n) => QdrantPointId::Num(n),
            Err(_) => QdrantPointId::Uuid(s.to_string()),
        }),
        StableKey::Int(i) => u64::try_from(*i)
            .map(QdrantPointId::Num)
            .map_err(|_| Error::engine("negative qdrant point id")),
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

fn point_sink(
    client: Arc<Qdrant>,
    collection_name: String,
    schema: CollectionSchema,
) -> TargetActionSink<PointAction> {
    TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<PointAction>>| {
        let client = client.clone();
        let collection_name = collection_name.clone();
        let schema = schema.clone();
        async move {
            let mut upserts: Vec<PointStruct> = Vec::new();
            let mut deletes: Vec<qdrant_client::qdrant::PointId> = Vec::new();
            for action in actions {
                match action {
                    TargetAction::Create(a) | TargetAction::Update(a) => {
                        if let Some(point) = a.point {
                            let id = point.id.to_qdrant();
                            let vectors = point.to_qdrant_vectors(&schema)?;
                            let payload: Payload = point.payload.into();
                            let struct_ = PointStruct::new(id, vectors, payload);
                            upserts.push(struct_);
                        }
                    }
                    TargetAction::Delete(a) => deletes.push(a.id.to_qdrant()),
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
    vector_search_by_field(conn, collection_name, None, query, top_k).await
}

/// Run a vector search against a named Qdrant vector field.
pub async fn named_vector_search(
    conn: &QdrantConnection,
    collection_name: &str,
    vector_name: &str,
    query: Vec<f32>,
    top_k: u64,
) -> Result<Vec<QdrantHit>> {
    validate_vector_name(vector_name)?;
    vector_search_by_field(conn, collection_name, Some(vector_name), query, top_k).await
}

async fn vector_search_by_field(
    conn: &QdrantConnection,
    collection_name: &str,
    vector_name: Option<&str>,
    query: Vec<f32>,
    top_k: u64,
) -> Result<Vec<QdrantHit>> {
    let mut builder = QueryPointsBuilder::new(collection_name)
        .query(query)
        .limit(top_k)
        .with_payload(true);
    if let Some(vector_name) = vector_name {
        builder = builder.using(vector_name);
    }
    let response = conn
        .client
        .query(builder)
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

/// Run a multi-vector (MAX_SIM) similarity search against a collection created
/// with [`CollectionSchema::multivector`], returning the top-`k` hits. `query`
/// is the list of query vectors (e.g. a ColPali query's token embeddings).
pub async fn multivector_search(
    conn: &QdrantConnection,
    collection_name: &str,
    query: Vec<Vec<f32>>,
    top_k: u64,
) -> Result<Vec<QdrantHit>> {
    multivector_search_by_field(conn, collection_name, None, query, top_k).await
}

/// Run a multivector search against a named Qdrant vector field.
pub async fn named_multivector_search(
    conn: &QdrantConnection,
    collection_name: &str,
    vector_name: &str,
    query: Vec<Vec<f32>>,
    top_k: u64,
) -> Result<Vec<QdrantHit>> {
    validate_vector_name(vector_name)?;
    multivector_search_by_field(conn, collection_name, Some(vector_name), query, top_k).await
}

async fn multivector_search_by_field(
    conn: &QdrantConnection,
    collection_name: &str,
    vector_name: Option<&str>,
    query: Vec<Vec<f32>>,
    top_k: u64,
) -> Result<Vec<QdrantHit>> {
    let mut builder = QueryPointsBuilder::new(collection_name)
        .query(VectorInput::new_multi(query))
        .limit(top_k)
        .with_payload(true);
    if let Some(vector_name) = vector_name {
        builder = builder.using(vector_name);
    }
    let response = conn
        .client
        .query(builder)
        .await
        .map_err(|e| Error::engine(format!("qdrant multivector query: {e}")))?;
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
    fn point_id_parses_numeric_uuid_and_int_keys() {
        assert_eq!(
            point_id(&StableKey::Str(Arc::from("42"))).unwrap(),
            QdrantPointId::Num(42)
        );
        assert_eq!(point_id(&StableKey::Int(7)).unwrap(), QdrantPointId::Num(7));
        // A non-numeric string is taken as a UUID id (Qdrant validates later).
        assert_eq!(
            point_id(&StableKey::Str(Arc::from("9b2e...-uuid"))).unwrap(),
            QdrantPointId::Uuid("9b2e...-uuid".to_string())
        );
        assert!(point_id(&StableKey::Int(-1)).is_err());
    }

    #[test]
    fn point_fingerprint_changes_with_content() {
        let mut payload = Map::new();
        payload.insert("text".into(), JsonValue::from("hello"));
        let p1 = Point {
            id: QdrantPointId::Num(1),
            vector: PointVector::Single(vec![0.1, 0.2]),
            payload: payload.clone(),
        };
        let mut p2 = p1.clone();
        p2.vector = PointVector::Single(vec![0.1, 0.3]);
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

    #[test]
    fn collection_schema_multivector_flag() {
        assert!(!CollectionSchema::new(512, Distance::Cosine).multivector);
        assert!(CollectionSchema::multivector(128, Distance::Cosine).multivector);
    }

    #[test]
    fn collection_schema_reads_legacy_single_vector_state() {
        let schema: CollectionSchema = serde_json::from_value(serde_json::json!({
            "vector_size": 512,
            "distance": "Cosine",
            "multivector": false
        }))
        .unwrap();
        let config: qdrant_client::qdrant::VectorsConfig = schema.vectors_config().into();
        let Some(qdrant_client::qdrant::vectors_config::Config::Params(params)) = config.config
        else {
            panic!("expected unnamed vector params");
        };
        assert_eq!(params.size, 512);
    }

    #[test]
    fn collection_schema_reads_legacy_multivector_state() {
        let schema: CollectionSchema = serde_json::from_value(serde_json::json!({
            "vector_size": 128,
            "distance": "Dot",
            "multivector": true
        }))
        .unwrap();
        let config: qdrant_client::qdrant::VectorsConfig = schema.vectors_config().into();
        let Some(qdrant_client::qdrant::vectors_config::Config::Params(params)) = config.config
        else {
            panic!("expected unnamed vector params");
        };
        assert_eq!(params.size, 128);
        assert!(params.multivector_config.is_some());
    }

    #[test]
    fn collection_schema_named_vectors_config() {
        let schema = CollectionSchema::named([
            ("text", QdrantVectorDef::f32(384, Distance::Cosine).unwrap()),
            (
                "image",
                QdrantVectorDef::multivector(
                    MultiVectorSchema {
                        vector_schema: VectorSchema::f32(128),
                    },
                    Distance::Dot,
                )
                .unwrap(),
            ),
        ])
        .unwrap();
        let config: qdrant_client::qdrant::VectorsConfig = schema.vectors_config().into();
        let Some(qdrant_client::qdrant::vectors_config::Config::ParamsMap(map)) = config.config
        else {
            panic!("expected named vector params map");
        };
        assert_eq!(map.map["text"].size, 384);
        assert_eq!(map.map["image"].size, 128);
        assert!(map.map["image"].multivector_config.is_some());
    }

    #[test]
    fn vector_datatype_reflects_element_type() {
        // f32 vectors leave datatype unset (Qdrant's default); f16 vectors set
        // datatype = Float16. Mirrors Python's `VectorParams(datatype=...)`.
        let schema = CollectionSchema::named([
            ("f32v", QdrantVectorDef::f32(8, Distance::Cosine).unwrap()),
            (
                "f16v",
                QdrantVectorDef::new(VectorSchema::f16(8), Distance::Cosine).unwrap(),
            ),
        ])
        .unwrap();
        let config: qdrant_client::qdrant::VectorsConfig = schema.vectors_config().into();
        let Some(qdrant_client::qdrant::vectors_config::Config::ParamsMap(map)) = config.config
        else {
            panic!("expected named vector params map");
        };
        assert_eq!(map.map["f32v"].datatype, None);
        assert_eq!(
            map.map["f16v"].datatype,
            Some(QdrantDatatype::Float16 as i32)
        );
    }

    #[test]
    fn single_f16_schema_sets_datatype() {
        let schema =
            CollectionSchema::from_vector_schema(VectorSchema::f16(16), Distance::Dot).unwrap();
        let config: qdrant_client::qdrant::VectorsConfig = schema.vectors_config().into();
        let Some(qdrant_client::qdrant::vectors_config::Config::Params(params)) = config.config
        else {
            panic!("expected unnamed vector params");
        };
        assert_eq!(params.datatype, Some(QdrantDatatype::Float16 as i32));
    }

    #[test]
    fn named_schema_rejects_empty_fields() {
        assert!(
            CollectionSchema::named([("", QdrantVectorDef::f32(3, Distance::Cosine).unwrap())])
                .is_err()
        );
        let empty: Vec<(String, QdrantVectorDef)> = Vec::new();
        assert!(CollectionSchema::named(empty).is_err());
    }

    struct StaticVectorProvider(VectorSchema);

    #[async_trait::async_trait]
    impl VectorSchemaProvider for StaticVectorProvider {
        async fn vector_schema(&self) -> Result<VectorSchema> {
            Ok(self.0)
        }
    }

    struct StaticMultiVectorProvider(MultiVectorSchema);

    #[async_trait::async_trait]
    impl MultiVectorSchemaProvider for StaticMultiVectorProvider {
        async fn multi_vector_schema(&self) -> Result<MultiVectorSchema> {
            Ok(self.0)
        }
    }

    #[tokio::test]
    async fn collection_schema_from_providers() {
        let dense = StaticVectorProvider(VectorSchema::f32(256));
        let schema = CollectionSchema::from_vector_provider(&dense, Distance::Euclid)
            .await
            .unwrap();
        assert_eq!(schema.vector_size, 256);
        assert!(!schema.multivector);

        let multi = StaticMultiVectorProvider(MultiVectorSchema {
            vector_schema: VectorSchema::f32(64),
        });
        let schema = CollectionSchema::from_multivector_provider(&multi, Distance::Cosine)
            .await
            .unwrap();
        assert_eq!(schema.vector_size, 64);
        assert!(schema.multivector);
    }

    #[test]
    fn point_vectors_validate_named_schema() {
        let schema = CollectionSchema::named([
            ("text", QdrantVectorDef::f32(3, Distance::Cosine).unwrap()),
            (
                "tokens",
                QdrantVectorDef::multivector(
                    MultiVectorSchema {
                        vector_schema: VectorSchema::f32(2),
                    },
                    Distance::Cosine,
                )
                .unwrap(),
            ),
        ])
        .unwrap();
        let point = Point {
            id: QdrantPointId::Num(1),
            vector: PointVector::Named(BTreeMap::from([
                (
                    "text".to_string(),
                    NamedPointVector::Single(vec![1.0, 2.0, 3.0]),
                ),
                (
                    "tokens".to_string(),
                    NamedPointVector::Multi(vec![vec![0.5, 0.5]]),
                ),
            ])),
            payload: Map::new(),
        };
        assert!(point.to_qdrant_vectors(&schema).is_ok());
    }

    #[test]
    fn point_vectors_reject_named_mismatches() {
        let schema = CollectionSchema::named([
            ("text", QdrantVectorDef::f32(3, Distance::Cosine).unwrap()),
            ("image", QdrantVectorDef::f32(2, Distance::Cosine).unwrap()),
        ])
        .unwrap();
        let missing = Point {
            id: QdrantPointId::Num(1),
            vector: PointVector::Named(BTreeMap::from([(
                "text".to_string(),
                NamedPointVector::Single(vec![1.0, 2.0, 3.0]),
            )])),
            payload: Map::new(),
        };
        assert!(missing.to_qdrant_vectors(&schema).is_err());

        let wrong_shape = Point {
            id: QdrantPointId::Num(1),
            vector: PointVector::Named(BTreeMap::from([
                (
                    "text".to_string(),
                    NamedPointVector::Single(vec![1.0, 2.0, 3.0]),
                ),
                (
                    "image".to_string(),
                    NamedPointVector::Multi(vec![vec![0.5, 0.5]]),
                ),
            ])),
            payload: Map::new(),
        };
        assert!(wrong_shape.to_qdrant_vectors(&schema).is_err());
    }

    #[test]
    fn point_fingerprint_distinguishes_single_and_multi() {
        let payload = Map::new();
        let single = Point {
            id: QdrantPointId::Num(1),
            vector: PointVector::Single(vec![0.1, 0.2]),
            payload: payload.clone(),
        };
        let multi = Point {
            id: QdrantPointId::Num(1),
            vector: PointVector::Multi(vec![vec![0.1, 0.2]]),
            payload,
        };
        // A single vector and a one-element multi-vector are distinct points.
        assert_ne!(point_fingerprint(&single), point_fingerprint(&multi));
    }
}
