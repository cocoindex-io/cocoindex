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
    Distance as QdrantDistance, Modifier as QdrantModifier, MultiVectorComparator,
    MultiVectorConfigBuilder, NamedVectors, PointStruct, PointsIdsList, QueryPointsBuilder,
    SparseVectorParamsBuilder, SparseVectorsConfigBuilder, UpsertPointsBuilder, Vector,
    VectorInput, VectorParams, VectorParamsBuilder, Vectors, VectorsConfigBuilder, value::Kind,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

use crate::ctx::{ContextKey, ContextStore, Ctx};
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
    ChildTargetDef, STATIC_ROOT_PROVIDERS, StableKey, StaticRootProvider, TargetAction,
    TargetActionSink, TargetChildInvalidation, TargetHandler, TargetReconcileOutput, TargetState,
    TargetStateProvider, declare_target_state, declare_target_state_with_child, mount_target,
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
    /// Connect to a Qdrant server (gRPC URL, e.g. `http://localhost:6334`)
    /// with the client's default settings. For an API key, TLS or timeouts,
    /// build the client yourself and pass it to [`QdrantConnection::from_client`].
    pub async fn connect(url: &str) -> Result<Self> {
        let client = Qdrant::from_url(url)
            .build()
            .map_err(|e| Error::engine(format!("qdrant connect {url:?}: {e}")))?;
        Ok(Self::from_client(url, client))
    }

    /// Wrap an already-built `qdrant_client::Qdrant`, e.g. one configured with
    /// an API key (the Rust analogue of passing extra arguments to Python's
    /// `create_client`). `state_id` is the stable identity used for memo
    /// dependencies; it should name the server, not hold a secret.
    ///
    /// ```no_run
    /// # use cocoindex::connectors::qdrant::QdrantConnection;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let url = "https://example.cloud.qdrant.io:6334";
    /// let client = qdrant_client::Qdrant::from_url(url)
    ///     .api_key(std::env::var("QDRANT_API_KEY").ok())
    ///     .build()?;
    /// let conn = QdrantConnection::from_client(url, client);
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_client(state_id: impl Into<String>, client: Qdrant) -> Self {
        Self {
            client: Arc::new(client),
            state_id: Arc::from(state_id.into()),
        }
    }

    /// Stable identity (the URL, or the `state_id` given to
    /// [`QdrantConnection::from_client`]) for use as a `ContextKey` state id /
    /// memo dep.
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
        // Map the SDK element type to Qdrant's stored datatype. Qdrant has no
        // 64-bit float vector, so `Float64` is stored as its default, f32.
        // NOTE: the Python connector currently does *not* forward the schema
        // dtype to `VectorParams(datatype=...)` — a latent Python bug that
        // silently stores an f16 schema as f32. Rust honors the requested f16;
        // Python should be fixed to match (tracked in the parity review).
        match self.element_type() {
            VectorElementType::Float32 | VectorElementType::Float64 => {}
            VectorElementType::Float16 => params = params.datatype(QdrantDatatype::Float16),
        }
        if self.is_multivector() {
            params = params.multivector_config(MultiVectorConfigBuilder::new(
                self.multivector_comparator.to_qdrant(),
            ));
        }
        params.into()
    }
}

/// Value modifier Qdrant applies to a sparse vector at query time.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SparseModifier {
    /// Inverse document frequency.
    Idf,
}

/// Sparse vector definition for one named Qdrant vector field.
///
/// Sparse vectors in Qdrant are always named and use dot-product scoring.
/// `modifier: Some(SparseModifier::Idf)` enables Qdrant's IDF modifier; `None`
/// (the default) applies no modifier.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct QdrantSparseVectorDef {
    pub modifier: Option<SparseModifier>,
}

impl QdrantSparseVectorDef {
    fn to_params(self) -> SparseVectorParamsBuilder {
        let params = SparseVectorParamsBuilder::default();
        match self.modifier {
            Some(SparseModifier::Idf) => params.modifier(QdrantModifier::Idf),
            None => params,
        }
    }
}

/// One entry of [`CollectionSchema::named`]: a dense (or multivector) field or
/// a sparse one. Both definition types convert into it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QdrantNamedVectorDef {
    Dense(QdrantVectorDef),
    Sparse(QdrantSparseVectorDef),
}

impl From<QdrantVectorDef> for QdrantNamedVectorDef {
    fn from(def: QdrantVectorDef) -> Self {
        Self::Dense(def)
    }
}

impl From<QdrantSparseVectorDef> for QdrantNamedVectorDef {
    fn from(def: QdrantSparseVectorDef) -> Self {
        Self::Sparse(def)
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
/// [`CollectionSchema::named`] for Python-style named vectors, dense and
/// sparse.
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
    /// Sparse vector fields of a named-vector collection. They share the
    /// collection's vector namespace with the dense fields in `vectors`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    sparse_vectors: BTreeMap<String, QdrantSparseVectorDef>,
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
            sparse_vectors: BTreeMap::new(),
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
            sparse_vectors: BTreeMap::new(),
        }
    }

    pub fn from_vector_schema(schema: VectorSchema, distance: Distance) -> Result<Self> {
        let def = QdrantVectorDef::new(schema, distance)?;
        Ok(Self {
            vector_size: def.vector_size(),
            distance,
            multivector: false,
            vectors: Some(VectorFields::Single(def)),
            sparse_vectors: BTreeMap::new(),
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
            sparse_vectors: BTreeMap::new(),
        })
    }

    pub async fn from_multivector_provider(
        provider: &(impl MultiVectorSchemaProvider + ?Sized),
        distance: Distance,
    ) -> Result<Self> {
        Self::from_multivector_schema(provider.multi_vector_schema().await?, distance)
    }

    /// Named vector fields: each entry is a [`QdrantVectorDef`] (dense or
    /// multivector) or a [`QdrantSparseVectorDef`]. Dense and sparse vectors
    /// share one namespace in Qdrant, so a name may appear once.
    ///
    /// ```
    /// use cocoindex::connectors::qdrant::{
    ///     CollectionSchema, Distance, QdrantNamedVectorDef, QdrantSparseVectorDef,
    ///     QdrantVectorDef,
    /// };
    /// # fn main() -> cocoindex::Result<()> {
    /// let schema = CollectionSchema::named([
    ///     (
    ///         "dense",
    ///         QdrantNamedVectorDef::from(QdrantVectorDef::f32(384, Distance::Cosine)?),
    ///     ),
    ///     ("sparse", QdrantSparseVectorDef::default().into()),
    /// ])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn named<I, K, V>(vectors: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<QdrantNamedVectorDef>,
    {
        let mut dense = BTreeMap::new();
        let mut sparse = BTreeMap::new();
        for (name, def) in vectors {
            let name = name.into();
            validate_vector_name(&name)?;
            if dense.contains_key(&name) || sparse.contains_key(&name) {
                return Err(Error::engine(format!(
                    "duplicate qdrant vector name: {name:?}"
                )));
            }
            match def.into() {
                QdrantNamedVectorDef::Dense(def) => {
                    dense.insert(name, def);
                }
                QdrantNamedVectorDef::Sparse(def) => {
                    sparse.insert(name, def);
                }
            }
        }
        if dense.is_empty() && sparse.is_empty() {
            return Err(Error::engine(
                "named qdrant schema must declare at least one vector field",
            ));
        }
        // The compatibility fields describe the first dense field; a
        // sparse-only collection has none (sparse vectors score by dot product).
        let (vector_size, distance, multivector) = match dense.values().next() {
            Some(first) => (first.vector_size(), first.distance, first.is_multivector()),
            None => (0, Distance::Dot, false),
        };
        Ok(Self {
            vector_size,
            distance,
            multivector,
            vectors: Some(VectorFields::Named(dense)),
            sparse_vectors: sparse,
        })
    }

    /// Derive named vector definitions from a `#[derive(SchemaFields)]` row.
    /// Non-vector fields remain schemaless Qdrant payload fields, matching the
    /// Python connector. Bare `#[coco(vector)]` fields are resolved later with
    /// [`CollectionSchema::with_vector_dim`].
    pub fn from_row<T: crate::row_schema::SchemaFields>(distance: Distance) -> Result<Self> {
        use crate::row_schema::LogicalType;

        let mut vectors = BTreeMap::new();
        for field in T::schema_fields() {
            let LogicalType::Vector { dim, half } = field.logical_type else {
                continue;
            };
            let size = dim as usize;
            if size > 0 {
                validate_vector_size(size).map_err(|err| {
                    Error::engine(format!(
                        "Qdrant vector field {:?} is invalid: {err}",
                        field.name
                    ))
                })?;
            }
            let schema = if half {
                VectorSchema::f16(size)
            } else {
                VectorSchema::f32(size)
            };
            vectors.insert(
                field.name,
                QdrantVectorDef {
                    schema: QdrantVectorSchema::Dense(schema),
                    distance,
                    multivector_comparator: MultivectorComparator::MaxSim,
                },
            );
        }
        if vectors.is_empty() {
            return Err(Error::engine(
                "Qdrant row schema does not contain a vector field",
            ));
        }
        let first = vectors.values().next().expect("non-empty");
        Ok(Self {
            vector_size: first.vector_size(),
            distance: first.distance,
            multivector: false,
            vectors: Some(VectorFields::Named(vectors)),
            sparse_vectors: BTreeMap::new(),
        })
    }

    /// Resolve or override one vector field's runtime dimension. For schemas
    /// built with [`CollectionSchema::new`] or [`CollectionSchema::multivector`],
    /// the single unnamed vector uses the field name `"vector"`.
    pub fn with_vector_dim(mut self, field_name: &str, dim: usize) -> Result<Self> {
        if dim == 0 {
            return Err(crate::row_schema::zero_vector_dimension_error(
                "Qdrant", field_name,
            ));
        }
        let mut fields = self.vector_fields();
        if self.sparse_vectors.contains_key(field_name) {
            return Err(Error::engine(format!(
                "Qdrant vector field {field_name:?} is sparse and has no dimension"
            )));
        }
        let def = match &mut fields {
            VectorFields::Named(vectors) => vectors.get_mut(field_name),
            VectorFields::Single(def) if field_name == "vector" => Some(def),
            VectorFields::Single(_) => None,
        }
        .ok_or_else(|| crate::row_schema::unknown_vector_field_error("Qdrant", field_name))?;
        match &mut def.schema {
            QdrantVectorSchema::Dense(schema) => schema.size = dim,
            QdrantVectorSchema::Multi(schema) => schema.vector_schema.size = dim,
        }
        let first = match &fields {
            VectorFields::Single(def) => Some(def),
            VectorFields::Named(vectors) => vectors.values().next(),
        };
        if let Some(first) = first {
            self.vector_size = first.vector_size();
        }
        self.vectors = Some(fields);
        Ok(self)
    }

    fn validate_vector_dimensions(&self) -> Result<()> {
        match self.vector_fields() {
            VectorFields::Single(def) => {
                if def.vector_size() == 0 {
                    crate::row_schema::require_resolved_vector_dimension("Qdrant", "vector")?;
                }
                Ok(())
            }
            VectorFields::Named(vectors) => {
                for (name, def) in vectors {
                    if def.vector_size() == 0 {
                        crate::row_schema::require_resolved_vector_dimension("Qdrant", &name)?;
                    }
                }
                Ok(())
            }
        }
    }

    /// The dense vector config, or `None` for a collection that has only
    /// sparse vectors.
    fn vectors_config(&self) -> Option<VectorsConfigBuilder> {
        let mut builder = VectorsConfigBuilder::default();
        match self.vector_fields() {
            VectorFields::Single(def) => {
                builder.add_vector_params(def.to_params());
            }
            VectorFields::Named(vectors) => {
                if vectors.is_empty() {
                    return None;
                }
                for (name, def) in vectors {
                    builder.add_named_vector_params(name, def.to_params());
                }
            }
        }
        Some(builder)
    }

    /// The sparse vector config, or `None` when no sparse field is declared.
    fn sparse_vectors_config(&self) -> Option<SparseVectorsConfigBuilder> {
        if self.sparse_vectors.is_empty() {
            return None;
        }
        let mut builder = SparseVectorsConfigBuilder::default();
        for (name, def) in &self.sparse_vectors {
            builder.add_named_vector_params(name, def.to_params());
        }
        Some(builder)
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
    /// A sparse vector for a field declared with [`QdrantSparseVectorDef`]:
    /// `values[i]` is the weight of dimension `indices[i]`. The indices must
    /// be unique.
    Sparse {
        indices: Vec<u32>,
        values: Vec<f32>,
    },
}

impl NamedPointVector {
    fn into_vector(self) -> Vector {
        match self {
            NamedPointVector::Single(v) => Vector::new_dense(v),
            NamedPointVector::Multi(v) => Vector::new_multi(v),
            NamedPointVector::Sparse { indices, values } => Vector::new_sparse(indices, values),
        }
    }
}

/// Check a sparse vector's own shape: one value per index, no index twice.
fn validate_sparse_vector(indices: &[u32], values: &[f32]) -> std::result::Result<(), String> {
    if indices.len() != values.len() {
        return Err(format!(
            "has {} indices but {} values",
            indices.len(),
            values.len()
        ));
    }
    let mut seen = std::collections::HashSet::with_capacity(indices.len());
    if let Some(dup) = indices.iter().find(|index| !seen.insert(**index)) {
        return Err(format!("repeats index {dup}"));
    }
    Ok(())
}

/// A point declared into a collection: an id, its vector(s), and a JSON payload.
/// A Qdrant point id — an unsigned integer or a UUID string (Qdrant's two
/// supported id types). Constructible from `u64`, `uuid::Uuid`, `String`, or
/// `&str`, so `declare_point(ctx, 7u64, ..)` and `declare_point(ctx, "uuid", ..)`
/// both work.
///
/// A UUID may be given in any text form Qdrant accepts (upper case, 32-char
/// hex, URN, braced); it is stored in its canonical hyphenated lower-case form,
/// so all forms of one UUID name one point and one target state.
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
impl From<uuid::Uuid> for QdrantPointId {
    fn from(v: uuid::Uuid) -> Self {
        Self::Uuid(v.hyphenated().to_string())
    }
}
impl From<String> for QdrantPointId {
    fn from(v: String) -> Self {
        Self::Uuid(canonical_uuid_text(&v).unwrap_or(v))
    }
}
impl From<&str> for QdrantPointId {
    fn from(v: &str) -> Self {
        Self::Uuid(canonical_uuid_text(v).unwrap_or_else(|| v.to_string()))
    }
}

/// The id Qdrant stores a UUID point under: its hyphenated lower-case form.
/// `None` when the text is not a UUID.
fn canonical_uuid_text(text: &str) -> Option<String> {
    uuid::Uuid::parse_str(text)
        .ok()
        .map(|id| id.hyphenated().to_string())
}

const POINT_ID_RULE: &str = "Qdrant point IDs must be an unsigned 64-bit integer or a UUID; see \
     https://qdrant.tech/documentation/manage-data/points/#point-ids. For a stable ID derived \
     from an arbitrary string key, use a version-5 UUID, e.g. \
     uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, format!(\"doc/{key}\").as_bytes()).";

impl QdrantPointId {
    /// Error for a string id that is not a UUID, at declare time: Qdrant would
    /// reject it at upsert time with an opaque transport error.
    fn validate(&self) -> Result<()> {
        match self {
            Self::Num(_) => Ok(()),
            Self::Uuid(text) if canonical_uuid_text(text).is_some() => Ok(()),
            Self::Uuid(text) => Err(Error::engine(format!(
                "Invalid Qdrant point ID {text:?}: strings must be UUIDs. {POINT_ID_RULE}"
            ))),
        }
    }

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
                    .chain(schema.sparse_vectors.keys())
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
                    let shape_matches = if let Some(def) = vectors.get(name) {
                        match point_vector {
                            NamedPointVector::Single(_) => !def.is_multivector(),
                            NamedPointVector::Multi(_) => def.is_multivector(),
                            NamedPointVector::Sparse { .. } => false,
                        }
                    } else if schema.sparse_vectors.contains_key(name) {
                        match point_vector {
                            NamedPointVector::Sparse { indices, values } => {
                                validate_sparse_vector(indices, values).map_err(|detail| {
                                    Error::engine(format!(
                                        "qdrant point {}: sparse vector field {:?} {detail}",
                                        self.id, name
                                    ))
                                })?;
                                true
                            }
                            NamedPointVector::Single(_) | NamedPointVector::Multi(_) => false,
                        }
                    } else {
                        return Err(Error::engine(format!(
                            "qdrant point {}: unexpected vector field {:?}",
                            self.id, name
                        )));
                    };
                    if !shape_matches {
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
                let names: Vec<_> = vectors
                    .keys()
                    .chain(schema.sparse_vectors.keys())
                    .cloned()
                    .collect();
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
    conn: &ContextKey<QdrantConnection>,
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
    conn: &ContextKey<QdrantConnection>,
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
/// [`mount_target`] /
/// [`declare_target_state_with_child`],
/// or use [`declare_collection_target`]/[`mount_collection_target`].
pub fn collection_target(
    ctx: &Ctx,
    conn: &ContextKey<QdrantConnection>,
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
    conn: &ContextKey<QdrantConnection>,
    collection_name: impl Into<String>,
    schema: CollectionSchema,
    options: ManagedTargetOptions,
) -> Result<TargetState<CollectionSpec>> {
    let collection_name = collection_name.into();
    schema.validate_vector_dimensions()?;
    let provider = COLLECTION_PROVIDER.get(ctx)?;
    Ok(provider.target_state(
        CollectionKey {
            conn_key: conn.name().to_string(),
            collection_name: collection_name.clone(),
        }
        .stable_key(),
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
    conn: &ContextKey<QdrantConnection>,
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
    conn: &ContextKey<QdrantConnection>,
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
        id.validate()?;
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
        id.validate()?;
        let key = id.stable_key();
        let point = Point {
            id,
            vector: PointVector::Multi(vectors),
            payload,
        };
        point.to_qdrant_vectors(&self.schema)?;
        declare_target_state(ctx, self.points.target_state(key, point))
    }

    /// Declare a point for a named-vector collection. Each map value is a
    /// single vector, a multivector or a sparse vector, depending on the
    /// field schema.
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
        id.validate()?;
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
    /// The `ContextKey` name of the connection the collection lives in.
    conn_key: String,
    name: String,
    schema: CollectionSchema,
    recreate: bool,
    drop: bool,
    managed_by: ManagedBy,
}

/// Resolve the live Qdrant client from the host context by the connection's
/// stable `db_key` (the ContextKey name), used at apply time.
fn resolve_client(host_ctx: &Arc<ContextStore>, conn_key: &str) -> Result<Arc<Qdrant>> {
    host_ctx
        .resolve::<QdrantConnection>(conn_key)
        .map(|conn| conn.client.clone())
        .ok_or_else(|| {
            Error::engine(format!(
                "qdrant target: connection `{conn_key}` was not provided in the environment \
                 (call Environment::builder().provide_key(&KEY, conn))"
            ))
        })
}

#[linkme::distributed_slice(STATIC_ROOT_PROVIDERS)]
static COLLECTION_PROVIDER: StaticRootProvider =
    StaticRootProvider::new::<CollectionSpec, CollectionHandler>("cocoindex/qdrant/collection");

/// A collection target state's key: the connection's `ContextKey` name plus
/// the collection name.
struct CollectionKey {
    conn_key: String,
    collection_name: String,
}

impl CollectionKey {
    fn stable_key(self) -> StableKey {
        StableKey::Array(Arc::from([
            StableKey::Str(Arc::from(self.conn_key)),
            StableKey::Str(Arc::from(self.collection_name)),
        ]))
    }

    fn from_stable_key(key: &StableKey) -> Result<Self> {
        if let StableKey::Array(parts) = key
            && let [StableKey::Str(conn_key), StableKey::Str(collection_name)] = &parts[..]
        {
            return Ok(Self {
                conn_key: conn_key.to_string(),
                collection_name: collection_name.to_string(),
            });
        }
        Err(Error::engine(format!(
            "unexpected qdrant collection target key: {key:?}"
        )))
    }
}

/// Root handler for every Qdrant collection target: stateless, the connection
/// is named by each target state's [`CollectionKey`].
struct CollectionHandler {
    sink: TargetActionSink<CollectionAction>,
}

impl Default for CollectionHandler {
    fn default() -> Self {
        Self {
            sink: collection_sink(),
        }
    }
}

impl TargetHandler<CollectionSpec> for CollectionHandler {
    type TrackingRecord = MutualTrackingRecord<CollectionSpec>;
    type Action = CollectionAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<CollectionSpec>,
        prev: Vec<MutualTrackingRecord<CollectionSpec>>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<CollectionAction, Self::TrackingRecord>>> {
        let conn_key = CollectionKey::from_stable_key(&key)?.conn_key;
        match desired {
            // Always emit when the collection is declared, so the sink builds
            // the point child handler.
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
                    conn_key,
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
                        conn_key,
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

fn collection_sink() -> TargetActionSink<CollectionAction> {
    TargetActionSink::from_async_fn_with_child_handler_ctx(
        move |host_ctx, actions: Vec<TargetAction<CollectionAction>>| async move {
            for action in actions {
                match action {
                    TargetAction::Create(a) | TargetAction::Update(a) => {
                        let client = resolve_client(&host_ctx, &a.conn_key)?;
                        ensure_collection(&client, &a).await?;
                    }
                    TargetAction::Delete(a) => {
                        let client = resolve_client(&host_ctx, &a.conn_key)?;
                        drop_collection(&client, &a.name).await?;
                    }
                }
            }
            Ok(())
        },
        |a: &CollectionAction| {
            Ok(ChildTargetDef::new::<Point, _>(PointHandler::new(
                a.conn_key.clone(),
                a.name.clone(),
                a.schema.clone(),
            )))
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
    // Dense and sparse fields share one namespace on our side; Qdrant takes
    // them as two config maps.
    let mut create = CreateCollectionBuilder::new(action.name.clone());
    if let Some(vectors_config) = action.schema.vectors_config() {
        create = create.vectors_config(vectors_config);
    }
    if let Some(sparse_vectors_config) = action.schema.sparse_vectors_config() {
        create = create.sparse_vectors_config(sparse_vectors_config);
    }
    client
        .create_collection(create)
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
    fn new(conn_key: String, collection_name: String, schema: CollectionSchema) -> Self {
        Self {
            sink: point_sink(conn_key, collection_name, schema),
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
                let fp = point_fingerprint(&point)?;
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
        // id. A key from before point ids were canonicalized (e.g. an upper-case
        // UUID) maps to the canonical id, so it names the same point.
        StableKey::Str(s) => Ok(match s.parse::<u64>() {
            Ok(n) => QdrantPointId::Num(n),
            Err(_) => QdrantPointId::from(s.as_ref()),
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
fn point_fingerprint(point: &Point) -> Result<String> {
    let fp = cocoindex_utils::fingerprint::Fingerprint::from(&(&point.vector, &point.payload))?;
    Ok(format!("{fp:?}"))
}

/// The points a batch upserts and the ids it deletes. A key from before point
/// ids were canonicalized (e.g. an upper-case UUID) names the same point as its
/// canonical key upserted in the same batch, so deleting it would remove the
/// point just written: such a delete is dropped.
fn split_point_actions(
    actions: Vec<TargetAction<PointAction>>,
) -> (Vec<Point>, Vec<QdrantPointId>) {
    let mut upserts: Vec<Point> = Vec::new();
    let mut deletes: Vec<QdrantPointId> = Vec::new();
    for action in actions {
        match action {
            TargetAction::Create(a) | TargetAction::Update(a) => {
                if let Some(point) = a.point {
                    upserts.push(point);
                }
            }
            TargetAction::Delete(a) => deletes.push(a.id),
        }
    }
    deletes.retain(|id| !upserts.iter().any(|point| point.id == *id));
    (upserts, deletes)
}

fn point_sink(
    conn_key: String,
    collection_name: String,
    schema: CollectionSchema,
) -> TargetActionSink<PointAction> {
    TargetActionSink::from_async_fn_with_ctx(
        move |host_ctx, actions: Vec<TargetAction<PointAction>>| {
            let conn_key = conn_key.clone();
            let collection_name = collection_name.clone();
            let schema = schema.clone();
            async move {
                let client = resolve_client(&host_ctx, &conn_key)?;
                let (upserted, deleted) = split_point_actions(actions);
                let mut upserts: Vec<PointStruct> = Vec::new();
                for point in upserted {
                    let id = point.id.to_qdrant();
                    let vectors = point.to_qdrant_vectors(&schema)?;
                    let payload: Payload = point.payload.into();
                    upserts.push(PointStruct::new(id, vectors, payload));
                }
                let deletes: Vec<qdrant_client::qdrant::PointId> =
                    deleted.iter().map(QdrantPointId::to_qdrant).collect();
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
        },
    )
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
        // A non-numeric string is taken as a UUID id, in its canonical form.
        assert_eq!(
            point_id(&StableKey::Str(Arc::from(
                "9B2E6F1A-0000-4000-8000-000000000001"
            )))
            .unwrap(),
            QdrantPointId::Uuid("9b2e6f1a-0000-4000-8000-000000000001".to_string())
        );
        assert!(point_id(&StableKey::Int(-1)).is_err());
    }

    #[test]
    fn uuid_point_ids_are_canonicalized_and_validated() {
        let canonical = "9b2e6f1a-0000-4000-8000-000000000001";
        for form in [
            "9b2e6f1a-0000-4000-8000-000000000001",
            "9B2E6F1A-0000-4000-8000-000000000001",
            "9b2e6f1a000040008000000000000001",
            "urn:uuid:9b2e6f1a-0000-4000-8000-000000000001",
            "{9b2e6f1a-0000-4000-8000-000000000001}",
        ] {
            let id = QdrantPointId::from(form);
            assert_eq!(id, QdrantPointId::Uuid(canonical.to_string()), "{form}");
            assert_eq!(id.stable_key(), StableKey::Str(Arc::from(canonical)));
            assert!(id.validate().is_ok());
        }
        assert_eq!(
            QdrantPointId::from(uuid::Uuid::parse_str(canonical).unwrap()),
            QdrantPointId::Uuid(canonical.to_string())
        );
        assert!(QdrantPointId::from(7u64).validate().is_ok());
        let err = QdrantPointId::from("doc-1")
            .validate()
            .unwrap_err()
            .to_string();
        assert!(err.contains("strings must be UUIDs"), "{err}");
        assert!(err.contains("new_v5"), "{err}");
    }

    #[test]
    fn a_delete_of_a_point_upserted_under_its_canonical_id_is_dropped() {
        let point = |id: &str| Point {
            id: QdrantPointId::from(id),
            vector: PointVector::Single(vec![0.1, 0.2]),
            payload: Map::new(),
        };
        let a = "9b2e6f1a-0000-4000-8000-000000000001";
        let b = "9b2e6f1a-0000-4000-8000-000000000002";
        let actions = vec![
            TargetAction::Update(PointAction {
                id: QdrantPointId::from(a),
                point: Some(point(a)),
            }),
            // The pre-canonicalization key of the same point.
            TargetAction::Delete(PointAction {
                id: point_id(&StableKey::Str(Arc::from(a.to_uppercase()))).unwrap(),
                point: None,
            }),
            TargetAction::Delete(PointAction {
                id: QdrantPointId::from(b),
                point: None,
            }),
        ];
        let (upserts, deletes) = split_point_actions(actions);
        assert_eq!(upserts.len(), 1);
        assert_eq!(deletes, vec![QdrantPointId::Uuid(b.to_string())]);
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
        let fp = |point: &Point| point_fingerprint(point).unwrap();
        assert_eq!(fp(&p1), fp(&p1.clone()));
        assert_ne!(fp(&p1), fp(&p2));
        assert_ne!(fp(&p1), fp(&p3));
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
        let config: qdrant_client::qdrant::VectorsConfig = schema.vectors_config().unwrap().into();
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
        let config: qdrant_client::qdrant::VectorsConfig = schema.vectors_config().unwrap().into();
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
        let config: qdrant_client::qdrant::VectorsConfig = schema.vectors_config().unwrap().into();
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
        let config: qdrant_client::qdrant::VectorsConfig = schema.vectors_config().unwrap().into();
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
        let config: qdrant_client::qdrant::VectorsConfig = schema.vectors_config().unwrap().into();
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

    fn hybrid_schema(modifier: Option<SparseModifier>) -> CollectionSchema {
        CollectionSchema::named([
            (
                "dense",
                QdrantNamedVectorDef::from(QdrantVectorDef::f32(3, Distance::Cosine).unwrap()),
            ),
            ("sparse", QdrantSparseVectorDef { modifier }.into()),
        ])
        .unwrap()
    }

    fn named_point(vectors: impl IntoIterator<Item = (&'static str, NamedPointVector)>) -> Point {
        Point {
            id: QdrantPointId::Num(1),
            vector: PointVector::Named(
                vectors
                    .into_iter()
                    .map(|(name, vector)| (name.to_string(), vector))
                    .collect(),
            ),
            payload: Map::new(),
        }
    }

    #[test]
    fn named_schema_splits_dense_and_sparse_configs() {
        let schema = hybrid_schema(Some(SparseModifier::Idf));
        let config: qdrant_client::qdrant::VectorsConfig = schema.vectors_config().unwrap().into();
        let Some(qdrant_client::qdrant::vectors_config::Config::ParamsMap(dense)) = config.config
        else {
            panic!("expected named vector params map");
        };
        assert_eq!(dense.map.keys().collect::<Vec<_>>(), ["dense"]);

        let sparse: qdrant_client::qdrant::SparseVectorConfig =
            schema.sparse_vectors_config().unwrap().into();
        assert_eq!(sparse.map.keys().collect::<Vec<_>>(), ["sparse"]);
        assert_eq!(
            sparse.map["sparse"].modifier,
            Some(QdrantModifier::Idf as i32)
        );

        let no_modifier: qdrant_client::qdrant::SparseVectorConfig =
            hybrid_schema(None).sparse_vectors_config().unwrap().into();
        assert_eq!(no_modifier.map["sparse"].modifier, None);
    }

    #[test]
    fn sparse_only_schema_has_no_dense_config() {
        let schema =
            CollectionSchema::named([("sparse", QdrantSparseVectorDef::default())]).unwrap();
        assert!(schema.vectors_config().is_none());
        assert!(schema.sparse_vectors_config().is_some());
        // Nothing to resolve: a sparse field has no dimension.
        schema.validate_vector_dimensions().unwrap();
        assert!(schema.with_vector_dim("sparse", 3).is_err());
    }

    #[test]
    fn dense_schema_has_no_sparse_config() {
        assert!(
            CollectionSchema::new(3, Distance::Cosine)
                .sparse_vectors_config()
                .is_none()
        );
    }

    #[test]
    fn named_schema_rejects_a_name_used_by_both_kinds() {
        let err = CollectionSchema::named([
            (
                "v",
                QdrantNamedVectorDef::from(QdrantVectorDef::f32(3, Distance::Cosine).unwrap()),
            ),
            ("v", QdrantSparseVectorDef::default().into()),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("duplicate"), "{err}");
    }

    #[test]
    fn point_vectors_validate_sparse_fields() {
        let schema = hybrid_schema(None);
        let dense = || NamedPointVector::Single(vec![1.0, 0.0, 0.0]);
        let sparse = |indices: &[u32], values: &[f32]| NamedPointVector::Sparse {
            indices: indices.to_vec(),
            values: values.to_vec(),
        };

        let vectors = named_point([
            ("dense", dense()),
            ("sparse", sparse(&[7, 42], &[0.5, 1.5])),
        ])
        .to_qdrant_vectors(&schema)
        .unwrap();
        let Some(qdrant_client::qdrant::vectors::VectorsOptions::Vectors(named)) =
            vectors.vectors_options
        else {
            panic!("expected named vectors");
        };
        assert_eq!(named.vectors.len(), 2);

        // An empty sparse vector is a valid value.
        named_point([("dense", dense()), ("sparse", sparse(&[], &[]))])
            .to_qdrant_vectors(&schema)
            .unwrap();

        let rejected = [
            // The sparse field is part of the schema, so a point must carry it.
            named_point([("dense", dense())]),
            named_point([("dense", dense()), ("sparse", sparse(&[1, 2], &[0.5]))]),
            named_point([("dense", dense()), ("sparse", sparse(&[1, 1], &[0.5, 0.5]))]),
            named_point([("dense", dense()), ("sparse", dense())]),
            named_point([
                ("dense", sparse(&[1], &[0.5])),
                ("sparse", sparse(&[1], &[0.5])),
            ]),
        ];
        for point in rejected {
            assert!(
                point.to_qdrant_vectors(&schema).is_err(),
                "{:?}",
                point.vector
            );
        }
    }

    #[test]
    fn sparse_schema_change_recreates_the_collection() {
        let handler = CollectionHandler::default();
        let key = CollectionKey {
            conn_key: "db".to_string(),
            collection_name: "docs".to_string(),
        }
        .stable_key();
        let spec = |schema: CollectionSchema| CollectionSpec {
            collection_name: "docs".to_string(),
            schema,
            managed_by: ManagedBy::System,
        };
        let recreate = |prev: CollectionSchema, desired: CollectionSchema| {
            let output = handler
                .reconcile(
                    key.clone(),
                    Some(spec(desired)),
                    vec![MutualTrackingRecord::new(spec(prev), ManagedBy::System)],
                    false,
                )
                .unwrap()
                .unwrap();
            let TargetAction::Update(action) = output.action else {
                panic!("expected an update action");
            };
            action.recreate
        };
        let dense_only = || {
            CollectionSchema::named([("dense", QdrantVectorDef::f32(3, Distance::Cosine).unwrap())])
                .unwrap()
        };

        assert!(recreate(dense_only(), hybrid_schema(None)), "sparse added");
        assert!(
            recreate(
                hybrid_schema(None),
                hybrid_schema(Some(SparseModifier::Idf))
            ),
            "modifier changed"
        );
        assert!(
            recreate(hybrid_schema(None), dense_only()),
            "sparse removed"
        );
        assert!(
            !recreate(hybrid_schema(None), hybrid_schema(None)),
            "unchanged"
        );
    }

    #[test]
    fn schema_stored_before_sparse_vectors_reads_as_dense_only() {
        let dense_only = CollectionSchema::named([(
            "dense",
            QdrantVectorDef::f32(3, Distance::Cosine).unwrap(),
        )])
        .unwrap();
        let mut stored = serde_json::to_value(&dense_only).unwrap();
        // A dense-only schema is stored as it was before sparse vectors existed.
        assert!(stored.as_object().unwrap().get("sparse_vectors").is_none());
        stored.as_object_mut().unwrap().remove("sparse_vectors");
        let read: CollectionSchema = serde_json::from_value(stored).unwrap();
        assert_eq!(read, dense_only);
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
        assert_ne!(
            point_fingerprint(&single).unwrap(),
            point_fingerprint(&multi).unwrap()
        );
    }
}
