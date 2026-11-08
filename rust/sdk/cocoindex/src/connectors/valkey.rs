//! Valkey vector-search target connector (RediSearch `FT.*` over HASH keys).
//!
//! An index target reconciles a search index (`FT.CREATE`/`FT.DROPINDEX`)
//! against the previous run; a `Replace` transition drops the index and purges
//! every document key under its prefix. Document children upsert atomically
//! (`MULTI` `DEL` + `HSET`) and delete via `DEL`, skipping unchanged documents
//! by a (vector, payload) fingerprint.
//!
//! The connection is supplied via a [`ContextKey<Valkey>`]; the same `redis`
//! crate as the FalkorDB connector backs it.

use std::collections::BTreeMap;
use std::sync::Arc;

use cocoindex_utils::fingerprint::Fingerprint;
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};

use crate::ctx::{ContextKey, ContextStore, Ctx};
use crate::error::{Error, Result};
use crate::resources::schema::{VectorSchema, VectorSchemaProvider};
use crate::statediff::{
    DiffAction, ManagedBy, ManagedTargetOptions, MutualTrackingRecord, diff,
    resolve_system_transition,
};
use crate::target_state::{
    ChildTargetDef, STATIC_ROOT_PROVIDERS, StableKey, StaticRootProvider, TargetAction,
    TargetActionSink, TargetChildInvalidation, TargetHandler, TargetReconcileOutput, TargetState,
    TargetStateProvider, declare_target_state, declare_target_state_with_child, mount_target,
};

const VECTOR_FIELD_NAME: &str = "vector";

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// A Valkey connection. Clone-cheap: the multiplexed connection is shared, and
/// every command or pipeline runs on a clone of it, so sinks of different
/// components never wait on one another.
#[derive(Clone)]
pub struct Valkey {
    conn: MultiplexedConnection,
    state_id: Arc<str>,
}

/// Connection settings for [`Valkey::connect_with_config`] — the Rust analogue
/// of Python's `create_client_config`. For anything these fields do not cover
/// (client certificates, a username, a database number), build a
/// `redis::Client` yourself and pass it to [`Valkey::from_client`]. Its `Debug`
/// output shows the password as redacted.
///
/// ```
/// use cocoindex::connectors::valkey::ClientConfig;
///
/// let config = ClientConfig {
///     host: "valkey.internal".to_string(),
///     password: Some("secret".to_string()),
///     use_tls: true,
///     ..ClientConfig::default()
/// };
/// ```
#[derive(Clone)]
pub struct ClientConfig {
    /// Valkey server host. Defaults to `localhost`.
    pub host: String,
    /// Valkey server port. Defaults to `6379`.
    pub port: u16,
    /// Optional authentication password.
    pub password: Option<String>,
    /// Whether to use TLS for the connection.
    pub use_tls: bool,
    /// Client name for the connection, visible in `CLIENT LIST` and monitoring
    /// dashboards. Defaults to `cocoindex_vector_store`; `None` sets no name.
    pub client_name: Option<String>,
}

/// Prints every field but the password, which is shown as redacted.
impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("use_tls", &self.use_tls)
            .field("client_name", &self.client_name)
            .finish()
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 6379,
            password: None,
            use_tls: false,
            client_name: Some("cocoindex_vector_store".to_string()),
        }
    }
}

impl ClientConfig {
    fn connection_info(&self) -> redis::ConnectionInfo {
        let addr = if self.use_tls {
            redis::ConnectionAddr::TcpTls {
                host: self.host.clone(),
                port: self.port,
                insecure: false,
                tls_params: None,
            }
        } else {
            redis::ConnectionAddr::Tcp(self.host.clone(), self.port)
        };
        redis::ConnectionInfo {
            addr,
            redis: redis::RedisConnectionInfo {
                password: self.password.clone(),
                ..Default::default()
            },
        }
    }

    /// The server's address as a URI, without the password.
    fn state_id(&self) -> String {
        state_id_of(&self.connection_info())
    }
}

/// The stable identity of a server: its address and database number as a URI,
/// never the username or password the connection authenticates with. A memo
/// dependency and the target-state store carry this id, so a credential must
/// not leak into them, and rotating one must not invalidate them.
fn state_id_of(info: &redis::ConnectionInfo) -> String {
    let mut id = match &info.addr {
        redis::ConnectionAddr::Tcp(host, port) => format!("redis://{host}:{port}"),
        redis::ConnectionAddr::TcpTls { host, port, .. } => format!("rediss://{host}:{port}"),
        redis::ConnectionAddr::Unix(path) => format!("redis+unix://{}", path.display()),
    };
    if info.redis.db != 0 {
        id.push('/');
        id.push_str(&info.redis.db.to_string());
    }
    id
}

impl Valkey {
    /// Connect to a Valkey/Redis server (e.g. `redis://localhost:6379`). The
    /// connection's [`state_id`](Self::state_id) is the server's address and
    /// database number; a password or username in the URI stays out of it.
    pub async fn connect(uri: &str) -> Result<Self> {
        let client =
            redis::Client::open(uri).map_err(|e| Error::engine(format!("valkey config: {e}")))?;
        Self::from_client(state_id_of(client.get_connection_info()), &client).await
    }

    /// Connect with a host, port, password, TLS and client name (see
    /// [`ClientConfig`]).
    pub async fn connect_with_config(config: ClientConfig) -> Result<Self> {
        let client = redis::Client::open(config.connection_info())
            .map_err(|e| Error::engine(format!("valkey config: {e}")))?;
        let db = Self::from_client(config.state_id(), &client).await?;
        if let Some(client_name) = &config.client_name {
            let mut conn = db.conn.clone();
            let _: redis::Value = redis::cmd("CLIENT")
                .arg("SETNAME")
                .arg(client_name)
                .query_async(&mut conn)
                .await
                .map_err(|e| Error::engine(format!("valkey CLIENT SETNAME {client_name}: {e}")))?;
        }
        Ok(db)
    }

    /// Connect through an already-configured `redis::Client` (advanced use:
    /// client certificates, a username, a database number). `state_id` is the
    /// stable identity used for memo dependencies; it should name the server,
    /// not hold a secret.
    pub async fn from_client(state_id: impl Into<String>, client: &redis::Client) -> Result<Self> {
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::engine(format!("valkey connect: {e}")))?;
        Ok(Self {
            conn,
            state_id: Arc::from(state_id.into()),
        })
    }

    /// Stable identity (the server address without credentials, or the
    /// `state_id` given to [`Valkey::from_client`]) for use as a `ContextKey`
    /// state id / memo dep.
    pub fn state_id(&self) -> &str {
        &self.state_id
    }
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// Vector distance metric.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum Distance {
    #[default]
    Cosine,
    L2,
    Ip,
}

impl Distance {
    fn as_metric(self) -> &'static str {
        match self {
            Distance::Cosine => "COSINE",
            Distance::L2 => "L2",
            Distance::Ip => "IP",
        }
    }
}

/// Vector index algorithm.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum VectorAlgorithm {
    #[default]
    Hnsw,
    Flat,
}

impl VectorAlgorithm {
    fn as_keyword(self) -> &'static str {
        match self {
            VectorAlgorithm::Hnsw => "HNSW",
            VectorAlgorithm::Flat => "FLAT",
        }
    }
}

/// Indexed payload field type.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum FieldType {
    Text,
    Tag,
    Numeric,
}

impl FieldType {
    fn as_keyword(self) -> &'static str {
        match self {
            FieldType::Text => "TEXT",
            FieldType::Tag => "TAG",
            FieldType::Numeric => "NUMERIC",
        }
    }
}

/// A vector field specification: a schema provider for the dimension plus the
/// distance metric and index algorithm.
pub struct VectorDef<'a> {
    /// Provides the vector dimension (e.g. an embedder).
    pub schema: &'a (dyn VectorSchemaProvider + 'a),
    pub distance: Distance,
    pub algorithm: VectorAlgorithm,
}

impl<'a> VectorDef<'a> {
    /// A vector def with the default distance (cosine) and algorithm (HNSW).
    pub fn new(schema: &'a (dyn VectorSchemaProvider + 'a)) -> Self {
        Self {
            schema,
            distance: Distance::default(),
            algorithm: VectorAlgorithm::default(),
        }
    }
}

/// A resolved vector field — dimension already discovered from the provider.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedVectorDef {
    schema: VectorSchema,
    distance: Distance,
    algorithm: VectorAlgorithm,
}

/// Definition of an indexed payload field included in `FT.CREATE`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldDef {
    pub name: String,
    pub field_type: FieldType,
    pub sortable: bool,
}

impl FieldDef {
    pub fn new(name: impl Into<String>, field_type: FieldType) -> Self {
        Self {
            name: name.into(),
            field_type,
            sortable: false,
        }
    }

    pub fn sortable(mut self) -> Self {
        self.sortable = true;
        self
    }
}

/// Schema for a Valkey search index: the resolved vector field plus optional
/// indexed payload fields.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexSchema {
    vectors: ResolvedVectorDef,
    fields: Vec<FieldDef>,
}

impl IndexSchema {
    /// Resolve the vector dimension from the provider and build the schema.
    pub async fn create(vectors: VectorDef<'_>, fields: Vec<FieldDef>) -> Result<Self> {
        let schema = vectors.schema.vector_schema().await?;
        if schema.size == 0 {
            return Err(Error::engine(
                "valkey vector size must be greater than zero",
            ));
        }
        for f in &fields {
            validate_name(&f.name, "field name")?;
        }
        Ok(Self {
            vectors: ResolvedVectorDef {
                schema,
                distance: vectors.distance,
                algorithm: vectors.algorithm,
            },
            fields,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Validate that a name contains only `[A-Za-z0-9_-]+`, keeping it out of the
/// search DSL and away from key-prefix collisions.
fn validate_name(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(Error::engine(format!(
            "valkey {label} must match [A-Za-z0-9_-]+, got: {value:?}"
        )));
    }
    Ok(())
}

/// The document-key prefix for an index (`"<index>:"`).
fn make_prefix(index_name: &str) -> String {
    format!("{index_name}:")
}

/// The full hash key for a document (`"<index>:<doc_id>"`).
fn make_hash_key(index_name: &str, doc_id: &str) -> String {
    format!("{}{}", make_prefix(index_name), doc_id)
}

/// Pack a vector into little-endian float32 bytes for HASH storage.
fn vector_to_bytes(vector: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(vector.len() * 4);
    for v in vector {
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    bytes
}

/// Build the `FT.CREATE` argument vector (after the command word).
fn ft_create_args(index_name: &str, schema: &IndexSchema) -> Vec<String> {
    let vec_def = &schema.vectors;
    let prefix = make_prefix(index_name);
    let mut args = vec![
        index_name.to_string(),
        "ON".to_string(),
        "HASH".to_string(),
        "PREFIX".to_string(),
        "1".to_string(),
        prefix,
        "SCHEMA".to_string(),
        VECTOR_FIELD_NAME.to_string(),
        "VECTOR".to_string(),
        vec_def.algorithm.as_keyword().to_string(),
        "6".to_string(),
        "TYPE".to_string(),
        "FLOAT32".to_string(),
        "DIM".to_string(),
        vec_def.schema.size.to_string(),
        "DISTANCE_METRIC".to_string(),
        vec_def.distance.as_metric().to_string(),
    ];
    for field in &schema.fields {
        args.push(field.name.clone());
        args.push(field.field_type.as_keyword().to_string());
        if field.sortable {
            args.push("SORTABLE".to_string());
        }
    }
    args
}

/// Resolve the live Valkey connection from the host context by `db_key`.
fn resolve_db(host_ctx: &Arc<ContextStore>, db_key: &str) -> Result<Arc<Valkey>> {
    host_ctx.resolve::<Valkey>(db_key).ok_or_else(|| {
        Error::engine(format!(
            "valkey target: connection `{db_key}` was not provided in the environment \
             (call Environment::builder().provide_key(&KEY, valkey))"
        ))
    })
}

async fn index_exists(db: &Valkey, index_name: &str) -> Result<bool> {
    let mut conn = db.conn.clone();
    let names: Vec<String> = redis::cmd("FT._LIST")
        .query_async(&mut conn)
        .await
        .map_err(|e| Error::engine(format!("valkey FT._LIST: {e}")))?;
    Ok(names.iter().any(|n| n == index_name))
}

// ---------------------------------------------------------------------------
// Public target API
// ---------------------------------------------------------------------------

/// A payload field value: text, or a number for a `NUMERIC` field. Stored as
/// its text form in the hash; the type still tells a changed document apart
/// (`1` and `"1"` fingerprint differently, as in the Python SDK).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PayloadValue {
    Str(String),
    Int(i64),
    Float(f64),
}

impl From<String> for PayloadValue {
    fn from(v: String) -> Self {
        Self::Str(v)
    }
}
impl From<&str> for PayloadValue {
    fn from(v: &str) -> Self {
        Self::Str(v.to_string())
    }
}
impl From<i64> for PayloadValue {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}
impl From<f64> for PayloadValue {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl std::fmt::Display for PayloadValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Str(s) => f.write_str(s),
            Self::Int(i) => write!(f, "{i}"),
            Self::Float(x) => write!(f, "{x:?}"),
        }
    }
}

/// A document to store in a Valkey index.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Document {
    pub id: String,
    pub vector: Vec<f32>,
    /// Extra hash fields stored beside the vector; a value is text or a number
    /// (see [`PayloadValue`]). The key `vector` is reserved for the vector.
    pub payload: Option<BTreeMap<String, PayloadValue>>,
}

impl Document {
    pub fn new(id: impl Into<String>, vector: Vec<f32>) -> Self {
        Self {
            id: id.into(),
            vector,
            payload: None,
        }
    }

    pub fn with_payload<K: Into<String>, V: Into<PayloadValue>>(
        mut self,
        payload: impl IntoIterator<Item = (K, V)>,
    ) -> Self {
        self.payload = Some(
            payload
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        );
        self
    }
}

/// A declarative Valkey index target — a handle to declare documents on.
///
/// As a `mount_each!` / `use_mount!` or memoized function argument it
/// fingerprints by its stable key, the database key and index name; the
/// vector dimension and runtime provider are not part of the identity.
#[derive(Clone, Serialize)]
pub struct IndexTarget {
    db_key: Arc<str>,
    index_name: Arc<str>,
    #[serde(skip)]
    dimension: usize,
    #[serde(skip)]
    documents: TargetStateProvider<Document>,
}

/// Build a composable [`TargetState`] for a Valkey index. Pass it to
/// [`declare_index_target`]/[`mount_index_target`] or the generic
/// [`declare_target_state_with_child`]/[`mount_target`].
pub fn index_target(
    ctx: &Ctx,
    db: &ContextKey<Valkey>,
    index_name: impl Into<String>,
    schema: IndexSchema,
) -> Result<TargetState<IndexSpec>> {
    index_target_with_options(ctx, db, index_name, schema, ManagedTargetOptions::default())
}

/// [`index_target`] with explicit [`ManagedTargetOptions`] (`managed_by`).
pub fn index_target_with_options(
    ctx: &Ctx,
    db: &ContextKey<Valkey>,
    index_name: impl Into<String>,
    schema: IndexSchema,
    options: ManagedTargetOptions,
) -> Result<TargetState<IndexSpec>> {
    let index_name = index_name.into();
    validate_name(&index_name, "index_name")?;
    let provider = INDEX_PROVIDER.get(ctx)?;
    Ok(provider.target_state(
        IndexKey {
            db_key: db.name().to_string(),
            index_name: index_name.clone(),
        }
        .stable_key(),
        IndexSpec {
            index_name,
            schema,
            managed_by: options.managed_by,
        },
    ))
}

/// Declare a Valkey index target in the **current** component and return a
/// pending handle. Documents declared on it resolve when the component commits;
/// use [`mount_index_target`] when documents must be declared immediately.
pub fn declare_index_target(
    ctx: &Ctx,
    db: &ContextKey<Valkey>,
    index_name: impl Into<String>,
    schema: IndexSchema,
) -> Result<IndexTarget> {
    declare_index_target_with_options(ctx, db, index_name, schema, ManagedTargetOptions::default())
}

/// [`declare_index_target`] with explicit [`ManagedTargetOptions`]
/// (`managed_by`).
pub fn declare_index_target_with_options(
    ctx: &Ctx,
    db: &ContextKey<Valkey>,
    index_name: impl Into<String>,
    schema: IndexSchema,
    options: ManagedTargetOptions,
) -> Result<IndexTarget> {
    let ts = index_target_with_options(ctx, db, index_name, schema, options)?;
    let name = ts.value().index_name.clone();
    let dimension = ts.value().schema.vectors.schema.size;
    let documents = declare_target_state_with_child::<IndexSpec, Document>(ctx, ts)?;
    Ok(IndexTarget {
        db_key: Arc::from(db.name()),
        index_name: Arc::from(name),
        dimension,
        documents,
    })
}

/// Mount a Valkey index target and return a ready-to-use handle. The index is
/// created to match `schema`; declared documents are upserted; orphaned
/// documents are deleted; an incompatible schema change recreates the index and
/// purges its documents.
pub async fn mount_index_target(
    ctx: &Ctx,
    db: &ContextKey<Valkey>,
    index_name: impl Into<String>,
    schema: IndexSchema,
) -> Result<IndexTarget> {
    mount_index_target_with_options(ctx, db, index_name, schema, ManagedTargetOptions::default())
        .await
}

/// [`mount_index_target`] with explicit [`ManagedTargetOptions`]
/// (`managed_by`): a user-managed index is never created, recreated or dropped
/// by CocoIndex, only its documents are.
pub async fn mount_index_target_with_options(
    ctx: &Ctx,
    db: &ContextKey<Valkey>,
    index_name: impl Into<String>,
    schema: IndexSchema,
    options: ManagedTargetOptions,
) -> Result<IndexTarget> {
    let ts = index_target_with_options(ctx, db, index_name, schema, options)?;
    let name = ts.value().index_name.clone();
    let dimension = ts.value().schema.vectors.schema.size;
    let documents = mount_target::<IndexSpec, Document>(ctx, ts).await?;
    Ok(IndexTarget {
        db_key: Arc::from(db.name()),
        index_name: Arc::from(name),
        dimension,
        documents,
    })
}

impl IndexTarget {
    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    /// Declare a document to upsert into the index. Its vector must have the
    /// index's dimension and finite elements: the search module skips a hash
    /// whose vector it cannot read, without an error, so such a document
    /// would be stored but never found.
    pub fn declare_document(&self, ctx: &Ctx, document: Document) -> Result<()> {
        validate_name(&document.id, "doc_id")?;
        if document.vector.len() != self.dimension {
            return Err(Error::engine(format!(
                "valkey document {:?}: vector has {} elements but index {:?} expects {}",
                document.id,
                document.vector.len(),
                self.index_name,
                self.dimension
            )));
        }
        if document.vector.iter().any(|x| !x.is_finite()) {
            return Err(Error::engine(format!(
                "valkey document {:?}: vector contains a non-finite (NaN/Inf) element",
                document.id
            )));
        }
        for key in document.payload.iter().flat_map(|p| p.keys()) {
            if key == VECTOR_FIELD_NAME {
                return Err(Error::engine(format!(
                    "valkey payload key {key:?} is reserved for the vector field"
                )));
            }
        }
        let key = StableKey::Str(Arc::from(document.id.as_str()));
        declare_target_state(ctx, self.documents.target_state(key, document))
    }
}

// ---------------------------------------------------------------------------
// Index handler (container)
// ---------------------------------------------------------------------------

/// Spec for a Valkey index (the declared container value).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IndexSpec {
    index_name: String,
    schema: IndexSchema,
    managed_by: ManagedBy,
}

/// Tracking-record core: the index name plus the resolved vector def + fields.
/// Equality decides whether the index is recreated; the name lets the drop path
/// recover the index to drop when nothing is declared.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct IndexTrackingRecordCore {
    index_name: String,
    vectors: ResolvedVectorDef,
    fields: Vec<FieldDef>,
}

type IndexTrackingRecord = MutualTrackingRecord<IndexTrackingRecordCore>;

fn tracking_record_core(spec: &IndexSpec) -> IndexTrackingRecordCore {
    IndexTrackingRecordCore {
        index_name: spec.index_name.clone(),
        vectors: spec.schema.vectors,
        fields: spec.schema.fields.clone(),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct IndexAction {
    /// The `ContextKey` name of the database the index lives in.
    db_key: String,
    index_name: String,
    spec: Option<IndexSpec>,
    /// `Replace` purges documents + recreates; `Delete` only drops the index.
    main_action: Option<DiffAction>,
}

#[linkme::distributed_slice(STATIC_ROOT_PROVIDERS)]
static INDEX_PROVIDER: StaticRootProvider =
    StaticRootProvider::new::<IndexSpec, IndexHandler>("cocoindex/valkey/index");

/// An index target state's key: the database's `ContextKey` name plus the
/// index name.
struct IndexKey {
    db_key: String,
    index_name: String,
}

impl IndexKey {
    fn stable_key(self) -> StableKey {
        StableKey::Array(Arc::from([
            StableKey::Str(Arc::from(self.db_key)),
            StableKey::Str(Arc::from(self.index_name)),
        ]))
    }

    fn from_stable_key(key: &StableKey) -> Result<Self> {
        if let StableKey::Array(parts) = key
            && let [StableKey::Str(db_key), StableKey::Str(index_name)] = &parts[..]
        {
            return Ok(Self {
                db_key: db_key.to_string(),
                index_name: index_name.to_string(),
            });
        }
        Err(Error::engine(format!(
            "unexpected valkey index target key: {key:?}"
        )))
    }
}

/// Root handler for every Valkey index target: stateless, the database is
/// named by each target state's [`IndexKey`].
struct IndexHandler {
    sink: TargetActionSink<IndexAction>,
}

impl Default for IndexHandler {
    fn default() -> Self {
        Self { sink: index_sink() }
    }
}

impl TargetHandler<IndexSpec> for IndexHandler {
    type TrackingRecord = IndexTrackingRecord;
    type Action = IndexAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<IndexSpec>,
        prev: Vec<IndexTrackingRecord>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<IndexAction, IndexTrackingRecord>>> {
        let db_key = IndexKey::from_stable_key(&key)?.db_key;
        match desired {
            // Always emit when declared so the sink builds the document child.
            Some(spec) => {
                let prev_is_empty = prev.is_empty();
                let tracking =
                    MutualTrackingRecord::new(tracking_record_core(&spec), spec.managed_by);
                let resolved =
                    resolve_system_transition(Some(tracking.clone()), prev, prev_may_be_missing);
                let main_action = diff(resolved.as_ref());
                // An index rebuild destroys every document under its prefix.
                let replace = matches!(main_action, Some(DiffAction::Replace));
                let action = IndexAction {
                    db_key,
                    index_name: spec.index_name.clone(),
                    spec: Some(spec),
                    main_action,
                };
                let target_action = if prev_is_empty {
                    TargetAction::Create(action)
                } else {
                    TargetAction::Update(action)
                };
                Ok(Some(TargetReconcileOutput {
                    action: target_action,
                    sink: self.sink.clone(),
                    tracking_record: Some(tracking),
                    child_invalidation: replace.then_some(TargetChildInvalidation::Destructive),
                }))
            }
            None => {
                let resolved = resolve_system_transition(None, prev.clone(), prev_may_be_missing);
                if resolved.is_none() {
                    return Ok(None);
                }
                let Some(index_name) = prev
                    .into_iter()
                    .find(|p| p.managed_by.is_system())
                    .map(|p| p.tracking_record.index_name)
                else {
                    return Ok(None);
                };
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Delete(IndexAction {
                        db_key,
                        index_name,
                        spec: None,
                        main_action: Some(DiffAction::Delete),
                    }),
                    sink: self.sink.clone(),
                    tracking_record: None,
                    child_invalidation: Some(TargetChildInvalidation::Destructive),
                }))
            }
        }
    }
}

fn index_sink() -> TargetActionSink<IndexAction> {
    TargetActionSink::from_async_fn_with_child_handler_ctx(
        move |host_ctx, actions: Vec<TargetAction<IndexAction>>| async move {
            for action in actions {
                match action {
                    TargetAction::Create(a) | TargetAction::Update(a) => {
                        let db = resolve_db(&host_ctx, &a.db_key)?;
                        let spec = a.spec.ok_or_else(|| {
                            Error::engine("valkey create/update action missing spec")
                        })?;
                        if matches!(
                            a.main_action,
                            Some(DiffAction::Replace | DiffAction::Delete)
                        ) {
                            drop_index(&db, &a.index_name).await?;
                            if matches!(a.main_action, Some(DiffAction::Replace)) {
                                delete_prefix_keys(&db, &a.index_name).await?;
                            }
                        }
                        if spec.managed_by.is_system()
                            && matches!(
                                a.main_action,
                                Some(DiffAction::Insert | DiffAction::Upsert | DiffAction::Replace)
                            )
                        {
                            create_index(&db, &a.index_name, &spec.schema).await?;
                        }
                    }
                    TargetAction::Delete(a) => {
                        let db = resolve_db(&host_ctx, &a.db_key)?;
                        drop_index(&db, &a.index_name).await?;
                        delete_prefix_keys(&db, &a.index_name).await?;
                    }
                }
            }
            Ok(())
        },
        |a: &IndexAction| {
            Ok(ChildTargetDef::new::<Document, _>(DocumentHandler::new(
                a.db_key.clone(),
                a.index_name.clone(),
            )))
        },
    )
}

/// Create the index unless it exists. An `Insert` (no tracked record) can
/// still find it in place, e.g. created by an earlier version whose tracking
/// used a different target-state key; a `Replace` has just dropped it.
async fn create_index(db: &Valkey, index_name: &str, schema: &IndexSchema) -> Result<()> {
    if index_exists(db, index_name).await? {
        return Ok(());
    }
    let args = ft_create_args(index_name, schema);
    let mut conn = db.conn.clone();
    let mut cmd = redis::cmd("FT.CREATE");
    for arg in &args {
        cmd.arg(arg);
    }
    let _: redis::Value = cmd
        .query_async(&mut conn)
        .await
        .map_err(|e| Error::engine(format!("valkey FT.CREATE {index_name}: {e}")))?;
    Ok(())
}

/// Drop the index if present. An "unknown index" error (already removed
/// externally) is ignored; any other error (transport, auth, …) propagates so it
/// is not masked by a later, more confusing failure. Mirrors Python's narrow
/// `RequestError` swallow.
async fn drop_index(db: &Valkey, index_name: &str) -> Result<()> {
    let mut conn = db.conn.clone();
    let result: redis::RedisResult<redis::Value> = redis::cmd("FT.DROPINDEX")
        .arg(index_name)
        .query_async(&mut conn)
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(e) if e.to_string().to_ascii_lowercase().contains("unknown index") => Ok(()),
        Err(e) => Err(Error::engine(format!(
            "valkey FT.DROPINDEX {index_name}: {e}"
        ))),
    }
}

/// Delete every hash key under the index prefix via a `SCAN`/`DEL` loop.
async fn delete_prefix_keys(db: &Valkey, index_name: &str) -> Result<()> {
    const MAX_ITERATIONS: usize = 10_000;
    let pattern = format!("{}*", make_prefix(index_name));
    let mut cursor: u64 = 0;
    let mut iterations = 0usize;
    loop {
        iterations += 1;
        if iterations > MAX_ITERATIONS {
            // Don't silently truncate a purge — stray keys would be left behind.
            tracing::warn!(
                pattern = %pattern,
                max_iterations = MAX_ITERATIONS,
                "valkey prefix-key purge hit its SCAN safety limit; some keys may remain"
            );
            break;
        }
        let mut conn = db.conn.clone();
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(500)
            .query_async(&mut conn)
            .await
            .map_err(|e| Error::engine(format!("valkey SCAN {pattern}: {e}")))?;
        if !keys.is_empty() {
            let mut del = redis::cmd("DEL");
            for k in &keys {
                del.arg(k);
            }
            let _: redis::Value = del
                .query_async(&mut conn)
                .await
                .map_err(|e| Error::engine(format!("valkey DEL: {e}")))?;
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Document handler (child)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DocumentAction {
    hash_key: String,
    /// `Some` upserts the hash fields; `None` deletes the key.
    document: Option<Document>,
}

struct DocumentHandler {
    sink: TargetActionSink<DocumentAction>,
    index_name: String,
}

impl DocumentHandler {
    fn new(db_key: String, index_name: String) -> Self {
        Self {
            sink: document_sink(db_key),
            index_name,
        }
    }
}

impl TargetHandler<Document> for DocumentHandler {
    type TrackingRecord = Fingerprint;
    type Action = DocumentAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<Document>,
        prev: Vec<Fingerprint>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<DocumentAction, Fingerprint>>> {
        let doc_id = stable_key_to_doc_id(&key)?;
        let hash_key = make_hash_key(&self.index_name, &doc_id);
        match desired {
            Some(document) => {
                let fp = Fingerprint::from(&(&document.vector, &document.payload))
                    .map_err(Error::from)?;
                let unchanged =
                    !prev_may_be_missing && !prev.is_empty() && prev.iter().all(|p| *p == fp);
                if unchanged {
                    return Ok(None);
                }
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Update(DocumentAction {
                        hash_key,
                        document: Some(document),
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
                    action: TargetAction::Delete(DocumentAction {
                        hash_key,
                        document: None,
                    }),
                    sink: self.sink.clone(),
                    tracking_record: None,
                    child_invalidation: None,
                }))
            }
        }
    }
}

fn stable_key_to_doc_id(key: &StableKey) -> Result<String> {
    match key {
        StableKey::Str(s) | StableKey::Symbol(s) => Ok(s.to_string()),
        StableKey::Int(i) => Ok(i.to_string()),
        other => Err(Error::engine(format!(
            "unsupported valkey doc key: {other:?}"
        ))),
    }
}

/// Documents per pipeline: one round trip writes up to this many documents.
const DOCUMENTS_PER_PIPELINE: usize = 1_000;

/// The pipelines that apply a batch of document actions: one `DEL` for every
/// deleted key, then the upserts in chunks of [`DOCUMENTS_PER_PIPELINE`], each
/// document as its own `MULTI` `DEL` `HSET` `EXEC` transaction so no stale
/// payload field survives an update and a document is never half written. A
/// batch of `n` documents costs `1 + ceil(n / DOCUMENTS_PER_PIPELINE)` round
/// trips, not `n`.
fn document_pipelines(deletes: &[String], upserts: &[(String, Document)]) -> Vec<redis::Pipeline> {
    let mut pipelines = Vec::new();
    if !deletes.is_empty() {
        let mut pipe = redis::pipe();
        pipe.cmd("DEL").arg(deletes).ignore();
        pipelines.push(pipe);
    }
    for chunk in upserts.chunks(DOCUMENTS_PER_PIPELINE) {
        let mut pipe = redis::pipe();
        for (hash_key, doc) in chunk {
            pipe.cmd("MULTI").ignore();
            pipe.cmd("DEL").arg(hash_key).ignore();
            let mut hset = redis::cmd("HSET");
            hset.arg(hash_key);
            hset.arg(VECTOR_FIELD_NAME)
                .arg(vector_to_bytes(&doc.vector));
            if let Some(payload) = &doc.payload {
                for (k, v) in payload {
                    hset.arg(k).arg(v.to_string());
                }
            }
            pipe.add_command(hset).ignore();
            pipe.cmd("EXEC");
        }
        pipelines.push(pipe);
    }
    pipelines
}

fn document_sink(db_key: String) -> TargetActionSink<DocumentAction> {
    TargetActionSink::from_async_fn_with_ctx(
        move |host_ctx, actions: Vec<TargetAction<DocumentAction>>| {
            let db_key = db_key.clone();
            async move {
                let db = resolve_db(&host_ctx, &db_key)?;
                let mut deletes: Vec<String> = Vec::new();
                let mut upserts: Vec<(String, Document)> = Vec::new();
                for action in actions {
                    let a = match action {
                        TargetAction::Create(a)
                        | TargetAction::Update(a)
                        | TargetAction::Delete(a) => a,
                    };
                    match a.document {
                        Some(doc) => upserts.push((a.hash_key, doc)),
                        None => deletes.push(a.hash_key),
                    }
                }
                let mut conn = db.conn.clone();
                for pipe in document_pipelines(&deletes, &upserts) {
                    // A transaction's `EXEC` reply carries any error of its
                    // commands; the pipeline query surfaces it as `Err`.
                    let _: Vec<redis::Value> = pipe
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| Error::engine(format!("valkey write documents: {e}")))?;
                }
                Ok(())
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_to_bytes_is_little_endian_f32() {
        let bytes = vector_to_bytes(&[1.0, 2.0]);
        assert_eq!(bytes.len(), 8);
        assert_eq!(&bytes[0..4], &1.0f32.to_le_bytes());
        assert_eq!(&bytes[4..8], &2.0f32.to_le_bytes());
    }

    #[test]
    fn validate_name_accepts_safe_and_rejects_unsafe() {
        assert!(validate_name("doc_1-abc", "x").is_ok());
        assert!(validate_name("Index9", "x").is_ok());
        assert!(validate_name("", "x").is_err());
        assert!(validate_name("a:b", "x").is_err());
        assert!(validate_name("a b", "x").is_err());
        assert!(validate_name("a*b", "x").is_err());
    }

    #[test]
    fn client_config_builds_the_connection_info() {
        let plain = ClientConfig::default().connection_info();
        assert!(matches!(
            &plain.addr,
            redis::ConnectionAddr::Tcp(host, 6379) if host == "localhost"
        ));
        assert_eq!(plain.redis.password, None);

        let config = ClientConfig {
            host: "valkey.internal".to_string(),
            port: 6380,
            password: Some("secret".to_string()),
            use_tls: true,
            ..ClientConfig::default()
        };
        let secured = config.connection_info();
        assert!(matches!(
            &secured.addr,
            redis::ConnectionAddr::TcpTls { host, port: 6380, insecure: false, .. }
                if host == "valkey.internal"
        ));
        assert_eq!(secured.redis.password.as_deref(), Some("secret"));
        // The identity names the server and never carries the password.
        assert_eq!(config.state_id(), "rediss://valkey.internal:6380");
        assert_eq!(ClientConfig::default().state_id(), "redis://localhost:6379");
    }

    #[test]
    fn client_config_debug_redacts_the_password() {
        let config = ClientConfig {
            password: Some("secret".to_string()),
            ..ClientConfig::default()
        };
        let printed = format!("{config:?}");
        assert!(!printed.contains("secret"), "{printed}");
        assert!(
            printed.contains(r#"password: Some("<redacted>")"#),
            "{printed}"
        );
        assert!(printed.contains(r#"host: "localhost""#), "{printed}");

        let printed = format!("{:?}", ClientConfig::default());
        assert!(printed.contains("password: None"), "{printed}");
    }

    #[tokio::test]
    async fn tls_connection_is_attempted_not_refused_as_unsupported() {
        let config = ClientConfig {
            host: "127.0.0.1".to_string(),
            port: 1,
            use_tls: true,
            ..ClientConfig::default()
        };
        // False when the `redis` crate is built without a TLS feature.
        assert!(config.connection_info().addr.is_supported());

        // Nothing listens on port 1: the attempt fails at the socket. Without
        // TLS support compiled in, `redis` refuses before it opens one.
        let Err(err) = Valkey::connect_with_config(config).await else {
            panic!("connected to a closed port");
        };
        let message = err.to_string();
        assert!(message.contains("valkey connect"), "{message}");
        assert!(!message.contains("without the tls feature"), "{message}");
    }

    #[test]
    fn state_id_of_a_uri_drops_the_credentials() {
        let id = |uri: &str| state_id_of(redis::Client::open(uri).unwrap().get_connection_info());
        assert_eq!(id("redis://localhost:6379"), "redis://localhost:6379");
        assert_eq!(
            id("redis://user:s3cret@valkey.internal:6380/2"),
            "redis://valkey.internal:6380/2"
        );
        assert_eq!(
            id("redis://:s3cret@valkey.internal"),
            "redis://valkey.internal:6379"
        );
        assert_eq!(
            id("rediss://:pw@valkey.internal:6380"),
            "rediss://valkey.internal:6380"
        );
        // Built by hand: `redis` refuses to parse a Unix-socket URI on Windows.
        let unix = redis::ConnectionInfo {
            addr: redis::ConnectionAddr::Unix("/run/valkey.sock".into()),
            redis: redis::RedisConnectionInfo {
                db: 1,
                password: Some("pw".to_string()),
                ..Default::default()
            },
        };
        assert_eq!(state_id_of(&unix), "redis+unix:///run/valkey.sock/1");
    }

    fn command_words(pipe: &redis::Pipeline) -> Vec<String> {
        pipe.cmd_iter()
            .map(|cmd| match cmd.args_iter().next() {
                Some(redis::Arg::Simple(word)) => String::from_utf8_lossy(word).into_owned(),
                _ => String::new(),
            })
            .collect()
    }

    #[test]
    fn a_batch_of_documents_is_a_bounded_number_of_pipelines() {
        let n = DOCUMENTS_PER_PIPELINE + 1;
        let upserts: Vec<(String, Document)> = (0..n)
            .map(|i| {
                let doc = Document::new(format!("d{i}"), vec![0.5]).with_payload([
                    ("n", PayloadValue::from(i as i64)),
                    ("f", PayloadValue::from(0.25)),
                    ("t", PayloadValue::from("text")),
                ]);
                (make_hash_key("idx", &doc.id), doc)
            })
            .collect();
        let deletes = vec!["idx:x".to_string(), "idx:y".to_string()];

        let pipelines = document_pipelines(&deletes, &upserts);
        assert_eq!(pipelines.len(), 3, "one DEL pipeline + two upsert chunks");
        assert_eq!(command_words(&pipelines[0]), vec!["DEL"]);
        assert_eq!(
            pipelines[0].cmd_iter().next().unwrap().args_iter().count(),
            3
        );

        let words = command_words(&pipelines[1]);
        assert_eq!(words.len(), 4 * DOCUMENTS_PER_PIPELINE);
        assert_eq!(&words[..4], &["MULTI", "DEL", "HSET", "EXEC"]);
        assert_eq!(
            command_words(&pipelines[2]),
            vec!["MULTI", "DEL", "HSET", "EXEC"]
        );

        // Numeric payload values are stored as their text form.
        let hset = pipelines[2].cmd_iter().nth(2).unwrap();
        let args: Vec<String> = hset
            .args_iter()
            .map(|a| match a {
                redis::Arg::Simple(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                redis::Arg::Cursor => String::new(),
            })
            .collect();
        assert_eq!(&args[..2], &["HSET", &format!("idx:d{}", n - 1)]);
        assert_eq!(args[2], VECTOR_FIELD_NAME);
        assert_eq!(
            &args[4..],
            &["f", "0.25", "n", (n - 1).to_string().as_str(), "t", "text"]
        );

        assert!(document_pipelines(&[], &[]).is_empty());
    }

    #[test]
    fn payload_values_fingerprint_by_type() {
        let doc = |v: PayloadValue| Document::new("d", vec![1.0]).with_payload([("k", v)]);
        let fp = |d: &Document| Fingerprint::from(&(&d.vector, &d.payload)).unwrap();
        assert_ne!(
            fp(&doc(PayloadValue::from(1i64))),
            fp(&doc(PayloadValue::from("1")))
        );
        assert_eq!(
            fp(&doc(PayloadValue::from(1i64))),
            fp(&doc(PayloadValue::from(1i64)))
        );
        assert_eq!(PayloadValue::from(1.0).to_string(), "1.0");
        assert_eq!(PayloadValue::from(-3i64).to_string(), "-3");
    }

    #[test]
    fn prefix_and_hash_key_construction() {
        assert_eq!(make_prefix("idx"), "idx:");
        assert_eq!(make_hash_key("idx", "d1"), "idx:d1");
    }

    fn schema(
        distance: Distance,
        algorithm: VectorAlgorithm,
        fields: Vec<FieldDef>,
    ) -> IndexSchema {
        IndexSchema {
            vectors: ResolvedVectorDef {
                schema: VectorSchema::f32(4),
                distance,
                algorithm,
            },
            fields,
        }
    }

    #[test]
    fn ft_create_args_hnsw_cosine_with_fields() {
        let s = schema(
            Distance::Cosine,
            VectorAlgorithm::Hnsw,
            vec![
                FieldDef::new("text", FieldType::Text),
                FieldDef::new("price", FieldType::Numeric).sortable(),
            ],
        );
        let args = ft_create_args("docs", &s);
        assert_eq!(
            args,
            vec![
                "docs",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "docs:",
                "SCHEMA",
                "vector",
                "VECTOR",
                "HNSW",
                "6",
                "TYPE",
                "FLOAT32",
                "DIM",
                "4",
                "DISTANCE_METRIC",
                "COSINE",
                "text",
                "TEXT",
                "price",
                "NUMERIC",
                "SORTABLE",
            ]
        );
    }

    #[test]
    fn ft_create_args_flat_l2_no_fields() {
        let s = schema(Distance::L2, VectorAlgorithm::Flat, vec![]);
        let args = ft_create_args("v", &s);
        assert_eq!(
            args,
            vec![
                "v",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "v:",
                "SCHEMA",
                "vector",
                "VECTOR",
                "FLAT",
                "6",
                "TYPE",
                "FLOAT32",
                "DIM",
                "4",
                "DISTANCE_METRIC",
                "L2",
            ]
        );
    }
}
