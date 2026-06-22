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
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::ctx::{ContextKey, ContextStore, Ctx};
use crate::error::{Error, Result};
use crate::resources::schema::{VectorSchema, VectorSchemaProvider};
use crate::statediff::{
    DiffAction, ManagedBy, ManagedTargetOptions, MutualTrackingRecord, diff,
    resolve_system_transition,
};
use crate::target_state::{
    ChildTargetDef, StableKey, TargetAction, TargetActionSink, TargetChildInvalidation,
    TargetHandler, TargetReconcileOutput, TargetState, TargetStateProvider, declare_target_state,
    declare_target_state_with_child, mount_target, register_root_target_states_provider,
};

const VECTOR_FIELD_NAME: &str = "vector";

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// A Valkey connection. Clone-cheap (the multiplexed connection is shared).
#[derive(Clone)]
pub struct Valkey {
    conn: Arc<Mutex<redis::aio::MultiplexedConnection>>,
    state_id: Arc<str>,
}

impl Valkey {
    /// Connect to a Valkey/Redis server (e.g. `redis://localhost:6379`).
    pub async fn connect(uri: &str) -> Result<Self> {
        let client = redis::Client::open(uri)
            .map_err(|e| Error::engine(format!("valkey config: {e}")))?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::engine(format!("valkey connect: {e}")))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            state_id: Arc::from(uri),
        })
    }

    /// Stable identity (the URI) for use as a `ContextKey` state id / memo dep.
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
            return Err(Error::engine("valkey vector size must be greater than zero"));
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
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
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
    let mut conn = db.conn.lock().await;
    let names: Vec<String> = redis::cmd("FT._LIST")
        .query_async(&mut *conn)
        .await
        .map_err(|e| Error::engine(format!("valkey FT._LIST: {e}")))?;
    Ok(names.iter().any(|n| n == index_name))
}

// ---------------------------------------------------------------------------
// Public target API
// ---------------------------------------------------------------------------

/// A document to store in a Valkey index.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Document {
    pub id: String,
    pub vector: Vec<f32>,
    pub payload: Option<BTreeMap<String, String>>,
}

impl Document {
    pub fn new(id: impl Into<String>, vector: Vec<f32>) -> Self {
        Self {
            id: id.into(),
            vector,
            payload: None,
        }
    }

    pub fn with_payload(mut self, payload: BTreeMap<String, String>) -> Self {
        self.payload = Some(payload);
        self
    }
}

/// A declarative Valkey index target — a handle to declare documents on.
#[derive(Clone)]
pub struct IndexTarget {
    index_name: Arc<str>,
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
    let provider = register_root_target_states_provider(
        ctx,
        format!("cocoindex/valkey/index/{}/{}", db.name(), index_name),
        IndexHandler::new(db.name().to_string()),
    )?;
    Ok(provider.target_state(
        "default",
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
    let ts = index_target(ctx, db, index_name, schema)?;
    let name = ts.value().index_name.clone();
    let documents = declare_target_state_with_child::<IndexSpec, Document>(ctx, ts)?;
    Ok(IndexTarget {
        index_name: Arc::from(name),
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
    let ts = index_target(ctx, db, index_name, schema)?;
    let name = ts.value().index_name.clone();
    let documents = mount_target::<IndexSpec, Document>(ctx, ts).await?;
    Ok(IndexTarget {
        index_name: Arc::from(name),
        documents,
    })
}

impl IndexTarget {
    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    /// Declare a document to upsert into the index.
    pub fn declare_document(&self, ctx: &Ctx, document: Document) -> Result<()> {
        validate_name(&document.id, "doc_id")?;
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
    index_name: String,
    spec: Option<IndexSpec>,
    /// `Replace` purges documents + recreates; `Delete` only drops the index.
    main_action: Option<DiffAction>,
}

struct IndexHandler {
    sink: TargetActionSink<IndexAction>,
}

impl IndexHandler {
    fn new(db_key: String) -> Self {
        Self {
            sink: index_sink(db_key),
        }
    }
}

impl TargetHandler<IndexSpec> for IndexHandler {
    type TrackingRecord = IndexTrackingRecord;
    type Action = IndexAction;

    fn reconcile(
        &self,
        _key: StableKey,
        desired: Option<IndexSpec>,
        prev: Vec<IndexTrackingRecord>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<IndexAction, IndexTrackingRecord>>> {
        match desired {
            // Always emit when declared so the sink fulfills the document child.
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

fn index_sink(db_key: String) -> TargetActionSink<IndexAction> {
    TargetActionSink::from_async_fn_with_children_ctx(
        move |host_ctx, actions: Vec<TargetAction<IndexAction>>| {
            let db_key = db_key.clone();
            async move {
                let db = resolve_db(&host_ctx, &db_key)?;
                let mut out: Vec<Option<ChildTargetDef>> = Vec::with_capacity(actions.len());
                for action in actions {
                    match action {
                        TargetAction::Create(a) | TargetAction::Update(a) => {
                            let spec = a.spec.ok_or_else(|| {
                                Error::engine("valkey create/update action missing spec")
                            })?;
                            if matches!(a.main_action, Some(DiffAction::Replace | DiffAction::Delete))
                            {
                                drop_index(&db, &a.index_name).await?;
                                if matches!(a.main_action, Some(DiffAction::Replace)) {
                                    delete_prefix_keys(&db, &a.index_name).await?;
                                }
                            }
                            if spec.managed_by.is_system()
                                && matches!(
                                    a.main_action,
                                    Some(
                                        DiffAction::Insert
                                            | DiffAction::Upsert
                                            | DiffAction::Replace
                                    )
                                )
                            {
                                create_index(
                                    &db,
                                    &a.index_name,
                                    &spec.schema,
                                    matches!(a.main_action, Some(DiffAction::Upsert)),
                                )
                                .await?;
                            }
                            out.push(Some(ChildTargetDef::new::<Document, _>(
                                DocumentHandler::new(db_key.clone(), a.index_name),
                            )));
                        }
                        TargetAction::Delete(a) => {
                            drop_index(&db, &a.index_name).await?;
                            delete_prefix_keys(&db, &a.index_name).await?;
                            out.push(None);
                        }
                    }
                }
                Ok(out)
            }
        },
    )
}

async fn create_index(
    db: &Valkey,
    index_name: &str,
    schema: &IndexSchema,
    if_not_exists: bool,
) -> Result<()> {
    if if_not_exists && index_exists(db, index_name).await? {
        return Ok(());
    }
    let args = ft_create_args(index_name, schema);
    let mut conn = db.conn.lock().await;
    let mut cmd = redis::cmd("FT.CREATE");
    for arg in &args {
        cmd.arg(arg);
    }
    let _: redis::Value = cmd
        .query_async(&mut *conn)
        .await
        .map_err(|e| Error::engine(format!("valkey FT.CREATE {index_name}: {e}")))?;
    Ok(())
}

/// Drop the index if present. A not-found error (already removed externally) is
/// ignored.
async fn drop_index(db: &Valkey, index_name: &str) -> Result<()> {
    let mut conn = db.conn.lock().await;
    let _: redis::RedisResult<redis::Value> = redis::cmd("FT.DROPINDEX")
        .arg(index_name)
        .query_async(&mut *conn)
        .await;
    Ok(())
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
        let mut conn = db.conn.lock().await;
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(500)
            .query_async(&mut *conn)
            .await
            .map_err(|e| Error::engine(format!("valkey SCAN {pattern}: {e}")))?;
        if !keys.is_empty() {
            let mut del = redis::cmd("DEL");
            for k in &keys {
                del.arg(k);
            }
            let _: redis::Value = del
                .query_async(&mut *conn)
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
        other => Err(Error::engine(format!("unsupported valkey doc key: {other:?}"))),
    }
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
                        TargetAction::Create(a) | TargetAction::Update(a) | TargetAction::Delete(a) => a,
                    };
                    match a.document {
                        Some(doc) => upserts.push((a.hash_key, doc)),
                        None => deletes.push(a.hash_key),
                    }
                }
                let mut conn = db.conn.lock().await;
                if !deletes.is_empty() {
                    let mut del = redis::cmd("DEL");
                    for k in &deletes {
                        del.arg(k);
                    }
                    let _: redis::Value = del
                        .query_async(&mut *conn)
                        .await
                        .map_err(|e| Error::engine(format!("valkey DEL docs: {e}")))?;
                }
                for (hash_key, doc) in upserts {
                    // Atomic DEL + HSET so no stale payload field survives.
                    let mut pipe = redis::pipe();
                    pipe.atomic();
                    pipe.cmd("DEL").arg(&hash_key);
                    let mut hset = redis::cmd("HSET");
                    hset.arg(&hash_key);
                    hset.arg(VECTOR_FIELD_NAME).arg(vector_to_bytes(&doc.vector));
                    if let Some(payload) = &doc.payload {
                        for (k, v) in payload {
                            hset.arg(k).arg(v);
                        }
                    }
                    pipe.add_command(hset);
                    let _: redis::Value = pipe
                        .query_async(&mut *conn)
                        .await
                        .map_err(|e| Error::engine(format!("valkey upsert {hash_key}: {e}")))?;
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
    fn prefix_and_hash_key_construction() {
        assert_eq!(make_prefix("idx"), "idx:");
        assert_eq!(make_hash_key("idx", "d1"), "idx:d1");
    }

    fn schema(distance: Distance, algorithm: VectorAlgorithm, fields: Vec<FieldDef>) -> IndexSchema {
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
                "docs", "ON", "HASH", "PREFIX", "1", "docs:", "SCHEMA", "vector", "VECTOR", "HNSW",
                "6", "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "COSINE", "text", "TEXT",
                "price", "NUMERIC", "SORTABLE",
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
                "v", "ON", "HASH", "PREFIX", "1", "v:", "SCHEMA", "vector", "VECTOR", "FLAT", "6",
                "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
            ]
        );
    }
}
