//! Zvec collection target connector.
//!
//! [Zvec](https://zvec.org) is an embedded, in-process vector database. This
//! module mirrors Python's `cocoindex.connectors.zvec` with a two-level target:
//!
//! 1. Collection level: creates and destroys collections on disk.
//! 2. Document level: upserts and deletes documents within a collection.
//!
//! Zvec is path-based: a [`ManagedConnection`] owns a base directory and each
//! collection lives in a subdirectory under it. Each document has a single
//! string id (the primary key), a set of dense vector fields, and a set of
//! scalar fields used for filtering or full-text search.
//!
//! Use [`collection_target`] to build a composable target state, or
//! [`declare_collection_target`] / [`mount_collection_target`] to get a
//! [`CollectionTarget`] for declaring rows. Querying stays outside the
//! connector: open the collection with [`ManagedConnection::open_existing`] and
//! use the re-exported [`zvec_rust`] API.
//!
//! Built on the `zvec-rust` bindings to the zvec C API. The bindings have no
//! document setter for sparse vectors or float16 vectors, so this connector
//! writes float32 dense vectors only.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use base64::Engine;
use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use zvec_rust::{Collection, CollectionOptions, DataType, Doc, FieldSchema, IndexParams};

/// Re-export of the upstream [`zvec_rust`] crate. A collection handle holds an
/// exclusive write lock, so queries go through the handle this connector opened
/// ([`ManagedConnection::open_existing`]) and this crate's query types.
pub use zvec_rust;

use crate::ctx::{ContextKey, ContextStore, Ctx};
use crate::error::{Error, Result};
use crate::row_schema::{LogicalType, SchemaField, SchemaFields};
use crate::statediff::{
    DiffAction, ManagedBy, ManagedTargetOptions, MutualTrackingRecord, diff,
    resolve_system_transition,
};
use crate::target_state::{
    ChildTargetDef, STATIC_ROOT_PROVIDERS, SinkCache, StableKey, StaticRootProvider, TargetAction,
    TargetActionSink, TargetChildInvalidation, TargetHandler, TargetReconcileOutput, TargetState,
    TargetStateProvider, declare_target_state, declare_target_state_with_child, mount_target,
};

/// Zvec rejects collection names shorter than this.
const MIN_COLLECTION_NAME_LEN: usize = 3;

/// HNSW graph degree and build-time candidate list size. `zvec-rust` requires
/// both; these are the defaults of zvec's own `HnswIndexParam`, which the
/// Python connector relies on.
const HNSW_M: i32 = 50;
const HNSW_EF_CONSTRUCTION: i32 = 500;

fn zvec_err(context: impl std::fmt::Display, e: zvec_rust::Error) -> Error {
    Error::engine(format!("zvec {context}: {e}"))
}

fn validate_identifier(name: &str, kind: &str) -> Result<()> {
    let mut chars = name.chars();
    let valid = chars
        .next()
        .is_some_and(|c| c == '_' || c.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c.is_ascii_alphanumeric());
    if valid {
        Ok(())
    } else {
        Err(Error::engine(format!(
            "invalid zvec {kind} {name:?}: use ASCII letters, digits and '_', not starting with a digit"
        )))
    }
}

fn validate_collection_name(name: &str) -> Result<()> {
    validate_identifier(name, "collection name")?;
    if name.len() < MIN_COLLECTION_NAME_LEN {
        return Err(Error::engine(format!(
            "invalid zvec collection name {name:?}: zvec requires at least \
             {MIN_COLLECTION_NAME_LEN} characters"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// Options for [`connect`].
#[derive(Clone, Copy, Debug)]
pub struct ConnectOptions {
    /// Whether zvec uses memory-mapped I/O for data files. Defaults to `true`.
    pub enable_mmap: bool,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self { enable_mmap: true }
    }
}

struct ConnectionInner {
    base_path: PathBuf,
    enable_mmap: bool,
    open: Mutex<HashMap<String, Arc<Collection>>>,
}

/// A handle to a base directory holding zvec collections. Clone-cheap.
///
/// Zvec opens each collection as a live handle and takes an exclusive write
/// lock on it. Opening the same collection twice (even within one process)
/// fails, so the connection caches open handles by collection name and reuses
/// them.
#[derive(Clone)]
pub struct ManagedConnection {
    inner: Arc<ConnectionInner>,
}

/// Create a [`ManagedConnection`] rooted at `base_path`. The directory is
/// created if missing.
pub fn connect(base_path: impl AsRef<Path>, options: ConnectOptions) -> Result<ManagedConnection> {
    static INITIALIZED: OnceLock<std::result::Result<(), String>> = OnceLock::new();
    INITIALIZED
        .get_or_init(|| {
            if zvec_rust::is_initialized() {
                return Ok(());
            }
            zvec_rust::initialize(None).map_err(|e| e.to_string())
        })
        .as_ref()
        .map_err(|e| Error::engine(format!("zvec initialize: {e}")))?;

    let base_path = base_path.as_ref().to_path_buf();
    std::fs::create_dir_all(&base_path).map_err(|e| {
        Error::engine(format!(
            "create zvec base directory {}: {e}",
            base_path.display()
        ))
    })?;
    Ok(ManagedConnection {
        inner: Arc::new(ConnectionInner {
            base_path,
            enable_mmap: options.enable_mmap,
            open: Mutex::new(HashMap::new()),
        }),
    })
}

impl ManagedConnection {
    /// The directory under which collections are stored.
    pub fn base_path(&self) -> &Path {
        &self.inner.base_path
    }

    /// The directory of the collection `name`.
    pub fn collection_path(&self, name: &str) -> PathBuf {
        self.inner.base_path.join(name)
    }

    /// Open an existing collection, reusing the cached handle when there is
    /// one. Use the returned handle to query. Drop it before a run that
    /// destroys the collection (a schema change, or the target no longer being
    /// declared): a collection with a live handle cannot be destroyed.
    pub async fn open_existing(&self, name: &str) -> Result<Arc<Collection>> {
        let conn = self.clone();
        let name = name.to_string();
        run_blocking(move || conn.open_existing_blocking(&name)).await
    }

    /// Release all cached collection handles (dropping their write locks).
    pub async fn close(&self) -> Result<()> {
        let conn = self.clone();
        run_blocking(move || {
            // Closing a handle flushes it, so the handles are dropped here on
            // the blocking thread and outside the lock.
            let handles = std::mem::take(&mut *conn.lock_open());
            drop(handles);
            Ok(())
        })
        .await
    }

    fn lock_open(&self) -> MutexGuard<'_, HashMap<String, Arc<Collection>>> {
        // The map stays consistent across a panic: every update is one insert
        // or remove.
        self.inner.open.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn collection_options(&self) -> Result<CollectionOptions> {
        let mut options =
            CollectionOptions::new().map_err(|e| zvec_err("create collection options", e))?;
        options
            .set_enable_mmap(self.inner.enable_mmap)
            .map_err(|e| zvec_err("set enable_mmap", e))?;
        Ok(options)
    }

    fn open_path(&self, name: &str) -> Result<Collection> {
        let path = self.collection_path(name);
        Collection::open(path_str(&path)?, Some(&self.collection_options()?))
            .map_err(|e| zvec_err(format_args!("open collection {name:?}"), e))
    }

    fn open_existing_blocking(&self, name: &str) -> Result<Arc<Collection>> {
        let mut open = self.lock_open();
        if let Some(collection) = open.get(name) {
            return Ok(collection.clone());
        }
        let collection = Arc::new(self.open_path(name)?);
        open.insert(name.to_string(), collection.clone());
        Ok(collection)
    }

    /// Open the collection, creating it from `schema` if it does not exist.
    fn open_or_create_blocking(&self, name: &str, schema: &CollectionSchema) -> Result<()> {
        let mut open = self.lock_open();
        if open.contains_key(name) {
            return Ok(());
        }
        let path = self.collection_path(name);
        let collection = if path.exists() {
            self.open_path(name)?
        } else {
            Collection::create_and_open(
                path_str(&path)?,
                &schema.zvec_schema(name)?,
                Some(&self.collection_options()?),
            )
            .map_err(|e| zvec_err(format_args!("create collection {name:?}"), e))?
        };
        open.insert(name.to_string(), Arc::new(collection));
        Ok(())
    }

    /// Permanently delete a collection from disk.
    fn destroy_blocking(&self, name: &str) -> Result<()> {
        let mut open = self.lock_open();
        if let Some(collection) = open.remove(name) {
            match Arc::try_unwrap(collection) {
                Ok(collection) => collection
                    .try_close()
                    .map_err(|e| zvec_err(format_args!("close collection {name:?}"), e))?,
                Err(collection) => {
                    open.insert(name.to_string(), collection);
                    return Err(Error::engine(format!(
                        "cannot destroy zvec collection {name:?}: a handle from \
                         ManagedConnection::open_existing is still alive; drop it first"
                    )));
                }
            }
        }
        let path = self.collection_path(name);
        match std::fs::remove_dir_all(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::engine(format!(
                "remove zvec collection directory {}: {e}",
                path.display()
            ))),
        }
    }
}

fn path_str(path: &Path) -> Result<&str> {
    path.to_str().ok_or_else(|| {
        Error::engine(format!(
            "zvec collection path {} is not valid UTF-8",
            path.display()
        ))
    })
}

/// Run a zvec call (synchronous FFI that does disk I/O) off the async runtime.
async fn run_blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::engine(format!("zvec blocking task failed: {e}")))?
}

// ---------------------------------------------------------------------------
// Schema and type mapping
// ---------------------------------------------------------------------------

/// The zvec data type of a scalar field.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ScalarType {
    Bool,
    Int32,
    Int64,
    Uint32,
    Uint64,
    Float,
    Double,
    String,
}

impl ScalarType {
    fn data_type(self) -> DataType {
        match self {
            ScalarType::Bool => DataType::Bool,
            ScalarType::Int32 => DataType::Int32,
            ScalarType::Int64 => DataType::Int64,
            ScalarType::Uint32 => DataType::Uint32,
            ScalarType::Uint64 => DataType::Uint64,
            ScalarType::Float => DataType::Float,
            ScalarType::Double => DataType::Double,
            ScalarType::String => DataType::String,
        }
    }

    /// Parse the name given in `#[coco(type = "…")]` (case-insensitive).
    fn from_name(name: &str) -> Option<Self> {
        Some(match name.to_ascii_lowercase().as_str() {
            "bool" => ScalarType::Bool,
            "int32" => ScalarType::Int32,
            "int64" => ScalarType::Int64,
            "uint32" => ScalarType::Uint32,
            "uint64" => ScalarType::Uint64,
            "float" => ScalarType::Float,
            "double" => ScalarType::Double,
            "string" => ScalarType::String,
            _ => return None,
        })
    }
}

/// Overrides the scalar field type of a column. Apply with
/// [`CollectionSchema::with_column_type`].
///
/// ```ignore
/// // Store as INT32 instead of the derived type, without a filter index.
/// let schema = CollectionSchema::from_row::<Row>(["id"])?
///     .with_column_type("count", ZvecType { data_type: ScalarType::Int32, indexed: false })?;
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ZvecType {
    pub data_type: ScalarType,
    /// Whether the field gets an inverted index for filtering.
    pub indexed: bool,
}

/// The distance metric of a dense vector field.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum VectorMetric {
    #[default]
    Cosine,
    Ip,
    L2,
}

impl VectorMetric {
    fn metric_type(self) -> zvec_rust::MetricType {
        match self {
            VectorMetric::Cosine => zvec_rust::MetricType::Cosine,
            VectorMetric::Ip => zvec_rust::MetricType::Ip,
            VectorMetric::L2 => zvec_rust::MetricType::L2,
        }
    }
}

/// The quantization of a dense vector index.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum VectorQuantize {
    Fp16,
    Int8,
    Int4,
}

impl VectorQuantize {
    fn quantize_type(self) -> zvec_rust::QuantizeType {
        match self {
            VectorQuantize::Fp16 => zvec_rust::QuantizeType::Fp16,
            VectorQuantize::Int8 => zvec_rust::QuantizeType::Int8,
            VectorQuantize::Int4 => zvec_rust::QuantizeType::Int4,
        }
    }
}

/// Tunes the index of a dense vector field. Apply with
/// [`CollectionSchema::with_vector_def`]. The default is a cosine HNSW index
/// without quantization.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ZvecVectorDef {
    pub metric: VectorMetric,
    /// `None` stores the index unquantized.
    pub quantize: Option<VectorQuantize>,
}

/// Marks a string column as full-text (FTS) indexed. Apply with
/// [`CollectionSchema::with_fts`].
///
/// The field is stored as a `STRING` field with a zvec FTS index, so it can be
/// queried with full-text match expressions directly through zvec.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZvecFtsType {
    pub tokenizer_name: String,
    pub filters: Vec<String>,
    pub extra_params: String,
}

impl Default for ZvecFtsType {
    fn default() -> Self {
        Self {
            tokenizer_name: "standard".to_string(),
            filters: vec!["lowercase".to_string()],
            extra_params: String::new(),
        }
    }
}

/// How a row's serialized value becomes the stored scalar value.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum ValueEncoding {
    /// The value already has the field's type.
    Native,
    /// Bytes, stored as a base64 string.
    Base64,
    /// A decimal, stored as its string form.
    DecimalString,
    /// A duration, stored as seconds.
    DurationSeconds,
    /// A complex value, stored as a JSON string.
    Json,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum ColumnKind {
    Scalar {
        data_type: ScalarType,
        encoding: ValueEncoding,
        indexed: bool,
    },
    Dense {
        /// Zero until resolved with [`CollectionSchema::with_vector_dim`].
        dimension: u32,
        metric: VectorMetric,
        quantize: Option<VectorQuantize>,
    },
    Fts {
        tokenizer_name: String,
        filters: Vec<String>,
        extra_params: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Column {
    name: String,
    nullable: bool,
    kind: ColumnKind,
}

fn column_from_field(field: SchemaField) -> Result<Column> {
    use LogicalType as L;
    let scalar = |data_type, encoding| ColumnKind::Scalar {
        data_type,
        encoding,
        indexed: true,
    };
    let kind = match &field.logical_type {
        L::Bool => scalar(ScalarType::Bool, ValueEncoding::Native),
        L::Int16 | L::Int32 => scalar(ScalarType::Int32, ValueEncoding::Native),
        L::Int64 => scalar(ScalarType::Int64, ValueEncoding::Native),
        L::Float32 => scalar(ScalarType::Float, ValueEncoding::Native),
        L::Float64 => scalar(ScalarType::Double, ValueEncoding::Native),
        L::Text | L::Uuid | L::Date | L::Time | L::DateTime => {
            scalar(ScalarType::String, ValueEncoding::Native)
        }
        L::Decimal => scalar(ScalarType::String, ValueEncoding::DecimalString),
        L::Bytes => scalar(ScalarType::String, ValueEncoding::Base64),
        L::Duration => scalar(ScalarType::Double, ValueEncoding::DurationSeconds),
        L::Json => scalar(ScalarType::String, ValueEncoding::Json),
        L::Vector { half: true, .. } => {
            return Err(Error::engine(format!(
                "zvec vector field {:?} is float16, which the zvec Rust bindings cannot write; \
                 use a float32 vector with ZvecVectorDef {{ quantize: Some(VectorQuantize::Fp16) }}",
                field.name
            )));
        }
        L::Vector { dim, half: false } => ColumnKind::Dense {
            dimension: *dim,
            metric: VectorMetric::default(),
            quantize: None,
        },
        L::Custom(name) => {
            let data_type = ScalarType::from_name(name).ok_or_else(|| {
                Error::engine(format!(
                    "zvec field {:?} has unsupported type {name:?}; expected one of bool, int32, \
                     int64, uint32, uint64, float, double, string",
                    field.name
                ))
            })?;
            scalar(data_type, ValueEncoding::Native)
        }
    };
    Ok(Column {
        name: field.name,
        nullable: field.nullable,
        kind,
    })
}

/// Schema definition for a zvec collection.
///
/// Built from a row type with [`from_row`](Self::from_row). The single
/// primary-key column becomes the document id; the remaining columns become
/// scalar fields or vector fields.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CollectionSchema {
    columns: Vec<Column>,
    primary_key: String,
}

impl CollectionSchema {
    /// Derive a schema from a `#[derive(SchemaFields)]` row type.
    ///
    /// `primary_key` names exactly one column. Its value becomes the document
    /// id, so the column must hold a string or an integer.
    pub fn from_row<T: SchemaFields>(
        primary_key: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let primary_key: Vec<String> = primary_key.into_iter().map(Into::into).collect();
        let [primary_key] = <[String; 1]>::try_from(primary_key).map_err(|pk| {
            Error::engine(format!(
                "zvec collections require exactly one primary key column (mapped to the \
                 document id), got {pk:?}"
            ))
        })?;

        let fields = T::schema_fields();
        let Some(pk_field) = fields.iter().find(|f| f.name == primary_key) else {
            return Err(Error::engine(format!(
                "primary key column {primary_key:?} not found in columns: {:?}",
                fields.iter().map(|f| &f.name).collect::<Vec<_>>()
            )));
        };
        // A document id is the key value as a string, which must be unambiguous.
        if !matches!(
            pk_field.logical_type,
            LogicalType::Int16
                | LogicalType::Int32
                | LogicalType::Int64
                | LogicalType::Text
                | LogicalType::Uuid
        ) {
            return Err(Error::engine(format!(
                "zvec primary key column {primary_key:?} must be a string, UUID or integer \
                 column, got {:?}; convert the value yourself if needed",
                pk_field.logical_type
            )));
        }
        if pk_field.nullable {
            return Err(Error::engine(format!(
                "zvec primary key column {primary_key:?} must not be nullable"
            )));
        }

        let columns = fields
            .into_iter()
            .map(column_from_field)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            columns,
            primary_key,
        })
    }

    /// The column whose value becomes the document id.
    pub fn primary_key(&self) -> &str {
        &self.primary_key
    }

    fn column_mut(&mut self, name: &str, what: &str) -> Result<&mut Column> {
        self.columns
            .iter_mut()
            .find(|c| c.name == name)
            .ok_or_else(|| Error::engine(format!("zvec {what} names unknown column {name:?}")))
    }

    /// Resolve or override the dimension of a dense vector column.
    pub fn with_vector_dim(mut self, column: &str, dim: usize) -> Result<Self> {
        let dim = u32::try_from(dim).ok().filter(|d| *d > 0).ok_or_else(|| {
            Error::engine(format!(
                "zvec vector column {column:?} requires a dimension in 1..=u32::MAX, got {dim}"
            ))
        })?;
        match &mut self.column_mut(column, "vector dimension")?.kind {
            ColumnKind::Dense { dimension, .. } => *dimension = dim,
            _ => {
                return Err(Error::engine(format!(
                    "zvec column {column:?} is not a vector column"
                )));
            }
        }
        Ok(self)
    }

    /// Set the index metric and quantization of a dense vector column.
    pub fn with_vector_def(mut self, column: &str, def: ZvecVectorDef) -> Result<Self> {
        match &mut self.column_mut(column, "vector definition")?.kind {
            ColumnKind::Dense {
                metric, quantize, ..
            } => {
                *metric = def.metric;
                *quantize = def.quantize;
            }
            _ => {
                return Err(Error::engine(format!(
                    "zvec column {column:?} is not a vector column"
                )));
            }
        }
        Ok(self)
    }

    /// Override the scalar field type of a column.
    pub fn with_column_type(mut self, column: &str, zvec_type: ZvecType) -> Result<Self> {
        match &mut self.column_mut(column, "type override")?.kind {
            ColumnKind::Scalar {
                data_type,
                encoding,
                indexed,
            } => {
                *data_type = zvec_type.data_type;
                *encoding = ValueEncoding::Native;
                *indexed = zvec_type.indexed;
            }
            ColumnKind::Dense { .. } => {
                return Err(Error::engine(format!(
                    "zvec column {column:?} is a vector column; ZvecType applies to scalar columns"
                )));
            }
            ColumnKind::Fts { .. } => {
                return Err(Error::engine(format!(
                    "zvec column {column:?} cannot combine ZvecFtsType with ZvecType"
                )));
            }
        }
        Ok(self)
    }

    /// Mark a string column as full-text indexed.
    pub fn with_fts(mut self, column: &str, fts: ZvecFtsType) -> Result<Self> {
        if column == self.primary_key {
            return Err(Error::engine(format!(
                "zvec primary key column {column:?} must be a scalar field, not a full-text field"
            )));
        }
        let col = self.column_mut(column, "full-text definition")?;
        match &col.kind {
            ColumnKind::Scalar {
                data_type: ScalarType::String,
                encoding: ValueEncoding::Native,
                ..
            } => {
                col.kind = ColumnKind::Fts {
                    tokenizer_name: fts.tokenizer_name,
                    filters: fts.filters,
                    extra_params: fts.extra_params,
                };
                Ok(self)
            }
            _ => Err(Error::engine(format!(
                "ZvecFtsType on zvec column {column:?} requires a string column"
            ))),
        }
    }

    /// The columns stored as fields: every column but the primary key, which
    /// maps to the document id.
    fn field_columns(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter().filter(|c| c.name != self.primary_key)
    }

    fn validate(&self) -> Result<()> {
        let mut has_vector = false;
        for col in self.field_columns() {
            validate_identifier(&col.name, "field name")?;
            if let ColumnKind::Dense { dimension, .. } = col.kind {
                has_vector = true;
                if dimension == 0 {
                    return Err(Error::engine(format!(
                        "zvec vector column {:?} has an unresolved dimension; call \
                         with_vector_dim({:?}, dimension) before declaring the target",
                        col.name, col.name
                    )));
                }
            }
        }
        if !has_vector {
            return Err(Error::engine(
                "zvec collections require at least one vector field",
            ));
        }
        Ok(())
    }

    fn zvec_schema(&self, collection_name: &str) -> Result<zvec_rust::CollectionSchema> {
        let err = |col: &Column, e| zvec_err(format_args!("field {:?} schema", col.name), e);
        let mut schema = zvec_rust::CollectionSchema::new(collection_name)
            .map_err(|e| zvec_err(format_args!("schema of collection {collection_name:?}"), e))?;
        for col in self.field_columns() {
            let (data_type, dimension, index) = match &col.kind {
                ColumnKind::Scalar {
                    data_type, indexed, ..
                } => {
                    let index = indexed
                        .then(|| IndexParams::invert(false, false))
                        .transpose()
                        .map_err(|e| err(col, e))?;
                    (data_type.data_type(), 0, index)
                }
                ColumnKind::Fts {
                    tokenizer_name,
                    filters,
                    extra_params,
                } => {
                    let filters: Vec<&str> = filters.iter().map(String::as_str).collect();
                    let index =
                        IndexParams::fts(Some(tokenizer_name), Some(&filters), Some(extra_params))
                            .map_err(|e| err(col, e))?;
                    (DataType::String, 0, Some(index))
                }
                ColumnKind::Dense {
                    dimension,
                    metric,
                    quantize,
                } => {
                    let index = match quantize {
                        Some(quantize) => IndexParams::hnsw_with_quantize(
                            metric.metric_type(),
                            HNSW_M,
                            HNSW_EF_CONSTRUCTION,
                            quantize.quantize_type(),
                        ),
                        None => {
                            IndexParams::hnsw(metric.metric_type(), HNSW_M, HNSW_EF_CONSTRUCTION)
                        }
                    }
                    .map_err(|e| err(col, e))?;
                    (DataType::VectorFp32, *dimension, Some(index))
                }
            };
            let mut field = FieldSchema::new(&col.name, data_type, col.nullable, dimension)
                .map_err(|e| err(col, e))?;
            if let Some(index) = &index {
                field.set_index_params(index).map_err(|e| err(col, e))?;
            }
            schema.add_field(&field).map_err(|e| err(col, e))?;
        }
        Ok(schema)
    }
}

// ---------------------------------------------------------------------------
// Document (row) level
// ---------------------------------------------------------------------------

/// A field value in the form zvec stores it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
enum FieldValue {
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Uint32(u32),
    Uint64(u64),
    Float(f32),
    Double(f64),
    String(String),
    Vector(Vec<f32>),
}

/// A declared document: its id and its non-null fields.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DocState {
    doc_id: String,
    fields: Vec<(String, FieldValue)>,
}

fn value_mismatch(col: &Column, expected: &str, value: &JsonValue) -> Error {
    Error::engine(format!(
        "zvec column {:?} expects {expected}, got {value}",
        col.name
    ))
}

fn scalar_value(
    col: &Column,
    data_type: ScalarType,
    encoding: ValueEncoding,
    value: &JsonValue,
) -> Result<FieldValue> {
    match encoding {
        ValueEncoding::Native => {}
        ValueEncoding::Base64 => {
            let bytes = value
                .as_array()
                .and_then(|items| {
                    items
                        .iter()
                        .map(|b| b.as_u64().and_then(|b| u8::try_from(b).ok()))
                        .collect::<Option<Vec<u8>>>()
                })
                .ok_or_else(|| value_mismatch(col, "bytes", value))?;
            return Ok(FieldValue::String(
                base64::engine::general_purpose::STANDARD.encode(bytes),
            ));
        }
        ValueEncoding::DecimalString => {
            return match value {
                JsonValue::String(s) => Ok(FieldValue::String(s.clone())),
                JsonValue::Number(n) => Ok(FieldValue::String(n.to_string())),
                _ => Err(value_mismatch(col, "a decimal", value)),
            };
        }
        ValueEncoding::DurationSeconds => {
            // `std::time::Duration` serializes as `{secs, nanos}`.
            let secs = value.get("secs").and_then(JsonValue::as_u64);
            let nanos = value.get("nanos").and_then(JsonValue::as_u64);
            return match (secs, nanos) {
                (Some(secs), Some(nanos)) => {
                    Ok(FieldValue::Double(secs as f64 + nanos as f64 / 1e9))
                }
                _ => Err(value_mismatch(col, "a duration", value)),
            };
        }
        ValueEncoding::Json => {
            return serde_json::to_string(value)
                .map(FieldValue::String)
                .map_err(|e| {
                    Error::engine(format!("encode zvec column {:?} as JSON: {e}", col.name))
                });
        }
    }
    let converted = match data_type {
        ScalarType::Bool => value.as_bool().map(FieldValue::Bool),
        ScalarType::Int32 => value
            .as_i64()
            .and_then(|v| i32::try_from(v).ok())
            .map(FieldValue::Int32),
        ScalarType::Int64 => value.as_i64().map(FieldValue::Int64),
        ScalarType::Uint32 => value
            .as_u64()
            .and_then(|v| u32::try_from(v).ok())
            .map(FieldValue::Uint32),
        ScalarType::Uint64 => value.as_u64().map(FieldValue::Uint64),
        ScalarType::Float => value.as_f64().map(|v| FieldValue::Float(v as f32)),
        ScalarType::Double => value.as_f64().map(FieldValue::Double),
        ScalarType::String => value.as_str().map(|s| FieldValue::String(s.to_string())),
    };
    converted.ok_or_else(|| value_mismatch(col, &format!("{data_type:?}"), value))
}

fn field_value(col: &Column, value: &JsonValue) -> Result<FieldValue> {
    match &col.kind {
        ColumnKind::Scalar {
            data_type,
            encoding,
            ..
        } => scalar_value(col, *data_type, *encoding, value),
        ColumnKind::Fts { .. } => value
            .as_str()
            .map(|s| FieldValue::String(s.to_string()))
            .ok_or_else(|| value_mismatch(col, "a string", value)),
        ColumnKind::Dense { dimension, .. } => {
            // A non-finite element serializes as null, so it fails here.
            let vector = value
                .as_array()
                .and_then(|items| {
                    items
                        .iter()
                        .map(|x| x.as_f64().map(|x| x as f32))
                        .collect::<Option<Vec<f32>>>()
                })
                .ok_or_else(|| value_mismatch(col, "a vector of finite numbers", value))?;
            if vector.len() != *dimension as usize {
                return Err(Error::engine(format!(
                    "zvec vector column {:?} expects dimension {dimension}, got {}",
                    col.name,
                    vector.len()
                )));
            }
            Ok(FieldValue::Vector(vector))
        }
    }
}

fn doc_state<R: Serialize>(row: &R, schema: &CollectionSchema) -> Result<DocState> {
    // Reject non-finite floats up front: `serde_json` would turn NaN/±Inf into
    // JSON null, silently storing a null.
    crate::finite::ensure_finite(row)
        .map_err(|e| Error::engine(format!("zvec target row has a {e}")))?;
    let value = serde_json::to_value(row)
        .map_err(|e| Error::engine(format!("serialize zvec target row: {e}")))?;
    let JsonValue::Object(row) = value else {
        return Err(Error::engine("zvec target row must serialize to an object"));
    };

    let doc_id = match row.get(&schema.primary_key) {
        Some(JsonValue::String(s)) => s.clone(),
        Some(JsonValue::Number(n)) if n.is_i64() || n.is_u64() => n.to_string(),
        Some(JsonValue::Null) | None => {
            return Err(Error::engine(format!(
                "zvec primary key {:?} value cannot be null",
                schema.primary_key
            )));
        }
        Some(other) => {
            return Err(Error::engine(format!(
                "zvec primary key {:?} must be a string or an integer, got {other}",
                schema.primary_key
            )));
        }
    };

    let mut fields = Vec::new();
    for col in schema.field_columns() {
        match row.get(&col.name) {
            Some(JsonValue::Null) | None => {
                if !col.nullable {
                    return Err(Error::engine(format!(
                        "non-nullable zvec column {:?} is missing or null",
                        col.name
                    )));
                }
            }
            Some(value) => fields.push((col.name.clone(), field_value(col, value)?)),
        }
    }
    Ok(DocState { doc_id, fields })
}

fn build_doc(state: &DocState) -> Result<Doc> {
    let mut doc = Doc::new().map_err(|e| zvec_err("create document", e))?;
    doc.try_set_pk(&state.doc_id)
        .map_err(|e| zvec_err(format_args!("set document id {:?}", state.doc_id), e))?;
    for (name, value) in &state.fields {
        match value {
            FieldValue::Bool(v) => doc.add_bool(name, *v),
            FieldValue::Int32(v) => doc.add_i32(name, *v),
            FieldValue::Int64(v) => doc.add_i64(name, *v),
            FieldValue::Uint32(v) => doc.add_u32(name, *v),
            FieldValue::Uint64(v) => doc.add_u64(name, *v),
            FieldValue::Float(v) => doc.add_f32(name, *v),
            FieldValue::Double(v) => doc.add_f64(name, *v),
            FieldValue::String(v) => doc.add_string(name, v),
            FieldValue::Vector(v) => doc.add_vector_f32(name, v),
        }
        .map_err(|e| {
            zvec_err(
                format_args!("set field {name:?} of document {:?}", state.doc_id),
                e,
            )
        })?;
    }
    Ok(doc)
}

/// Fail if any document of a write was rejected.
fn check_write(operation: &str, result: &zvec_rust::WriteResult) -> Result<()> {
    if result.error_count == 0 {
        return Ok(());
    }
    let first = result
        .results
        .iter()
        .find(|r| !r.is_success())
        .map(|r| format!(": code={:?} message={}", r.code, r.message))
        .unwrap_or_default();
    Err(Error::engine(format!(
        "zvec {operation} failed for {} document(s){first}",
        result.error_count
    )))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DocAction {
    doc_id: String,
    /// `None` deletes the document.
    state: Option<DocState>,
}

struct DocHandler {
    sink: SinkCache<DocAction>,
    db_key: String,
    collection_name: String,
}

impl DocHandler {
    fn new(db_key: String, collection_name: String) -> Self {
        Self {
            sink: SinkCache::default(),
            db_key,
            collection_name,
        }
    }

    fn doc_sink(&self) -> TargetActionSink<DocAction> {
        let db_key = self.db_key.clone();
        let collection_name = self.collection_name.clone();
        TargetActionSink::from_async_fn_with_ctx(
            move |host_ctx, actions: Vec<TargetAction<DocAction>>| {
                let db_key = db_key.clone();
                let collection_name = collection_name.clone();
                async move {
                    if actions.is_empty() {
                        return Ok(());
                    }
                    let conn = resolve_connection(&host_ctx, &db_key)?;
                    run_blocking(move || apply_docs(&conn, &collection_name, actions)).await
                }
            },
        )
    }
}

fn apply_docs(
    conn: &ManagedConnection,
    collection_name: &str,
    actions: Vec<TargetAction<DocAction>>,
) -> Result<()> {
    let collection = conn.open_existing_blocking(collection_name)?;
    let mut upserts: Vec<Doc> = Vec::new();
    let mut deletes: Vec<String> = Vec::new();
    for action in actions {
        let (TargetAction::Create(doc) | TargetAction::Update(doc) | TargetAction::Delete(doc)) =
            action;
        match &doc.state {
            Some(state) => upserts.push(build_doc(state)?),
            None => deletes.push(doc.doc_id),
        }
    }

    if !upserts.is_empty() {
        let docs: Vec<&Doc> = upserts.iter().collect();
        let result = collection
            .upsert(&docs)
            .map_err(|e| zvec_err(format_args!("upsert into {collection_name:?}"), e))?;
        check_write("upsert", &result)?;
    }
    if !deletes.is_empty() {
        let ids: Vec<&str> = deletes.iter().map(String::as_str).collect();
        let result = collection
            .delete(&ids)
            .map_err(|e| zvec_err(format_args!("delete from {collection_name:?}"), e))?;
        check_write("delete", &result)?;
    }
    collection
        .optimize()
        .map_err(|e| zvec_err(format_args!("optimize {collection_name:?}"), e))
}

impl TargetHandler<DocState> for DocHandler {
    type TrackingRecord = Fingerprint;
    type Action = DocAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<DocState>,
        prev: Vec<Fingerprint>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<DocAction, Fingerprint>>> {
        let StableKey::Str(doc_id) = &key else {
            return Err(Error::engine(format!(
                "unexpected zvec document key: {key:?}"
            )));
        };
        let doc_id = doc_id.to_string();
        let Some(state) = desired else {
            if prev.is_empty() && !prev_may_be_missing {
                return Ok(None);
            }
            return Ok(Some(TargetReconcileOutput {
                action: TargetAction::Delete(DocAction {
                    doc_id,
                    state: None,
                }),
                sink: self.sink.get(|| self.doc_sink()),
                tracking_record: None,
                child_invalidation: None,
            }));
        };

        let fingerprint = Fingerprint::from(&state).map_err(Error::from)?;
        if !prev_may_be_missing && !prev.is_empty() && prev.iter().all(|p| p == &fingerprint) {
            return Ok(None);
        }
        Ok(Some(TargetReconcileOutput {
            action: TargetAction::Update(DocAction {
                doc_id,
                state: Some(state),
            }),
            sink: self.sink.get(|| self.doc_sink()),
            tracking_record: Some(fingerprint),
            child_invalidation: None,
        }))
    }
}

// ---------------------------------------------------------------------------
// Collection level
// ---------------------------------------------------------------------------

/// Spec for a zvec collection (the declared container value).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CollectionSpec {
    schema: CollectionSchema,
    #[serde(default)]
    managed_by: ManagedBy,
}

/// The part of a collection's schema whose change rebuilds the collection: the
/// primary key and the field columns, in name order.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct CollectionTrackingCore {
    primary_key: String,
    columns: Vec<Column>,
}

type CollectionTrackingRecord = MutualTrackingRecord<CollectionTrackingCore>;

fn tracking_core(schema: &CollectionSchema) -> CollectionTrackingCore {
    let mut columns: Vec<Column> = schema.field_columns().cloned().collect();
    columns.sort_by(|a, b| a.name.cmp(&b.name));
    CollectionTrackingCore {
        primary_key: schema.primary_key.clone(),
        columns,
    }
}

/// A collection target state's key: the connection's `ContextKey` name plus the
/// collection name.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct CollectionKey {
    db_key: String,
    collection_name: String,
}

impl CollectionKey {
    fn stable_key(&self) -> StableKey {
        StableKey::Array(Arc::from([
            StableKey::Str(Arc::from(self.db_key.as_str())),
            StableKey::Str(Arc::from(self.collection_name.as_str())),
        ]))
    }

    fn from_stable_key(key: &StableKey) -> Result<Self> {
        if let StableKey::Array(parts) = key
            && let [StableKey::Str(db_key), StableKey::Str(collection_name)] = &parts[..]
        {
            return Ok(Self {
                db_key: db_key.to_string(),
                collection_name: collection_name.to_string(),
            });
        }
        Err(Error::engine(format!(
            "unexpected zvec collection target key: {key:?}"
        )))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CollectionAction {
    key: CollectionKey,
    /// `None` when the collection is no longer declared.
    spec: Option<CollectionSpec>,
    /// `Some` implies a system-managed collection: `resolve_system_transition`
    /// yields no transition for a user-managed one.
    main_action: Option<DiffAction>,
}

/// Resolve the live connection from the host context by its `ContextKey` name,
/// at apply time.
fn resolve_connection(host_ctx: &Arc<ContextStore>, db_key: &str) -> Result<ManagedConnection> {
    host_ctx
        .resolve::<ManagedConnection>(db_key)
        .map(|conn| conn.as_ref().clone())
        .ok_or_else(|| {
            Error::engine(format!(
                "zvec target: connection `{db_key}` was not provided in the environment \
                 (call Environment::builder().provide_key(&KEY, connection))"
            ))
        })
}

#[linkme::distributed_slice(STATIC_ROOT_PROVIDERS)]
static COLLECTION_PROVIDER: StaticRootProvider =
    StaticRootProvider::new::<CollectionSpec, CollectionHandler>("cocoindex/zvec/collection");

/// Root handler for every zvec collection target: stateless, the connection is
/// named by each target state's [`CollectionKey`].
#[derive(Default)]
struct CollectionHandler {
    sink: SinkCache<CollectionAction>,
}

impl TargetHandler<CollectionSpec> for CollectionHandler {
    type TrackingRecord = CollectionTrackingRecord;
    type Action = CollectionAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<CollectionSpec>,
        prev: Vec<CollectionTrackingRecord>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<CollectionAction, CollectionTrackingRecord>>> {
        let key = CollectionKey::from_stable_key(&key)?;
        let tracking = desired
            .as_ref()
            .map(|spec| MutualTrackingRecord::new(tracking_core(&spec.schema), spec.managed_by));
        let resolved = resolve_system_transition(tracking.clone(), prev, prev_may_be_missing);
        let main_action = diff(resolved.as_ref());

        let Some(spec) = desired else {
            if main_action.is_none() {
                return Ok(None);
            }
            return Ok(Some(TargetReconcileOutput {
                action: TargetAction::Delete(CollectionAction {
                    key,
                    spec: None,
                    main_action,
                }),
                sink: self.sink.get(collection_sink),
                tracking_record: None,
                child_invalidation: Some(TargetChildInvalidation::Destructive),
            }));
        };

        // There is no in-place schema evolution: any schema change rebuilds
        // the collection, which destroys all documents.
        let child_invalidation = matches!(main_action, Some(DiffAction::Replace))
            .then_some(TargetChildInvalidation::Destructive);
        // Always emit when declared so the sink builds the document handler.
        Ok(Some(TargetReconcileOutput {
            action: TargetAction::Update(CollectionAction {
                key,
                spec: Some(spec),
                main_action,
            }),
            sink: self.sink.get(collection_sink),
            tracking_record: tracking,
            child_invalidation,
        }))
    }
}

fn apply_collection_action(conn: &ManagedConnection, action: &CollectionAction) -> Result<()> {
    let name = &action.key.collection_name;
    if matches!(
        action.main_action,
        Some(DiffAction::Replace | DiffAction::Delete)
    ) {
        conn.destroy_blocking(name)?;
    }
    if let Some(spec) = &action.spec
        && matches!(
            action.main_action,
            Some(DiffAction::Insert | DiffAction::Upsert | DiffAction::Replace)
        )
    {
        conn.open_or_create_blocking(name, &spec.schema)?;
    }
    Ok(())
}

fn collection_sink() -> TargetActionSink<CollectionAction> {
    TargetActionSink::from_async_fn_with_child_handler_ctx(
        move |host_ctx, actions: Vec<TargetAction<CollectionAction>>| async move {
            run_blocking(move || {
                for action in actions {
                    let (TargetAction::Create(action)
                    | TargetAction::Update(action)
                    | TargetAction::Delete(action)) = action;
                    let conn = resolve_connection(&host_ctx, &action.key.db_key)?;
                    apply_collection_action(&conn, &action)?;
                }
                Ok(())
            })
            .await
        },
        |action: &CollectionAction| {
            Ok(ChildTargetDef::new::<DocState, _>(DocHandler::new(
                action.key.db_key.clone(),
                action.key.collection_name.clone(),
            )))
        },
    )
}

// ---------------------------------------------------------------------------
// User-facing API
// ---------------------------------------------------------------------------

/// A target for writing documents to a zvec collection.
#[derive(Clone)]
pub struct CollectionTarget {
    schema: CollectionSchema,
    docs: TargetStateProvider<DocState>,
}

impl CollectionTarget {
    /// Declare a document (row) to be upserted to this collection. `row` must
    /// serialize to an object containing the schema's columns (extra fields are
    /// dropped, missing ones are null). The primary-key value becomes the
    /// document id, as a string.
    pub fn declare_row<R: Serialize>(&self, ctx: &Ctx, row: &R) -> Result<()> {
        let state = doc_state(row, &self.schema)?;
        let key = StableKey::Str(Arc::from(state.doc_id.as_str()));
        declare_target_state(ctx, self.docs.target_state(key, state))
    }
}

/// Build a composable [`TargetState`] for a zvec collection. Pass it to
/// [`declare_collection_target`] / [`mount_collection_target`] or the generic
/// `declare_target_state_with_child` / `mount_target`. System-managed.
///
/// - `db`: the `ContextKey` of the [`ManagedConnection`].
/// - `collection_name`: a subdirectory under the connection's base path.
/// - `schema`: built with [`CollectionSchema::from_row`].
pub fn collection_target(
    ctx: &Ctx,
    db: &ContextKey<ManagedConnection>,
    collection_name: impl Into<String>,
    schema: CollectionSchema,
) -> Result<TargetState<CollectionSpec>> {
    collection_target_with_options(
        ctx,
        db,
        collection_name,
        schema,
        ManagedTargetOptions::default(),
    )
}

/// [`collection_target`] with explicit [`ManagedTargetOptions`]: whether
/// CocoIndex manages the collection lifecycle (`System`), or the collection
/// must already exist and only its documents are managed (`User`).
pub fn collection_target_with_options(
    ctx: &Ctx,
    db: &ContextKey<ManagedConnection>,
    collection_name: impl Into<String>,
    schema: CollectionSchema,
    options: ManagedTargetOptions,
) -> Result<TargetState<CollectionSpec>> {
    let collection_name = collection_name.into();
    validate_collection_name(&collection_name)?;
    schema.validate()?;
    let key = CollectionKey {
        db_key: db.name().to_string(),
        collection_name,
    };
    Ok(COLLECTION_PROVIDER.get(ctx)?.target_state(
        key.stable_key(),
        CollectionSpec {
            schema,
            managed_by: options.managed_by,
        },
    ))
}

/// Declare a zvec collection target in the current component and return a
/// pending [`CollectionTarget`]. System-managed. Its document provider
/// resolves when this component commits; use [`mount_collection_target`] when
/// rows must be declared immediately.
pub fn declare_collection_target(
    ctx: &Ctx,
    db: &ContextKey<ManagedConnection>,
    collection_name: impl Into<String>,
    schema: CollectionSchema,
) -> Result<CollectionTarget> {
    declare_collection_target_with_options(
        ctx,
        db,
        collection_name,
        schema,
        ManagedTargetOptions::default(),
    )
}

/// [`declare_collection_target`] with explicit [`ManagedTargetOptions`].
pub fn declare_collection_target_with_options(
    ctx: &Ctx,
    db: &ContextKey<ManagedConnection>,
    collection_name: impl Into<String>,
    schema: CollectionSchema,
    options: ManagedTargetOptions,
) -> Result<CollectionTarget> {
    let target_state = collection_target_with_options(ctx, db, collection_name, schema, options)?;
    let schema = target_state.value().schema.clone();
    let docs = declare_target_state_with_child::<CollectionSpec, DocState>(ctx, target_state)?;
    Ok(CollectionTarget { schema, docs })
}

/// Mount a zvec collection target and return a ready-to-use
/// [`CollectionTarget`]. System-managed. A schema change rebuilds the
/// collection and re-declares its documents.
pub async fn mount_collection_target(
    ctx: &Ctx,
    db: &ContextKey<ManagedConnection>,
    collection_name: impl Into<String>,
    schema: CollectionSchema,
) -> Result<CollectionTarget> {
    mount_collection_target_with_options(
        ctx,
        db,
        collection_name,
        schema,
        ManagedTargetOptions::default(),
    )
    .await
}

/// [`mount_collection_target`] with explicit [`ManagedTargetOptions`].
pub async fn mount_collection_target_with_options(
    ctx: &Ctx,
    db: &ContextKey<ManagedConnection>,
    collection_name: impl Into<String>,
    schema: CollectionSchema,
    options: ManagedTargetOptions,
) -> Result<CollectionTarget> {
    let target_state = collection_target_with_options(ctx, db, collection_name, schema, options)?;
    let schema = target_state.value().schema.clone();
    let docs = mount_target::<CollectionSpec, DocState>(ctx, target_state).await?;
    Ok(CollectionTarget { schema, docs })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, kind: ColumnKind) -> Column {
        Column {
            name: name.to_string(),
            nullable: true,
            kind,
        }
    }

    #[test]
    fn scalar_encodings_convert_serialized_values() {
        let string = |encoding| {
            column(
                "c",
                ColumnKind::Scalar {
                    data_type: ScalarType::String,
                    encoding,
                    indexed: true,
                },
            )
        };
        assert_eq!(
            field_value(
                &string(ValueEncoding::Base64),
                &serde_json::json!([104, 105])
            )
            .unwrap(),
            FieldValue::String("aGk=".to_string())
        );
        assert_eq!(
            field_value(
                &string(ValueEncoding::Json),
                &serde_json::json!({"a": [1, 2]})
            )
            .unwrap(),
            FieldValue::String("{\"a\":[1,2]}".to_string())
        );
        assert_eq!(
            field_value(
                &string(ValueEncoding::DecimalString),
                &serde_json::json!("1.50")
            )
            .unwrap(),
            FieldValue::String("1.50".to_string())
        );
        let duration = column(
            "c",
            ColumnKind::Scalar {
                data_type: ScalarType::Double,
                encoding: ValueEncoding::DurationSeconds,
                indexed: true,
            },
        );
        assert_eq!(
            field_value(
                &duration,
                &serde_json::to_value(std::time::Duration::from_millis(1500)).unwrap()
            )
            .unwrap(),
            FieldValue::Double(1.5)
        );
    }

    #[test]
    fn values_that_do_not_fit_the_column_are_errors() {
        let int32 = column(
            "n",
            ColumnKind::Scalar {
                data_type: ScalarType::Int32,
                encoding: ValueEncoding::Native,
                indexed: true,
            },
        );
        let err = field_value(&int32, &serde_json::json!(i64::from(i32::MAX) + 1)).unwrap_err();
        assert!(err.to_string().contains("\"n\""), "{err}");

        let vector = column(
            "v",
            ColumnKind::Dense {
                dimension: 3,
                metric: VectorMetric::Cosine,
                quantize: None,
            },
        );
        let err = field_value(&vector, &serde_json::json!([1.0, 2.0])).unwrap_err();
        assert!(err.to_string().contains("dimension 3"), "{err}");
        // A NaN element reaches the connector as null.
        let err = field_value(&vector, &serde_json::json!([1.0, null, 2.0])).unwrap_err();
        assert!(err.to_string().contains("finite"), "{err}");
    }
}
