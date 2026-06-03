//! Apache Doris table target connector.
//!
//! Table targets reconcile declared rows against the previous run: changed rows
//! are upserted, unchanged rows are skipped, and rows no longer declared are
//! deleted. `managed_by` controls whether CocoIndex owns table DDL.
//!
//! Doris uses two protocols, mirroring Python's `doris` connector:
//! * **DDL & deletes** go over the MySQL protocol (FE query port, default
//!   `9030`) via `sqlx`.
//! * **Row ingestion** goes over **Stream Load** — an HTTP `PUT` to the FE
//!   (`/api/{db}/{table}/_stream_load`, default port `8030`/`8080`) carrying a
//!   JSON array of rows.
//!
//! Tables are created with the **`DUPLICATE KEY`** model (required for vector
//! indexes). Because that model appends rather than upserts, an update is a
//! *delete-before-insert*: the primary keys being (re)loaded are first removed
//! with a SQL `DELETE`, then the new rows are Stream Loaded.
//!
//! Use [`table_target`] to build a composable target state,
//! [`declare_table_target`] inside the current component, or
//! [`mount_table_target`] when rows must be declared immediately. Vector and
//! inverted (full-text) indexes are declared through [`DorisTableOptions`].

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use sqlx::MySqlPool;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::sql_ident::validate_ident;
use crate::statediff::{
    CompositeTrackingRecord, DiffAction, ManagedBy, MutualTrackingRecord, diff, diff_composite,
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

/// Connection configuration for a Doris cluster. Mirrors Python's
/// `DorisConnectionConfig`. Construct with [`DorisConfig::new`] and adjust with
/// the builder setters.
#[derive(Clone, Debug)]
pub struct DorisConfig {
    /// Frontend host (used for both the MySQL query port and Stream Load HTTP).
    pub fe_host: String,
    /// Database (schema) name. Must already exist.
    pub database: String,
    /// Frontend HTTP port for Stream Load (Python default `8080`).
    pub fe_http_port: u16,
    /// Frontend MySQL query port for DDL/DELETE (default `9030`).
    pub query_port: u16,
    /// MySQL/Stream-Load user (default `root`).
    pub username: String,
    /// Password (default empty).
    pub password: String,
    /// Use `https` for Stream Load.
    pub enable_https: bool,
    /// When set, a Stream Load `307` redirect to a backend is rewritten to use
    /// this host (the redirected BE host is otherwise often only reachable
    /// inside the cluster network).
    pub be_load_host: Option<String>,
    /// Max rows per Stream Load / DELETE batch.
    pub batch_size: usize,
    /// Per-Stream-Load HTTP timeout (seconds).
    pub stream_load_timeout: u64,
    /// Stream Load retry attempts on transient transport errors.
    pub max_retries: u32,
    /// First retry backoff (seconds); doubles each attempt up to `retry_max_delay`.
    pub retry_base_delay: f64,
    /// Backoff cap (seconds).
    pub retry_max_delay: f64,
    /// `replication_num` table property (default `1`; an all-in-one cluster has
    /// a single backend).
    pub replication_num: u32,
    /// `DISTRIBUTED BY HASH(...) BUCKETS <n>` — `"auto"` or a number.
    pub buckets: String,
}

impl DorisConfig {
    /// Minimal config: frontend host + database, everything else defaulted to
    /// match Python's `DorisConnectionConfig`.
    pub fn new(fe_host: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            fe_host: fe_host.into(),
            database: database.into(),
            fe_http_port: 8080,
            query_port: 9030,
            username: "root".to_string(),
            password: String::new(),
            enable_https: false,
            be_load_host: None,
            batch_size: 10_000,
            stream_load_timeout: 600,
            max_retries: 3,
            retry_base_delay: 1.0,
            retry_max_delay: 30.0,
            replication_num: 1,
            buckets: "auto".to_string(),
        }
    }

    pub fn fe_http_port(mut self, port: u16) -> Self {
        self.fe_http_port = port;
        self
    }
    pub fn query_port(mut self, port: u16) -> Self {
        self.query_port = port;
        self
    }
    pub fn username(mut self, user: impl Into<String>) -> Self {
        self.username = user.into();
        self
    }
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = password.into();
        self
    }
    pub fn enable_https(mut self, enable: bool) -> Self {
        self.enable_https = enable;
        self
    }
    pub fn be_load_host(mut self, host: impl Into<String>) -> Self {
        self.be_load_host = Some(host.into());
        self
    }
    pub fn replication_num(mut self, n: u32) -> Self {
        self.replication_num = n;
        self
    }
    /// `BUCKETS` clause — `"auto"` or a positive number (as a string).
    pub fn buckets(mut self, buckets: impl Into<String>) -> Self {
        self.buckets = buckets.into();
        self
    }
    pub fn batch_size(mut self, n: usize) -> Self {
        self.batch_size = n.max(1);
        self
    }

    fn stream_load_url(&self, table: &str) -> String {
        let proto = if self.enable_https { "https" } else { "http" };
        format!(
            "{proto}://{}:{}/api/{}/{}/_stream_load",
            self.fe_host, self.fe_http_port, self.database, table
        )
    }

    fn buckets_clause(&self) -> String {
        if self.buckets.eq_ignore_ascii_case("auto") {
            "AUTO".to_string()
        } else {
            self.buckets.clone()
        }
    }
}

/// A Doris connection handle. Clone-cheap (the MySQL pool and HTTP client are
/// shared). `state_id` (`host:query_port/database`, credentials excluded) is the
/// stable identity used for target-state keys.
#[derive(Clone)]
pub struct DorisConnection {
    config: Arc<DorisConfig>,
    pool: MySqlPool,
    http: reqwest::Client,
    state_id: Arc<str>,
}

impl DorisConnection {
    /// Connect to a Doris cluster. Opens a MySQL connection pool to the FE query
    /// port (used for DDL/DELETE) and builds an HTTP client for Stream Load. The
    /// `database` must already exist.
    pub async fn connect(config: DorisConfig) -> Result<Self> {
        // Doris speaks the MySQL wire protocol but rejects the `SET
        // sql_mode=CONCAT(@@sql_mode, …)` and timezone statements sqlx issues on
        // connect ("Set statement doesn't support non-constant expr"). Disable
        // those handshake options so only constant SETs (if any) are sent.
        let mut options = MySqlConnectOptions::new()
            .host(&config.fe_host)
            .port(config.query_port)
            .username(&config.username)
            .database(&config.database)
            .pipes_as_concat(false)
            .no_engine_substitution(false)
            .timezone(None)
            .set_names(false);
        // Only send a password when one is set; an empty `.password("")` makes
        // sqlx authenticate "using password: YES", which a no-password Doris
        // root rejects.
        if !config.password.is_empty() {
            options = options.password(&config.password);
        }
        let pool = MySqlPoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .map_err(mysql_err)?;
        let http = reqwest::Client::builder()
            // Stream Load redirects FE -> BE; follow it manually so credentials
            // and the body are re-sent (and so `be_load_host` can be applied).
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| Error::engine(format!("doris http client: {e}")))?;
        let state_id = format!(
            "{}:{}/{}",
            config.fe_host, config.query_port, config.database
        );
        Ok(Self {
            config: Arc::new(config),
            pool,
            http,
            state_id: Arc::from(state_id),
        })
    }

    pub fn config(&self) -> &DorisConfig {
        &self.config
    }

    /// The MySQL connection pool (FE query port). Useful in tests to inspect
    /// loaded data.
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    pub fn state_id(&self) -> &str {
        &self.state_id
    }
}

fn mysql_err(e: sqlx::Error) -> Error {
    Error::engine(format!("doris mysql: {e}"))
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// A Doris column. `doris_type` is the SQL type as written in `CREATE TABLE`
/// (e.g. `BIGINT`, `DOUBLE`, `TEXT`, `VARCHAR(255)`, `ARRAY<FLOAT>`). Mark
/// vector columns with [`ColumnDef::vector`] so their dimension can be supplied
/// to a vector index.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnDef {
    pub doris_type: String,
    pub nullable: bool,
    pub is_vector: bool,
    pub vector_dimension: Option<u32>,
}

impl ColumnDef {
    pub fn new(doris_type: impl Into<String>) -> Self {
        Self {
            doris_type: doris_type.into(),
            nullable: true,
            is_vector: false,
            vector_dimension: None,
        }
    }

    /// Mark the column `NOT NULL`.
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Declare an `ARRAY<FLOAT>` vector column of the given dimension. Vector
    /// columns are always `NOT NULL` (a Doris vector-index requirement).
    pub fn vector(dimension: u32) -> Self {
        Self {
            doris_type: "ARRAY<FLOAT>".to_string(),
            nullable: false,
            is_vector: true,
            vector_dimension: Some(dimension),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSchema {
    columns: BTreeMap<String, ColumnDef>,
    primary_key: Vec<String>,
}

impl TableSchema {
    pub fn new(
        columns: impl IntoIterator<Item = (impl Into<String>, ColumnDef)>,
        primary_key: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let mut out = BTreeMap::new();
        for (name, def) in columns {
            let name = name.into();
            validate_ident(&name, "column name")?;
            validate_doris_type(&def.doris_type)?;
            out.insert(name, def);
        }
        let primary_key: Vec<String> = primary_key.into_iter().map(Into::into).collect();
        if primary_key.is_empty() {
            return Err(Error::engine("Doris table primary key cannot be empty"));
        }
        for name in &primary_key {
            validate_ident(name, "primary key column")?;
            if !out.contains_key(name) {
                return Err(Error::engine(format!(
                    "primary key column {name:?} is not in table schema"
                )));
            }
        }
        Ok(Self {
            columns: out,
            primary_key,
        })
    }

    pub fn columns(&self) -> &BTreeMap<String, ColumnDef> {
        &self.columns
    }

    pub fn primary_key(&self) -> &[String] {
        &self.primary_key
    }

    /// Derive a schema from a `#[derive(SchemaFields)]` row type (the Rust
    /// analogue of Python's `TableSchema.from_class`). Each field maps to a Doris
    /// column via the same leaf-type table as Python's `doris` `from_class`.
    pub fn from_row<T: crate::row_schema::SchemaFields>(
        primary_key: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let columns = T::schema_fields()
            .into_iter()
            .map(|f| (f.name.clone(), doris_column_def(&f)));
        Self::new(columns, primary_key)
    }
}

/// Map a connector-agnostic [`SchemaField`](crate::row_schema::SchemaField) to a
/// Doris [`ColumnDef`], mirroring Python's `doris` `_LEAF_TYPE_MAPPINGS`.
fn doris_column_def(field: &crate::row_schema::SchemaField) -> ColumnDef {
    use crate::row_schema::LogicalType as L;
    if let L::Vector { dim, .. } = field.logical_type {
        // Doris vectors are `ARRAY<FLOAT>` and always NOT NULL.
        return ColumnDef::vector(dim);
    }
    let doris_type = match &field.logical_type {
        L::Bool => "BOOLEAN",
        // Doris `from_class` maps Python `int` to BIGINT (no width variants).
        L::Int16 | L::Int32 | L::Int64 | L::Duration => "BIGINT",
        L::Float32 | L::Float64 => "DOUBLE",
        L::Decimal => "TEXT",
        L::Text => "TEXT",
        L::Bytes => "STRING",
        L::Uuid => "VARCHAR(36)",
        L::Date => "DATE",
        L::Time => "VARCHAR(20)",
        L::DateTime => "DATETIME(6)",
        L::Json => "JSON",
        L::Custom(s) => s.as_str(),
        L::Vector { .. } => unreachable!("handled above"),
    };
    let mut def = ColumnDef::new(doris_type);
    def.nullable = field.nullable;
    def
}

/// A vector (ANN) index. Mirrors Python's `VectorIndexDef`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorIndexDef {
    pub field_name: String,
    /// `"HNSW"` (default) or `"IVF"`.
    pub index_type: String,
    /// `"l2_distance"` (default), `"inner_product"`, or `"cosine_distance"`.
    pub metric_type: String,
    pub max_degree: Option<u32>,
    pub ef_construction: Option<u32>,
    pub nlist: Option<u32>,
}

impl VectorIndexDef {
    /// HNSW index with the default `l2_distance` metric.
    pub fn new(field_name: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            index_type: "HNSW".to_string(),
            metric_type: "l2_distance".to_string(),
            max_degree: None,
            ef_construction: None,
            nlist: None,
        }
    }

    pub fn metric_type(mut self, metric: impl Into<String>) -> Self {
        self.metric_type = metric.into();
        self
    }
    pub fn index_type(mut self, index_type: impl Into<String>) -> Self {
        self.index_type = index_type.into();
        self
    }
    pub fn max_degree(mut self, v: u32) -> Self {
        self.max_degree = Some(v);
        self
    }
    pub fn ef_construction(mut self, v: u32) -> Self {
        self.ef_construction = Some(v);
        self
    }
    pub fn nlist(mut self, v: u32) -> Self {
        self.nlist = Some(v);
        self
    }
}

/// An inverted (full-text) index. Mirrors Python's `InvertedIndexDef`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct InvertedIndexDef {
    pub field_name: String,
    /// Tokenizer: `"english"`, `"chinese"`, `"unicode"`, … `None` for an
    /// untokenized inverted index.
    pub parser: Option<String>,
}

impl InvertedIndexDef {
    pub fn new(field_name: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            parser: None,
        }
    }

    pub fn parser(mut self, parser: impl Into<String>) -> Self {
        self.parser = Some(parser.into());
        self
    }
}

/// Options for the `*_with_options` table-target constructors.
#[derive(Clone, Debug, Default)]
pub struct DorisTableOptions {
    pub managed_by: ManagedBy,
    pub vector_indexes: Vec<VectorIndexDef>,
    pub inverted_indexes: Vec<InvertedIndexDef>,
}

// ---------------------------------------------------------------------------
// Public target API: constructor / declaration / mount split
// ---------------------------------------------------------------------------

/// A declarative Doris table target — a handle to declare rows on.
#[derive(Clone)]
pub struct TableTarget {
    table_name: Arc<str>,
    table_schema: TableSchema,
    rows: TargetStateProvider<RowState>,
}

/// Build a composable [`TargetState`] for a Doris table (system-managed, no
/// indexes).
pub fn table_target(
    ctx: &Ctx,
    conn: &DorisConnection,
    table_name: impl Into<String>,
    table_schema: TableSchema,
) -> Result<TargetState<TableSpec>> {
    table_target_with_options(
        ctx,
        conn,
        table_name,
        table_schema,
        DorisTableOptions::default(),
    )
}

/// [`table_target`] with explicit [`DorisTableOptions`] (`managed_by`,
/// vector/inverted indexes).
pub fn table_target_with_options(
    ctx: &Ctx,
    conn: &DorisConnection,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    options: DorisTableOptions,
) -> Result<TargetState<TableSpec>> {
    let table_name = table_name.into();
    validate_ident(&table_name, "table name")?;
    validate_indexes(&table_schema, &options)?;
    let provider = register_root_target_states_provider(
        ctx,
        format!("cocoindex/doris/table/{}/{}", conn.state_id(), table_name),
        TableHandler { conn: conn.clone() },
    )?;
    Ok(provider.target_state(
        "default",
        TableSpec {
            table_name,
            table_schema,
            managed_by: options.managed_by,
            vector_indexes: options.vector_indexes,
            inverted_indexes: options.inverted_indexes,
        },
    ))
}

/// Declare a Doris table target in the **current** component (the row child
/// provider resolves at this component's commit) and return a handle.
pub fn declare_table_target(
    ctx: &Ctx,
    conn: &DorisConnection,
    table_name: impl Into<String>,
    table_schema: TableSchema,
) -> Result<TableTarget> {
    declare_table_target_with_options(
        ctx,
        conn,
        table_name,
        table_schema,
        DorisTableOptions::default(),
    )
}

/// [`declare_table_target`] with explicit [`DorisTableOptions`].
pub fn declare_table_target_with_options(
    ctx: &Ctx,
    conn: &DorisConnection,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    options: DorisTableOptions,
) -> Result<TableTarget> {
    let ts = table_target_with_options(ctx, conn, table_name, table_schema, options)?;
    let spec = ts.value().clone();
    let rows = declare_target_state_with_child::<TableSpec, RowState>(ctx, ts)?;
    Ok(table_target_handle(spec, rows))
}

/// Mount a Doris table target **foreground** (rows can be declared immediately)
/// and return a handle.
pub async fn mount_table_target(
    ctx: &Ctx,
    conn: &DorisConnection,
    table_name: impl Into<String>,
    table_schema: TableSchema,
) -> Result<TableTarget> {
    mount_table_target_with_options(
        ctx,
        conn,
        table_name,
        table_schema,
        DorisTableOptions::default(),
    )
    .await
}

/// [`mount_table_target`] with explicit [`DorisTableOptions`].
pub async fn mount_table_target_with_options(
    ctx: &Ctx,
    conn: &DorisConnection,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    options: DorisTableOptions,
) -> Result<TableTarget> {
    let ts = table_target_with_options(ctx, conn, table_name, table_schema, options)?;
    let spec = ts.value().clone();
    let rows = mount_target::<TableSpec, RowState>(ctx, ts).await?;
    Ok(table_target_handle(spec, rows))
}

fn table_target_handle(spec: TableSpec, rows: TargetStateProvider<RowState>) -> TableTarget {
    TableTarget {
        table_name: Arc::from(spec.table_name),
        table_schema: spec.table_schema,
        rows,
    }
}

impl TableTarget {
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn declare_row<R: Serialize>(&self, ctx: &Ctx, row: &R) -> Result<()> {
        let fields = row_state(row, &self.table_schema)?;
        let key = pk_stable_key(&fields, self.table_schema.primary_key())?;
        declare_target_state(ctx, self.rows.target_state(key, RowState { fields }))
    }
}

// ---------------------------------------------------------------------------
// Internal specs / actions
// ---------------------------------------------------------------------------

/// Spec for a Doris table (the declared container value).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSpec {
    table_name: String,
    table_schema: TableSchema,
    #[serde(default)]
    managed_by: ManagedBy,
    #[serde(default)]
    vector_indexes: Vec<VectorIndexDef>,
    #[serde(default)]
    inverted_indexes: Vec<InvertedIndexDef>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RowState {
    fields: Map<String, JsonValue>,
}

// ---------------------------------------------------------------------------
// Composite schema tracking (mirrors Python `connectors/doris/_target.py`)
//
// A table's tracking record is split into a `main` record (PK columns + their
// types + the declared vector/inverted index fields) and one `sub` record per
// non-PK column (type + nullability). `diff_composite` then distinguishes a
// structural change requiring DROP+CREATE (main changed) from an incremental
// `ALTER TABLE ADD/DROP COLUMN` (main unchanged, individual subs changed).
// ---------------------------------------------------------------------------

const COL_SUBKEY_PREFIX: &str = "col:";

fn col_subkey(col_name: &str) -> String {
    format!("{COL_SUBKEY_PREFIX}{col_name}")
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct PkColumnInfo {
    name: String,
    doris_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TablePrimaryTrackingRecord {
    table_name: String,
    primary_key_columns: Vec<PkColumnInfo>,
    vector_indexes: Option<Vec<String>>,
    inverted_indexes: Option<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NonPkColumnTrackingRecord {
    doris_type: String,
    nullable: bool,
}

type TableCompositeRecord =
    CompositeTrackingRecord<TablePrimaryTrackingRecord, String, NonPkColumnTrackingRecord>;

type TableTrackingRecord = MutualTrackingRecord<TableCompositeRecord>;

fn table_composite_record(spec: &TableSpec) -> TableCompositeRecord {
    let schema = &spec.table_schema;
    let pk: std::collections::HashSet<&String> = schema.primary_key().iter().collect();
    let main = TablePrimaryTrackingRecord {
        table_name: spec.table_name.clone(),
        primary_key_columns: schema
            .primary_key()
            .iter()
            .map(|name| PkColumnInfo {
                name: name.clone(),
                doris_type: schema.columns()[name].doris_type.clone(),
            })
            .collect(),
        vector_indexes: (!spec.vector_indexes.is_empty()).then(|| {
            spec.vector_indexes
                .iter()
                .map(|v| v.field_name.clone())
                .collect()
        }),
        inverted_indexes: (!spec.inverted_indexes.is_empty()).then(|| {
            spec.inverted_indexes
                .iter()
                .map(|v| v.field_name.clone())
                .collect()
        }),
    };
    let sub: HashMap<String, NonPkColumnTrackingRecord> = schema
        .columns()
        .iter()
        .filter(|(name, _)| !pk.contains(*name))
        .map(|(name, col)| {
            (
                col_subkey(name),
                NonPkColumnTrackingRecord {
                    doris_type: col.doris_type.clone(),
                    nullable: col.nullable,
                },
            )
        })
        .collect();
    CompositeTrackingRecord::new(main, sub)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TableAction {
    spec: Option<TableSpec>,
    drop: Option<String>,
    main_action: Option<DiffAction>,
    column_actions: BTreeMap<String, DiffAction>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RowAction {
    pk: Vec<JsonValue>,
    state: Option<RowState>,
}

// ---------------------------------------------------------------------------
// Table container handler (root) + sink yielding row children
// ---------------------------------------------------------------------------

struct TableHandler {
    conn: DorisConnection,
}

impl TargetHandler<TableSpec> for TableHandler {
    type TrackingRecord = TableTrackingRecord;
    type Action = TableAction;

    fn reconcile(
        &self,
        _key: StableKey,
        desired: Option<TableSpec>,
        prev: Vec<TableTrackingRecord>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<TableAction, TableTrackingRecord>>> {
        match desired {
            Some(spec) => {
                let tracking =
                    MutualTrackingRecord::new(table_composite_record(&spec), spec.managed_by);
                let resolved =
                    resolve_system_transition(Some(tracking.clone()), prev, prev_may_be_missing);
                let (main_action, column_transitions) = diff_composite(resolved.as_ref());
                let mut column_actions = BTreeMap::new();
                if main_action.is_none() {
                    for (sub_key, transition) in &column_transitions {
                        if let Some(action) = diff(Some(transition)) {
                            column_actions.insert(sub_key.clone(), action);
                        }
                    }
                }

                // main replace → table dropped & recreated → destructive.
                // column change other than a pure add → may lose data → lossy.
                let child_invalidation = if matches!(main_action, Some(DiffAction::Replace)) {
                    Some(TargetChildInvalidation::Destructive)
                } else if main_action.is_none()
                    && column_actions
                        .values()
                        .any(|a| !matches!(a, DiffAction::Insert))
                {
                    Some(TargetChildInvalidation::Lossy)
                } else {
                    None
                };

                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Update(TableAction {
                        spec: Some(spec),
                        drop: None,
                        main_action,
                        column_actions,
                    }),
                    sink: self.table_sink(),
                    tracking_record: Some(tracking),
                    child_invalidation,
                }))
            }
            None => {
                let resolved = resolve_system_transition(None, prev.clone(), prev_may_be_missing);
                if resolved.is_none() {
                    return Ok(None);
                }
                let Some(prev_record) = prev.into_iter().find(|v| v.managed_by.is_system()) else {
                    return Ok(None);
                };
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Delete(TableAction {
                        spec: None,
                        drop: Some(prev_record.tracking_record.main.table_name),
                        main_action: Some(DiffAction::Delete),
                        column_actions: BTreeMap::new(),
                    }),
                    sink: self.table_sink(),
                    tracking_record: None,
                    child_invalidation: Some(TargetChildInvalidation::Destructive),
                }))
            }
        }
    }
}

impl TableHandler {
    fn table_sink(&self) -> TargetActionSink<TableAction> {
        let conn = self.conn.clone();
        TargetActionSink::from_async_fn_with_children(
            move |actions: Vec<TargetAction<TableAction>>| {
                let conn = conn.clone();
                async move {
                    let mut out: Vec<Option<ChildTargetDef>> = Vec::with_capacity(actions.len());
                    for action in actions {
                        let a = match action {
                            TargetAction::Create(a)
                            | TargetAction::Update(a)
                            | TargetAction::Delete(a) => a,
                        };
                        out.push(apply_table_action(&conn, a).await?);
                    }
                    Ok(out)
                }
            },
        )
    }
}

/// Apply one resolved table action and return the row child provider (or `None`
/// for a drop). Mirrors Python's `_apply_table_actions`.
async fn apply_table_action(
    conn: &DorisConnection,
    action: TableAction,
) -> Result<Option<ChildTargetDef>> {
    let TableAction {
        spec,
        drop,
        main_action,
        column_actions,
    } = action;

    // A structural rewrite or a drop removes the existing table first.
    if matches!(
        main_action,
        Some(DiffAction::Replace) | Some(DiffAction::Delete)
    ) {
        let table_name = spec
            .as_ref()
            .map(|s| s.table_name.clone())
            .or(drop)
            .ok_or_else(|| Error::engine("Doris drop action missing table name"))?;
        drop_table(conn, &table_name).await?;
    }

    let Some(spec) = spec else {
        return Ok(None);
    };

    match main_action {
        Some(DiffAction::Insert | DiffAction::Upsert | DiffAction::Replace) => {
            create_table(conn, &spec).await?;
        }
        _ => {
            if !column_actions.is_empty() {
                apply_column_actions(conn, &spec, &column_actions).await?;
            }
        }
    }

    Ok(Some(ChildTargetDef::new::<RowState, _>(RowHandler {
        conn: conn.clone(),
        spec,
    })))
}

// ---------------------------------------------------------------------------
// Row handler (child) + sink
// ---------------------------------------------------------------------------

struct RowHandler {
    conn: DorisConnection,
    spec: TableSpec,
}

impl TargetHandler<RowState> for RowHandler {
    type TrackingRecord = Fingerprint;
    type Action = RowAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<RowState>,
        prev: Vec<Fingerprint>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<RowAction, Fingerprint>>> {
        let pk = stable_key_to_pk(&key)?;
        let desired_fp = match &desired {
            Some(state) => Some(Fingerprint::from(state).map_err(Error::from)?),
            None => None,
        };
        // Skip only when every previous fingerprint matches (mirrors the Kafka /
        // Iggy row handlers and Python's `all(prev == target_fp ...)`); `any`
        // would wrongly skip a row whose previous records disagree.
        let prev_same = desired_fp
            .as_ref()
            .is_some_and(|fp| !prev.is_empty() && prev.iter().all(|p| p == fp));
        if desired.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }
        Ok(Some(TargetReconcileOutput {
            action: TargetAction::Update(RowAction { pk, state: desired }),
            sink: self.row_sink(),
            tracking_record: desired_fp,
            child_invalidation: None,
        }))
    }
}

impl RowHandler {
    fn row_sink(&self) -> TargetActionSink<RowAction> {
        let conn = self.conn.clone();
        let spec = self.spec.clone();
        TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<RowAction>>| {
            let conn = conn.clone();
            let spec = spec.clone();
            async move {
                let mut mutations = Vec::with_capacity(actions.len());
                for action in actions {
                    let row = match action {
                        TargetAction::Create(r)
                        | TargetAction::Update(r)
                        | TargetAction::Delete(r) => r,
                    };
                    mutations.push((row.pk, row.state));
                }
                apply_rows(&conn, &spec, mutations).await
            }
        })
    }
}

// ---------------------------------------------------------------------------
// DB I/O
// ---------------------------------------------------------------------------

async fn create_table(conn: &DorisConnection, spec: &TableSpec) -> Result<()> {
    if spec.managed_by.is_user() {
        return Ok(());
    }
    let sql = create_table_sql(&conn.config, spec);
    sqlx::raw_sql(&sql)
        .execute(conn.pool())
        .await
        .map_err(mysql_err)?;
    Ok(())
}

async fn drop_table(conn: &DorisConnection, table_name: &str) -> Result<()> {
    validate_ident(table_name, "table name")?;
    let sql = format!(
        "DROP TABLE IF EXISTS `{}`.`{}`",
        conn.config.database, table_name
    );
    sqlx::raw_sql(&sql)
        .execute(conn.pool())
        .await
        .map_err(mysql_err)?;
    Ok(())
}

/// Apply per-column changes to an existing table via `ALTER TABLE`, preserving
/// rows. Mirrors Python's incremental column reconcile (best-effort: Doris
/// schema-change jobs are asynchronous, so a failed/duplicate ALTER is logged
/// and skipped rather than aborting the reconcile). PK columns are never altered
/// here — they belong to the `main` record and force DROP+CREATE.
async fn apply_column_actions(
    conn: &DorisConnection,
    spec: &TableSpec,
    column_actions: &BTreeMap<String, DiffAction>,
) -> Result<()> {
    if spec.managed_by.is_user() {
        return Ok(());
    }
    let db = &conn.config.database;
    let table = &spec.table_name;
    let schema = &spec.table_schema;
    let pk: std::collections::HashSet<&str> =
        schema.primary_key().iter().map(String::as_str).collect();

    for (sub_key, action) in column_actions {
        let Some(col_name) = sub_key.strip_prefix(COL_SUBKEY_PREFIX) else {
            return Err(Error::engine(format!(
                "Doris column action has unexpected sub-key {sub_key:?}"
            )));
        };
        if pk.contains(col_name) {
            continue;
        }
        match action {
            DiffAction::Delete => {
                run_best_effort(
                    conn,
                    &format!("ALTER TABLE `{db}`.`{table}` DROP COLUMN `{col_name}`"),
                )
                .await;
            }
            DiffAction::Insert | DiffAction::Upsert => {
                if let Some(col) = schema.columns().get(col_name) {
                    let null = if col.nullable { "NULL" } else { "NOT NULL" };
                    run_best_effort(
                        conn,
                        &format!(
                            "ALTER TABLE `{db}`.`{table}` ADD COLUMN `{col_name}` {} {null}",
                            col.doris_type
                        ),
                    )
                    .await;
                }
            }
            DiffAction::Replace => {
                // Doris has no portable in-place column retype here; drop then
                // re-add (a schema-change job).
                if let Some(col) = schema.columns().get(col_name) {
                    run_best_effort(
                        conn,
                        &format!("ALTER TABLE `{db}`.`{table}` DROP COLUMN `{col_name}`"),
                    )
                    .await;
                    let null = if col.nullable { "NULL" } else { "NOT NULL" };
                    run_best_effort(
                        conn,
                        &format!(
                            "ALTER TABLE `{db}`.`{table}` ADD COLUMN `{col_name}` {} {null}",
                            col.doris_type
                        ),
                    )
                    .await;
                }
            }
        }
    }
    Ok(())
}

async fn run_best_effort(conn: &DorisConnection, sql: &str) {
    if let Err(e) = sqlx::raw_sql(sql).execute(conn.pool()).await {
        tracing::debug!("doris best-effort DDL skipped ({sql:?}): {e}");
    }
}

/// Apply row mutations. For the `DUPLICATE KEY` model an upsert is a
/// delete-before-insert: the upserted PKs are deleted first, then the new rows
/// are Stream Loaded; rows that were un-declared are deleted directly. Mirrors
/// Python's `_RowHandler._apply_actions`.
async fn apply_rows(
    conn: &DorisConnection,
    spec: &TableSpec,
    mutations: Vec<(Vec<JsonValue>, Option<RowState>)>,
) -> Result<()> {
    if mutations.is_empty() {
        return Ok(());
    }
    let pk_cols = spec.table_schema.primary_key();
    let mut upsert_rows: Vec<Map<String, JsonValue>> = Vec::new();
    let mut upsert_keys: Vec<Vec<JsonValue>> = Vec::new();
    let mut delete_keys: Vec<Vec<JsonValue>> = Vec::new();

    for (pk, state) in mutations {
        match state {
            Some(state) => {
                upsert_keys.push(pk_values(&state.fields, pk_cols)?);
                upsert_rows.push(state.fields);
            }
            None => delete_keys.push(pk),
        }
    }

    let batch = conn.config.batch_size;
    // Delete-before-insert for upserts.
    for chunk in upsert_keys.chunks(batch) {
        execute_delete(conn, &spec.table_name, pk_cols, chunk).await?;
    }
    for chunk in upsert_rows.chunks(batch) {
        stream_load(conn, &spec.table_name, chunk).await?;
    }
    // Direct deletes for un-declared rows.
    for chunk in delete_keys.chunks(batch) {
        execute_delete(conn, &spec.table_name, pk_cols, chunk).await?;
    }
    Ok(())
}

/// SQL `DELETE` of rows by primary key (over the MySQL protocol). Mirrors
/// Python's `_execute_delete`: one `( ... AND ... )` group per key, OR-joined.
async fn execute_delete(
    conn: &DorisConnection,
    table_name: &str,
    pk_cols: &[String],
    keys: &[Vec<JsonValue>],
) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    validate_ident(table_name, "table name")?;
    let mut groups = Vec::with_capacity(keys.len());
    for key in keys {
        if key.len() != pk_cols.len() {
            return Err(Error::engine("Doris row key length mismatch"));
        }
        let mut parts = Vec::with_capacity(pk_cols.len());
        for (col, val) in pk_cols.iter().zip(key) {
            if val.is_null() {
                parts.push(format!("`{col}` IS NULL"));
            } else {
                parts.push(format!("`{col}` = {}", sql_value_literal(val)));
            }
        }
        groups.push(format!("({})", parts.join(" AND ")));
    }
    let sql = format!(
        "DELETE FROM `{}`.`{}` WHERE {}",
        conn.config.database,
        table_name,
        groups.join(" OR ")
    );
    sqlx::raw_sql(&sql)
        .execute(conn.pool())
        .await
        .map_err(mysql_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Stream Load (HTTP)
// ---------------------------------------------------------------------------

async fn stream_load(
    conn: &DorisConnection,
    table_name: &str,
    rows: &[Map<String, JsonValue>],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let config = &conn.config;
    let url = config.stream_load_url(table_name);
    let label = stream_load_label();

    // `columns` header: the sorted union of all keys present in the batch.
    let mut all_columns: BTreeSet<&str> = BTreeSet::new();
    for row in rows {
        for k in row.keys() {
            all_columns.insert(k.as_str());
        }
    }
    let columns = all_columns.into_iter().collect::<Vec<_>>().join(", ");

    let body = serde_json::to_string(rows)
        .map_err(|e| Error::engine(format!("doris stream-load encode: {e}")))?;

    let mut attempt: u32 = 0;
    loop {
        match do_stream_load(conn, &url, &label, &columns, &body).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if !e.retryable || attempt >= config.max_retries {
                    return Err(Error::engine(format!(
                        "doris stream load to {table_name} failed: {}",
                        e.message
                    )));
                }
                let delay = (config.retry_base_delay * 2f64.powi(attempt as i32))
                    .min(config.retry_max_delay);
                tracing::warn!(
                    "doris stream load attempt {} failed ({}); retrying in {:.1}s",
                    attempt + 1,
                    e.message,
                    delay
                );
                tokio::time::sleep(Duration::from_secs_f64(delay)).await;
                attempt += 1;
            }
        }
    }
}

struct StreamLoadError {
    message: String,
    retryable: bool,
}

/// Perform a single Stream Load attempt, following the FE→BE `307` redirect
/// manually (so basic auth and the body are re-sent, applying `be_load_host` if
/// configured). Transport errors are retryable; auth (`401`/`403`) and a
/// non-`Success`/`Publish Timeout` load status are fatal.
async fn do_stream_load(
    conn: &DorisConnection,
    url: &str,
    label: &str,
    columns: &str,
    body: &str,
) -> std::result::Result<(), StreamLoadError> {
    let config = &conn.config;
    let timeout = Duration::from_secs(config.stream_load_timeout);

    let send = |target_url: String| {
        let req = conn
            .http
            .put(&target_url)
            .basic_auth(&config.username, Some(&config.password))
            .header("format", "json")
            .header("strip_outer_array", "true")
            .header("label", label)
            .header("Expect", "100-continue")
            .timeout(timeout);
        let req = if columns.is_empty() {
            req
        } else {
            req.header("columns", columns)
        };
        req.body(body.to_string()).send()
    };

    let resp = send(url.to_string()).await.map_err(transport_err)?;
    let status = resp.status();
    let resp = if status.as_u16() == 307 {
        let location = resp
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| StreamLoadError {
                message: "stream load 307 redirect without Location header".to_string(),
                retryable: false,
            })?;
        let target = rewrite_redirect(&location, config.be_load_host.as_deref());
        send(target).await.map_err(transport_err)?
    } else {
        resp
    };

    let status = resp.status();
    if status.as_u16() == 401 || status.as_u16() == 403 {
        return Err(StreamLoadError {
            message: format!("authentication failed: HTTP {status}"),
            retryable: false,
        });
    }
    let text = resp.text().await.map_err(transport_err)?;
    let result: JsonValue = serde_json::from_str(&text).map_err(|_| StreamLoadError {
        message: format!("invalid stream load response: {}", truncate(&text, 200)),
        retryable: false,
    })?;
    let load_status = result
        .get("Status")
        .and_then(JsonValue::as_str)
        .unwrap_or("Unknown");
    if load_status == "Success" || load_status == "Publish Timeout" {
        Ok(())
    } else {
        let msg = result
            .get("Message")
            .and_then(JsonValue::as_str)
            .unwrap_or("unknown error");
        let err_url = result
            .get("ErrorURL")
            .and_then(JsonValue::as_str)
            .map(|u| format!(" (ErrorURL: {u})"))
            .unwrap_or_default();
        Err(StreamLoadError {
            message: format!("load status {load_status}: {msg}{err_url}"),
            retryable: false,
        })
    }
}

fn transport_err(e: reqwest::Error) -> StreamLoadError {
    let retryable = e.is_timeout() || e.is_connect() || e.is_request();
    StreamLoadError {
        message: e.to_string(),
        retryable,
    }
}

/// Rewrite a redirect URL's host[:port] to `be_load_host` (keeping the original
/// port) when configured; otherwise return the location unchanged.
fn rewrite_redirect(location: &str, be_load_host: Option<&str>) -> String {
    let Some(host) = be_load_host else {
        return location.to_string();
    };
    let Some((scheme, rest)) = location.split_once("://") else {
        return location.to_string();
    };
    let (authority, path) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, ""),
    };
    let port = authority.rsplit_once(':').map(|(_, p)| p);
    let new_authority = match port {
        Some(p) => format!("{host}:{p}"),
        None => host.to_string(),
    };
    format!("{scheme}://{new_authority}{path}")
}

fn stream_load_label() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let rand = uuid::Uuid::new_v4().simple().to_string();
    format!("cocoindex_{millis}_{}", &rand[..8])
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

// ---------------------------------------------------------------------------
// DDL builder
// ---------------------------------------------------------------------------

/// Build the `CREATE TABLE IF NOT EXISTS` DDL. Mirrors Python's
/// `_generate_create_table_ddl`: DUPLICATE KEY model, PK text types narrowed to
/// `VARCHAR(512)`, vector/inverted INDEX clauses inline, `DISTRIBUTED BY HASH`
/// over the PK, and a `replication_num` property.
fn create_table_sql(config: &DorisConfig, spec: &TableSpec) -> String {
    let schema = &spec.table_schema;
    let pk: std::collections::HashSet<&String> = schema.primary_key().iter().collect();
    let mut col_defs: Vec<String> = Vec::new();

    // Doris requires key (PK) columns to be an ordered prefix of the column
    // list, so emit them first (in primary-key order), then the rest.
    for name in schema.primary_key() {
        let col = &schema.columns()[name];
        let ty = key_column_type(&col.doris_type);
        col_defs.push(format!("    `{name}` {ty} NOT NULL"));
    }
    for (name, col) in schema.columns() {
        if pk.contains(name) {
            continue;
        }
        if col.is_vector {
            col_defs.push(format!("    `{name}` {} NOT NULL", col.doris_type));
        } else {
            let null = if col.nullable { "NULL" } else { "NOT NULL" };
            col_defs.push(format!("    `{name}` {} {null}", col.doris_type));
        }
    }

    for idx in &spec.vector_indexes {
        let idx_name = format!("idx_vec_{}", idx.field_name);
        let mut props = vec![
            format!("\"index_type\" = \"{}\"", idx.index_type.to_lowercase()),
            format!("\"metric_type\" = \"{}\"", idx.metric_type.to_lowercase()),
        ];
        if let Some(dim) = schema
            .columns()
            .get(&idx.field_name)
            .and_then(|c| c.vector_dimension)
        {
            props.push(format!("\"dim\" = \"{dim}\""));
        }
        if let Some(v) = idx.max_degree {
            props.push(format!("\"max_degree\" = \"{v}\""));
        }
        if let Some(v) = idx.ef_construction {
            props.push(format!("\"ef_construction\" = \"{v}\""));
        }
        if let Some(v) = idx.nlist {
            props.push(format!("\"nlist\" = \"{v}\""));
        }
        col_defs.push(format!(
            "    INDEX {idx_name} (`{}`) USING ANN PROPERTIES ({})",
            idx.field_name,
            props.join(", ")
        ));
    }

    for inv in &spec.inverted_indexes {
        let idx_name = format!("idx_inv_{}", inv.field_name);
        match &inv.parser {
            Some(parser) => col_defs.push(format!(
                "    INDEX {idx_name} (`{}`) USING INVERTED PROPERTIES (\"parser\" = \"{parser}\")",
                inv.field_name
            )),
            None => col_defs.push(format!(
                "    INDEX {idx_name} (`{}`) USING INVERTED",
                inv.field_name
            )),
        }
    }

    let pk_list = schema
        .primary_key()
        .iter()
        .map(|c| format!("`{c}`"))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "CREATE TABLE IF NOT EXISTS `{}`.`{}` (\n{}\n)\nENGINE = OLAP\nDUPLICATE KEY({pk_list})\nDISTRIBUTED BY HASH({pk_list}) BUCKETS {}\nPROPERTIES (\n    \"replication_num\" = \"{}\"\n)",
        config.database,
        spec.table_name,
        col_defs.join(",\n"),
        config.buckets_clause(),
        config.replication_num,
    )
}

/// PK columns can't be variable-length `TEXT`/`STRING` in Doris key columns;
/// narrow them to `VARCHAR(512)` (mirrors Python's `_convert_to_key_column_type`).
fn key_column_type(doris_type: &str) -> String {
    match doris_type.to_uppercase().as_str() {
        "TEXT" | "STRING" => "VARCHAR(512)".to_string(),
        _ => doris_type.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Value / key helpers
// ---------------------------------------------------------------------------

fn row_state<R: Serialize>(row: &R, schema: &TableSchema) -> Result<Map<String, JsonValue>> {
    let value = serde_json::to_value(row)
        .map_err(|e| Error::engine(format!("serialize Doris target row: {e}")))?;
    let JsonValue::Object(mut fields) = value else {
        return Err(Error::engine(
            "Doris target row must serialize to an object",
        ));
    };
    fields.retain(|name, _| schema.columns().contains_key(name));
    for name in schema.columns().keys() {
        fields.entry(name.clone()).or_insert(JsonValue::Null);
    }
    Ok(fields)
}

fn pk_values(fields: &Map<String, JsonValue>, pk_cols: &[String]) -> Result<Vec<JsonValue>> {
    pk_cols
        .iter()
        .map(|name| {
            fields
                .get(name)
                .cloned()
                .ok_or_else(|| Error::engine(format!("missing primary key column {name:?}")))
        })
        .collect()
}

fn pk_stable_key(fields: &Map<String, JsonValue>, primary_key: &[String]) -> Result<StableKey> {
    let mut parts = Vec::with_capacity(primary_key.len());
    for name in primary_key {
        let value = fields
            .get(name)
            .ok_or_else(|| Error::engine(format!("missing primary key column {name:?}")))?;
        parts.push(json_scalar_to_stable_key(value)?);
    }
    if parts.len() == 1 {
        Ok(parts.remove(0))
    } else {
        Ok(StableKey::Array(Arc::from(parts)))
    }
}

fn stable_key_to_pk(key: &StableKey) -> Result<Vec<JsonValue>> {
    match key {
        StableKey::Array(parts) => parts.iter().map(stable_key_to_json).collect(),
        other => Ok(vec![stable_key_to_json(other)?]),
    }
}

fn stable_key_to_json(key: &StableKey) -> Result<JsonValue> {
    match key {
        StableKey::Int(i) => Ok(JsonValue::from(*i)),
        StableKey::Str(s) | StableKey::Symbol(s) => Ok(JsonValue::from(s.to_string())),
        StableKey::Uuid(u) => Ok(JsonValue::from(u.to_string())),
        other => Err(Error::engine(format!(
            "unsupported Doris row key: {other:?}"
        ))),
    }
}

fn json_scalar_to_stable_key(value: &JsonValue) -> Result<StableKey> {
    match value {
        JsonValue::String(s) => Ok(StableKey::Str(Arc::from(s.clone()))),
        JsonValue::Number(n) => n
            .as_i64()
            .map(StableKey::Int)
            .ok_or_else(|| Error::engine(format!("unsupported numeric primary key: {n}"))),
        JsonValue::Bool(b) => Ok(StableKey::Str(Arc::from(b.to_string()))),
        JsonValue::Null => Err(Error::engine("primary key value cannot be null")),
        other => Err(Error::engine(format!(
            "primary key value must be scalar, got {other}"
        ))),
    }
}

/// Render a JSON scalar as a SQL literal for a `DELETE … WHERE` predicate.
/// Strings are single-quoted with `'` and `\` escaped (mirrors Python's
/// `val.replace("'", "\\'")`, plus backslash escaping for safety).
fn sql_value_literal(value: &JsonValue) -> String {
    match value {
        JsonValue::String(s) => {
            let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
            format!("'{escaped}'")
        }
        JsonValue::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Null => "NULL".to_string(),
        other => {
            let escaped = other.to_string().replace('\\', "\\\\").replace('\'', "\\'");
            format!("'{escaped}'")
        }
    }
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

fn validate_doris_type(value: &str) -> Result<()> {
    if value.is_empty()
        || !value.chars().all(|c| {
            c.is_ascii_alphanumeric()
                || matches!(c, '_' | '[' | ']' | '(' | ')' | ',' | '<' | '>' | ' ')
        })
    {
        return Err(Error::engine(format!("invalid Doris type: {value}")));
    }
    Ok(())
}

fn validate_indexes(schema: &TableSchema, options: &DorisTableOptions) -> Result<()> {
    for idx in &options.vector_indexes {
        validate_ident(&idx.field_name, "vector index field")?;
        if !schema.columns().contains_key(&idx.field_name) {
            return Err(Error::engine(format!(
                "vector index field {:?} is not in the table schema",
                idx.field_name
            )));
        }
    }
    for inv in &options.inverted_indexes {
        validate_ident(&inv.field_name, "inverted index field")?;
        if !schema.columns().contains_key(&inv.field_name) {
            return Err(Error::engine(format!(
                "inverted index field {:?} is not in the table schema",
                inv.field_name
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> DorisConfig {
        DorisConfig::new("localhost", "testdb")
    }

    fn schema() -> TableSchema {
        TableSchema::new(
            [
                ("id", ColumnDef::new("TEXT")),
                ("name", ColumnDef::new("TEXT")),
                ("value", ColumnDef::new("BIGINT")),
            ],
            ["id"],
        )
        .unwrap()
    }

    fn spec(table_schema: TableSchema) -> TableSpec {
        TableSpec {
            table_name: "items".to_string(),
            table_schema,
            managed_by: ManagedBy::System,
            vector_indexes: Vec::new(),
            inverted_indexes: Vec::new(),
        }
    }

    #[test]
    fn create_table_sql_uses_duplicate_key_and_narrows_text_pk() {
        let sql = create_table_sql(&config(), &spec(schema()));
        assert!(
            sql.contains("CREATE TABLE IF NOT EXISTS `testdb`.`items`"),
            "{sql}"
        );
        // The TEXT primary key is narrowed to VARCHAR(512) NOT NULL.
        assert!(sql.contains("`id` VARCHAR(512) NOT NULL"), "{sql}");
        // Non-PK TEXT stays TEXT and nullable.
        assert!(sql.contains("`name` TEXT NULL"), "{sql}");
        assert!(sql.contains("`value` BIGINT NULL"), "{sql}");
        assert!(sql.contains("DUPLICATE KEY(`id`)"), "{sql}");
        assert!(
            sql.contains("DISTRIBUTED BY HASH(`id`) BUCKETS AUTO"),
            "{sql}"
        );
        assert!(sql.contains("\"replication_num\" = \"1\""), "{sql}");
    }

    #[test]
    fn create_table_sql_emits_vector_and_inverted_index_clauses() {
        let schema = TableSchema::new(
            [
                ("id", ColumnDef::new("TEXT")),
                ("content", ColumnDef::new("TEXT")),
                ("embedding", ColumnDef::vector(4)),
            ],
            ["id"],
        )
        .unwrap();
        let mut s = spec(schema);
        s.vector_indexes = vec![VectorIndexDef::new("embedding").max_degree(32)];
        s.inverted_indexes = vec![InvertedIndexDef::new("content").parser("english")];
        let sql = create_table_sql(&config(), &s);
        assert!(sql.contains("`embedding` ARRAY<FLOAT> NOT NULL"), "{sql}");
        assert!(
            sql.contains(
                "INDEX idx_vec_embedding (`embedding`) USING ANN PROPERTIES (\"index_type\" = \"hnsw\", \"metric_type\" = \"l2_distance\", \"dim\" = \"4\", \"max_degree\" = \"32\")"
            ),
            "{sql}"
        );
        assert!(
            sql.contains(
                "INDEX idx_inv_content (`content`) USING INVERTED PROPERTIES (\"parser\" = \"english\")"
            ),
            "{sql}"
        );
    }

    #[test]
    fn delete_literal_escapes_quotes_and_backslashes() {
        assert_eq!(sql_value_literal(&JsonValue::from("a'b")), "'a\\'b'");
        assert_eq!(sql_value_literal(&JsonValue::from("a\\b")), "'a\\\\b'");
        assert_eq!(sql_value_literal(&JsonValue::from(7)), "7");
        assert_eq!(sql_value_literal(&JsonValue::from(true)), "1");
    }

    #[test]
    fn rewrite_redirect_replaces_host_keeps_port_and_path() {
        let got = rewrite_redirect(
            "http://10.0.0.5:8040/api/db/t/_stream_load",
            Some("localhost"),
        );
        assert_eq!(got, "http://localhost:8040/api/db/t/_stream_load");
        // Without be_load_host the location is unchanged.
        let same = rewrite_redirect("http://10.0.0.5:8040/x", None);
        assert_eq!(same, "http://10.0.0.5:8040/x");
    }

    #[test]
    fn buckets_clause_auto_is_case_insensitive() {
        assert_eq!(config().buckets("AuTo").buckets_clause(), "AUTO");
        assert_eq!(config().buckets("10").buckets_clause(), "10");
    }

    #[test]
    fn vector_column_is_not_null_array_float() {
        let col = ColumnDef::vector(8);
        assert_eq!(col.doris_type, "ARRAY<FLOAT>");
        assert!(!col.nullable);
        assert!(col.is_vector);
        assert_eq!(col.vector_dimension, Some(8));
    }

    #[test]
    fn schema_rejects_pk_not_in_columns() {
        let err = TableSchema::new([("id", ColumnDef::new("BIGINT"))], ["missing"]);
        assert!(err.is_err());
    }

    #[test]
    fn validate_doris_type_accepts_array_and_paren_types() {
        assert!(validate_doris_type("ARRAY<FLOAT>").is_ok());
        assert!(validate_doris_type("VARCHAR(255)").is_ok());
        assert!(validate_doris_type("DECIMAL(10, 2)").is_ok());
        assert!(validate_doris_type("DROP; TABLE").is_err());
    }
}
