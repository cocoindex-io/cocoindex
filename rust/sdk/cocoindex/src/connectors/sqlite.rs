//! SQLite table target connector.
//!
//! Table targets reconcile declared rows against the previous run: changed rows
//! are upserted, unchanged rows are skipped, and rows no longer declared are
//! deleted. `managed_by` controls whether CocoIndex owns table DDL.
//!
//! Use [`table_target`] to build a composable target state,
//! [`declare_table_target`] inside the current component, or
//! [`mount_table_target`] when rows must be declared immediately.
//!
//! `sqlite-vec` virtual tables are supported via [`Vec0TableDef`] (the table is
//! created with `CREATE VIRTUAL TABLE … USING vec0(…)`); this requires the
//! `vec0` extension at runtime, which [`Database::connect_with_vec`] loads.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use crate::ctx::{ContextKey, Ctx};
use crate::error::{Error, Result};
use crate::resources::schema::VectorSchemaProvider;
use crate::sql_ident::validate_ident;
use crate::statediff::{
    CompositeTrackingRecord, DiffAction, ManagedBy, MutualTrackingRecord, diff, diff_composite,
    resolve_system_transition,
};
use crate::target_state::{
    ChildTargetDef, STATIC_ROOT_PROVIDERS, SinkCache, StableKey, StaticRootProvider, TargetAction,
    TargetActionSink, TargetChildInvalidation, TargetHandler, TargetReconcileOutput, TargetState,
    TargetStateProvider, declare_target_state, declare_target_state_with_child, mount_target,
};

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// How long a connection waits for a lock held by another connection or
/// process before failing (Python's `connect(timeout=5.0)` default).
pub const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Options for [`Database::connect_with_options`] (Python's `connect(database,
/// timeout=…, load_vec=…)`).
#[derive(Clone, Debug)]
pub struct ConnectOptions {
    /// How long to wait for a lock held by another connection or process.
    pub busy_timeout: Duration,
    /// The `sqlite-vec` extension to load so [`Vec0TableDef`] tables work:
    /// the loadable library as SQLite's extension loader takes it, a path with
    /// or without the platform suffix, or a bare name such as `"vec0"` to
    /// search the system library path. Connecting fails when it cannot be
    /// loaded. Python finds the library inside its `sqlite-vec` package; a
    /// Rust build has no such package, so the caller names the library.
    pub vec_extension: Option<String>,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            busy_timeout: DEFAULT_BUSY_TIMEOUT,
            vec_extension: None,
        }
    }
}

/// A SQLite connection pool. Clone-cheap (the underlying pool is shared).
///
/// SQLite is a single-writer engine, so the pool holds one connection and
/// serializes the statements run on it, as Python's `ManagedConnection` lock
/// does. For your own SQL on the same database, `pool().begin()` gives a
/// transaction (Python's `transaction()`), and `pool()` runs a read directly
/// (Python's `readonly()`).
#[derive(Clone)]
pub struct Database {
    pool: SqlitePool,
    state_id: Arc<str>,
}

/// Shows the handle's identity only, never credentials.
impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("state_id", &self.state_id())
            .finish_non_exhaustive()
    }
}

impl Database {
    /// Open (creating if missing) a SQLite database at `path` with the default
    /// [`ConnectOptions`].
    pub async fn connect(path: &str) -> Result<Self> {
        Self::connect_with_options(path, ConnectOptions::default()).await
    }

    /// [`connect`](Self::connect), loading the `sqlite-vec` extension named by
    /// `vec_extension` (see [`ConnectOptions::vec_extension`]; Python's
    /// `connect(database, load_vec=True)`).
    pub async fn connect_with_vec(path: &str, vec_extension: &str) -> Result<Self> {
        Self::connect_with_options(
            path,
            ConnectOptions {
                vec_extension: Some(vec_extension.to_string()),
                ..ConnectOptions::default()
            },
        )
        .await
    }

    /// Open (creating if missing) a SQLite database at `path` with explicit
    /// [`ConnectOptions`].
    pub async fn connect_with_options(path: &str, options: ConnectOptions) -> Result<Self> {
        let mut connect_options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .busy_timeout(options.busy_timeout);
        if let Some(extension) = options.vec_extension {
            connect_options = connect_options.extension(extension);
        }
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .map_err(sqlite_err)?;
        Ok(Self {
            pool,
            state_id: Arc::from(path),
        })
    }

    pub fn from_pool(state_id: impl Into<String>, pool: SqlitePool) -> Self {
        Self {
            pool,
            state_id: Arc::from(state_id.into()),
        }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub fn state_id(&self) -> &str {
        &self.state_id
    }
}

fn sqlite_err(e: sqlx::Error) -> Error {
    Error::engine(format!("sqlite: {e}"))
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnDef {
    pub sqlite_type: String,
    pub nullable: bool,
}

impl ColumnDef {
    pub fn new(sqlite_type: impl Into<String>) -> Self {
        Self {
            sqlite_type: sqlite_type.into(),
            nullable: true,
        }
    }

    /// Mark the column `NOT NULL`.
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
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
            validate_sqlite_type(&def.sqlite_type)?;
            if out.insert(name.clone(), def).is_some() {
                return Err(Error::engine(format!(
                    "SQLite table schema contains duplicate column {name:?}"
                )));
            }
        }
        let primary_key: Vec<String> = primary_key.into_iter().map(Into::into).collect();
        if primary_key.is_empty() {
            return Err(Error::engine("SQLite table primary key cannot be empty"));
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
    /// analogue of Python's `TableSchema.from_class`). Each field maps to a
    /// SQLite column via the same leaf-type table as Python's `sqlite`
    /// `from_class`. A `#[coco(vector = N)]` field becomes a `float[N]` column
    /// (for `sqlite-vec` / [`Vec0TableDef`]).
    pub fn from_row<T: crate::row_schema::SchemaFields>(
        primary_key: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let columns = T::schema_fields()
            .into_iter()
            .map(|f| (f.name.clone(), sqlite_column_def(&f)));
        Self::new(columns, primary_key)
    }

    /// Resolve a vector field derived from a row from a
    /// [`VectorSchemaProvider`], typically the embedder that fills it (Python's
    /// `column_overrides={"embedding": embedder}`). `sqlite-vec` columns are
    /// `float[N]` whatever the element type. For an embedder held in the
    /// context, pass `ctx.get_key(&EMBEDDER)?`.
    pub async fn with_vector_schema(
        self,
        field_name: &str,
        provider: &(impl VectorSchemaProvider + ?Sized),
    ) -> Result<Self> {
        let schema =
            crate::row_schema::provided_vector_schema("SQLite", field_name, provider).await?;
        self.with_vector_dim(field_name, schema.size)
    }

    /// Replace the SQLite type of one column of a schema derived from a row
    /// (Python's `column_overrides={"col": SqliteType("…")}`), keeping its
    /// nullability.
    pub fn with_column_type(
        mut self,
        field_name: &str,
        sqlite_type: impl Into<String>,
    ) -> Result<Self> {
        let sqlite_type = sqlite_type.into();
        validate_sqlite_type(&sqlite_type)?;
        let def = self
            .columns
            .get_mut(field_name)
            .ok_or_else(|| crate::row_schema::unknown_column_error("SQLite", field_name))?;
        def.sqlite_type = sqlite_type;
        Ok(self)
    }

    /// Resolve or override the dimension of a vector field derived from a row.
    pub fn with_vector_dim(mut self, field_name: &str, dim: usize) -> Result<Self> {
        if dim == 0 {
            return Err(crate::row_schema::zero_vector_dimension_error(
                "SQLite", field_name,
            ));
        }
        let def = self
            .columns
            .get_mut(field_name)
            .ok_or_else(|| crate::row_schema::unknown_vector_field_error("SQLite", field_name))?;
        if !(def.sqlite_type.starts_with("float[") && def.sqlite_type.ends_with(']')) {
            return Err(crate::row_schema::not_vector_field_error(
                "SQLite", field_name,
            ));
        }
        def.sqlite_type = format!("float[{dim}]");
        Ok(self)
    }

    fn validate_vector_dimensions(&self) -> Result<()> {
        for (name, def) in &self.columns {
            if def.sqlite_type == "float[0]" {
                crate::row_schema::require_resolved_vector_dimension("SQLite", name)?;
            }
        }
        Ok(())
    }
}

/// Map a connector-agnostic [`SchemaField`](crate::row_schema::SchemaField) to a
/// SQLite [`ColumnDef`], mirroring Python's `sqlite` `_LEAF_TYPE_MAPPINGS`.
fn sqlite_column_def(field: &crate::row_schema::SchemaField) -> ColumnDef {
    use crate::row_schema::LogicalType as L;
    let sqlite_type = match &field.logical_type {
        L::Bool | L::Int16 | L::Int32 | L::Int64 => "INTEGER".to_string(),
        L::Float32 | L::Float64 | L::Duration => "REAL".to_string(),
        L::Decimal | L::Text | L::Uuid | L::Date | L::Time | L::DateTime | L::Json => {
            "TEXT".to_string()
        }
        L::Bytes => "BLOB".to_string(),
        // `sqlite-vec` vector columns are `float[N]`.
        L::Vector { dim, .. } => format!("float[{dim}]"),
        L::Custom(s) => s.clone(),
    };
    let mut def = ColumnDef::new(sqlite_type);
    def.nullable = field.nullable;
    def
}

/// `sqlite-vec` virtual-table configuration. When present, the table is created
/// as `CREATE VIRTUAL TABLE … USING vec0(…)`; the primary key must be a single
/// `INTEGER` column and at least one `float[N]` (vector) column is required.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Vec0TableDef {
    /// Columns used as vec0 partition keys (sharding).
    pub partition_key_columns: Vec<String>,
    /// Auxiliary columns (stored, prefixed with `+`, not used in KNN filtering).
    pub auxiliary_columns: Vec<String>,
}

/// Options for the `*_with_options` table-target constructors.
#[derive(Clone, Debug, Default)]
pub struct SqliteTableOptions {
    pub managed_by: ManagedBy,
    /// When set, create a `vec0` virtual table instead of a regular table.
    pub virtual_table_def: Option<Vec0TableDef>,
}

// ---------------------------------------------------------------------------
// Public target API: constructor / declaration / mount split
// ---------------------------------------------------------------------------

/// A declarative SQLite table target — a handle to declare rows on.
///
/// As a `mount_each!` / `use_mount!` argument it fingerprints by its table name
/// (its stable key); the schema and runtime provider are not part of the
/// component-memo identity.
#[derive(Clone, Serialize)]
pub struct TableTarget {
    table_name: Arc<str>,
    #[serde(skip)]
    table_schema: TableSchema,
    #[serde(skip)]
    rows: TargetStateProvider<RowState>,
}

/// Build a composable [`TargetState`] for a SQLite table (the spec
/// constructor). System-managed regular table.
pub fn table_target(
    ctx: &Ctx,
    db: &ContextKey<Database>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
) -> Result<TargetState<TableSpec>> {
    table_target_with_options(
        ctx,
        db,
        table_name,
        table_schema,
        SqliteTableOptions::default(),
    )
}

/// [`table_target`] with explicit [`SqliteTableOptions`] (`managed_by`,
/// `virtual_table_def`).
pub fn table_target_with_options(
    ctx: &Ctx,
    db: &ContextKey<Database>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    options: SqliteTableOptions,
) -> Result<TargetState<TableSpec>> {
    let table_name = table_name.into();
    validate_ident(&table_name, "table name")?;
    table_schema.validate_vector_dimensions()?;
    if let Some(def) = &options.virtual_table_def {
        validate_vec0(&table_name, &table_schema, def)?;
    }
    let provider = TABLE_PROVIDER.get(ctx)?;
    Ok(provider.target_state(
        // Stable connection identity = the ContextKey's name (`db_key`), never
        // live connection details; the pool is resolved at apply time from the
        // host context (see `design_connectors.md` §5.5).
        TableKey {
            db_key: db.name().to_string(),
            table_name: table_name.clone(),
        }
        .stable_key(),
        TableSpec {
            table_name,
            table_schema,
            virtual_table_def: options.virtual_table_def,
            managed_by: options.managed_by,
        },
    ))
}

/// Declare a SQLite table target in the **current** component (the row child
/// provider resolves at this component's commit) and return a handle.
pub fn declare_table_target(
    ctx: &Ctx,
    db: &ContextKey<Database>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
) -> Result<TableTarget> {
    declare_table_target_with_options(
        ctx,
        db,
        table_name,
        table_schema,
        SqliteTableOptions::default(),
    )
}

/// [`declare_table_target`] with explicit [`SqliteTableOptions`].
pub fn declare_table_target_with_options(
    ctx: &Ctx,
    db: &ContextKey<Database>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    options: SqliteTableOptions,
) -> Result<TableTarget> {
    let ts = table_target_with_options(ctx, db, table_name, table_schema, options)?;
    let spec = ts.value().clone();
    let rows = declare_target_state_with_child::<TableSpec, RowState>(ctx, ts)?;
    Ok(table_target_handle(spec, rows))
}

/// Mount a SQLite table target **foreground** (rows can be declared
/// immediately) and return a handle. System-managed regular table.
pub async fn mount_table_target(
    ctx: &Ctx,
    db: &ContextKey<Database>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
) -> Result<TableTarget> {
    mount_table_target_with_options(
        ctx,
        db,
        table_name,
        table_schema,
        SqliteTableOptions::default(),
    )
    .await
}

/// [`mount_table_target`] with explicit [`SqliteTableOptions`].
pub async fn mount_table_target_with_options(
    ctx: &Ctx,
    db: &ContextKey<Database>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    options: SqliteTableOptions,
) -> Result<TableTarget> {
    let ts = table_target_with_options(ctx, db, table_name, table_schema, options)?;
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

    /// Declare a row. `row` serializes to an object whose entries are the
    /// table's columns (extra entries are ignored, missing ones are NULL); a
    /// value is bound by its column's declared type.
    ///
    /// Python's `ColumnDef.encoder` converts a field's value before it is
    /// bound (a `Decimal` or `UUID` to text, a `datetime` to ISO 8601, a
    /// collection to JSON). Here the row's `Serialize` impl does that: the
    /// defaults of `uuid`, `chrono` and `rust_decimal` serialize to the text
    /// these columns take, a collection serializes to JSON text, and a field
    /// needing another form uses `#[serde(serialize_with = "…")]`.
    pub fn declare_row<R: Serialize>(&self, ctx: &Ctx, row: &R) -> Result<()> {
        let fields = row_state(row, &self.table_schema)?;
        let key = pk_stable_key(&fields, self.table_schema.primary_key())?;
        declare_target_state(ctx, self.rows.target_state(key, RowState { fields }))
    }
}

// ---------------------------------------------------------------------------
// Internal specs / actions
// ---------------------------------------------------------------------------

/// Spec for a SQLite table (the declared container value).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSpec {
    table_name: String,
    table_schema: TableSchema,
    virtual_table_def: Option<Vec0TableDef>,
    #[serde(default)]
    managed_by: ManagedBy,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RowState {
    fields: Map<String, JsonValue>,
}

// ---------------------------------------------------------------------------
// Composite schema tracking (mirrors Python `connectors/sqlite/_target.py`)
//
// A table's tracking record is split into a `main` record (table name + PK
// columns + virtual-table config) and one `sub` record per non-PK column
// (type + nullability). `diff_composite` then tells a structural change that
// requires DROP+CREATE (main changed) apart from an incremental
// `ALTER TABLE ADD/DROP COLUMN` (main unchanged, individual subs changed).
// ---------------------------------------------------------------------------

/// Sub-key prefix for a non-PK column's tracking record. Mirrors Python's
/// `_COL_SUBKEY_PREFIX`.
const COL_SUBKEY_PREFIX: &str = "col:";

fn col_subkey(col_name: &str) -> String {
    format!("{COL_SUBKEY_PREFIX}{col_name}")
}

/// The `main` half of a table's composite tracking record. A change here (PK
/// columns, their types, or the virtual-table config) forces a full table
/// rewrite. The table identity is the target-state key ([`TableKey`]).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TablePrimaryTrackingRecord {
    primary_key_columns: Vec<PkColumnInfo>,
    virtual_table_def: Option<Vec0TableDef>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct PkColumnInfo {
    name: String,
    sqlite_type: String,
}

/// The `sub` half: one per non-PK column.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NonPkColumnTrackingRecord {
    sqlite_type: String,
    nullable: bool,
}

type TableCompositeRecord =
    CompositeTrackingRecord<TablePrimaryTrackingRecord, String, NonPkColumnTrackingRecord>;

type TableTrackingRecord = MutualTrackingRecord<TableCompositeRecord>;

fn table_composite_record(spec: &TableSpec) -> TableCompositeRecord {
    let schema = &spec.table_schema;
    let pk: std::collections::HashSet<&String> = schema.primary_key().iter().collect();
    let main = TablePrimaryTrackingRecord {
        primary_key_columns: schema
            .primary_key()
            .iter()
            .map(|name| PkColumnInfo {
                name: name.clone(),
                sqlite_type: schema.columns()[name].sqlite_type.clone(),
            })
            .collect(),
        virtual_table_def: spec.virtual_table_def.clone(),
    };
    let sub: HashMap<String, NonPkColumnTrackingRecord> = schema
        .columns()
        .iter()
        .filter(|(name, _)| !pk.contains(*name))
        .map(|(name, col)| {
            (
                col_subkey(name),
                NonPkColumnTrackingRecord {
                    sqlite_type: col.sqlite_type.clone(),
                    nullable: col.nullable,
                },
            )
        })
        .collect();
    CompositeTrackingRecord::new(main, sub)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TableAction {
    /// The `ContextKey` name of the database the table lives in.
    db_key: String,
    table_name: String,
    /// `Some` for a create/update (carrying the desired spec); `None` for a
    /// drop of the orphaned table.
    spec: Option<TableSpec>,
    /// Structural action for the table itself (`None` = no structural change,
    /// reconcile columns incrementally instead).
    main_action: Option<DiffAction>,
    /// Per-column actions (keyed by `col:<name>`), applied only when
    /// `main_action` is `None`.
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

#[linkme::distributed_slice(STATIC_ROOT_PROVIDERS)]
static TABLE_PROVIDER: StaticRootProvider =
    StaticRootProvider::new::<TableSpec, TableHandler>("cocoindex/sqlite/table");

/// A table target state's key: the database's `ContextKey` name plus the table
/// name. Mirrors Python's `_TableKey`.
struct TableKey {
    db_key: String,
    table_name: String,
}

impl TableKey {
    fn stable_key(self) -> StableKey {
        StableKey::Array(Arc::from([
            StableKey::Str(Arc::from(self.db_key)),
            StableKey::Str(Arc::from(self.table_name)),
        ]))
    }

    fn from_stable_key(key: &StableKey) -> Result<Self> {
        if let StableKey::Array(parts) = key
            && let [StableKey::Str(db_key), StableKey::Str(table_name)] = &parts[..]
        {
            return Ok(Self {
                db_key: db_key.to_string(),
                table_name: table_name.to_string(),
            });
        }
        Err(Error::engine(format!(
            "unexpected sqlite table target key: {key:?}"
        )))
    }
}

/// Root handler for every SQLite table target: stateless, the database and
/// table are named by each target state's [`TableKey`]; the live pool is
/// resolved from the host context at apply time.
#[derive(Default)]
struct TableHandler {
    sink: SinkCache<TableAction>,
}

impl TargetHandler<TableSpec> for TableHandler {
    type TrackingRecord = TableTrackingRecord;
    type Action = TableAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<TableSpec>,
        prev: Vec<TableTrackingRecord>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<TableAction, TableTrackingRecord>>> {
        let TableKey { db_key, table_name } = TableKey::from_stable_key(&key)?;
        match desired {
            // Always emit when declared so the sink builds the row child.
            Some(spec) => {
                let tracking =
                    MutualTrackingRecord::new(table_composite_record(&spec), spec.managed_by);
                let resolved =
                    resolve_system_transition(Some(tracking.clone()), prev, prev_may_be_missing);
                // Split the diff into a structural (main) action plus per-column
                // transitions. `insert` / `replace` build the table from the
                // desired schema, so every column is already correct. `upsert`
                // means the table may or may not exist: `CREATE TABLE IF NOT
                // EXISTS` can land on a table carrying the previous column set,
                // so its columns still need reconciling.
                let (main_action, column_transitions) = diff_composite(resolved.as_ref());
                let mut column_actions = BTreeMap::new();
                if matches!(main_action, None | Some(DiffAction::Upsert)) {
                    for (sub_key, transition) in &column_transitions {
                        if let Some(action) = diff(Some(transition)) {
                            column_actions.insert(sub_key.clone(), action);
                        }
                    }
                }

                // Mirror Python's child-invalidation contract:
                //   - main replace  → table dropped & recreated → destructive.
                //   - column change other than a pure add → may lose data → lossy.
                let child_invalidation = if matches!(main_action, Some(DiffAction::Replace)) {
                    Some(TargetChildInvalidation::Destructive)
                } else if column_actions
                    .values()
                    .any(|a| !matches!(a, DiffAction::Insert))
                {
                    Some(TargetChildInvalidation::Lossy)
                } else {
                    None
                };

                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Update(TableAction {
                        db_key,
                        table_name,
                        spec: Some(spec),
                        main_action,
                        column_actions,
                    }),
                    sink: self.sink.get(table_sink),
                    tracking_record: Some(tracking),
                    child_invalidation,
                }))
            }
            None => {
                let has_system_prev = prev.iter().any(|v| v.managed_by.is_system());
                let resolved = resolve_system_transition(None, prev, prev_may_be_missing);
                if resolved.is_none() || !has_system_prev {
                    return Ok(None);
                }
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Delete(TableAction {
                        db_key,
                        table_name,
                        spec: None,
                        main_action: Some(DiffAction::Delete),
                        column_actions: BTreeMap::new(),
                    }),
                    sink: self.sink.get(table_sink),
                    tracking_record: None,
                    child_invalidation: Some(TargetChildInvalidation::Destructive),
                }))
            }
        }
    }
}

fn table_sink() -> TargetActionSink<TableAction> {
    TargetActionSink::from_async_fn_with_child_handler_ctx(
        move |host_ctx, actions: Vec<TargetAction<TableAction>>| async move {
            for action in actions {
                let (TargetAction::Create(a) | TargetAction::Update(a) | TargetAction::Delete(a)) =
                    action;
                let db = resolve_db(&host_ctx, &a.db_key)?;
                apply_table_action(&db, a).await?;
            }
            Ok(())
        },
        |a: &TableAction| {
            let spec = a
                .spec
                .clone()
                .ok_or_else(|| Error::engine("SQLite table action missing spec"))?;
            Ok(ChildTargetDef::new::<RowState, _>(RowHandler {
                sink: SinkCache::default(),
                db_key: a.db_key.clone(),
                spec,
            }))
        },
    )
}

/// Resolve the live SQLite [`Database`] from the host context by its stable
/// `db_key` (the `ContextKey` name it was provided under).
fn resolve_db(
    host_ctx: &std::sync::Arc<crate::ctx::ContextStore>,
    db_key: &str,
) -> Result<std::sync::Arc<Database>> {
    host_ctx.resolve::<Database>(db_key).ok_or_else(|| {
        Error::engine(format!(
            "sqlite target: database `{db_key}` was not provided in the environment \
             (call Environment::builder().provide_key(&KEY, db))"
        ))
    })
}

/// Apply one resolved table action. Mirrors Python's `_apply_table_actions`
/// decision tree.
async fn apply_table_action(db: &Database, action: TableAction) -> Result<()> {
    let TableAction {
        db_key: _,
        table_name,
        spec,
        mut main_action,
        mut column_actions,
    } = action;

    // Virtual (vec0) tables can't use ALTER TABLE — any column change is
    // upgraded to a full DROP+CREATE, matching Python.
    let is_virtual = spec.as_ref().is_some_and(|s| s.virtual_table_def.is_some());
    if is_virtual
        && matches!(main_action, None | Some(DiffAction::Upsert))
        && !column_actions.is_empty()
    {
        main_action = Some(DiffAction::Replace);
        column_actions.clear();
    }

    // A structural rewrite or a drop removes the existing table first.
    if matches!(
        main_action,
        Some(DiffAction::Replace) | Some(DiffAction::Delete)
    ) {
        drop_table(db, &table_name).await?;
    }

    let Some(spec) = spec else {
        // Pure drop.
        return Ok(());
    };

    if matches!(
        main_action,
        Some(DiffAction::Insert | DiffAction::Upsert | DiffAction::Replace)
    ) {
        // (Re)create the whole table. `create_table` uses `IF NOT EXISTS`,
        // so an `upsert` against an already-present table is a no-op — which
        // is exactly why an `upsert` still falls through to the column
        // reconcile below.
        create_table(db, &spec).await?;
    }
    if matches!(main_action, None | Some(DiffAction::Upsert)) && !column_actions.is_empty() {
        // Reconcile non-PK columns incrementally, preserving existing rows.
        // (Virtual tables never reach here — upgraded to Replace above.)
        apply_column_actions(db, &spec, &column_actions).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Row handler (child) + sink
// ---------------------------------------------------------------------------

struct RowHandler {
    sink: SinkCache<RowAction>,
    db_key: String,
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
            sink: self.sink.get(|| self.row_sink()),
            tracking_record: desired_fp,
            child_invalidation: None,
        }))
    }
}

impl RowHandler {
    fn row_sink(&self) -> TargetActionSink<RowAction> {
        let db_key = self.db_key.clone();
        let spec = Arc::new(self.spec.clone());
        let statements = Arc::new(RowStatements::new(&self.spec));
        let table_created = Arc::new(tokio::sync::OnceCell::new());
        TargetActionSink::from_async_fn_with_ctx(
            move |host_ctx, actions: Vec<TargetAction<RowAction>>| {
                let db_key = db_key.clone();
                let spec = spec.clone();
                let statements = statements.clone();
                let table_created = table_created.clone();
                async move {
                    let db = resolve_db(&host_ctx, &db_key)?;
                    let mut mutations = Vec::with_capacity(actions.len());
                    for action in actions {
                        let row = match action {
                            TargetAction::Create(r)
                            | TargetAction::Update(r)
                            | TargetAction::Delete(r) => r,
                        };
                        mutations.push((row.pk, row.state));
                    }
                    if mutations.is_empty() {
                        return Ok(());
                    }
                    // Ensure the table exists before this sink's first row
                    // mutation (once per sink, not per batch). This keeps a
                    // delete-only reconciliation idempotent after a crash or
                    // an external table cleanup.
                    if spec.managed_by.is_system() {
                        table_created
                            .get_or_try_init(|| create_table(&db, &spec))
                            .await?;
                    }
                    apply_rows(&db, &statements, mutations).await
                }
            },
        )
    }
}

// ---------------------------------------------------------------------------
// DB I/O
// ---------------------------------------------------------------------------

async fn create_table(db: &Database, spec: &TableSpec) -> Result<()> {
    if spec.managed_by.is_user() {
        return Ok(());
    }
    let sql = match &spec.virtual_table_def {
        Some(def) => create_vec0_sql(spec, def),
        None => create_table_sql(spec),
    };
    sqlx::query(&sql)
        .execute(db.pool())
        .await
        .map_err(sqlite_err)?;
    Ok(())
}

async fn drop_table(db: &Database, table_name: &str) -> Result<()> {
    validate_ident(table_name, "table name")?;
    sqlx::query(&format!("DROP TABLE IF EXISTS {}", quote_ident(table_name)))
        .execute(db.pool())
        .await
        .map_err(sqlite_err)?;
    Ok(())
}

/// Apply per-column changes to an existing table via `ALTER TABLE`, preserving
/// rows. Mirrors Python's `_apply_column_actions`:
///   - `insert` / `upsert` → `ALTER TABLE … ADD COLUMN`
///   - `delete`            → `ALTER TABLE … DROP COLUMN` (SQLite ≥ 3.35)
///   - `replace`           → `DROP COLUMN` then `ADD COLUMN` (no in-place
///     type change in SQLite)
///
/// Statements that SQLite can't honor (e.g. `DROP COLUMN` on old builds, or a
/// re-`ADD` of an existing column on an `upsert`) are tolerated — like Python's
/// per-statement `OperationalError` swallow — so a best-effort schema sync
/// doesn't abort the whole reconcile. PK columns are never altered here.
async fn apply_column_actions(
    db: &Database,
    spec: &TableSpec,
    column_actions: &BTreeMap<String, DiffAction>,
) -> Result<()> {
    // User-managed tables: never touch DDL, only rows.
    if spec.managed_by.is_user() {
        return Ok(());
    }
    let table = quote_ident(&spec.table_name);
    let schema = &spec.table_schema;
    let pk: std::collections::HashSet<&str> =
        schema.primary_key().iter().map(String::as_str).collect();

    for (sub_key, action) in column_actions {
        let Some(col_name) = sub_key.strip_prefix(COL_SUBKEY_PREFIX) else {
            return Err(Error::engine(format!(
                "SQLite column action has unexpected sub-key {sub_key:?}"
            )));
        };
        // Defensive: PK columns belong to the `main` record and are handled by
        // DROP+CREATE, never by ALTER.
        if pk.contains(col_name) {
            continue;
        }
        let col_ident = quote_ident(col_name);

        match action {
            DiffAction::Delete => {
                // A removed column that cannot be dropped (an index references
                // it, or SQLite < 3.35) is left in place as unused.
                let sql = format!("ALTER TABLE {table} DROP COLUMN {col_ident}");
                if let Err(e) = sqlx::query(&sql).execute(db.pool()).await {
                    tracing::warn!("SQLite target: could not drop removed column ({sql:?}): {e}");
                }
            }
            DiffAction::Insert | DiffAction::Upsert => {
                if let Some(col) = schema.columns().get(col_name) {
                    // Add the column as NULLABLE even when the schema declares it
                    // NOT NULL: SQLite rejects `ADD COLUMN … NOT NULL` on a table
                    // that already has rows (no default to backfill), which would
                    // fail here and leave the table missing a column that
                    // `upsert_sql` then references. This mirrors the Postgres
                    // connector, which also adds columns nullable. The NOT NULL
                    // constraint is only applied on a full table (re)create.
                    add_column(db, &table, &col_ident, &col.sqlite_type).await?;
                }
            }
            DiffAction::Replace => {
                // SQLite has no `ALTER COLUMN TYPE`; drop then re-add. Re-add as
                // nullable for the same reason as the Insert/Upsert arm (a NOT
                // NULL add fails on a populated table). A failed drop would
                // leave the old type while the new one is recorded, so it fails
                // the apply instead, and the change is retried on the next run.
                if let Some(col) = schema.columns().get(col_name) {
                    let sql = format!("ALTER TABLE {table} DROP COLUMN {col_ident}");
                    sqlx::query(&sql).execute(db.pool()).await.map_err(|e| {
                        Error::engine(format!(
                            "SQLite target: cannot change the type of column {col_name:?} \
                             in {table} ({sql:?}): {e}"
                        ))
                    })?;
                    add_column(db, &table, &col_ident, &col.sqlite_type).await?;
                }
            }
        }
    }
    Ok(())
}

/// Add a column, tolerating only the case where it already exists (an
/// earlier, interrupted apply added it).
async fn add_column(db: &Database, table: &str, col_ident: &str, sqlite_type: &str) -> Result<()> {
    let sql = format!("ALTER TABLE {table} ADD COLUMN {col_ident} {sqlite_type}");
    match sqlx::query(&sql).execute(db.pool()).await {
        Ok(_) => Ok(()),
        Err(e) if e.to_string().contains("duplicate column name") => Ok(()),
        Err(e) => Err(Error::engine(format!("SQLite target: {sql:?} failed: {e}"))),
    }
}

/// SQLite's default `SQLITE_MAX_VARIABLE_NUMBER` since 3.32 (the build `sqlx`
/// bundles); a statement binds at most this many parameters.
const BIND_LIMIT: usize = 32766;

/// Apply a batch of row mutations in one transaction: upserts through
/// multi-row `VALUES` statements of as many rows as the bind limit allows (one
/// `DELETE` + `INSERT` per row for a `vec0` table, which has no UPSERT), then
/// deletes through `IN` lists of keys (one `DELETE` per row for a `vec0`
/// table).
async fn apply_rows(
    db: &Database,
    statements: &RowStatements,
    mutations: Vec<(Vec<JsonValue>, Option<RowState>)>,
) -> Result<()> {
    let mut upserts = Vec::new();
    let mut deletes = Vec::new();
    for (pk, state) in mutations {
        match state {
            Some(state) => upserts.push(state.fields),
            None => deletes.push(pk),
        }
    }
    let mut tx = db.pool().begin().await.map_err(sqlite_err)?;
    if statements.virtual_table {
        for row in &upserts {
            let key = statements.row_key(row)?;
            execute_with_values(&mut tx, &statements.delete_sql(1), key).await?;
            execute_with_values(
                &mut tx,
                &statements.upsert_sql(1),
                statements.row_values(row)?,
            )
            .await?;
        }
        for key in &deletes {
            execute_with_values(
                &mut tx,
                &statements.delete_sql(1),
                statements.key_values(key)?,
            )
            .await?;
        }
    } else {
        for chunk in upserts.chunks(statements.upsert_rows_per_statement()) {
            let mut values = Vec::with_capacity(chunk.len() * statements.columns.len());
            for row in chunk {
                values.extend(statements.row_values(row)?);
            }
            execute_with_values(&mut tx, &statements.upsert_sql(chunk.len()), values).await?;
        }
        for chunk in deletes.chunks(statements.delete_rows_per_statement()) {
            let mut values = Vec::with_capacity(chunk.len() * statements.pk_columns.len());
            for key in chunk {
                values.extend(statements.key_values(key)?);
            }
            execute_with_values(&mut tx, &statements.delete_sql(chunk.len()), values).await?;
        }
    }
    tx.commit().await.map_err(sqlite_err)?;
    Ok(())
}

/// Run `sql` with `values` bound to its `?` placeholders, in order.
async fn execute_with_values(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    sql: &str,
    values: Vec<SqliteValue>,
) -> Result<()> {
    let mut query = sqlx::query(sql);
    for value in values {
        query = match value {
            SqliteValue::Null => query.bind(None::<i64>),
            SqliteValue::Integer(i) => query.bind(i),
            SqliteValue::Real(r) => query.bind(r),
            SqliteValue::Text(t) => query.bind(t),
            SqliteValue::Blob(b) => query.bind(b),
        };
    }
    query.execute(&mut **tx).await.map_err(sqlite_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// DDL / SQL builders
// ---------------------------------------------------------------------------

fn create_table_sql(spec: &TableSpec) -> String {
    let mut cols = Vec::new();
    for (name, col) in spec.table_schema.columns() {
        // PK columns are always NOT NULL (matches Python's `_create_table`,
        // which forces NOT NULL on primary-key columns regardless of the
        // declared nullability — a SQLite non-rowid PK otherwise permits NULL).
        let is_pk = spec.table_schema.primary_key().contains(name);
        let null = if col.nullable && !is_pk {
            ""
        } else {
            " NOT NULL"
        };
        cols.push(format!("{} {}{}", quote_ident(name), col.sqlite_type, null));
    }
    let pk = spec
        .table_schema
        .primary_key()
        .iter()
        .map(|name| quote_ident(name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({}, PRIMARY KEY ({}))",
        quote_ident(&spec.table_name),
        cols.join(", "),
        pk
    )
}

fn create_vec0_sql(spec: &TableSpec, def: &Vec0TableDef) -> String {
    let pk = &spec.table_schema.primary_key()[0];
    let mut cols = Vec::new();

    cols.push(format!("{pk} integer primary key"));

    for (name, col) in spec.table_schema.columns() {
        if name != pk
            && !def.partition_key_columns.iter().any(|c| c == name)
            && !def.auxiliary_columns.iter().any(|c| c == name)
        {
            cols.push(format!("{name} {}", col.sqlite_type));
        }
    }
    for name in &def.partition_key_columns {
        let col = &spec.table_schema.columns()[name];
        cols.push(format!("{name} {} partition key", col.sqlite_type));
    }
    for name in &def.auxiliary_columns {
        let col = &spec.table_schema.columns()[name];
        cols.push(format!("+{name} {}", col.sqlite_type));
    }
    format!(
        "CREATE VIRTUAL TABLE IF NOT EXISTS {} USING vec0({})",
        quote_ident(&spec.table_name),
        cols.join(", ")
    )
}

/// The row statements of one table, built once per row sink. Values are bound
/// as `?` parameters, one per value, so a statement's row count is bounded by
/// [`BIND_LIMIT`].
struct RowStatements {
    table: String,
    /// Whether the table is a `vec0` virtual table (no UPSERT: one `DELETE` +
    /// `INSERT` per row).
    virtual_table: bool,
    /// Every column, in statement order.
    columns: Vec<(String, ColumnDef)>,
    /// The primary-key columns, in key order.
    pk_columns: Vec<(String, ColumnDef)>,
    /// `INSERT INTO <table> (<columns>) VALUES `
    insert_prefix: String,
    /// `(?, ?, …)` for one row.
    row_placeholders: String,
    /// `ON CONFLICT (<pk>) DO UPDATE SET …` (empty for a `vec0` table).
    conflict_clause: String,
}

impl RowStatements {
    fn new(spec: &TableSpec) -> Self {
        let schema = &spec.table_schema;
        let columns: Vec<(String, ColumnDef)> = schema
            .columns()
            .iter()
            .map(|(name, col)| (name.clone(), col.clone()))
            .collect();
        let pk_columns: Vec<(String, ColumnDef)> = schema
            .primary_key()
            .iter()
            .map(|name| (name.clone(), schema.columns()[name].clone()))
            .collect();
        let table = quote_ident(&spec.table_name);
        let col_list = ident_list(&columns);
        let pk_list = ident_list(&pk_columns);
        let virtual_table = spec.virtual_table_def.is_some();
        let conflict_clause = if virtual_table {
            String::new()
        } else {
            let non_pk: Vec<String> = columns
                .iter()
                .filter(|(name, _)| !schema.primary_key().contains(name))
                .map(|(name, _)| format!("{0} = excluded.{0}", quote_ident(name)))
                .collect();
            if non_pk.is_empty() {
                format!(" ON CONFLICT ({pk_list}) DO NOTHING")
            } else {
                format!(
                    " ON CONFLICT ({pk_list}) DO UPDATE SET {}",
                    non_pk.join(", ")
                )
            }
        };
        Self {
            insert_prefix: format!("INSERT INTO {table} ({col_list}) VALUES "),
            row_placeholders: format!("({})", placeholders(columns.len())),
            conflict_clause,
            table,
            virtual_table,
            columns,
            pk_columns,
        }
    }

    fn upsert_rows_per_statement(&self) -> usize {
        (BIND_LIMIT / self.columns.len().max(1)).max(1)
    }

    fn delete_rows_per_statement(&self) -> usize {
        (BIND_LIMIT / self.pk_columns.len().max(1)).max(1)
    }

    /// `INSERT INTO t (…) VALUES (?, …), (?, …) ON CONFLICT …` for `rows` rows.
    fn upsert_sql(&self, rows: usize) -> String {
        let mut sql = self.insert_prefix.clone();
        for i in 0..rows {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&self.row_placeholders);
        }
        sql.push_str(&self.conflict_clause);
        sql
    }

    /// `DELETE FROM t WHERE pk IN (?, …)` for `keys` keys; a composite key
    /// compares as a row value: `WHERE (a, b) IN (VALUES (?, ?), …)`.
    fn delete_sql(&self, keys: usize) -> String {
        let table = &self.table;
        if self.pk_columns.len() == 1 {
            let pk = quote_ident(&self.pk_columns[0].0);
            format!("DELETE FROM {table} WHERE {pk} IN ({})", placeholders(keys))
        } else {
            let pk_list = ident_list(&self.pk_columns);
            let row = format!("({})", placeholders(self.pk_columns.len()));
            let rows = std::iter::repeat_n(row.as_str(), keys)
                .collect::<Vec<_>>()
                .join(", ");
            format!("DELETE FROM {table} WHERE ({pk_list}) IN (VALUES {rows})")
        }
    }

    /// A row's values in column order, for [`Self::upsert_sql`].
    fn row_values(&self, row: &Map<String, JsonValue>) -> Result<Vec<SqliteValue>> {
        self.columns
            .iter()
            .map(|(name, col)| sqlite_value(row.get(name).unwrap_or(&JsonValue::Null), col))
            .collect()
    }

    /// A row's primary-key values in key order, for [`Self::delete_sql`].
    fn row_key(&self, row: &Map<String, JsonValue>) -> Result<Vec<SqliteValue>> {
        self.pk_columns
            .iter()
            .map(|(name, col)| sqlite_value(row.get(name).unwrap_or(&JsonValue::Null), col))
            .collect()
    }

    /// A stored key's values in key order, for [`Self::delete_sql`].
    fn key_values(&self, key: &[JsonValue]) -> Result<Vec<SqliteValue>> {
        if key.len() != self.pk_columns.len() {
            return Err(Error::engine(
                "SQLite row target primary key length mismatch",
            ));
        }
        self.pk_columns
            .iter()
            .zip(key)
            .map(|((_, col), part)| sqlite_value(&key_part_value(part, col), col))
            .collect()
    }
}

/// `"a", "b", …`
fn ident_list(columns: &[(String, ColumnDef)]) -> String {
    columns
        .iter()
        .map(|(name, _)| quote_ident(name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `?, ?, …`
fn placeholders(count: usize) -> String {
    std::iter::repeat_n("?", count)
        .collect::<Vec<_>>()
        .join(", ")
}

// ---------------------------------------------------------------------------
// Value / key helpers
// ---------------------------------------------------------------------------

fn row_state<R: Serialize>(row: &R, schema: &TableSchema) -> Result<Map<String, JsonValue>> {
    // Reject non-finite floats up front: `serde_json` would turn NaN/±Inf into
    // JSON null, silently corrupting the value (or failing a NOT NULL column with
    // a misleading "got null" error).
    crate::finite::ensure_finite(row)
        .map_err(|e| Error::engine(format!("SQLite target row has a {e}")))?;
    let value = serde_json::to_value(row)
        .map_err(|e| Error::engine(format!("serialize SQLite target row: {e}")))?;
    let JsonValue::Object(mut fields) = value else {
        return Err(Error::engine(
            "SQLite target row must serialize to an object",
        ));
    };
    fields.retain(|name, _| schema.columns().contains_key(name));
    for name in schema.columns().keys() {
        fields.entry(name.clone()).or_insert(JsonValue::Null);
    }
    Ok(fields)
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
            "unsupported SQLite row key: {other:?}"
        ))),
    }
}

/// A row key stores a boolean key part as the string `"true"`/`"false"` (see
/// [`json_scalar_to_stable_key`]); turn it back into a boolean for a column
/// that holds booleans, so a row with a boolean key can be deleted.
fn key_part_value(value: &JsonValue, col: &ColumnDef) -> JsonValue {
    if let JsonValue::String(s) = value
        && let Ok(b) = s.parse::<bool>()
        && is_integer_type(&col.sqlite_type.to_ascii_lowercase())
    {
        return JsonValue::Bool(b);
    }
    value.clone()
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

/// A value bound to a statement parameter.
#[derive(Clone, Debug, PartialEq)]
enum SqliteValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

/// A column value as bound for its declared type.
fn sqlite_value(value: &JsonValue, col: &ColumnDef) -> Result<SqliteValue> {
    if value.is_null() {
        return if col.nullable {
            Ok(SqliteValue::Null)
        } else {
            Err(Error::engine(format!(
                "non-nullable SQLite column of type {} got null",
                col.sqlite_type
            )))
        };
    }
    let t = col.sqlite_type.to_ascii_lowercase();
    if t.starts_with("float[") {
        // sqlite-vec accepts a JSON array string for vector columns.
        return vector_text(value).map(SqliteValue::Text);
    }
    if is_integer_type(&t) {
        if let Some(i) = value.as_i64() {
            return Ok(SqliteValue::Integer(i));
        }
        if let Some(b) = value.as_bool() {
            return Ok(SqliteValue::Integer(i64::from(b)));
        }
        // A fractional number (e.g. 1.5), or one past i64, must NOT be
        // silently stored — SQLite affinity would keep it as a REAL in an
        // INTEGER column, violating the declared schema. Reject it instead.
        return Err(Error::engine(format!(
            "integer column {} requires an integer value",
            col.sqlite_type
        )));
    }
    if is_real_type(&t) {
        if let Some((secs, nanos)) = crate::row_schema::serialized_duration_parts(value) {
            return Ok(SqliteValue::Real(secs as f64 + f64::from(nanos) / 1e9));
        }
        return match value.as_f64() {
            Some(n) => Ok(SqliteValue::Real(n)),
            None => Err(Error::engine(format!(
                "real column {} requires a number",
                col.sqlite_type
            ))),
        };
    }
    if is_text_type(&t) {
        return Ok(SqliteValue::Text(value_to_string(value)));
    }
    if is_blob_type(&t) {
        return blob_value(value);
    }
    // Fallback (unknown affinity): store scalars directly, complex as JSON text.
    Ok(match value {
        JsonValue::String(s) => SqliteValue::Text(s.clone()),
        JsonValue::Number(n) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) => SqliteValue::Integer(i),
            (None, Some(f)) => SqliteValue::Real(f),
            (None, None) => SqliteValue::Text(n.to_string()),
        },
        JsonValue::Bool(b) => SqliteValue::Integer(i64::from(*b)),
        other => SqliteValue::Text(other.to_string()),
    })
}

fn blob_value(value: &JsonValue) -> Result<SqliteValue> {
    let bytes: Vec<u8> = match value {
        JsonValue::Array(items) => items
            .iter()
            .map(|item| {
                item.as_u64()
                    .filter(|value| *value <= 255)
                    .map(|value| value as u8)
                    .ok_or_else(|| Error::engine("BLOB array elements must be integers 0..=255"))
            })
            .collect::<Result<_>>()?,
        JsonValue::String(value) => value.as_bytes().to_vec(),
        _ => {
            return Err(Error::engine(
                "BLOB column requires a byte array or string JSON value",
            ));
        }
    };
    Ok(SqliteValue::Blob(bytes))
}

fn value_to_string(value: &JsonValue) -> String {
    match value {
        JsonValue::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// A JSON number array as sqlite-vec text input: `[1,2,3]`.
fn vector_text(value: &JsonValue) -> Result<String> {
    let arr = value
        .as_array()
        .ok_or_else(|| Error::engine("vector column requires a JSON array"))?;
    let mut parts = Vec::with_capacity(arr.len());
    for v in arr {
        let n = v
            .as_f64()
            .ok_or_else(|| Error::engine("vector values must be numbers"))?;
        if !n.is_finite() {
            return Err(Error::engine("vector values must be finite"));
        }
        parts.push(n.to_string());
    }
    Ok(format!("[{}]", parts.join(",")))
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn is_integer_type(t: &str) -> bool {
    t.contains("int")
}

fn is_real_type(t: &str) -> bool {
    ["real", "float", "double", "numeric", "decimal"]
        .iter()
        .any(|k| t.contains(k))
        && !t.starts_with("float[")
}

fn is_text_type(t: &str) -> bool {
    ["char", "text", "clob"].iter().any(|k| t.contains(k))
}

fn is_blob_type(t: &str) -> bool {
    t.contains("blob")
}

fn validate_sqlite_type(value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '[' | ']' | '(' | ')' | ' '))
    {
        return Err(Error::engine(format!("invalid SQLite type: {value}")));
    }
    Ok(())
}

/// Validate a `vec0` virtual-table configuration. Runtime extension loading is
/// checked when the table is created.
fn validate_vec0(table_name: &str, schema: &TableSchema, def: &Vec0TableDef) -> Result<()> {
    if schema.primary_key().len() != 1 {
        return Err(Error::engine(format!(
            "vec0 table {table_name:?} requires exactly one primary key column"
        )));
    }
    let pk = &schema.primary_key()[0];
    let pk_type = schema.columns()[pk].sqlite_type.to_ascii_lowercase();
    if !pk_type.contains("int") {
        return Err(Error::engine(format!(
            "vec0 table {table_name:?} primary key {pk:?} must be INTEGER, got {pk_type}"
        )));
    }
    let has_vector = schema
        .columns()
        .values()
        .any(|c| c.sqlite_type.to_ascii_lowercase().starts_with("float["));
    if !has_vector {
        return Err(Error::engine(format!(
            "vec0 table {table_name:?} requires at least one float[N] vector column"
        )));
    }
    for name in &def.partition_key_columns {
        if !schema.columns().contains_key(name) {
            return Err(Error::engine(format!(
                "vec0 partition key column {name:?} is not in the schema"
            )));
        }
    }
    for name in &def.auxiliary_columns {
        if !schema.columns().contains_key(name) {
            return Err(Error::engine(format!(
                "vec0 auxiliary column {name:?} is not in the schema"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> TableSchema {
        TableSchema::new(
            [
                ("id", ColumnDef::new("INTEGER")),
                ("name", ColumnDef::new("TEXT")),
                ("score", ColumnDef::new("REAL")),
            ],
            ["id"],
        )
        .unwrap()
    }

    fn spec(schema: TableSchema) -> TableSpec {
        TableSpec {
            table_name: "items".to_string(),
            table_schema: schema,
            virtual_table_def: None,
            managed_by: ManagedBy::System,
        }
    }

    #[test]
    fn create_table_sql_quotes_and_declares_primary_key() {
        let sql = create_table_sql(&spec(schema()));
        assert_eq!(
            sql,
            "CREATE TABLE IF NOT EXISTS \"items\" (\"id\" INTEGER NOT NULL, \"name\" TEXT, \"score\" REAL, PRIMARY KEY (\"id\"))"
        );
    }

    #[test]
    fn upsert_statement_binds_rows_as_parameters() {
        let mut fields = Map::new();
        fields.insert("id".into(), JsonValue::from(7));
        fields.insert("name".into(), JsonValue::from("a'b"));
        fields.insert("score".into(), JsonValue::from(1.5));
        let statements = RowStatements::new(&spec(schema()));
        // Regular tables: one multi-row ON CONFLICT upsert statement.
        assert_eq!(
            statements.upsert_sql(2),
            "INSERT INTO \"items\" (\"id\", \"name\", \"score\") VALUES (?, ?, ?), (?, ?, ?) \
             ON CONFLICT (\"id\") DO UPDATE SET \"name\" = excluded.\"name\", \"score\" = excluded.\"score\""
        );
        assert_eq!(
            statements.row_values(&fields).unwrap(),
            vec![
                SqliteValue::Integer(7),
                SqliteValue::Text("a'b".to_string()),
                SqliteValue::Real(1.5),
            ]
        );
        // 3 columns: 10,922 rows fit the bind limit.
        assert_eq!(statements.upsert_rows_per_statement(), BIND_LIMIT / 3);
    }

    #[test]
    fn vec0_statements_have_no_upsert_clause() {
        // vec0 has no UPSERT: a row is a DELETE by key then an INSERT.
        let mut spec = spec(schema());
        spec.virtual_table_def = Some(Vec0TableDef::default());
        let statements = RowStatements::new(&spec);
        assert!(statements.virtual_table);
        assert_eq!(
            statements.delete_sql(1),
            "DELETE FROM \"items\" WHERE \"id\" IN (?)"
        );
        assert_eq!(
            statements.upsert_sql(1),
            "INSERT INTO \"items\" (\"id\", \"name\", \"score\") VALUES (?, ?, ?)"
        );
    }

    #[test]
    fn row_with_boolean_primary_key_can_be_deleted() {
        let schema = TableSchema::new(
            [
                ("tenant", ColumnDef::new("TEXT")),
                ("active", ColumnDef::new("INTEGER")),
            ],
            ["tenant", "active"],
        )
        .unwrap();
        let key = StableKey::Array(Arc::from([
            json_scalar_to_stable_key(&JsonValue::from("true")).unwrap(),
            json_scalar_to_stable_key(&JsonValue::Bool(true)).unwrap(),
        ]));
        let statements = RowStatements::new(&spec(schema));
        // A composite key compares as a row value.
        assert_eq!(
            statements.delete_sql(2),
            "DELETE FROM \"items\" WHERE (\"tenant\", \"active\") IN (VALUES (?, ?), (?, ?))"
        );
        // A text key part spelled "true" stays text; the boolean key part is
        // bound as the integer it is stored as.
        assert_eq!(
            statements
                .key_values(&stable_key_to_pk(&key).unwrap())
                .unwrap(),
            vec![
                SqliteValue::Text("true".to_string()),
                SqliteValue::Integer(1)
            ]
        );
    }

    #[test]
    fn delete_statement_lists_keys() {
        let statements = RowStatements::new(&spec(schema()));
        assert_eq!(
            statements.delete_sql(3),
            "DELETE FROM \"items\" WHERE \"id\" IN (?, ?, ?)"
        );
        assert_eq!(
            statements.key_values(&[JsonValue::from(9)]).unwrap(),
            vec![SqliteValue::Integer(9)]
        );
        assert!(statements.key_values(&[]).is_err());
    }

    #[test]
    fn integer_column_rejects_fractional_number() {
        let col = ColumnDef::new("INTEGER");
        // Integer values (and bools) are fine.
        assert_eq!(
            sqlite_value(&JsonValue::from(7), &col).unwrap(),
            SqliteValue::Integer(7)
        );
        assert_eq!(
            sqlite_value(&JsonValue::from(true), &col).unwrap(),
            SqliteValue::Integer(1)
        );
        // A fractional number must be rejected, not stored as REAL in an INTEGER column.
        assert!(sqlite_value(&JsonValue::from(1.5), &col).is_err());
    }

    #[test]
    fn blob_value_rejects_out_of_range_bytes() {
        let col = ColumnDef::new("BLOB");
        assert!(sqlite_value(&serde_json::json!([256]), &col).is_err());
        assert_eq!(
            sqlite_value(&serde_json::json!([104, 105]), &col).unwrap(),
            SqliteValue::Blob(b"hi".to_vec())
        );
    }

    #[test]
    fn single_column_table_upsert_does_nothing_on_conflict() {
        let schema = TableSchema::new([("id", ColumnDef::new("INTEGER"))], ["id"]).unwrap();
        let statements = RowStatements::new(&spec(schema));
        assert!(
            statements
                .upsert_sql(1)
                .ends_with("ON CONFLICT (\"id\") DO NOTHING")
        );
    }

    #[test]
    fn vec0_ddl_marks_partition_auxiliary_and_vector_columns() {
        let schema = TableSchema::new(
            [
                ("id", ColumnDef::new("INTEGER")),
                ("embedding", ColumnDef::new("float[3]")),
                ("year", ColumnDef::new("integer")),
                ("meta", ColumnDef::new("text")),
            ],
            ["id"],
        )
        .unwrap();
        let def = Vec0TableDef {
            partition_key_columns: vec!["year".to_string()],
            auxiliary_columns: vec!["meta".to_string()],
        };
        validate_vec0("vecs", &schema, &def).unwrap();
        let mut s = spec(schema);
        s.table_name = "vecs".to_string();
        s.virtual_table_def = Some(def.clone());
        let sql = create_vec0_sql(&s, &def);
        assert_eq!(
            sql,
            "CREATE VIRTUAL TABLE IF NOT EXISTS \"vecs\" USING vec0(\
id integer primary key, embedding float[3], year integer partition key, +meta text)"
        );
    }

    #[test]
    fn vec0_requires_single_integer_pk_and_a_vector_column() {
        // Composite PK rejected.
        let composite = TableSchema::new(
            [
                ("a", ColumnDef::new("INTEGER")),
                ("b", ColumnDef::new("INTEGER")),
                ("v", ColumnDef::new("float[2]")),
            ],
            ["a", "b"],
        )
        .unwrap();
        assert!(validate_vec0("t", &composite, &Vec0TableDef::default()).is_err());

        // Non-integer PK rejected.
        let str_pk = TableSchema::new(
            [
                ("id", ColumnDef::new("TEXT")),
                ("v", ColumnDef::new("float[2]")),
            ],
            ["id"],
        )
        .unwrap();
        assert!(validate_vec0("t", &str_pk, &Vec0TableDef::default()).is_err());

        // Missing vector column rejected.
        let no_vec = TableSchema::new([("id", ColumnDef::new("INTEGER"))], ["id"]).unwrap();
        assert!(validate_vec0("t", &no_vec, &Vec0TableDef::default()).is_err());

        // Unknown partition/auxiliary column rejected.
        let ok_schema = TableSchema::new(
            [
                ("id", ColumnDef::new("INTEGER")),
                ("v", ColumnDef::new("float[2]")),
            ],
            ["id"],
        )
        .unwrap();
        let bad_part = Vec0TableDef {
            partition_key_columns: vec!["nope".to_string()],
            auxiliary_columns: vec![],
        };
        assert!(validate_vec0("t", &ok_schema, &bad_part).is_err());
    }
}
