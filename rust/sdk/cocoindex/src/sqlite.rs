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
//! `vec0` extension to be available in the SQLite build at runtime.

use std::collections::BTreeMap;
use std::sync::Arc;

use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::statediff::{
    DiffAction, ManagedBy, MutualTrackingRecord, diff, resolve_system_transition,
};
use crate::target_state::{
    ChildTargetDef, StableKey, TargetAction, TargetActionSink, TargetChildInvalidation,
    TargetHandler, TargetReconcileOutput, TargetState, TargetStateProvider, declare_target_state,
    declare_target_state_with_child, mount_target, register_root_target_states_provider,
};

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// A SQLite connection pool. Clone-cheap (the underlying pool is shared).
#[derive(Clone)]
pub struct Database {
    pool: SqlitePool,
    state_id: Arc<str>,
}

impl Database {
    /// Open (creating if missing) a SQLite database at `path`. SQLite is a
    /// single-writer engine, so the pool is capped at one connection.
    pub async fn connect(path: &str) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
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
            out.insert(name, def);
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
#[derive(Clone)]
pub struct TableTarget {
    table_name: Arc<str>,
    table_schema: TableSchema,
    rows: TargetStateProvider<RowState>,
}

/// Build a composable [`TargetState`] for a SQLite table (the spec
/// constructor). System-managed regular table.
pub fn table_target(
    ctx: &Ctx,
    db: &Database,
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
    db: &Database,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    options: SqliteTableOptions,
) -> Result<TargetState<TableSpec>> {
    let table_name = table_name.into();
    validate_ident(&table_name, "table name")?;
    if let Some(def) = &options.virtual_table_def {
        validate_vec0(&table_name, &table_schema, def)?;
    }
    let provider = register_root_target_states_provider(
        ctx,
        format!("cocoindex/sqlite/table/{}/{}", db.state_id(), table_name),
        TableHandler { db: db.clone() },
    )?;
    Ok(provider.target_state(
        "default",
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
    db: &Database,
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
    db: &Database,
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
    db: &Database,
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
    db: &Database,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TableAction {
    /// `Some` for a create/update (carrying the desired spec).
    spec: Option<TableSpec>,
    /// `Some` for a drop (orphaned table name).
    drop: Option<String>,
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
    db: Database,
}

impl TargetHandler<TableSpec> for TableHandler {
    type TrackingRecord = MutualTrackingRecord<TableSpec>;
    type Action = TableAction;

    fn reconcile(
        &self,
        _key: StableKey,
        desired: Option<TableSpec>,
        prev: Vec<MutualTrackingRecord<TableSpec>>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<TableAction, MutualTrackingRecord<TableSpec>>>> {
        match desired {
            // Always emit when declared so the sink fulfills the row child.
            Some(spec) => {
                let tracking = MutualTrackingRecord::new(spec.clone(), spec.managed_by);
                let resolved =
                    resolve_system_transition(Some(tracking.clone()), prev, prev_may_be_missing);
                let main_action = diff(resolved.as_ref());
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Update(TableAction {
                        spec: Some(spec),
                        drop: None,
                    }),
                    sink: self.table_sink(),
                    tracking_record: Some(tracking),
                    child_invalidation: matches!(main_action, Some(DiffAction::Replace))
                        .then_some(TargetChildInvalidation::Lossy),
                }))
            }
            None => {
                let resolved = resolve_system_transition(None, prev.clone(), prev_may_be_missing);
                if resolved.is_none() {
                    return Ok(None);
                }
                let Some(prev_spec) = prev
                    .into_iter()
                    .find(|v| v.managed_by.is_system())
                    .map(|v| v.tracking_record)
                else {
                    return Ok(None);
                };
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Delete(TableAction {
                        spec: None,
                        drop: Some(prev_spec.table_name),
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
        let db = self.db.clone();
        TargetActionSink::from_async_fn_with_children(
            move |actions: Vec<TargetAction<TableAction>>| {
                let db = db.clone();
                async move {
                    let mut out: Vec<Option<ChildTargetDef>> = Vec::with_capacity(actions.len());
                    for action in actions {
                        match action {
                            TargetAction::Create(a) | TargetAction::Update(a) => {
                                let spec = a.spec.ok_or_else(|| {
                                    Error::engine("SQLite table action missing spec")
                                })?;
                                create_table(&db, &spec).await?;
                                out.push(Some(ChildTargetDef::new::<RowState, _>(RowHandler {
                                    db: db.clone(),
                                    spec,
                                })));
                            }
                            TargetAction::Delete(a) => {
                                if let Some(table_name) = a.drop {
                                    drop_table(&db, &table_name).await?;
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
}

// ---------------------------------------------------------------------------
// Row handler (child) + sink
// ---------------------------------------------------------------------------

struct RowHandler {
    db: Database,
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
            .is_some_and(|fp| prev.iter().any(|p| p == fp));
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
        let db = self.db.clone();
        let spec = self.spec.clone();
        TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<RowAction>>| {
            let db = db.clone();
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
                apply_rows(&db, &spec, mutations).await
            }
        })
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

async fn apply_rows(
    db: &Database,
    spec: &TableSpec,
    mutations: Vec<(Vec<JsonValue>, Option<RowState>)>,
) -> Result<()> {
    if mutations.is_empty() {
        return Ok(());
    }
    if spec.managed_by.is_system() {
        create_table(db, spec).await?;
    }
    let mut tx = db.pool().begin().await.map_err(sqlite_err)?;
    for (pk, state) in mutations {
        let sql = match state {
            Some(state) => upsert_sql(spec, &state.fields)?,
            None => delete_sql(spec, &pk)?,
        };
        sqlx::query(&sql)
            .execute(&mut *tx)
            .await
            .map_err(sqlite_err)?;
    }
    tx.commit().await.map_err(sqlite_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// DDL / SQL builders
// ---------------------------------------------------------------------------

fn create_table_sql(spec: &TableSpec) -> String {
    let mut cols = Vec::new();
    for (name, col) in spec.table_schema.columns() {
        let null = if col.nullable { "" } else { " NOT NULL" };
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

fn upsert_sql(spec: &TableSpec, fields: &Map<String, JsonValue>) -> Result<String> {
    let cols: Vec<&String> = spec.table_schema.columns().keys().collect();
    let col_sql = cols
        .iter()
        .map(|name| quote_ident(name))
        .collect::<Vec<_>>()
        .join(", ");
    let values = cols
        .iter()
        .map(|name| {
            let col = spec
                .table_schema
                .columns()
                .get(*name)
                .expect("schema column");
            let value = fields.get(*name).unwrap_or(&JsonValue::Null);
            sql_literal(value, col)
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    // vec0 virtual tables do not support UPSERT; delete-then-insert per row.
    if spec.virtual_table_def.is_some() {
        let pk_predicate = pk_predicate(spec, fields)?;
        return Ok(format!(
            "DELETE FROM {table} WHERE {pk_predicate}; INSERT INTO {table} ({col_sql}) VALUES ({values})",
            table = quote_ident(&spec.table_name),
        ));
    }
    let pk_sql = spec
        .table_schema
        .primary_key()
        .iter()
        .map(|name| quote_ident(name))
        .collect::<Vec<_>>()
        .join(", ");
    let non_pk = cols
        .iter()
        .filter(|name| !spec.table_schema.primary_key().contains(name))
        .map(|name| format!("{} = excluded.{}", quote_ident(name), quote_ident(name)))
        .collect::<Vec<_>>();
    let conflict = if non_pk.is_empty() {
        format!("ON CONFLICT ({pk_sql}) DO NOTHING")
    } else {
        format!("ON CONFLICT ({pk_sql}) DO UPDATE SET {}", non_pk.join(", "))
    };
    Ok(format!(
        "INSERT INTO {} ({col_sql}) VALUES ({values}) {conflict}",
        quote_ident(&spec.table_name)
    ))
}

fn delete_sql(spec: &TableSpec, pk: &[JsonValue]) -> Result<String> {
    if pk.len() != spec.table_schema.primary_key().len() {
        return Err(Error::engine(
            "SQLite row target primary key length mismatch",
        ));
    }
    let mut predicates = Vec::with_capacity(pk.len());
    for (idx, name) in spec.table_schema.primary_key().iter().enumerate() {
        let col = spec.table_schema.columns().get(name).expect("pk column");
        predicates.push(format!(
            "{} = {}",
            quote_ident(name),
            sql_literal(&pk[idx], col)?
        ));
    }
    Ok(format!(
        "DELETE FROM {} WHERE {}",
        quote_ident(&spec.table_name),
        predicates.join(" AND ")
    ))
}

fn pk_predicate(spec: &TableSpec, fields: &Map<String, JsonValue>) -> Result<String> {
    let mut predicates = Vec::new();
    for name in spec.table_schema.primary_key() {
        let col = spec.table_schema.columns().get(name).expect("pk column");
        let value = fields.get(name).unwrap_or(&JsonValue::Null);
        predicates.push(format!(
            "{} = {}",
            quote_ident(name),
            sql_literal(value, col)?
        ));
    }
    Ok(predicates.join(" AND "))
}

// ---------------------------------------------------------------------------
// Value / key helpers
// ---------------------------------------------------------------------------

fn row_state<R: Serialize>(row: &R, schema: &TableSchema) -> Result<Map<String, JsonValue>> {
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

fn sql_literal(value: &JsonValue, col: &ColumnDef) -> Result<String> {
    if value.is_null() {
        return if col.nullable {
            Ok("NULL".to_string())
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
        return vector_literal(value);
    }
    if is_integer_type(&t) {
        return match value {
            JsonValue::Number(n) if n.is_i64() || n.is_u64() => Ok(n.to_string()),
            JsonValue::Bool(b) => Ok(if *b { "1" } else { "0" }.to_string()),
            JsonValue::Number(n) => Ok(n.to_string()),
            _ => Err(Error::engine(format!(
                "integer column {} requires a number",
                col.sqlite_type
            ))),
        };
    }
    if is_real_type(&t) {
        return match value {
            JsonValue::Number(n) => Ok(n.to_string()),
            _ => Err(Error::engine(format!(
                "real column {} requires a number",
                col.sqlite_type
            ))),
        };
    }
    if is_text_type(&t) {
        return Ok(quote_string(value_to_string(value)));
    }
    // Fallback (BLOB / unknown): store scalars directly, complex as JSON text.
    match value {
        JsonValue::String(s) => Ok(quote_string(s)),
        JsonValue::Number(n) => Ok(n.to_string()),
        JsonValue::Bool(b) => Ok(if *b { "1" } else { "0" }.to_string()),
        other => Ok(quote_string(other.to_string())),
    }
}

fn value_to_string(value: &JsonValue) -> String {
    match value {
        JsonValue::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn vector_literal(value: &JsonValue) -> Result<String> {
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
    Ok(quote_string(format!("[{}]", parts.join(","))))
}

fn quote_string(value: impl AsRef<str>) -> String {
    let value = value.as_ref().replace('\0', "").replace('\'', "''");
    format!("'{value}'")
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

fn validate_ident(value: &str, label: &str) -> Result<()> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(Error::engine(format!("{label} cannot be empty")));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(Error::engine(format!("invalid {label}: {value}")));
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(Error::engine(format!("invalid {label}: {value}")));
    }
    Ok(())
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
            "CREATE TABLE IF NOT EXISTS \"items\" (\"id\" INTEGER, \"name\" TEXT, \"score\" REAL, PRIMARY KEY (\"id\"))"
        );
    }

    #[test]
    fn upsert_sql_uses_on_conflict_with_typed_literals() {
        let mut fields = Map::new();
        fields.insert("id".into(), JsonValue::from(7));
        fields.insert("name".into(), JsonValue::from("a'b"));
        fields.insert("score".into(), JsonValue::from(1.5));
        let sql = upsert_sql(&spec(schema()), &fields).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"items\" (\"id\", \"name\", \"score\") VALUES (7, 'a''b', 1.5) \
             ON CONFLICT (\"id\") DO UPDATE SET \"name\" = excluded.\"name\", \"score\" = excluded.\"score\""
        );
    }

    #[test]
    fn delete_sql_uses_primary_key_literal() {
        let sql = delete_sql(&spec(schema()), &[JsonValue::from(9)]).unwrap();
        assert_eq!(sql, "DELETE FROM \"items\" WHERE \"id\" = 9");
    }

    #[test]
    fn single_column_table_upsert_does_nothing_on_conflict() {
        let schema = TableSchema::new([("id", ColumnDef::new("INTEGER"))], ["id"]).unwrap();
        let mut fields = Map::new();
        fields.insert("id".into(), JsonValue::from(1));
        let sql = upsert_sql(&spec(schema), &fields).unwrap();
        assert!(sql.ends_with("ON CONFLICT (\"id\") DO NOTHING"));
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
