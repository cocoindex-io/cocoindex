//! Postgres source and target helpers.
//!
//! Table targets reconcile declared rows against the previous run: changed rows
//! are upserted, unchanged rows are skipped, and rows no longer declared are
//! deleted. System-managed targets also create/drop table DDL and attachments
//! such as vector indexes and SQL setup commands.
//!
//! Use [`table_target`] to build a composable target state, [`declare_table_target`]
//! inside the current component, or [`mount_table_target`] when rows must be
//! declared immediately. [`read_table`] and [`read_table_items`] stream source
//! rows for use with `Ctx::mount_each`.
//!
//! Rows are written in batches: one parameterized `unnest` statement per
//! chunk of `ROWS_PER_STATEMENT` rows (see `RowStatements`), the statements
//! of a batch running concurrently over the pool.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use cocoindex_utils::fingerprint::Fingerprint;
use futures::{SinkExt as _, Stream, StreamExt as _};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::ctx::{ContextKey, ContextStore, Ctx};
use crate::error::{Error, Result};
use crate::resources::schema::{VectorElementType, VectorSchemaProvider};
use crate::statediff::{
    ManagedBy, ManagedTargetOptions, MutualTrackingRecord, resolve_system_transition,
};
use crate::target_state::{
    ChildTargetDef, STATIC_ROOT_PROVIDERS, SinkCache, StableKey, StaticRootProvider, TargetAction,
    TargetActionSink, TargetChildInvalidation, TargetHandler, TargetReconcileOutput, TargetState,
    TargetStateProvider, declare_target_state, declare_target_state_with_child, mount_target,
};

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
    state_id: Arc<str>,
}

impl Database {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(url)
            .await
            .map_err(pg_err)?;
        Ok(Self {
            pool,
            state_id: Arc::from(url.to_string()),
        })
    }

    pub fn from_pool(state_id: impl Into<String>, pool: PgPool) -> Self {
        Self {
            pool,
            state_id: Arc::from(state_id.into()),
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn state_id(&self) -> &str {
        &self.state_id
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnDef {
    pub pg_type: String,
    pub nullable: bool,
}

impl ColumnDef {
    pub fn new(pg_type: impl Into<String>) -> Self {
        Self {
            pg_type: pg_type.into(),
            nullable: false,
        }
    }

    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSchema {
    columns: BTreeMap<String, ColumnDef>,
    primary_key: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    unresolved_vector_columns: BTreeSet<String>,
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
            validate_pg_type(&def.pg_type)?;
            if out.insert(name.clone(), def).is_some() {
                return Err(Error::engine(format!(
                    "Postgres table schema contains duplicate column {name:?}"
                )));
            }
        }
        let primary_key: Vec<String> = primary_key.into_iter().map(Into::into).collect::<Vec<_>>();
        if primary_key.is_empty() {
            return Err(Error::engine("Postgres table primary key cannot be empty"));
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
            unresolved_vector_columns: BTreeSet::new(),
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
    /// Postgres column via the same leaf-type table as Python's `postgres`
    /// `from_class`. A `#[coco(vector = N)]` field becomes `vector(N)` (or
    /// `halfvec(N)` with `#[coco(vector = N, half)]`) for `pgvector`.
    pub fn from_row<T: crate::row_schema::SchemaFields>(
        primary_key: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let fields = T::schema_fields();
        let unresolved_vector_columns = fields
            .iter()
            .filter_map(|field| match field.logical_type {
                crate::row_schema::LogicalType::Vector { dim: 0, .. } => Some(field.name.clone()),
                _ => None,
            })
            .collect();
        let columns = fields
            .into_iter()
            .map(|f| (f.name.clone(), postgres_column_def(&f)));
        let mut schema = Self::new(columns, primary_key)?;
        schema.unresolved_vector_columns = unresolved_vector_columns;
        Ok(schema)
    }

    /// Resolve or override the dimension of a vector field derived from a row.
    pub fn with_vector_dim(self, field_name: &str, dim: usize) -> Result<Self> {
        self.set_vector_column(field_name, dim, None)
    }

    /// Resolve a vector field derived from a row from a
    /// [`VectorSchemaProvider`], typically the embedder that fills it (Python's
    /// `column_overrides={"embedding": embedder}`). The column becomes
    /// `halfvec(N)` for `f16` elements and `vector(N)` otherwise. For an
    /// embedder held in the context, pass `ctx.get_key(&EMBEDDER)?`.
    pub async fn with_vector_schema(
        self,
        field_name: &str,
        provider: &(impl VectorSchemaProvider + ?Sized),
    ) -> Result<Self> {
        let schema =
            crate::row_schema::provided_vector_schema("Postgres", field_name, provider).await?;
        let base = match schema.element_type {
            VectorElementType::Float16 => "halfvec",
            VectorElementType::Float32 | VectorElementType::Float64 => "vector",
        };
        self.set_vector_column(field_name, schema.size, Some(base))
    }

    /// Replace the Postgres type of one column of a schema derived from a row
    /// (Python's `column_overrides={"col": PgType("…")}`), keeping its
    /// nullability.
    pub fn with_column_type(
        mut self,
        field_name: &str,
        pg_type: impl Into<String>,
    ) -> Result<Self> {
        let pg_type = pg_type.into();
        validate_pg_type(&pg_type)?;
        let def = self
            .columns
            .get_mut(field_name)
            .ok_or_else(|| crate::row_schema::unknown_column_error("Postgres", field_name))?;
        def.pg_type = pg_type;
        self.unresolved_vector_columns.remove(field_name);
        Ok(self)
    }

    /// Set the dimension of the vector column `field_name`, and its pgvector
    /// type base when `base` is given.
    fn set_vector_column(
        mut self,
        field_name: &str,
        dim: usize,
        base: Option<&'static str>,
    ) -> Result<Self> {
        if dim == 0 {
            return Err(crate::row_schema::zero_vector_dimension_error(
                "Postgres", field_name,
            ));
        }
        let def = self
            .columns
            .get_mut(field_name)
            .ok_or_else(|| crate::row_schema::unknown_vector_field_error("Postgres", field_name))?;
        let current_base = pgvector_type_base(&def.pg_type)
            .ok_or_else(|| crate::row_schema::not_vector_field_error("Postgres", field_name))?;
        def.pg_type = format!("{}({dim})", base.unwrap_or(current_base));
        self.unresolved_vector_columns.remove(field_name);
        Ok(self)
    }

    fn validate_vector_dimensions(&self) -> Result<()> {
        for name in &self.unresolved_vector_columns {
            crate::row_schema::require_resolved_vector_dimension("Postgres", name)?;
        }
        Ok(())
    }
}

fn pgvector_type_base(pg_type: &str) -> Option<&'static str> {
    ["vector", "halfvec"].into_iter().find(|base| {
        pg_type == *base
            || pg_type
                .strip_prefix(*base)
                .is_some_and(|suffix| suffix.starts_with('(') && suffix.ends_with(')'))
    })
}

/// Map a connector-agnostic [`SchemaField`](crate::row_schema::SchemaField) to a
/// Postgres [`ColumnDef`], mirroring Python's `postgres` `_LEAF_TYPE_MAPPINGS`.
fn postgres_column_def(field: &crate::row_schema::SchemaField) -> ColumnDef {
    use crate::row_schema::LogicalType as L;
    let pg_type = match &field.logical_type {
        L::Bool => "boolean".to_string(),
        L::Int16 => "smallint".to_string(),
        L::Int32 => "integer".to_string(),
        L::Int64 => "bigint".to_string(),
        L::Float32 => "real".to_string(),
        L::Float64 => "double precision".to_string(),
        L::Decimal => "numeric".to_string(),
        L::Text => "text".to_string(),
        L::Bytes => "bytea".to_string(),
        L::Uuid => "uuid".to_string(),
        L::Date => "date".to_string(),
        L::Time => "time with time zone".to_string(),
        L::DateTime => "timestamp with time zone".to_string(),
        L::Duration => "interval".to_string(),
        L::Json => "jsonb".to_string(),
        L::Vector { dim, half } => {
            let base = if *half { "halfvec" } else { "vector" };
            if *dim == 0 {
                base.to_string()
            } else {
                format!("{base}({dim})")
            }
        }
        L::Custom(s) => s.clone(),
    };
    let def = ColumnDef::new(pg_type);
    if field.nullable { def.nullable() } else { def }
}

// ---------------------------------------------------------------------------
// Public target API: constructor / declaration / mount split
// ---------------------------------------------------------------------------

/// A declarative Postgres table target — a handle to declare rows (and a vector
/// index) on. See the [module docs](self).
///
/// As a `mount_each!` / `use_mount!` argument it fingerprints by its qualified
/// table name (its stable key); the schema, managed-by flag, and runtime
/// providers are not part of the component-memo identity.
#[derive(Clone, Serialize)]
pub struct TableTarget {
    pg_schema_name: Option<Arc<str>>,
    table_name: Arc<str>,
    #[serde(skip)]
    table_schema: TableSchema,
    #[serde(skip)]
    managed_by: ManagedBy,
    /// The table's row provider; also carries its attachments (vector
    /// indexes, SQL commands).
    #[serde(skip)]
    rows: TargetStateProvider<RowState>,
}

/// Build a composable [`TargetState`] for a Postgres table (the spec
/// constructor). System-managed. Pass it to
/// [`declare_table_target`]/[`mount_table_target`] or the generic
/// [`declare_target_state_with_child`]/[`mount_target`].
pub fn table_target(
    ctx: &Ctx,
    db: &ContextKey<Database>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    pg_schema_name: Option<&str>,
) -> Result<TargetState<TableSpec>> {
    table_target_with_options(
        ctx,
        db,
        table_name,
        table_schema,
        pg_schema_name,
        ManagedTargetOptions::default(),
    )
}

/// [`table_target`] with explicit [`ManagedTargetOptions`] (`managed_by`).
pub fn table_target_with_options(
    ctx: &Ctx,
    db: &ContextKey<Database>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    pg_schema_name: Option<&str>,
    options: ManagedTargetOptions,
) -> Result<TargetState<TableSpec>> {
    let table_name = table_name.into();
    validate_ident(&table_name, "table name")?;
    table_schema.validate_vector_dimensions()?;
    if let Some(schema) = pg_schema_name {
        validate_ident(schema, "schema name")?;
    }
    let provider = TABLE_PROVIDER.get(ctx)?;
    Ok(provider.target_state(
        // Stable connection identity = the ContextKey's name (`db_key`); the pool
        // is resolved at apply time from the host context (design_connectors §5.5).
        TableKey {
            db_key: db.name().to_string(),
            pg_schema_name: pg_schema_name.unwrap_or("public").to_string(),
            table_name: table_name.clone(),
        }
        .stable_key(),
        TableSpec {
            pg_schema_name: pg_schema_name.map(str::to_string),
            table_name,
            table_schema,
            managed_by: options.managed_by,
        },
    ))
}

/// Declare a Postgres table target in the **current** component and return a
/// pending handle. The row child provider resolves when this component commits;
/// use [`mount_table_target`] when rows or attachments must be declared
/// immediately.
pub fn declare_table_target(
    ctx: &Ctx,
    db: &ContextKey<Database>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    pg_schema_name: Option<&str>,
) -> Result<TableTarget> {
    declare_table_target_with_options(
        ctx,
        db,
        table_name,
        table_schema,
        pg_schema_name,
        ManagedTargetOptions::default(),
    )
}

/// [`declare_table_target`] with explicit [`ManagedTargetOptions`] (`managed_by`).
pub fn declare_table_target_with_options(
    ctx: &Ctx,
    db: &ContextKey<Database>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    pg_schema_name: Option<&str>,
    options: ManagedTargetOptions,
) -> Result<TableTarget> {
    let ts = table_target_with_options(ctx, db, table_name, table_schema, pg_schema_name, options)?;
    let spec = ts.value().clone();
    let rows = declare_target_state_with_child::<TableSpec, RowState>(ctx, ts)?;
    Ok(table_target_handle(spec, rows))
}

/// Mount a Postgres table target **foreground** (rows can be declared
/// immediately) and return a handle. System-managed.
pub async fn mount_table_target(
    ctx: &Ctx,
    db: &ContextKey<Database>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    pg_schema_name: Option<&str>,
) -> Result<TableTarget> {
    mount_table_target_with_options(
        ctx,
        db,
        table_name,
        table_schema,
        pg_schema_name,
        ManagedTargetOptions::default(),
    )
    .await
}

/// [`mount_table_target`] with explicit [`ManagedTargetOptions`] (`managed_by`).
pub async fn mount_table_target_with_options(
    ctx: &Ctx,
    db: &ContextKey<Database>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    pg_schema_name: Option<&str>,
    options: ManagedTargetOptions,
) -> Result<TableTarget> {
    let ts = table_target_with_options(ctx, db, table_name, table_schema, pg_schema_name, options)?;
    let spec = ts.value().clone();
    let rows = mount_target::<TableSpec, RowState>(ctx, ts).await?;
    Ok(table_target_handle(spec, rows))
}

fn table_target_handle(spec: TableSpec, rows: TargetStateProvider<RowState>) -> TableTarget {
    TableTarget {
        pg_schema_name: spec.pg_schema_name.map(Arc::from),
        table_name: Arc::from(spec.table_name),
        table_schema: spec.table_schema,
        managed_by: spec.managed_by,
        rows,
    }
}

impl TableTarget {
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Declare a row. `row` serializes to an object whose entries are the
    /// table's columns (extra entries are ignored, missing ones are NULL); a
    /// value is written in its column type's input syntax.
    ///
    /// Python's `ColumnDef.encoder` converts a field's value before it is
    /// bound. Here the row's `Serialize` impl does that: `uuid`, `chrono`,
    /// `rust_decimal`, `std::net::IpAddr` (for `inet`) and `Vec<f32>` (for
    /// pgvector) serialize to what their columns take, and a field needing
    /// another form uses `#[serde(serialize_with = "…")]`.
    pub fn declare_row<R: Serialize>(&self, ctx: &Ctx, row: &R) -> Result<()> {
        let row = row_state(row, &self.table_schema)?;
        let key = pk_stable_key(&row, self.table_schema.primary_key())?;
        declare_target_state(ctx, self.rows.target_state(key, RowState { fields: row }))
    }

    /// Declare a pgvector index on `column` as an attachment of this table. The
    /// index is created/recreated/dropped to match the declared options.
    pub fn declare_vector_index(
        &self,
        ctx: &Ctx,
        column: &str,
        options: VectorIndexOptions,
    ) -> Result<()> {
        validate_ident(column, "vector index column")?;
        let Some(col) = self.table_schema.columns().get(column) else {
            return Err(Error::engine(format!(
                "vector index column {column:?} is not in table schema"
            )));
        };
        let op_class = pgvector_op_class(&col.pg_type, options.metric)?;
        let name = options.name.unwrap_or_else(|| column.to_string());
        validate_ident(&name, "vector index name")?;
        let provider: TargetStateProvider<VectorIndexSpec> =
            self.rows.attachment(ctx, "vector_index")?;
        let spec = VectorIndexSpec {
            pg_schema_name: self.pg_schema_name.as_deref().map(str::to_string),
            table_name: self.table_name.to_string(),
            table_schema: self.table_schema.clone(),
            managed_by: self.managed_by,
            name: name.clone(),
            column: column.to_string(),
            method: options.method.to_string(),
            metric: options.metric.to_string(),
            op_class: op_class.to_string(),
            lists: options.lists,
            m: options.m,
            ef_construction: options.ef_construction,
        };
        declare_target_state(
            ctx,
            provider.target_state(StableKey::Str(Arc::from(name)), spec),
        )
    }

    /// Declare a SQL command attachment on this table. `setup_sql` runs when the
    /// attachment is created or changed; `teardown_sql` (if given) runs when it
    /// is removed, and before re-running setup on a change.
    pub fn declare_sql_command_attachment(
        &self,
        ctx: &Ctx,
        name: &str,
        setup_sql: impl Into<String>,
        teardown_sql: Option<String>,
    ) -> Result<()> {
        let provider: TargetStateProvider<SqlCommandSpec> =
            self.rows.attachment(ctx, "sql_command_attachment")?;
        let spec = SqlCommandSpec {
            setup_sql: setup_sql.into(),
            teardown_sql,
        };
        declare_target_state(
            ctx,
            provider.target_state(StableKey::Str(Arc::from(name)), spec),
        )
    }
}

#[derive(Clone, Debug)]
pub struct VectorIndexOptions {
    pub name: Option<String>,
    pub metric: &'static str,
    pub method: &'static str,
    pub lists: Option<u32>,
    pub m: Option<u32>,
    pub ef_construction: Option<u32>,
}

impl Default for VectorIndexOptions {
    fn default() -> Self {
        Self {
            name: None,
            metric: "cosine",
            method: "ivfflat",
            lists: None,
            m: None,
            ef_construction: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ReadTableOptions {
    pub pg_schema_name: Option<String>,
    pub columns: Option<Vec<String>>,
}

impl ReadTableOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn pg_schema_name(mut self, schema: impl Into<String>) -> Self {
        self.pg_schema_name = Some(schema.into());
        self
    }

    pub fn columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }
}

// ---------------------------------------------------------------------------
// Internal specs / actions
// ---------------------------------------------------------------------------

/// Spec for a Postgres table (the declared container value).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSpec {
    pg_schema_name: Option<String>,
    table_name: String,
    table_schema: TableSchema,
    #[serde(default)]
    managed_by: ManagedBy,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RowState {
    fields: Map<String, JsonValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DropTarget {
    pg_schema_name: Option<String>,
    table_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TableAction {
    /// The `ContextKey` name of the database the table lives in.
    db_key: String,
    /// `Some` for a define (create/update), carrying the desired spec.
    spec: Option<TableSpec>,
    /// `Some` for a drop (orphaned table).
    drop: Option<DropTarget>,
    /// The primary-key signature changed: drop and recreate the table.
    #[serde(default)]
    recreate: bool,
    /// Non-PK columns whose declared type changed since the last run; retype
    /// them (preserving rows where the cast allows).
    #[serde(default)]
    retype_cols: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RowAction {
    pk: Vec<JsonValue>,
    state: Option<RowState>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct VectorIndexAction {
    spec: VectorIndexSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorIndexSpec {
    pg_schema_name: Option<String>,
    table_name: String,
    table_schema: TableSchema,
    #[serde(default)]
    managed_by: ManagedBy,
    name: String,
    column: String,
    method: String,
    metric: String,
    op_class: String,
    lists: Option<u32>,
    m: Option<u32>,
    ef_construction: Option<u32>,
}

// ---------------------------------------------------------------------------
// Table container handler (root) + sink yielding row children
// ---------------------------------------------------------------------------

#[linkme::distributed_slice(STATIC_ROOT_PROVIDERS)]
static TABLE_PROVIDER: StaticRootProvider =
    StaticRootProvider::new::<TableSpec, TableHandler>("cocoindex/postgres/table");

/// A table target state's key: the database's `ContextKey` name plus the
/// qualified table name.
struct TableKey {
    db_key: String,
    pg_schema_name: String,
    table_name: String,
}

impl TableKey {
    fn stable_key(self) -> StableKey {
        StableKey::Array(Arc::from([
            StableKey::Str(Arc::from(self.db_key)),
            StableKey::Str(Arc::from(self.pg_schema_name)),
            StableKey::Str(Arc::from(self.table_name)),
        ]))
    }

    fn from_stable_key(key: &StableKey) -> Result<Self> {
        if let StableKey::Array(parts) = key
            && let [
                StableKey::Str(db_key),
                StableKey::Str(pg_schema_name),
                StableKey::Str(table_name),
            ] = &parts[..]
        {
            return Ok(Self {
                db_key: db_key.to_string(),
                pg_schema_name: pg_schema_name.to_string(),
                table_name: table_name.to_string(),
            });
        }
        Err(Error::engine(format!(
            "unexpected postgres table target key: {key:?}"
        )))
    }
}

/// Root handler for every Postgres table target: stateless, the database is
/// named by each target state's [`TableKey`].
#[derive(Default)]
struct TableHandler {
    sink: SinkCache<TableAction>,
}

impl TargetHandler<TableSpec> for TableHandler {
    type TrackingRecord = MutualTrackingRecord<TableSpec>;
    type Action = TableAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<TableSpec>,
        prev: Vec<MutualTrackingRecord<TableSpec>>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<TableAction, MutualTrackingRecord<TableSpec>>>> {
        let db_key = TableKey::from_stable_key(&key)?.db_key;
        match desired {
            // Always emit when declared so the sink builds the row child.
            Some(spec) => {
                let tracking = MutualTrackingRecord::new(spec.clone(), spec.managed_by);
                // The previous system-managed spec (if any) drives the schema-
                // evolution decision: a PK-signature change forces a destructive
                // drop+recreate; a non-PK column type change is an in-place
                // retype; a column drop is lossy. (`reconcile_columns` in the
                // sink still handles plain add/drop against `information_schema`.)
                let prev_spec = prev
                    .iter()
                    .find(|v| v.managed_by.is_system())
                    .map(|v| v.tracking_record.clone());
                let _resolved =
                    resolve_system_transition(Some(tracking.clone()), prev, prev_may_be_missing);

                let mut recreate = false;
                let mut retype_cols: Vec<String> = Vec::new();
                let mut dropped_col = false;
                if spec.managed_by.is_system()
                    && let Some(prev_spec) = &prev_spec
                {
                    if pk_signature(prev_spec) != pk_signature(&spec) {
                        recreate = true;
                    } else {
                        let pk: std::collections::HashSet<&String> =
                            spec.table_schema.primary_key().iter().collect();
                        for (name, col) in spec.table_schema.columns() {
                            if pk.contains(name) {
                                continue;
                            }
                            if let Some(prev_col) = prev_spec.table_schema.columns().get(name)
                                && prev_col.pg_type != col.pg_type
                            {
                                retype_cols.push(name.clone());
                            }
                        }
                        dropped_col = prev_spec
                            .table_schema
                            .columns()
                            .keys()
                            .any(|name| !spec.table_schema.columns().contains_key(name));
                    }
                }

                let child_invalidation = if recreate {
                    Some(TargetChildInvalidation::Destructive)
                } else if dropped_col || !retype_cols.is_empty() {
                    Some(TargetChildInvalidation::Lossy)
                } else {
                    None
                };

                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Update(TableAction {
                        db_key,
                        spec: Some(spec),
                        drop: None,
                        recreate,
                        retype_cols,
                    }),
                    sink: self.sink.get(table_sink),
                    tracking_record: Some(tracking),
                    child_invalidation,
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
                        db_key,
                        spec: None,
                        drop: Some(DropTarget {
                            pg_schema_name: prev_spec.pg_schema_name,
                            table_name: prev_spec.table_name,
                        }),
                        recreate: false,
                        retype_cols: Vec::new(),
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
                match action {
                    TargetAction::Create(a) | TargetAction::Update(a) => {
                        let db = resolve_db(&host_ctx, &a.db_key)?;
                        let spec = a
                            .spec
                            .ok_or_else(|| Error::engine("Postgres table action missing spec"))?;
                        // A PK-signature change can't be applied in place;
                        // drop and recreate the table (rows are replayed via
                        // the Destructive child invalidation set at reconcile).
                        if a.recreate && spec.managed_by.is_system() {
                            drop_table(&db, spec.pg_schema_name.as_deref(), &spec.table_name)
                                .await?;
                        }
                        define_table(&db, &spec).await?;
                        if !a.retype_cols.is_empty() {
                            apply_retypes(&db, &spec, &a.retype_cols).await?;
                        }
                    }
                    TargetAction::Delete(a) => {
                        if let Some(d) = a.drop {
                            let db = resolve_db(&host_ctx, &a.db_key)?;
                            drop_table(&db, d.pg_schema_name.as_deref(), &d.table_name).await?;
                        }
                    }
                }
            }
            Ok(())
        },
        |a: &TableAction| {
            let spec = a
                .spec
                .clone()
                .ok_or_else(|| Error::engine("Postgres table action missing spec"))?;
            Ok(ChildTargetDef::new::<RowState, _>(RowHandler {
                sink: SinkCache::default(),
                db_key: a.db_key.clone(),
                spec,
            }))
        },
    )
}

// ---------------------------------------------------------------------------
// Row handler (child) + sink + vector-index / SQL-command attachments
// ---------------------------------------------------------------------------

struct RowHandler {
    sink: SinkCache<RowAction>,
    db_key: String,
    spec: TableSpec,
}

impl TargetHandler<RowState> for RowHandler {
    type TrackingRecord = Fingerprint;
    type Action = RowAction;

    fn attachments(&self) -> Result<Vec<(String, ChildTargetDef)>> {
        Ok(vec![
            (
                "vector_index".to_string(),
                ChildTargetDef::new::<VectorIndexSpec, _>(VectorIndexHandler {
                    sink: SinkCache::default(),
                    db_key: self.db_key.clone(),
                }),
            ),
            (
                "sql_command_attachment".to_string(),
                ChildTargetDef::new::<SqlCommandSpec, _>(SqlCommandHandler {
                    sink: SinkCache::default(),
                    db_key: self.db_key.clone(),
                }),
            ),
        ])
    }

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<RowState>,
        prev: Vec<Fingerprint>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<RowAction, Fingerprint>>> {
        let pk = stable_key_to_pk(&key)?;
        // Track a cheap fingerprint of the row state (not the full row) so
        // unchanged rows are skipped without persisting every column to LMDB.
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

/// Resolve the live Postgres [`Database`] from the host context by its stable
/// `db_key` (the `ContextKey` name it was provided under).
fn resolve_db(host_ctx: &Arc<ContextStore>, db_key: &str) -> Result<Arc<Database>> {
    host_ctx.resolve::<Database>(db_key).ok_or_else(|| {
        Error::engine(format!(
            "postgres target: database `{db_key}` was not provided in the environment \
             (call Environment::builder().provide_key(&KEY, db))"
        ))
    })
}

impl RowHandler {
    fn row_sink(&self) -> TargetActionSink<RowAction> {
        let db_key = self.db_key.clone();
        let spec = Arc::new(self.spec.clone());
        let statements = Arc::new(RowStatements::new(&self.spec));
        let table_defined = Arc::new(tokio::sync::OnceCell::new());
        TargetActionSink::from_async_fn_with_ctx(
            move |host_ctx, actions: Vec<TargetAction<RowAction>>| {
                let db_key = db_key.clone();
                let spec = spec.clone();
                let statements = statements.clone();
                let table_defined = table_defined.clone();
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
                    // delete-only reconciliation idempotent after a crash or an
                    // external table cleanup.
                    if spec.managed_by.is_system() {
                        table_defined
                            .get_or_try_init(|| define_table(&db, &spec))
                            .await?;
                    }
                    apply_rows(&db, &statements, mutations).await
                }
            },
        )
    }
}

// ---------------------------------------------------------------------------
// Vector-index attachment handler + sink
// ---------------------------------------------------------------------------

struct VectorIndexHandler {
    sink: SinkCache<VectorIndexAction>,
    db_key: String,
}

impl TargetHandler<VectorIndexSpec> for VectorIndexHandler {
    type TrackingRecord = VectorIndexSpec;
    type Action = VectorIndexAction;

    fn reconcile(
        &self,
        _key: StableKey,
        desired: Option<VectorIndexSpec>,
        prev: Vec<VectorIndexSpec>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<VectorIndexAction, VectorIndexSpec>>> {
        let prev_same = desired
            .as_ref()
            .is_some_and(|d| !prev.is_empty() && prev.iter().all(|p| p == d));
        if desired.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        let prev_spec = prev.into_iter().next();
        match desired {
            Some(spec) => Ok(Some(TargetReconcileOutput {
                action: TargetAction::Update(VectorIndexAction { spec: spec.clone() }),
                sink: self.sink.get(|| self.vector_index_sink()),
                tracking_record: Some(spec),
                child_invalidation: None,
            })),
            // No recorded spec means nothing is known to exist (for example a
            // drop that was applied before its commit was interrupted).
            None => Ok(prev_spec.map(|spec| TargetReconcileOutput {
                action: TargetAction::Delete(VectorIndexAction { spec }),
                sink: self.sink.get(|| self.vector_index_sink()),
                tracking_record: None,
                child_invalidation: None,
            })),
        }
    }
}

impl VectorIndexHandler {
    fn vector_index_sink(&self) -> TargetActionSink<VectorIndexAction> {
        let db_key = self.db_key.clone();
        TargetActionSink::from_async_fn_with_ctx(
            move |host_ctx, actions: Vec<TargetAction<VectorIndexAction>>| {
                let db_key = db_key.clone();
                async move {
                    let db = resolve_db(&host_ctx, &db_key)?;
                    for action in actions {
                        let (is_delete, spec) = match action {
                            TargetAction::Delete(a) => (true, a.spec),
                            TargetAction::Create(a) | TargetAction::Update(a) => (false, a.spec),
                        };
                        if is_delete {
                            drop_vector_index(&db, &spec).await?;
                        } else {
                            define_table(
                                &db,
                                &TableSpec {
                                    pg_schema_name: spec.pg_schema_name.clone(),
                                    table_name: spec.table_name.clone(),
                                    table_schema: spec.table_schema.clone(),
                                    managed_by: spec.managed_by,
                                },
                            )
                            .await?;
                            recreate_vector_index(&db, &spec).await?;
                        }
                    }
                    Ok(())
                }
            },
        )
    }
}

// ---------------------------------------------------------------------------
// SQL command attachment handler + sink
// ---------------------------------------------------------------------------

/// Spec for a SQL command attachment (an attachment of a table). Used as both
/// the declared value and the tracking record (equality = no change).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SqlCommandSpec {
    setup_sql: String,
    teardown_sql: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SqlCommandAction {
    /// `Some` to (re)run setup, `None` to remove.
    spec: Option<SqlCommandSpec>,
    /// Teardown SQL from the previous state, run before setup-on-change / on remove.
    prev_teardown_sql: Option<String>,
}

/// First non-None teardown SQL among the previous states.
fn collect_teardown_sql(prev: &[SqlCommandSpec]) -> Option<String> {
    prev.iter().find_map(|p| p.teardown_sql.clone())
}

struct SqlCommandHandler {
    sink: SinkCache<SqlCommandAction>,
    db_key: String,
}

impl TargetHandler<SqlCommandSpec> for SqlCommandHandler {
    type TrackingRecord = SqlCommandSpec;
    type Action = SqlCommandAction;

    fn reconcile(
        &self,
        _key: StableKey,
        desired: Option<SqlCommandSpec>,
        prev: Vec<SqlCommandSpec>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<SqlCommandAction, SqlCommandSpec>>> {
        match desired {
            None => {
                if prev.is_empty() && !prev_may_be_missing {
                    return Ok(None);
                }
                let prev_teardown_sql = collect_teardown_sql(&prev);
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Delete(SqlCommandAction {
                        spec: None,
                        prev_teardown_sql,
                    }),
                    sink: self.sink.get(|| self.sql_command_sink()),
                    tracking_record: None,
                    child_invalidation: None,
                }))
            }
            Some(spec) => {
                let unchanged = !prev_may_be_missing && prev.iter().all(|p| *p == spec);
                if !prev.is_empty() && unchanged {
                    return Ok(None);
                }
                let prev_teardown_sql = collect_teardown_sql(&prev);
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Update(SqlCommandAction {
                        spec: Some(spec.clone()),
                        prev_teardown_sql,
                    }),
                    sink: self.sink.get(|| self.sql_command_sink()),
                    tracking_record: Some(spec),
                    child_invalidation: None,
                }))
            }
        }
    }
}

impl SqlCommandHandler {
    fn sql_command_sink(&self) -> TargetActionSink<SqlCommandAction> {
        let db_key = self.db_key.clone();
        TargetActionSink::from_async_fn_with_ctx(
            move |host_ctx, actions: Vec<TargetAction<SqlCommandAction>>| {
                let db_key = db_key.clone();
                async move {
                    let db = resolve_db(&host_ctx, &db_key)?;
                    for action in actions {
                        let action = match action {
                            TargetAction::Create(a)
                            | TargetAction::Update(a)
                            | TargetAction::Delete(a) => a,
                        };
                        // Run previous teardown first (on change or removal), then setup.
                        if let Some(teardown) = action.prev_teardown_sql {
                            sqlx::query(&teardown)
                                .execute(db.pool())
                                .await
                                .map_err(pg_err)?;
                        }
                        if let Some(spec) = action.spec {
                            sqlx::query(&spec.setup_sql)
                                .execute(db.pool())
                                .await
                                .map_err(pg_err)?;
                        }
                    }
                    Ok(())
                }
            },
        )
    }
}

// ---------------------------------------------------------------------------
// DB I/O
// ---------------------------------------------------------------------------

async fn define_table(db: &Database, spec: &TableSpec) -> Result<()> {
    if spec.managed_by.is_user() {
        return Ok(());
    }
    if let Some(schema) = &spec.pg_schema_name {
        sqlx::query(&format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            quote_ident(schema)
        ))
        .execute(db.pool())
        .await
        .map_err(pg_err)?;
    }
    if schema_uses_pgvector(&spec.table_schema) {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
            .execute(db.pool())
            .await
            .map_err(pg_err)?;
    }
    let mut defs = Vec::new();
    for (name, col) in spec.table_schema.columns() {
        let nullable = if col.nullable && !spec.table_schema.primary_key().contains(name) {
            ""
        } else {
            " NOT NULL"
        };
        defs.push(format!("{} {}{}", quote_ident(name), col.pg_type, nullable));
    }
    let pk = spec
        .table_schema
        .primary_key()
        .iter()
        .map(|name| quote_ident(name))
        .collect::<Vec<_>>()
        .join(", ");
    defs.push(format!("PRIMARY KEY ({pk})"));
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        qualified_table_name(spec),
        defs.join(", ")
    );
    sqlx::query(&sql).execute(db.pool()).await.map_err(pg_err)?;
    // Reconcile an already-existing table's columns to the declared schema.
    reconcile_columns(db, spec).await?;
    Ok(())
}

/// Bring an existing system-managed table's columns in line with the declared
/// schema: add columns that are missing and drop columns that are no longer
/// declared. Reconciling against the live `information_schema` (rather than a
/// stored schema history) is idempotent and self-healing — a column drop that
/// fails (e.g. a dependent view) simply surfaces as an error and is retried on
/// the next run once the dependency is gone.
async fn reconcile_columns(db: &Database, spec: &TableSpec) -> Result<()> {
    let schema_name = spec.pg_schema_name.as_deref().unwrap_or("public");
    let existing: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2",
    )
    .bind(schema_name)
    .bind(&spec.table_name)
    .fetch_all(db.pool())
    .await
    .map_err(pg_err)?;
    let existing: std::collections::BTreeSet<String> = existing.into_iter().collect();

    // Add declared columns the table is missing. Added columns are nullable
    // (a NOT NULL add would fail against existing rows) — the PK columns are
    // fixed at CREATE time and never added here.
    for (name, col) in spec.table_schema.columns() {
        if !existing.contains(name) {
            let sql = format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
                qualified_table_name(spec),
                quote_ident(name),
                col.pg_type,
            );
            sqlx::query(&sql).execute(db.pool()).await.map_err(pg_err)?;
        }
    }

    // Drop columns that are no longer declared.
    let desired: std::collections::BTreeSet<&str> = spec
        .table_schema
        .columns()
        .keys()
        .map(String::as_str)
        .collect();
    for name in &existing {
        if !desired.contains(name.as_str()) {
            let sql = format!(
                "ALTER TABLE {} DROP COLUMN IF EXISTS {}",
                qualified_table_name(spec),
                quote_ident(name),
            );
            sqlx::query(&sql).execute(db.pool()).await.map_err(pg_err)?;
        }
    }
    Ok(())
}

/// The primary-key signature `(column, pg_type)*` used to detect a PK change
/// that requires a destructive table rebuild.
fn pk_signature(spec: &TableSpec) -> Vec<(String, String)> {
    spec.table_schema
        .primary_key()
        .iter()
        .map(|name| {
            let ty = spec
                .table_schema
                .columns()
                .get(name)
                .map(|c| c.pg_type.clone())
                .unwrap_or_default();
            (name.clone(), ty)
        })
        .collect()
}

/// Apply in-place type changes to existing non-PK columns. Tries
/// `ALTER COLUMN ... TYPE` (preserves rows when the cast is valid); on failure
/// (an incompatible cast) it drops and re-adds the column — mirroring Python's
/// drop-retry on `_apply_column_actions`. PK columns are never retyped here (a PK
/// change is handled by a full recreate).
async fn apply_retypes(db: &Database, spec: &TableSpec, cols: &[String]) -> Result<()> {
    if spec.managed_by.is_user() {
        return Ok(());
    }
    let table = qualified_table_name(spec);
    for name in cols {
        let Some(col) = spec.table_schema.columns().get(name) else {
            continue;
        };
        let ident = quote_ident(name);
        let alter = format!(
            "ALTER TABLE {table} ALTER COLUMN {ident} TYPE {}",
            col.pg_type
        );
        if sqlx::query(&alter).execute(db.pool()).await.is_err() {
            // Incompatible cast: drop and re-add (best-effort, column data lost —
            // rows are repopulated by the Lossy child invalidation).
            sqlx::query(&format!(
                "ALTER TABLE {table} DROP COLUMN IF EXISTS {ident}"
            ))
            .execute(db.pool())
            .await
            .map_err(pg_err)?;
            sqlx::query(&format!(
                "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {ident} {}",
                col.pg_type
            ))
            .execute(db.pool())
            .await
            .map_err(pg_err)?;
        }
    }
    Ok(())
}

async fn drop_table(db: &Database, pg_schema_name: Option<&str>, table_name: &str) -> Result<()> {
    let sql = format!(
        "DROP TABLE IF EXISTS {}",
        qualified_table_name_from_parts(pg_schema_name, table_name)
    );
    sqlx::query(&sql).execute(db.pool()).await.map_err(pg_err)?;
    Ok(())
}

/// Rows per upsert / delete statement. A statement's parameter count doesn't
/// grow with its row count (one `text[]` per column), so the bind limit doesn't
/// apply; throughput is flat from about 1,000 to 8,000 rows per statement, and
/// smaller statements spread a large batch over more connections (Python's
/// `_UNNEST_UPSERT_ROWS`).
const ROWS_PER_STATEMENT: usize = 2000;

/// Apply a batch of row mutations: upserts and deletes, each in statements of
/// up to `ROWS_PER_STATEMENT` rows. The statements of a batch run
/// concurrently, each on its own pool connection and committing on its own, as
/// the Python connector's do: a batch is not atomic, and a statement that
/// fails is retried by the next run.
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
    let mut executions = Vec::new();
    for chunk in upserts.chunks(ROWS_PER_STATEMENT) {
        executions.push(execute_with_columns(
            db,
            &statements.upsert_sql,
            statements.upsert_params(chunk)?,
        ));
    }
    for chunk in deletes.chunks(ROWS_PER_STATEMENT) {
        executions.push(execute_with_columns(
            db,
            &statements.delete_sql,
            statements.delete_params(chunk)?,
        ));
    }
    futures::future::try_join_all(executions).await?;
    Ok(())
}

/// Run `sql` with one `text[]` parameter per column.
async fn execute_with_columns(
    db: &Database,
    sql: &str,
    columns: Vec<Vec<Option<String>>>,
) -> Result<()> {
    let mut query = sqlx::query(sql);
    for column in columns {
        query = query.bind(column);
    }
    query.execute(db.pool()).await.map_err(pg_err)?;
    Ok(())
}

async fn recreate_vector_index(db: &Database, spec: &VectorIndexSpec) -> Result<()> {
    drop_vector_index(db, spec).await?;
    let index_name = format!("{}__vector__{}", spec.table_name, spec.name);
    let with_sql = vector_index_with_clause(&spec.method, spec.lists, spec.m, spec.ef_construction);
    let sql = format!(
        "CREATE INDEX {} ON {} USING {} ({} {}){}",
        quote_ident(&index_name),
        qualified_table_name_from_parts(spec.pg_schema_name.as_deref(), &spec.table_name),
        spec.method,
        quote_ident(&spec.column),
        spec.op_class,
        with_sql
    );
    sqlx::query(&sql).execute(db.pool()).await.map_err(pg_err)?;
    Ok(())
}

async fn drop_vector_index(db: &Database, spec: &VectorIndexSpec) -> Result<()> {
    let index_name = format!("{}__vector__{}", spec.table_name, spec.name);
    sqlx::query(&format!(
        "DROP INDEX IF EXISTS {}",
        qualified_index_name(spec.pg_schema_name.as_deref(), &index_name)
    ))
    .execute(db.pool())
    .await
    .map_err(pg_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// SQL builders + helpers
// ---------------------------------------------------------------------------

/// The row statements of one table, built once per row sink.
///
/// Every column travels as one `text[]` parameter holding the chunk's values in
/// Postgres input syntax, and the statement casts each back to the declared
/// column type (an I/O conversion cast). One statement shape thus covers every
/// column type, pgvector, `jsonb`, `bytea`, `interval` and `inet` included, and
/// its text and parameter count don't grow with the row count. Python's
/// connector upserts through the same `unnest` shape with typed arrays.
struct RowStatements {
    /// Every column, in statement order.
    columns: Vec<(String, ColumnDef)>,
    /// `INSERT … SELECT … FROM unnest(…) … ON CONFLICT …`.
    upsert_sql: String,
    /// The primary-key columns, in key order.
    pk_columns: Vec<(String, ColumnDef)>,
    /// `DELETE … WHERE (pk) IN (SELECT … FROM unnest(…))`.
    delete_sql: String,
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
        let table = qualified_table_name(spec);

        let col_list = ident_list(&columns);
        let pk_list = ident_list(&pk_columns);
        let non_pk: Vec<String> = columns
            .iter()
            .filter(|(name, _)| !schema.primary_key().contains(name))
            .map(|(name, _)| format!("{0} = EXCLUDED.{0}", quote_ident(name)))
            .collect();
        let conflict = if non_pk.is_empty() {
            format!("ON CONFLICT ({pk_list}) DO NOTHING")
        } else {
            format!(
                "ON CONFLICT ({pk_list}) DO UPDATE SET {}",
                non_pk.join(", ")
            )
        };
        let upsert_sql = format!(
            "INSERT INTO {table} ({col_list}) SELECT {} FROM unnest({}) AS r({col_list}) {conflict}",
            cast_list(&columns),
            param_list(columns.len()),
        );
        let delete_sql = format!(
            "DELETE FROM {table} WHERE ({pk_list}) IN (SELECT {} FROM unnest({}) AS r({pk_list}))",
            cast_list(&pk_columns),
            param_list(pk_columns.len()),
        );
        Self {
            columns,
            upsert_sql,
            pk_columns,
            delete_sql,
        }
    }

    /// The parameters of [`Self::upsert_sql`] for `rows`: one `text[]` per
    /// column.
    fn upsert_params(&self, rows: &[Map<String, JsonValue>]) -> Result<Vec<Vec<Option<String>>>> {
        let mut params: Vec<Vec<Option<String>>> = self
            .columns
            .iter()
            .map(|_| Vec::with_capacity(rows.len()))
            .collect();
        for row in rows {
            for (param, (name, col)) in params.iter_mut().zip(&self.columns) {
                let value = row.get(name).unwrap_or(&JsonValue::Null);
                param.push(sql_text(value, col)?);
            }
        }
        Ok(params)
    }

    /// The parameters of [`Self::delete_sql`] for `keys`: one `text[]` per
    /// primary-key column.
    fn delete_params(&self, keys: &[Vec<JsonValue>]) -> Result<Vec<Vec<Option<String>>>> {
        let mut params: Vec<Vec<Option<String>>> = self
            .pk_columns
            .iter()
            .map(|_| Vec::with_capacity(keys.len()))
            .collect();
        for key in keys {
            if key.len() != self.pk_columns.len() {
                return Err(Error::engine(
                    "Postgres row target primary key length mismatch",
                ));
            }
            for ((param, (_, col)), part) in params.iter_mut().zip(&self.pk_columns).zip(key) {
                param.push(sql_text(&key_part_value(part, col), col)?);
            }
        }
        Ok(params)
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

/// `r."a"::type_a, r."b"::type_b, …`: the unnested text values cast back to
/// their column types.
fn cast_list(columns: &[(String, ColumnDef)]) -> String {
    columns
        .iter()
        .map(|(name, col)| format!("r.{}::{}", quote_ident(name), col.pg_type))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `$1::text[], $2::text[], …`
fn param_list(count: usize) -> String {
    (1..=count)
        .map(|i| format!("${i}::text[]"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn row_state<R: Serialize>(row: &R, schema: &TableSchema) -> Result<Map<String, JsonValue>> {
    // Reject non-finite floats up front: `serde_json` would turn NaN/±Inf into
    // JSON null, silently corrupting the value (or failing a NOT NULL column).
    crate::finite::ensure_finite(row)
        .map_err(|e| Error::engine(format!("Postgres target row has a {e}")))?;
    let value = serde_json::to_value(row)
        .map_err(|e| Error::engine(format!("serialize Postgres target row: {e}")))?;
    let JsonValue::Object(mut fields) = value else {
        return Err(Error::engine(
            "Postgres target row must serialize to an object",
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
            "unsupported Postgres row key: {other:?}"
        ))),
    }
}

/// A row key stores a boolean key part as the string `"true"`/`"false"` (see
/// [`json_scalar_to_stable_key`]); turn it back into a boolean for a column
/// that holds booleans, so a row with a boolean key can be deleted.
fn key_part_value(value: &JsonValue, col: &ColumnDef) -> JsonValue {
    if let JsonValue::String(s) = value
        && let Ok(b) = s.parse::<bool>()
        && matches!(
            col.pg_type.to_ascii_lowercase().as_str(),
            "boolean" | "bool"
        )
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

/// A column value in Postgres input syntax for its declared type (`None` for
/// SQL NULL), as the row statements bind it. Postgres rejects U+0000 in any
/// text parameter, so it is stripped from every value.
fn sql_text(value: &JsonValue, col: &ColumnDef) -> Result<Option<String>> {
    if value.is_null() {
        return if col.nullable {
            Ok(None)
        } else {
            Err(Error::engine(format!(
                "non-nullable Postgres column of type {} got null",
                col.pg_type
            )))
        };
    }
    let lower = col.pg_type.to_ascii_lowercase();
    let text = if is_text_type(&lower) {
        value_to_string(value)?.to_string()
    } else if lower == "boolean" || lower == "bool" {
        value
            .as_bool()
            .map(|b| b.to_string())
            .ok_or_else(|| Error::engine("boolean column requires bool JSON value"))?
    } else if lower.starts_with("interval") {
        interval_text(value)?
    } else if is_numeric_type(&lower) {
        match value {
            JsonValue::Number(n) => n.to_string(),
            JsonValue::String(s) if is_decimal_type(&lower) && is_decimal_literal(s) => s.clone(),
            _ => {
                return Err(Error::engine(format!(
                    "numeric column {} requires numeric JSON value",
                    col.pg_type
                )));
            }
        }
    } else if _is_pgvector_type(&lower) {
        vector_text(value)?
    } else if lower == "bytea" {
        // A Rust `Vec<u8>` serializes to a JSON array of byte values; encode it
        // as Postgres hex bytea input (`\x…`). Its JSON text (`[104,105]`)
        // would be invalid bytea input.
        bytea_text(value)?
    } else if lower == "json" || lower == "jsonb" {
        // Postgres rejects U+0000 in json/jsonb on parse, and serde serializes a
        // NUL inside a nested string/key as the `\u0000` escape (which the
        // final byte-level strip can't catch). Remove NULs from every string
        // and object key before serializing.
        sanitize_json_nul(value).to_string()
    } else {
        // Any other type (uuid, date/time, inet, …) takes its value's text, or
        // its JSON text for a non-scalar.
        match value {
            JsonValue::String(s) => s.clone(),
            JsonValue::Number(n) => n.to_string(),
            JsonValue::Bool(b) => b.to_string(),
            other => other.to_string(),
        }
    };
    Ok(Some(strip_nul(text)))
}

fn interval_text(value: &JsonValue) -> Result<String> {
    if let Some((secs, nanos)) = crate::row_schema::serialized_duration_parts(value) {
        return Ok(format!("{secs}.{nanos:09} seconds"));
    }
    match value {
        JsonValue::String(s) => Ok(s.clone()),
        _ => Err(Error::engine(
            "interval column requires a duration or string JSON value",
        )),
    }
}

fn value_to_string(value: &JsonValue) -> Result<&str> {
    value
        .as_str()
        .ok_or_else(|| Error::engine("text column requires string JSON value"))
}

/// Encode a JSON value as Postgres hex `bytea` input. Accepts a byte array (the
/// serde default for `Vec<u8>`) or a string (encoded as its UTF-8 bytes).
fn bytea_text(value: &JsonValue) -> Result<String> {
    let bytes: Vec<u8> = match value {
        JsonValue::Array(arr) => arr
            .iter()
            .map(|v| {
                v.as_u64()
                    .filter(|n| *n <= 255)
                    .map(|n| n as u8)
                    .ok_or_else(|| Error::engine("bytea array elements must be integers 0..=255"))
            })
            .collect::<Result<Vec<u8>>>()?,
        JsonValue::String(s) => s.as_bytes().to_vec(),
        _ => {
            return Err(Error::engine(
                "bytea column requires a byte array or string JSON value",
            ));
        }
    };
    let mut hex = String::with_capacity(2 + bytes.len() * 2);
    hex.push_str("\\x");
    for b in &bytes {
        hex.push_str(&format!("{b:02x}"));
    }
    Ok(hex)
}

/// A JSON number array as pgvector input: `[1,2,3]`.
fn vector_text(value: &JsonValue) -> Result<String> {
    let arr = value
        .as_array()
        .ok_or_else(|| Error::engine("vector column requires JSON array"))?;
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

/// Strip U+0000, which Postgres rejects in text, from a value's text.
fn strip_nul(value: String) -> String {
    if value.contains('\0') {
        value.replace('\0', "")
    } else {
        value
    }
}

/// Build the pgvector index `WITH (...)` clause, gating each parameter by the
/// index method: `lists` is only valid for `ivfflat`, `m`/`ef_construction` only
/// for `hnsw`. Emitting a param for the wrong method produces invalid DDL.
fn vector_index_with_clause(
    method: &str,
    lists: Option<u32>,
    m: Option<u32>,
    ef_construction: Option<u32>,
) -> String {
    let mut parts = Vec::new();
    match method.to_ascii_lowercase().as_str() {
        "ivfflat" => {
            if let Some(lists) = lists {
                parts.push(format!("lists = {lists}"));
            }
        }
        "hnsw" => {
            if let Some(m) = m {
                parts.push(format!("m = {m}"));
            }
            if let Some(ef) = ef_construction {
                parts.push(format!("ef_construction = {ef}"));
            }
        }
        _ => {}
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" WITH ({})", parts.join(", "))
    }
}

/// Recursively strip NUL (U+0000) from all strings and object keys in a JSON
/// value, so it can be stored in a Postgres `json`/`jsonb` column (which rejects
/// U+0000 on parse).
fn sanitize_json_nul(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::String(s) if s.contains('\0') => JsonValue::String(s.replace('\0', "")),
        JsonValue::Array(items) => JsonValue::Array(items.iter().map(sanitize_json_nul).collect()),
        JsonValue::Object(map) => JsonValue::Object(
            map.iter()
                .map(|(k, v)| (k.replace('\0', ""), sanitize_json_nul(v)))
                .collect(),
        ),
        other => other.clone(),
    }
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

fn validate_pg_type(value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '(' | ')' | ',' | ' '))
    {
        return Err(Error::engine(format!("invalid Postgres type: {value}")));
    }
    Ok(())
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn qualified_table_name(spec: &TableSpec) -> String {
    qualified_table_name_from_parts(spec.pg_schema_name.as_deref(), &spec.table_name)
}

fn qualified_table_name_from_parts(schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(table)),
        None => quote_ident(table),
    }
}

fn qualified_index_name(schema: Option<&str>, index: &str) -> String {
    match schema {
        Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(index)),
        None => quote_ident(index),
    }
}

fn schema_uses_pgvector(schema: &TableSchema) -> bool {
    schema
        .columns()
        .values()
        .any(|col| _is_pgvector_type(&col.pg_type.to_ascii_lowercase()))
}

fn _is_pgvector_type(lower: &str) -> bool {
    lower.starts_with("vector(") || lower.starts_with("halfvec(")
}

fn pgvector_op_class(pg_type: &str, metric: &str) -> Result<&'static str> {
    let lower = pg_type.to_ascii_lowercase();
    let base = if lower.starts_with("vector(") {
        "vector"
    } else if lower.starts_with("halfvec(") {
        "halfvec"
    } else {
        return Err(Error::engine(format!(
            "Postgres column type {pg_type:?} is not a pgvector type"
        )));
    };
    match (base, metric) {
        ("vector", "cosine") => Ok("vector_cosine_ops"),
        ("vector", "l2") => Ok("vector_l2_ops"),
        ("vector", "ip") => Ok("vector_ip_ops"),
        ("halfvec", "cosine") => Ok("halfvec_cosine_ops"),
        ("halfvec", "l2") => Ok("halfvec_l2_ops"),
        ("halfvec", "ip") => Ok("halfvec_ip_ops"),
        _ => Err(Error::engine(format!(
            "unsupported pgvector distance metric: {metric}"
        ))),
    }
}

fn is_text_type(lower: &str) -> bool {
    lower == "text" || lower == "varchar" || lower.starts_with("varchar(")
}

fn is_numeric_type(lower: &str) -> bool {
    matches!(
        lower,
        "smallint"
            | "integer"
            | "int"
            | "int2"
            | "int4"
            | "int8"
            | "bigint"
            | "real"
            | "float4"
            | "double precision"
            | "float8"
    ) || is_decimal_type(lower)
}

fn is_decimal_type(lower: &str) -> bool {
    lower == "numeric"
        || lower == "decimal"
        || lower.starts_with("numeric(")
        || lower.starts_with("decimal(")
}

fn is_decimal_literal(value: &str) -> bool {
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    let mut parts = unsigned.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next();
    !whole.is_empty()
        && whole.bytes().all(|b| b.is_ascii_digit())
        && fraction.is_none_or(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
        && parts.next().is_none()
}

fn pg_err(e: sqlx::Error) -> Error {
    Error::engine(format!("postgres: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_index_orphan_delete_without_recorded_spec_is_a_no_op() {
        let handler = VectorIndexHandler {
            sink: SinkCache::default(),
            db_key: "db".to_string(),
        };
        // What the engine sends after a drop was applied but its commit was
        // interrupted: no live previous state, and the sink may be missing.
        let output = handler
            .reconcile(StableKey::Str("idx".into()), None, Vec::new(), true)
            .unwrap();
        assert!(output.is_none());
    }
    use serde::Serialize;

    #[derive(Serialize)]
    struct CodeRow {
        id: i64,
        filename: String,
        code: String,
        embedding: Vec<f32>,
        start_line: i32,
        end_line: i32,
    }

    fn code_schema() -> TableSchema {
        TableSchema::new(
            [
                ("id", ColumnDef::new("bigint")),
                ("filename", ColumnDef::new("text")),
                ("code", ColumnDef::new("text")),
                ("embedding", ColumnDef::new("vector(3)")),
                ("start_line", ColumnDef::new("integer")),
                ("end_line", ColumnDef::new("integer")),
            ],
            ["id"],
        )
        .unwrap()
    }

    #[test]
    fn table_schema_rejects_unknown_primary_key() {
        let err = TableSchema::new([("id", ColumnDef::new("bigint"))], ["missing"]).unwrap_err();
        assert!(err.to_string().contains("primary key column"));
    }

    #[test]
    fn row_state_generates_primary_key_and_upsert_sql() {
        let schema = code_schema();
        let spec = TableSpec {
            pg_schema_name: Some("coco_examples".to_string()),
            table_name: "code_embeddings".to_string(),
            table_schema: schema.clone(),
            managed_by: ManagedBy::System,
        };
        let row = CodeRow {
            id: 42,
            filename: "src/lib.rs".to_string(),
            code: "fn answer() -> i32 { 42 }".to_string(),
            // Exact in f32, so the text is the same through the f64 JSON form.
            embedding: vec![0.5, 0.25, 0.75],
            start_line: 1,
            end_line: 3,
        };

        let fields = row_state(&row, &schema).unwrap();
        assert_eq!(
            pk_stable_key(&fields, schema.primary_key()).unwrap(),
            StableKey::Int(42)
        );

        let statements = RowStatements::new(&spec);
        assert_eq!(
            statements.upsert_sql,
            "INSERT INTO \"coco_examples\".\"code_embeddings\" (\"code\", \"embedding\", \"end_line\", \
             \"filename\", \"id\", \"start_line\") SELECT r.\"code\"::text, r.\"embedding\"::vector(3), \
             r.\"end_line\"::integer, r.\"filename\"::text, r.\"id\"::bigint, r.\"start_line\"::integer \
             FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[]) \
             AS r(\"code\", \"embedding\", \"end_line\", \"filename\", \"id\", \"start_line\") \
             ON CONFLICT (\"id\") DO UPDATE SET \"code\" = EXCLUDED.\"code\", \
             \"embedding\" = EXCLUDED.\"embedding\", \"end_line\" = EXCLUDED.\"end_line\", \
             \"filename\" = EXCLUDED.\"filename\", \"start_line\" = EXCLUDED.\"start_line\""
        );
        // One `text[]` per column, in statement order, over every row.
        let params = statements.upsert_params(&[fields.clone(), fields]).unwrap();
        assert_eq!(params.len(), 6);
        assert_eq!(params[1], vec![Some("[0.5,0.25,0.75]".to_string()); 2]);
        assert_eq!(params[4], vec![Some("42".to_string()); 2]);
    }

    #[test]
    fn delete_statement_takes_one_array_per_primary_key_column() {
        let schema = code_schema();
        let spec = TableSpec {
            pg_schema_name: Some("coco_examples".to_string()),
            table_name: "code_embeddings".to_string(),
            table_schema: schema,
            managed_by: ManagedBy::System,
        };
        let statements = RowStatements::new(&spec);
        assert_eq!(
            statements.delete_sql,
            "DELETE FROM \"coco_examples\".\"code_embeddings\" WHERE (\"id\") IN \
             (SELECT r.\"id\"::bigint FROM unnest($1::text[]) AS r(\"id\"))"
        );
        let params = statements
            .delete_params(&[vec![JsonValue::from(42)], vec![JsonValue::from(43)]])
            .unwrap();
        assert_eq!(
            params,
            vec![vec![Some("42".to_string()), Some("43".to_string())]]
        );
        assert!(statements.delete_params(&[vec![]]).is_err());
    }

    #[test]
    fn row_with_boolean_primary_key_can_be_deleted() {
        let spec = TableSpec {
            pg_schema_name: None,
            table_name: "flags".to_string(),
            table_schema: TableSchema::new(
                [
                    ("tenant", ColumnDef::new("text")),
                    ("active", ColumnDef::new("boolean")),
                ],
                ["tenant", "active"],
            )
            .unwrap(),
            managed_by: ManagedBy::System,
        };
        let key = StableKey::Array(Arc::from([
            json_scalar_to_stable_key(&JsonValue::from("true")).unwrap(),
            json_scalar_to_stable_key(&JsonValue::Bool(true)).unwrap(),
        ]));
        let statements = RowStatements::new(&spec);
        let params = statements
            .delete_params(&[stable_key_to_pk(&key).unwrap()])
            .unwrap();
        // A text key part spelled "true" stays a string; the boolean key part
        // is bound as a boolean's text.
        assert_eq!(
            params,
            vec![
                vec![Some("true".to_string())],
                vec![Some("true".to_string())]
            ]
        );
        assert!(statements.delete_sql.contains("r.\"active\"::boolean"));
    }

    #[test]
    fn vector_index_options_select_pgvector_op_class() {
        assert_eq!(
            pgvector_op_class("vector(384)", "cosine").unwrap(),
            "vector_cosine_ops"
        );
        assert_eq!(
            pgvector_op_class("halfvec(384)", "l2").unwrap(),
            "halfvec_l2_ops"
        );
        assert!(pgvector_op_class("text", "cosine").is_err());
    }

    #[test]
    fn sql_text_strips_nul_and_keeps_quotes() {
        // Postgres rejects U+0000 in a text parameter; a quote needs no escape
        // in a bound parameter.
        let col = ColumnDef::new("text");
        assert_eq!(
            sql_text(&JsonValue::from("a\0b'c"), &col).unwrap(),
            Some("ab'c".to_string())
        );
        assert_eq!(
            sql_text(&JsonValue::from("plain"), &col).unwrap(),
            Some("plain".to_string())
        );
        assert!(sql_text(&JsonValue::Null, &col).is_err());
        assert_eq!(sql_text(&JsonValue::Null, &col.nullable()).unwrap(), None);
    }
}

// ---------------------------------------------------------------------------
// Source: read rows from a Postgres table
// ---------------------------------------------------------------------------

/// Rows buffered between the reading task and the consumer of a
/// [`read_table`] stream.
const READ_BUFFER_ROWS: usize = 256;

/// Read every row of `table_name` (in the connection's default search path) as
/// `T`, as a stream.
///
/// The rows are read inside one `REPEATABLE READ, READ ONLY` transaction, so
/// the whole table is observed as one consistent snapshot, and they are
/// streamed from the server as the consumer takes them (Python's
/// `PgTableSource.fetch_rows()` cursor): at most `READ_BUFFER_ROWS` rows are
/// held ahead of the consumer. Dropping the stream ends the read.
///
/// `T` must be `Deserialize` (column names map to struct fields; unknown columns
/// are ignored) and is usually also `Serialize` so each row can key memoized
/// per-row work via `ctx.mount_each`, which takes the collected rows
/// (`futures::TryStreamExt::try_collect`). Incrementality comes from
/// memoization (unchanged rows skip processing) plus target-state
/// reconciliation (rows that disappear from the source have their derived
/// target states deleted).
pub async fn read_table<T>(
    db: &Database,
    table_name: &str,
) -> Result<impl Stream<Item = Result<T>> + Send + 'static>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    read_table_with_options(db, table_name, ReadTableOptions::default()).await
}

/// [`read_table`] with explicit [`ReadTableOptions`].
pub async fn read_table_with_options<T>(
    db: &Database,
    table_name: &str,
    options: ReadTableOptions,
) -> Result<impl Stream<Item = Result<T>> + Send + 'static>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    validate_ident(table_name, "table name")?;
    if let Some(schema) = &options.pg_schema_name {
        validate_ident(schema, "schema name")?;
    }
    let select_list = match &options.columns {
        Some(columns) => {
            if columns.is_empty() {
                return Err(Error::engine("Postgres source columns cannot be empty"));
            }
            columns
                .iter()
                .map(|column| {
                    validate_ident(column, "column name")?;
                    Ok(quote_ident(column))
                })
                .collect::<Result<Vec<_>>>()?
                .join(", ")
        }
        None => "*".to_string(),
    };
    let sql = format!(
        "SELECT {select_list} FROM {}",
        qualified_table_name_from_parts(options.pg_schema_name.as_deref(), table_name)
    );
    let mut tx = db.pool().begin().await.map_err(pg_err)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(pg_err)?;
    let (mut sender, receiver) = futures::channel::mpsc::channel::<Result<T>>(READ_BUFFER_ROWS);
    // The reading task owns the transaction; the stream is the channel's
    // receiving end. The task stops at the first error, or when the receiver
    // is dropped.
    tokio::spawn(async move {
        let mut rows = sqlx::query(&sql).fetch(&mut *tx);
        while let Some(row) = rows.next().await {
            let item = row
                .map_err(|e| Error::engine(format!("postgres source read failed: {e}")))
                .and_then(|row| source_row::<T>(&row));
            let failed = item.is_err();
            if sender.send(item).await.is_err() || failed {
                break;
            }
        }
        drop(rows);
        // Nothing to commit in a read-only transaction; ending it releases the
        // snapshot and returns the connection to the pool.
        let _ = tx.commit().await;
    });
    Ok(receiver)
}

/// Read every row paired with a stable key derived by `key_fn`, ready to feed
/// [`Ctx::mount_each`](crate::Ctx::mount_each) once collected. Reads use the
/// same repeatable-read snapshot and streaming as [`read_table`].
pub async fn read_table_items<T, K, S>(
    db: &Database,
    table_name: &str,
    key_fn: K,
) -> Result<impl Stream<Item = Result<(StableKey, T)>> + Send + 'static>
where
    T: serde::de::DeserializeOwned + Send + 'static,
    K: Fn(&T) -> S + Send + 'static,
    S: crate::target_state::IntoStableKey,
{
    read_table_items_with_options(db, table_name, ReadTableOptions::default(), key_fn).await
}

/// [`read_table_items`] with explicit [`ReadTableOptions`].
pub async fn read_table_items_with_options<T, K, S>(
    db: &Database,
    table_name: &str,
    options: ReadTableOptions,
    key_fn: K,
) -> Result<impl Stream<Item = Result<(StableKey, T)>> + Send + 'static>
where
    T: serde::de::DeserializeOwned + Send + 'static,
    K: Fn(&T) -> S + Send + 'static,
    S: crate::target_state::IntoStableKey,
{
    let rows = read_table_with_options::<T>(db, table_name, options).await?;
    Ok(rows.map(move |row| {
        row.map(|row| {
            let key = key_fn(&row).into_stable_key();
            (key, row)
        })
    }))
}

/// Convert one source row to `T` through its JSON form.
fn source_row<T: serde::de::DeserializeOwned>(row: &sqlx::postgres::PgRow) -> Result<T> {
    let json = pg_row_to_json(row)?;
    serde_json::from_value(json)
        .map_err(|e| Error::engine(format!("postgres source row does not match row type: {e}")))
}

fn pg_row_to_json(row: &sqlx::postgres::PgRow) -> Result<JsonValue> {
    use sqlx::{Column, Row, TypeInfo};
    let mut map = Map::new();
    for (i, col) in row.columns().iter().enumerate() {
        let ty = col.type_info().name().to_uppercase();
        let value = pg_col_to_json(row, i, &ty)
            .map_err(|e| Error::engine(format!("postgres source column {}: {e}", col.name())))?;
        map.insert(col.name().to_string(), value);
    }
    Ok(JsonValue::Object(map))
}

fn pg_col_to_json(
    row: &sqlx::postgres::PgRow,
    i: usize,
    ty: &str,
) -> cocoindex_utils::error::Result<JsonValue> {
    use sqlx::Row;
    macro_rules! get {
        ($t:ty) => {
            row.try_get::<Option<$t>, _>(i)
                .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?
        };
    }
    let num = |v: f64| serde_json::Number::from_f64(v).map_or(JsonValue::Null, JsonValue::Number);
    Ok(match ty {
        "BOOL" => get!(bool).map_or(JsonValue::Null, JsonValue::Bool),
        "INT2" => get!(i16).map_or(JsonValue::Null, JsonValue::from),
        "INT4" => get!(i32).map_or(JsonValue::Null, JsonValue::from),
        "INT8" => get!(i64).map_or(JsonValue::Null, JsonValue::from),
        "FLOAT4" => get!(f32).map_or(JsonValue::Null, |v| num(v as f64)),
        "FLOAT8" => get!(f64).map_or(JsonValue::Null, num),
        "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" | "CHAR" | "CITEXT" => {
            get!(String).map_or(JsonValue::Null, JsonValue::String)
        }
        "TIMESTAMP" => get!(chrono::NaiveDateTime)
            .map_or(JsonValue::Null, |d| JsonValue::String(d.to_string())),
        "TIMESTAMPTZ" => get!(chrono::DateTime<chrono::Utc>)
            .map_or(JsonValue::Null, |d| JsonValue::String(d.to_rfc3339())),
        "DATE" => {
            get!(chrono::NaiveDate).map_or(JsonValue::Null, |d| JsonValue::String(d.to_string()))
        }
        _ => {
            return Err(cocoindex_utils::error::Error::internal_msg(format!(
                "unsupported Postgres source column type {ty:?}"
            )));
        }
    })
}

#[cfg(test)]
mod review_fix_tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr as _;

    // --- vector index WITH-clause gating (pgvector) ---

    #[test]
    fn ivfflat_emits_only_lists() {
        assert_eq!(
            vector_index_with_clause("ivfflat", Some(100), Some(16), Some(64)),
            " WITH (lists = 100)"
        );
    }

    #[test]
    fn hnsw_emits_only_m_and_ef() {
        assert_eq!(
            vector_index_with_clause("hnsw", Some(100), Some(16), Some(64)),
            " WITH (m = 16, ef_construction = 64)"
        );
    }

    #[test]
    fn hnsw_ignores_lists() {
        // Regression: `lists` set on an hnsw index used to leak into the DDL.
        assert_eq!(vector_index_with_clause("HNSW", Some(100), None, None), "");
    }

    #[test]
    fn no_params_no_clause() {
        assert_eq!(vector_index_with_clause("ivfflat", None, None, None), "");
        assert_eq!(vector_index_with_clause("hnsw", None, None, None), "");
    }

    // --- jsonb NUL sanitization ---

    #[test]
    fn jsonb_literal_strips_nul_in_nested_strings_and_keys() {
        let value = serde_json::json!({
            "na\0me": "al\0ice",
            "tags": ["a\0b", {"k\0": "v\0"}],
        });
        let text = sql_text(&value, &ColumnDef::new("jsonb")).unwrap().unwrap();
        assert!(!text.contains('\0'), "raw NUL leaked: {text}");
        assert!(!text.contains("\\u0000"), "escaped NUL leaked: {text}");
        let parsed: JsonValue = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed["name"], serde_json::json!("alice"));
        assert_eq!(parsed["tags"][0], serde_json::json!("ab"));
        assert_eq!(parsed["tags"][1]["k"], serde_json::json!("v"));
    }

    #[test]
    fn sanitize_json_nul_leaves_clean_values_untouched() {
        let value = serde_json::json!({"a": [1, 2], "b": "ok"});
        assert_eq!(sanitize_json_nul(&value), value);
    }

    #[test]
    fn numeric_literal_accepts_rust_decimal_string_encoding() {
        let value = serde_json::to_value(Decimal::from_str("1234567890.012300").unwrap()).unwrap();
        assert_eq!(
            sql_text(&value, &ColumnDef::new("numeric(28, 6)")).unwrap(),
            Some("1234567890.012300".to_string())
        );
        assert!(
            sql_text(
                &JsonValue::String("1); DROP TABLE rows; --".to_string()),
                &ColumnDef::new("numeric")
            )
            .is_err()
        );
    }

    #[test]
    fn interval_literal_accepts_std_duration_encoding() {
        let value = serde_json::to_value(std::time::Duration::new(12, 345_000_000)).unwrap();
        assert_eq!(
            sql_text(&value, &ColumnDef::new("interval")).unwrap(),
            Some("12.345000000 seconds".to_string())
        );
    }

    #[test]
    fn explicit_dimensionless_vector_type_is_not_an_unresolved_derive_sentinel() {
        let schema = TableSchema::new(
            [
                ("id", ColumnDef::new("bigint")),
                ("embedding", ColumnDef::new("vector")),
            ],
            ["id"],
        )
        .unwrap();
        schema.validate_vector_dimensions().unwrap();
    }

    #[test]
    fn table_schema_rejects_duplicate_columns() {
        let error = TableSchema::new(
            [
                ("id", ColumnDef::new("bigint")),
                ("id", ColumnDef::new("text")),
            ],
            ["id"],
        )
        .unwrap_err();
        assert!(error.to_string().contains("duplicate column \"id\""));
    }
}
