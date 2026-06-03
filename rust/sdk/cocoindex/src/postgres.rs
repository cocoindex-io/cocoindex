//! Postgres source and target helpers.
//!
//! Table targets reconcile declared rows against the previous run: changed rows
//! are upserted, unchanged rows are skipped, and rows no longer declared are
//! deleted. System-managed targets also create/drop table DDL and attachments
//! such as vector indexes and SQL setup commands.
//!
//! Use [`table_target`] to build a composable target state, [`declare_table_target`]
//! inside the current component, or [`mount_table_target`] when rows must be
//! declared immediately. [`read_table`] and [`read_table_items`] read source rows
//! for use with `Ctx::mount_each`.

use std::collections::BTreeMap;
use std::sync::Arc;

use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::statediff::{
    ManagedBy, ManagedTargetOptions, MutualTrackingRecord, resolve_system_transition,
};
use crate::target_state::{
    ChildTargetDef, StableKey, TargetAction, TargetActionSink, TargetChildInvalidation,
    TargetHandler, TargetReconcileOutput, TargetState, TargetStateProvider, declare_target_state,
    declare_target_state_with_child, mount_target, register_root_target_states_provider,
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
            out.insert(name, def);
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
        let columns = T::schema_fields()
            .into_iter()
            .map(|f| (f.name.clone(), postgres_column_def(&f)));
        Self::new(columns, primary_key)
    }
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
            if *half {
                format!("halfvec({dim})")
            } else {
                format!("vector({dim})")
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
#[derive(Clone)]
pub struct TableTarget {
    pg_schema_name: Option<Arc<str>>,
    table_name: Arc<str>,
    table_schema: TableSchema,
    managed_by: ManagedBy,
    table_provider: TargetStateProvider<TableSpec>,
    rows: TargetStateProvider<RowState>,
}

/// Build a composable [`TargetState`] for a Postgres table (the spec
/// constructor). System-managed. Pass it to
/// [`declare_table_target`]/[`mount_table_target`] or the generic
/// [`declare_target_state_with_child`]/[`mount_target`].
pub fn table_target(
    ctx: &Ctx,
    db: &Database,
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
    db: &Database,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    pg_schema_name: Option<&str>,
    options: ManagedTargetOptions,
) -> Result<TargetState<TableSpec>> {
    let table_name = table_name.into();
    validate_ident(&table_name, "table name")?;
    if let Some(schema) = pg_schema_name {
        validate_ident(schema, "schema name")?;
    }
    let provider = register_root_target_states_provider(
        ctx,
        format!(
            "cocoindex/postgres/table/{}/{}/{}",
            db.state_id(),
            pg_schema_name.unwrap_or("public"),
            table_name
        ),
        TableHandler { db: db.clone() },
    )?;
    Ok(provider.target_state(
        "default",
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
/// use [`mount_table_target`] when rows must be declared immediately.
pub fn declare_table_target(
    ctx: &Ctx,
    db: &Database,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    pg_schema_name: Option<&str>,
) -> Result<TableTarget> {
    let ts = table_target(ctx, db, table_name, table_schema, pg_schema_name)?;
    let spec = ts.value().clone();
    let table_provider = ts.provider().clone();
    let rows = declare_target_state_with_child::<TableSpec, RowState>(ctx, ts)?;
    Ok(table_target_handle(spec, table_provider, rows))
}

/// Mount a Postgres table target **foreground** (rows can be declared
/// immediately) and return a handle. System-managed.
pub async fn mount_table_target(
    ctx: &Ctx,
    db: &Database,
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
    db: &Database,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    pg_schema_name: Option<&str>,
    options: ManagedTargetOptions,
) -> Result<TableTarget> {
    let ts = table_target_with_options(ctx, db, table_name, table_schema, pg_schema_name, options)?;
    let spec = ts.value().clone();
    let table_provider = ts.provider().clone();
    let rows = mount_target::<TableSpec, RowState>(ctx, ts).await?;
    Ok(table_target_handle(spec, table_provider, rows))
}

fn table_target_handle(
    spec: TableSpec,
    table_provider: TargetStateProvider<TableSpec>,
    rows: TargetStateProvider<RowState>,
) -> TableTarget {
    TableTarget {
        pg_schema_name: spec.pg_schema_name.map(Arc::from),
        table_name: Arc::from(spec.table_name),
        table_schema: spec.table_schema,
        managed_by: spec.managed_by,
        table_provider,
        rows,
    }
}

impl TableTarget {
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

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
            self.table_provider.attachment(ctx, "vector_index")?;
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
        let provider: TargetStateProvider<SqlCommandSpec> = self
            .table_provider
            .attachment(ctx, "sql_command_attachment")?;
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
// Table container handler (root) + sink yielding row children + vector-index
// attachment
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
                        spec: Some(spec),
                        drop: None,
                        recreate,
                        retype_cols,
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
                        drop: Some(DropTarget {
                            pg_schema_name: prev_spec.pg_schema_name,
                            table_name: prev_spec.table_name,
                        }),
                        recreate: false,
                        retype_cols: Vec::new(),
                    }),
                    sink: self.table_sink(),
                    tracking_record: None,
                    child_invalidation: Some(TargetChildInvalidation::Destructive),
                }))
            }
        }
    }

    fn attachments(&self) -> Result<Vec<(String, ChildTargetDef)>> {
        Ok(vec![
            (
                "vector_index".to_string(),
                ChildTargetDef::new::<VectorIndexSpec, _>(VectorIndexHandler {
                    db: self.db.clone(),
                }),
            ),
            (
                "sql_command_attachment".to_string(),
                ChildTargetDef::new::<SqlCommandSpec, _>(SqlCommandHandler {
                    db: self.db.clone(),
                }),
            ),
        ])
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
                                    Error::engine("Postgres table action missing spec")
                                })?;
                                // A PK-signature change can't be applied in place;
                                // drop and recreate the table (rows are replayed via
                                // the Destructive child invalidation set at reconcile).
                                if a.recreate && spec.managed_by.is_system() {
                                    drop_table(
                                        &db,
                                        spec.pg_schema_name.as_deref(),
                                        &spec.table_name,
                                    )
                                    .await?;
                                }
                                define_table(&db, &spec).await?;
                                if !a.retype_cols.is_empty() {
                                    apply_retypes(&db, &spec, &a.retype_cols).await?;
                                }
                                out.push(Some(ChildTargetDef::new::<RowState, _>(RowHandler {
                                    db: db.clone(),
                                    spec,
                                })));
                            }
                            TargetAction::Delete(a) => {
                                if let Some(d) = a.drop {
                                    drop_table(&db, d.pg_schema_name.as_deref(), &d.table_name)
                                        .await?;
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
// Vector-index attachment handler + sink
// ---------------------------------------------------------------------------

struct VectorIndexHandler {
    db: Database,
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
        let prev_spec = prev.into_iter().next();
        let prev_same = desired
            .as_ref()
            .is_some_and(|d| prev_spec.as_ref() == Some(d));
        if desired.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired.is_none() && prev_spec.is_none() && !prev_may_be_missing {
            return Ok(None);
        }
        match desired {
            Some(spec) => Ok(Some(TargetReconcileOutput {
                action: TargetAction::Update(VectorIndexAction { spec: spec.clone() }),
                sink: self.vector_index_sink(),
                tracking_record: Some(spec),
                child_invalidation: None,
            })),
            None => {
                let spec = prev_spec.expect("delete path implies a previous index spec");
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Delete(VectorIndexAction { spec }),
                    sink: self.vector_index_sink(),
                    tracking_record: None,
                    child_invalidation: None,
                }))
            }
        }
    }
}

impl VectorIndexHandler {
    fn vector_index_sink(&self) -> TargetActionSink<VectorIndexAction> {
        let db = self.db.clone();
        TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<VectorIndexAction>>| {
            let db = db.clone();
            async move {
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
        })
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
    db: Database,
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
                    sink: self.sql_command_sink(),
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
                    sink: self.sql_command_sink(),
                    tracking_record: Some(spec),
                    child_invalidation: None,
                }))
            }
        }
    }
}

impl SqlCommandHandler {
    fn sql_command_sink(&self) -> TargetActionSink<SqlCommandAction> {
        let db = self.db.clone();
        TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<SqlCommandAction>>| {
            let db = db.clone();
            async move {
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
        })
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

async fn apply_rows(
    db: &Database,
    spec: &TableSpec,
    mutations: Vec<(Vec<JsonValue>, Option<RowState>)>,
) -> Result<()> {
    if mutations.is_empty() {
        return Ok(());
    }
    // Ensure the table exists before any row mutation. This keeps delete-only
    // reconciliation idempotent after a crash or external table cleanup.
    if spec.managed_by.is_system() {
        define_table(db, spec).await?;
    }
    // Apply the whole batch atomically.
    let mut tx = db.pool().begin().await.map_err(pg_err)?;
    for (pk, state) in mutations {
        match state {
            Some(state) => {
                let sql = upsert_sql(spec, &state.fields)?;
                sqlx::query(&sql).execute(&mut *tx).await.map_err(pg_err)?;
            }
            None => {
                let sql = delete_sql(spec, &pk)?;
                sqlx::query(&sql).execute(&mut *tx).await.map_err(pg_err)?;
            }
        }
    }
    tx.commit().await.map_err(pg_err)?;
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

fn upsert_sql(spec: &TableSpec, fields: &Map<String, JsonValue>) -> Result<String> {
    let cols = spec
        .table_schema
        .columns()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
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
                .get(name)
                .expect("schema column");
            let value = fields.get(name).unwrap_or(&JsonValue::Null);
            sql_literal(value, col)
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");
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
        .map(|name| format!("{} = EXCLUDED.{}", quote_ident(name), quote_ident(name)))
        .collect::<Vec<_>>();
    let conflict = if non_pk.is_empty() {
        format!("ON CONFLICT ({pk_sql}) DO NOTHING")
    } else {
        format!("ON CONFLICT ({pk_sql}) DO UPDATE SET {}", non_pk.join(", "))
    };
    Ok(format!(
        "INSERT INTO {} ({col_sql}) VALUES ({values}) {conflict}",
        qualified_table_name(spec)
    ))
}

fn delete_sql(spec: &TableSpec, pk: &[JsonValue]) -> Result<String> {
    if pk.len() != spec.table_schema.primary_key().len() {
        return Err(Error::engine(
            "Postgres row target primary key length mismatch",
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
        qualified_table_name(spec),
        predicates.join(" AND ")
    ))
}

fn row_state<R: Serialize>(row: &R, schema: &TableSchema) -> Result<Map<String, JsonValue>> {
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
                "non-nullable Postgres column of type {} got null",
                col.pg_type
            )))
        };
    }
    let lower = col.pg_type.to_ascii_lowercase();
    if is_text_type(&lower) {
        return Ok(quote_string(value_to_string(value)?));
    }
    if lower == "boolean" || lower == "bool" {
        return value
            .as_bool()
            .map(|b| if b { "TRUE" } else { "FALSE" }.to_string())
            .ok_or_else(|| Error::engine("boolean column requires bool JSON value"));
    }
    if is_numeric_type(&lower) {
        return match value {
            JsonValue::Number(n) => Ok(n.to_string()),
            _ => Err(Error::engine(format!(
                "numeric column {} requires numeric JSON value",
                col.pg_type
            ))),
        };
    }
    if _is_pgvector_type(&lower) {
        return vector_literal(value, &col.pg_type);
    }
    if lower == "bytea" {
        // A Rust `Vec<u8>` serializes to a JSON array of byte values; encode it
        // as Postgres hex bytea (`'\x..'::bytea`). The fallback `'[104,105]'`
        // would be invalid bytea input.
        return bytea_literal(value);
    }
    if lower == "json" || lower == "jsonb" {
        // Postgres rejects U+0000 in json/jsonb on parse, and serde serializes a
        // NUL inside a nested string/key as the ` ` escape (which
        // `quote_string`'s byte-level strip can't catch). Remove NULs from every
        // string and object key before serializing.
        return Ok(format!(
            "{}::{}",
            quote_string(sanitize_json_nul(value).to_string()),
            col.pg_type
        ));
    }
    match value {
        JsonValue::String(s) => Ok(format!("{}::{}", quote_string(s), col.pg_type)),
        JsonValue::Number(n) => Ok(format!("{}::{}", n, col.pg_type)),
        JsonValue::Bool(b) => Ok(format!(
            "{}::{}",
            if *b { "TRUE" } else { "FALSE" },
            col.pg_type
        )),
        _ => Ok(format!(
            "{}::{}",
            quote_string(value.to_string()),
            col.pg_type
        )),
    }
}

fn value_to_string(value: &JsonValue) -> Result<&str> {
    value
        .as_str()
        .ok_or_else(|| Error::engine("text column requires string JSON value"))
}

/// Encode a JSON value as a Postgres hex `bytea` literal. Accepts a byte array
/// (the serde default for `Vec<u8>`) or a string (encoded as its UTF-8 bytes).
fn bytea_literal(value: &JsonValue) -> Result<String> {
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
    Ok(format!("{}::bytea", quote_string(hex)))
}

fn vector_literal(value: &JsonValue, pg_type: &str) -> Result<String> {
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
    Ok(format!(
        "{}::{}",
        quote_string(format!("[{}]", parts.join(","))),
        pg_type
    ))
}

fn quote_string(value: impl AsRef<str>) -> String {
    let value = value.as_ref().replace('\0', "").replace('\'', "''");
    format!("'{value}'")
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
            | "numeric"
    )
}

fn pg_err(e: sqlx::Error) -> Error {
    Error::engine(format!("postgres: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
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
            embedding: vec![0.1, 0.2, 0.3],
            start_line: 1,
            end_line: 3,
        };

        let fields = row_state(&row, &schema).unwrap();
        assert_eq!(
            pk_stable_key(&fields, schema.primary_key()).unwrap(),
            StableKey::Int(42)
        );

        let sql = upsert_sql(&spec, &fields).unwrap();
        assert!(sql.contains("INSERT INTO \"coco_examples\".\"code_embeddings\""));
        assert!(sql.contains("'["));
        assert!(sql.contains("]'::vector(3)"));
        assert!(sql.contains("ON CONFLICT (\"id\") DO UPDATE SET"));
    }

    #[test]
    fn delete_sql_uses_typed_primary_key_literal() {
        let schema = code_schema();
        let spec = TableSpec {
            pg_schema_name: Some("coco_examples".to_string()),
            table_name: "code_embeddings".to_string(),
            table_schema: schema,
            managed_by: ManagedBy::System,
        };
        let sql = delete_sql(&spec, &[JsonValue::from(42)]).unwrap();
        assert_eq!(
            sql,
            "DELETE FROM \"coco_examples\".\"code_embeddings\" WHERE \"id\" = 42"
        );
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
    fn quote_string_strips_nul_and_escapes_quotes() {
        // Postgres text/jsonb reject U+0000; it must be stripped, and single
        // quotes doubled.
        assert_eq!(quote_string("a\0b'c"), "'ab''c'");
        assert_eq!(quote_string("plain"), "'plain'");
    }
}

// ---------------------------------------------------------------------------
// Source: read rows from a Postgres table
// ---------------------------------------------------------------------------

/// Read every row of `table_name` (in the connection's default search path) as
/// `T`.
///
/// `T` must be `Deserialize` (column names map to struct fields; unknown columns
/// are ignored) and is usually also `Serialize` so each row can key memoized
/// per-row work via `ctx.mount_each`. Incrementality comes from memoization
/// (unchanged rows skip processing) plus target-state reconciliation (rows that
/// disappear from the source have their derived target states deleted).
pub async fn read_table<T: serde::de::DeserializeOwned>(
    db: &Database,
    table_name: &str,
) -> Result<Vec<T>> {
    read_table_with_options(db, table_name, ReadTableOptions::default()).await
}

pub async fn read_table_with_options<T: serde::de::DeserializeOwned>(
    db: &Database,
    table_name: &str,
    options: ReadTableOptions,
) -> Result<Vec<T>> {
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
    // Read inside a REPEATABLE READ, READ ONLY transaction so the whole table is
    // observed as one consistent snapshot.
    let mut tx = db.pool().begin().await.map_err(pg_err)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(pg_err)?;
    let rows = sqlx::query(&sql)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| Error::engine(format!("postgres source read failed: {e}")))?;
    tx.commit().await.map_err(pg_err)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let json = pg_row_to_json(row)?;
        out.push(serde_json::from_value(json).map_err(|e| {
            Error::engine(format!("postgres source row does not match row type: {e}"))
        })?);
    }
    Ok(out)
}

/// Read every row paired with a stable key derived by `key_fn`, ready to feed
/// [`Ctx::mount_each`](crate::Ctx::mount_each). Reads use the same
/// repeatable-read snapshot as [`read_table`].
pub async fn read_table_items<T, K, S>(
    db: &Database,
    table_name: &str,
    key_fn: K,
) -> Result<Vec<(StableKey, T)>>
where
    T: serde::de::DeserializeOwned,
    K: Fn(&T) -> S,
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
) -> Result<Vec<(StableKey, T)>>
where
    T: serde::de::DeserializeOwned,
    K: Fn(&T) -> S,
    S: crate::target_state::IntoStableKey,
{
    let rows: Vec<T> = read_table_with_options(db, table_name, options).await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let key = key_fn(&row).into_stable_key();
            (key, row)
        })
        .collect())
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
        let lit = sql_literal(&value, &ColumnDef::new("jsonb")).unwrap();
        assert!(!lit.contains('\0'), "raw NUL leaked: {lit}");
        assert!(!lit.contains("\\u0000"), "escaped NUL leaked: {lit}");
        assert!(lit.ends_with("::jsonb"));
        let inner = lit.trim_start_matches('\'').trim_end_matches("'::jsonb");
        let parsed: JsonValue = serde_json::from_str(&inner.replace("''", "'")).unwrap();
        assert_eq!(parsed["name"], serde_json::json!("alice"));
        assert_eq!(parsed["tags"][0], serde_json::json!("ab"));
        assert_eq!(parsed["tags"][1]["k"], serde_json::json!("v"));
    }

    #[test]
    fn sanitize_json_nul_leaves_clean_values_untouched() {
        let value = serde_json::json!({"a": [1, 2], "b": "ok"});
        assert_eq!(sanitize_json_nul(&value), value);
    }
}
