//! LanceDB table target connector.
//!
//! Table targets reconcile declared rows against the previous run: changed rows
//! are upserted, unchanged rows are skipped, and rows no longer declared are
//! deleted. `managed_by` controls whether CocoIndex owns table DDL.
//!
//! Use [`table_target`] to build a composable target state, or
//! [`declare_table_target`] / [`mount_table_target`] to get a handle for
//! declaring rows.
//!
//! Built on the native Rust `lancedb` crate (LanceDB's core is Rust) + Arrow.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cocoindex_utils::fingerprint::Fingerprint;
use lancedb::Connection;
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::table::NewColumnTransform;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::statediff::{
    CompositeTrackingRecord, DiffAction, ManagedBy, ManagedTargetOptions, MutualTrackingRecord,
    diff, diff_composite, resolve_system_transition,
};
use crate::target_state::{
    ChildTargetDef, StableKey, TargetAction, TargetActionSink, TargetChildInvalidation,
    TargetHandler, TargetReconcileOutput, TargetState, TargetStateProvider, declare_target_state,
    mount_target, register_root_target_states_provider,
};

// ---------------------------------------------------------------------------
// LanceDatabase — connection handle
// ---------------------------------------------------------------------------

/// A LanceDB connection. Clone-cheap (the underlying connection is shared).
#[derive(Clone)]
pub struct LanceDatabase {
    conn: Arc<Connection>,
    state_id: Arc<str>,
}

impl LanceDatabase {
    /// Open (or create) a LanceDB database at `uri` (a local path like
    /// `./lancedb_data`, or a cloud URI).
    pub async fn connect(uri: &str) -> Result<Self> {
        let conn = lancedb::connect(uri)
            .execute()
            .await
            .map_err(|e| Error::engine(format!("lancedb connect {uri:?}: {e}")))?;
        Ok(Self {
            conn: Arc::new(conn),
            state_id: Arc::from(uri),
        })
    }

    /// Stable identity (the URI) for use as a `ContextKey` state id / memo dep.
    pub fn state_id(&self) -> &str {
        &self.state_id
    }

    /// The underlying `lancedb::Connection` (e.g. for queries).
    pub fn connection(&self) -> &Connection {
        &self.conn
    }
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// A LanceDB column type (the subset CocoIndex maps natively).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ColumnType {
    Int64,
    Float64,
    Text,
    /// Fixed-size float32 vector of the given dimension.
    Vector(usize),
}

impl ColumnType {
    fn arrow_data_type(&self) -> DataType {
        match self {
            ColumnType::Int64 => DataType::Int64,
            ColumnType::Float64 => DataType::Float64,
            ColumnType::Text => DataType::Utf8,
            ColumnType::Vector(dim) => DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                *dim as i32,
            ),
        }
    }
}

/// A column definition: its type and nullability.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnDef {
    col_type: ColumnType,
    nullable: bool,
}

impl ColumnDef {
    pub fn new(col_type: ColumnType) -> Self {
        Self {
            col_type,
            nullable: false,
        }
    }

    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }
}

/// A LanceDB table schema: ordered columns + a primary key.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSchema {
    columns: Vec<(String, ColumnDef)>,
    primary_key: Vec<String>,
}

impl TableSchema {
    pub fn new(
        columns: impl IntoIterator<Item = (impl Into<String>, ColumnDef)>,
        primary_key: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let columns: Vec<(String, ColumnDef)> =
            columns.into_iter().map(|(n, d)| (n.into(), d)).collect();
        let primary_key: Vec<String> = primary_key.into_iter().map(Into::into).collect();
        if primary_key.is_empty() {
            return Err(Error::engine("LanceDB table requires a primary key"));
        }
        let mut seen = std::collections::HashSet::new();
        for (name, _) in &columns {
            validate_identifier(name)?;
            if !seen.insert(name.as_str()) {
                return Err(Error::engine(format!(
                    "duplicate LanceDB table column: {name:?}"
                )));
            }
        }
        for pk in &primary_key {
            validate_identifier(pk)?;
            if !columns.iter().any(|(n, _)| n == pk) {
                return Err(Error::engine(format!(
                    "primary key column {pk:?} is not in the table schema"
                )));
            }
        }
        Ok(Self {
            columns,
            primary_key,
        })
    }

    pub fn primary_key(&self) -> &[String] {
        &self.primary_key
    }

    fn column_names(&self) -> impl Iterator<Item = &String> {
        self.columns.iter().map(|(n, _)| n)
    }

    fn arrow_schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|(name, def)| Field::new(name, def.col_type.arrow_data_type(), def.nullable))
            .collect();
        Arc::new(Schema::new(fields))
    }
}

// ---------------------------------------------------------------------------
// Public target API: constructor / declaration / mount split
// ---------------------------------------------------------------------------

/// A declarative LanceDB table target — a handle to declare rows on. See the
/// [module docs](self).
#[derive(Clone)]
pub struct LanceTableTarget {
    table_name: Arc<str>,
    table_schema: TableSchema,
    rows: TargetStateProvider<RowState>,
}

/// Build a composable [`TargetState`] for a LanceDB table (the spec constructor).
/// Pass it to [`declare_table_target`]/[`mount_table_target`] or the generic
/// [`declare_target_state_with_child`]/[`mount_target`]. System-managed.
pub fn table_target(
    ctx: &Ctx,
    db: &LanceDatabase,
    table_name: impl Into<String>,
    table_schema: TableSchema,
) -> Result<TargetState<TableSpec>> {
    table_target_with_options(
        ctx,
        db,
        table_name,
        table_schema,
        ManagedTargetOptions::default(),
    )
}

/// [`table_target`] with explicit [`ManagedTargetOptions`] (`managed_by`).
pub fn table_target_with_options(
    ctx: &Ctx,
    db: &LanceDatabase,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    options: ManagedTargetOptions,
) -> Result<TargetState<TableSpec>> {
    let table_name = table_name.into();
    let provider = register_root_target_states_provider(
        ctx,
        format!("cocoindex/lancedb/table/{}/{}", db.state_id(), table_name),
        TableHandler { db: db.clone() },
    )?;
    Ok(provider.target_state(
        "default",
        TableSpec {
            table_name,
            table_schema,
            managed_by: options.managed_by,
        },
    ))
}

/// Declare a LanceDB table target and return a same-component handle.
///
/// Prefer [`mount_table_target`] when rows can be declared from async code: that
/// path uses CocoIndex's child-provider invalidation directly. This sync helper
/// keeps same-component declaration ergonomic and keys its row provider by the
/// table schema so destructive schema changes do not skip unchanged rows.
pub fn declare_table_target(
    ctx: &Ctx,
    db: &LanceDatabase,
    table_name: impl Into<String>,
    table_schema: TableSchema,
) -> Result<LanceTableTarget> {
    let table_name = table_name.into();
    let target_state = table_target_with_options(
        ctx,
        db,
        table_name.clone(),
        table_schema,
        ManagedTargetOptions::default(),
    )?;
    let spec = target_state.value().clone();
    let schema_fp = Fingerprint::from(&table_tracking_record(&spec)).map_err(Error::from)?;
    declare_target_state(ctx, target_state)?;
    let rows = register_root_target_states_provider(
        ctx,
        format!(
            "cocoindex/lancedb/row/{}/{}/{}",
            db.state_id(),
            table_name,
            schema_fp
        ),
        RowHandler {
            db: db.clone(),
            spec: spec.clone(),
        },
    )?;
    Ok(LanceTableTarget {
        table_name: Arc::from(table_name),
        table_schema: spec.table_schema,
        rows,
    })
}

/// Declare a LanceDB table target in the current component and return a handle
/// for declaring rows. Existing tables are preserved on compatible schema
/// changes; incompatible system-managed schema changes recreate the table and
/// invalidate child rows.
pub async fn mount_table_target(
    ctx: &Ctx,
    db: &LanceDatabase,
    table_name: impl Into<String>,
    table_schema: TableSchema,
) -> Result<LanceTableTarget> {
    mount_table_target_with_options(
        ctx,
        db,
        table_name,
        table_schema,
        ManagedTargetOptions::default(),
    )
    .await
}

/// [`mount_table_target`] with explicit [`ManagedTargetOptions`] (`managed_by`).
pub async fn mount_table_target_with_options(
    ctx: &Ctx,
    db: &LanceDatabase,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    options: ManagedTargetOptions,
) -> Result<LanceTableTarget> {
    let table_name = table_name.into();
    let target_state =
        table_target_with_options(ctx, db, table_name.clone(), table_schema.clone(), options)?;
    let spec = target_state.value().clone();
    let rows = mount_target::<TableSpec, RowState>(ctx, target_state).await?;
    Ok(LanceTableTarget {
        table_name: Arc::from(table_name),
        table_schema: spec.table_schema,
        rows,
    })
}

impl LanceTableTarget {
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Declare a row to upsert into the table. `row` must serialize to an object
    /// containing the schema's columns (extra fields are dropped, missing ones
    /// become null).
    pub fn declare_row<R: Serialize>(&self, ctx: &Ctx, row: &R) -> Result<()> {
        let fields = row_state(row, &self.table_schema)?;
        let key = pk_stable_key(&fields, self.table_schema.primary_key())?;
        declare_target_state(ctx, self.rows.target_state(key, RowState { fields }))
    }
}

// ---------------------------------------------------------------------------
// Table container handler (root) + sink yielding row children
// ---------------------------------------------------------------------------

/// Spec for a LanceDB table (the declared container value).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TableSpec {
    table_name: String,
    table_schema: TableSchema,
    #[serde(default)]
    managed_by: ManagedBy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TableAction {
    table_name: String,
    spec: Option<TableSpec>,
    recreate: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TableMainState {
    table_name: String,
    primary_key: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ColumnState {
    col_type: ColumnType,
    nullable: bool,
}

type TableTrackingRecord =
    MutualTrackingRecord<CompositeTrackingRecord<TableMainState, String, ColumnState>>;

fn table_tracking_record(
    spec: &TableSpec,
) -> CompositeTrackingRecord<TableMainState, String, ColumnState> {
    let sub = spec
        .table_schema
        .columns
        .iter()
        .map(|(name, def)| {
            (
                name.clone(),
                ColumnState {
                    col_type: def.col_type.clone(),
                    nullable: def.nullable,
                },
            )
        })
        .collect::<HashMap<_, _>>();
    CompositeTrackingRecord::new(
        TableMainState {
            table_name: spec.table_name.clone(),
            primary_key: spec.table_schema.primary_key.clone(),
        },
        sub,
    )
}

struct TableHandler {
    db: LanceDatabase,
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
            // Always emit when declared so the sink fulfills the row child.
            Some(spec) => {
                let tracking =
                    MutualTrackingRecord::new(table_tracking_record(&spec), spec.managed_by);
                let resolved =
                    resolve_system_transition(Some(tracking.clone()), prev, prev_may_be_missing);
                let (main_action, column_transitions) = diff_composite(resolved.as_ref());
                let check_column_actions = matches!(main_action, None | Some(DiffAction::Upsert));
                // A pure column *add* is additive (a nullable column added via
                // `add_columns`, including vectors — see `evolve_existing_table`),
                // so it never recreates. A column *retype* or *drop* can't be done
                // in place in LanceDB, so it forces a destructive recreate (matches
                // Python, which upgrades non-add column actions to a replace).
                let recreate = matches!(main_action, Some(DiffAction::Replace))
                    || (check_column_actions
                        && column_transitions.into_iter().any(|(_name, transition)| {
                            matches!(
                                diff(Some(&transition)),
                                Some(DiffAction::Replace | DiffAction::Delete)
                            )
                        }));
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Update(TableAction {
                        table_name: spec.table_name.clone(),
                        spec: Some(spec),
                        recreate,
                    }),
                    sink: self.table_sink(),
                    tracking_record: Some(tracking),
                    child_invalidation: recreate.then_some(TargetChildInvalidation::Destructive),
                }))
            }
            None => {
                let resolved = resolve_system_transition(None, prev.clone(), prev_may_be_missing);
                if resolved.is_none() {
                    return Ok(None);
                }
                let table_name = prev
                    .into_iter()
                    .find(|p| p.managed_by.is_system())
                    .map(|p| p.tracking_record.main.table_name)
                    .ok_or_else(|| Error::engine("cannot drop LanceDB table without prior spec"))?;
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Delete(TableAction {
                        table_name,
                        spec: None,
                        recreate: false,
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
                                    Error::engine("LanceDB create/update action missing spec")
                                })?;
                                if spec.managed_by.is_system() {
                                    ensure_table(&db, &spec, a.recreate).await?;
                                }
                                out.push(Some(ChildTargetDef::new::<RowState, _>(RowHandler {
                                    db: db.clone(),
                                    spec,
                                })));
                            }
                            TargetAction::Delete(a) => {
                                drop_table(&db, &a.table_name).await?;
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

async fn table_exists(db: &LanceDatabase, table_name: &str) -> Result<bool> {
    let names = db
        .conn
        .table_names()
        .execute()
        .await
        .map_err(|e| Error::engine(format!("lancedb list tables: {e}")))?;
    Ok(names.iter().any(|n| n == table_name))
}

async fn ensure_table(db: &LanceDatabase, spec: &TableSpec, recreate: bool) -> Result<()> {
    let exists = table_exists(db, &spec.table_name).await?;
    if exists && recreate {
        drop_table(db, &spec.table_name).await?;
    } else if exists {
        match evolve_existing_table(db, spec).await? {
            TableEvolution::Done => return Ok(()),
            TableEvolution::Recreate => drop_table(db, &spec.table_name).await?,
        }
    }
    db.conn
        .create_empty_table(&spec.table_name, spec.table_schema.arrow_schema())
        .execute()
        .await
        .map_err(|e| Error::engine(format!("lancedb create table {:?}: {e}", spec.table_name)))?;
    Ok(())
}

enum TableEvolution {
    Done,
    Recreate,
}

async fn evolve_existing_table(db: &LanceDatabase, spec: &TableSpec) -> Result<TableEvolution> {
    let table = db
        .conn
        .open_table(&spec.table_name)
        .execute()
        .await
        .map_err(|e| Error::engine(format!("lancedb open table for schema check: {e}")))?;
    let existing = table
        .schema()
        .await
        .map_err(|e| Error::engine(format!("lancedb read schema: {e}")))?;
    let mut add_fields = Vec::new();
    for (name, def) in &spec.table_schema.columns {
        match existing.field_with_name(name) {
            Ok(field) => {
                // An in-place type change isn't supported by LanceDB schema
                // evolution — fall back to a destructive recreate.
                if field.data_type() != &def.col_type.arrow_data_type() {
                    return Ok(TableEvolution::Recreate);
                }
            }
            Err(_) => {
                // Add the new column as a nullable, all-null column. `AllNulls`
                // (rather than a SQL `CAST(NULL AS ..)` expression) supports every
                // type, including fixed-size-list vector columns — so adding a
                // vector column is additive instead of wiping the table.
                add_fields.push(Field::new(name, def.col_type.arrow_data_type(), true));
            }
        }
    }
    if !add_fields.is_empty() {
        let add_schema = Arc::new(Schema::new(add_fields));
        table
            .add_columns(NewColumnTransform::AllNulls(add_schema), None)
            .await
            .map_err(|e| Error::engine(format!("lancedb add columns: {e}")))?;
    }
    Ok(TableEvolution::Done)
}

async fn drop_table(db: &LanceDatabase, table_name: &str) -> Result<()> {
    if !table_exists(db, table_name).await? {
        return Ok(());
    }
    db.conn
        .drop_table(table_name, &[])
        .await
        .map_err(|e| Error::engine(format!("lancedb drop table {table_name:?}: {e}")))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Row handler (child) + sink
// ---------------------------------------------------------------------------

/// A declared row's column values.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RowState {
    fields: Map<String, JsonValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RowAction {
    pk: Vec<JsonValue>,
    state: Option<RowState>,
}

struct RowHandler {
    db: LanceDatabase,
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
                let mut upserts: Vec<Map<String, JsonValue>> = Vec::new();
                let mut deletes: Vec<Vec<JsonValue>> = Vec::new();
                for action in actions {
                    let row = match action {
                        TargetAction::Create(r)
                        | TargetAction::Update(r)
                        | TargetAction::Delete(r) => r,
                    };
                    match row.state {
                        Some(state) => upserts.push(state.fields),
                        None => deletes.push(row.pk),
                    }
                }
                apply_rows(&db, &spec, upserts, deletes).await
            }
        })
    }
}

async fn apply_rows(
    db: &LanceDatabase,
    spec: &TableSpec,
    upserts: Vec<Map<String, JsonValue>>,
    deletes: Vec<Vec<JsonValue>>,
) -> Result<()> {
    if upserts.is_empty() && deletes.is_empty() {
        return Ok(());
    }
    // Rows imply the table exists for system-managed targets. User-managed
    // targets intentionally leave DDL to the caller and let open_table surface
    // missing/incompatible tables.
    if spec.managed_by.is_system() {
        ensure_table(db, spec, false).await?;
    }
    let table = db
        .conn
        .open_table(&spec.table_name)
        .execute()
        .await
        .map_err(|e| Error::engine(format!("lancedb open table: {e}")))?;

    if !upserts.is_empty() {
        let schema = spec.table_schema.arrow_schema();
        let batch = build_record_batch(&spec.table_schema, &schema, &upserts)?;
        let pk_refs: Vec<&str> = spec
            .table_schema
            .primary_key
            .iter()
            .map(String::as_str)
            .collect();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let mut builder = table.merge_insert(&pk_refs);
        builder.when_matched_update_all(None);
        builder.when_not_matched_insert_all();
        builder
            .execute(Box::new(reader))
            .await
            .map_err(|e| Error::engine(format!("lancedb merge_insert: {e}")))?;
    }

    for pk in &deletes {
        let predicate = delete_predicate(&spec.table_schema.primary_key, pk)?;
        table
            .delete(&predicate)
            .await
            .map_err(|e| Error::engine(format!("lancedb delete: {e}")))?;
    }
    Ok(())
}

fn delete_predicate(primary_key: &[String], pk: &[JsonValue]) -> Result<String> {
    let mut parts = Vec::with_capacity(primary_key.len());
    for (name, value) in primary_key.iter().zip(pk) {
        let rhs = match value {
            JsonValue::String(s) => format!("'{}'", s.replace('\'', "''")),
            JsonValue::Number(n) => n.to_string(),
            other => {
                return Err(Error::engine(format!(
                    "unsupported LanceDB key value: {other}"
                )));
            }
        };
        validate_identifier(name)?;
        parts.push(format!("{name} = {rhs}"));
    }
    Ok(parts.join(" AND "))
}

// ---------------------------------------------------------------------------
// Arrow record-batch construction
// ---------------------------------------------------------------------------

fn build_record_batch(
    schema: &TableSchema,
    arrow_schema: &SchemaRef,
    rows: &[Map<String, JsonValue>],
) -> Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.columns.len());
    for (name, def) in &schema.columns {
        let values = rows.iter().map(|r| r.get(name).unwrap_or(&JsonValue::Null));
        let array: ArrayRef = match &def.col_type {
            ColumnType::Int64 => Arc::new(Int64Array::from(
                values
                    .map(|v| nullable_value(name, def, v).map(|v| v.as_i64()))
                    .collect::<Result<Vec<_>>>()?,
            )),
            ColumnType::Float64 => Arc::new(Float64Array::from(
                values
                    .map(|v| nullable_value(name, def, v).map(|v| v.as_f64()))
                    .collect::<Result<Vec<_>>>()?,
            )),
            ColumnType::Text => Arc::new(StringArray::from(
                values
                    .map(|v| nullable_value(name, def, v).map(|v| v.as_str().map(str::to_string)))
                    .collect::<Result<Vec<_>>>()?,
            )),
            ColumnType::Vector(dim) => build_vector_array(name, *dim, def.nullable, values)?,
        };
        arrays.push(array);
    }
    RecordBatch::try_new(arrow_schema.clone(), arrays)
        .map_err(|e| Error::engine(format!("build LanceDB record batch: {e}")))
}

fn build_vector_array<'a>(
    column: &str,
    dim: usize,
    nullable: bool,
    values: impl Iterator<Item = &'a JsonValue>,
) -> Result<ArrayRef> {
    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dim as i32)
        .with_field(Arc::new(Field::new("item", DataType::Float32, true)));
    let mut count = 0usize;
    for value in values {
        if value.is_null() {
            if !nullable {
                return Err(Error::engine(format!(
                    "non-nullable LanceDB column {column:?} is null"
                )));
            }
            for _ in 0..dim {
                builder.values().append_null();
            }
            builder.append(false);
            count += 1;
            continue;
        }
        let arr = value
            .as_array()
            .ok_or_else(|| Error::engine(format!("column {column:?} must be a vector array")))?;
        if arr.len() != dim {
            return Err(Error::engine(format!(
                "column {column:?} vector length {} != schema dim {dim}",
                arr.len()
            )));
        }
        for v in arr {
            let f = v.as_f64().ok_or_else(|| {
                Error::engine(format!("column {column:?} has non-numeric vector element"))
            })?;
            builder.values().append_value(f as f32);
        }
        builder.append(true);
        count += 1;
    }
    let array = builder.finish();
    debug_assert_eq!(array.len(), count);
    Ok(Arc::new(array))
}

// ---------------------------------------------------------------------------
// Helpers (row state / keys) — local copies, parallel to postgres
// ---------------------------------------------------------------------------

fn row_state<R: Serialize>(row: &R, schema: &TableSchema) -> Result<Map<String, JsonValue>> {
    let value = serde_json::to_value(row)
        .map_err(|e| Error::engine(format!("serialize LanceDB target row: {e}")))?;
    let JsonValue::Object(mut fields) = value else {
        return Err(Error::engine(
            "LanceDB target row must serialize to an object",
        ));
    };
    let names: std::collections::HashSet<&str> =
        schema.column_names().map(String::as_str).collect();
    fields.retain(|name, _| names.contains(name.as_str()));
    for (name, def) in &schema.columns {
        let value = fields.entry(name.clone()).or_insert(JsonValue::Null);
        if value.is_null() && !def.nullable {
            return Err(Error::engine(format!(
                "non-nullable LanceDB column {name:?} is missing or null"
            )));
        }
    }
    Ok(fields)
}

fn nullable_value<'a>(name: &str, def: &ColumnDef, value: &'a JsonValue) -> Result<&'a JsonValue> {
    if value.is_null() && !def.nullable {
        return Err(Error::engine(format!(
            "non-nullable LanceDB column {name:?} is null"
        )));
    }
    Ok(value)
}

fn validate_identifier(name: &str) -> Result<()> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(Error::engine("LanceDB identifier must not be empty"));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(Error::engine(format!(
            "invalid LanceDB identifier {name:?}: must start with a letter or '_'"
        )));
    }
    if !chars.all(|c| c == '_' || c.is_ascii_alphanumeric()) {
        return Err(Error::engine(format!(
            "invalid LanceDB identifier {name:?}: only ASCII letters, digits, and '_' are supported"
        )));
    }
    Ok(())
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

fn json_scalar_to_stable_key(value: &JsonValue) -> Result<StableKey> {
    match value {
        JsonValue::String(s) => Ok(StableKey::Str(Arc::from(s.clone()))),
        JsonValue::Number(n) => n
            .as_i64()
            .map(StableKey::Int)
            .ok_or_else(|| Error::engine(format!("unsupported numeric primary key: {n}"))),
        other => Err(Error::engine(format!(
            "unsupported primary key value: {other}"
        ))),
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
        other => Err(Error::engine(format!(
            "unsupported LanceDB row key: {other:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Query helper (convenience for examples)
// ---------------------------------------------------------------------------

/// Run a cosine-distance vector similarity search and return the top-`k` rows as
/// JSON objects (including the `_distance` column). `1.0 - _distance` is a cosine
/// similarity in `[0, 1]` (matching the pgvector examples).
pub async fn vector_search(
    db: &LanceDatabase,
    table_name: &str,
    column: &str,
    query: Vec<f32>,
    top_k: usize,
) -> Result<Vec<Map<String, JsonValue>>> {
    use futures::TryStreamExt;

    let table = db
        .conn
        .open_table(table_name)
        .execute()
        .await
        .map_err(|e| Error::engine(format!("lancedb open table: {e}")))?;
    let stream = table
        .vector_search(query)
        .map_err(|e| Error::engine(format!("lancedb vector_search: {e}")))?
        .column(column)
        .distance_type(lancedb::DistanceType::Cosine)
        .limit(top_k)
        .execute()
        .await
        .map_err(|e| Error::engine(format!("lancedb search execute: {e}")))?;
    let batches: Vec<RecordBatch> = stream
        .try_collect()
        .await
        .map_err(|e| Error::engine(format!("lancedb search collect: {e}")))?;
    record_batches_to_json(&batches)
}

fn record_batches_to_json(batches: &[RecordBatch]) -> Result<Vec<Map<String, JsonValue>>> {
    let mut out = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            let mut obj = Map::new();
            for (col, field) in schema.fields().iter().enumerate() {
                let array = batch.column(col);
                obj.insert(field.name().clone(), array_value_to_json(array, row));
            }
            out.push(obj);
        }
    }
    Ok(out)
}

fn array_value_to_json(array: &ArrayRef, row: usize) -> JsonValue {
    use arrow_array::cast::AsArray;
    if array.is_null(row) {
        return JsonValue::Null;
    }
    match array.data_type() {
        DataType::Int64 => JsonValue::from(
            array
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(row),
        ),
        DataType::Float64 => JsonValue::from(
            array
                .as_primitive::<arrow_array::types::Float64Type>()
                .value(row),
        ),
        DataType::Float32 => JsonValue::from(
            array
                .as_primitive::<arrow_array::types::Float32Type>()
                .value(row),
        ),
        DataType::Utf8 => JsonValue::from(array.as_string::<i32>().value(row).to_string()),
        _ => JsonValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> TableSchema {
        TableSchema::new(
            [
                ("id", ColumnDef::new(ColumnType::Int64)),
                ("name", ColumnDef::new(ColumnType::Text)),
                ("embedding", ColumnDef::new(ColumnType::Vector(3))),
            ],
            ["id"],
        )
        .unwrap()
    }

    #[test]
    fn schema_requires_pk_in_columns() {
        assert!(TableSchema::new([("a", ColumnDef::new(ColumnType::Int64))], ["missing"]).is_err());
        assert!(
            TableSchema::new(
                [("a", ColumnDef::new(ColumnType::Int64))],
                Vec::<String>::new()
            )
            .is_err()
        );
        assert!(TableSchema::new([("a", ColumnDef::new(ColumnType::Int64))], ["a"]).is_ok());
        assert!(
            TableSchema::new(
                [
                    ("a", ColumnDef::new(ColumnType::Int64)),
                    ("a", ColumnDef::new(ColumnType::Text)),
                ],
                ["a"],
            )
            .is_err()
        );
        assert!(
            TableSchema::new(
                [("bad-name", ColumnDef::new(ColumnType::Int64))],
                ["bad-name"]
            )
            .is_err()
        );
    }

    #[test]
    fn arrow_schema_maps_types() {
        let s = schema();
        let a = s.arrow_schema();
        assert_eq!(a.field(0).data_type(), &DataType::Int64);
        assert_eq!(a.field(1).data_type(), &DataType::Utf8);
        match a.field(2).data_type() {
            DataType::FixedSizeList(f, 3) => assert_eq!(f.data_type(), &DataType::Float32),
            other => panic!("expected fixed size list, got {other:?}"),
        }
    }

    #[test]
    fn row_state_filters_and_fills_nullable_missing_values() {
        #[derive(serde::Serialize)]
        struct Row {
            id: i64,
            name: String,
            extra: i64,
        }
        let schema = TableSchema::new(
            [
                ("id", ColumnDef::new(ColumnType::Int64)),
                ("name", ColumnDef::new(ColumnType::Text)),
                (
                    "embedding",
                    ColumnDef::new(ColumnType::Vector(3)).nullable(),
                ),
            ],
            ["id"],
        )
        .unwrap();
        let fields = row_state(
            &Row {
                id: 1,
                name: "x".into(),
                extra: 9,
            },
            &schema,
        )
        .unwrap();
        assert!(fields.contains_key("id"));
        assert!(fields.contains_key("embedding"));
        assert!(!fields.contains_key("extra"));
        assert_eq!(fields["embedding"], JsonValue::Null);
    }

    #[test]
    fn row_state_rejects_missing_non_nullable_columns() {
        #[derive(serde::Serialize)]
        struct Row {
            id: i64,
            name: String,
        }
        let err = row_state(
            &Row {
                id: 1,
                name: "x".into(),
            },
            &schema(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("non-nullable LanceDB column"));
    }

    #[test]
    fn pk_and_delete_predicate() {
        let mut fields = Map::new();
        fields.insert("id".into(), JsonValue::from(42));
        let key = pk_stable_key(&fields, &["id".to_string()]).unwrap();
        assert_eq!(key, StableKey::Int(42));
        let pk = stable_key_to_pk(&key).unwrap();
        assert_eq!(
            delete_predicate(&["id".to_string()], &pk).unwrap(),
            "id = 42"
        );

        let pred = delete_predicate(&["name".to_string()], &[JsonValue::from("a'b")]).unwrap();
        assert_eq!(pred, "name = 'a''b'");
        assert!(delete_predicate(&["bad-name".to_string()], &pk).is_err());
    }

    #[test]
    fn build_batch_with_vector() {
        let s = schema();
        let arrow = s.arrow_schema();
        let mut row = Map::new();
        row.insert("id".into(), JsonValue::from(1));
        row.insert("name".into(), JsonValue::from("hello"));
        row.insert("embedding".into(), JsonValue::from(vec![0.1f64, 0.2, 0.3]));
        let batch = build_record_batch(&s, &arrow, &[row]).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn build_batch_with_nullable_vector() {
        let s = TableSchema::new(
            [
                ("id", ColumnDef::new(ColumnType::Int64)),
                (
                    "embedding",
                    ColumnDef::new(ColumnType::Vector(3)).nullable(),
                ),
            ],
            ["id"],
        )
        .unwrap();
        let arrow = s.arrow_schema();
        let mut row = Map::new();
        row.insert("id".into(), JsonValue::from(1));
        row.insert("embedding".into(), JsonValue::Null);
        let batch = build_record_batch(&s, &arrow, &[row]).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column(1).is_null(0));
    }
}
