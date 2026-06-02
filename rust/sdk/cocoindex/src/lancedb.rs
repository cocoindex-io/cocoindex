//! LanceDB target connector — the Rust analogue of Python's
//! `cocoindex.connectors.lancedb` target.
//!
//! A declarative, two-level managed target (mirrors `postgres`): a *table*
//! (created/dropped to match the declared schema) containing *rows* you
//! [`declare_row`](LanceTableTarget::declare_row). Reconciliation upserts changed
//! rows, skips unchanged ones (fingerprint tracking), and deletes rows that are
//! no longer declared.
//!
//! Built on the native Rust `lancedb` crate (LanceDB's core is Rust) + Arrow.

use std::pin::Pin;
use std::sync::Arc;

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cocoindex_core::engine::target_state::{
    ChildInvalidation, TargetReconcileOutput, TargetStateProvider,
};
use cocoindex_core::state::stable_path::StableKey;
use cocoindex_utils::fingerprint::Fingerprint;
use lancedb::Connection;
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::table::NewColumnTransform;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::profile::{Action, BoxedHandler, BoxedSink, RustProfile, Value};
use crate::statediff::{
    DiffAction, ManagedBy, ManagedTargetOptions, MutualTrackingRecord, diff,
    resolve_system_transition,
};

// ---------------------------------------------------------------------------
// LanceDatabase — connection handle (mirrors postgres::Database)
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

    fn sql_null_type(&self) -> Option<&'static str> {
        match self {
            ColumnType::Int64 => Some("BIGINT"),
            ColumnType::Float64 => Some("DOUBLE"),
            ColumnType::Text => Some("STRING"),
            ColumnType::Vector(_) => None,
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
// Public target API
// ---------------------------------------------------------------------------

/// A declarative LanceDB table target. See the [module docs](self).
#[derive(Clone)]
pub struct LanceTableTarget {
    table_name: Arc<str>,
    table_schema: TableSchema,
    provider: TargetStateProvider<RustProfile>,
}

/// Mount a declarative LanceDB table target. The table is created to match
/// `table_schema`; declared rows are upserted; orphaned rows are deleted; when
/// the table is no longer declared it is dropped. Existing tables are preserved
/// on schema changes; missing scalar columns are added with `NULL` defaults.
pub fn mount_table_target(
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
}

pub fn mount_table_target_with_options(
    ctx: &Ctx,
    db: &LanceDatabase,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    options: ManagedTargetOptions,
) -> Result<LanceTableTarget> {
    let table_name = table_name.into();
    let spec = TableSpec {
        table_name: table_name.clone(),
        table_schema: table_schema.clone(),
        managed_by: options.managed_by,
    };
    let table_root = ctx.register_root_target_provider(
        format!("cocoindex/lancedb/table/{}/{}", db.state_id(), table_name),
        table_handler(db.clone()),
    )?;
    ctx.declare_target_state(
        table_root,
        StableKey::Str(Arc::from("default")),
        Value::from_serializable(&spec)?,
    )?;
    let provider = ctx.register_root_target_provider(
        format!("cocoindex/lancedb/row/{}/{}", db.state_id(), table_name),
        row_handler(db.clone(), spec),
    )?;
    Ok(LanceTableTarget {
        table_name: Arc::from(table_name),
        table_schema,
        provider,
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
        ctx.declare_target_state(
            self.provider.clone(),
            key,
            Value::from_serializable(&RowState { fields })?,
        )
    }
}

// ---------------------------------------------------------------------------
// Internal specs / actions
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct TableSpec {
    table_name: String,
    table_schema: TableSchema,
    #[serde(default)]
    managed_by: ManagedBy,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct RowState {
    fields: Map<String, JsonValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum LanceAction {
    CreateTable {
        spec: TableSpec,
        recreate: bool,
    },
    DropTable {
        table_name: String,
    },
    Row {
        spec: TableSpec,
        pk: Vec<JsonValue>,
        state: Option<RowState>,
    },
}

// ---------------------------------------------------------------------------
// Table handler / sink
// ---------------------------------------------------------------------------

fn table_handler(db: LanceDatabase) -> BoxedHandler {
    let sink = table_sink(db);
    BoxedHandler::new(move |_key, desired, prev, prev_may_be_missing| {
        let desired_spec = desired
            .map(Value::deserialize::<TableSpec>)
            .transpose()
            .map_err(internal)?;
        let prev_records = prev_table_records(&prev);

        match desired_spec {
            Some(spec) => {
                let tracking_record = MutualTrackingRecord::new(spec.clone(), spec.managed_by);
                let resolved = resolve_system_transition(
                    Some(tracking_record.clone()),
                    prev_records,
                    prev_may_be_missing,
                );
                let main_action = diff(resolved.as_ref());
                if main_action.is_none() && !spec.managed_by.is_user() {
                    return Ok(None);
                }
                let schema_changed = matches!(main_action, Some(DiffAction::Replace));
                let action = LanceAction::CreateTable {
                    spec: spec.clone(),
                    recreate: false,
                };
                Ok(Some(TargetReconcileOutput {
                    action: Action::Update(Value::from_serializable(&action).map_err(internal)?),
                    sink: sink.clone(),
                    tracking_record: Some(
                        Value::from_serializable(&tracking_record).map_err(internal)?,
                    ),
                    child_invalidation: schema_changed.then_some(ChildInvalidation::Lossy),
                }))
            }
            None => {
                let resolved =
                    resolve_system_transition(None, prev_records.clone(), prev_may_be_missing);
                if resolved.is_none() {
                    return Ok(None);
                }
                let table_name = prev_records
                    .into_iter()
                    .find(|p| p.managed_by.is_system())
                    .map(|p| p.tracking_record.table_name)
                    .ok_or_else(|| internal_msg("cannot drop LanceDB table without prior spec"))?;
                let action = LanceAction::DropTable { table_name };
                Ok(Some(TargetReconcileOutput {
                    action: Action::Delete(Value::from_serializable(&action).map_err(internal)?),
                    sink: sink.clone(),
                    tracking_record: None,
                    child_invalidation: Some(ChildInvalidation::Destructive),
                }))
            }
        }
    })
}

fn prev_table_records(prev: &[Value]) -> Vec<MutualTrackingRecord<TableSpec>> {
    prev.iter()
        .filter_map(|v| {
            v.deserialize::<MutualTrackingRecord<TableSpec>>()
                .or_else(|_| {
                    v.deserialize::<TableSpec>()
                        .map(|spec| MutualTrackingRecord::new(spec, ManagedBy::System))
                })
                .ok()
        })
        .collect()
}

fn table_sink(db: LanceDatabase) -> BoxedSink {
    BoxedSink::new(move |actions| {
        let db = db.clone();
        Box::pin(async move {
            for action in actions {
                match action_value(action)? {
                    LanceAction::CreateTable { spec, recreate } => {
                        if spec.managed_by.is_system() {
                            ensure_table(&db, &spec, recreate).await.map_err(internal)?;
                        }
                    }
                    LanceAction::DropTable { table_name } => {
                        drop_table(&db, &table_name).await.map_err(internal)?;
                    }
                    other => {
                        return Err(internal_msg(format!(
                            "unexpected action in LanceDB table sink: {other:?}"
                        )));
                    }
                }
            }
            Ok(None)
        }) as Pin<Box<_>>
    })
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
        evolve_existing_table(db, spec).await?;
        return Ok(());
    }
    db.conn
        .create_empty_table(&spec.table_name, spec.table_schema.arrow_schema())
        .execute()
        .await
        .map_err(|e| Error::engine(format!("lancedb create table {:?}: {e}", spec.table_name)))?;
    Ok(())
}

async fn evolve_existing_table(db: &LanceDatabase, spec: &TableSpec) -> Result<()> {
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
    let mut add_exprs = Vec::new();
    for (name, def) in &spec.table_schema.columns {
        match existing.field_with_name(name) {
            Ok(field) => {
                if field.data_type() != &def.col_type.arrow_data_type() {
                    return Err(Error::engine(format!(
                        "existing LanceDB column {name:?} has type {:?}, expected {:?}",
                        field.data_type(),
                        def.col_type.arrow_data_type()
                    )));
                }
            }
            Err(_) => {
                let Some(sql_type) = def.col_type.sql_null_type() else {
                    return Err(Error::engine(format!(
                        "cannot add LanceDB vector column {name:?} to existing table without rebuilding"
                    )));
                };
                add_exprs.push((name.clone(), format!("CAST(NULL AS {sql_type})")));
            }
        }
    }
    if !add_exprs.is_empty() {
        table
            .add_columns(NewColumnTransform::SqlExpressions(add_exprs), None)
            .await
            .map_err(|e| Error::engine(format!("lancedb add columns: {e}")))?;
    }
    Ok(())
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
// Row handler / sink
// ---------------------------------------------------------------------------

fn row_handler(db: LanceDatabase, spec: TableSpec) -> BoxedHandler {
    let sink = row_sink(db);
    BoxedHandler::new(move |key, desired, prev, prev_may_be_missing| {
        let pk = stable_key_to_pk(&key).map_err(internal)?;
        let desired_state = desired
            .map(Value::deserialize::<RowState>)
            .transpose()
            .map_err(internal)?;
        let desired_fp = match &desired_state {
            Some(state) => Some(Fingerprint::from(state).map_err(internal)?),
            None => None,
        };
        let prev_same = desired_fp.as_ref().is_some_and(|fp| {
            prev.iter()
                .filter_map(|v| v.deserialize::<Fingerprint>().ok())
                .any(|p| &p == fp)
        });
        if desired_state.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired_state.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }
        let tracking_record = match &desired_fp {
            Some(fp) => Some(Value::from_serializable(fp).map_err(internal)?),
            None => None,
        };
        Ok(Some(TargetReconcileOutput {
            action: Action::Update(
                Value::from_serializable(&LanceAction::Row {
                    spec: spec.clone(),
                    pk,
                    state: desired_state,
                })
                .map_err(internal)?,
            ),
            sink: sink.clone(),
            tracking_record,
            child_invalidation: None,
        }))
    })
}

fn row_sink(db: LanceDatabase) -> BoxedSink {
    BoxedSink::new(move |actions| {
        let db = db.clone();
        Box::pin(async move {
            apply_rows(&db, actions).await.map_err(internal)?;
            Ok(None)
        }) as Pin<Box<_>>
    })
}

async fn apply_rows(db: &LanceDatabase, actions: Vec<Action>) -> Result<()> {
    let mut spec: Option<TableSpec> = None;
    let mut upserts: Vec<Map<String, JsonValue>> = Vec::new();
    let mut deletes: Vec<Vec<JsonValue>> = Vec::new();

    for action in actions {
        let LanceAction::Row {
            spec: row_spec,
            pk,
            state,
        } = action_value(action)?
        else {
            return Err(Error::engine("unexpected action in LanceDB row sink"));
        };
        spec.get_or_insert(row_spec);
        match state {
            Some(state) => upserts.push(state.fields),
            None => deletes.push(pk),
        }
    }

    let Some(spec) = spec else {
        return Ok(());
    };
    // Rows imply the table exists for system-managed targets. User-managed
    // targets intentionally leave DDL to the caller and let open_table surface
    // missing/incompatible tables.
    if spec.managed_by.is_system() {
        ensure_table(db, &spec, false).await?;
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
                    .map(|v| nullable_value(name, def, v).and_then(|v| Ok(v.as_i64())))
                    .collect::<Result<Vec<_>>>()?,
            )),
            ColumnType::Float64 => Arc::new(Float64Array::from(
                values
                    .map(|v| nullable_value(name, def, v).and_then(|v| Ok(v.as_f64())))
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

fn action_value(action: Action) -> Result<LanceAction> {
    let value = match action {
        Action::Create(v) | Action::Update(v) | Action::Delete(v) => v,
    };
    value.deserialize::<LanceAction>().map_err(Error::from)
}

fn internal(e: impl std::fmt::Display) -> cocoindex_utils::error::Error {
    cocoindex_utils::error::Error::internal_msg(e.to_string())
}

fn internal_msg(msg: impl Into<String>) -> cocoindex_utils::error::Error {
    cocoindex_utils::error::Error::internal_msg(msg.into())
}

// ---------------------------------------------------------------------------
// Query helper (convenience for examples)
// ---------------------------------------------------------------------------

/// Run a cosine-distance vector similarity search and return the top-`k` rows as
/// JSON objects (including the `_distance` column). Convenience over the raw
/// `lancedb` query API for the common "search a table by an embedding" case.
///
/// Uses cosine distance, so `1.0 - _distance` is a cosine similarity in `[0, 1]`
/// (matching the pgvector examples).
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
    use arrow_schema::DataType;
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
        assert!(fields.contains_key("embedding")); // filled with null
        assert!(!fields.contains_key("extra")); // dropped
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
