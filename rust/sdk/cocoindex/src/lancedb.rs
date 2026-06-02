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

use arrow_array::builder::Float32Builder;
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array, Int64Array, RecordBatch,
    RecordBatchIterator, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cocoindex_core::engine::target_state::{
    ChildInvalidation, TargetReconcileOutput, TargetStateProvider,
};
use cocoindex_core::state::stable_path::StableKey;
use cocoindex_utils::fingerprint::Fingerprint;
use lancedb::Connection;
use lancedb::query::{ExecutableQuery, QueryBase};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::profile::{Action, BoxedHandler, BoxedSink, RustProfile, Value};

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
        for pk in &primary_key {
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
/// the table is no longer declared it is dropped.
pub fn mount_table_target(
    ctx: &Ctx,
    db: &LanceDatabase,
    table_name: impl Into<String>,
    table_schema: TableSchema,
) -> Result<LanceTableTarget> {
    let table_name = table_name.into();
    let spec = TableSpec {
        table_name: table_name.clone(),
        table_schema: table_schema.clone(),
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
        let prev_spec = prev
            .iter()
            .filter_map(|v| v.deserialize::<TableSpec>().ok())
            .next();

        let prev_same = match (&desired_spec, &prev_spec) {
            (Some(d), Some(p)) => d == p,
            _ => false,
        };
        if desired_spec.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired_spec.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }

        match desired_spec {
            Some(spec) => {
                // A schema change means the table must be rebuilt — invalidate rows.
                let schema_changed = prev_spec
                    .as_ref()
                    .is_some_and(|p| p.table_schema != spec.table_schema);
                let action = LanceAction::CreateTable {
                    spec: spec.clone(),
                    recreate: schema_changed,
                };
                Ok(Some(TargetReconcileOutput {
                    action: Action::Update(Value::from_serializable(&action).map_err(internal)?),
                    sink: sink.clone(),
                    tracking_record: Some(Value::from_serializable(&spec).map_err(internal)?),
                    child_invalidation: schema_changed.then_some(ChildInvalidation::Destructive),
                }))
            }
            None => {
                let table_name = prev_spec
                    .map(|p| p.table_name)
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

fn table_sink(db: LanceDatabase) -> BoxedSink {
    BoxedSink::new(move |actions| {
        let db = db.clone();
        Box::pin(async move {
            for action in actions {
                match action_value(action)? {
                    LanceAction::CreateTable { spec, recreate } => {
                        ensure_table(&db, &spec, recreate).await.map_err(internal)?;
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
        return Ok(());
    }
    db.conn
        .create_empty_table(&spec.table_name, spec.table_schema.arrow_schema())
        .execute()
        .await
        .map_err(|e| Error::engine(format!("lancedb create table {:?}: {e}", spec.table_name)))?;
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
    // Rows imply the table exists.
    ensure_table(db, &spec, false).await?;
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
                values.map(|v| v.as_i64()).collect::<Vec<_>>(),
            )),
            ColumnType::Float64 => Arc::new(Float64Array::from(
                values.map(|v| v.as_f64()).collect::<Vec<_>>(),
            )),
            ColumnType::Text => Arc::new(StringArray::from(
                values
                    .map(|v| v.as_str().map(str::to_string))
                    .collect::<Vec<_>>(),
            )),
            ColumnType::Vector(dim) => build_vector_array(name, *dim, values)?,
        };
        arrays.push(array);
    }
    RecordBatch::try_new(arrow_schema.clone(), arrays)
        .map_err(|e| Error::engine(format!("build LanceDB record batch: {e}")))
}

fn build_vector_array<'a>(
    column: &str,
    dim: usize,
    values: impl Iterator<Item = &'a JsonValue>,
) -> Result<ArrayRef> {
    let mut builder = Float32Builder::new();
    let mut count = 0usize;
    for value in values {
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
            builder.append_value(f as f32);
        }
        count += 1;
    }
    let flat: Float32Array = builder.finish();
    let field = Arc::new(Field::new("item", DataType::Float32, true));
    let array = FixedSizeListArray::try_new(field, dim as i32, Arc::new(flat), None)
        .map_err(|e| Error::engine(format!("build vector array for {column:?}: {e}")))?;
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
    for name in schema.column_names() {
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
    fn row_state_filters_and_fills() {
        #[derive(serde::Serialize)]
        struct Row {
            id: i64,
            name: String,
            extra: i64,
        }
        let fields = row_state(
            &Row {
                id: 1,
                name: "x".into(),
                extra: 9,
            },
            &schema(),
        )
        .unwrap();
        assert!(fields.contains_key("id"));
        assert!(fields.contains_key("embedding")); // filled with null
        assert!(!fields.contains_key("extra")); // dropped
        assert_eq!(fields["embedding"], JsonValue::Null);
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
}
