//! SurrealDB target connector.
//!
//! Table and relation targets reconcile declared records against the previous
//! run: changed records are upserted, unchanged records are skipped, and records
//! no longer declared are deleted. `managed_by` controls whether CocoIndex owns
//! schema DDL.
//!
//! Ownership follows the Python connector: [`TableTarget::declare_record`]
//! writes with `UPSERT … MERGE`, never `CONTENT`, so fields written by other
//! owners survive and fields a record stops declaring are unset.
//! [`TableTarget::declare_fields`] lets a component contribute only some fields
//! of a record that another component owns. Relation records are single-owner
//! and are replaced whole.
//!
//! A [`TableSchema`] makes the table `SCHEMAFULL`. Derive it from a row struct
//! with [`TableSchema::from_row`]. Top-level values of `datetime`, `uuid`,
//! `decimal`, `duration` and `bytes` columns are written as typed SurrealQL
//! values, and a top-level `null` is written as `NONE`.
//!
//! Use the target constructors ([`table_target`], [`relation_target_many`],
//! [`relation_target_unconstrained`]) for composition, `declare_*` helpers inside
//! the current component, or async `mount_*` helpers when records must be
//! declared immediately.

use std::collections::BTreeMap;
use std::sync::Arc;

use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use surrealdb::Surreal;
use surrealdb::engine::remote::ws::{Client, Ws};
use surrealdb::opt::auth::Root;
use surrealdb::types::RecordId;

use crate::ctx::{ContextKey, ContextStore, Ctx};
use crate::error::{Error, Result};
use crate::row_schema::{LogicalType, SchemaField, SchemaFields};
use crate::sql_ident::validate_ident;
use crate::statediff::{
    ManagedBy, ManagedTargetOptions, MutualTrackingRecord, resolve_system_transition,
};
use crate::target_state::{
    ChildTargetDef, STATIC_ROOT_PROVIDERS, SinkCache, StableKey, StaticRootProvider, TargetAction,
    TargetActionSink, TargetChildInvalidation, TargetHandler, TargetReconcileOutput, TargetState,
    TargetStateProvider, declare_target_state, declare_target_state_with_child, mount_target,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnDef {
    pub surreal_type: String,
    pub nullable: bool,
}

impl ColumnDef {
    pub fn new(surreal_type: impl Into<String>) -> Self {
        Self {
            surreal_type: surreal_type.into(),
            nullable: false,
        }
    }

    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    /// The type in a `DEFINE FIELD … TYPE` clause: `option<T>` when nullable.
    fn type_expr(&self) -> String {
        if self.nullable {
            format!("option<{}>", self.surreal_type)
        } else {
            self.surreal_type.clone()
        }
    }
}

/// Map a derived row field to its SurrealDB type, as the leaf-type table of
/// Python's `surrealdb` `from_class` does.
fn surreal_type(field: &SchemaField) -> Result<String> {
    let surreal_type = match &field.logical_type {
        LogicalType::Bool => "bool",
        LogicalType::Int16 | LogicalType::Int32 | LogicalType::Int64 => "int",
        LogicalType::Float32 | LogicalType::Float64 => "float",
        LogicalType::Decimal => "decimal",
        LogicalType::Text => "string",
        LogicalType::Bytes => "bytes",
        LogicalType::Uuid => "uuid",
        // SurrealDB has no date-only type; a date is stored at midnight UTC.
        LogicalType::Date | LogicalType::DateTime => "datetime",
        // SurrealDB has no time-of-day type; stored as an ISO string.
        LogicalType::Time => "string",
        LogicalType::Duration => "duration",
        // Collections, maps and nested structs are sent as native arrays and
        // objects; the derive does not tell them apart, so no type constraint.
        LogicalType::Json => "any",
        LogicalType::Vector { dim: 0, .. } => {
            return Err(Error::engine(format!(
                "SurrealDB vector field {:?} has an unresolved dimension; \
                 use #[coco(vector = N)]",
                field.name
            )));
        }
        LogicalType::Vector { dim, .. } => return Ok(format!("array<float, {dim}>")),
        LogicalType::Custom(surreal_type) => return Ok(surreal_type.trim().to_string()),
    };
    Ok(surreal_type.to_string())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSchema {
    columns: BTreeMap<String, ColumnDef>,
}

impl TableSchema {
    pub fn new(columns: impl IntoIterator<Item = (impl Into<String>, ColumnDef)>) -> Result<Self> {
        let mut out = BTreeMap::new();
        for (name, def) in columns {
            let name = name.into();
            validate_ident(&name, "column name")?;
            out.insert(name, def);
        }
        Ok(Self { columns: out })
    }

    /// Derive a schema from a `#[derive(SchemaFields)]` row type (the Rust
    /// analogue of Python's `TableSchema.from_class`). Each field maps to a
    /// SurrealDB type via the same leaf-type table as Python; an `Option<T>`
    /// field becomes `option<T>`. A `#[coco(vector = N)]` field becomes
    /// `array<float, N>`, and `#[coco(type = "…")]` sets the type verbatim.
    /// Collections, maps and nested structs get no type constraint (`any`).
    pub fn from_row<T: SchemaFields>() -> Result<Self> {
        let columns = T::schema_fields()
            .into_iter()
            .map(|field| {
                let def = ColumnDef {
                    surreal_type: surreal_type(&field)?,
                    nullable: field.nullable,
                };
                Ok((field.name, def))
            })
            .collect::<Result<Vec<_>>>()?;
        Self::new(columns)
    }

    pub fn columns(&self) -> &BTreeMap<String, ColumnDef> {
        &self.columns
    }
}

pub trait IntoRecordId {
    fn to_record_id_value(&self) -> RecordIdValue;

    fn to_stable_key(&self) -> StableKey {
        self.to_record_id_value().stable_key()
    }
}

macro_rules! impl_numeric_record_id {
    ($($ty:ty),* $(,)?) => {
        $(
            impl IntoRecordId for $ty {
                fn to_record_id_value(&self) -> RecordIdValue {
                    RecordIdValue::Number(self.to_string())
                }
            }
        )*
    };
}

impl_numeric_record_id!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize);

impl IntoRecordId for String {
    fn to_record_id_value(&self) -> RecordIdValue {
        RecordIdValue::String(self.clone())
    }
}

impl IntoRecordId for &String {
    fn to_record_id_value(&self) -> RecordIdValue {
        RecordIdValue::String((*self).clone())
    }
}

impl IntoRecordId for &str {
    fn to_record_id_value(&self) -> RecordIdValue {
        RecordIdValue::String((*self).to_string())
    }
}

#[derive(Clone)]
pub struct Graph {
    db: Arc<Surreal<Client>>,
    state_id: Arc<str>,
}

fn surreal_err(e: surrealdb::Error) -> Error {
    Error::engine(format!("surrealdb: {e}"))
}

impl Graph {
    pub async fn connect(
        url: &str,
        ns: &str,
        db_name: &str,
        user: &str,
        pass: &str,
    ) -> Result<Self> {
        let db = Surreal::new::<Ws>(url).await.map_err(surreal_err)?;
        db.signin(Root {
            username: user.to_string(),
            password: pass.to_string(),
        })
        .await
        .map_err(surreal_err)?;
        db.use_ns(ns.to_string())
            .use_db(db_name.to_string())
            .await
            .map_err(surreal_err)?;
        Ok(Self {
            db: Arc::new(db),
            state_id: Arc::from(format!("{url}/{ns}/{db_name}")),
        })
    }

    pub fn state_id(&self) -> &str {
        &self.state_id
    }

    pub async fn count(&self, table: &str) -> Result<usize> {
        validate_ident(table, "table name")?;
        let mut res = self
            .db
            .query(format!("SELECT VALUE id FROM {table}"))
            .await
            .map_err(surreal_err)?;
        let ids: Vec<RecordId> = res.take(0).map_err(surreal_err)?;
        Ok(ids.len())
    }

    /// The names of the tables defined in the database (`INFO FOR DB`).
    pub async fn table_names(&self) -> Result<Vec<String>> {
        let mut res = self.db.query("INFO FOR DB").await.map_err(surreal_err)?;
        let info: Option<JsonValue> = res.take(0).map_err(surreal_err)?;
        Ok(json_object_keys(info.as_ref(), "tables"))
    }

    /// The names of the indexes defined on `table` (`INFO FOR TABLE`).
    pub async fn index_names(&self, table: &str) -> Result<Vec<String>> {
        Ok(json_object_keys(
            self.info_for_table(table).await?.as_ref(),
            "indexes",
        ))
    }

    /// The names of the fields defined on `table` (`INFO FOR TABLE`).
    pub async fn field_names(&self, table: &str) -> Result<Vec<String>> {
        Ok(json_object_keys(
            self.info_for_table(table).await?.as_ref(),
            "fields",
        ))
    }

    /// The fields of record `id` of `table`, without its `id`; `None` when the
    /// record does not exist.
    pub async fn record(&self, table: &str, id: impl IntoRecordId) -> Result<Option<JsonValue>> {
        validate_ident(table, "table name")?;
        let record_ref = format!("{table}:{}", id.to_record_id_value().surrealql());
        let mut res = self
            .db
            .query(format!("SELECT * OMIT id FROM {record_ref}"))
            .await
            .map_err(surreal_err)?;
        let rows: Vec<JsonValue> = res.take(0).map_err(surreal_err)?;
        Ok(rows.into_iter().next())
    }

    async fn info_for_table(&self, table: &str) -> Result<Option<JsonValue>> {
        validate_ident(table, "table name")?;
        let mut res = self
            .db
            .query(format!("INFO FOR TABLE {table}"))
            .await
            .map_err(surreal_err)?;
        res.take(0).map_err(surreal_err)
    }
}

/// Collect the keys of the object at `info[field]` (used to parse `INFO FOR …`).
fn json_object_keys(info: Option<&JsonValue>, field: &str) -> Vec<String> {
    info.and_then(|v| v.get(field))
        .and_then(|v| v.as_object())
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Target handles
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct TableTarget {
    table_name: Arc<str>,
    managed_by: ManagedBy,
    /// The table's record provider; also carries its vector-index attachment.
    records: TargetStateProvider<RecordState>,
}

impl TableTarget {
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Declare a record to be upserted to this table. It is written with
    /// `UPSERT … MERGE`: fields written by other owners (see
    /// [`declare_fields`](Self::declare_fields)) survive, and fields the row
    /// stops declaring are unset.
    pub fn declare_record<R: Serialize>(
        &self,
        ctx: &Ctx,
        id: impl IntoRecordId,
        row: &R,
    ) -> Result<()> {
        let value = record_state(row)?;
        declare_target_state(ctx, self.records.target_state(id.to_stable_key(), value))
    }

    /// Declare the fields one owner (`group`) contributes to an existing
    /// record. `fields` is a struct or map of field name to value.
    ///
    /// It writes `UPDATE … MERGE` with only these fields, so fields written by
    /// other owners (or by [`declare_record`](Self::declare_record)) are never
    /// touched. When the declaration goes away, or a field is dropped from it,
    /// those fields are unset. If the record does not exist, the write fails
    /// and is retried on the next run, so declare the base record in the same
    /// or another component.
    pub fn declare_fields<R: Serialize>(
        &self,
        ctx: &Ctx,
        id: impl IntoRecordId,
        group: &str,
        fields: &R,
    ) -> Result<()> {
        let value = fields_state(fields)?;
        let key = RecordKey {
            record_id: id.to_record_id_value(),
            group: Some(group.to_string()),
        };
        declare_target_state(ctx, self.records.target_state(key.stable_key(), value))
    }

    /// Declare a record whose id is taken from the row's own scalar `id` field
    /// (the analogue of Python's `declare_row`). Use [`declare_record`](Self::declare_record) to pass
    /// the id separately.
    pub fn declare_row<R: Serialize>(&self, ctx: &Ctx, row: &R) -> Result<()> {
        let value = record_state(row)?;
        let id = value
            .fields
            .get("id")
            .and_then(RecordIdValue::from_json_scalar)
            .ok_or_else(|| {
                Error::engine(
                    "declare_row requires the row to have a scalar `id` field; \
                     use declare_record to pass an explicit id",
                )
            })?;
        declare_target_state(ctx, self.records.target_state(id.stable_key(), value))
    }

    /// Declare a vector index on `field` as an attachment of this table. The
    /// index is created/recreated/dropped to match the declared options
    /// (`DEFINE INDEX … <METHOD> DIMENSION … DIST … TYPE …`).
    pub fn declare_vector_index(
        &self,
        ctx: &Ctx,
        field: &str,
        dimension: usize,
        options: VectorIndexOptions,
    ) -> Result<()> {
        validate_ident(field, "vector index field")?;
        let name = options
            .name
            .unwrap_or_else(|| format!("idx_{}__{}", self.table_name, field));
        validate_ident(&name, "vector index name")?;
        let provider: TargetStateProvider<VectorIndexSpec> =
            self.records.attachment(ctx, "vector_index")?;
        let spec = VectorIndexSpec {
            table_name: self.table_name.to_string(),
            name: name.clone(),
            field: field.to_string(),
            metric: options.metric.to_string(),
            method: options.method.to_string(),
            dimension,
            vector_type: options.vector_type.to_string(),
            managed_by: self.managed_by,
        };
        declare_target_state(
            ctx,
            provider.target_state(StableKey::Str(Arc::from(name)), spec),
        )
    }
}

/// Options for [`TableTarget::declare_vector_index`].
#[derive(Clone, Debug)]
pub struct VectorIndexOptions {
    /// Index name; defaults to `idx_<table>__<field>`.
    pub name: Option<String>,
    /// Distance metric: `"cosine"`, `"euclidean"`, or `"manhattan"`.
    pub metric: &'static str,
    /// Index method: `"mtree"` or `"hnsw"`.
    pub method: &'static str,
    /// Vector element type: `"f32"`, `"f64"`, `"i16"`, `"i32"`, or `"i64"`.
    pub vector_type: &'static str,
}

impl Default for VectorIndexOptions {
    fn default() -> Self {
        Self {
            name: None,
            metric: "cosine",
            method: "mtree",
            vector_type: "f32",
        }
    }
}

#[derive(Clone)]
pub struct RelationTarget {
    table_name: Arc<str>,
    from_tables: Arc<[String]>,
    to_tables: Arc<[String]>,
    from_table: Option<Arc<str>>,
    to_table: Option<Arc<str>>,
    records: TargetStateProvider<RecordState>,
}

impl RelationTarget {
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn declare_relation(
        &self,
        ctx: &Ctx,
        from_id: impl IntoRecordId,
        to_id: impl IntoRecordId,
    ) -> Result<()> {
        self.declare_relation_record(ctx, from_id, to_id, &JsonValue::Object(Map::new()))
    }

    pub fn declare_relation_record<R: Serialize>(
        &self,
        ctx: &Ctx,
        from_id: impl IntoRecordId,
        to_id: impl IntoRecordId,
        record: &R,
    ) -> Result<()> {
        let from_table = self.from_table.as_ref().ok_or_else(|| {
            Error::engine(
                "declare_relation requires a fixed from_table; use declare_relation_between",
            )
        })?;
        let to_table = self.to_table.as_ref().ok_or_else(|| {
            Error::engine(
                "declare_relation requires a fixed to_table; use declare_relation_between",
            )
        })?;
        self.declare_relation_record_between(
            ctx,
            from_table.as_ref(),
            from_id,
            to_table.as_ref(),
            to_id,
            record,
        )
    }

    pub fn declare_relation_between(
        &self,
        ctx: &Ctx,
        from_table: &str,
        from_id: impl IntoRecordId,
        to_table: &str,
        to_id: impl IntoRecordId,
    ) -> Result<()> {
        self.declare_relation_record_between(
            ctx,
            from_table,
            from_id,
            to_table,
            to_id,
            &JsonValue::Object(Map::new()),
        )
    }

    pub fn declare_relation_record_between<R: Serialize>(
        &self,
        ctx: &Ctx,
        from_table: &str,
        from_id: impl IntoRecordId,
        to_table: &str,
        to_id: impl IntoRecordId,
        record: &R,
    ) -> Result<()> {
        validate_ident(from_table, "relation from table name")?;
        validate_ident(to_table, "relation to table name")?;
        if !self.from_tables.is_empty() && !self.from_tables.iter().any(|t| t == from_table) {
            return Err(Error::engine(format!(
                "from_table {from_table:?} is not valid for relation {}",
                self.table_name
            )));
        }
        if !self.to_tables.is_empty() && !self.to_tables.iter().any(|t| t == to_table) {
            return Err(Error::engine(format!(
                "to_table {to_table:?} is not valid for relation {}",
                self.table_name
            )));
        }
        let from_id = from_id.to_record_id_value();
        let to_id = to_id.to_record_id_value();
        let mut fields = record_state(record)?.fields;
        let record_id = fields
            .remove("id")
            .and_then(|v| RecordIdValue::from_json_scalar(&v))
            .unwrap_or_else(|| {
                RecordIdValue::String(format!(
                    "{}{}{}{}",
                    relation_key_part(from_table),
                    relation_key_part(&from_id.key_fragment()),
                    relation_key_part(to_table),
                    relation_key_part(&to_id.key_fragment())
                ))
            });
        let value = RecordState {
            fields,
            relation: Some(RelationEndpoints {
                from_table: from_table.to_string(),
                from_id,
                to_table: to_table.to_string(),
                to_id,
            }),
        };
        declare_target_state(
            ctx,
            self.records.target_state(record_id.stable_key(), value),
        )
    }
}

fn relation_key_part(value: &str) -> String {
    format!("{}:{value};", value.len())
}

fn table_handle(spec: TableSpec, records: TargetStateProvider<RecordState>) -> TableTarget {
    TableTarget {
        table_name: Arc::from(spec.table_name),
        managed_by: spec.managed_by,
        records,
    }
}

fn relation_handle(spec: TableSpec, records: TargetStateProvider<RecordState>) -> RelationTarget {
    let from_names = spec.from_tables;
    let to_names = spec.to_tables;
    let from_table = (from_names.len() == 1).then(|| Arc::from(from_names[0].clone()));
    let to_table = (to_names.len() == 1).then(|| Arc::from(to_names[0].clone()));
    RelationTarget {
        table_name: Arc::from(spec.table_name),
        from_table,
        to_table,
        from_tables: Arc::from(from_names),
        to_tables: Arc::from(to_names),
        records,
    }
}

// ---------------------------------------------------------------------------
// Spec constructors (the composable `TargetState<TableSpec>`)
// ---------------------------------------------------------------------------

fn table_target_state(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    spec: TableSpec,
) -> Result<TargetState<TableSpec>> {
    validate_ident(&spec.table_name, "table name")?;
    let provider = TABLE_PROVIDER.get(ctx)?;
    Ok(provider.target_state(
        // Stable connection identity = the ContextKey's name; the graph is
        // resolved at apply time from the host context.
        TableKey {
            graph_key: graph.name().to_string(),
            table_name: spec.table_name.clone(),
        }
        .stable_key(),
        spec,
    ))
}

/// Build a composable [`TargetState`] for a schemaless, system-managed
/// SurrealDB table. Pass it to [`declare_table_target`]/[`mount_table_target`]
/// or the generic target-state helpers.
pub fn table_target(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
) -> Result<TargetState<TableSpec>> {
    table_target_with_schema_and_options(
        ctx,
        graph,
        table_name,
        None,
        ManagedTargetOptions::default(),
    )
}

/// [`table_target`] with an explicit schema and [`ManagedTargetOptions`].
pub fn table_target_with_schema_and_options(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
    table_schema: Option<TableSchema>,
    options: ManagedTargetOptions,
) -> Result<TargetState<TableSpec>> {
    table_target_state(
        ctx,
        graph,
        TableSpec::table(table_name.into(), table_schema, options.managed_by),
    )
}

/// Build a composable [`TargetState`] for a SurrealDB relation table that may
/// connect any of `from_tables` to any of `to_tables`.
pub fn relation_target_many(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
    from_tables: &[&TableTarget],
    to_tables: &[&TableTarget],
    table_schema: Option<TableSchema>,
) -> Result<TargetState<TableSpec>> {
    relation_target_many_with_options(
        ctx,
        graph,
        table_name,
        from_tables,
        to_tables,
        table_schema,
        ManagedTargetOptions::default(),
    )
}

/// [`relation_target_many`] with explicit [`ManagedTargetOptions`].
pub fn relation_target_many_with_options(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
    from_tables: &[&TableTarget],
    to_tables: &[&TableTarget],
    table_schema: Option<TableSchema>,
    options: ManagedTargetOptions,
) -> Result<TargetState<TableSpec>> {
    let from_names: Vec<String> = from_tables
        .iter()
        .map(|table| table.table_name().to_string())
        .collect();
    let to_names: Vec<String> = to_tables
        .iter()
        .map(|table| table.table_name().to_string())
        .collect();
    if from_names.is_empty() {
        return Err(Error::engine(
            "relation target requires at least one from table",
        ));
    }
    if to_names.is_empty() {
        return Err(Error::engine(
            "relation target requires at least one to table",
        ));
    }
    table_target_state(
        ctx,
        graph,
        TableSpec::relation(
            table_name.into(),
            from_names,
            to_names,
            table_schema,
            options.managed_by,
        ),
    )
}

/// Build a composable [`TargetState`] for an unconstrained SurrealDB relation
/// table (no fixed `from`/`to` tables).
pub fn relation_target_unconstrained(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
) -> Result<TargetState<TableSpec>> {
    relation_target_unconstrained_with_options(
        ctx,
        graph,
        table_name,
        ManagedTargetOptions::default(),
    )
}

/// [`relation_target_unconstrained`] with explicit [`ManagedTargetOptions`].
pub fn relation_target_unconstrained_with_options(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
    options: ManagedTargetOptions,
) -> Result<TargetState<TableSpec>> {
    table_target_state(
        ctx,
        graph,
        TableSpec::relation(
            table_name.into(),
            Vec::new(),
            Vec::new(),
            None,
            options.managed_by,
        ),
    )
}

// ---------------------------------------------------------------------------
// declare_* (pending declaration in the current component)
// ---------------------------------------------------------------------------

/// Declare a SurrealDB table target in the **current** component and return a
/// pending handle. The record child provider resolves when this component
/// commits; use [`mount_table_target`] when records must be declared
/// immediately.
pub fn declare_table_target(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
) -> Result<TableTarget> {
    declare_table_target_with_schema_and_options(
        ctx,
        graph,
        table_name,
        None,
        ManagedTargetOptions::default(),
    )
}

/// [`declare_table_target`] with an explicit schema and [`ManagedTargetOptions`].
pub fn declare_table_target_with_schema_and_options(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
    table_schema: Option<TableSchema>,
    options: ManagedTargetOptions,
) -> Result<TableTarget> {
    let ts = table_target_with_schema_and_options(ctx, graph, table_name, table_schema, options)?;
    let spec = ts.value().clone();
    let records = declare_target_state_with_child::<TableSpec, RecordState>(ctx, ts)?;
    Ok(table_handle(spec, records))
}

/// Declare a SurrealDB relation target in the **current** component and return
/// a pending handle. Use [`mount_relation_target`] when relation records must
/// be declared immediately.
pub fn declare_relation_target_many(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
    from_tables: &[&TableTarget],
    to_tables: &[&TableTarget],
    table_schema: Option<TableSchema>,
) -> Result<RelationTarget> {
    declare_relation_target_many_with_options(
        ctx,
        graph,
        table_name,
        from_tables,
        to_tables,
        table_schema,
        ManagedTargetOptions::default(),
    )
}

/// [`declare_relation_target_many`] with explicit [`ManagedTargetOptions`].
pub fn declare_relation_target_many_with_options(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
    from_tables: &[&TableTarget],
    to_tables: &[&TableTarget],
    table_schema: Option<TableSchema>,
    options: ManagedTargetOptions,
) -> Result<RelationTarget> {
    let ts = relation_target_many_with_options(
        ctx,
        graph,
        table_name,
        from_tables,
        to_tables,
        table_schema,
        options,
    )?;
    let spec = ts.value().clone();
    let records = declare_target_state_with_child::<TableSpec, RecordState>(ctx, ts)?;
    Ok(relation_handle(spec, records))
}

/// Declare an unconstrained SurrealDB relation target in the current component.
pub fn declare_relation_target_unconstrained(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
) -> Result<RelationTarget> {
    declare_relation_target_unconstrained_with_options(
        ctx,
        graph,
        table_name,
        ManagedTargetOptions::default(),
    )
}

/// [`declare_relation_target_unconstrained`] with explicit [`ManagedTargetOptions`].
pub fn declare_relation_target_unconstrained_with_options(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
    options: ManagedTargetOptions,
) -> Result<RelationTarget> {
    let ts = relation_target_unconstrained_with_options(ctx, graph, table_name, options)?;
    let spec = ts.value().clone();
    let records = declare_target_state_with_child::<TableSpec, RecordState>(ctx, ts)?;
    Ok(relation_handle(spec, records))
}

// ---------------------------------------------------------------------------
// mount_* (foreground; records can be declared immediately)
// ---------------------------------------------------------------------------

pub async fn mount_table_target(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
) -> Result<TableTarget> {
    mount_table_target_with_schema_and_options(
        ctx,
        graph,
        table_name,
        None,
        ManagedTargetOptions::default(),
    )
    .await
}

pub async fn mount_table_target_with_options(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
    options: ManagedTargetOptions,
) -> Result<TableTarget> {
    mount_table_target_with_schema_and_options(ctx, graph, table_name, None, options).await
}

pub async fn mount_table_target_with_schema(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
    table_schema: Option<TableSchema>,
) -> Result<TableTarget> {
    mount_table_target_with_schema_and_options(
        ctx,
        graph,
        table_name,
        table_schema,
        ManagedTargetOptions::default(),
    )
    .await
}

pub async fn mount_table_target_with_schema_and_options(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
    table_schema: Option<TableSchema>,
    options: ManagedTargetOptions,
) -> Result<TableTarget> {
    let ts = table_target_with_schema_and_options(ctx, graph, table_name, table_schema, options)?;
    let spec = ts.value().clone();
    let records = mount_target::<TableSpec, RecordState>(ctx, ts).await?;
    Ok(table_handle(spec, records))
}

pub async fn mount_relation_target(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
) -> Result<RelationTarget> {
    mount_relation_target_many_with_options(
        ctx,
        graph,
        table_name,
        &[from_table],
        &[to_table],
        None,
        ManagedTargetOptions::default(),
    )
    .await
}

pub async fn mount_relation_target_with_options(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
    options: ManagedTargetOptions,
) -> Result<RelationTarget> {
    mount_relation_target_many_with_options(
        ctx,
        graph,
        table_name,
        &[from_table],
        &[to_table],
        None,
        options,
    )
    .await
}

pub async fn mount_relation_target_many(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
    from_tables: &[&TableTarget],
    to_tables: &[&TableTarget],
    table_schema: Option<TableSchema>,
) -> Result<RelationTarget> {
    mount_relation_target_many_with_options(
        ctx,
        graph,
        table_name,
        from_tables,
        to_tables,
        table_schema,
        ManagedTargetOptions::default(),
    )
    .await
}

pub async fn mount_relation_target_many_with_options(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
    from_tables: &[&TableTarget],
    to_tables: &[&TableTarget],
    table_schema: Option<TableSchema>,
    options: ManagedTargetOptions,
) -> Result<RelationTarget> {
    let ts = relation_target_many_with_options(
        ctx,
        graph,
        table_name,
        from_tables,
        to_tables,
        table_schema,
        options,
    )?;
    let spec = ts.value().clone();
    let records = mount_target::<TableSpec, RecordState>(ctx, ts).await?;
    Ok(relation_handle(spec, records))
}

pub async fn mount_relation_target_unconstrained(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
) -> Result<RelationTarget> {
    mount_relation_target_unconstrained_with_options(
        ctx,
        graph,
        table_name,
        ManagedTargetOptions::default(),
    )
    .await
}

pub async fn mount_relation_target_unconstrained_with_options(
    ctx: &Ctx,
    graph: &ContextKey<Graph>,
    table_name: impl Into<String>,
    options: ManagedTargetOptions,
) -> Result<RelationTarget> {
    let ts = relation_target_unconstrained_with_options(ctx, graph, table_name, options)?;
    let spec = ts.value().clone();
    let records = mount_target::<TableSpec, RecordState>(ctx, ts).await?;
    Ok(relation_handle(spec, records))
}

// ---------------------------------------------------------------------------
// Internal specs / actions
// ---------------------------------------------------------------------------

/// Spec for a SurrealDB table/relation (the declared container value).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSpec {
    table_name: String,
    table_schema: Option<TableSchema>,
    is_relation: bool,
    from_tables: Vec<String>,
    to_tables: Vec<String>,
    #[serde(default)]
    managed_by: ManagedBy,
}

impl TableSpec {
    fn table(table_name: String, table_schema: Option<TableSchema>, managed_by: ManagedBy) -> Self {
        Self {
            table_name,
            table_schema,
            is_relation: false,
            from_tables: Vec::new(),
            to_tables: Vec::new(),
            managed_by,
        }
    }

    fn relation(
        table_name: String,
        mut from_tables: Vec<String>,
        mut to_tables: Vec<String>,
        table_schema: Option<TableSchema>,
        managed_by: ManagedBy,
    ) -> Self {
        from_tables.sort();
        from_tables.dedup();
        to_tables.sort();
        to_tables.dedup();
        Self {
            table_name,
            table_schema,
            is_relation: true,
            from_tables,
            to_tables,
            managed_by,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RecordState {
    fields: Map<String, JsonValue>,
    relation: Option<RelationEndpoints>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct RelationEndpoints {
    from_table: String,
    from_id: RecordIdValue,
    to_table: String,
    to_id: RecordIdValue,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum RecordIdValue {
    Number(String),
    String(String),
}

impl RecordIdValue {
    fn from_json_scalar(value: &JsonValue) -> Option<Self> {
        match value {
            JsonValue::String(s) => Some(Self::String(s.clone())),
            JsonValue::Number(n) => Some(Self::Number(n.to_string())),
            JsonValue::Bool(b) => Some(Self::String(b.to_string())),
            _ => None,
        }
    }

    fn stable_key(&self) -> StableKey {
        match self {
            Self::Number(value) => value
                .parse::<i64>()
                .map(StableKey::Int)
                .unwrap_or_else(|_| StableKey::Str(Arc::from(value.clone()))),
            Self::String(value) => StableKey::Str(Arc::from(value.clone())),
        }
    }

    fn key_fragment(&self) -> String {
        match self {
            Self::Number(value) | Self::String(value) => value.clone(),
        }
    }

    fn surrealql(&self) -> String {
        match self {
            Self::Number(value) => value.clone(),
            Self::String(value) => quote_record_id(value),
        }
    }
}

/// Action emitted by the table container handler.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct TableAction {
    /// The `ContextKey` name of the graph the table lives in.
    graph_key: String,
    /// `Some` for a define (create/update), carrying the desired spec.
    spec: Option<TableSpec>,
    /// `Some` for a removal (orphaned table), carrying the table name.
    drop: Option<String>,
    /// The table's "main" signature (relation/schema-mode/from-to) changed:
    /// `REMOVE TABLE` and redefine.
    #[serde(default)]
    recreate: bool,
    /// Fields no longer declared — `REMOVE FIELD`.
    #[serde(default)]
    drop_fields: Vec<String>,
    /// Fields whose declared type/nullability changed — `DEFINE FIELD OVERWRITE`.
    #[serde(default)]
    retype_fields: Vec<String>,
}

/// Action emitted by the record child handler.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct RecordAction {
    record_id: RecordIdValue,
    write: RecordWrite,
}

/// What a [`RecordAction`] does to its record.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
enum RecordWrite {
    /// Record mode: `UPSERT … MERGE`, creating the record when it is missing.
    Upsert(MergeContent),
    /// Fields mode: `UPDATE … MERGE` onto a record that must exist.
    SetFields(MergeContent),
    /// Fields mode, group removed: unset the fields it owned. A missing record
    /// is fine.
    UnsetFields(Vec<String>),
    /// Relation records are single-owner: `DELETE` + `RELATE` replaces the edge.
    Relate {
        fields: Map<String, JsonValue>,
        endpoints: RelationEndpoints,
    },
    Delete,
}

/// The body of a `MERGE`: fields to set, and fields owned before but no longer
/// declared, which become `NONE`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct MergeContent {
    set: Map<String, JsonValue>,
    unset: Vec<String>,
}

impl RecordWrite {
    /// Apply order within one batch: upserts, fields writes, relation upserts,
    /// deletes. A fields write then finds the record an upsert of the same
    /// batch creates.
    fn rank(&self) -> u8 {
        match self {
            Self::Upsert(_) => 0,
            Self::SetFields(_) | Self::UnsetFields(_) => 1,
            Self::Relate { .. } => 2,
            Self::Delete => 3,
        }
    }
}

/// A record target state's key: the record id, plus the field group for a
/// [`TableTarget::declare_fields`] declaration.
struct RecordKey {
    record_id: RecordIdValue,
    group: Option<String>,
}

impl RecordKey {
    fn stable_key(&self) -> StableKey {
        let id = self.record_id.stable_key();
        match &self.group {
            Some(group) => {
                StableKey::Array(Arc::from([id, StableKey::Str(Arc::from(group.as_str()))]))
            }
            None => id,
        }
    }

    fn from_stable_key(key: &StableKey) -> Result<Self> {
        if let StableKey::Array(parts) = key {
            if let [id, StableKey::Str(group)] = &parts[..] {
                return Ok(Self {
                    record_id: stable_key_to_record_id(id)?,
                    group: Some(group.to_string()),
                });
            }
            return Err(Error::engine(format!(
                "unsupported SurrealDB record key: {key:?}"
            )));
        }
        Ok(Self {
            record_id: stable_key_to_record_id(key)?,
            group: None,
        })
    }
}

/// Tracking record of a record target state.
#[derive(Clone, Serialize, Deserialize, PartialEq)]
enum RecordTracking {
    /// A relation record: a fingerprint of the whole edge. It is replaced
    /// whole, so it owns no field names.
    Fingerprint(Fingerprint),
    /// A record or field group written with `MERGE`.
    Fields(FieldsTracking),
}

/// A fingerprint plus the owned field names. The names let a delete, or a
/// shrunk declaration, unset exactly the fields this owner wrote.
#[derive(Clone, Serialize, Deserialize, PartialEq)]
struct FieldsTracking {
    fingerprint: Fingerprint,
    names: Vec<String>,
}

/// Field names any previous run may have written.
fn owned_names(prev: &[RecordTracking]) -> std::collections::BTreeSet<&str> {
    prev.iter()
        .filter_map(|tracking| match tracking {
            RecordTracking::Fields(fields) => Some(fields.names.iter().map(String::as_str)),
            RecordTracking::Fingerprint(_) => None,
        })
        .flatten()
        .collect()
}

// ---------------------------------------------------------------------------
// Table container handler (root) + sink yielding record children
// ---------------------------------------------------------------------------

/// Resolve the live SurrealDB graph connection from the host context by its
/// stable `db_key` (the ContextKey name), used at apply time.
fn resolve_graph(host_ctx: &Arc<ContextStore>, graph_key: &str) -> Result<Arc<Graph>> {
    host_ctx.resolve::<Graph>(graph_key).ok_or_else(|| {
        Error::engine(format!(
            "surrealdb target: connection `{graph_key}` was not provided in the environment \
             (call Environment::builder().provide_key(&KEY, graph))"
        ))
    })
}

#[linkme::distributed_slice(STATIC_ROOT_PROVIDERS)]
static TABLE_PROVIDER: StaticRootProvider =
    StaticRootProvider::new::<TableSpec, TableHandler>("cocoindex/surrealdb/table");

/// A table (or relation) target state's key: the graph's `ContextKey` name
/// plus the table name.
struct TableKey {
    graph_key: String,
    table_name: String,
}

impl TableKey {
    fn stable_key(self) -> StableKey {
        StableKey::Array(Arc::from([
            StableKey::Str(Arc::from(self.graph_key)),
            StableKey::Str(Arc::from(self.table_name)),
        ]))
    }

    fn from_stable_key(key: &StableKey) -> Result<Self> {
        if let StableKey::Array(parts) = key
            && let [StableKey::Str(graph_key), StableKey::Str(table_name)] = &parts[..]
        {
            return Ok(Self {
                graph_key: graph_key.to_string(),
                table_name: table_name.to_string(),
            });
        }
        Err(Error::engine(format!(
            "unexpected surrealdb table target key: {key:?}"
        )))
    }
}

/// Root handler for every SurrealDB table/relation target: stateless, the
/// graph is named by each target state's [`TableKey`].
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
        let graph_key = TableKey::from_stable_key(&key)?.graph_key;
        match desired {
            // Always emit when declared so the sink builds the record child.
            Some(spec) => {
                let tracking = MutualTrackingRecord::new(spec.clone(), spec.managed_by);
                // The previous system spec drives schema evolution: a change to the
                // table's "main" shape (relation flag, schemafull/schemaless, or the
                // relation from/to tables) forces a destructive `REMOVE TABLE` +
                // redefine; otherwise individual fields are dropped (`REMOVE FIELD`)
                // or retyped (`DEFINE FIELD OVERWRITE`) in place.
                let prev_spec = prev
                    .iter()
                    .find(|v| v.managed_by.is_system())
                    .map(|v| v.tracking_record.clone());
                let _resolved =
                    resolve_system_transition(Some(tracking.clone()), prev, prev_may_be_missing);

                let mut recreate = false;
                let mut drop_fields: Vec<String> = Vec::new();
                let mut retype_fields: Vec<String> = Vec::new();
                if spec.managed_by.is_system()
                    && let Some(prev_spec) = &prev_spec
                {
                    let main_changed = prev_spec.is_relation != spec.is_relation
                        || prev_spec.table_schema.is_some() != spec.table_schema.is_some()
                        || prev_spec.from_tables != spec.from_tables
                        || prev_spec.to_tables != spec.to_tables;
                    if main_changed {
                        recreate = true;
                    } else if let (Some(prev_schema), Some(desired_schema)) =
                        (&prev_spec.table_schema, &spec.table_schema)
                    {
                        for (name, col) in desired_schema.columns() {
                            if name == "id" {
                                continue;
                            }
                            if let Some(prev_col) = prev_schema.columns().get(name)
                                && (prev_col.surreal_type != col.surreal_type
                                    || prev_col.nullable != col.nullable)
                            {
                                retype_fields.push(name.clone());
                            }
                        }
                        for name in prev_schema.columns().keys() {
                            if name != "id" && !desired_schema.columns().contains_key(name) {
                                drop_fields.push(name.clone());
                            }
                        }
                    }
                }

                let child_invalidation = if recreate {
                    Some(TargetChildInvalidation::Destructive)
                } else if !drop_fields.is_empty() || !retype_fields.is_empty() {
                    Some(TargetChildInvalidation::Lossy)
                } else {
                    None
                };

                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Update(TableAction {
                        graph_key,
                        spec: Some(spec),
                        drop: None,
                        recreate,
                        drop_fields,
                        retype_fields,
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
                        graph_key,
                        spec: None,
                        drop: Some(prev_spec.table_name),
                        recreate: false,
                        drop_fields: Vec::new(),
                        retype_fields: Vec::new(),
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
                        let graph = resolve_graph(&host_ctx, &a.graph_key)?;
                        let spec = a
                            .spec
                            .ok_or_else(|| Error::engine("SurrealDB table action missing spec"))?;
                        // A "main"-shape change can't be applied in place;
                        // remove and redefine (records replayed via the
                        // Destructive child invalidation set at reconcile).
                        if a.recreate && spec.managed_by.is_system() {
                            remove_table(&graph, &spec.table_name).await?;
                        }
                        define_table(&graph, &spec).await?;
                        if !a.drop_fields.is_empty() || !a.retype_fields.is_empty() {
                            apply_field_changes(&graph, &spec, &a.drop_fields, &a.retype_fields)
                                .await?;
                        }
                    }
                    TargetAction::Delete(a) => {
                        if let Some(table_name) = a.drop {
                            let graph = resolve_graph(&host_ctx, &a.graph_key)?;
                            remove_table(&graph, &table_name).await?;
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
                .ok_or_else(|| Error::engine("SurrealDB table action missing spec"))?;
            Ok(ChildTargetDef::new::<RecordState, _>(RecordHandler {
                sink: SinkCache::default(),
                graph_key: a.graph_key.clone(),
                spec,
            }))
        },
    )
}

// ---------------------------------------------------------------------------
// Record handler (child) + sink + vector-index attachment
// ---------------------------------------------------------------------------

struct RecordHandler {
    sink: SinkCache<RecordAction>,
    graph_key: String,
    spec: TableSpec,
}

impl TargetHandler<RecordState> for RecordHandler {
    type TrackingRecord = RecordTracking;
    type Action = RecordAction;

    fn attachments(&self) -> Result<Vec<(String, ChildTargetDef)>> {
        Ok(vec![(
            "vector_index".to_string(),
            ChildTargetDef::new::<VectorIndexSpec, _>(VectorIndexHandler {
                sink: SinkCache::default(),
                graph_key: self.graph_key.clone(),
            }),
        )])
    }

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<RecordState>,
        prev: Vec<RecordTracking>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<RecordAction, RecordTracking>>> {
        let RecordKey { record_id, group } = RecordKey::from_stable_key(&key)?;
        let in_fields_mode = group.is_some();
        let (write, tracking) = match desired {
            None => {
                if prev.is_empty() && !prev_may_be_missing {
                    return Ok(None);
                }
                let write = if in_fields_mode {
                    RecordWrite::UnsetFields(
                        owned_names(&prev).into_iter().map(String::from).collect(),
                    )
                } else {
                    RecordWrite::Delete
                };
                (write, None)
            }
            Some(state) => {
                // Track a cheap fingerprint of the record state (not the full record).
                let fingerprint = Fingerprint::from(&state).map_err(Error::from)?;
                let RecordState { fields, relation } = state;
                let (tracking, write) = match relation {
                    Some(endpoints) => (
                        RecordTracking::Fingerprint(fingerprint),
                        RecordWrite::Relate { fields, endpoints },
                    ),
                    None => {
                        // `fields` is sorted by name (`serde_json::Map`).
                        let names: Vec<String> = fields.keys().cloned().collect();
                        let unset = owned_names(&prev)
                            .into_iter()
                            .filter(|name| !fields.contains_key(*name))
                            .map(String::from)
                            .collect();
                        let content = MergeContent { set: fields, unset };
                        let write = if in_fields_mode {
                            RecordWrite::SetFields(content)
                        } else {
                            RecordWrite::Upsert(content)
                        };
                        (
                            RecordTracking::Fields(FieldsTracking { fingerprint, names }),
                            write,
                        )
                    }
                };
                if !prev_may_be_missing && !prev.is_empty() && prev.iter().all(|p| *p == tracking) {
                    return Ok(None);
                }
                (write, Some(tracking))
            }
        };
        Ok(Some(TargetReconcileOutput {
            action: TargetAction::Update(RecordAction { record_id, write }),
            sink: self.sink.get(|| self.record_sink()),
            tracking_record: tracking,
            child_invalidation: None,
        }))
    }
}

impl RecordHandler {
    fn record_sink(&self) -> TargetActionSink<RecordAction> {
        let graph_key = self.graph_key.clone();
        let spec = self.spec.clone();
        TargetActionSink::from_async_fn_with_ctx(
            move |host_ctx, actions: Vec<TargetAction<RecordAction>>| {
                let graph_key = graph_key.clone();
                let spec = spec.clone();
                async move {
                    let graph = resolve_graph(&host_ctx, &graph_key)?;
                    let actions = actions
                        .into_iter()
                        .map(|action| match action {
                            TargetAction::Create(r)
                            | TargetAction::Update(r)
                            | TargetAction::Delete(r) => r,
                        })
                        .collect();
                    apply_records(&graph, &spec, actions).await
                }
            },
        )
    }
}

// ---------------------------------------------------------------------------
// Vector-index attachment handler + sink
// ---------------------------------------------------------------------------

/// Spec for a SurrealDB vector index (an attachment of a table). Used as both
/// the declared value and the tracking record (equality = no change).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorIndexSpec {
    table_name: String,
    name: String,
    field: String,
    metric: String,
    method: String,
    dimension: usize,
    vector_type: String,
    #[serde(default)]
    managed_by: ManagedBy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct VectorIndexAction {
    name: String,
    table_name: String,
    /// `Some` to (re)create the index, `None` to remove it.
    spec: Option<VectorIndexSpec>,
}

struct VectorIndexHandler {
    sink: SinkCache<VectorIndexAction>,
    graph_key: String,
}

impl TargetHandler<VectorIndexSpec> for VectorIndexHandler {
    type TrackingRecord = VectorIndexSpec;
    type Action = VectorIndexAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<VectorIndexSpec>,
        prev: Vec<VectorIndexSpec>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<VectorIndexAction, VectorIndexSpec>>> {
        let name = match &key {
            StableKey::Str(s) | StableKey::Symbol(s) => s.to_string(),
            other => {
                return Err(Error::engine(format!(
                    "unsupported vector index key: {other:?}"
                )));
            }
        };
        let prev_same = desired
            .as_ref()
            .is_some_and(|d| !prev.is_empty() && prev.iter().all(|p| p == d));
        if desired.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }
        let table_name = desired
            .as_ref()
            .map(|s| s.table_name.clone())
            .or_else(|| prev.first().map(|p| p.table_name.clone()))
            .unwrap_or_default();
        Ok(Some(TargetReconcileOutput {
            action: TargetAction::Update(VectorIndexAction {
                name,
                table_name,
                spec: desired.clone(),
            }),
            sink: self.sink.get(|| self.vector_index_sink()),
            tracking_record: desired,
            child_invalidation: None,
        }))
    }
}

impl VectorIndexHandler {
    fn vector_index_sink(&self) -> TargetActionSink<VectorIndexAction> {
        let graph_key = self.graph_key.clone();
        TargetActionSink::from_async_fn_with_ctx(
            move |host_ctx, actions: Vec<TargetAction<VectorIndexAction>>| {
                let graph_key = graph_key.clone();
                async move {
                    let graph = resolve_graph(&host_ctx, &graph_key)?;
                    for action in actions {
                        let action = match action {
                            TargetAction::Create(a)
                            | TargetAction::Update(a)
                            | TargetAction::Delete(a) => a,
                        };
                        apply_vector_index(&graph, action).await?;
                    }
                    Ok(())
                }
            },
        )
    }
}

async fn apply_vector_index(graph: &Graph, action: VectorIndexAction) -> Result<()> {
    validate_ident(&action.name, "vector index name")?;
    validate_ident(&action.table_name, "table name")?;
    match action.spec {
        // User-managed indexes: CocoIndex does not own the DDL.
        Some(spec) if spec.managed_by.is_user() => Ok(()),
        Some(spec) => {
            validate_ident(&spec.field, "vector index field")?;
            run_query(
                graph,
                &format!(
                    "REMOVE INDEX IF EXISTS {} ON TABLE {}",
                    action.name, action.table_name
                ),
            )
            .await?;
            let stmt = format!(
                "DEFINE INDEX {} ON {} FIELDS {} {} DIMENSION {} DIST {} TYPE {}",
                action.name,
                action.table_name,
                spec.field,
                spec.method.to_uppercase(),
                spec.dimension,
                spec.metric.to_uppercase(),
                spec.vector_type.to_uppercase(),
            );
            run_query(graph, &stmt).await
        }
        None => {
            run_query(
                graph,
                &format!(
                    "REMOVE INDEX IF EXISTS {} ON TABLE {}",
                    action.name, action.table_name
                ),
            )
            .await
        }
    }
}

async fn run_query(graph: &Graph, stmt: &str) -> Result<()> {
    graph
        .db
        .query(stmt)
        .await
        .map_err(surreal_err)?
        .check()
        .map_err(surreal_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// DB I/O
// ---------------------------------------------------------------------------

async fn define_table(graph: &Graph, spec: &TableSpec) -> Result<()> {
    if spec.managed_by.is_user() {
        return Ok(());
    }
    let schema_mode = if spec.table_schema.is_some() {
        "SCHEMAFULL"
    } else {
        "SCHEMALESS"
    };
    let stmt = if spec.is_relation {
        if spec.from_tables.is_empty() || spec.to_tables.is_empty() {
            format!(
                "DEFINE TABLE IF NOT EXISTS {} TYPE RELATION {schema_mode}",
                spec.table_name
            )
        } else {
            format!(
                "DEFINE TABLE IF NOT EXISTS {} TYPE RELATION FROM {} TO {} {schema_mode}",
                spec.table_name,
                spec.from_tables.join("|"),
                spec.to_tables.join("|"),
            )
        }
    } else {
        format!(
            "DEFINE TABLE IF NOT EXISTS {} {schema_mode}",
            spec.table_name
        )
    };
    graph
        .db
        .query(stmt)
        .await
        .map_err(surreal_err)?
        .check()
        .map_err(surreal_err)?;
    if let Some(schema) = &spec.table_schema {
        for (name, column) in schema.columns() {
            if name == "id" {
                continue;
            }
            let type_expr = column.type_expr();
            graph
                .db
                .query(format!(
                    "DEFINE FIELD IF NOT EXISTS {name} ON {} TYPE {type_expr}",
                    spec.table_name
                ))
                .await
                .map_err(surreal_err)?
                .check()
                .map_err(surreal_err)?;
        }
    }
    Ok(())
}

/// Apply incremental field changes to an existing system-managed table: drop
/// undeclared fields and overwrite the type of changed ones. (Added fields are
/// handled by `DEFINE FIELD IF NOT EXISTS` in `define_table`.) The `id` field is
/// never altered here.
async fn apply_field_changes(
    graph: &Graph,
    spec: &TableSpec,
    drop_fields: &[String],
    retype_fields: &[String],
) -> Result<()> {
    if spec.managed_by.is_user() {
        return Ok(());
    }
    let Some(schema) = &spec.table_schema else {
        return Ok(());
    };
    for name in drop_fields {
        graph
            .db
            .query(format!(
                "REMOVE FIELD IF EXISTS {name} ON {}",
                spec.table_name
            ))
            .await
            .map_err(surreal_err)?
            .check()
            .map_err(surreal_err)?;
    }
    for name in retype_fields {
        if let Some(col) = schema.columns().get(name) {
            let type_expr = col.type_expr();
            graph
                .db
                .query(format!(
                    "DEFINE FIELD OVERWRITE {name} ON {} TYPE {type_expr}",
                    spec.table_name
                ))
                .await
                .map_err(surreal_err)?
                .check()
                .map_err(surreal_err)?;
        }
    }
    Ok(())
}

async fn remove_table(graph: &Graph, table_name: &str) -> Result<()> {
    validate_ident(table_name, "table name")?;
    graph
        .db
        .query(format!("REMOVE TABLE IF EXISTS {table_name}"))
        .await
        .map_err(surreal_err)?
        .check()
        .map_err(surreal_err)?;
    Ok(())
}

async fn apply_records(graph: &Graph, spec: &TableSpec, actions: Vec<RecordAction>) -> Result<()> {
    if actions.is_empty() {
        return Ok(());
    }
    // Ensure the table exists for system-managed targets (idempotent).
    if spec.managed_by.is_system() {
        define_table(graph, spec).await?;
    }
    let statements = record_statements(spec, actions)?;
    graph
        .db
        .query(statements)
        .await
        .map_err(surreal_err)?
        .check()
        .map_err(surreal_err)?;
    Ok(())
}

/// The SurrealQL of one batch of record actions: one transaction, ordered by
/// [`RecordWrite::rank`].
fn record_statements(spec: &TableSpec, mut actions: Vec<RecordAction>) -> Result<String> {
    validate_ident(&spec.table_name, "table name")?;
    actions.sort_by_key(|action| action.write.rank()); // stable
    let schema = spec.table_schema.as_ref();

    let mut statements = String::from("BEGIN TRANSACTION;\n");
    for RecordAction { record_id, write } in actions {
        let record_ref = format!("{}:{}", spec.table_name, record_id.surrealql());
        match write {
            RecordWrite::Upsert(content) => {
                let content = merge_object(schema, &content.set, &content.unset)?;
                statements.push_str(&format!(
                    "UPSERT {record_ref} MERGE {content} RETURN NONE;\n"
                ));
            }
            RecordWrite::SetFields(content) => {
                let content = merge_object(schema, &content.set, &content.unset)?;
                let missing = json_text(&format!("fields target record missing: {record_ref}"))?;
                statements.push_str(&format!(
                    "LET $updated = (UPDATE ONLY {record_ref} MERGE {content} RETURN id);\n\
                     IF $updated = NONE {{ THROW {missing}; }};\n"
                ));
            }
            RecordWrite::UnsetFields(names) => {
                // Nothing to unset when no earlier run recorded its field names.
                if !names.iter().all(|name| name == "id") {
                    let content = merge_object(schema, &Map::new(), &names)?;
                    statements.push_str(&format!(
                        "UPDATE {record_ref} MERGE {content} RETURN NONE;\n"
                    ));
                }
            }
            RecordWrite::Relate { fields, endpoints } => {
                validate_ident(&endpoints.from_table, "relation from table name")?;
                validate_ident(&endpoints.to_table, "relation to table name")?;
                let from_ref =
                    format!("{}:{}", endpoints.from_table, endpoints.from_id.surrealql());
                let to_ref = format!("{}:{}", endpoints.to_table, endpoints.to_id.surrealql());
                let content = merge_object(schema, &fields, &[])?;
                statements.push_str(&format!(
                    "DELETE {record_ref}; RELATE {from_ref}->{record_ref}->{to_ref} CONTENT {content};\n"
                ));
            }
            RecordWrite::Delete => {
                statements.push_str(&format!("DELETE {record_ref};\n"));
            }
        }
    }
    statements.push_str("COMMIT TRANSACTION;\n");
    Ok(statements)
}

fn json_text<T: Serialize + ?Sized>(value: &T) -> Result<String> {
    serde_json::to_string(value)
        .map_err(|e| Error::engine(format!("serialize SurrealDB record: {e}")))
}

/// A SurrealQL object literal with `set` fields as values and `unset` fields as
/// `NONE` (in a `MERGE`, `NONE` removes the field). The record id is carried by
/// the statement's record reference, so an `id` field is left out (matches
/// Python, which excludes `id` from the content).
fn merge_object(
    schema: Option<&TableSchema>,
    set: &Map<String, JsonValue>,
    unset: &[String],
) -> Result<String> {
    let mut parts = Vec::with_capacity(set.len() + unset.len());
    for name in unset {
        if name != "id" {
            parts.push(format!("{}: NONE", json_text(name)?));
        }
    }
    for (name, value) in set {
        if name != "id" {
            let column = schema.and_then(|schema| schema.columns.get(name));
            parts.push(format!(
                "{}: {}",
                json_text(name)?,
                surql_value(column, value)?
            ));
        }
    }
    Ok(format!("{{{}}}", parts.join(", ")))
}

/// A top-level field value as SurrealQL. `null` is `NONE` (what an `option<T>`
/// field takes). The value of a `datetime`, `uuid`, `decimal`, `duration` or
/// `bytes` column is written typed, as Python sends typed parameters; any other
/// value is its JSON, which SurrealQL reads as native arrays and objects.
fn surql_value(column: Option<&ColumnDef>, value: &JsonValue) -> Result<String> {
    if value.is_null() {
        return Ok("NONE".to_string());
    }
    let typed = match (column.map(|c| c.surreal_type.trim()), value) {
        (Some("datetime"), JsonValue::String(s)) => {
            Some(format!("d{}", json_text(&datetime_text(s))?))
        }
        (Some("uuid"), JsonValue::String(_)) => Some(format!("u{}", json_text(value)?)),
        (Some("decimal"), JsonValue::String(_) | JsonValue::Number(_)) => {
            Some(format!("<decimal> {}", json_text(value)?))
        }
        (Some("duration"), JsonValue::String(_)) => {
            Some(format!("<duration> {}", json_text(value)?))
        }
        (Some("duration"), JsonValue::Object(_)) => duration_literal(value),
        (Some("bytes"), JsonValue::Array(items)) => items
            .iter()
            .map(|item| item.as_u64().and_then(|byte| u8::try_from(byte).ok()))
            .collect::<Option<Vec<u8>>>()
            .map(|bytes| format!("encoding::base64::decode(\"{}\")", base64_text(&bytes))),
        _ => None,
    };
    match typed {
        Some(text) => Ok(text),
        None => json_text(value),
    }
}

/// An RFC 3339 datetime from a serialized `chrono` value: a date gets midnight
/// UTC, and a datetime without an offset is taken as UTC.
fn datetime_text(value: &str) -> String {
    match value.split_once('T') {
        None => format!("{value}T00:00:00Z"),
        Some((_, time)) if time.ends_with('Z') || time.contains(['+', '-']) => value.to_string(),
        Some(_) => format!("{value}Z"),
    }
}

/// A duration literal (`5s250ns`) from a serialized `std::time::Duration`
/// (`{"secs": …, "nanos": …}`).
fn duration_literal(value: &JsonValue) -> Option<String> {
    let object = value.as_object()?;
    if object.len() != 2 {
        return None;
    }
    let secs = object.get("secs")?.as_u64()?;
    let nanos = object.get("nanos")?.as_u64()?;
    Some(match (secs, nanos) {
        (secs, 0) => format!("{secs}s"),
        (0, nanos) => format!("{nanos}ns"),
        (secs, nanos) => format!("{secs}s{nanos}ns"),
    })
}

/// Standard base64 with padding.
fn base64_text(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let group = chunk.iter().enumerate().fold(0u32, |acc, (i, byte)| {
            acc | (u32::from(*byte) << (16 - 8 * i))
        });
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(char::from(
                    ALPHABET[((group >> (18 - 6 * i)) & 0x3f) as usize],
                ));
            } else {
                out.push('=');
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn record_state<R: Serialize>(row: &R) -> Result<RecordState> {
    // Reject non-finite floats up front: `serde_json` would turn NaN/±Inf into
    // JSON null, silently corrupting the value.
    crate::finite::ensure_finite(row)
        .map_err(|e| Error::engine(format!("SurrealDB target record has a {e}")))?;
    let value = serde_json::to_value(row)
        .map_err(|e| Error::engine(format!("serialize SurrealDB target record: {e}")))?;
    let fields = match value {
        JsonValue::Object(map) => map,
        other => {
            let mut map = Map::new();
            map.insert("value".to_string(), other);
            map
        }
    };
    Ok(RecordState {
        fields,
        relation: None,
    })
}

/// The state of a [`TableTarget::declare_fields`] declaration: `fields` must be
/// a struct or map, since each entry names a field of the record.
fn fields_state<R: Serialize>(fields: &R) -> Result<RecordState> {
    crate::finite::ensure_finite(fields)
        .map_err(|e| Error::engine(format!("SurrealDB target fields have a {e}")))?;
    let value = serde_json::to_value(fields)
        .map_err(|e| Error::engine(format!("serialize SurrealDB target fields: {e}")))?;
    let JsonValue::Object(fields) = value else {
        return Err(Error::engine(
            "declare_fields requires `fields` to be a struct or a map of field name to value",
        ));
    };
    for name in fields.keys() {
        validate_ident(name, "field name")?;
    }
    Ok(RecordState {
        fields,
        relation: None,
    })
}

fn quote_record_id(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('`', "\\`");
    format!("`{escaped}`")
}

fn stable_key_to_record_id(key: &StableKey) -> Result<RecordIdValue> {
    match key {
        StableKey::Int(i) => Ok(RecordIdValue::Number(i.to_string())),
        StableKey::Str(s) | StableKey::Symbol(s) => Ok(RecordIdValue::String(s.to_string())),
        StableKey::Uuid(u) => Ok(RecordIdValue::String(u.to_string())),
        other => Err(Error::engine(format!(
            "unsupported SurrealDB record key: {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relation_key_parts_are_unambiguous() {
        let first = format!("{}{}", relation_key_part("ab"), relation_key_part("c"));
        let second = format!("{}{}", relation_key_part("a"), relation_key_part("bc"));
        assert_ne!(first, second);
        assert_eq!(first, "2:ab;1:c;");
        assert_eq!(second, "1:a;2:bc;");
    }

    fn string_id(id: &str) -> RecordIdValue {
        RecordIdValue::String(id.to_string())
    }

    fn fields(value: JsonValue) -> Map<String, JsonValue> {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn a_batch_upserts_records_before_it_sets_their_fields() {
        let spec = TableSpec::table("person".to_string(), None, ManagedBy::System);
        let actions = vec![
            RecordAction {
                record_id: string_id("gone"),
                write: RecordWrite::Delete,
            },
            RecordAction {
                record_id: string_id("old"),
                write: RecordWrite::UnsetFields(vec!["score".to_string()]),
            },
            RecordAction {
                record_id: string_id("ann"),
                write: RecordWrite::SetFields(MergeContent {
                    set: fields(serde_json::json!({ "score": 7 })),
                    unset: vec!["rank".to_string()],
                }),
            },
            RecordAction {
                record_id: string_id("ann"),
                write: RecordWrite::Upsert(MergeContent {
                    set: fields(serde_json::json!({ "id": "ann", "name": "Ann", "nick": null })),
                    unset: vec![],
                }),
            },
        ];
        assert_eq!(
            record_statements(&spec, actions).unwrap(),
            "BEGIN TRANSACTION;\n\
             UPSERT person:`ann` MERGE {\"name\": \"Ann\", \"nick\": NONE} RETURN NONE;\n\
             UPDATE person:`old` MERGE {\"score\": NONE} RETURN NONE;\n\
             LET $updated = (UPDATE ONLY person:`ann` MERGE {\"rank\": NONE, \"score\": 7} RETURN id);\n\
             IF $updated = NONE { THROW \"fields target record missing: person:`ann`\"; };\n\
             DELETE person:`gone`;\n\
             COMMIT TRANSACTION;\n"
        );
    }

    #[test]
    fn a_removed_group_with_no_recorded_fields_writes_nothing() {
        let spec = TableSpec::table("person".to_string(), None, ManagedBy::System);
        let actions = vec![RecordAction {
            record_id: string_id("ann"),
            write: RecordWrite::UnsetFields(vec![]),
        }];
        assert_eq!(
            record_statements(&spec, actions).unwrap(),
            "BEGIN TRANSACTION;\nCOMMIT TRANSACTION;\n"
        );
    }

    #[test]
    fn typed_columns_are_written_as_typed_values() {
        let schema = TableSchema::new([
            ("at", ColumnDef::new("datetime")),
            ("day", ColumnDef::new("datetime")),
            ("naive", ColumnDef::new("datetime")),
            ("uid", ColumnDef::new("uuid")),
            ("price", ColumnDef::new("decimal")),
            ("took", ColumnDef::new("duration")),
            ("blob", ColumnDef::new("bytes")),
            ("tags", ColumnDef::new("array<string>")),
        ])
        .unwrap();
        let set = fields(serde_json::json!({
            "at": "2024-05-01T10:00:00+02:00",
            "day": "2024-05-01",
            "naive": "2024-05-01T10:00:00",
            "uid": "67e55044-10b1-426f-9247-bb680e5fe0c8",
            "price": "12.50",
            "took": { "secs": 5, "nanos": 250 },
            "blob": [104, 105],
            "tags": ["a", "b"],
            "untyped": "2024-05-01",
        }));
        assert_eq!(
            merge_object(Some(&schema), &set, &[]).unwrap(),
            "{\"at\": d\"2024-05-01T10:00:00+02:00\", \
             \"blob\": encoding::base64::decode(\"aGk=\"), \
             \"day\": d\"2024-05-01T00:00:00Z\", \
             \"naive\": d\"2024-05-01T10:00:00Z\", \
             \"price\": <decimal> \"12.50\", \
             \"tags\": [\"a\",\"b\"], \
             \"took\": 5s250ns, \
             \"uid\": u\"67e55044-10b1-426f-9247-bb680e5fe0c8\", \
             \"untyped\": \"2024-05-01\"}"
        );
    }

    #[test]
    fn base64_pads_every_remainder() {
        assert_eq!(base64_text(b""), "");
        assert_eq!(base64_text(b"f"), "Zg==");
        assert_eq!(base64_text(b"fo"), "Zm8=");
        assert_eq!(base64_text(b"foo"), "Zm9v");
        assert_eq!(base64_text(&[0xfb, 0xff]), "+/8=");
    }

    #[test]
    fn tracking_records_read_back_from_messagepack() {
        let fingerprint = Fingerprint::from(&"state").unwrap();
        let records = [
            RecordTracking::Fingerprint(fingerprint),
            RecordTracking::Fields(FieldsTracking {
                fingerprint,
                names: vec!["name".to_string(), "score".to_string()],
            }),
        ];
        for record in records {
            for encoded in [
                rmp_serde::to_vec(&record).unwrap(),
                rmp_serde::to_vec_named(&record).unwrap(),
            ] {
                assert!(rmp_serde::from_slice::<RecordTracking>(&encoded).unwrap() == record);
            }
        }
    }

    #[test]
    fn a_field_group_key_carries_the_record_id_and_the_group() {
        let key = RecordKey {
            record_id: RecordIdValue::Number("7".to_string()),
            group: Some("scores".to_string()),
        }
        .stable_key();
        let parsed = RecordKey::from_stable_key(&key).unwrap();
        assert_eq!(parsed.record_id, RecordIdValue::Number("7".to_string()));
        assert_eq!(parsed.group.as_deref(), Some("scores"));

        let plain = RecordKey::from_stable_key(&string_id("ann").stable_key()).unwrap();
        assert_eq!(plain.record_id, string_id("ann"));
        assert_eq!(plain.group, None);
    }
}
