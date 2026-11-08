//! SurrealDB target connector.
//!
//! Table and relation targets reconcile declared records against the previous
//! run: changed records are upserted, unchanged records are skipped, and records
//! no longer declared are deleted. `managed_by` controls whether CocoIndex owns
//! schema DDL.
//!
//! Ownership follows the Python connector: [`TableTarget::declare_record`]
//! writes with `UPSERT … SET`, one assignment per field, never `CONTENT`, so
//! fields written by other owners survive and fields a record stops declaring
//! are unset. Each declared field's value is replaced whole, nested objects
//! included. Field names must be identifiers (ASCII letters, digits and `_`).
//! [`TableTarget::declare_fields`] lets a component contribute only some fields
//! of a record that another component owns. Relation records are single-owner
//! and are replaced whole.
//!
//! A [`TableSchema`] makes the table `SCHEMAFULL`. Derive it from a row struct
//! with [`TableSchema::from_row`]. Values are sent as typed parameters, never
//! inlined: top-level values of `datetime`, `uuid`, `decimal`, `duration` and
//! `bytes` columns arrive in their SurrealDB type, and a top-level `null` is
//! sent as `NONE`.
//!
//! A batch of record actions is applied in chunks of 500, each chunk one
//! transaction and one round trip, with one statement per shape of write rather
//! than one per record. A chunk that hits a transaction conflict is retried with
//! exponential backoff, five attempts in all, as the Python connector does.
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
use surrealdb::opt::auth;
use surrealdb::types::{
    Bytes, Datetime, Decimal, Duration, Number, Object, QueryError, RecordId, RecordIdKey,
    SurrealValue, Uuid, Value, Variables,
};

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

/// The type of a vector column of dimension `dim`.
fn vector_type(dim: usize) -> String {
    format!("array<float, {dim}>")
}

/// The type [`TableSchema::from_row`] gives a `#[coco(vector)]` field whose
/// dimension is not known yet: [`TableSchema::with_vector_schema`] resolves
/// it, and a table declared with it still unresolved is rejected.
const UNRESOLVED_VECTOR_TYPE: &str = "array<float, 0>";

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
        // Resolved by `with_vector_schema`, or rejected when the table is declared.
        LogicalType::Vector { dim: 0, .. } => return Ok(UNRESOLVED_VECTOR_TYPE.to_string()),
        LogicalType::Vector { dim, .. } => return Ok(vector_type(*dim as usize)),
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

    /// Replace the SurrealDB type of one column of a schema derived from a row
    /// (Python's `column_overrides={"col": SurrealType(…)}`), keeping its
    /// nullability.
    pub fn with_column_type(
        mut self,
        field_name: &str,
        surreal_type: impl Into<String>,
    ) -> Result<Self> {
        self.column_mut(field_name)?.surreal_type = surreal_type.into().trim().to_string();
        Ok(self)
    }

    /// Type a vector column of a schema derived from a row from a
    /// [`VectorSchemaProvider`](crate::resources::schema::VectorSchemaProvider),
    /// typically the embedder that fills it (Python's
    /// `column_overrides={"embedding": embedder}`): the column becomes
    /// `array<float, N>` for the provider's dimension `N`. For an embedder held
    /// in the context, pass `ctx.get_key(&EMBEDDER)?`.
    pub async fn with_vector_schema(
        mut self,
        field_name: &str,
        provider: &(impl crate::resources::schema::VectorSchemaProvider + ?Sized),
    ) -> Result<Self> {
        let schema =
            crate::row_schema::provided_vector_schema("SurrealDB", field_name, provider).await?;
        let def = self.column_mut(field_name)?;
        if !def.surreal_type.starts_with("array<float") {
            return Err(crate::row_schema::not_vector_field_error(
                "SurrealDB",
                field_name,
            ));
        }
        def.surreal_type = vector_type(schema.size);
        Ok(self)
    }

    /// Reject a vector column whose dimension `from_row` could not resolve.
    fn validate_vector_dimensions(&self) -> Result<()> {
        for (name, def) in &self.columns {
            if def.surreal_type == UNRESOLVED_VECTOR_TYPE {
                return Err(crate::row_schema::vector_dimension_error(
                    "SurrealDB",
                    name,
                    "has an unresolved dimension; use #[coco(vector = N)] or \
                     with_vector_schema(…) before declaring the target",
                ));
            }
        }
        Ok(())
    }

    fn column_mut(&mut self, field_name: &str) -> Result<&mut ColumnDef> {
        self.columns
            .get_mut(field_name)
            .ok_or_else(|| crate::row_schema::unknown_column_error("SurrealDB", field_name))
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

/// How [`Graph::connect_with_credentials`] signs in (Python's `credentials`
/// dict of `ConnectionFactory`). A namespace or database user belongs to the
/// namespace and database the connection uses.
#[derive(Clone, Debug)]
pub enum Credentials {
    /// Sign in as a root user.
    Root { username: String, password: String },
    /// Sign in as a user of the connection's namespace.
    Namespace { username: String, password: String },
    /// Sign in as a user of the connection's database.
    Database { username: String, password: String },
    /// Sign in as a record user through the access method `access` of the
    /// connection's database, with the parameters that method expects.
    Record { access: String, params: JsonValue },
    /// No sign-in, for a server started without authentication.
    None,
}

impl Graph {
    /// Connect over WebSocket and sign in as the root user `user`.
    pub async fn connect(
        url: &str,
        ns: &str,
        db_name: &str,
        user: &str,
        pass: &str,
    ) -> Result<Self> {
        Self::connect_with_credentials(
            url,
            ns,
            db_name,
            Credentials::Root {
                username: user.to_string(),
                password: pass.to_string(),
            },
        )
        .await
    }

    /// Connect over WebSocket and sign in with `credentials`.
    pub async fn connect_with_credentials(
        url: &str,
        ns: &str,
        db_name: &str,
        credentials: Credentials,
    ) -> Result<Self> {
        let db = Surreal::new::<Ws>(url).await.map_err(surreal_err)?;
        match credentials {
            Credentials::Root { username, password } => {
                db.signin(auth::Root { username, password }).await.map(drop)
            }
            Credentials::Namespace { username, password } => db
                .signin(auth::Namespace {
                    namespace: ns.to_string(),
                    username,
                    password,
                })
                .await
                .map(drop),
            Credentials::Database { username, password } => db
                .signin(auth::Database {
                    namespace: ns.to_string(),
                    database: db_name.to_string(),
                    username,
                    password,
                })
                .await
                .map(drop),
            Credentials::Record { access, params } => db
                .signin(auth::Record {
                    namespace: ns.to_string(),
                    database: db_name.to_string(),
                    access,
                    params,
                })
                .await
                .map(drop),
            Credentials::None => Ok(()),
        }
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

    /// The underlying client, for queries of your own (Python's
    /// `ConnectionFactory.acquire()`).
    pub fn client(&self) -> &Surreal<Client> {
        &self.db
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

/// A SurrealDB table target — a handle to declare records on.
///
/// As a `mount_each!` / `use_mount!` or memoized function argument it
/// fingerprints by its stable key, the graph key and table name; the runtime
/// provider is not part of the identity.
#[derive(Clone, Serialize)]
pub struct TableTarget {
    graph_key: Arc<str>,
    table_name: Arc<str>,
    /// The table's record provider; also carries its vector-index attachment.
    #[serde(skip)]
    records: TargetStateProvider<RecordState>,
}

impl TableTarget {
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Declare a record to be upserted to this table. It is written with
    /// `UPSERT … SET`, one assignment per field: each field's value is replaced
    /// whole, fields written by other owners (see
    /// [`declare_fields`](Self::declare_fields)) survive, and fields the row
    /// stops declaring are unset. Field names must be identifiers.
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
    /// It writes `UPDATE … SET` with only these fields, so fields written by
    /// other owners (or by [`declare_record`](Self::declare_record)) are never
    /// touched. `fields` cannot name `id`. When the declaration goes away, or a field is dropped from it,
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
    /// (`DEFINE INDEX … <METHOD> DIMENSION … DIST … TYPE …`), on a
    /// user-managed table too (as in Python): declaring it hands it to
    /// CocoIndex.
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
            managed_by: ManagedBy::System,
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
    /// Index method: `"hnsw"` (SurrealDB 3 removed `MTREE`).
    pub method: &'static str,
    /// Vector element type: `"f32"`, `"f64"`, `"i16"`, `"i32"`, or `"i64"`.
    pub vector_type: &'static str,
}

impl Default for VectorIndexOptions {
    fn default() -> Self {
        Self {
            name: None,
            metric: "cosine",
            method: "hnsw",
            vector_type: "f32",
        }
    }
}

/// A SurrealDB relation target — a handle to declare relations on.
///
/// As a `mount_each!` / `use_mount!` or memoized function argument it
/// fingerprints by its stable key, the graph key and table name; the endpoints
/// and runtime provider are not part of the identity.
#[derive(Clone, Serialize)]
pub struct RelationTarget {
    graph_key: Arc<str>,
    table_name: Arc<str>,
    #[serde(skip)]
    from_tables: Arc<[String]>,
    #[serde(skip)]
    to_tables: Arc<[String]>,
    #[serde(skip)]
    from_table: Option<Arc<str>>,
    #[serde(skip)]
    to_table: Option<Arc<str>>,
    #[serde(skip)]
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

fn table_handle(
    graph: &ContextKey<Graph>,
    spec: TableSpec,
    records: TargetStateProvider<RecordState>,
) -> TableTarget {
    TableTarget {
        graph_key: Arc::from(graph.name()),
        table_name: Arc::from(spec.table_name),
        records,
    }
}

fn relation_handle(
    graph: &ContextKey<Graph>,
    spec: TableSpec,
    records: TargetStateProvider<RecordState>,
) -> RelationTarget {
    let from_names = spec.from_tables;
    let to_names = spec.to_tables;
    let from_table = (from_names.len() == 1).then(|| Arc::from(from_names[0].clone()));
    let to_table = (to_names.len() == 1).then(|| Arc::from(to_names[0].clone()));
    RelationTarget {
        graph_key: Arc::from(graph.name()),
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
    if let Some(schema) = &spec.table_schema {
        schema.validate_vector_dimensions()?;
    }
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
    Ok(table_handle(graph, spec, records))
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
    Ok(relation_handle(graph, spec, records))
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
    Ok(relation_handle(graph, spec, records))
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
    Ok(table_handle(graph, spec, records))
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
    Ok(relation_handle(graph, spec, records))
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
    Ok(relation_handle(graph, spec, records))
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

    /// The typed record id of this value in `table`: a numeric id that fits an
    /// `i64` is a number key, any other id a string key.
    fn to_record_id(&self, table: &str) -> RecordId {
        let key = match self {
            Self::Number(value) => value
                .parse::<i64>()
                .map(RecordIdKey::Number)
                .unwrap_or_else(|_| RecordIdKey::String(value.clone())),
            Self::String(value) => RecordIdKey::String(value.clone()),
        };
        RecordId::new(table, key)
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
    /// Record mode: `UPSERT … SET`, creating the record when it is missing.
    Upsert(FieldChanges),
    /// Fields mode: `UPDATE … SET` on a record that must exist.
    SetFields(FieldChanges),
    /// Fields mode, group removed: unset the fields it owned. A missing record
    /// is fine.
    UnsetFields(Vec<String>),
    /// Relation records are single-owner: the edge is replaced whole, in place
    /// when its endpoints are unchanged, else by `DELETE` + `RELATE`.
    Relate {
        fields: Map<String, JsonValue>,
        endpoints: RelationEndpoints,
    },
    Delete,
}

/// The assignments of a `SET`: fields to set, each replaced whole, and fields
/// owned before but no longer declared, which become `NONE`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct FieldChanges {
    set: Map<String, JsonValue>,
    unset: Vec<String>,
}

impl RecordWrite {
    /// Apply order within one batch: field unsets, upserts, field sets,
    /// relation upserts, deletes. A removed group then cannot erase a field
    /// that the record, or another group, takes over in the same batch, and a
    /// field set finds the record an upsert of the same batch creates.
    fn rank(&self) -> u8 {
        match self {
            Self::UnsetFields(_) => 0,
            Self::Upsert(_) => 1,
            Self::SetFields(_) => 2,
            Self::Relate { .. } => 3,
            Self::Delete => 4,
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
    /// A record or field group written with `SET`.
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
                        define_table(&graph, &spec, &a.drop_fields, &a.retype_fields).await?;
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

/// One write to a table: a record write, or a change to one of its vector
/// indexes. Records and indexes share one sink, so that one apply sees both and
/// can order them: an index whose definition changes is removed before the
/// records are written, and defined after them. The engine orders no two
/// sinks, and a changed index cannot be defined over the old records (a new
/// dimension), nor the new records written under the old index.
#[derive(Clone, Debug, Serialize, Deserialize)]
enum TableWrite {
    Record(RecordAction),
    Index(VectorIndexAction),
}

/// The sink of a table's record and vector-index writes (see [`TableWrite`]).
fn table_write_sink(graph_key: String, spec: TableSpec) -> TargetActionSink<TableWrite> {
    TargetActionSink::from_async_fn_with_ctx(
        move |host_ctx, actions: Vec<TargetAction<TableWrite>>| {
            let graph_key = graph_key.clone();
            let spec = spec.clone();
            async move {
                let graph = resolve_graph(&host_ctx, &graph_key)?;
                let mut records = Vec::new();
                let mut indexes = Vec::new();
                for action in actions {
                    match action {
                        TargetAction::Create(write)
                        | TargetAction::Update(write)
                        | TargetAction::Delete(write) => match write {
                            TableWrite::Record(record) => records.push(record),
                            TableWrite::Index(index) => indexes.push(index),
                        },
                    }
                }
                for index in &indexes {
                    remove_vector_index(&graph, index).await?;
                }
                apply_records(&graph, &spec, records).await?;
                for index in &indexes {
                    define_vector_index(&graph, index).await?;
                }
                Ok(())
            }
        },
    )
}

struct RecordHandler {
    /// Shared with the vector-index attachment handler (see [`TableWrite`]).
    sink: SinkCache<TableWrite>,
    graph_key: String,
    spec: TableSpec,
}

impl RecordHandler {
    fn write_sink(&self) -> TargetActionSink<TableWrite> {
        self.sink
            .get(|| table_write_sink(self.graph_key.clone(), self.spec.clone()))
    }
}

impl TargetHandler<RecordState> for RecordHandler {
    type TrackingRecord = RecordTracking;
    type Action = TableWrite;

    fn attachments(&self) -> Result<Vec<(String, ChildTargetDef)>> {
        Ok(vec![(
            "vector_index".to_string(),
            ChildTargetDef::new::<VectorIndexSpec, _>(VectorIndexHandler {
                sink: self.write_sink(),
            }),
        )])
    }

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<RecordState>,
        prev: Vec<RecordTracking>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<TableWrite, RecordTracking>>> {
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
                let RecordState {
                    mut fields,
                    relation,
                } = state;
                // The record id is carried by the statement's record reference
                // (matches Python, which excludes `id` from the content).
                fields.remove("id");
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
                        let content = FieldChanges { set: fields, unset };
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
            action: TargetAction::Update(TableWrite::Record(RecordAction { record_id, write })),
            sink: self.write_sink(),
            tracking_record: tracking,
            child_invalidation: None,
        }))
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
    /// Always `System`: a declared index is CocoIndex's, whoever manages the
    /// table. Kept so that tracking records written with it read back.
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
    /// The table's record sink (see [`TableWrite`]).
    sink: TargetActionSink<TableWrite>,
}

impl TargetHandler<VectorIndexSpec> for VectorIndexHandler {
    type TrackingRecord = VectorIndexSpec;
    type Action = TableWrite;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<VectorIndexSpec>,
        prev: Vec<VectorIndexSpec>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<TableWrite, VectorIndexSpec>>> {
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
            action: TargetAction::Update(TableWrite::Index(VectorIndexAction {
                name,
                table_name,
                spec: desired.clone(),
            })),
            sink: self.sink.clone(),
            tracking_record: desired,
            child_invalidation: None,
        }))
    }
}

/// The first half of applying a [`VectorIndexAction`], before the records of
/// its batch are written: remove the index.
async fn remove_vector_index(graph: &Graph, action: &VectorIndexAction) -> Result<()> {
    validate_ident(&action.name, "vector index name")?;
    validate_ident(&action.table_name, "table name")?;
    run_query(
        graph,
        &format!(
            "REMOVE INDEX IF EXISTS {} ON TABLE {}",
            action.name, action.table_name
        ),
    )
    .await
}

/// The second half, after the records of its batch are written: define the
/// index, unless it is removed.
async fn define_vector_index(graph: &Graph, action: &VectorIndexAction) -> Result<()> {
    let Some(spec) = &action.spec else {
        return Ok(());
    };
    validate_ident(&action.name, "vector index name")?;
    validate_ident(&action.table_name, "table name")?;
    validate_ident(&spec.field, "vector index field")?;
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

/// Define a system-managed table and its fields, in one round trip: `DEFINE
/// TABLE IF NOT EXISTS`, one `DEFINE FIELD IF NOT EXISTS` per schema column,
/// then `REMOVE FIELD` for each of `drop_fields` and `DEFINE FIELD OVERWRITE`
/// for each of `retype_fields` (the `id` field is never altered). Nothing is
/// sent for a user-managed table.
async fn define_table(
    graph: &Graph,
    spec: &TableSpec,
    drop_fields: &[String],
    retype_fields: &[String],
) -> Result<()> {
    if spec.managed_by.is_user() {
        return Ok(());
    }
    run_query(
        graph,
        &define_table_statements(spec, drop_fields, retype_fields)?,
    )
    .await
}

/// The statements [`define_table`] sends.
fn define_table_statements(
    spec: &TableSpec,
    drop_fields: &[String],
    retype_fields: &[String],
) -> Result<String> {
    validate_ident(&spec.table_name, "table name")?;
    let table = &spec.table_name;
    let schema_mode = if spec.table_schema.is_some() {
        "SCHEMAFULL"
    } else {
        "SCHEMALESS"
    };
    let mut statements = if spec.is_relation {
        if spec.from_tables.is_empty() || spec.to_tables.is_empty() {
            format!("DEFINE TABLE IF NOT EXISTS {table} TYPE RELATION {schema_mode};\n")
        } else {
            format!(
                "DEFINE TABLE IF NOT EXISTS {table} TYPE RELATION FROM {} TO {} {schema_mode};\n",
                spec.from_tables.join("|"),
                spec.to_tables.join("|"),
            )
        }
    } else {
        format!("DEFINE TABLE IF NOT EXISTS {table} {schema_mode};\n")
    };
    if let Some(schema) = &spec.table_schema {
        for (name, column) in schema.columns() {
            if name == "id" {
                continue;
            }
            let type_expr = column.type_expr();
            statements.push_str(&format!(
                "DEFINE FIELD IF NOT EXISTS {name} ON {table} TYPE {type_expr};\n"
            ));
        }
        for name in drop_fields {
            validate_ident(name, "field name")?;
            statements.push_str(&format!("REMOVE FIELD IF EXISTS {name} ON {table};\n"));
        }
        for name in retype_fields {
            if let Some(col) = schema.columns().get(name) {
                let type_expr = col.type_expr();
                statements.push_str(&format!(
                    "DEFINE FIELD OVERWRITE {name} ON {table} TYPE {type_expr};\n"
                ));
            }
        }
    }
    Ok(statements)
}

async fn remove_table(graph: &Graph, table_name: &str) -> Result<()> {
    validate_ident(table_name, "table name")?;
    run_query(graph, &format!("REMOVE TABLE IF EXISTS {table_name}")).await
}

/// The most record actions one transaction (one round trip) carries.
const MAX_ACTIONS_PER_TXN: usize = 500;
/// How many times a chunk is sent when it keeps hitting a transaction conflict.
const MAX_ATTEMPTS: u32 = 5;
/// The backoff before the second attempt; it doubles each attempt, with jitter.
const RETRY_INITIAL_BACKOFF: std::time::Duration = std::time::Duration::from_millis(50);

async fn apply_records(graph: &Graph, spec: &TableSpec, actions: Vec<RecordAction>) -> Result<()> {
    for chunk in record_batches(spec, actions)? {
        apply_chunk(graph, chunk).await?;
    }
    Ok(())
}

/// Send one chunk, retried when its transaction conflicts.
async fn apply_chunk(graph: &Graph, chunk: RecordChunk) -> Result<()> {
    retry_on_conflict(|| async {
        let mut response = graph
            .db
            .query(chunk.surql.as_str())
            .bind(chunk.params.clone())
            .await?;
        first_real_error(response.take_errors()).map_or(Ok(()), Err)
    })
    .await
    .map_err(surreal_err)
}

/// Run `send` until it succeeds or fails with anything but a transaction
/// conflict (another writer touched the same records, and SurrealDB says the
/// operation can be retried), at most [`MAX_ATTEMPTS`] times, with exponential
/// backoff and jitter, as Python's `_apply_chunk`.
async fn retry_on_conflict<F, Fut>(mut send: F) -> std::result::Result<(), surrealdb::Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<(), surrealdb::Error>>,
{
    let mut backoff = RETRY_INITIAL_BACKOFF;
    let mut attempt = 1;
    loop {
        match send().await {
            Err(error) if attempt < MAX_ATTEMPTS && is_transaction_conflict(&error) => {
                tokio::time::sleep(backoff.mul_f64(1.0 + jitter())).await;
                backoff *= 2;
                attempt += 1;
            }
            result => return result,
        }
    }
}

fn is_transaction_conflict(error: &surrealdb::Error) -> bool {
    if matches!(error.query_details(), Some(QueryError::TransactionConflict)) {
        return true;
    }
    let message = error.to_string().to_lowercase();
    message.contains("can be retried") || message.contains("conflict")
}

/// A number in `[0, 1)` from the clock, enough to spread retries of
/// concurrent writers without a random-number dependency.
fn jitter() -> f64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    f64::from(nanos % 1_000) / 1_000.0
}

/// The error to report for a failed batch. When one statement of a transaction
/// fails, every other statement reports a "not executed" or "cancelled" echo;
/// the lowest-index error is then an echo, so skip those (as Python's
/// `_check_response` does).
fn first_real_error(
    errors: std::collections::HashMap<usize, surrealdb::Error>,
) -> Option<surrealdb::Error> {
    let mut errors: Vec<(usize, surrealdb::Error)> = errors.into_iter().collect();
    if errors.is_empty() {
        return None;
    }
    errors.sort_by_key(|(index, _)| *index);
    // Only echoes: report the first one rather than hide the failure.
    let real = errors
        .iter()
        .position(|(_, error)| !is_echo(error))
        .unwrap_or(0);
    Some(errors.swap_remove(real).1)
}

fn is_echo(error: &surrealdb::Error) -> bool {
    matches!(
        error.query_details(),
        Some(QueryError::NotExecuted | QueryError::Cancelled)
    )
}

/// One transaction of record writes: a script with one `FOR` loop per shape
/// of write, and the records each loop runs over, bound as typed parameters.
#[derive(Clone, Debug)]
struct RecordChunk {
    surql: String,
    params: Variables,
}

impl RecordChunk {
    /// The number of statements in the transaction, between `BEGIN` and
    /// `COMMIT`: one per shape of write, not one per record.
    #[cfg(test)]
    fn statement_count(&self) -> usize {
        self.surql.matches("FOR $r IN ").count()
    }
}

/// Group the actions of one batch into transactions of at most
/// [`MAX_ACTIONS_PER_TXN`], ordered by [`RecordWrite::rank`]. Within a chunk,
/// the writes of one shape (the same kind and the same field names) run in one
/// `FOR` loop over one bound parameter, so the script's size is bounded by the
/// number of shapes, and every value travels as a typed parameter.
fn record_batches(spec: &TableSpec, mut actions: Vec<RecordAction>) -> Result<Vec<RecordChunk>> {
    validate_ident(&spec.table_name, "table name")?;
    actions.sort_by_key(|action| action.write.rank()); // stable
    let mut chunks = Vec::with_capacity(actions.len().div_ceil(MAX_ACTIONS_PER_TXN));
    let mut actions = actions.into_iter().peekable();
    while actions.peek().is_some() {
        let chunk: Vec<RecordAction> = actions.by_ref().take(MAX_ACTIONS_PER_TXN).collect();
        chunks.push(record_chunk(spec, chunk)?);
    }
    Ok(chunks)
}

/// The script and parameters of one transaction (see [`record_batches`]).
fn record_chunk(spec: &TableSpec, actions: Vec<RecordAction>) -> Result<RecordChunk> {
    let schema = spec.table_schema.as_ref();
    // Loops in order of first appearance, which [`record_batches`] made the
    // rank order; each loop's records are bound as `$g<index>`.
    let mut loops: Vec<(String, Vec<Value>)> = Vec::new();
    let mut loop_index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for RecordAction { record_id, write } in actions {
        let id = Value::RecordId(record_id.to_record_id(&spec.table_name));
        let (body, record) = match write {
            RecordWrite::Upsert(changes) => {
                let set = set_clause(&changes.set, &changes.unset)?;
                (
                    format!("UPSERT $r.id{set} RETURN NONE;"),
                    record_params(id, fields_params(schema, &changes.set)?),
                )
            }
            RecordWrite::SetFields(changes) => {
                let set = set_clause(&changes.set, &changes.unset)?;
                (
                    format!(
                        "LET $n = (UPDATE ONLY $r.id{set} RETURN id); \
                         IF $n = NONE {{ THROW 'fields target record missing: ' + <string>$r.id; }};"
                    ),
                    record_params(id, fields_params(schema, &changes.set)?),
                )
            }
            RecordWrite::UnsetFields(names) => {
                // Nothing to unset when the group owned no fields.
                if names.is_empty() {
                    continue;
                }
                let set = set_clause(&Map::new(), &names)?;
                (
                    format!("UPDATE $r.id{set} RETURN NONE;"),
                    record_params(id, Object::new()),
                )
            }
            RecordWrite::Relate { fields, endpoints } => {
                validate_ident(&endpoints.from_table, "relation from table name")?;
                validate_ident(&endpoints.to_table, "relation to table name")?;
                let from = Value::RecordId(endpoints.from_id.to_record_id(&endpoints.from_table));
                let to = Value::RecordId(endpoints.to_id.to_record_id(&endpoints.to_table));
                // The content carries the endpoints, so that `UPDATE … CONTENT`
                // keeps them.
                let mut content = fields_params(schema, &fields)?;
                content.insert("in", from.clone());
                content.insert("out", to.clone());
                let mut record = record_params(id, content);
                record.insert("f", from);
                record.insert("t", to);
                // An edge that exists between the same endpoints is replaced
                // in place. Otherwise any old edge is deleted and the new one
                // related: SurrealDB 3.2 cannot create a record deleted earlier
                // in the same transaction, so only an edge whose endpoints
                // change takes `DELETE` + `RELATE`.
                (
                    "IF (SELECT VALUE [in, out] FROM ONLY $r.id) = [$r.f, $r.t] { \
                     UPDATE $r.id CONTENT $r.c RETURN NONE; \
                     } ELSE { \
                     DELETE $r.id RETURN NONE; \
                     RELATE ($r.f)->($r.id)->($r.t) CONTENT $r.c RETURN NONE; \
                     };"
                    .to_string(),
                    record,
                )
            }
            RecordWrite::Delete => (
                "DELETE $r.id RETURN NONE;".to_string(),
                record_params(id, Object::new()),
            ),
        };
        let index = match loop_index.get(&body) {
            Some(index) => *index,
            None => {
                loops.push((body.clone(), Vec::new()));
                loop_index.insert(body, loops.len() - 1);
                loops.len() - 1
            }
        };
        loops[index].1.push(Value::Object(record));
    }

    let mut surql = String::from("BEGIN TRANSACTION;\n");
    let mut params = Variables::new();
    for (index, (body, records)) in loops.into_iter().enumerate() {
        surql.push_str(&format!("FOR $r IN $g{index} {{ {body} }};\n"));
        params.insert(format!("g{index}"), records);
    }
    surql.push_str("COMMIT TRANSACTION;\n");
    Ok(RecordChunk { surql, params })
}

/// One record of a loop's parameter: its id and its field values, `c`.
fn record_params(id: Value, content: Object) -> Object {
    let mut record = Object::new();
    record.insert("id", id);
    record.insert("c", Value::Object(content));
    record
}

/// A ` SET` clause with one assignment per field: `set` fields take their
/// value from the bound record (`$r.c`) and `unset` fields take `NONE`, which
/// removes them. An assignment replaces the field's value whole, nested
/// objects included (a `MERGE` would merge them, and so keep nested keys the
/// row stops declaring). Empty when there is nothing to assign. Field names
/// are validated identifiers.
fn set_clause(set: &Map<String, JsonValue>, unset: &[String]) -> Result<String> {
    let mut parts = Vec::with_capacity(set.len() + unset.len());
    for name in unset {
        validate_ident(name, "field name")?;
        parts.push(format!("`{name}` = NONE"));
    }
    for name in set.keys() {
        validate_ident(name, "field name")?;
        parts.push(format!("`{name}` = $r.c.`{name}`"));
    }
    if parts.is_empty() {
        return Ok(String::new());
    }
    Ok(format!(" SET {}", parts.join(", ")))
}

/// The field values of one record as a bound object, each in the type its
/// column calls for.
fn fields_params(schema: Option<&TableSchema>, fields: &Map<String, JsonValue>) -> Result<Object> {
    let mut content = Object::new();
    for (name, value) in fields {
        let column = schema.and_then(|schema| schema.columns.get(name));
        content.insert(name.clone(), surreal_value(name, column, value)?);
    }
    Ok(content)
}

/// A top-level field value as a SurrealDB value. `null` is `NONE` (what an
/// `option<T>` field takes). The value of a `datetime`, `uuid`, `decimal`,
/// `duration` or `bytes` column arrives in that type, as Python sends typed
/// CBOR parameters, and a value such a column cannot take is an error. Any
/// other value is sent as it is: arrays and objects become native arrays and
/// objects.
fn surreal_value(name: &str, column: Option<&ColumnDef>, value: &JsonValue) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::None);
    }
    let surreal_type = column.map(|c| c.surreal_type.trim());
    let typed = match (surreal_type, value) {
        (Some("datetime"), JsonValue::String(s)) => datetime_text(s)
            .parse::<Datetime>()
            .ok()
            .map(Value::Datetime),
        (Some("uuid"), JsonValue::String(s)) => s.parse::<Uuid>().ok().map(Value::Uuid),
        (Some("decimal"), JsonValue::String(s)) => s
            .parse::<Decimal>()
            .ok()
            .map(|d| Value::Number(Number::Decimal(d))),
        (Some("decimal"), JsonValue::Number(n)) => n
            .to_string()
            .parse::<Decimal>()
            .ok()
            .map(|d| Value::Number(Number::Decimal(d))),
        (Some("duration"), JsonValue::String(s)) => s.parse::<Duration>().ok().map(Value::Duration),
        (Some("duration"), JsonValue::Object(_)) => duration_value(value).map(Value::Duration),
        (Some("bytes"), JsonValue::Array(items)) => items
            .iter()
            .map(|item| item.as_u64().and_then(|byte| u8::try_from(byte).ok()))
            .collect::<Option<Vec<u8>>>()
            .map(|bytes| Value::Bytes(Bytes::from(bytes))),
        (Some("datetime" | "uuid" | "decimal" | "duration" | "bytes"), _) => None,
        _ => return Ok(value.clone().into_value()),
    };
    typed.ok_or_else(|| {
        Error::engine(format!(
            "SurrealDB field {name:?} has type {}, which cannot take the value {value}",
            surreal_type.unwrap_or_default()
        ))
    })
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

/// A duration from a serialized `std::time::Duration` (`{"secs": …, "nanos": …}`).
fn duration_value(value: &JsonValue) -> Option<Duration> {
    let object = value.as_object()?;
    if object.len() != 2 {
        return None;
    }
    let secs = object.get("secs")?.as_u64()?;
    let nanos = u32::try_from(object.get("nanos")?.as_u64()?).ok()?;
    Some(Duration::new(secs, nanos))
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
    // Field names are interpolated into the write statement.
    for name in fields.keys() {
        validate_ident(name, "field name")?;
    }
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
        if name == "id" {
            return Err(Error::engine(
                "declare_fields cannot set `id`: the record id is the `id` argument",
            ));
        }
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

    fn upsert(id: &str, set: JsonValue, unset: &[&str]) -> RecordAction {
        RecordAction {
            record_id: string_id(id),
            write: RecordWrite::Upsert(FieldChanges {
                set: fields(set),
                unset: unset.iter().map(|s| s.to_string()).collect(),
            }),
        }
    }

    fn delete(id: &str) -> RecordAction {
        RecordAction {
            record_id: string_id(id),
            write: RecordWrite::Delete,
        }
    }

    /// The records bound to loop `index` of a chunk.
    fn bound_records(chunk: &RecordChunk, index: usize) -> Vec<Object> {
        let Some(Value::Array(records)) = chunk.params.get(&format!("g{index}")) else {
            panic!("loop {index} has no bound records: {chunk:?}");
        };
        records
            .iter()
            .map(|record| match record {
                Value::Object(object) => object.clone(),
                other => panic!("a bound record is not an object: {other:?}"),
            })
            .collect()
    }

    fn one_chunk(spec: &TableSpec, actions: Vec<RecordAction>) -> RecordChunk {
        let mut chunks = record_batches(spec, actions).unwrap();
        assert_eq!(chunks.len(), 1);
        chunks.pop().unwrap()
    }

    #[test]
    fn a_batch_unsets_then_upserts_then_sets_fields() {
        let spec = TableSpec::table("person".to_string(), None, ManagedBy::System);
        let actions = vec![
            delete("gone"),
            RecordAction {
                record_id: string_id("ann"),
                write: RecordWrite::SetFields(FieldChanges {
                    set: fields(serde_json::json!({ "score": 7 })),
                    unset: vec!["rank".to_string()],
                }),
            },
            upsert(
                "ann",
                serde_json::json!({ "name": "Ann", "nick": null, "meta": { "en": 1 } }),
                &["age"],
            ),
            RecordAction {
                record_id: string_id("old"),
                write: RecordWrite::UnsetFields(vec!["score".to_string()]),
            },
        ];
        let chunk = one_chunk(&spec, actions);
        assert_eq!(
            chunk.surql,
            "BEGIN TRANSACTION;\n\
             FOR $r IN $g0 { UPDATE $r.id SET `score` = NONE RETURN NONE; };\n\
             FOR $r IN $g1 { UPSERT $r.id SET `age` = NONE, `meta` = $r.c.`meta`, `name` = $r.c.`name`, `nick` = $r.c.`nick` RETURN NONE; };\n\
             FOR $r IN $g2 { LET $n = (UPDATE ONLY $r.id SET `rank` = NONE, `score` = $r.c.`score` RETURN id); \
             IF $n = NONE { THROW 'fields target record missing: ' + <string>$r.id; }; };\n\
             FOR $r IN $g3 { DELETE $r.id RETURN NONE; };\n\
             COMMIT TRANSACTION;\n"
        );
        // Every value travels as a bound parameter; `null` is `NONE`.
        let [ann] = &bound_records(&chunk, 1)[..] else {
            panic!("expected one upsert");
        };
        assert_eq!(
            ann.get("id"),
            Some(&Value::RecordId(RecordId::new("person", "ann")))
        );
        let Some(Value::Object(content)) = ann.get("c") else {
            panic!("no content: {ann:?}");
        };
        assert_eq!(content.get("name"), Some(&Value::String("Ann".to_string())));
        assert_eq!(content.get("nick"), Some(&Value::None));
        assert_eq!(
            content.get("meta"),
            Some(&serde_json::json!({ "en": 1 }).into_value())
        );
        assert_eq!(content.get("age"), None);
        assert_eq!(
            bound_records(&chunk, 3)[0].get("id"),
            Some(&Value::RecordId(RecordId::new("person", "gone")))
        );
    }

    /// The record takes over `score` in the batch that removes the group which
    /// owned it: the unset must run first, or it would erase the new value.
    #[test]
    fn a_removed_group_does_not_erase_a_field_the_record_takes_over() {
        let spec = TableSpec::table("person".to_string(), None, ManagedBy::System);
        let actions = vec![
            upsert("ann", serde_json::json!({ "score": 9 }), &[]),
            RecordAction {
                record_id: string_id("ann"),
                write: RecordWrite::UnsetFields(vec!["score".to_string()]),
            },
        ];
        assert_eq!(
            one_chunk(&spec, actions).surql,
            "BEGIN TRANSACTION;\n\
             FOR $r IN $g0 { UPDATE $r.id SET `score` = NONE RETURN NONE; };\n\
             FOR $r IN $g1 { UPSERT $r.id SET `score` = $r.c.`score` RETURN NONE; };\n\
             COMMIT TRANSACTION;\n"
        );
    }

    #[test]
    fn a_write_with_no_fields_has_no_set_clause() {
        let spec = TableSpec::table("person".to_string(), None, ManagedBy::System);
        let no_changes = || FieldChanges {
            set: Map::new(),
            unset: vec![],
        };
        let actions = vec![
            RecordAction {
                record_id: string_id("ann"),
                write: RecordWrite::UnsetFields(vec![]),
            },
            RecordAction {
                record_id: string_id("ann"),
                write: RecordWrite::Upsert(no_changes()),
            },
            RecordAction {
                record_id: string_id("ann"),
                write: RecordWrite::SetFields(no_changes()),
            },
        ];
        assert_eq!(
            one_chunk(&spec, actions).surql,
            "BEGIN TRANSACTION;\n\
             FOR $r IN $g0 { UPSERT $r.id RETURN NONE; };\n\
             FOR $r IN $g1 { LET $n = (UPDATE ONLY $r.id RETURN id); \
             IF $n = NONE { THROW 'fields target record missing: ' + <string>$r.id; }; };\n\
             COMMIT TRANSACTION;\n"
        );
    }

    /// A batch of any size costs a bounded number of round trips: one
    /// transaction per `MAX_ACTIONS_PER_TXN` actions, and within it one
    /// statement per shape of write, not one per record.
    #[test]
    fn a_large_batch_is_sent_in_bounded_round_trips() {
        let spec = TableSpec::table("person".to_string(), None, ManagedBy::System);
        let mut actions = Vec::new();
        for i in 0..300 {
            actions.push(delete(&format!("gone{i}")));
        }
        for i in 0..1200 {
            actions.push(upsert(
                &format!("p{i}"),
                serde_json::json!({ "name": format!("P{i}"), "score": i }),
                &[],
            ));
        }
        let chunks = record_batches(&spec, actions).unwrap();
        assert_eq!(chunks.len(), 3);
        let mut upserts = 0;
        let mut deletes = 0;
        for chunk in &chunks {
            assert!(chunk.statement_count() <= 2, "{}", chunk.surql);
            let bound: usize = (0..chunk.statement_count())
                .map(|index| bound_records(chunk, index).len())
                .sum();
            assert!(bound <= MAX_ACTIONS_PER_TXN);
            upserts += chunk.surql.matches("UPSERT $r.id").count();
            deletes += chunk.surql.matches("DELETE $r.id").count();
        }
        // Upserts come before deletes, so the first two chunks are upserts
        // only and the deletes share the last chunk with the remaining ones.
        assert_eq!(chunks[0].statement_count(), 1);
        assert_eq!(chunks[1].statement_count(), 1);
        assert_eq!(chunks[2].statement_count(), 2);
        assert_eq!(upserts, 3);
        assert_eq!(deletes, 1);
        let total: usize = chunks
            .iter()
            .flat_map(|chunk| (0..chunk.statement_count()).map(move |i| (chunk, i)))
            .map(|(chunk, index)| bound_records(chunk, index).len())
            .sum();
        assert_eq!(total, 1500);
    }

    /// Rows of one table that declare different field sets each get a loop
    /// of their own: the field list is part of the statement.
    #[test]
    fn rows_with_different_fields_get_their_own_statement() {
        let spec = TableSpec::table("person".to_string(), None, ManagedBy::System);
        let actions = vec![
            upsert("a", serde_json::json!({ "name": "A" }), &[]),
            upsert("b", serde_json::json!({ "name": "B", "age": 3 }), &[]),
            upsert("c", serde_json::json!({ "name": "C" }), &[]),
        ];
        let chunk = one_chunk(&spec, actions);
        assert_eq!(chunk.statement_count(), 2);
        assert_eq!(bound_records(&chunk, 0).len(), 2);
        assert_eq!(bound_records(&chunk, 1).len(), 1);
    }

    /// One loop serves relations between any tables: the endpoint tables
    /// travel in the bound record ids.
    #[test]
    fn relations_bind_their_endpoints() {
        let spec = TableSpec::relation(
            "knows".to_string(),
            vec!["person".to_string(), "org".to_string()],
            vec!["person".to_string()],
            None,
            ManagedBy::System,
        );
        let relate = |id: &str, from_table: &str, from_id: &str| RecordAction {
            record_id: string_id(id),
            write: RecordWrite::Relate {
                fields: fields(serde_json::json!({ "since": 2020 })),
                endpoints: RelationEndpoints {
                    from_table: from_table.to_string(),
                    from_id: string_id(from_id),
                    to_table: "person".to_string(),
                    to_id: RecordIdValue::Number("7".to_string()),
                },
            },
        };
        let chunk = one_chunk(
            &spec,
            vec![relate("r1", "person", "ann"), relate("r2", "org", "acme")],
        );
        assert_eq!(
            chunk.surql,
            "BEGIN TRANSACTION;\n\
             FOR $r IN $g0 { \
             IF (SELECT VALUE [in, out] FROM ONLY $r.id) = [$r.f, $r.t] { \
             UPDATE $r.id CONTENT $r.c RETURN NONE; \
             } ELSE { \
             DELETE $r.id RETURN NONE; \
             RELATE ($r.f)->($r.id)->($r.t) CONTENT $r.c RETURN NONE; \
             }; };\n\
             COMMIT TRANSACTION;\n"
        );
        let [r1, r2] = &bound_records(&chunk, 0)[..] else {
            panic!("expected two relations");
        };
        // The content carries the endpoints, which `UPDATE … CONTENT` keeps.
        let Some(Value::Object(content)) = r1.get("c") else {
            panic!("expected the content object");
        };
        assert_eq!(
            content.get("since"),
            Some(&Value::Number(Number::Int(2020)))
        );
        assert_eq!(content.get("in"), r1.get("f"));
        assert_eq!(content.get("out"), r1.get("t"));
        assert_eq!(
            r1.get("id"),
            Some(&Value::RecordId(RecordId::new("knows", "r1")))
        );
        assert_eq!(
            r1.get("f"),
            Some(&Value::RecordId(RecordId::new("person", "ann")))
        );
        assert_eq!(
            r1.get("t"),
            Some(&Value::RecordId(RecordId::new("person", 7i64)))
        );
        assert_eq!(
            r2.get("f"),
            Some(&Value::RecordId(RecordId::new("org", "acme")))
        );
    }

    fn conflict() -> surrealdb::Error {
        surrealdb::Error::internal("The transaction can be retried".to_string())
    }

    /// `results` are what each attempt returns; the last repeats. Gives the
    /// number of attempts made and the outcome.
    async fn attempts(
        results: Vec<std::result::Result<(), surrealdb::Error>>,
    ) -> (usize, std::result::Result<(), surrealdb::Error>) {
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let outcome = retry_on_conflict(|| {
            let index = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let result = results[index.min(results.len() - 1)].clone();
            async move { result }
        })
        .await;
        (calls.into_inner(), outcome)
    }

    #[tokio::test]
    async fn a_conflicted_chunk_is_retried_until_it_lands() {
        let (calls, outcome) = attempts(vec![Err(conflict()), Err(conflict()), Ok(())]).await;
        assert_eq!(calls, 3);
        assert!(outcome.is_ok());
    }

    #[tokio::test]
    async fn a_chunk_that_keeps_conflicting_fails_after_the_last_attempt() {
        let (calls, outcome) = attempts(vec![Err(conflict())]).await;
        assert_eq!(calls, MAX_ATTEMPTS as usize);
        assert!(is_transaction_conflict(&outcome.unwrap_err()));
    }

    #[tokio::test]
    async fn any_other_error_is_not_retried() {
        let other = surrealdb::Error::internal("Expected `string` but found `3`".to_string());
        let (calls, outcome) = attempts(vec![Err(other)]).await;
        assert_eq!(calls, 1);
        assert!(!is_transaction_conflict(&outcome.unwrap_err()));
    }

    #[test]
    fn a_table_is_defined_in_one_script() {
        let schema = TableSchema::new([
            ("name", ColumnDef::new("string")),
            ("age", ColumnDef::new("int").nullable()),
            ("old", ColumnDef::new("string")),
        ])
        .unwrap();
        let spec = TableSpec::table("person".to_string(), Some(schema), ManagedBy::System);
        assert_eq!(
            define_table_statements(&spec, &["gone".to_string()], &["age".to_string()]).unwrap(),
            "DEFINE TABLE IF NOT EXISTS person SCHEMAFULL;\n\
             DEFINE FIELD IF NOT EXISTS age ON person TYPE option<int>;\n\
             DEFINE FIELD IF NOT EXISTS name ON person TYPE string;\n\
             DEFINE FIELD IF NOT EXISTS old ON person TYPE string;\n\
             REMOVE FIELD IF EXISTS gone ON person;\n\
             DEFINE FIELD OVERWRITE age ON person TYPE option<int>;\n"
        );
        let relation = TableSpec::relation(
            "knows".to_string(),
            vec!["person".to_string()],
            vec!["person".to_string(), "org".to_string()],
            None,
            ManagedBy::System,
        );
        assert_eq!(
            define_table_statements(&relation, &[], &[]).unwrap(),
            "DEFINE TABLE IF NOT EXISTS knows TYPE RELATION FROM person TO org|person SCHEMALESS;\n"
        );
    }

    #[test]
    fn field_names_must_be_identifiers() {
        let message = record_state(&serde_json::json!({ "a` = 1, `b": 2 }))
            .unwrap_err()
            .to_string();
        assert!(message.contains("field name"), "{message}");

        let message = fields_state(&serde_json::json!({ "id": "bob" }))
            .unwrap_err()
            .to_string();
        assert!(
            message.contains("declare_fields cannot set `id`"),
            "{message}"
        );
    }

    fn typed_schema() -> TableSchema {
        TableSchema::new([
            ("at", ColumnDef::new("datetime")),
            ("day", ColumnDef::new("datetime")),
            ("naive", ColumnDef::new("datetime")),
            ("uid", ColumnDef::new("uuid")),
            ("price", ColumnDef::new("decimal")),
            ("took", ColumnDef::new("duration")),
            ("blob", ColumnDef::new("bytes")),
            ("tags", ColumnDef::new("array<string>")),
        ])
        .unwrap()
    }

    #[test]
    fn typed_columns_are_sent_as_typed_values() {
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
        let content = fields_params(Some(&typed_schema()), &set).unwrap();
        assert_eq!(
            content.get("at"),
            Some(&Value::Datetime("2024-05-01T08:00:00Z".parse().unwrap()))
        );
        assert_eq!(
            content.get("day"),
            Some(&Value::Datetime("2024-05-01T00:00:00Z".parse().unwrap()))
        );
        assert_eq!(
            content.get("naive"),
            Some(&Value::Datetime("2024-05-01T10:00:00Z".parse().unwrap()))
        );
        assert_eq!(
            content.get("uid"),
            Some(&Value::Uuid(
                "67e55044-10b1-426f-9247-bb680e5fe0c8".parse().unwrap()
            ))
        );
        assert_eq!(
            content.get("price"),
            Some(&Value::Number(Number::Decimal("12.50".parse().unwrap())))
        );
        assert_eq!(
            content.get("took"),
            Some(&Value::Duration(Duration::new(5, 250)))
        );
        assert_eq!(
            content.get("blob"),
            Some(&Value::Bytes(Bytes::from(b"hi".to_vec())))
        );
        assert_eq!(
            content.get("tags"),
            Some(&serde_json::json!(["a", "b"]).into_value())
        );
        assert_eq!(
            content.get("untyped"),
            Some(&Value::String("2024-05-01".to_string()))
        );
    }

    #[test]
    fn a_typed_column_rejects_a_value_it_cannot_encode() {
        let schema = typed_schema();
        for (name, value) in [
            ("at", serde_json::json!(1714557600)),
            ("at", serde_json::json!("yesterday")),
            ("uid", serde_json::json!(7)),
            ("price", serde_json::json!(true)),
            ("took", serde_json::json!({ "secs": 5 })),
            ("blob", serde_json::json!([104, 300])),
            ("blob", serde_json::json!("aGk")),
        ] {
            let set = fields(serde_json::json!({ name: value }));
            let message = fields_params(Some(&schema), &set).unwrap_err().to_string();
            assert!(message.contains(&format!("{name:?}")), "{message}");
            assert!(message.contains("cannot take the value"), "{message}");
        }
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
