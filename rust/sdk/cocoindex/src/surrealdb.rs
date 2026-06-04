//! SurrealDB target connector.
//!
//! Table and relation targets reconcile declared records against the previous
//! run: changed records are upserted, unchanged records are skipped, and records
//! no longer declared are deleted. `managed_by` controls whether CocoIndex owns
//! schema DDL.
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

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::sql_ident::validate_ident;
use crate::statediff::{
    ManagedBy, ManagedTargetOptions, MutualTrackingRecord, resolve_system_transition,
};
use crate::target_state::{
    ChildTargetDef, StableKey, TargetAction, TargetActionSink, TargetChildInvalidation,
    TargetHandler, TargetReconcileOutput, TargetState, TargetStateProvider, declare_target_state,
    declare_target_state_with_child, mount_target, register_root_target_states_provider,
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
    table_provider: TargetStateProvider<TableSpec>,
    records: TargetStateProvider<RecordState>,
}

impl TableTarget {
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn declare_record<R: Serialize>(
        &self,
        ctx: &Ctx,
        id: impl IntoRecordId,
        row: &R,
    ) -> Result<()> {
        let value = record_state(row)?;
        declare_target_state(ctx, self.records.target_state(id.to_stable_key(), value))
    }

    /// Declare a record whose id is taken from the row's own scalar `id` field
    /// (the analogue of Python's `declare_row`). Use [`declare_record`] to pass
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
            self.table_provider.attachment(ctx, "vector_index")?;
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

fn table_handle(
    spec: TableSpec,
    table_provider: TargetStateProvider<TableSpec>,
    records: TargetStateProvider<RecordState>,
) -> TableTarget {
    TableTarget {
        table_name: Arc::from(spec.table_name),
        managed_by: spec.managed_by,
        table_provider,
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

fn table_target_state(ctx: &Ctx, graph: &Graph, spec: TableSpec) -> Result<TargetState<TableSpec>> {
    validate_ident(&spec.table_name, "table name")?;
    let provider = register_root_target_states_provider(
        ctx,
        format!(
            "cocoindex/surrealdb/table/{}/{}",
            graph.state_id(),
            spec.table_name
        ),
        TableHandler {
            graph: graph.clone(),
        },
    )?;
    Ok(provider.target_state("default", spec))
}

/// Build a composable [`TargetState`] for a schemaless, system-managed
/// SurrealDB table. Pass it to [`declare_table_target`]/[`mount_table_target`]
/// or the generic target-state helpers.
pub fn table_target(
    ctx: &Ctx,
    graph: &Graph,
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
    graph: &Graph,
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
    graph: &Graph,
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
    graph: &Graph,
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
    graph: &Graph,
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
    graph: &Graph,
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
    graph: &Graph,
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
    graph: &Graph,
    table_name: impl Into<String>,
    table_schema: Option<TableSchema>,
    options: ManagedTargetOptions,
) -> Result<TableTarget> {
    let ts = table_target_with_schema_and_options(ctx, graph, table_name, table_schema, options)?;
    let spec = ts.value().clone();
    let table_provider = ts.provider().clone();
    let records = declare_target_state_with_child::<TableSpec, RecordState>(ctx, ts)?;
    Ok(table_handle(spec, table_provider, records))
}

/// Declare a SurrealDB relation target in the **current** component and return
/// a pending handle. Use [`mount_relation_target`] when relation records must
/// be declared immediately.
pub fn declare_relation_target_many(
    ctx: &Ctx,
    graph: &Graph,
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
    graph: &Graph,
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
    graph: &Graph,
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
    graph: &Graph,
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
    graph: &Graph,
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
    graph: &Graph,
    table_name: impl Into<String>,
    options: ManagedTargetOptions,
) -> Result<TableTarget> {
    mount_table_target_with_schema_and_options(ctx, graph, table_name, None, options).await
}

pub async fn mount_table_target_with_schema(
    ctx: &Ctx,
    graph: &Graph,
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
    graph: &Graph,
    table_name: impl Into<String>,
    table_schema: Option<TableSchema>,
    options: ManagedTargetOptions,
) -> Result<TableTarget> {
    let ts = table_target_with_schema_and_options(ctx, graph, table_name, table_schema, options)?;
    let spec = ts.value().clone();
    let table_provider = ts.provider().clone();
    let records = mount_target::<TableSpec, RecordState>(ctx, ts).await?;
    Ok(table_handle(spec, table_provider, records))
}

pub async fn mount_relation_target(
    ctx: &Ctx,
    graph: &Graph,
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
    graph: &Graph,
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
    graph: &Graph,
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
    graph: &Graph,
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
    graph: &Graph,
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
    graph: &Graph,
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
    state: Option<RecordState>,
}

// ---------------------------------------------------------------------------
// Table container handler (root) + sink yielding record children
// ---------------------------------------------------------------------------

struct TableHandler {
    graph: Graph,
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
            // Always emit when declared so the sink fulfills the record child.
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
                        spec: Some(spec),
                        drop: None,
                        recreate,
                        drop_fields,
                        retype_fields,
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
                        drop: Some(prev_spec.table_name),
                        recreate: false,
                        drop_fields: Vec::new(),
                        retype_fields: Vec::new(),
                    }),
                    sink: self.table_sink(),
                    tracking_record: None,
                    child_invalidation: Some(TargetChildInvalidation::Destructive),
                }))
            }
        }
    }

    fn attachments(&self) -> Result<Vec<(String, ChildTargetDef)>> {
        Ok(vec![(
            "vector_index".to_string(),
            ChildTargetDef::new::<VectorIndexSpec, _>(VectorIndexHandler {
                graph: self.graph.clone(),
            }),
        )])
    }
}

impl TableHandler {
    fn table_sink(&self) -> TargetActionSink<TableAction> {
        let graph = self.graph.clone();
        TargetActionSink::from_async_fn_with_children(
            move |actions: Vec<TargetAction<TableAction>>| {
                let graph = graph.clone();
                async move {
                    let mut out: Vec<Option<ChildTargetDef>> = Vec::with_capacity(actions.len());
                    for action in actions {
                        match action {
                            TargetAction::Create(a) | TargetAction::Update(a) => {
                                let spec = a.spec.ok_or_else(|| {
                                    Error::engine("SurrealDB table action missing spec")
                                })?;
                                // A "main"-shape change can't be applied in place;
                                // remove and redefine (records replayed via the
                                // Destructive child invalidation set at reconcile).
                                if a.recreate && spec.managed_by.is_system() {
                                    remove_table(&graph, &spec.table_name).await?;
                                }
                                define_table(&graph, &spec).await?;
                                if !a.drop_fields.is_empty() || !a.retype_fields.is_empty() {
                                    apply_field_changes(
                                        &graph,
                                        &spec,
                                        &a.drop_fields,
                                        &a.retype_fields,
                                    )
                                    .await?;
                                }
                                out.push(Some(ChildTargetDef::new::<RecordState, _>(
                                    RecordHandler {
                                        graph: graph.clone(),
                                        spec,
                                    },
                                )));
                            }
                            TargetAction::Delete(a) => {
                                if let Some(table_name) = a.drop {
                                    remove_table(&graph, &table_name).await?;
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
// Record handler (child) + sink
// ---------------------------------------------------------------------------

struct RecordHandler {
    graph: Graph,
    spec: TableSpec,
}

impl TargetHandler<RecordState> for RecordHandler {
    type TrackingRecord = Fingerprint;
    type Action = RecordAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<RecordState>,
        prev: Vec<Fingerprint>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<RecordAction, Fingerprint>>> {
        let record_id = stable_key_to_record_id(&key)?;
        // Track a cheap fingerprint of the record state (not the full record).
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
            action: TargetAction::Update(RecordAction {
                record_id,
                state: desired,
            }),
            sink: self.record_sink(),
            tracking_record: desired_fp,
            child_invalidation: None,
        }))
    }
}

impl RecordHandler {
    fn record_sink(&self) -> TargetActionSink<RecordAction> {
        let graph = self.graph.clone();
        let spec = self.spec.clone();
        TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<RecordAction>>| {
            let graph = graph.clone();
            let spec = spec.clone();
            async move {
                let mut mutations = Vec::with_capacity(actions.len());
                for action in actions {
                    let record = match action {
                        TargetAction::Create(r)
                        | TargetAction::Update(r)
                        | TargetAction::Delete(r) => r,
                    };
                    mutations.push((record.record_id, record.state));
                }
                apply_records(&graph, &spec, mutations).await
            }
        })
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
    graph: Graph,
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
            .is_some_and(|d| prev.iter().any(|p| p == d));
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
            sink: self.vector_index_sink(),
            tracking_record: desired,
            child_invalidation: None,
        }))
    }
}

impl VectorIndexHandler {
    fn vector_index_sink(&self) -> TargetActionSink<VectorIndexAction> {
        let graph = self.graph.clone();
        TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<VectorIndexAction>>| {
            let graph = graph.clone();
            async move {
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
        })
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
            let type_expr = if column.nullable {
                format!("option<{}>", column.surreal_type)
            } else {
                column.surreal_type.clone()
            };
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
            let type_expr = if col.nullable {
                format!("option<{}>", col.surreal_type)
            } else {
                col.surreal_type.clone()
            };
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

async fn apply_records(
    graph: &Graph,
    spec: &TableSpec,
    mutations: Vec<(RecordIdValue, Option<RecordState>)>,
) -> Result<()> {
    if mutations.is_empty() {
        return Ok(());
    }
    // Ensure the table exists for system-managed targets (idempotent).
    if spec.managed_by.is_system() {
        define_table(graph, spec).await?;
    }
    validate_ident(&spec.table_name, "table name")?;

    let mut statements = String::from("BEGIN TRANSACTION;\n");
    for (record_id, state) in mutations {
        let record_ref = format!("{}:{}", spec.table_name, record_id.surrealql());
        match state {
            Some(state) => {
                // The record id is carried by `record_ref` (`table:id`); strip any
                // `id` field from CONTENT so it isn't written into the body (matches
                // Python, which excludes `id` from the upsert content).
                let RecordState {
                    mut fields,
                    relation,
                } = state;
                fields.remove("id");
                let content = serde_json::to_string(&JsonValue::Object(fields))
                    .map_err(|e| Error::engine(format!("serialize SurrealDB record: {e}")))?;
                if let Some(rel) = relation {
                    validate_ident(&rel.from_table, "relation from table name")?;
                    validate_ident(&rel.to_table, "relation to table name")?;
                    let from_ref = format!("{}:{}", rel.from_table, rel.from_id.surrealql());
                    let to_ref = format!("{}:{}", rel.to_table, rel.to_id.surrealql());
                    statements.push_str(&format!(
                        "DELETE {record_ref}; RELATE {from_ref}->{record_ref}->{to_ref} CONTENT {content};\n"
                    ));
                } else {
                    statements.push_str(&format!("UPSERT {record_ref} CONTENT {content};\n"));
                }
            }
            None => {
                statements.push_str(&format!("DELETE {record_ref};\n"));
            }
        }
    }
    statements.push_str("COMMIT TRANSACTION;\n");

    graph
        .db
        .query(statements)
        .await
        .map_err(surreal_err)?
        .check()
        .map_err(surreal_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn record_state<R: Serialize>(row: &R) -> Result<RecordState> {
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
}
