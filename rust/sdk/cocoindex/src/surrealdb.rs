//! SurrealDB target connector.
//!
//! Built **on the public target-state facade** ([`crate::target_state`]): a
//! *table* (or *relation*) container — defined/removed to match the declared
//! schema — containing *records* you declare via [`TableTarget::declare_record`]
//! / [`RelationTarget::declare_relation`]. Reconciliation upserts changed records
//! (one transaction per batch), skips unchanged ones (fingerprint tracking), and
//! deletes records that disappeared. `managed_by` (via [`ManagedTargetOptions`])
//! controls whether CocoIndex owns the DDL. Each kind exposes the
//! constructor/declaration/mount split mirroring Python: a spec constructor
//! ([`table_target`]/[`relation_target_many`]/[`relation_target_unconstrained`]),
//! a pending declaration ([`declare_table_target`]/…), and a foreground
//! [`mount_table_target`]/… (async).

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
use crate::statediff::{
    DiffAction, ManagedBy, ManagedTargetOptions, MutualTrackingRecord, diff,
    resolve_system_transition,
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
}

// ---------------------------------------------------------------------------
// Target handles
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct TableTarget {
    table_name: Arc<str>,
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

/// Build a composable [`TargetState`] for a SurrealDB table (schemaless,
/// system-managed). Pass it to [`declare_table_target`]/[`mount_table_target`]
/// or the generic facade helpers.
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

/// Declare a SurrealDB table target in the **current** component (the record
/// child provider resolves at this component's commit) and return a handle.
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
    let records = declare_target_state_with_child::<TableSpec, RecordState>(ctx, ts)?;
    Ok(table_handle(spec, records))
}

/// Declare a SurrealDB relation target in the **current** component.
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
    let records = mount_target::<TableSpec, RecordState>(ctx, ts).await?;
    Ok(table_handle(spec, records))
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
                                define_table(&graph, &spec).await?;
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
            .is_some_and(|fp| prev.iter().any(|p| p == fp));
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
                let content = serde_json::to_string(&JsonValue::Object(state.fields))
                    .map_err(|e| Error::engine(format!("serialize SurrealDB record: {e}")))?;
                if let Some(rel) = state.relation {
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
