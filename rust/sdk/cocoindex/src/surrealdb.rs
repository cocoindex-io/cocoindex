//! SurrealDB target helpers.
//!
//! This first Rust SDK target surface mirrors the core Python API shape:
//! mount a table/relation target, then declare records/relations into it. The
//! implementation is schemaless for now, but it uses CocoIndex target-state
//! reconciliation so stale records are deleted by the engine.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

use cocoindex_core::engine::target_state::{ChildTargetDef, TargetReconcileOutput};
use cocoindex_core::state::stable_path::StableKey;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use surrealdb::Surreal;
use surrealdb::engine::remote::ws::{Client, Ws};
use surrealdb::opt::auth::Root;
use surrealdb::types::RecordId;

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::profile::{Action, BoxedHandler, BoxedSink, RustProfile, Value};

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

#[derive(Clone)]
pub struct TableTarget {
    table_name: Arc<str>,
    provider: cocoindex_core::engine::target_state::TargetStateProvider<RustProfile>,
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
        ctx.declare_target_state(
            self.provider.clone(),
            id.to_stable_key(),
            Value::from_serializable(&value)?,
        )
    }
}

#[derive(Clone)]
pub struct RelationTarget {
    table_name: Arc<str>,
    from_tables: Arc<[String]>,
    to_tables: Arc<[String]>,
    from_table: Option<Arc<str>>,
    to_table: Option<Arc<str>>,
    provider: cocoindex_core::engine::target_state::TargetStateProvider<RustProfile>,
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
                    "{}_{}_{}_{}",
                    from_table,
                    from_id.key_fragment(),
                    to_table,
                    to_id.key_fragment()
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
        ctx.declare_target_state(
            self.provider.clone(),
            record_id.stable_key(),
            Value::from_serializable(&value)?,
        )
    }
}

pub fn mount_table_target(
    ctx: &Ctx,
    graph: &Graph,
    table_name: impl Into<String>,
) -> Result<TableTarget> {
    mount_table_target_with_schema(ctx, graph, table_name, None)
}

pub fn mount_table_target_with_schema(
    ctx: &Ctx,
    graph: &Graph,
    table_name: impl Into<String>,
    table_schema: Option<TableSchema>,
) -> Result<TableTarget> {
    let table_name = table_name.into();
    validate_ident(&table_name, "table name")?;
    let provider = mount_table_like(
        ctx,
        graph,
        TableSpec::table(table_name.clone(), table_schema),
    )?;
    Ok(TableTarget {
        table_name: Arc::from(table_name),
        provider,
    })
}

pub fn mount_relation_target(
    ctx: &Ctx,
    graph: &Graph,
    table_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
) -> Result<RelationTarget> {
    let table_name = table_name.into();
    validate_ident(&table_name, "relation table name")?;
    mount_relation_target_many(ctx, graph, table_name, &[from_table], &[to_table], None)
}

pub fn mount_relation_target_many(
    ctx: &Ctx,
    graph: &Graph,
    table_name: impl Into<String>,
    from_tables: &[&TableTarget],
    to_tables: &[&TableTarget],
    table_schema: Option<TableSchema>,
) -> Result<RelationTarget> {
    let table_name = table_name.into();
    validate_ident(&table_name, "relation table name")?;
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
    let provider = mount_table_like(
        ctx,
        graph,
        TableSpec::relation(
            table_name.clone(),
            from_names.clone(),
            to_names.clone(),
            table_schema,
        ),
    )?;
    Ok(RelationTarget {
        table_name: Arc::from(table_name),
        from_tables: Arc::from(from_names.clone()),
        to_tables: Arc::from(to_names.clone()),
        from_table: (from_names.len() == 1).then(|| Arc::from(from_names[0].clone())),
        to_table: (to_names.len() == 1).then(|| Arc::from(to_names[0].clone())),
        provider,
    })
}

pub fn mount_relation_target_unconstrained(
    ctx: &Ctx,
    graph: &Graph,
    table_name: impl Into<String>,
) -> Result<RelationTarget> {
    let table_name = table_name.into();
    validate_ident(&table_name, "relation table name")?;
    let provider = mount_table_like(
        ctx,
        graph,
        TableSpec::relation(table_name.clone(), Vec::new(), Vec::new(), None),
    )?;
    Ok(RelationTarget {
        table_name: Arc::from(table_name),
        from_tables: Arc::from([]),
        to_tables: Arc::from([]),
        from_table: None,
        to_table: None,
        provider,
    })
}

fn mount_table_like(
    ctx: &Ctx,
    graph: &Graph,
    spec: TableSpec,
) -> Result<cocoindex_core::engine::target_state::TargetStateProvider<RustProfile>> {
    let table_root = ctx.register_root_target_provider(
        format!("cocoindex/surrealdb/table/{}", spec.table_name),
        table_handler(graph.clone()),
    )?;
    let key = StableKey::Array(Arc::from([
        StableKey::Str(Arc::from("default")),
        StableKey::Str(Arc::from(spec.table_name.clone())),
    ]));
    ctx.declare_target_state(table_root, key, Value::from_serializable(&spec)?)?;

    ctx.register_root_target_provider(
        format!("cocoindex/surrealdb/record/{}", spec.table_name),
        record_handler(graph.clone(), spec),
    )
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TableSpec {
    table_name: String,
    table_schema: Option<TableSchema>,
    is_relation: bool,
    from_tables: Vec<String>,
    to_tables: Vec<String>,
}

impl TableSpec {
    fn table(table_name: String, table_schema: Option<TableSchema>) -> Self {
        Self {
            table_name,
            table_schema,
            is_relation: false,
            from_tables: Vec::new(),
            to_tables: Vec::new(),
        }
    }

    fn relation(
        table_name: String,
        mut from_tables: Vec<String>,
        mut to_tables: Vec<String>,
        table_schema: Option<TableSchema>,
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
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct RecordState {
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

#[derive(Clone, Debug, Serialize, Deserialize)]
enum TargetAction {
    Table {
        table_name: String,
        spec: Option<TableSpec>,
    },
    Record {
        spec: TableSpec,
        table_name: String,
        record_id: RecordIdValue,
        state: Option<RecordState>,
    },
}

fn table_handler(graph: Graph) -> BoxedHandler {
    let sink = table_sink(graph.clone());
    BoxedHandler::new(move |key, desired, prev, prev_may_be_missing| {
        let desired_spec = desired
            .map(Value::deserialize::<TableSpec>)
            .transpose()
            .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
        let table_name = desired_spec
            .as_ref()
            .map(|spec| Ok(spec.table_name.clone()))
            .unwrap_or_else(|| table_name_from_key(&key))
            .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
        let prev_same = desired_spec.as_ref().is_some_and(|desired| {
            prev.iter()
                .filter_map(|v| v.deserialize::<TableSpec>().ok())
                .any(|prev| &prev == desired)
        });
        if desired_spec.is_some() && prev_same && !prev_may_be_missing {
            return Ok(Some(TargetReconcileOutput {
                action: Action::Update(
                    Value::from_serializable(&TargetAction::Table {
                        table_name,
                        spec: desired_spec.clone(),
                    })
                    .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?,
                ),
                sink: sink.clone(),
                tracking_record: desired.cloned(),
                child_invalidation: None,
            }));
        }
        if desired_spec.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }
        Ok(Some(TargetReconcileOutput {
            action: Action::Update(
                Value::from_serializable(&TargetAction::Table {
                    table_name,
                    spec: desired_spec,
                })
                .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?,
            ),
            sink: sink.clone(),
            tracking_record: desired.cloned(),
            child_invalidation: None,
        }))
    })
}

fn record_handler(graph: Graph, spec: TableSpec) -> BoxedHandler {
    let sink = record_sink(graph);
    BoxedHandler::new(move |key, desired, prev, prev_may_be_missing| {
        let record_id = stable_key_to_record_id(&key)
            .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
        let desired_state = desired
            .map(Value::deserialize::<RecordState>)
            .transpose()
            .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
        let prev_same = desired_state.as_ref().is_some_and(|desired| {
            prev.iter()
                .filter_map(|v| v.deserialize::<RecordState>().ok())
                .any(|prev| &prev == desired)
        });
        if desired_state.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired_state.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }
        let action = TargetAction::Record {
            spec: spec.clone(),
            table_name: spec.table_name.clone(),
            record_id,
            state: desired_state,
        };
        Ok(Some(TargetReconcileOutput {
            action: Action::Update(
                Value::from_serializable(&action)
                    .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?,
            ),
            sink: sink.clone(),
            tracking_record: desired.cloned(),
            child_invalidation: None,
        }))
    })
}

fn table_sink(graph: Graph) -> BoxedSink {
    BoxedSink::new(move |actions| {
        let graph = graph.clone();
        Box::pin(async move {
            let mut out = Vec::with_capacity(actions.len());
            for action in actions {
                let action = action_value(action)?;
                match action {
                    TargetAction::Table {
                        table_name: _,
                        spec: Some(spec),
                    } => {
                        define_table(&graph, &spec).await?;
                        out.push(Some(ChildTargetDef {
                            handler: record_handler(graph.clone(), spec),
                        }));
                    }
                    TargetAction::Table {
                        table_name,
                        spec: None,
                    } => {
                        remove_table(&graph, &table_name).await?;
                        out.push(None);
                    }
                    TargetAction::Record { .. } => {
                        return Err(cocoindex_utils::error::Error::internal_msg(
                            "record action routed to table sink",
                        ));
                    }
                }
            }
            Ok(Some(out))
        }) as Pin<Box<_>>
    })
}

fn record_sink(graph: Graph) -> BoxedSink {
    BoxedSink::new(move |actions| {
        let graph = graph.clone();
        Box::pin(async move {
            let mut mutations = Vec::with_capacity(actions.len());
            for action in actions {
                let action = action_value(action)?;
                if let TargetAction::Record {
                    spec,
                    table_name,
                    record_id,
                    state,
                } = action
                {
                    mutations.push(RecordMutation {
                        spec,
                        table_name,
                        record_id,
                        state,
                    });
                }
            }
            apply_records(&graph, mutations).await?;
            Ok(None)
        }) as Pin<Box<_>>
    })
}

fn action_value(action: Action) -> cocoindex_utils::error::Result<TargetAction> {
    let value = match action {
        Action::Create(v) | Action::Update(v) | Action::Delete(v) => v,
    };
    value
        .deserialize()
        .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))
}

async fn define_table(graph: &Graph, spec: &TableSpec) -> cocoindex_utils::error::Result<()> {
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
        .map_err(|e| cocoindex_utils::error::Error::internal_msg(format!("surrealdb: {e}")))?
        .check()
        .map_err(|e| cocoindex_utils::error::Error::internal_msg(format!("surrealdb: {e}")))?;
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
                .map_err(|e| {
                    cocoindex_utils::error::Error::internal_msg(format!("surrealdb: {e}"))
                })?
                .check()
                .map_err(|e| {
                    cocoindex_utils::error::Error::internal_msg(format!("surrealdb: {e}"))
                })?;
        }
    }
    Ok(())
}

async fn remove_table(graph: &Graph, table_name: &str) -> cocoindex_utils::error::Result<()> {
    validate_ident(table_name, "table name")
        .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
    graph
        .db
        .query(format!("REMOVE TABLE IF EXISTS {table_name}"))
        .await
        .map_err(|e| cocoindex_utils::error::Error::internal_msg(format!("surrealdb: {e}")))?
        .check()
        .map_err(|e| cocoindex_utils::error::Error::internal_msg(format!("surrealdb: {e}")))?;
    Ok(())
}

struct RecordMutation {
    spec: TableSpec,
    table_name: String,
    record_id: RecordIdValue,
    state: Option<RecordState>,
}

async fn apply_records(
    graph: &Graph,
    mutations: Vec<RecordMutation>,
) -> cocoindex_utils::error::Result<()> {
    if mutations.is_empty() {
        return Ok(());
    }

    let mut defined_tables = BTreeMap::new();
    for mutation in &mutations {
        defined_tables.insert(mutation.table_name.clone(), mutation.spec.clone());
    }
    for spec in defined_tables.values() {
        define_table(graph, spec).await?;
    }

    let mut statements = String::from("BEGIN TRANSACTION;\n");
    for mutation in mutations {
        validate_ident(&mutation.table_name, "table name")
            .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
        let record_ref = format!("{}:{}", mutation.table_name, mutation.record_id.surrealql());
        match mutation.state {
            Some(state) => {
                let content = serde_json::to_string(&JsonValue::Object(state.fields))
                    .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
                if let Some(rel) = state.relation {
                    validate_ident(&rel.from_table, "relation from table name")
                        .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
                    validate_ident(&rel.to_table, "relation to table name")
                        .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
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
        .map_err(|e| cocoindex_utils::error::Error::internal_msg(format!("surrealdb: {e}")))?
        .check()
        .map_err(|e| cocoindex_utils::error::Error::internal_msg(format!("surrealdb: {e}")))?;
    Ok(())
}

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

fn stable_key_to_id(key: &StableKey) -> Result<String> {
    match key {
        StableKey::Str(s) | StableKey::Symbol(s) => Ok(s.to_string()),
        StableKey::Int(i) => Ok(i.to_string()),
        StableKey::Uuid(u) => Ok(u.to_string()),
        other => Err(Error::engine(format!(
            "unsupported SurrealDB record key: {other:?}"
        ))),
    }
}

fn table_name_from_key(key: &StableKey) -> Result<String> {
    match key {
        StableKey::Array(parts) if parts.len() == 2 => stable_key_to_id(&parts[1]),
        _ => stable_key_to_id(key),
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
