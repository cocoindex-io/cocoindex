//! Postgres target helpers.
//!
//! This mirrors the Python connector's core `TableTarget` shape: mount a table
//! target, declare rows into it, and let CocoIndex target-state reconciliation
//! upsert changed rows and delete rows that disappeared from the desired state.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

use cocoindex_core::engine::target_state::TargetReconcileOutput;
use cocoindex_core::state::stable_path::StableKey;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::profile::{Action, BoxedHandler, BoxedSink, RustProfile, Value};

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
        let primary_key: Vec<String> = primary_key
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
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
}

#[derive(Clone)]
pub struct TableTarget {
    db: Database,
    pg_schema_name: Option<Arc<str>>,
    table_name: Arc<str>,
    table_schema: TableSchema,
    provider: cocoindex_core::engine::target_state::TargetStateProvider<RustProfile>,
}

impl TableTarget {
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn declare_row<R: Serialize>(&self, ctx: &Ctx, row: &R) -> Result<()> {
        let row = row_state(row, &self.table_schema)?;
        let key = pk_stable_key(&row, self.table_schema.primary_key())?;
        ctx.declare_target_state(
            self.provider.clone(),
            key,
            Value::from_serializable(&RowState { fields: row })?,
        )
    }

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
        let provider = ctx.register_root_target_provider(
            format!(
                "cocoindex/postgres/vector_index/{}/{}/{}",
                self.db.state_id(),
                self.pg_schema_name.as_deref().unwrap_or("public"),
                self.table_name
            ),
            vector_index_handler(self.db.clone()),
        )?;
        let spec = VectorIndexSpec {
            pg_schema_name: self.pg_schema_name.as_deref().map(str::to_string),
            table_name: self.table_name.to_string(),
            table_schema: self.table_schema.clone(),
            name: name.clone(),
            column: column.to_string(),
            method: options.method.to_string(),
            metric: options.metric.to_string(),
            op_class: op_class.to_string(),
            lists: options.lists,
            m: options.m,
            ef_construction: options.ef_construction,
        };
        ctx.declare_target_state(
            provider,
            StableKey::Str(Arc::from(name)),
            Value::from_serializable(&spec)?,
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

pub fn mount_table_target(
    ctx: &Ctx,
    db: &Database,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    pg_schema_name: Option<&str>,
) -> Result<TableTarget> {
    let table_name = table_name.into();
    validate_ident(&table_name, "table name")?;
    if let Some(schema) = pg_schema_name {
        validate_ident(schema, "schema name")?;
    }
    let table_root = ctx.register_root_target_provider(
        format!(
            "cocoindex/postgres/table/{}/{}/{}",
            db.state_id(),
            pg_schema_name.unwrap_or("public"),
            table_name
        ),
        table_handler(db.clone()),
    )?;
    let spec = TableSpec {
        pg_schema_name: pg_schema_name.map(str::to_string),
        table_name: table_name.clone(),
        table_schema: table_schema.clone(),
    };
    ctx.declare_target_state(
        table_root,
        StableKey::Str(Arc::from("default")),
        Value::from_serializable(&spec)?,
    )?;
    let provider = ctx.register_root_target_provider(
        format!(
            "cocoindex/postgres/row/{}/{}/{}",
            db.state_id(),
            pg_schema_name.unwrap_or("public"),
            table_name
        ),
        row_handler(db.clone(), spec),
    )?;
    Ok(TableTarget {
        db: db.clone(),
        pg_schema_name: pg_schema_name.map(Arc::from),
        table_name: Arc::from(table_name),
        table_schema,
        provider,
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TableSpec {
    pg_schema_name: Option<String>,
    table_name: String,
    table_schema: TableSchema,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct RowState {
    fields: Map<String, JsonValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum TargetAction {
    Table {
        spec: Option<TableSpec>,
    },
    Row {
        spec: TableSpec,
        pk: Vec<JsonValue>,
        state: Option<RowState>,
    },
    VectorIndex {
        spec: Option<VectorIndexSpec>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct VectorIndexSpec {
    pg_schema_name: Option<String>,
    table_name: String,
    table_schema: TableSchema,
    name: String,
    column: String,
    method: String,
    metric: String,
    op_class: String,
    lists: Option<u32>,
    m: Option<u32>,
    ef_construction: Option<u32>,
}

fn table_handler(db: Database) -> BoxedHandler {
    let sink = table_sink(db);
    BoxedHandler::new(move |_key, desired, prev, prev_may_be_missing| {
        let desired_spec = desired
            .map(Value::deserialize::<TableSpec>)
            .transpose()
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
                Value::from_serializable(&TargetAction::Table { spec: desired_spec })
                    .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?,
            ),
            sink: sink.clone(),
            tracking_record: desired.cloned(),
            child_invalidation: None,
        }))
    })
}

fn row_handler(db: Database, spec: TableSpec) -> BoxedHandler {
    let sink = row_sink(db);
    BoxedHandler::new(move |key, desired, prev, prev_may_be_missing| {
        let pk = stable_key_to_pk(&key)
            .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
        let desired_state = desired
            .map(Value::deserialize::<RowState>)
            .transpose()
            .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
        let prev_same = desired_state.as_ref().is_some_and(|desired| {
            prev.iter()
                .filter_map(|v| v.deserialize::<RowState>().ok())
                .any(|prev| &prev == desired)
        });
        if desired_state.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired_state.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }
        Ok(Some(TargetReconcileOutput {
            action: Action::Update(
                Value::from_serializable(&TargetAction::Row {
                    spec: spec.clone(),
                    pk,
                    state: desired_state,
                })
                .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?,
            ),
            sink: sink.clone(),
            tracking_record: desired.cloned(),
            child_invalidation: None,
        }))
    })
}

fn vector_index_handler(db: Database) -> BoxedHandler {
    let sink = vector_index_sink(db);
    BoxedHandler::new(move |_key, desired, prev, prev_may_be_missing| {
        let desired_spec = desired
            .map(Value::deserialize::<VectorIndexSpec>)
            .transpose()
            .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
        let prev_same = desired_spec.as_ref().is_some_and(|desired| {
            prev.iter()
                .filter_map(|v| v.deserialize::<VectorIndexSpec>().ok())
                .any(|prev| &prev == desired)
        });
        if desired_spec.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired_spec.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }
        Ok(Some(TargetReconcileOutput {
            action: Action::Update(
                Value::from_serializable(&TargetAction::VectorIndex { spec: desired_spec })
                    .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?,
            ),
            sink: sink.clone(),
            tracking_record: desired.cloned(),
            child_invalidation: None,
        }))
    })
}

fn table_sink(db: Database) -> BoxedSink {
    BoxedSink::new(move |actions| {
        let db = db.clone();
        Box::pin(async move {
            for action in actions {
                match action_value(action)? {
                    TargetAction::Table { spec: Some(spec) } => {
                        define_table(&db, &spec).await?;
                    }
                    TargetAction::Table { spec: None } => {}
                    _ => {
                        return Err(cocoindex_utils::error::Error::internal_msg(
                            "non-table action routed to Postgres table sink",
                        ));
                    }
                }
            }
            Ok(None)
        }) as Pin<Box<_>>
    })
}

fn row_sink(db: Database) -> BoxedSink {
    BoxedSink::new(move |actions| {
        let db = db.clone();
        Box::pin(async move {
            let mut mutations = Vec::with_capacity(actions.len());
            for action in actions {
                if let TargetAction::Row { spec, pk, state } = action_value(action)? {
                    mutations.push((spec, pk, state));
                }
            }
            apply_rows(&db, mutations).await?;
            Ok(None)
        }) as Pin<Box<_>>
    })
}

fn vector_index_sink(db: Database) -> BoxedSink {
    BoxedSink::new(move |actions| {
        let db = db.clone();
        Box::pin(async move {
            for action in actions {
                if let TargetAction::VectorIndex { spec } = action_value(action)? {
                    if let Some(spec) = spec {
                        define_table(
                            &db,
                            &TableSpec {
                                pg_schema_name: spec.pg_schema_name.clone(),
                                table_name: spec.table_name.clone(),
                                table_schema: spec.table_schema.clone(),
                            },
                        )
                        .await?;
                        recreate_vector_index(&db, &spec).await?;
                    }
                }
            }
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

async fn define_table(db: &Database, spec: &TableSpec) -> cocoindex_utils::error::Result<()> {
    if let Some(schema) = &spec.pg_schema_name {
        sqlx::query(&format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            quote_ident(schema)
        ))
            .execute(db.pool())
            .await
            .map_err(pg_internal)?;
    }
    if schema_uses_pgvector(&spec.table_schema) {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
            .execute(db.pool())
            .await
            .map_err(pg_internal)?;
    }
    let mut defs = Vec::new();
    for (name, col) in spec.table_schema.columns() {
        let nullable = if col.nullable && !spec.table_schema.primary_key().contains(name) {
            ""
        } else {
            " NOT NULL"
        };
        defs.push(format!(
            "{} {}{}",
            quote_ident(name),
            col.pg_type,
            nullable
        ));
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
    sqlx::query(&sql)
        .execute(db.pool())
        .await
        .map_err(pg_internal)?;
    Ok(())
}

async fn apply_rows(
    db: &Database,
    mutations: Vec<(TableSpec, Vec<JsonValue>, Option<RowState>)>,
) -> cocoindex_utils::error::Result<()> {
    for (spec, pk, state) in mutations {
        define_table(db, &spec).await?;
        match state {
            Some(state) => {
                let sql = upsert_sql(&spec, &state.fields)?;
                sqlx::query(&sql)
                    .execute(db.pool())
                    .await
                    .map_err(pg_internal)?;
            }
            None => {
                let sql = delete_sql(&spec, &pk)?;
                sqlx::query(&sql)
                    .execute(db.pool())
                    .await
                    .map_err(pg_internal)?;
            }
        }
    }
    Ok(())
}

async fn recreate_vector_index(
    db: &Database,
    spec: &VectorIndexSpec,
) -> cocoindex_utils::error::Result<()> {
    let index_name = format!("{}__vector__{}", spec.table_name, spec.name);
    sqlx::query(&format!(
        "DROP INDEX IF EXISTS {}",
        qualified_index_name(spec.pg_schema_name.as_deref(), &index_name)
    ))
    .execute(db.pool())
    .await
    .map_err(pg_internal)?;
    let mut with_parts = Vec::new();
    if let Some(lists) = spec.lists {
        with_parts.push(format!("lists = {lists}"));
    }
    if let Some(m) = spec.m {
        with_parts.push(format!("m = {m}"));
    }
    if let Some(ef) = spec.ef_construction {
        with_parts.push(format!("ef_construction = {ef}"));
    }
    let with_sql = if with_parts.is_empty() {
        String::new()
    } else {
        format!(" WITH ({})", with_parts.join(", "))
    };
    let sql = format!(
        "CREATE INDEX {} ON {} USING {} ({} {}){}",
        quote_ident(&index_name),
        qualified_table_name_from_parts(spec.pg_schema_name.as_deref(), &spec.table_name),
        spec.method,
        quote_ident(&spec.column),
        spec.op_class,
        with_sql
    );
    sqlx::query(&sql)
        .execute(db.pool())
        .await
        .map_err(pg_internal)?;
    Ok(())
}

fn upsert_sql(spec: &TableSpec, fields: &Map<String, JsonValue>) -> cocoindex_utils::error::Result<String> {
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
            let col = spec.table_schema.columns().get(name).expect("schema column");
            let value = fields.get(name).unwrap_or(&JsonValue::Null);
            sql_literal(value, col)
        })
        .collect::<Result<Vec<_>>>()
        .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?
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

fn delete_sql(spec: &TableSpec, pk: &[JsonValue]) -> cocoindex_utils::error::Result<String> {
    if pk.len() != spec.table_schema.primary_key().len() {
        return Err(cocoindex_utils::error::Error::internal_msg(
            "Postgres row target primary key length mismatch",
        ));
    }
    let mut predicates = Vec::with_capacity(pk.len());
    for (idx, name) in spec.table_schema.primary_key().iter().enumerate() {
        let col = spec.table_schema.columns().get(name).expect("pk column");
        predicates.push(format!("{} = {}", quote_ident(name), sql_literal(&pk[idx], col).map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?));
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
        return Err(Error::engine("Postgres target row must serialize to an object"));
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
        other => Err(Error::engine(format!("unsupported Postgres row key: {other:?}"))),
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
    if lower == "json" || lower == "jsonb" {
        return Ok(format!("{}::{}", quote_string(value.to_string()), col.pg_type));
    }
    match value {
        JsonValue::String(s) => Ok(format!("{}::{}", quote_string(s), col.pg_type)),
        JsonValue::Number(n) => Ok(format!("{}::{}", n, col.pg_type)),
        JsonValue::Bool(b) => Ok(format!(
            "{}::{}",
            if *b { "TRUE" } else { "FALSE" },
            col.pg_type
        )),
        _ => Ok(format!("{}::{}", quote_string(value.to_string()), col.pg_type)),
    }
}

fn value_to_string(value: &JsonValue) -> Result<&str> {
    value
        .as_str()
        .ok_or_else(|| Error::engine("text column requires string JSON value"))
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
    Ok(format!("{}::{}", quote_string(format!("[{}]", parts.join(","))), pg_type))
}

fn quote_string(value: impl AsRef<str>) -> String {
    let value = value.as_ref().replace('\0', "").replace('\'', "''");
    format!("'{value}'")
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

fn pg_internal(e: sqlx::Error) -> cocoindex_utils::error::Error {
    cocoindex_utils::error::Error::internal_msg(format!("postgres: {e}"))
}
