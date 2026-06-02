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
use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::profile::{Action, BoxedHandler, BoxedSink, RustProfile, Value};
use crate::statediff::{
    DiffAction, ManagedBy, ManagedTargetOptions, MutualTrackingRecord, diff,
    resolve_system_transition,
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
}

#[derive(Clone)]
pub struct TableTarget {
    pg_schema_name: Option<Arc<str>>,
    table_name: Arc<str>,
    table_schema: TableSchema,
    managed_by: ManagedBy,
    table_provider: cocoindex_core::engine::target_state::TargetStateProvider<RustProfile>,
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
        let provider =
            ctx.register_attachment_target_provider(&self.table_provider, "vector_index")?;
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
    mount_table_target_with_options(
        ctx,
        db,
        table_name,
        table_schema,
        pg_schema_name,
        ManagedTargetOptions::default(),
    )
}

pub fn mount_table_target_with_options(
    ctx: &Ctx,
    db: &Database,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    pg_schema_name: Option<&str>,
    options: ManagedTargetOptions,
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
        managed_by: options.managed_by,
    };
    ctx.declare_target_state(
        table_root.clone(),
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
        pg_schema_name: pg_schema_name.map(Arc::from),
        table_name: Arc::from(table_name),
        table_schema,
        managed_by: options.managed_by,
        table_provider: table_root,
        provider,
    })
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TableSpec {
    pg_schema_name: Option<String>,
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
enum TargetAction {
    Table {
        spec: Option<TableSpec>,
    },
    DropTable {
        pg_schema_name: Option<String>,
        table_name: String,
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

fn table_handler(db: Database) -> BoxedHandler {
    let attachment_db = db.clone();
    let sink = table_sink(db);
    BoxedHandler::new(move |_key, desired, prev, prev_may_be_missing| {
        let desired_spec = desired
            .map(Value::deserialize::<TableSpec>)
            .transpose()
            .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
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
                Ok(Some(TargetReconcileOutput {
                    action: Action::Update(
                        Value::from_serializable(&TargetAction::Table { spec: Some(spec) })
                            .map_err(|e| {
                                cocoindex_utils::error::Error::internal_msg(e.to_string())
                            })?,
                    ),
                    sink: sink.clone(),
                    tracking_record: Some(
                        Value::from_serializable(&tracking_record).map_err(|e| {
                            cocoindex_utils::error::Error::internal_msg(e.to_string())
                        })?,
                    ),
                    child_invalidation: matches!(main_action, Some(DiffAction::Replace))
                        .then_some(cocoindex_core::engine::target_state::ChildInvalidation::Lossy),
                }))
            }
            None => {
                let resolved =
                    resolve_system_transition(None, prev_records.clone(), prev_may_be_missing);
                if resolved.is_none() {
                    return Ok(None);
                }
                let Some(prev_spec) = prev_records
                    .into_iter()
                    .find(|v| v.managed_by.is_system())
                    .map(|v| v.tracking_record)
                else {
                    return Ok(None);
                };
                Ok(Some(TargetReconcileOutput {
                    action: Action::Delete(
                        Value::from_serializable(&TargetAction::DropTable {
                            pg_schema_name: prev_spec.pg_schema_name,
                            table_name: prev_spec.table_name,
                        })
                        .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?,
                    ),
                    sink: sink.clone(),
                    tracking_record: None,
                    child_invalidation: Some(
                        cocoindex_core::engine::target_state::ChildInvalidation::Destructive,
                    ),
                }))
            }
        }
    })
    .with_attachments(move || {
        Ok(vec![(
            Arc::from("vector_index"),
            vector_index_handler(attachment_db.clone()),
        )])
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

fn row_handler(db: Database, spec: TableSpec) -> BoxedHandler {
    let sink = row_sink(db);
    BoxedHandler::new(move |key, desired, prev, prev_may_be_missing| {
        let pk = stable_key_to_pk(&key)
            .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
        let desired_state = desired
            .map(Value::deserialize::<RowState>)
            .transpose()
            .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
        // Track a cheap fingerprint of the row state (not the full row) so
        // unchanged rows are skipped without persisting every column to LMDB.
        let desired_fp = match &desired_state {
            Some(state) => Some(
                Fingerprint::from(state)
                    .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?,
            ),
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
            Some(fp) => Some(
                Value::from_serializable(fp)
                    .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?,
            ),
            None => None,
        };
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
            tracking_record,
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
        let prev_spec = prev
            .iter()
            .filter_map(|v| v.deserialize::<VectorIndexSpec>().ok())
            .next();
        let prev_same = desired_spec
            .as_ref()
            .is_some_and(|desired| prev_spec.as_ref().is_some_and(|prev| prev == desired));
        if desired_spec.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired_spec.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }
        let action = if desired_spec.is_some() {
            Action::Update(
                Value::from_serializable(&TargetAction::VectorIndex { spec: desired_spec })
                    .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?,
            )
        } else {
            Action::Delete(
                Value::from_serializable(&TargetAction::VectorIndex { spec: prev_spec })
                    .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?,
            )
        };
        Ok(Some(TargetReconcileOutput {
            action,
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
                    TargetAction::DropTable {
                        pg_schema_name,
                        table_name,
                    } => {
                        drop_table(&db, pg_schema_name.as_deref(), &table_name).await?;
                    }
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
                let is_delete = matches!(&action, Action::Delete(_));
                if let TargetAction::VectorIndex { spec } = action_value(action)? {
                    if let Some(spec) = spec {
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
    sqlx::query(&sql)
        .execute(db.pool())
        .await
        .map_err(pg_internal)?;
    Ok(())
}

async fn drop_table(
    db: &Database,
    pg_schema_name: Option<&str>,
    table_name: &str,
) -> cocoindex_utils::error::Result<()> {
    let sql = format!(
        "DROP TABLE IF EXISTS {}",
        qualified_table_name_from_parts(pg_schema_name, table_name)
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
    if mutations.is_empty() {
        return Ok(());
    }
    // Ensure the table exists before any row mutation. This keeps delete-only
    // reconciliation idempotent after a crash or external table cleanup.
    if let Some((spec, _, _)) = mutations.first() {
        if spec.managed_by.is_system() {
            define_table(db, spec).await?;
        }
    }
    // Apply the whole batch atomically.
    let mut tx = db.pool().begin().await.map_err(pg_internal)?;
    for (spec, pk, state) in mutations {
        match state {
            Some(state) => {
                let sql = upsert_sql(&spec, &state.fields)?;
                sqlx::query(&sql)
                    .execute(&mut *tx)
                    .await
                    .map_err(pg_internal)?;
            }
            None => {
                let sql = delete_sql(&spec, &pk)?;
                sqlx::query(&sql)
                    .execute(&mut *tx)
                    .await
                    .map_err(pg_internal)?;
            }
        }
    }
    tx.commit().await.map_err(pg_internal)?;
    Ok(())
}

async fn recreate_vector_index(
    db: &Database,
    spec: &VectorIndexSpec,
) -> cocoindex_utils::error::Result<()> {
    drop_vector_index(db, spec).await?;
    let index_name = format!("{}__vector__{}", spec.table_name, spec.name);
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

async fn drop_vector_index(
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
    Ok(())
}

fn upsert_sql(
    spec: &TableSpec,
    fields: &Map<String, JsonValue>,
) -> cocoindex_utils::error::Result<String> {
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
        predicates.push(format!(
            "{} = {}",
            quote_ident(name),
            sql_literal(&pk[idx], col)
                .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?
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
    if lower == "json" || lower == "jsonb" {
        return Ok(format!(
            "{}::{}",
            quote_string(value.to_string()),
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
}

// ---------------------------------------------------------------------------
// Source: read rows from a Postgres table (parallel to Python's PgTableSource)
// ---------------------------------------------------------------------------

/// Read every row of `table_name` (in the connection's default search path) as
/// `T`. This is the source analogue of Python's `PgTableSource(...).fetch_rows()`.
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
    let rows = sqlx::query(&format!(
        "SELECT {select_list} FROM {}",
        qualified_table_name_from_parts(options.pg_schema_name.as_deref(), table_name)
    ))
    .fetch_all(db.pool())
    .await
    .map_err(|e| Error::engine(format!("postgres source read failed: {e}")))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let json = pg_row_to_json(row)?;
        out.push(serde_json::from_value(json).map_err(|e| {
            Error::engine(format!("postgres source row does not match row type: {e}"))
        })?);
    }
    Ok(out)
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
