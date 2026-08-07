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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::builder::{FixedSizeListBuilder, PrimitiveBuilder};
use arrow_array::types::{ArrowPrimitiveType, Float16Type, Float32Type};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, RecordBatchIterator, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cocoindex_utils::fingerprint::Fingerprint;
use half::f16;
use lancedb::Connection;
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::table::NewColumnTransform;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

use crate::ctx::{ContextKey, ContextStore, Ctx};
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
    Bool,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Text,
    Binary,
    /// Complex values encoded as JSON strings, matching Python's fallback.
    Json,
    /// Fixed-size float32 vector of the given dimension.
    Vector(usize),
    /// Fixed-size float16 vector of the given dimension.
    HalfVector(usize),
}

impl ColumnType {
    fn arrow_data_type(&self) -> DataType {
        match self {
            ColumnType::Bool => DataType::Boolean,
            ColumnType::Int16 => DataType::Int16,
            ColumnType::Int32 => DataType::Int32,
            ColumnType::Int64 => DataType::Int64,
            ColumnType::Float32 => DataType::Float32,
            ColumnType::Float64 => DataType::Float64,
            ColumnType::Text | ColumnType::Json => DataType::Utf8,
            ColumnType::Binary => DataType::Binary,
            ColumnType::Vector(dim) => DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                *dim as i32,
            ),
            ColumnType::HalfVector(dim) => DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float16, true)),
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

    /// Derive a schema from a `#[derive(SchemaFields)]` row type.
    pub fn from_row<T: crate::row_schema::SchemaFields>(
        primary_key: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let columns = T::schema_fields()
            .into_iter()
            .map(|field| {
                let def = lancedb_column_def(&field)?;
                Ok((field.name, def))
            })
            .collect::<Result<Vec<_>>>()?;
        Self::new(columns, primary_key)
    }

    /// Resolve or override the dimension of a vector field derived from a row.
    pub fn with_vector_dim(mut self, field_name: &str, dim: usize) -> Result<Self> {
        if dim == 0 || i32::try_from(dim).is_err() {
            return Err(crate::row_schema::vector_dimension_error(
                "LanceDB",
                field_name,
                "requires a dimension in 1..=i32::MAX",
            ));
        }
        let (_, def) = self
            .columns
            .iter_mut()
            .find(|(name, _)| name == field_name)
            .ok_or_else(|| crate::row_schema::unknown_vector_field_error("LanceDB", field_name))?;
        match &mut def.col_type {
            ColumnType::Vector(current_dim) | ColumnType::HalfVector(current_dim) => {
                *current_dim = dim;
            }
            _ => {
                return Err(crate::row_schema::not_vector_field_error(
                    "LanceDB", field_name,
                ));
            }
        }
        Ok(self)
    }

    fn validate_vector_dimensions(&self) -> Result<()> {
        for (name, def) in &self.columns {
            let dim = match def.col_type {
                ColumnType::Vector(dim) | ColumnType::HalfVector(dim) => dim,
                _ => continue,
            };
            if dim == 0 {
                crate::row_schema::require_resolved_vector_dimension("LanceDB", name)?;
            }
            if i32::try_from(dim).is_err() {
                return Err(crate::row_schema::vector_dimension_error(
                    "LanceDB",
                    name,
                    "requires a dimension in 1..=i32::MAX",
                ));
            }
        }
        Ok(())
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

fn lancedb_column_def(field: &crate::row_schema::SchemaField) -> Result<ColumnDef> {
    use crate::row_schema::LogicalType as L;

    let col_type = match &field.logical_type {
        L::Bool => ColumnType::Bool,
        L::Int16 => ColumnType::Int16,
        L::Int32 => ColumnType::Int32,
        L::Int64 => ColumnType::Int64,
        L::Float32 => ColumnType::Float32,
        L::Float64 => ColumnType::Float64,
        L::Text | L::Uuid | L::Date | L::Time | L::DateTime => ColumnType::Text,
        L::Bytes => ColumnType::Binary,
        L::Decimal | L::Duration | L::Json => ColumnType::Json,
        L::Vector { dim, half } => {
            if *half {
                ColumnType::HalfVector(*dim as usize)
            } else {
                ColumnType::Vector(*dim as usize)
            }
        }
        L::Custom(custom) => {
            return Err(Error::engine(format!(
                "LanceDB field {:?} has unsupported custom logical type {custom:?}",
                field.name
            )));
        }
    };
    let def = ColumnDef::new(col_type);
    Ok(if field.nullable { def.nullable() } else { def })
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
    managed_by: ManagedBy,
    rows: TargetStateProvider<RowState>,
}

/// Build a composable [`TargetState`] for a LanceDB table (the spec constructor).
/// Pass it to [`declare_table_target`]/[`mount_table_target`] or the generic
/// [`declare_target_state_with_child`]/[`mount_target`]. System-managed.
pub fn table_target(
    ctx: &Ctx,
    db: &ContextKey<LanceDatabase>,
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
    db: &ContextKey<LanceDatabase>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    options: ManagedTargetOptions,
) -> Result<TargetState<TableSpec>> {
    let table_name = table_name.into();
    table_schema.validate_vector_dimensions()?;
    let provider = register_root_target_states_provider(
        ctx,
        format!("cocoindex/lancedb/table/{}/{}", db.name(), table_name),
        TableHandler {
            db_key: db.name().to_string(),
        },
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
    db: &ContextKey<LanceDatabase>,
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
            db.name(),
            table_name,
            schema_fp
        ),
        RowHandler::new(db.name().to_string(), spec.clone(), HashSet::new()),
    )?;
    Ok(LanceTableTarget {
        table_name: Arc::from(table_name),
        table_schema: spec.table_schema,
        managed_by: spec.managed_by,
        rows,
    })
}

/// Declare a LanceDB table target in the current component and return a handle
/// for declaring rows. Existing tables are preserved on compatible schema
/// changes; incompatible system-managed schema changes recreate the table and
/// invalidate child rows.
pub async fn mount_table_target(
    ctx: &Ctx,
    db: &ContextKey<LanceDatabase>,
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
    db: &ContextKey<LanceDatabase>,
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
        managed_by: spec.managed_by,
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

    fn column_type(&self, column: &str) -> Option<&ColumnType> {
        self.table_schema
            .columns
            .iter()
            .find(|(n, _)| n == column)
            .map(|(_, d)| &d.col_type)
    }

    /// Declare a vector index on `column` as an attachment of this table. The
    /// index is created/recreated/dropped to match the declared options. The
    /// column must be a [`ColumnType::Vector`] or [`ColumnType::HalfVector`].
    pub fn declare_vector_index(
        &self,
        ctx: &Ctx,
        column: &str,
        options: VectorIndexOptions,
    ) -> Result<()> {
        validate_identifier(column)?;
        match self.column_type(column) {
            Some(ColumnType::Vector(_) | ColumnType::HalfVector(_)) => {}
            Some(_) => {
                return Err(Error::engine(format!(
                    "LanceDB vector index column {column:?} is not a vector column"
                )));
            }
            None => {
                return Err(Error::engine(format!(
                    "LanceDB vector index column {column:?} is not in the table schema"
                )));
            }
        }
        let name = options.name.unwrap_or_else(|| format!("{column}_idx"));
        validate_identifier(&name)?;
        // Attach to the ROW provider, not the root table provider: the row
        // provider is destructively invalidated when the table is recreated
        // (incompatible schema change), so the index is rebuilt on the new table
        // instead of being silently lost. Mirrors Python's `_RowHandler.attachments`.
        let provider: TargetStateProvider<VectorIndexSpec> =
            self.rows.attachment(ctx, "vector_index")?;
        let spec = VectorIndexSpec {
            table_name: self.table_name.to_string(),
            name: name.clone(),
            column: column.to_string(),
            metric: options.metric.to_string(),
            index_type: options.index_type.to_string(),
            num_partitions: options.num_partitions,
            num_sub_vectors: options.num_sub_vectors,
            num_bits: options.num_bits,
            m: options.m,
            ef_construction: options.ef_construction,
            managed_by: self.managed_by,
        };
        declare_target_state(
            ctx,
            provider.target_state(StableKey::Str(Arc::from(name)), spec),
        )
    }

    /// Declare a full-text-search (inverted) index on `column` as an attachment
    /// of this table. The column must be [`ColumnType::Text`].
    pub fn declare_fts_index(
        &self,
        ctx: &Ctx,
        column: &str,
        options: FtsIndexOptions,
    ) -> Result<()> {
        validate_identifier(column)?;
        match self.column_type(column) {
            Some(ColumnType::Text) => {}
            Some(_) => {
                return Err(Error::engine(format!(
                    "LanceDB FTS index column {column:?} is not a text column"
                )));
            }
            None => {
                return Err(Error::engine(format!(
                    "LanceDB FTS index column {column:?} is not in the table schema"
                )));
            }
        }
        let name = options.name.unwrap_or_else(|| format!("{column}_fts_idx"));
        validate_identifier(&name)?;
        // Attach to the ROW provider (see `declare_vector_index`) so the index
        // survives a destructive table replace.
        let provider: TargetStateProvider<FtsIndexSpec> = self.rows.attachment(ctx, "fts_index")?;
        let spec = FtsIndexSpec {
            table_name: self.table_name.to_string(),
            name: name.clone(),
            column: column.to_string(),
            with_position: options.with_position,
            language: options.language,
            managed_by: self.managed_by,
        };
        declare_target_state(
            ctx,
            provider.target_state(StableKey::Str(Arc::from(name)), spec),
        )
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

/// Resolve the live LanceDB database from the host context by the connection's
/// stable `db_key` (the ContextKey name), used at apply time.
fn resolve_db(host_ctx: &Arc<ContextStore>, db_key: &str) -> Result<Arc<LanceDatabase>> {
    host_ctx.resolve::<LanceDatabase>(db_key).ok_or_else(|| {
        Error::engine(format!(
            "lancedb target: database `{db_key}` was not provided in the environment \
             (call Environment::builder().provide_key(&KEY, db))"
        ))
    })
}

struct TableHandler {
    db_key: String,
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
        let db_key = self.db_key.clone();
        TargetActionSink::from_async_fn_with_children_ctx(
            move |host_ctx, actions: Vec<TargetAction<TableAction>>| {
                let db_key = db_key.clone();
                async move {
                    let db = resolve_db(&host_ctx, &db_key)?;
                    let mut out: Vec<Option<ChildTargetDef>> = Vec::with_capacity(actions.len());
                    for action in actions {
                        match action {
                            TargetAction::Create(a) | TargetAction::Update(a) => {
                                let spec = a.spec.ok_or_else(|| {
                                    Error::engine("LanceDB create/update action missing spec")
                                })?;
                                let mut null_backfilled_columns = HashSet::new();
                                if spec.managed_by.is_system() {
                                    null_backfilled_columns =
                                        ensure_table(&db, &spec, a.recreate).await?;
                                }
                                out.push(Some(ChildTargetDef::new::<RowState, _>(
                                    RowHandler::new(db_key.clone(), spec, null_backfilled_columns),
                                )));
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

async fn ensure_table(
    db: &LanceDatabase,
    spec: &TableSpec,
    recreate: bool,
) -> Result<HashSet<String>> {
    let exists = table_exists(db, &spec.table_name).await?;
    if exists && recreate {
        drop_table(db, &spec.table_name).await?;
    } else if exists {
        match evolve_existing_table(db, spec).await? {
            TableEvolution::Done {
                null_backfilled_columns,
            } => return Ok(null_backfilled_columns),
            TableEvolution::Recreate => drop_table(db, &spec.table_name).await?,
        }
    }
    db.conn
        .create_empty_table(&spec.table_name, spec.table_schema.arrow_schema())
        .execute()
        .await
        .map_err(|e| Error::engine(format!("lancedb create table {:?}: {e}", spec.table_name)))?;
    Ok(HashSet::new())
}

enum TableEvolution {
    Done {
        null_backfilled_columns: HashSet<String>,
    },
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
    let mut null_backfilled_columns = HashSet::new();
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
                if def.nullable {
                    null_backfilled_columns.insert(name.clone());
                }
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
    Ok(TableEvolution::Done {
        null_backfilled_columns,
    })
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
    #[serde(default)]
    track_only: bool,
}

/// Number of applied row-batches (transactions) after which the table is
/// optimized (compaction + index maintenance). Mirrors Python's
/// `num_transactions_before_optimize` default of 50 — counted per batch, not per
/// row.
const TRANSACTIONS_BEFORE_OPTIMIZE: u64 = 50;

struct RowHandler {
    db_key: String,
    spec: TableSpec,
    /// Shared across this handler's per-batch sinks: a monotonic count of applied
    /// row-batches so the table is optimized every
    /// [`TRANSACTIONS_BEFORE_OPTIMIZE`] batches (see [`apply_rows`]).
    optimize_counter: Arc<std::sync::atomic::AtomicU64>,
    null_backfilled_columns: HashSet<String>,
}

impl RowHandler {
    fn new(db_key: String, spec: TableSpec, null_backfilled_columns: HashSet<String>) -> Self {
        Self {
            db_key,
            spec,
            optimize_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            null_backfilled_columns,
        }
    }

    fn legacy_fingerprint_for_null_backfill(
        &self,
        desired: &RowState,
    ) -> Result<Option<Fingerprint>> {
        if self.null_backfilled_columns.is_empty() {
            return Ok(None);
        }

        let mut legacy_fields = desired.fields.clone();
        let mut removed_any = false;
        for col_name in &self.null_backfilled_columns {
            match legacy_fields.get(col_name) {
                Some(value) if value.is_null() => {
                    legacy_fields.remove(col_name);
                    removed_any = true;
                }
                Some(_) => return Ok(None),
                None => {}
            }
        }

        if !removed_any {
            return Ok(None);
        }
        Fingerprint::from(&RowState {
            fields: legacy_fields,
        })
        .map(Some)
        .map_err(Error::from)
    }
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
        if !prev_may_be_missing
            && !prev.is_empty()
            && let Some(desired_state) = desired.as_ref()
            && let Some(legacy_fp) = self.legacy_fingerprint_for_null_backfill(desired_state)?
            && prev.iter().any(|p| p == &legacy_fp)
        {
            return Ok(Some(TargetReconcileOutput {
                action: TargetAction::Update(RowAction {
                    pk,
                    state: None,
                    track_only: true,
                }),
                sink: self.row_sink(),
                tracking_record: desired_fp,
                child_invalidation: None,
            }));
        }
        Ok(Some(TargetReconcileOutput {
            action: TargetAction::Update(RowAction {
                pk,
                state: desired,
                track_only: false,
            }),
            sink: self.row_sink(),
            tracking_record: desired_fp,
            child_invalidation: None,
        }))
    }

    /// Vector/FTS indexes attach to the row provider so they share its lifecycle:
    /// a destructive table replace invalidates the row provider and therefore its
    /// index attachments, forcing them to be rebuilt on the recreated table.
    fn attachments(&self) -> Result<Vec<(String, ChildTargetDef)>> {
        Ok(vec![
            (
                "vector_index".to_string(),
                ChildTargetDef::new::<VectorIndexSpec, _>(VectorIndexHandler {
                    db_key: self.db_key.clone(),
                }),
            ),
            (
                "fts_index".to_string(),
                ChildTargetDef::new::<FtsIndexSpec, _>(FtsIndexHandler {
                    db_key: self.db_key.clone(),
                }),
            ),
        ])
    }
}

impl RowHandler {
    fn row_sink(&self) -> TargetActionSink<RowAction> {
        let db_key = self.db_key.clone();
        let spec = self.spec.clone();
        let optimize_counter = self.optimize_counter.clone();
        TargetActionSink::from_async_fn_with_ctx(
            move |host_ctx, actions: Vec<TargetAction<RowAction>>| {
                let db_key = db_key.clone();
                let spec = spec.clone();
                let optimize_counter = optimize_counter.clone();
                async move {
                    let db = resolve_db(&host_ctx, &db_key)?;
                    let mut upserts: Vec<Map<String, JsonValue>> = Vec::new();
                    let mut deletes: Vec<Vec<JsonValue>> = Vec::new();
                    for action in actions {
                        let row = match action {
                            TargetAction::Create(r)
                            | TargetAction::Update(r)
                            | TargetAction::Delete(r) => r,
                        };
                        if row.track_only {
                            continue;
                        }
                        match row.state {
                            Some(state) => upserts.push(state.fields),
                            None => deletes.push(row.pk),
                        }
                    }
                    apply_rows(&db, &spec, upserts, deletes, &optimize_counter).await
                }
            },
        )
    }
}

async fn apply_rows(
    db: &LanceDatabase,
    spec: &TableSpec,
    upserts: Vec<Map<String, JsonValue>>,
    deletes: Vec<Vec<JsonValue>>,
    optimize_counter: &std::sync::atomic::AtomicU64,
) -> Result<()> {
    use std::sync::atomic::Ordering;

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

    // Periodically compact + fold new rows into existing indices. Done inline
    // (rather than Python's debounced background task) every
    // `TRANSACTIONS_BEFORE_OPTIMIZE` batches. The counter is monotonic and the
    // trigger is `count % N == 0`, so concurrent batches each get a distinct
    // count via the atomic `fetch_add` and exactly one in N optimizes — no reset,
    // hence no lost-increment race or double-optimize.
    let count = optimize_counter.fetch_add(1, Ordering::Relaxed) + 1;
    if count % TRANSACTIONS_BEFORE_OPTIMIZE == 0 {
        table
            .optimize(lancedb::table::OptimizeAction::All)
            .await
            .map_err(|e| Error::engine(format!("lancedb optimize: {e}")))?;
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
            ColumnType::Bool => Arc::new(BooleanArray::from(
                values
                    .map(|v| nullable_value(name, def, v).map(JsonValue::as_bool))
                    .collect::<Result<Vec<_>>>()?,
            )),
            ColumnType::Int16 => Arc::new(Int16Array::from(
                values
                    .map(|v| {
                        nullable_value(name, def, v)
                            .map(|v| v.as_i64().and_then(|n| i16::try_from(n).ok()))
                    })
                    .collect::<Result<Vec<_>>>()?,
            )),
            ColumnType::Int32 => Arc::new(Int32Array::from(
                values
                    .map(|v| {
                        nullable_value(name, def, v)
                            .map(|v| v.as_i64().and_then(|n| i32::try_from(n).ok()))
                    })
                    .collect::<Result<Vec<_>>>()?,
            )),
            ColumnType::Int64 => Arc::new(Int64Array::from(
                values
                    .map(|v| nullable_value(name, def, v).map(|v| v.as_i64()))
                    .collect::<Result<Vec<_>>>()?,
            )),
            ColumnType::Float32 => Arc::new(Float32Array::from(
                values
                    .map(|v| nullable_value(name, def, v).map(|v| v.as_f64().map(|n| n as f32)))
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
            ColumnType::Binary => {
                let bytes = values
                    .map(|v| {
                        let value = nullable_value(name, def, v)?;
                        if value.is_null() {
                            return Ok(None);
                        }
                        let values = value.as_array().ok_or_else(|| {
                            Error::engine(format!("column {name:?} must be a byte array"))
                        })?;
                        values
                            .iter()
                            .map(|value| {
                                value
                                    .as_u64()
                                    .and_then(|n| u8::try_from(n).ok())
                                    .ok_or_else(|| {
                                        Error::engine(format!(
                                            "column {name:?} has a non-byte element"
                                        ))
                                    })
                            })
                            .collect::<Result<Vec<_>>>()
                            .map(Some)
                    })
                    .collect::<Result<Vec<_>>>()?;
                Arc::new(BinaryArray::from_iter(
                    bytes.iter().map(|value| value.as_deref()),
                ))
            }
            ColumnType::Json => Arc::new(StringArray::from(
                values
                    .map(|v| {
                        let value = nullable_value(name, def, v)?;
                        if value.is_null() {
                            Ok(None)
                        } else {
                            serde_json::to_string(value).map(Some).map_err(|e| {
                                Error::engine(format!("encode LanceDB JSON field {name:?}: {e}"))
                            })
                        }
                    })
                    .collect::<Result<Vec<_>>>()?,
            )),
            ColumnType::Vector(dim) => build_vector_array::<Float32Type>(
                name,
                *dim,
                def.nullable,
                DataType::Float32,
                values,
                |value| value,
            )?,
            ColumnType::HalfVector(dim) => build_vector_array::<Float16Type>(
                name,
                *dim,
                def.nullable,
                DataType::Float16,
                values,
                f16::from_f32,
            )?,
        };
        arrays.push(array);
    }
    RecordBatch::try_new(arrow_schema.clone(), arrays)
        .map_err(|e| Error::engine(format!("build LanceDB record batch: {e}")))
}

fn build_vector_array<'a, T: ArrowPrimitiveType>(
    column: &str,
    dim: usize,
    nullable: bool,
    item_type: DataType,
    values: impl Iterator<Item = &'a JsonValue>,
    convert: impl Fn(f32) -> T::Native,
) -> Result<ArrayRef> {
    let mut builder = FixedSizeListBuilder::new(PrimitiveBuilder::<T>::new(), dim as i32)
        .with_field(Arc::new(Field::new("item", item_type, true)));
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
            builder.values().append_value(convert(f as f32));
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

// ---------------------------------------------------------------------------
// Vector / FTS index attachments
// ---------------------------------------------------------------------------

/// Options for [`LanceTableTarget::declare_vector_index`].
#[derive(Clone, Debug)]
pub struct VectorIndexOptions {
    /// Index name; defaults to `<column>_idx`.
    pub name: Option<String>,
    /// Distance metric: `"cosine"`, `"l2"`, or `"dot"`.
    pub metric: &'static str,
    /// Index type: `"ivf_pq"` or `"ivf_hnsw_pq"`.
    pub index_type: &'static str,
    /// IVF: number of partitions (clusters). `None` lets LanceDB choose.
    pub num_partitions: Option<u32>,
    /// PQ: number of sub-vectors. `None` lets LanceDB choose from the dimension.
    pub num_sub_vectors: Option<u32>,
    /// PQ: number of bits per sub-vector. `None` uses the LanceDB default.
    pub num_bits: Option<u32>,
    /// HNSW (`ivf_hnsw_pq` only): number of edges per node. `None` uses default.
    pub m: Option<u32>,
    /// HNSW (`ivf_hnsw_pq` only): construction-time search width. `None` default.
    pub ef_construction: Option<u32>,
}

impl Default for VectorIndexOptions {
    fn default() -> Self {
        Self {
            name: None,
            metric: "cosine",
            index_type: "ivf_pq",
            num_partitions: None,
            num_sub_vectors: None,
            num_bits: None,
            m: None,
            ef_construction: None,
        }
    }
}

/// Options for [`LanceTableTarget::declare_fts_index`].
#[derive(Clone, Debug, Default)]
pub struct FtsIndexOptions {
    /// Index name; defaults to `<column>_fts_idx`.
    pub name: Option<String>,
    /// Record token positions (enables phrase queries) at the cost of a larger
    /// index. Defaults to `false`.
    pub with_position: bool,
    /// Stemming/stop-word language (e.g. `"English"`, `"French"`). `None` uses
    /// LanceDB's default (English), matching the Python connector.
    pub language: Option<String>,
}

/// Spec for a LanceDB vector index (an attachment of a table). Used as both the
/// declared value and the tracking record (equality = no change).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorIndexSpec {
    table_name: String,
    name: String,
    column: String,
    metric: String,
    index_type: String,
    num_partitions: Option<u32>,
    num_sub_vectors: Option<u32>,
    num_bits: Option<u32>,
    m: Option<u32>,
    ef_construction: Option<u32>,
    #[serde(default)]
    managed_by: ManagedBy,
}

/// Spec for a LanceDB full-text-search index (an attachment of a table).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FtsIndexSpec {
    table_name: String,
    name: String,
    column: String,
    with_position: bool,
    #[serde(default)]
    language: Option<String>,
    #[serde(default)]
    managed_by: ManagedBy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct VectorIndexAction {
    /// `Some` to (re)create the index, `None` to drop it.
    spec: Option<VectorIndexSpec>,
    /// Index name, retained for the drop path when `spec` is `None`.
    name: String,
    table_name: String,
    /// Retained (from `prev` on the drop path) so a user-managed index is never
    /// dropped by CocoIndex — it doesn't own that DDL.
    managed_by: ManagedBy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FtsIndexAction {
    spec: Option<FtsIndexSpec>,
    name: String,
    table_name: String,
    /// See [`VectorIndexAction::managed_by`].
    managed_by: ManagedBy,
}

struct VectorIndexHandler {
    db_key: String,
}

impl TargetHandler<VectorIndexSpec> for VectorIndexHandler {
    type TrackingRecord = VectorIndexSpec;
    type Action = VectorIndexAction;

    fn reconcile(
        &self,
        _key: StableKey,
        desired: Option<VectorIndexSpec>,
        prev: Vec<VectorIndexSpec>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<VectorIndexAction, VectorIndexSpec>>> {
        let prev_same = desired
            .as_ref()
            .is_some_and(|d| prev.iter().any(|p| p == d));
        if desired.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }
        let (name, table_name, managed_by) = desired
            .as_ref()
            .map(|s| (s.name.clone(), s.table_name.clone(), s.managed_by))
            .or_else(|| {
                prev.first()
                    .map(|p| (p.name.clone(), p.table_name.clone(), p.managed_by))
            })
            .unwrap_or_default();
        Ok(Some(TargetReconcileOutput {
            action: TargetAction::Update(VectorIndexAction {
                spec: desired.clone(),
                name,
                table_name,
                managed_by,
            }),
            sink: self.sink(),
            tracking_record: desired,
            child_invalidation: None,
        }))
    }
}

impl VectorIndexHandler {
    fn sink(&self) -> TargetActionSink<VectorIndexAction> {
        let db_key = self.db_key.clone();
        TargetActionSink::from_async_fn_with_ctx(
            move |host_ctx, actions: Vec<TargetAction<VectorIndexAction>>| {
                let db_key = db_key.clone();
                async move {
                    let db = resolve_db(&host_ctx, &db_key)?;
                    for action in actions {
                        let action = match action {
                            TargetAction::Create(a)
                            | TargetAction::Update(a)
                            | TargetAction::Delete(a) => a,
                        };
                        apply_vector_index(&db, action).await?;
                    }
                    Ok(())
                }
            },
        )
    }
}

struct FtsIndexHandler {
    db_key: String,
}

impl TargetHandler<FtsIndexSpec> for FtsIndexHandler {
    type TrackingRecord = FtsIndexSpec;
    type Action = FtsIndexAction;

    fn reconcile(
        &self,
        _key: StableKey,
        desired: Option<FtsIndexSpec>,
        prev: Vec<FtsIndexSpec>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<FtsIndexAction, FtsIndexSpec>>> {
        let prev_same = desired
            .as_ref()
            .is_some_and(|d| prev.iter().any(|p| p == d));
        if desired.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }
        let (name, table_name, managed_by) = desired
            .as_ref()
            .map(|s| (s.name.clone(), s.table_name.clone(), s.managed_by))
            .or_else(|| {
                prev.first()
                    .map(|p| (p.name.clone(), p.table_name.clone(), p.managed_by))
            })
            .unwrap_or_default();
        Ok(Some(TargetReconcileOutput {
            action: TargetAction::Update(FtsIndexAction {
                spec: desired.clone(),
                name,
                table_name,
                managed_by,
            }),
            sink: self.sink(),
            tracking_record: desired,
            child_invalidation: None,
        }))
    }
}

impl FtsIndexHandler {
    fn sink(&self) -> TargetActionSink<FtsIndexAction> {
        let db_key = self.db_key.clone();
        TargetActionSink::from_async_fn_with_ctx(
            move |host_ctx, actions: Vec<TargetAction<FtsIndexAction>>| {
                let db_key = db_key.clone();
                async move {
                    let db = resolve_db(&host_ctx, &db_key)?;
                    for action in actions {
                        let action = match action {
                            TargetAction::Create(a)
                            | TargetAction::Update(a)
                            | TargetAction::Delete(a) => a,
                        };
                        apply_fts_index(&db, action).await?;
                    }
                    Ok(())
                }
            },
        )
    }
}

fn lance_distance_type(metric: &str) -> Result<lancedb::DistanceType> {
    match metric.to_ascii_lowercase().as_str() {
        "cosine" => Ok(lancedb::DistanceType::Cosine),
        "l2" | "euclidean" => Ok(lancedb::DistanceType::L2),
        "dot" | "ip" => Ok(lancedb::DistanceType::Dot),
        other => Err(Error::engine(format!(
            "unsupported LanceDB vector metric {other:?} (expected cosine, l2, or dot)"
        ))),
    }
}

/// Open the table and drop `name` if it currently exists (LanceDB errors when
/// dropping an index that isn't there).
async fn drop_index_if_exists(table: &lancedb::table::Table, name: &str) -> Result<()> {
    let indices = table
        .list_indices()
        .await
        .map_err(|e| Error::engine(format!("lancedb list_indices: {e}")))?;
    if indices.iter().any(|i| i.name == name) {
        table
            .drop_index(name)
            .await
            .map_err(|e| Error::engine(format!("lancedb drop_index {name:?}: {e}")))?;
    }
    Ok(())
}

async fn apply_vector_index(db: &LanceDatabase, action: VectorIndexAction) -> Result<()> {
    use lancedb::index::Index;
    use lancedb::index::vector::{IvfHnswPqIndexBuilder, IvfPqIndexBuilder};

    // User-managed indexes: CocoIndex owns neither their creation nor their
    // drop. Guard on the action's `managed_by` (carried from `prev` on the drop
    // path) so an undeclared user-managed index is not dropped.
    if action.managed_by.is_user() {
        return Ok(());
    }
    if !table_exists(db, &action.table_name).await? {
        return Ok(());
    }
    let table = db
        .conn
        .open_table(&action.table_name)
        .execute()
        .await
        .map_err(|e| Error::engine(format!("lancedb open table for vector index: {e}")))?;

    let Some(spec) = action.spec else {
        return drop_index_if_exists(&table, &action.name).await;
    };

    let distance = lance_distance_type(&spec.metric)?;
    let index = match spec.index_type.as_str() {
        "ivf_pq" => {
            let mut b = IvfPqIndexBuilder::default().distance_type(distance);
            if let Some(v) = spec.num_partitions {
                b = b.num_partitions(v);
            }
            if let Some(v) = spec.num_sub_vectors {
                b = b.num_sub_vectors(v);
            }
            if let Some(v) = spec.num_bits {
                b = b.num_bits(v);
            }
            Index::IvfPq(b)
        }
        "ivf_hnsw_pq" => {
            let mut b = IvfHnswPqIndexBuilder::default().distance_type(distance);
            if let Some(v) = spec.num_partitions {
                b = b.num_partitions(v);
            }
            if let Some(v) = spec.num_sub_vectors {
                b = b.num_sub_vectors(v);
            }
            if let Some(v) = spec.num_bits {
                b = b.num_bits(v);
            }
            if let Some(v) = spec.m {
                b = b.num_edges(v);
            }
            if let Some(v) = spec.ef_construction {
                b = b.ef_construction(v);
            }
            Index::IvfHnswPq(b)
        }
        other => {
            return Err(Error::engine(format!(
                "unsupported LanceDB vector index type {other:?} (expected ivf_pq or ivf_hnsw_pq)"
            )));
        }
    };
    table
        .create_index(&[spec.column.as_str()], index)
        .name(spec.name.clone())
        .replace(true)
        .execute()
        .await
        .map_err(|e| Error::engine(format!("lancedb create vector index {:?}: {e}", spec.name)))
}

async fn apply_fts_index(db: &LanceDatabase, action: FtsIndexAction) -> Result<()> {
    use lancedb::index::Index;
    use lancedb::index::scalar::FtsIndexBuilder;

    // See `apply_vector_index`: user-managed indexes are never created or dropped
    // by CocoIndex; guard on the action's `managed_by` so the drop path respects it.
    if action.managed_by.is_user() {
        return Ok(());
    }
    if !table_exists(db, &action.table_name).await? {
        return Ok(());
    }
    let table = db
        .conn
        .open_table(&action.table_name)
        .execute()
        .await
        .map_err(|e| Error::engine(format!("lancedb open table for fts index: {e}")))?;

    let Some(spec) = action.spec else {
        return drop_index_if_exists(&table, &action.name).await;
    };

    let mut params = FtsIndexBuilder::default().with_position(spec.with_position);
    if let Some(language) = &spec.language {
        params = params
            .language(language)
            .map_err(|e| Error::engine(format!("lancedb fts index language {language:?}: {e}")))?;
    }
    table
        .create_index(&[spec.column.as_str()], Index::FTS(params))
        .name(spec.name.clone())
        .replace(true)
        .execute()
        .await
        .map_err(|e| Error::engine(format!("lancedb create fts index {:?}: {e}", spec.name)))
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

    fn row_state_from_fields(
        fields: impl IntoIterator<Item = (&'static str, JsonValue)>,
    ) -> RowState {
        RowState {
            fields: fields
                .into_iter()
                .map(|(name, value)| (name.to_string(), value))
                .collect(),
        }
    }

    fn row_handler(null_backfilled_columns: impl IntoIterator<Item = &'static str>) -> RowHandler {
        let table_schema = TableSchema::new(
            [
                ("id", ColumnDef::new(ColumnType::Int64)),
                ("name", ColumnDef::new(ColumnType::Text)),
                ("summary", ColumnDef::new(ColumnType::Text).nullable()),
                ("score", ColumnDef::new(ColumnType::Float64)),
            ],
            ["id"],
        )
        .unwrap();
        RowHandler::new(
            "db".to_string(),
            TableSpec {
                table_name: "docs".to_string(),
                table_schema,
                managed_by: ManagedBy::System,
            },
            null_backfilled_columns
                .into_iter()
                .map(str::to_string)
                .collect(),
        )
    }

    #[test]
    fn row_reconcile_tracks_only_nulls_from_new_nullable_columns() {
        let handler = row_handler(["summary"]);
        let old_row = row_state_from_fields([
            ("id", JsonValue::from(1)),
            ("name", JsonValue::from("alpha")),
        ]);
        let new_row = row_state_from_fields([
            ("id", JsonValue::from(1)),
            ("name", JsonValue::from("alpha")),
            ("summary", JsonValue::Null),
        ]);

        let result = handler
            .reconcile(
                StableKey::Int(1),
                Some(new_row.clone()),
                vec![Fingerprint::from(&old_row).unwrap()],
                false,
            )
            .unwrap()
            .unwrap();

        match result.action {
            TargetAction::Update(action) => {
                assert!(action.track_only);
                assert!(action.state.is_none());
            }
            other => panic!("expected update action, got {other:?}"),
        }
        assert_eq!(
            result.tracking_record,
            Some(Fingerprint::from(&new_row).unwrap())
        );
    }

    #[test]
    fn row_reconcile_upserts_non_null_value_for_new_nullable_column() {
        let handler = row_handler(["summary"]);
        let old_row = row_state_from_fields([
            ("id", JsonValue::from(1)),
            ("name", JsonValue::from("alpha")),
        ]);
        let new_row = row_state_from_fields([
            ("id", JsonValue::from(1)),
            ("name", JsonValue::from("alpha")),
            ("summary", JsonValue::from("new")),
        ]);

        let result = handler
            .reconcile(
                StableKey::Int(1),
                Some(new_row.clone()),
                vec![Fingerprint::from(&old_row).unwrap()],
                false,
            )
            .unwrap()
            .unwrap();

        match result.action {
            TargetAction::Update(action) => {
                assert!(!action.track_only);
                assert_eq!(action.state, Some(new_row));
            }
            other => panic!("expected update action, got {other:?}"),
        }
    }

    #[test]
    fn row_reconcile_upserts_existing_column_change_with_new_null_column() {
        let handler = row_handler(["summary"]);
        let old_row = row_state_from_fields([
            ("id", JsonValue::from(1)),
            ("name", JsonValue::from("alpha")),
        ]);
        let new_row = row_state_from_fields([
            ("id", JsonValue::from(1)),
            ("name", JsonValue::from("alpha-updated")),
            ("summary", JsonValue::Null),
        ]);

        let result = handler
            .reconcile(
                StableKey::Int(1),
                Some(new_row.clone()),
                vec![Fingerprint::from(&old_row).unwrap()],
                false,
            )
            .unwrap()
            .unwrap();

        match result.action {
            TargetAction::Update(action) => {
                assert!(!action.track_only);
                assert_eq!(action.state, Some(new_row));
            }
            other => panic!("expected update action, got {other:?}"),
        }
    }

    #[test]
    fn row_reconcile_upserts_when_previous_row_may_be_missing() {
        let handler = row_handler(["summary"]);
        let old_row = row_state_from_fields([
            ("id", JsonValue::from(1)),
            ("name", JsonValue::from("alpha")),
        ]);
        let new_row = row_state_from_fields([
            ("id", JsonValue::from(1)),
            ("name", JsonValue::from("alpha")),
            ("summary", JsonValue::Null),
        ]);

        let result = handler
            .reconcile(
                StableKey::Int(1),
                Some(new_row.clone()),
                vec![Fingerprint::from(&old_row).unwrap()],
                true,
            )
            .unwrap()
            .unwrap();

        match result.action {
            TargetAction::Update(action) => {
                assert!(!action.track_only);
                assert_eq!(action.state, Some(new_row));
            }
            other => panic!("expected update action, got {other:?}"),
        }
    }

    #[test]
    fn row_reconcile_upserts_non_nullable_added_column() {
        let handler = row_handler([]);
        let old_row = row_state_from_fields([
            ("id", JsonValue::from(1)),
            ("name", JsonValue::from("alpha")),
        ]);
        let new_row = row_state_from_fields([
            ("id", JsonValue::from(1)),
            ("name", JsonValue::from("alpha")),
            ("score", JsonValue::from(1.0)),
        ]);

        let result = handler
            .reconcile(
                StableKey::Int(1),
                Some(new_row.clone()),
                vec![Fingerprint::from(&old_row).unwrap()],
                false,
            )
            .unwrap()
            .unwrap();

        match result.action {
            TargetAction::Update(action) => {
                assert!(!action.track_only);
                assert_eq!(action.state, Some(new_row));
            }
            other => panic!("expected update action, got {other:?}"),
        }
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
