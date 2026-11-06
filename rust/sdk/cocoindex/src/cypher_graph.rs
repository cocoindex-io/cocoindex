use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

use crate::ctx::{ContextKey, ContextStore, Ctx};
use crate::error::{Error, Result};
use crate::row_schema::{LogicalType, SchemaFields};
use crate::statediff::{
    ManagedBy, ManagedTargetOptions, MutualTrackingRecord, diff, resolve_system_transition,
};
use crate::target_state::{
    ChildTargetDef, IntoStableKey, SinkCache, StableKey, StaticRootProvider, TargetAction,
    TargetActionSink, TargetHandler, TargetReconcileOutput, TargetState, TargetStateProvider,
    declare_target_state, declare_target_state_with_child, mount_target,
};

/// Named parameters bound to a parameterized Cypher statement. Values are JSON
/// (the connector maps them to Bolt types / FalkorDB `CYPHER` header literals).
pub(crate) type CypherParams = BTreeMap<String, JsonValue>;

/// A data-bearing Cypher statement with its bound parameters.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CypherStatement {
    pub(crate) cypher: String,
    pub(crate) params: CypherParams,
}

/// One transaction applies at most this many record actions (Python's
/// `_MAX_ACTIONS_PER_TXN`): a batch is otherwise unbounded. A chunk can still
/// go over the server's transaction memory limit (large properties); it is
/// then split in two.
const MAX_ACTIONS_PER_TXN: usize = 1000;

#[async_trait]
pub(crate) trait CypherExecutor: Clone + Send + Sync + 'static {
    fn dialect(&self) -> &'static str;

    /// The connector's static root provider for tables and relations, built
    /// with [`table_root_provider`] for this executor type.
    fn table_provider() -> &'static StaticRootProvider;

    /// Execute a statement with no bound parameters (DDL: indexes/constraints,
    /// built only from validated identifiers).
    async fn execute(&self, cypher: &str) -> Result<()>;

    /// Execute a data-bearing statement with bound parameters. Record writes go
    /// through here so user values are never interpolated into the query body
    /// (Neo4j binds Bolt params; FalkorDB uses the `CYPHER name=value` header).
    async fn execute_with_params(&self, cypher: &str, params: &CypherParams) -> Result<()>;

    /// Execute data-bearing statements as one unit. Neo4j runs them in one
    /// transaction; the default runs them one after the other, for a dialect
    /// with no multi-statement transaction (FalkorDB).
    async fn execute_batch(&self, statements: &[CypherStatement]) -> Result<()> {
        for statement in statements {
            self.execute_with_params(&statement.cypher, &statement.params)
                .await?;
        }
        Ok(())
    }

    /// Whether `error` reports a transaction over the server's memory limit:
    /// the same statements fail the same way again, so the chunk they apply
    /// is split in two instead of retried (Python's `_is_memory_limit`).
    fn is_transaction_too_large(&self, _error: &Error) -> bool {
        false
    }

    async fn execute_unique_constraint(
        &self,
        _op: &str,
        _entity_kind: &str,
        _label: &str,
        _field: &str,
    ) -> Result<()> {
        Ok(())
    }

    /// Create (or replace) a vector index on `label`.`field`. The Cypher differs
    /// by dialect (resolved via [`dialect`](Self::dialect)); the default covers
    /// Neo4j (`CREATE VECTOR INDEX … OPTIONS {indexConfig: …}`, named) and
    /// FalkorDB (`CREATE VECTOR INDEX FOR … OPTIONS {dimension, similarityFunction}`).
    async fn create_vector_index(
        &self,
        label: &str,
        field: &str,
        dimension: u32,
        metric: VectorMetric,
    ) -> Result<()> {
        let cypher = match self.dialect() {
            "neo4j" => format!(
                "CREATE VECTOR INDEX `{name}` IF NOT EXISTS FOR (n:`{label}`) ON n.`{field}` \
                 OPTIONS {{ indexConfig: {{ `vector.dimensions`: {dimension}, \
                 `vector.similarity_function`: '{metric}' }} }}",
                name = vector_index_name(label, field),
                metric = metric.neo4j()?,
            ),
            "falkordb" => format!(
                "CREATE VECTOR INDEX FOR (e:`{label}`) ON (e.`{field}`) \
                 OPTIONS {{dimension: {dimension}, similarityFunction: '{}'}}",
                metric.falkordb(),
            ),
            other => {
                return Err(Error::engine(format!(
                    "vector index not supported for dialect {other:?}"
                )));
            }
        };
        self.execute(&cypher).await
    }

    /// Drop the vector index on `label`.`field` if it exists (Neo4j drops by
    /// name, FalkorDB by `(label, field)`).
    async fn drop_vector_index(&self, label: &str, field: &str) -> Result<()> {
        let cypher = match self.dialect() {
            "neo4j" => format!("DROP INDEX `{}` IF EXISTS", vector_index_name(label, field)),
            "falkordb" => format!("DROP VECTOR INDEX FOR (e:`{label}`) ON (e.`{field}`)"),
            other => {
                return Err(Error::engine(format!(
                    "vector index not supported for dialect {other:?}"
                )));
            }
        };
        ignore_missing_index(self.execute(&cypher).await)
    }

    /// Create a (non-vector) node-property index on `label` over `fields`. Neo4j
    /// names the index; FalkorDB identifies it by `(label, fields)`.
    async fn create_node_index(&self, label: &str, fields: &[String]) -> Result<()> {
        let on_neo4j = fields
            .iter()
            .map(|f| format!("n.`{f}`"))
            .collect::<Vec<_>>()
            .join(", ");
        let on_falkor = fields
            .iter()
            .map(|f| format!("e.`{f}`"))
            .collect::<Vec<_>>()
            .join(", ");
        let cypher = match self.dialect() {
            "neo4j" => format!(
                "CREATE INDEX `{}` IF NOT EXISTS FOR (n:`{label}`) ON ({on_neo4j})",
                node_index_name(label, fields),
            ),
            "falkordb" => format!("CREATE INDEX FOR (e:`{label}`) ON ({on_falkor})"),
            other => {
                return Err(Error::engine(format!(
                    "node index not supported for dialect {other:?}"
                )));
            }
        };
        self.execute(&cypher).await
    }

    /// Drop the node-property index on `label` over `fields` if it exists.
    async fn drop_node_index(&self, label: &str, fields: &[String]) -> Result<()> {
        let on_falkor = fields
            .iter()
            .map(|f| format!("e.`{f}`"))
            .collect::<Vec<_>>()
            .join(", ");
        let cypher = match self.dialect() {
            "neo4j" => format!("DROP INDEX `{}` IF EXISTS", node_index_name(label, fields)),
            "falkordb" => format!("DROP INDEX FOR (e:`{label}`) ON ({on_falkor})"),
            other => {
                return Err(Error::engine(format!(
                    "node index not supported for dialect {other:?}"
                )));
            }
        };
        ignore_missing_index(self.execute(&cypher).await)
    }
}

/// Treat dropping an index that doesn't exist as done: FalkorDB has no
/// `DROP ... IF EXISTS` and reports `no such index` instead (Neo4j's drops use
/// `IF EXISTS`).
fn ignore_missing_index(result: Result<()>) -> Result<()> {
    match result {
        Err(e) if e.to_string().contains("no such index") => Ok(()),
        result => result,
    }
}

/// Distance metric for a graph vector index.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum VectorMetric {
    Cosine,
    Euclidean,
    /// Inner product (FalkorDB only).
    InnerProduct,
}

impl VectorMetric {
    fn neo4j(self) -> Result<&'static str> {
        match self {
            VectorMetric::Cosine => Ok("cosine"),
            VectorMetric::Euclidean => Ok("euclidean"),
            VectorMetric::InnerProduct => Err(Error::engine(
                "Neo4j vector index does not support the inner-product metric",
            )),
        }
    }

    fn falkordb(self) -> &'static str {
        match self {
            VectorMetric::Cosine => "cosine",
            VectorMetric::Euclidean => "euclidean",
            VectorMetric::InnerProduct => "ip",
        }
    }
}

/// Deterministic name of the database's vector index for a `(label, field)`
/// pair (Neo4j names its indexes; FalkorDB identifies them by `(label, field)`
/// instead). The declared name of an index (see
/// [`TableTarget::declare_vector_index`]) only identifies its target state.
pub(crate) fn vector_index_name(label: &str, field: &str) -> String {
    format!("coco_vec_{label}__{field}")
}

/// The declared name a vector index gets when the caller gives none, as in
/// Python's `declare_vector_index(name=None)`.
fn default_vector_index_name(label: &str, field: &str) -> String {
    format!("vec_{label}__{field}")
}

/// Deterministic node-index name for a `(label, fields)` pair. Fields are joined
/// with `__` to match Python's `index_name` (`"__".join(fields)`), so both SDKs
/// mint the same multi-field index name.
pub(crate) fn node_index_name(label: &str, fields: &[String]) -> String {
    format!("coco_idx_node_{label}__{}", fields.join("__"))
}

/// A property of a graph table. The type is metadata only: the graph enforces
/// no property types, so it contributes to the schema's tracking record (a
/// change re-upserts the records) and no DDL is emitted from it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnDef {
    pub cypher_type: String,
    pub nullable: bool,
}

impl ColumnDef {
    pub fn new(cypher_type: impl Into<String>) -> Self {
        Self {
            cypher_type: cypher_type.into(),
            nullable: false,
        }
    }

    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }
}

/// Schema of a graph table: a node table (label) or a relation table
/// (relationship type). The primary key is one field.
///
/// Build it from explicit columns ([`new`](Self::new)), from a row struct
/// ([`from_row`](Self::from_row)), or with no declared columns
/// ([`schemaless`](Self::schemaless)).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSchema {
    /// `None` for a schemaless table.
    columns: Option<BTreeMap<String, ColumnDef>>,
    primary_key: String,
}

impl TableSchema {
    /// A schema with explicit columns. `primary_key` must name one of them.
    pub fn new(
        columns: impl IntoIterator<Item = (impl Into<String>, ColumnDef)>,
        primary_key: impl Into<String>,
    ) -> Result<Self> {
        let primary_key = primary_key.into();
        validate_ident(&primary_key, "primary key")?;
        let mut out = BTreeMap::new();
        for (name, def) in columns {
            let name = name.into();
            validate_ident(&name, "column name")?;
            out.insert(name, def);
        }
        if !out.contains_key(&primary_key) {
            return Err(Error::engine(format!(
                "primary_key {primary_key:?} not found in columns"
            )));
        }
        Ok(Self {
            columns: Some(out),
            primary_key,
        })
    }

    /// Derive a schema from a `#[derive(SchemaFields)]` row type (the Rust
    /// analogue of Python's `TableSchema.from_class`). Each field maps to a
    /// Cypher property type as in Python's `from_class`; a `#[coco(vector)]`
    /// field becomes `LIST<FLOAT>` (its dimension belongs to the vector index,
    /// not the property), `#[coco(type = "…")]` is used verbatim, and a
    /// collection, map or nested struct is recorded as `MAP`.
    ///
    /// ```ignore
    /// #[derive(serde::Serialize, cocoindex::SchemaFields)]
    /// struct Person {
    ///     id: String,
    ///     name: Option<String>,
    ///     #[coco(vector)]
    ///     embedding: Vec<f32>,
    /// }
    /// let schema = cocoindex::connectors::neo4j::TableSchema::from_row::<Person>("id")?;
    /// ```
    pub fn from_row<T: SchemaFields>(primary_key: impl Into<String>) -> Result<Self> {
        Self::new(
            T::schema_fields().into_iter().map(|field| {
                let def = ColumnDef {
                    cypher_type: cypher_type(field.logical_type),
                    nullable: field.nullable,
                };
                (field.name, def)
            }),
            primary_key,
        )
    }

    /// Replace the Cypher property type of one column of a schema derived
    /// from a row (Python's `from_class(column_overrides={"col": …})`),
    /// keeping its nullability.
    pub fn with_column_type(
        mut self,
        field_name: &str,
        cypher_type: impl Into<String>,
    ) -> Result<Self> {
        let def = self
            .columns
            .as_mut()
            .and_then(|columns| columns.get_mut(field_name))
            .ok_or_else(|| crate::row_schema::unknown_column_error("graph", field_name))?;
        def.cypher_type = cypher_type.into();
        Ok(self)
    }

    /// A schema with no declared columns (Python's `table_schema=None`): the
    /// table's records are keyed by the `primary_key` property and written as
    /// they serialize.
    pub fn schemaless(primary_key: impl Into<String>) -> Result<Self> {
        let primary_key = primary_key.into();
        validate_ident(&primary_key, "primary key")?;
        Ok(Self {
            columns: None,
            primary_key,
        })
    }
}

/// The Cypher property type of a derived column (Python's
/// `_LEAF_TYPE_MAPPINGS` for the graph targets).
fn cypher_type(logical_type: LogicalType) -> String {
    let name = match logical_type {
        LogicalType::Bool => "BOOLEAN",
        LogicalType::Int16 | LogicalType::Int32 | LogicalType::Int64 => "INTEGER",
        LogicalType::Float32 | LogicalType::Float64 => "FLOAT",
        // The graph has no decimal or UUID type: both are stored as strings.
        LogicalType::Decimal | LogicalType::Text | LogicalType::Uuid => "STRING",
        LogicalType::Bytes => "BYTES",
        LogicalType::Date => "DATE",
        LogicalType::Time => "LOCAL_TIME",
        LogicalType::DateTime => "ZONED_DATETIME",
        LogicalType::Duration => "DURATION",
        LogicalType::Json => "MAP",
        LogicalType::Vector { .. } => "LIST<FLOAT>",
        LogicalType::Custom(cypher_type) => return cypher_type,
    };
    name.to_string()
}

#[derive(Clone)]
pub(crate) struct TableTarget {
    /// The `ContextKey` name of the graph the table lives in.
    graph_key: Arc<str>,
    table_name: Arc<str>,
    primary_key: Arc<str>,
    /// The connector's static root provider; carries the index attachments.
    table_provider: TargetStateProvider<TableSpec>,
    provider: TargetStateProvider<RecordState>,
    /// Field -> declared name of the vector indexes declared through this
    /// target: the database keeps one vector index per label and field.
    vector_index_names: Arc<Mutex<BTreeMap<String, String>>>,
}

impl TableTarget {
    pub(crate) fn table_name(&self) -> &str {
        &self.table_name
    }

    pub(crate) fn primary_key(&self) -> &str {
        &self.primary_key
    }

    pub(crate) fn declare_record<R: Serialize>(
        &self,
        ctx: &Ctx,
        id: impl IntoStableKey,
        row: &R,
    ) -> Result<()> {
        declare_target_state(ctx, self.provider.target_state(id, record_state(row)?))
    }

    /// Declare a vector index on `field` as an attachment of this node table.
    /// `name` identifies the index's target state (default
    /// `vec_{table}__{field}`); the database's own index is identified by the
    /// label and field, so one field takes one vector index.
    pub(crate) fn declare_vector_index(
        &self,
        ctx: &Ctx,
        name: Option<&str>,
        field: &str,
        dimension: u32,
        metric: VectorMetric,
    ) -> Result<()> {
        validate_ident(field, "vector index field")?;
        let name = match name {
            Some(name) => {
                validate_ident(name, "vector index name")?;
                name.to_string()
            }
            None => default_vector_index_name(&self.table_name, field),
        };
        if dimension == 0 {
            return Err(Error::engine(format!(
                "Invalid vector dimension: {dimension}"
            )));
        }
        {
            let mut names = self
                .vector_index_names
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let declared = names
                .entry(field.to_string())
                .or_insert_with(|| name.clone());
            if *declared != name {
                return Err(Error::engine(format!(
                    "Vector indexes {declared:?} and {name:?} both index {}.{field}: declare \
                     one vector index per field.",
                    self.table_name
                )));
            }
        }
        let provider: TargetStateProvider<VectorIndexSpec> =
            self.table_provider.attachment(ctx, "vector_index")?;
        let spec = VectorIndexSpec {
            label: self.table_name.to_string(),
            field: field.to_string(),
            metric,
            dimension,
        };
        declare_target_state(
            ctx,
            provider.target_state(
                vector_index_key(&self.graph_key, &self.table_name, &name),
                spec,
            ),
        )
    }

    /// Declare a (non-vector) secondary index on `fields` for this node table.
    pub(crate) fn declare_node_index(&self, ctx: &Ctx, fields: &[&str]) -> Result<()> {
        if fields.is_empty() {
            return Err(Error::engine("node index requires at least one field"));
        }
        let fields: Vec<String> = fields
            .iter()
            .map(|f| {
                validate_ident(f, "node index field")?;
                Ok((*f).to_string())
            })
            .collect::<Result<Vec<_>>>()?;
        let provider: TargetStateProvider<NodeIndexSpec> =
            self.table_provider.attachment(ctx, "node_index")?;
        let key = node_index_name(&self.table_name, &fields);
        let spec = NodeIndexSpec {
            label: self.table_name.to_string(),
            fields,
        };
        declare_target_state(
            ctx,
            provider.target_state(index_key(&self.graph_key, key), spec),
        )
    }
}

#[derive(Clone)]
pub(crate) struct RelationTarget {
    /// The property that keys the relation's records.
    primary_key: Arc<str>,
    from_table: TableEndpoint,
    to_table: TableEndpoint,
    provider: TargetStateProvider<RecordState>,
}

impl RelationTarget {
    pub(crate) fn declare_relation(
        &self,
        ctx: &Ctx,
        from_id: impl IntoStableKey,
        to_id: impl IntoStableKey,
    ) -> Result<()> {
        self.declare_relation_record(ctx, from_id, to_id, &JsonValue::Object(Map::new()))
    }

    pub(crate) fn declare_relation_record<R: Serialize>(
        &self,
        ctx: &Ctx,
        from_id: impl IntoStableKey,
        to_id: impl IntoStableKey,
        record: &R,
    ) -> Result<()> {
        let from_id = key_value(from_id.into_stable_key())?;
        let to_id = key_value(to_id.into_stable_key())?;
        let mut fields = record_state(record)?.fields;
        // A record carrying the primary key is keyed by it; otherwise the id
        // is derived from the endpoints.
        let record_id = match fields.remove(&*self.primary_key) {
            None | Some(JsonValue::Null) => StableKey::Str(Arc::from(relation_key(
                &self.from_table.table_name,
                &from_id.key_fragment(),
                &self.to_table.table_name,
                &to_id.key_fragment(),
            ))),
            Some(JsonValue::String(id)) => StableKey::Str(Arc::from(id)),
            Some(id) => match id.as_i64() {
                Some(id) => StableKey::Int(id),
                None => {
                    return Err(Error::engine(format!(
                        "relation record's primary key {:?} must be a string or an integer, \
                         got {id}",
                        self.primary_key
                    )));
                }
            },
        };
        declare_target_state(
            ctx,
            self.provider.target_state(
                record_id,
                RecordState {
                    fields,
                    relation: Some(RelationEndpoints {
                        from_table: self.from_table.clone(),
                        from_id,
                        to_table: self.to_table.clone(),
                        to_id,
                    }),
                },
            ),
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TableEndpoint {
    table_name: String,
    primary_key: String,
}

/// Spec for a graph table/relation (the declared container value). Public so the
/// `*_target` spec constructors can return a composable [`TargetState`]; fields
/// are private.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSpec {
    table_name: String,
    /// `None` for a schemaless table.
    schema: Option<TableSchema>,
    primary_key: String,
    is_relation: bool,
    from_table: Option<TableEndpoint>,
    to_table: Option<TableEndpoint>,
    managed_by: ManagedBy,
}

impl TableSpec {
    fn table(table_name: String, schema: TableSchema, managed_by: ManagedBy) -> Self {
        Self {
            table_name,
            primary_key: schema.primary_key.clone(),
            schema: schema.columns.is_some().then_some(schema),
            is_relation: false,
            from_table: None,
            to_table: None,
            managed_by,
        }
    }

    fn relation(
        relation_name: String,
        from_table: TableEndpoint,
        to_table: TableEndpoint,
        schema: TableSchema,
        managed_by: ManagedBy,
    ) -> Self {
        Self {
            table_name: relation_name,
            primary_key: schema.primary_key.clone(),
            schema: schema.columns.is_some().then_some(schema),
            is_relation: true,
            from_table: Some(from_table),
            to_table: Some(to_table),
            managed_by,
        }
    }

    /// Whether `other` keys its records the same way: same MERGE pattern (label
    /// or relationship type, primary key field, kind, endpoints). A table whose
    /// identity changed has to be rebuilt — its existing nodes / relationships
    /// can no longer be matched record by record. Any other change to the
    /// declaration (a schema edit) only re-upserts records: the graph has no
    /// server-side schema to alter.
    fn same_identity(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.primary_key == other.primary_key
            && self.is_relation == other.is_relation
            && self.from_table == other.from_table
            && self.to_table == other.to_table
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct RecordState {
    fields: Map<String, JsonValue>,
    relation: Option<RelationEndpoints>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct RelationEndpoints {
    from_table: TableEndpoint,
    from_id: KeyValue,
    to_table: TableEndpoint,
    to_id: KeyValue,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
enum KeyValue {
    Int(i64),
    Str(String),
}

impl KeyValue {
    /// The id's part of a relation's target-state key. Integers and ordinary
    /// strings keep their plain form (existing keys stay stable); a string
    /// that would read as an integer, or that starts with the escape marker,
    /// is escaped, so `Int(1)` and `Str("1")` never share a key.
    fn key_fragment(&self) -> String {
        const STR_ESCAPE: &str = "s:";
        match self {
            Self::Int(i) => i.to_string(),
            Self::Str(s) if s.starts_with(STR_ESCAPE) || s.parse::<i64>().is_ok() => {
                format!("{STR_ESCAPE}{s}")
            }
            Self::Str(s) => s.clone(),
        }
    }
}

/// Action emitted by the table container handler.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct TableAction {
    /// The `ContextKey` name of the graph the table lives in.
    graph_key: String,
    op: TableOp,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum TableOp {
    Ensure(TableSpec),
    Replace {
        /// Boxed to keep the variants close in size.
        prev: Box<TableSpec>,
        next: TableSpec,
    },
    Drop(TableSpec),
}

/// Action emitted by the record child handler. The table it belongs to is the
/// record sink's, which captures the spec once for every action of a batch.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct RecordAction {
    record_id: KeyValue,
    state: Option<RecordState>,
}

/// Build the composable [`TargetState`] for a graph table (the row child
/// resolves when declared/mounted).
pub(crate) fn table_target_state<C: CypherExecutor>(
    ctx: &Ctx,
    graph: &ContextKey<C>,
    table_name: impl Into<String>,
    schema: TableSchema,
    options: ManagedTargetOptions,
) -> Result<TargetState<TableSpec>> {
    let table_name = table_name.into();
    validate_ident(&table_name, "table name")?;
    let spec = TableSpec::table(table_name.clone(), schema, options.managed_by);
    let provider = C::table_provider().get(ctx)?;
    Ok(provider.target_state(
        // Stable connection identity = the ContextKey's name; the graph is
        // resolved at apply time from the host context.
        TableKey {
            graph_key: graph.name().to_string(),
            kind: TableKind::Table,
            name: table_name,
        }
        .stable_key(),
        spec,
    ))
}

/// Build the composable [`TargetState`] for a graph relation.
pub(crate) fn relation_target_state<C: CypherExecutor>(
    ctx: &Ctx,
    graph: &ContextKey<C>,
    relation_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
    schema: TableSchema,
    options: ManagedTargetOptions,
) -> Result<TargetState<TableSpec>> {
    let relation_name = relation_name.into();
    validate_ident(&relation_name, "relation name")?;
    let from_endpoint = TableEndpoint {
        table_name: from_table.table_name().to_string(),
        primary_key: from_table.primary_key().to_string(),
    };
    let to_endpoint = TableEndpoint {
        table_name: to_table.table_name().to_string(),
        primary_key: to_table.primary_key().to_string(),
    };
    let spec = TableSpec::relation(
        relation_name.clone(),
        from_endpoint,
        to_endpoint,
        schema,
        options.managed_by,
    );
    let provider = C::table_provider().get(ctx)?;
    Ok(provider.target_state(
        TableKey {
            graph_key: graph.name().to_string(),
            kind: TableKind::Relation,
            name: relation_name,
        }
        .stable_key(),
        spec,
    ))
}

fn table_handle(
    graph_key: &str,
    spec: &TableSpec,
    table_provider: TargetStateProvider<TableSpec>,
    child: TargetStateProvider<RecordState>,
) -> TableTarget {
    TableTarget {
        graph_key: Arc::from(graph_key),
        table_name: Arc::from(spec.table_name.clone()),
        primary_key: Arc::from(spec.primary_key.clone()),
        table_provider,
        provider: child,
        vector_index_names: Arc::default(),
    }
}

fn relation_handle(
    spec: &TableSpec,
    child: TargetStateProvider<RecordState>,
) -> Result<RelationTarget> {
    let (Some(from_table), Some(to_table)) = (&spec.from_table, &spec.to_table) else {
        return Err(Error::engine(format!(
            "relation table {:?} has no endpoints",
            spec.table_name
        )));
    };
    Ok(RelationTarget {
        primary_key: Arc::from(spec.primary_key.as_str()),
        from_table: from_table.clone(),
        to_table: to_table.clone(),
        provider: child,
    })
}

pub(crate) async fn mount_table_target_with_options<C: CypherExecutor>(
    ctx: &Ctx,
    graph: &ContextKey<C>,
    table_name: impl Into<String>,
    schema: TableSchema,
    options: ManagedTargetOptions,
) -> Result<TableTarget> {
    let ts = table_target_state(ctx, graph, table_name, schema, options)?;
    let spec = ts.value().clone();
    let table_provider = ts.provider().clone();
    let child = mount_target(ctx, ts).await?;
    Ok(table_handle(graph.name(), &spec, table_provider, child))
}

pub(crate) fn declare_table_target_with_options<C: CypherExecutor>(
    ctx: &Ctx,
    graph: &ContextKey<C>,
    table_name: impl Into<String>,
    schema: TableSchema,
    options: ManagedTargetOptions,
) -> Result<TableTarget> {
    let ts = table_target_state(ctx, graph, table_name, schema, options)?;
    let spec = ts.value().clone();
    let table_provider = ts.provider().clone();
    let child = declare_target_state_with_child::<TableSpec, RecordState>(ctx, ts)?;
    Ok(table_handle(graph.name(), &spec, table_provider, child))
}

pub(crate) async fn mount_relation_target_with_options<C: CypherExecutor>(
    ctx: &Ctx,
    graph: &ContextKey<C>,
    relation_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
    schema: TableSchema,
    options: ManagedTargetOptions,
) -> Result<RelationTarget> {
    let ts = relation_target_state(
        ctx,
        graph,
        relation_name,
        from_table,
        to_table,
        schema,
        options,
    )?;
    let spec = ts.value().clone();
    let child = mount_target(ctx, ts).await?;
    relation_handle(&spec, child)
}

pub(crate) fn declare_relation_target_with_options<C: CypherExecutor>(
    ctx: &Ctx,
    graph: &ContextKey<C>,
    relation_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
    schema: TableSchema,
    options: ManagedTargetOptions,
) -> Result<RelationTarget> {
    let ts = relation_target_state(
        ctx,
        graph,
        relation_name,
        from_table,
        to_table,
        schema,
        options,
    )?;
    let spec = ts.value().clone();
    let child = declare_target_state_with_child::<TableSpec, RecordState>(ctx, ts)?;
    relation_handle(&spec, child)
}

/// Resolve the graph provided under `graph_key` in the host context.
fn resolve_graph<C: CypherExecutor>(host_ctx: &ContextStore, graph_key: &str) -> Result<Arc<C>> {
    host_ctx.resolve::<C>(graph_key).ok_or_else(|| {
        Error::engine(format!(
            "graph target: connection `{graph_key}` was not provided in the environment \
             (call Environment::builder().provide_key(&KEY, graph))"
        ))
    })
}

/// The connector's static root provider for tables and relations: declare one
/// per [`CypherExecutor`] in `STATIC_ROOT_PROVIDERS` and return it from
/// [`CypherExecutor::table_provider`].
pub(crate) const fn table_root_provider<C: CypherExecutor>(
    name: &'static str,
) -> StaticRootProvider {
    StaticRootProvider::new::<TableSpec, TableHandler<C>>(name)
}

/// Whether a [`TableKey`] names a node table or a relation; part of the key so
/// a table and a relation with the same name stay distinct.
enum TableKind {
    Table,
    Relation,
}

impl TableKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Relation => "relation",
        }
    }

    fn parse(kind: &str) -> Option<Self> {
        match kind {
            "table" => Some(Self::Table),
            "relation" => Some(Self::Relation),
            _ => None,
        }
    }
}

/// A table (or relation) target state's key: the graph's `ContextKey` name,
/// the kind, and the table/relation name.
struct TableKey {
    graph_key: String,
    kind: TableKind,
    name: String,
}

impl TableKey {
    fn stable_key(self) -> StableKey {
        StableKey::Array(Arc::from([
            StableKey::Str(Arc::from(self.graph_key)),
            StableKey::Str(Arc::from(self.kind.as_str())),
            StableKey::Str(Arc::from(self.name)),
        ]))
    }

    fn from_stable_key(key: &StableKey) -> Result<Self> {
        if let StableKey::Array(parts) = key
            && let [
                StableKey::Str(graph_key),
                StableKey::Str(kind),
                StableKey::Str(name),
            ] = &parts[..]
            && let Some(kind) = TableKind::parse(kind)
        {
            return Ok(Self {
                graph_key: graph_key.to_string(),
                kind,
                name: name.to_string(),
            });
        }
        Err(Error::engine(format!(
            "unexpected graph table target key: {key:?}"
        )))
    }
}

/// Root handler for every table/relation target of one graph connector:
/// stateless, the graph is named by each target state's [`TableKey`].
struct TableHandler<C> {
    sink: SinkCache<TableAction>,
    _graph: PhantomData<fn() -> C>,
}

// Hand-written: `derive(Default)` would require `C: Default`.
impl<C> Default for TableHandler<C> {
    fn default() -> Self {
        Self {
            sink: SinkCache::default(),
            _graph: PhantomData,
        }
    }
}

impl<C: CypherExecutor> TargetHandler<TableSpec> for TableHandler<C> {
    type TrackingRecord = MutualTrackingRecord<TableSpec>;
    type Action = TableAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<TableSpec>,
        prev: Vec<MutualTrackingRecord<TableSpec>>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<Self::Action, Self::TrackingRecord>>> {
        let sink = self.sink.get(table_sink::<C>);
        let graph_key = TableKey::from_stable_key(&key)?.graph_key;
        let table_action = |op| TableAction {
            graph_key: graph_key.clone(),
            op,
        };
        match desired {
            Some(spec) => {
                let prev_is_empty = prev.is_empty();
                let tracking_record = MutualTrackingRecord::new(spec.clone(), spec.managed_by);
                let resolved = resolve_system_transition(
                    Some(tracking_record.clone()),
                    prev,
                    prev_may_be_missing,
                );
                let main_action = diff(resolved.as_ref());
                let changed = matches!(main_action, Some(crate::statediff::DiffAction::Replace));
                let prev_system_spec = resolved
                    .as_ref()
                    .and_then(|r| r.prev.iter().find(|p| *p != &spec))
                    .cloned();
                // A rebuild deletes every node / relationship of the table, so
                // only an identity change gets one. A schema-only change keeps
                // the table and is lossy: the records re-upsert.
                let (action, child_invalidation) = match prev_system_spec {
                    Some(prev_spec) if changed && !prev_spec.same_identity(&spec) => (
                        TargetAction::Update(table_action(TableOp::Replace {
                            prev: Box::new(prev_spec),
                            next: spec.clone(),
                        })),
                        Some(crate::target_state::TargetChildInvalidation::Destructive),
                    ),
                    _ => {
                        let ensure = table_action(TableOp::Ensure(spec.clone()));
                        (
                            if prev_is_empty {
                                TargetAction::Create(ensure)
                            } else {
                                TargetAction::Update(ensure)
                            },
                            changed.then_some(crate::target_state::TargetChildInvalidation::Lossy),
                        )
                    }
                };
                Ok(Some(TargetReconcileOutput {
                    action,
                    sink,
                    tracking_record: Some(tracking_record),
                    child_invalidation,
                }))
            }
            None => {
                let resolved = resolve_system_transition(None, prev.clone(), prev_may_be_missing);
                if resolved.is_none() {
                    return Ok(None);
                }
                let spec = prev
                    .into_iter()
                    .find(|p| p.managed_by.is_system())
                    .map(|p| p.tracking_record)
                    .ok_or_else(|| {
                        Error::engine("orphan table target has no previous tracking record")
                    })?;
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Delete(table_action(TableOp::Drop(spec))),
                    sink,
                    tracking_record: None,
                    child_invalidation: Some(
                        crate::target_state::TargetChildInvalidation::Destructive,
                    ),
                }))
            }
        }
    }

    // On the root rather than the record child: dropping a table deletes its
    // contents and its key constraint/index only, so its other indexes must be
    // reconciled on their own when the table goes away.
    fn attachments(&self) -> Result<Vec<(String, ChildTargetDef)>> {
        Ok(vec![
            (
                "vector_index".to_string(),
                ChildTargetDef::new::<VectorIndexSpec, _>(VectorIndexHandler::<C>::default()),
            ),
            (
                "node_index".to_string(),
                ChildTargetDef::new::<NodeIndexSpec, _>(NodeIndexHandler::<C>::default()),
            ),
        ])
    }
}

/// An index attachment's key: the graph's `ContextKey` name plus the index
/// name (which embeds the table name, so it is unique per graph).
fn index_key(graph_key: &str, index_name: String) -> StableKey {
    StableKey::Array(Arc::from([
        StableKey::Str(Arc::from(graph_key)),
        StableKey::Str(Arc::from(index_name)),
    ]))
}

/// The graph key of an [`index_key`].
fn index_key_graph(key: &StableKey) -> Result<String> {
    if let StableKey::Array(parts) = key
        && let [StableKey::Str(graph_key), StableKey::Str(_)] = &parts[..]
    {
        return Ok(graph_key.to_string());
    }
    Err(Error::engine(format!(
        "unexpected graph index target key: {key:?}"
    )))
}

/// A vector index's key: the graph's `ContextKey` name, the node table and
/// the index's declared name (unique per table).
fn vector_index_key(graph_key: &str, label: &str, name: &str) -> StableKey {
    StableKey::Array(Arc::from([
        StableKey::Str(Arc::from(graph_key)),
        StableKey::Str(Arc::from(label)),
        StableKey::Str(Arc::from(name)),
    ]))
}

/// The graph key and declared index name of a [`vector_index_key`].
struct VectorIndexKey {
    graph_key: String,
    name: String,
}

impl VectorIndexKey {
    fn from_stable_key(key: &StableKey) -> Result<Self> {
        if let StableKey::Array(parts) = key
            && let [
                StableKey::Str(graph_key),
                StableKey::Str(_),
                StableKey::Str(name),
            ] = &parts[..]
        {
            return Ok(Self {
                graph_key: graph_key.to_string(),
                name: name.to_string(),
            });
        }
        Err(Error::engine(format!(
            "unexpected graph vector index target key: {key:?}"
        )))
    }
}

// ---------------------------------------------------------------------------
// Vector-index attachment (node tables) — per-dialect Cypher via CypherExecutor
// ---------------------------------------------------------------------------

/// Spec for a graph vector index (an attachment of a node table). Also the
/// tracking record (equality = no change).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorIndexSpec {
    label: String,
    field: String,
    metric: VectorMetric,
    dimension: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct VectorIndexAction {
    /// The `ContextKey` name of the graph the index lives in.
    graph_key: String,
    /// The index's declared name (its target-state identity).
    name: String,
    label: String,
    /// `Some` to (re)create the index on the spec's field.
    spec: Option<VectorIndexSpec>,
    /// Fields this name indexed before and no longer does: the name was
    /// undeclared, or re-pointed at another field. Their indexes are dropped.
    released_fields: Vec<String>,
}

/// Stateless: the graph is named by each index's [`vector_index_key`].
struct VectorIndexHandler<C> {
    sink: SinkCache<VectorIndexAction>,
    _graph: PhantomData<fn() -> C>,
}

impl<C> Default for VectorIndexHandler<C> {
    fn default() -> Self {
        Self {
            sink: SinkCache::default(),
            _graph: PhantomData,
        }
    }
}

impl<C: CypherExecutor> TargetHandler<VectorIndexSpec> for VectorIndexHandler<C> {
    type TrackingRecord = VectorIndexSpec;
    type Action = VectorIndexAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<VectorIndexSpec>,
        prev: Vec<VectorIndexSpec>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<VectorIndexAction, VectorIndexSpec>>> {
        let prev_same = desired
            .as_ref()
            .is_some_and(|d| !prev.is_empty() && prev.iter().all(|p| p == d));
        if desired.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }
        let Some(label) = desired.as_ref().or(prev.first()).map(|s| s.label.clone()) else {
            return Err(Error::engine(format!(
                "vector index {key:?} has neither desired nor previous state"
            )));
        };
        let mut released_fields: Vec<String> = Vec::new();
        for p in &prev {
            if desired.as_ref().is_none_or(|d| d.field != p.field)
                && !released_fields.contains(&p.field)
            {
                released_fields.push(p.field.clone());
            }
        }
        let key = VectorIndexKey::from_stable_key(&key)?;
        Ok(Some(TargetReconcileOutput {
            action: TargetAction::Update(VectorIndexAction {
                graph_key: key.graph_key,
                name: key.name,
                label,
                spec: desired.clone(),
                released_fields,
            }),
            sink: self.sink.get(vector_index_sink::<C>),
            tracking_record: desired,
            child_invalidation: None,
        }))
    }
}

/// An index is tracked by its declared name, but the database identifies it by
/// its label and field. A renamed index therefore arrives as a create under
/// the new name and a drop under the old one, both of one database index: the
/// drop is skipped. A name re-pointed at another field releases its old
/// field, whose index is dropped unless another name creates it. Two declared
/// indexes of one database index are an error.
fn vector_index_sink<C: CypherExecutor>() -> TargetActionSink<VectorIndexAction> {
    TargetActionSink::from_async_fn_with_ctx(
        |host_ctx, actions: Vec<TargetAction<VectorIndexAction>>| {
            async move {
                let actions: Vec<VectorIndexAction> = actions
                    .into_iter()
                    .map(|action| match action {
                        TargetAction::Create(a)
                        | TargetAction::Update(a)
                        | TargetAction::Delete(a) => a,
                    })
                    .collect();
                // (graph, label, field) of every index created -> declared name.
                let mut created: BTreeMap<(&str, &str, &str), &str> = BTreeMap::new();
                for action in &actions {
                    let Some(spec) = &action.spec else { continue };
                    let declared = *created
                        .entry((&action.graph_key, &action.label, &spec.field))
                        .or_insert(&action.name);
                    if declared != action.name {
                        return Err(Error::engine(format!(
                            "Vector indexes {declared:?} and {:?} both index {}.{}: declare \
                             one vector index per field.",
                            action.name, action.label, spec.field
                        )));
                    }
                }
                for action in &actions {
                    let graph = resolve_graph::<C>(&host_ctx, &action.graph_key)?;
                    for field in &action.released_fields {
                        let recreated = created.contains_key(&(
                            action.graph_key.as_str(),
                            action.label.as_str(),
                            field.as_str(),
                        ));
                        if !recreated {
                            graph.drop_vector_index(&action.label, field).await?;
                        }
                    }
                    if let Some(spec) = &action.spec {
                        // Drop-then-create so a metric/dimension change is applied.
                        let _ = graph.drop_vector_index(&action.label, &spec.field).await;
                        graph
                            .create_vector_index(
                                &action.label,
                                &spec.field,
                                spec.dimension,
                                spec.metric,
                            )
                            .await?;
                    }
                }
                Ok(())
            }
        },
    )
}

// ---------------------------------------------------------------------------
// Node-property index attachment (non-vector secondary index)
// ---------------------------------------------------------------------------

/// Spec for a node-property index (an attachment of a node table). Also the
/// tracking record (equality = no change).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeIndexSpec {
    label: String,
    fields: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NodeIndexAction {
    /// The `ContextKey` name of the graph the index lives in.
    graph_key: String,
    label: String,
    fields: Vec<String>,
    /// `true` to create the index, `false` to drop it.
    present: bool,
}

/// Stateless: the graph is named by each index's [`index_key`].
struct NodeIndexHandler<C> {
    sink: SinkCache<NodeIndexAction>,
    _graph: PhantomData<fn() -> C>,
}

impl<C> Default for NodeIndexHandler<C> {
    fn default() -> Self {
        Self {
            sink: SinkCache::default(),
            _graph: PhantomData,
        }
    }
}

impl<C: CypherExecutor> TargetHandler<NodeIndexSpec> for NodeIndexHandler<C> {
    type TrackingRecord = NodeIndexSpec;
    type Action = NodeIndexAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<NodeIndexSpec>,
        prev: Vec<NodeIndexSpec>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<NodeIndexAction, NodeIndexSpec>>> {
        let prev_same = desired
            .as_ref()
            .is_some_and(|d| !prev.is_empty() && prev.iter().all(|p| p == d));
        if desired.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }
        let (label, fields, present) = match (&desired, prev.first()) {
            (Some(d), _) => (d.label.clone(), d.fields.clone(), true),
            (None, Some(p)) => (p.label.clone(), p.fields.clone(), false),
            (None, None) => {
                return Err(Error::engine(format!(
                    "node index {key:?} has neither desired nor previous state"
                )));
            }
        };
        Ok(Some(TargetReconcileOutput {
            action: TargetAction::Update(NodeIndexAction {
                graph_key: index_key_graph(&key)?,
                label,
                fields,
                present,
            }),
            sink: self.sink.get(node_index_sink::<C>),
            tracking_record: desired,
            child_invalidation: None,
        }))
    }
}

fn node_index_sink<C: CypherExecutor>() -> TargetActionSink<NodeIndexAction> {
    TargetActionSink::from_async_fn_with_ctx(
        |host_ctx, actions: Vec<TargetAction<NodeIndexAction>>| {
            async move {
                for action in actions {
                    let action = match action {
                        TargetAction::Create(a)
                        | TargetAction::Update(a)
                        | TargetAction::Delete(a) => a,
                    };
                    let graph = resolve_graph::<C>(&host_ctx, &action.graph_key)?;
                    if action.present {
                        // Drop-then-create so a field-set change is applied cleanly.
                        let _ = graph.drop_node_index(&action.label, &action.fields).await;
                        graph
                            .create_node_index(&action.label, &action.fields)
                            .await?;
                    } else {
                        graph.drop_node_index(&action.label, &action.fields).await?;
                    }
                }
                Ok(())
            }
        },
    )
}

// ---------------------------------------------------------------------------
// Record handler (child) + sink
// ---------------------------------------------------------------------------

struct RecordHandler<C> {
    sink: SinkCache<RecordAction>,
    graph_key: String,
    spec: Arc<TableSpec>,
    _graph: PhantomData<fn() -> C>,
}

impl<C: CypherExecutor> TargetHandler<RecordState> for RecordHandler<C> {
    type TrackingRecord = Fingerprint;
    type Action = RecordAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<RecordState>,
        prev: Vec<Fingerprint>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<Self::Action, Self::TrackingRecord>>> {
        let desired_fp = desired
            .as_ref()
            .map(Fingerprint::from)
            .transpose()
            .map_err(|e| Error::engine(e.to_string()))?;
        let prev_same = desired_fp
            .as_ref()
            .is_some_and(|fp| !prev.is_empty() && prev.iter().all(|p| p == fp));
        if desired.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }
        let action = RecordAction {
            record_id: key_value(key)?,
            state: desired,
        };
        let action = if action.state.is_some() {
            TargetAction::Update(action)
        } else {
            TargetAction::Delete(action)
        };
        Ok(Some(TargetReconcileOutput {
            action,
            tracking_record: desired_fp,
            sink: self
                .sink
                .get(|| record_sink::<C>(self.graph_key.clone(), self.spec.clone())),
            child_invalidation: None,
        }))
    }
}

fn table_sink<C: CypherExecutor>() -> TargetActionSink<TableAction> {
    TargetActionSink::from_async_fn_with_child_handler_ctx(
        |host_ctx, actions: Vec<TargetAction<TableAction>>| async move {
            for action in actions {
                let (is_delete, action) = match action {
                    TargetAction::Create(a) | TargetAction::Update(a) => (false, a),
                    TargetAction::Delete(a) => (true, a),
                };
                let graph = resolve_graph::<C>(&host_ctx, &action.graph_key)?;
                match (is_delete, action.op) {
                    (false, TableOp::Ensure(spec)) => {
                        ensure_table(graph.as_ref(), &spec).await?;
                    }
                    (false, TableOp::Replace { prev, next }) => {
                        drop_table(graph.as_ref(), &prev).await?;
                        ensure_table(graph.as_ref(), &next).await?;
                    }
                    (true, TableOp::Ensure(spec)) | (_, TableOp::Drop(spec)) => {
                        drop_table(graph.as_ref(), &spec).await?;
                    }
                    (true, TableOp::Replace { prev, next }) => {
                        drop_table(graph.as_ref(), &prev).await?;
                        drop_table(graph.as_ref(), &next).await?;
                    }
                }
            }
            Ok(())
        },
        // The record handler targets the table that exists after the action.
        |a: &TableAction| {
            let spec = match &a.op {
                TableOp::Ensure(spec) | TableOp::Replace { next: spec, .. } => spec.clone(),
                TableOp::Drop(_) => {
                    return Err(Error::engine("a table drop action has no record child"));
                }
            };
            Ok(ChildTargetDef::new::<RecordState, _>(RecordHandler::<C> {
                sink: SinkCache::default(),
                graph_key: a.graph_key.clone(),
                spec: Arc::new(spec),
                _graph: PhantomData,
            }))
        },
    )
}

fn record_sink<C: CypherExecutor>(
    graph_key: String,
    spec: Arc<TableSpec>,
) -> TargetActionSink<RecordAction> {
    TargetActionSink::from_async_fn_with_ctx(
        move |host_ctx, actions: Vec<TargetAction<RecordAction>>| {
            let graph_key = graph_key.clone();
            let spec = spec.clone();
            async move {
                let graph = resolve_graph::<C>(&host_ctx, &graph_key)?;
                let actions = actions
                    .into_iter()
                    .map(|action| {
                        let (TargetAction::Create(action)
                        | TargetAction::Update(action)
                        | TargetAction::Delete(action)) = action;
                        action
                    })
                    .collect();
                apply_records(graph.as_ref(), &spec, actions).await
            }
        },
    )
}

async fn ensure_table<C: CypherExecutor>(graph: &C, spec: &TableSpec) -> Result<()> {
    if spec.managed_by.is_user() {
        return Ok(());
    }
    match (graph.dialect(), spec.is_relation) {
        ("neo4j", false) => {
            graph
                .execute(&format!(
                    "CREATE CONSTRAINT `{}` IF NOT EXISTS FOR (n:`{}`) REQUIRE n.`{}` IS UNIQUE",
                    constraint_name(&spec.table_name, &spec.primary_key),
                    spec.table_name,
                    spec.primary_key
                ))
                .await?;
        }
        ("neo4j", true) => {
            graph
                .execute(&format!(
                    "CREATE INDEX `{}` IF NOT EXISTS FOR ()-[r:`{}`]-() ON (r.`{}`)",
                    relation_index_name(&spec.table_name, &spec.primary_key),
                    spec.table_name,
                    spec.primary_key
                ))
                .await?;
        }
        ("falkordb", false) => {
            // FalkorDB errors when an index/constraint already exists, so setup
            // is best-effort.
            let _ = graph
                .execute(&format!(
                    "CREATE INDEX FOR (e:`{}`) ON (e.`{}`)",
                    spec.table_name, spec.primary_key
                ))
                .await;
            let _ = graph
                .execute_unique_constraint("CREATE", "NODE", &spec.table_name, &spec.primary_key)
                .await;
        }
        ("falkordb", true) => {
            let _ = graph
                .execute(&format!(
                    "CREATE INDEX FOR ()-[e:`{}`]-() ON (e.`{}`)",
                    spec.table_name, spec.primary_key
                ))
                .await;
            let _ = graph
                .execute_unique_constraint(
                    "CREATE",
                    "RELATIONSHIP",
                    &spec.table_name,
                    &spec.primary_key,
                )
                .await;
        }
        _ => {}
    }
    Ok(())
}

/// Destroy a system-managed table: delete its contents, then its DDL artifact.
///
/// The table owns its label / relationship type the way a Postgres table owns
/// its rows: every node carrying the label (with the relationships attached to
/// it) or every relationship of the type is deleted, whoever wrote it. The
/// engine prunes the records' tracking without reconciling them once their
/// container is gone, so this is where their data goes away. A label shared
/// with data CocoIndex does not own belongs in a user-managed table, which is
/// abandoned instead.
async fn drop_table<C: CypherExecutor>(graph: &C, spec: &TableSpec) -> Result<()> {
    if spec.managed_by.is_user() {
        return Ok(());
    }
    let delete_contents = match (graph.dialect(), spec.is_relation) {
        // Neo4j: default-sized inner transactions so a large label need not fit
        // one transaction's heap. `CALL … IN TRANSACTIONS` requires the
        // auto-commit execution `execute` uses.
        ("neo4j", false) => format!(
            "MATCH (n:`{}`) CALL {{ WITH n DETACH DELETE n }} IN TRANSACTIONS",
            spec.table_name
        ),
        ("neo4j", true) => format!(
            "MATCH ()-[r:`{}`]->() CALL {{ WITH r DELETE r }} IN TRANSACTIONS",
            spec.table_name
        ),
        (_, false) => format!("MATCH (n:`{}`) DETACH DELETE n", spec.table_name),
        (_, true) => format!("MATCH ()-[r:`{}`]->() DELETE r", spec.table_name),
    };
    graph.execute(&delete_contents).await?;
    match (graph.dialect(), spec.is_relation) {
        ("neo4j", false) => {
            graph
                .execute(&format!(
                    "DROP CONSTRAINT `{}` IF EXISTS",
                    constraint_name(&spec.table_name, &spec.primary_key)
                ))
                .await?;
        }
        ("neo4j", true) => {
            graph
                .execute(&format!(
                    "DROP INDEX `{}` IF EXISTS",
                    relation_index_name(&spec.table_name, &spec.primary_key)
                ))
                .await?;
        }
        ("falkordb", false) => {
            let _ = graph
                .execute_unique_constraint("DROP", "NODE", &spec.table_name, &spec.primary_key)
                .await;
            let _ = graph
                .execute(&format!(
                    "DROP INDEX FOR (e:`{}`) ON (e.`{}`)",
                    spec.table_name, spec.primary_key
                ))
                .await;
        }
        ("falkordb", true) => {
            let _ = graph
                .execute_unique_constraint(
                    "DROP",
                    "RELATIONSHIP",
                    &spec.table_name,
                    &spec.primary_key,
                )
                .await;
            let _ = graph
                .execute(&format!(
                    "DROP INDEX FOR ()-[e:`{}`]-() ON (e.`{}`)",
                    spec.table_name, spec.primary_key
                ))
                .await;
        }
        _ => {}
    }
    Ok(())
}

/// Apply a batch of one table's record actions: upserts first, then deletes,
/// in transactions of at most [`MAX_ACTIONS_PER_TXN`] actions, one after the
/// other. A failed transaction raises, so the batch's components are retried;
/// earlier transactions stay committed, which is safe as the writes are
/// idempotent (`MERGE`).
async fn apply_records<C: CypherExecutor>(
    graph: &C,
    spec: &TableSpec,
    actions: Vec<RecordAction>,
) -> Result<()> {
    let (upserts, deletes): (Vec<_>, Vec<_>) = actions
        .into_iter()
        .partition(|action| action.state.is_some());
    let ordered: Vec<RecordAction> = upserts.into_iter().chain(deletes).collect();
    for chunk in ordered.chunks(MAX_ACTIONS_PER_TXN) {
        apply_chunk(graph, spec, chunk).await?;
    }
    Ok(())
}

/// Apply `chunk` as one unit ([`CypherExecutor::execute_batch`]); split it in
/// two when it goes over the server's transaction memory limit.
async fn apply_chunk<C: CypherExecutor>(
    graph: &C,
    spec: &TableSpec,
    chunk: &[RecordAction],
) -> Result<()> {
    let statements = record_statements(spec, chunk)?;
    match graph.execute_batch(&statements).await {
        Ok(()) => Ok(()),
        Err(error) if chunk.len() > 1 && graph.is_transaction_too_large(&error) => {
            let (first, second) = chunk.split_at(chunk.len() / 2);
            Box::pin(apply_chunk(graph, spec, first)).await?;
            Box::pin(apply_chunk(graph, spec, second)).await
        }
        Err(error) => Err(error),
    }
}

/// The statements that apply `actions` (already in order), one `UNWIND $rows`
/// statement per statement shape: the shape of a node upsert or delete is
/// fixed by the table, a relation upsert's also by its endpoint tables. Rows
/// of one shape keep their order, and so do the shapes (by first row).
fn record_statements(spec: &TableSpec, actions: &[RecordAction]) -> Result<Vec<CypherStatement>> {
    let mut statements: Vec<(String, Vec<JsonValue>)> = Vec::new();
    for action in actions {
        let (cypher, row) = record_statement(spec, action)?;
        match statements.iter_mut().find(|(shape, _)| *shape == cypher) {
            Some((_, rows)) => rows.push(row),
            None => statements.push((cypher, vec![row])),
        }
    }
    Ok(statements
        .into_iter()
        .map(|(cypher, rows)| CypherStatement {
            cypher,
            params: CypherParams::from([("rows".to_string(), JsonValue::Array(rows))]),
        })
        .collect())
}

/// The `UNWIND $rows AS row` statement that applies `action`, and the row it
/// contributes to `$rows`.
fn record_statement(spec: &TableSpec, action: &RecordAction) -> Result<(String, JsonValue)> {
    let mut row = Map::new();
    let cypher = match (&action.state, spec.is_relation) {
        (Some(state), false) => {
            let mut props = state.fields.clone();
            props.remove(&spec.primary_key);
            row.insert("key".to_string(), key_json(&action.record_id));
            row.insert("props".to_string(), JsonValue::Object(props));
            format!(
                "UNWIND $rows AS row MERGE (n:`{}` {{`{}`: row.key}}) SET n += row.props",
                spec.table_name, spec.primary_key
            )
        }
        (None, false) => {
            row.insert("key".to_string(), key_json(&action.record_id));
            format!(
                "UNWIND $rows AS row MATCH (n:`{}` {{`{}`: row.key}}) DETACH DELETE n",
                spec.table_name, spec.primary_key
            )
        }
        (Some(state), true) => {
            let rel = state
                .relation
                .as_ref()
                .ok_or_else(|| Error::engine("relation record missing endpoints"))?;
            row.insert("from_id".to_string(), key_json(&rel.from_id));
            row.insert("to_id".to_string(), key_json(&rel.to_id));
            row.insert("rel_id".to_string(), key_json(&action.record_id));
            row.insert("props".to_string(), JsonValue::Object(state.fields.clone()));
            let rel_pattern = format!(
                "`{}` {{`{}`: row.rel_id}}",
                spec.table_name, spec.primary_key
            );
            // The key identifies one relationship. An edge with this key
            // between other endpoints is this relationship before its
            // endpoints changed: delete it, or the MERGE would add a second
            // edge beside it.
            format!(
                "UNWIND $rows AS row \
                 MERGE (s:`{}` {{`{}`: row.from_id}}) MERGE (t:`{}` {{`{}`: row.to_id}}) \
                 WITH s, t, row OPTIONAL MATCH (a)-[o:{rel_pattern}]->(b) WHERE a <> s OR b <> t \
                 DELETE o WITH DISTINCT s, t, row \
                 MERGE (s)-[r:{rel_pattern}]->(t) SET r += row.props",
                rel.from_table.table_name,
                rel.from_table.primary_key,
                rel.to_table.table_name,
                rel.to_table.primary_key,
            )
        }
        (None, true) => {
            row.insert("rel_id".to_string(), key_json(&action.record_id));
            format!(
                "UNWIND $rows AS row MATCH ()-[r:`{}` {{`{}`: row.rel_id}}]->() DELETE r",
                spec.table_name, spec.primary_key
            )
        }
    };
    Ok((cypher, JsonValue::Object(row)))
}

fn record_state<R: Serialize>(row: &R) -> Result<RecordState> {
    // Reject non-finite floats: `serde_json` turns NaN/±Inf into JSON null, which
    // would be written as a null property (Bolt) / `null` literal (FalkorDB),
    // silently corrupting numeric/vector properties.
    crate::finite::ensure_finite(row)
        .map_err(|e| Error::engine(format!("graph target record has a {e}")))?;
    let value = serde_json::to_value(row).map_err(|e| Error::engine(e.to_string()))?;
    let fields = match value {
        JsonValue::Object(map) => map,
        _ => {
            return Err(Error::engine(
                "graph target records must serialize to a JSON object",
            ));
        }
    };
    Ok(RecordState {
        fields,
        relation: None,
    })
}

fn key_value(key: StableKey) -> Result<KeyValue> {
    match key {
        StableKey::Int(i) => Ok(KeyValue::Int(i)),
        StableKey::Str(s) | StableKey::Symbol(s) => Ok(KeyValue::Str(s.to_string())),
        other => Err(Error::engine(format!(
            "graph target keys must be string-like or integer, got {other:?}"
        ))),
    }
}

/// A key value as JSON, for binding as a Cypher parameter.
fn key_json(key: &KeyValue) -> JsonValue {
    match key {
        KeyValue::Int(i) => JsonValue::from(*i),
        KeyValue::Str(s) => JsonValue::from(s.clone()),
    }
}

/// Render a JSON value as a Cypher literal — used by FalkorDB to build the
/// `CYPHER name=value` parameter header (Neo4j binds Bolt params instead).
#[cfg(feature = "falkordb")]
pub(crate) fn cypher_literal(value: &JsonValue) -> Result<String> {
    cypher_value(value)
}

#[cfg(feature = "falkordb")]
fn cypher_map(map: &Map<String, JsonValue>) -> Result<String> {
    let mut out = String::from("{");
    for (idx, (key, value)) in map.iter().enumerate() {
        validate_ident(key, "property name")?;
        if idx > 0 {
            out.push_str(", ");
        }
        out.push('`');
        out.push_str(key);
        out.push_str("`: ");
        out.push_str(&cypher_value(value)?);
    }
    out.push('}');
    Ok(out)
}

#[cfg(feature = "falkordb")]
fn cypher_value(value: &JsonValue) -> Result<String> {
    Ok(match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => cypher_string(s),
        JsonValue::Array(values) => {
            let values = values
                .iter()
                .map(cypher_value)
                .collect::<Result<Vec<_>>>()?
                .join(", ");
            format!("[{values}]")
        }
        JsonValue::Object(map) => cypher_map(map)?,
    })
}

#[cfg(feature = "falkordb")]
fn cypher_string(value: &str) -> String {
    let mut out = String::from("'");
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out.push('\'');
    out
}

fn relation_key(from_table: &str, from_id: &str, to_table: &str, to_id: &str) -> String {
    format!(
        "{}{}{}{}",
        relation_key_part(from_table),
        relation_key_part(from_id),
        relation_key_part(to_table),
        relation_key_part(to_id)
    )
}

fn relation_key_part(value: &str) -> String {
    format!("{}:{value};", value.len())
}

fn constraint_name(table: &str, primary_key: &str) -> String {
    format!("coco_uniq_{table}__{primary_key}")
}

fn relation_index_name(table: &str, primary_key: &str) -> String {
    format!("coco_idx_rel_{table}__{primary_key}")
}

pub(crate) fn validate_ident(name: &str, what: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::engine(format!("Invalid {what}: empty identifier")));
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(Error::engine(format!("Invalid {what}: {name:?}")));
    }
    if chars.any(|c| !(c == '_' || c.is_ascii_alphanumeric())) {
        return Err(Error::engine(format!("Invalid {what}: {name:?}")));
    }
    Ok(())
}

/// Generate a graph connector's public table/relation target surface for a
/// given `$graph` executor type. The neo4j and falkordb connectors differ only
/// in their `Graph`/`CypherExecutor`; this macro is the single source of truth
/// for the identical `TableTarget`/`RelationTarget` newtypes and the
/// `mount_*`/`declare_*`/`*_target` free functions that wrap [`cypher_graph`].
///
/// The functions take the graph as a `ContextKey`: the key's name is the
/// graph's stable identity in target-state keys, and the `Graph` provided under
/// it (`Environment::builder().provide_key(&KEY, graph)`) is resolved when
/// changes are applied, including on `App::drop_state`.
macro_rules! graph_target_api {
    ($graph:ty) => {
        /// A node-table target. Cheap to clone.
        #[derive(Clone)]
        pub struct TableTarget($crate::cypher_graph::TableTarget);

        impl TableTarget {
            /// The table/label name.
            pub fn table_name(&self) -> &str {
                self.0.table_name()
            }

            /// Declare a node record keyed by `id`.
            pub fn declare_record<R: ::serde::Serialize>(
                &self,
                ctx: &$crate::ctx::Ctx,
                id: impl $crate::target_state::IntoStableKey,
                row: &R,
            ) -> $crate::error::Result<()> {
                self.0.declare_record(ctx, id, row)
            }

            /// Declare a vector index on `field`, created/recreated/dropped to
            /// match the declaration. It is named `vec_{table}__{field}`.
            pub fn declare_vector_index(
                &self,
                ctx: &$crate::ctx::Ctx,
                field: &str,
                dimension: u32,
                metric: $crate::cypher_graph::VectorMetric,
            ) -> $crate::error::Result<()> {
                self.0
                    .declare_vector_index(ctx, None, field, dimension, metric)
            }

            /// [`declare_vector_index`](Self::declare_vector_index) with an
            /// explicit `name`. The name identifies the declaration. The
            /// database identifies the index by its label and field, so a
            /// field takes one vector index, and renaming the declaration
            /// rebuilds that index once and does not remove it. Declaring the
            /// name on another field drops the index on the old field.
            pub fn declare_vector_index_named(
                &self,
                ctx: &$crate::ctx::Ctx,
                name: &str,
                field: &str,
                dimension: u32,
                metric: $crate::cypher_graph::VectorMetric,
            ) -> $crate::error::Result<()> {
                self.0
                    .declare_vector_index(ctx, Some(name), field, dimension, metric)
            }

            /// Declare a (non-vector) secondary index on `fields`,
            /// created/dropped to match the declaration.
            pub fn declare_node_index(
                &self,
                ctx: &$crate::ctx::Ctx,
                fields: &[&str],
            ) -> $crate::error::Result<()> {
                self.0.declare_node_index(ctx, fields)
            }
        }

        /// A relation target. Cheap to clone.
        #[derive(Clone)]
        pub struct RelationTarget($crate::cypher_graph::RelationTarget);

        impl RelationTarget {
            /// Declare a relation (no properties) from `from_id` to `to_id`.
            pub fn declare_relation(
                &self,
                ctx: &$crate::ctx::Ctx,
                from_id: impl $crate::target_state::IntoStableKey,
                to_id: impl $crate::target_state::IntoStableKey,
            ) -> $crate::error::Result<()> {
                self.0.declare_relation(ctx, from_id, to_id)
            }

            /// Declare a relation carrying `record` properties. A record
            /// that has the relation's primary key is keyed by it (a string or
            /// an integer); otherwise the id is derived from the endpoints.
            pub fn declare_relation_record<R: ::serde::Serialize>(
                &self,
                ctx: &$crate::ctx::Ctx,
                from_id: impl $crate::target_state::IntoStableKey,
                to_id: impl $crate::target_state::IntoStableKey,
                record: &R,
            ) -> $crate::error::Result<()> {
                self.0.declare_relation_record(ctx, from_id, to_id, record)
            }
        }

        /// Mount a node-table target (record children resolve immediately) in
        /// the graph provided under `graph`.
        pub async fn mount_table_target(
            ctx: &$crate::ctx::Ctx,
            graph: &$crate::ContextKey<$graph>,
            table_name: impl Into<String>,
            schema: $crate::cypher_graph::TableSchema,
        ) -> $crate::error::Result<TableTarget> {
            mount_table_target_with_options(
                ctx,
                graph,
                table_name,
                schema,
                $crate::statediff::ManagedTargetOptions::default(),
            )
            .await
        }

        /// [`mount_table_target`] with explicit options.
        pub async fn mount_table_target_with_options(
            ctx: &$crate::ctx::Ctx,
            graph: &$crate::ContextKey<$graph>,
            table_name: impl Into<String>,
            schema: $crate::cypher_graph::TableSchema,
            options: $crate::statediff::ManagedTargetOptions,
        ) -> $crate::error::Result<TableTarget> {
            Ok(TableTarget(
                $crate::cypher_graph::mount_table_target_with_options(
                    ctx, graph, table_name, schema, options,
                )
                .await?,
            ))
        }

        /// Mount a relation target (relation records resolve immediately).
        /// The relation is schemaless and keyed by its `id` property.
        pub async fn mount_relation_target(
            ctx: &$crate::ctx::Ctx,
            graph: &$crate::ContextKey<$graph>,
            relation_name: impl Into<String>,
            from_table: &TableTarget,
            to_table: &TableTarget,
        ) -> $crate::error::Result<RelationTarget> {
            mount_relation_target_with_schema(
                ctx,
                graph,
                relation_name,
                from_table,
                to_table,
                $crate::cypher_graph::TableSchema::schemaless("id")?,
            )
            .await
        }

        /// [`mount_relation_target`] with a `schema`, which names the
        /// relation's primary key.
        pub async fn mount_relation_target_with_schema(
            ctx: &$crate::ctx::Ctx,
            graph: &$crate::ContextKey<$graph>,
            relation_name: impl Into<String>,
            from_table: &TableTarget,
            to_table: &TableTarget,
            schema: $crate::cypher_graph::TableSchema,
        ) -> $crate::error::Result<RelationTarget> {
            mount_relation_target_with_options(
                ctx,
                graph,
                relation_name,
                from_table,
                to_table,
                schema,
                $crate::statediff::ManagedTargetOptions::default(),
            )
            .await
        }

        /// [`mount_relation_target_with_schema`] with explicit options.
        pub async fn mount_relation_target_with_options(
            ctx: &$crate::ctx::Ctx,
            graph: &$crate::ContextKey<$graph>,
            relation_name: impl Into<String>,
            from_table: &TableTarget,
            to_table: &TableTarget,
            schema: $crate::cypher_graph::TableSchema,
            options: $crate::statediff::ManagedTargetOptions,
        ) -> $crate::error::Result<RelationTarget> {
            Ok(RelationTarget(
                $crate::cypher_graph::mount_relation_target_with_options(
                    ctx,
                    graph,
                    relation_name,
                    &from_table.0,
                    &to_table.0,
                    schema,
                    options,
                )
                .await?,
            ))
        }

        /// Build a composable
        /// [`TargetState`](crate::target_state::TargetState) for a node table.
        /// Pass it to the generic
        /// [`mount_target`](crate::target_state::mount_target) /
        /// [`declare_target_state_with_child`](crate::target_state::declare_target_state_with_child),
        /// or use [`declare_table_target`]/[`mount_table_target`].
        pub fn table_target(
            ctx: &$crate::ctx::Ctx,
            graph: &$crate::ContextKey<$graph>,
            table_name: impl Into<String>,
            schema: $crate::cypher_graph::TableSchema,
        ) -> $crate::error::Result<
            $crate::target_state::TargetState<$crate::cypher_graph::TableSpec>,
        > {
            table_target_with_options(
                ctx,
                graph,
                table_name,
                schema,
                $crate::statediff::ManagedTargetOptions::default(),
            )
        }

        /// [`table_target`] with explicit options.
        pub fn table_target_with_options(
            ctx: &$crate::ctx::Ctx,
            graph: &$crate::ContextKey<$graph>,
            table_name: impl Into<String>,
            schema: $crate::cypher_graph::TableSchema,
            options: $crate::statediff::ManagedTargetOptions,
        ) -> $crate::error::Result<
            $crate::target_state::TargetState<$crate::cypher_graph::TableSpec>,
        > {
            $crate::cypher_graph::table_target_state(ctx, graph, table_name, schema, options)
        }

        /// Build a composable
        /// [`TargetState`](crate::target_state::TargetState) for a relation.
        /// The relation is schemaless and keyed by its `id` property.
        pub fn relation_target(
            ctx: &$crate::ctx::Ctx,
            graph: &$crate::ContextKey<$graph>,
            relation_name: impl Into<String>,
            from_table: &TableTarget,
            to_table: &TableTarget,
        ) -> $crate::error::Result<
            $crate::target_state::TargetState<$crate::cypher_graph::TableSpec>,
        > {
            relation_target_with_schema(
                ctx,
                graph,
                relation_name,
                from_table,
                to_table,
                $crate::cypher_graph::TableSchema::schemaless("id")?,
            )
        }

        /// [`relation_target`] with a `schema`, which names the relation's
        /// primary key.
        pub fn relation_target_with_schema(
            ctx: &$crate::ctx::Ctx,
            graph: &$crate::ContextKey<$graph>,
            relation_name: impl Into<String>,
            from_table: &TableTarget,
            to_table: &TableTarget,
            schema: $crate::cypher_graph::TableSchema,
        ) -> $crate::error::Result<
            $crate::target_state::TargetState<$crate::cypher_graph::TableSpec>,
        > {
            relation_target_with_options(
                ctx,
                graph,
                relation_name,
                from_table,
                to_table,
                schema,
                $crate::statediff::ManagedTargetOptions::default(),
            )
        }

        /// [`relation_target_with_schema`] with explicit options.
        pub fn relation_target_with_options(
            ctx: &$crate::ctx::Ctx,
            graph: &$crate::ContextKey<$graph>,
            relation_name: impl Into<String>,
            from_table: &TableTarget,
            to_table: &TableTarget,
            schema: $crate::cypher_graph::TableSchema,
            options: $crate::statediff::ManagedTargetOptions,
        ) -> $crate::error::Result<
            $crate::target_state::TargetState<$crate::cypher_graph::TableSpec>,
        > {
            $crate::cypher_graph::relation_target_state(
                ctx,
                graph,
                relation_name,
                &from_table.0,
                &to_table.0,
                schema,
                options,
            )
        }

        /// Declare a node-table target in the **current** component (the record
        /// child provider resolves when this component commits). Use
        /// [`mount_table_target`] when records must be declared immediately.
        pub fn declare_table_target(
            ctx: &$crate::ctx::Ctx,
            graph: &$crate::ContextKey<$graph>,
            table_name: impl Into<String>,
            schema: $crate::cypher_graph::TableSchema,
        ) -> $crate::error::Result<TableTarget> {
            declare_table_target_with_options(
                ctx,
                graph,
                table_name,
                schema,
                $crate::statediff::ManagedTargetOptions::default(),
            )
        }

        /// [`declare_table_target`] with explicit options.
        pub fn declare_table_target_with_options(
            ctx: &$crate::ctx::Ctx,
            graph: &$crate::ContextKey<$graph>,
            table_name: impl Into<String>,
            schema: $crate::cypher_graph::TableSchema,
            options: $crate::statediff::ManagedTargetOptions,
        ) -> $crate::error::Result<TableTarget> {
            Ok(TableTarget(
                $crate::cypher_graph::declare_table_target_with_options(
                    ctx, graph, table_name, schema, options,
                )?,
            ))
        }

        /// Declare a relation target in the **current** component. Use
        /// [`mount_relation_target`] when relation records must be declared
        /// immediately. The relation is schemaless and keyed by its `id`
        /// property.
        pub fn declare_relation_target(
            ctx: &$crate::ctx::Ctx,
            graph: &$crate::ContextKey<$graph>,
            relation_name: impl Into<String>,
            from_table: &TableTarget,
            to_table: &TableTarget,
        ) -> $crate::error::Result<RelationTarget> {
            declare_relation_target_with_schema(
                ctx,
                graph,
                relation_name,
                from_table,
                to_table,
                $crate::cypher_graph::TableSchema::schemaless("id")?,
            )
        }

        /// [`declare_relation_target`] with a `schema`, which names the
        /// relation's primary key.
        pub fn declare_relation_target_with_schema(
            ctx: &$crate::ctx::Ctx,
            graph: &$crate::ContextKey<$graph>,
            relation_name: impl Into<String>,
            from_table: &TableTarget,
            to_table: &TableTarget,
            schema: $crate::cypher_graph::TableSchema,
        ) -> $crate::error::Result<RelationTarget> {
            declare_relation_target_with_options(
                ctx,
                graph,
                relation_name,
                from_table,
                to_table,
                schema,
                $crate::statediff::ManagedTargetOptions::default(),
            )
        }

        /// [`declare_relation_target_with_schema`] with explicit options.
        pub fn declare_relation_target_with_options(
            ctx: &$crate::ctx::Ctx,
            graph: &$crate::ContextKey<$graph>,
            relation_name: impl Into<String>,
            from_table: &TableTarget,
            to_table: &TableTarget,
            schema: $crate::cypher_graph::TableSchema,
            options: $crate::statediff::ManagedTargetOptions,
        ) -> $crate::error::Result<RelationTarget> {
            Ok(RelationTarget(
                $crate::cypher_graph::declare_relation_target_with_options(
                    ctx,
                    graph,
                    relation_name,
                    &from_table.0,
                    &to_table.0,
                    schema,
                    options,
                )?,
            ))
        }
    };
}

pub(crate) use graph_target_api;

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct FakeGraph;

    #[async_trait]
    impl CypherExecutor for FakeGraph {
        fn dialect(&self) -> &'static str {
            "fake"
        }

        fn table_provider() -> &'static StaticRootProvider {
            static PROVIDER: StaticRootProvider =
                table_root_provider::<FakeGraph>("cocoindex/test_fake/table");
            &PROVIDER
        }

        async fn execute(&self, _cypher: &str) -> Result<()> {
            Ok(())
        }

        async fn execute_with_params(&self, _cypher: &str, _params: &CypherParams) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct RecordingGraph {
        dialect: &'static str,
        statements: Arc<std::sync::Mutex<Vec<String>>>,
        constraints: Arc<std::sync::Mutex<Vec<String>>>,
        param_calls: Arc<std::sync::Mutex<Vec<(String, CypherParams)>>>,
        /// The number of statements of each `execute_batch` call.
        batches: Arc<std::sync::Mutex<Vec<usize>>>,
    }

    impl RecordingGraph {
        fn new(dialect: &'static str) -> Self {
            Self {
                dialect,
                statements: Arc::new(std::sync::Mutex::new(Vec::new())),
                constraints: Arc::new(std::sync::Mutex::new(Vec::new())),
                param_calls: Arc::new(std::sync::Mutex::new(Vec::new())),
                batches: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        fn statements(&self) -> Vec<String> {
            self.statements.lock().unwrap().clone()
        }

        fn constraints(&self) -> Vec<String> {
            self.constraints.lock().unwrap().clone()
        }

        fn param_calls(&self) -> Vec<(String, CypherParams)> {
            self.param_calls.lock().unwrap().clone()
        }

        fn batches(&self) -> Vec<usize> {
            self.batches.lock().unwrap().clone()
        }

        /// A host context providing this graph under [`RECORDING`].
        fn host_ctx(&self) -> Arc<ContextStore> {
            let mut store = ContextStore::default();
            store.provide(&RECORDING, self.clone()).unwrap();
            Arc::new(store)
        }
    }

    crate::context_key!(static RECORDING: RecordingGraph, key = "cypher_graph_test_recording");

    // Registered like a connector's, so an app can declare recording targets.
    #[linkme::distributed_slice(crate::target_state::STATIC_ROOT_PROVIDERS)]
    static RECORDING_TABLE_PROVIDER: StaticRootProvider =
        table_root_provider::<RecordingGraph>("cocoindex/test_recording/table");

    /// An app whose environment provides `graph` under [`RECORDING`].
    async fn recording_app(graph: &RecordingGraph, db: &std::path::Path) -> crate::App {
        crate::Environment::builder()
            .db_path(db)
            .provide_key(&RECORDING, graph.clone())
            .build()
            .await
            .unwrap()
            .app("CypherGraphTest")
            .await
            .unwrap()
    }

    fn meeting_table_key() -> StableKey {
        TableKey {
            graph_key: RECORDING.name().to_string(),
            kind: TableKind::Table,
            name: "Meeting".to_string(),
        }
        .stable_key()
    }

    #[async_trait]
    impl CypherExecutor for RecordingGraph {
        fn dialect(&self) -> &'static str {
            self.dialect
        }

        fn table_provider() -> &'static StaticRootProvider {
            &RECORDING_TABLE_PROVIDER
        }

        async fn execute(&self, cypher: &str) -> Result<()> {
            self.statements.lock().unwrap().push(cypher.to_string());
            Ok(())
        }

        async fn execute_with_params(&self, cypher: &str, params: &CypherParams) -> Result<()> {
            self.param_calls
                .lock()
                .unwrap()
                .push((cypher.to_string(), params.clone()));
            Ok(())
        }

        async fn execute_batch(&self, statements: &[CypherStatement]) -> Result<()> {
            self.batches.lock().unwrap().push(statements.len());
            for statement in statements {
                self.execute_with_params(&statement.cypher, &statement.params)
                    .await?;
            }
            Ok(())
        }

        async fn execute_unique_constraint(
            &self,
            op: &str,
            entity_kind: &str,
            label: &str,
            field: &str,
        ) -> Result<()> {
            self.constraints
                .lock()
                .unwrap()
                .push(format!("{op} {entity_kind} {label}.{field}"));
            Ok(())
        }
    }

    /// The `$rows` a recorded statement binds.
    fn rows(params: &CypherParams) -> &[JsonValue] {
        params["rows"].as_array().unwrap()
    }

    fn person_spec(id_type: &str) -> TableSpec {
        TableSpec::table(
            "Person".to_string(),
            TableSchema::new([("id", ColumnDef::new(id_type))], "id").unwrap(),
            crate::statediff::ManagedBy::System,
        )
    }

    #[cfg(feature = "falkordb")]
    #[test]
    fn cypher_literals_escape_strings() {
        assert_eq!(cypher_string("a'b\\c\n"), "'a\\'b\\\\c\\n'");
    }

    #[tokio::test]
    async fn node_record_upsert_uses_bound_params() {
        // A record write must bind user values as parameters, not interpolate
        // them into the query body.
        let graph = RecordingGraph::new("neo4j");
        let mut fields = Map::new();
        fields.insert("id".to_string(), JsonValue::from("p1"));
        fields.insert("name".to_string(), JsonValue::from("O'Brien"));
        let action = RecordAction {
            record_id: KeyValue::Str("p1".to_string()),
            state: Some(RecordState {
                fields,
                relation: None,
            }),
        };

        apply_records(&graph, &person_spec("STRING"), vec![action])
            .await
            .unwrap();

        let calls = graph.param_calls();
        assert_eq!(calls.len(), 1);
        let (cypher, params) = &calls[0];
        assert_eq!(
            cypher,
            "UNWIND $rows AS row MERGE (n:`Person` {`id`: row.key}) SET n += row.props"
        );
        // The injection-prone value travels as a param, never in the query text.
        assert!(!cypher.contains("O'Brien"));
        // The primary key is stripped from the SET props.
        assert_eq!(
            rows(params),
            [serde_json::json!({ "key": "p1", "props": { "name": "O'Brien" } })]
        );
    }

    #[tokio::test]
    async fn a_batch_of_records_takes_one_statement_per_shape_per_thousand() {
        // Python applies a batch in transactions of at most 1,000 actions;
        // each transaction holds one `UNWIND` statement per statement shape.
        let graph = RecordingGraph::new("neo4j");
        let upsert = |i: i64| RecordAction {
            record_id: KeyValue::Int(i),
            state: Some(RecordState {
                fields: Map::from_iter([("n".to_string(), JsonValue::from(i))]),
                relation: None,
            }),
        };
        let delete = |i: i64| RecordAction {
            record_id: KeyValue::Int(i),
            state: None,
        };
        // Deletes interleaved with upserts: the batch applies upserts first.
        let actions = (0..2500)
            .map(|i| if i % 5 == 0 { delete(i) } else { upsert(i) })
            .collect::<Vec<_>>();

        apply_records(&graph, &person_spec("INTEGER"), actions)
            .await
            .unwrap();

        // 2,000 upserts, then 500 deletes: three transactions, the second
        // holding both shapes.
        assert_eq!(graph.batches(), vec![1, 1, 1]);
        let calls = graph.param_calls();
        let shapes = calls
            .iter()
            .map(|(cypher, params)| (cypher.as_str(), rows(params).len()))
            .collect::<Vec<_>>();
        assert_eq!(
            shapes,
            vec![
                (
                    "UNWIND $rows AS row MERGE (n:`Person` {`id`: row.key}) SET n += row.props",
                    1000
                ),
                (
                    "UNWIND $rows AS row MERGE (n:`Person` {`id`: row.key}) SET n += row.props",
                    1000
                ),
                (
                    "UNWIND $rows AS row MATCH (n:`Person` {`id`: row.key}) DETACH DELETE n",
                    500
                ),
            ]
        );
        assert_eq!(rows(&calls[0].1)[0]["key"], JsonValue::from(1));
        assert_eq!(rows(&calls[2].1)[0]["key"], JsonValue::from(0));
    }

    #[derive(Clone)]
    struct MemoryLimitedGraph {
        inner: RecordingGraph,
        max_rows: usize,
    }

    #[async_trait]
    impl CypherExecutor for MemoryLimitedGraph {
        fn dialect(&self) -> &'static str {
            "neo4j"
        }

        fn table_provider() -> &'static StaticRootProvider {
            static PROVIDER: StaticRootProvider =
                table_root_provider::<MemoryLimitedGraph>("cocoindex/test_memory_limited/table");
            &PROVIDER
        }

        async fn execute(&self, cypher: &str) -> Result<()> {
            self.inner.execute(cypher).await
        }

        async fn execute_with_params(&self, cypher: &str, params: &CypherParams) -> Result<()> {
            self.inner.execute_with_params(cypher, params).await
        }

        async fn execute_batch(&self, statements: &[CypherStatement]) -> Result<()> {
            let total: usize = statements.iter().map(|s| rows(&s.params).len()).sum();
            if total > self.max_rows {
                return Err(Error::engine("too large"));
            }
            self.inner.execute_batch(statements).await
        }

        fn is_transaction_too_large(&self, error: &Error) -> bool {
            error.to_string().contains("too large")
        }
    }

    #[tokio::test]
    async fn a_chunk_over_the_memory_limit_is_split_in_two() {
        let graph = MemoryLimitedGraph {
            inner: RecordingGraph::new("neo4j"),
            max_rows: 3,
        };
        let actions = (0..10)
            .map(|i| RecordAction {
                record_id: KeyValue::Int(i),
                state: Some(RecordState {
                    fields: Map::new(),
                    relation: None,
                }),
            })
            .collect();

        apply_records(&graph, &person_spec("INTEGER"), actions)
            .await
            .unwrap();

        // 10 → 5 + 5 → (2 + 3) + (2 + 3): every row lands exactly once, in order.
        assert_eq!(graph.inner.batches(), vec![1, 1, 1, 1]);
        let keys = graph
            .inner
            .param_calls()
            .iter()
            .flat_map(|(_, params)| rows(params).iter().map(|row| row["key"].clone()))
            .collect::<Vec<_>>();
        assert_eq!(keys, (0..10).map(JsonValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn column_type_override_replaces_one_derived_column() {
        let schema = TableSchema::new(
            [
                ("id", ColumnDef::new("STRING")),
                ("location", ColumnDef::new("STRING").nullable()),
            ],
            "id",
        )
        .unwrap()
        .with_column_type("location", "POINT")
        .unwrap();
        assert_eq!(
            schema,
            TableSchema::new(
                [
                    ("id", ColumnDef::new("STRING")),
                    ("location", ColumnDef::new("POINT").nullable()),
                ],
                "id",
            )
            .unwrap()
        );

        let err = TableSchema::new([("id", ColumnDef::new("STRING"))], "id")
            .unwrap()
            .with_column_type("missing", "POINT")
            .unwrap_err();
        assert!(err.to_string().contains("\"missing\""), "{err}");
        assert!(
            TableSchema::schemaless("id")
                .unwrap()
                .with_column_type("id", "INTEGER")
                .is_err()
        );
    }

    #[tokio::test]
    async fn node_record_delete_binds_key_param() {
        let graph = RecordingGraph::new("neo4j");
        let action = RecordAction {
            record_id: KeyValue::Int(42),
            state: None,
        };

        apply_records(&graph, &person_spec("INTEGER"), vec![action])
            .await
            .unwrap();

        let calls = graph.param_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].0,
            "UNWIND $rows AS row MATCH (n:`Person` {`id`: row.key}) DETACH DELETE n"
        );
        assert_eq!(rows(&calls[0].1), [serde_json::json!({ "key": 42 })]);
    }

    #[tokio::test]
    async fn relation_record_upsert_binds_endpoint_and_id_params() {
        let graph = RecordingGraph::new("neo4j");
        let endpoint = |name: &str| TableEndpoint {
            table_name: name.to_string(),
            primary_key: "id".to_string(),
        };
        let spec = TableSpec::relation(
            "ATTENDED".to_string(),
            endpoint("Person"),
            endpoint("Event"),
            TableSchema::schemaless("id").unwrap(),
            crate::statediff::ManagedBy::System,
        );
        let action = RecordAction {
            record_id: KeyValue::Str("rel-1".to_string()),
            state: Some(RecordState {
                fields: Map::new(),
                relation: Some(RelationEndpoints {
                    from_table: endpoint("Person"),
                    from_id: KeyValue::Str("p1".to_string()),
                    to_table: endpoint("Event"),
                    to_id: KeyValue::Int(9),
                }),
            }),
        };

        apply_records(&graph, &spec, vec![action]).await.unwrap();

        let calls = graph.param_calls();
        assert_eq!(calls.len(), 1);
        let (cypher, params) = &calls[0];
        assert_eq!(
            cypher,
            "UNWIND $rows AS row \
             MERGE (s:`Person` {`id`: row.from_id}) MERGE (t:`Event` {`id`: row.to_id}) \
             WITH s, t, row OPTIONAL MATCH (a)-[o:`ATTENDED` {`id`: row.rel_id}]->(b) WHERE a <> s OR b <> t \
             DELETE o WITH DISTINCT s, t, row \
             MERGE (s)-[r:`ATTENDED` {`id`: row.rel_id}]->(t) SET r += row.props"
        );
        assert_eq!(
            rows(params),
            [serde_json::json!({ "from_id": "p1", "to_id": 9, "rel_id": "rel-1", "props": {} })]
        );
    }

    #[test]
    fn relation_key_fragments_distinguish_int_and_string_ids() {
        let fragment = |id: KeyValue| id.key_fragment();
        assert_ne!(
            fragment(KeyValue::Int(1)),
            fragment(KeyValue::Str("1".into()))
        );
        assert_ne!(
            fragment(KeyValue::Str("1".into())),
            fragment(KeyValue::Str("s:1".into()))
        );
        // Ordinary ids keep their existing key form.
        assert_eq!(fragment(KeyValue::Int(42)), "42");
        assert_eq!(fragment(KeyValue::Str("alice".into())), "alice");
    }

    #[test]
    fn relation_keys_are_unambiguous() {
        assert_ne!(
            relation_key("ab", "c", "d", "e"),
            relation_key("a", "bc", "d", "e")
        );
    }

    #[test]
    fn identifiers_reject_cypher_punctuation() {
        assert!(validate_ident("Meeting", "table").is_ok());
        assert!(validate_ident("_rel1", "table").is_ok());
        assert!(validate_ident("bad-name", "table").is_err());
        assert!(validate_ident("1bad", "table").is_err());
        assert!(validate_ident("bad`name", "table").is_err());
    }

    #[test]
    fn dropping_a_missing_index_succeeds() {
        // FalkorDB's error for a `DROP ... INDEX` of an index that doesn't exist.
        let missing = Error::engine(
            "falkordb: Unable to drop index on :Doc(name): no such index.; query: DROP INDEX",
        );
        assert!(ignore_missing_index(Err(missing)).is_ok());
        assert!(ignore_missing_index(Err(Error::engine("falkordb: connection refused"))).is_err());
    }

    #[test]
    fn vector_index_name_is_deterministic() {
        assert_eq!(
            vector_index_name("Doc", "embedding"),
            "coco_vec_Doc__embedding"
        );
    }

    #[tokio::test]
    async fn neo4j_vector_index_cypher() {
        let graph = RecordingGraph::new("neo4j");
        graph
            .create_vector_index("Doc", "embedding", 384, VectorMetric::Cosine)
            .await
            .unwrap();
        graph.drop_vector_index("Doc", "embedding").await.unwrap();
        assert_eq!(
            graph.statements(),
            vec![
                "CREATE VECTOR INDEX `coco_vec_Doc__embedding` IF NOT EXISTS FOR (n:`Doc`) ON n.`embedding` \
                 OPTIONS { indexConfig: { `vector.dimensions`: 384, `vector.similarity_function`: 'cosine' } }",
                "DROP INDEX `coco_vec_Doc__embedding` IF EXISTS",
            ]
        );
    }

    #[tokio::test]
    async fn falkordb_vector_index_cypher() {
        let graph = RecordingGraph::new("falkordb");
        graph
            .create_vector_index("Doc", "embedding", 8, VectorMetric::Euclidean)
            .await
            .unwrap();
        graph.drop_vector_index("Doc", "embedding").await.unwrap();
        assert_eq!(
            graph.statements(),
            vec![
                "CREATE VECTOR INDEX FOR (e:`Doc`) ON (e.`embedding`) \
                 OPTIONS {dimension: 8, similarityFunction: 'euclidean'}",
                "DROP VECTOR INDEX FOR (e:`Doc`) ON (e.`embedding`)",
            ]
        );
    }

    #[tokio::test]
    async fn falkordb_supports_inner_product_but_neo4j_does_not() {
        let falkor = RecordingGraph::new("falkordb");
        falkor
            .create_vector_index("Doc", "v", 4, VectorMetric::InnerProduct)
            .await
            .unwrap();
        assert!(falkor.statements()[0].contains("similarityFunction: 'ip'"));

        let neo4j = RecordingGraph::new("neo4j");
        assert!(
            neo4j
                .create_vector_index("Doc", "v", 4, VectorMetric::InnerProduct)
                .await
                .is_err(),
            "Neo4j must reject the inner-product metric"
        );
    }

    #[tokio::test]
    async fn neo4j_node_index_cypher() {
        let graph = RecordingGraph::new("neo4j");
        let fields = vec!["name".to_string(), "year".to_string()];
        graph.create_node_index("Doc", &fields).await.unwrap();
        graph.drop_node_index("Doc", &fields).await.unwrap();
        assert_eq!(
            graph.statements(),
            vec![
                "CREATE INDEX `coco_idx_node_Doc__name__year` IF NOT EXISTS FOR (n:`Doc`) ON (n.`name`, n.`year`)",
                "DROP INDEX `coco_idx_node_Doc__name__year` IF EXISTS",
            ]
        );
    }

    #[tokio::test]
    async fn falkordb_node_index_cypher() {
        let graph = RecordingGraph::new("falkordb");
        let fields = vec!["name".to_string()];
        graph.create_node_index("Doc", &fields).await.unwrap();
        graph.drop_node_index("Doc", &fields).await.unwrap();
        assert_eq!(
            graph.statements(),
            vec![
                "CREATE INDEX FOR (e:`Doc`) ON (e.`name`)",
                "DROP INDEX FOR (e:`Doc`) ON (e.`name`)",
            ]
        );
    }

    #[tokio::test]
    async fn neo4j_table_drop_deletes_nodes_then_constraint() {
        let schema = TableSchema::new([("id", ColumnDef::new("INTEGER"))], "id").unwrap();
        let spec = TableSpec::table(
            "Meeting".to_string(),
            schema,
            crate::statediff::ManagedBy::System,
        );
        let graph = RecordingGraph::new("neo4j");

        drop_table(&graph, &spec).await.unwrap();

        assert_eq!(
            graph.statements(),
            vec![
                "MATCH (n:`Meeting`) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS",
                "DROP CONSTRAINT `coco_uniq_Meeting__id` IF EXISTS",
            ]
        );
    }

    #[tokio::test]
    async fn user_managed_table_drop_is_a_no_op() {
        let schema = TableSchema::new([("id", ColumnDef::new("INTEGER"))], "id").unwrap();
        let spec = TableSpec::table(
            "Meeting".to_string(),
            schema,
            crate::statediff::ManagedBy::User,
        );
        let graph = RecordingGraph::new("neo4j");

        drop_table(&graph, &spec).await.unwrap();

        assert!(graph.statements().is_empty());
    }

    #[tokio::test]
    async fn neo4j_relation_artifacts_are_created_and_dropped() {
        let endpoint = TableEndpoint {
            table_name: "Person".to_string(),
            primary_key: "id".to_string(),
        };
        let spec = TableSpec::relation(
            "ATTENDED".to_string(),
            endpoint.clone(),
            endpoint,
            TableSchema::schemaless("id").unwrap(),
            crate::statediff::ManagedBy::System,
        );
        let graph = RecordingGraph::new("neo4j");

        ensure_table(&graph, &spec).await.unwrap();
        drop_table(&graph, &spec).await.unwrap();

        assert_eq!(
            graph.statements(),
            vec![
                "CREATE INDEX `coco_idx_rel_ATTENDED__id` IF NOT EXISTS FOR ()-[r:`ATTENDED`]-() ON (r.`id`)",
                "MATCH ()-[r:`ATTENDED`]->() CALL { WITH r DELETE r } IN TRANSACTIONS",
                "DROP INDEX `coco_idx_rel_ATTENDED__id` IF EXISTS",
            ]
        );
    }

    #[tokio::test]
    async fn neo4j_primary_key_change_rebuilds_table() {
        let old_schema = TableSchema::new([("id", ColumnDef::new("INTEGER"))], "id").unwrap();
        let new_schema = TableSchema::new([("uuid", ColumnDef::new("STRING"))], "uuid").unwrap();
        let old_spec = TableSpec::table(
            "Meeting".to_string(),
            old_schema,
            crate::statediff::ManagedBy::System,
        );
        let new_spec = TableSpec::table(
            "Meeting".to_string(),
            new_schema,
            crate::statediff::ManagedBy::System,
        );
        let graph = RecordingGraph::new("neo4j");
        let handler = TableHandler::<RecordingGraph>::default();

        let out = handler
            .reconcile(
                meeting_table_key(),
                Some(new_spec),
                vec![MutualTrackingRecord::new(
                    old_spec,
                    crate::statediff::ManagedBy::System,
                )],
                false,
            )
            .unwrap()
            .unwrap();

        assert_eq!(
            out.child_invalidation,
            Some(crate::target_state::TargetChildInvalidation::Destructive)
        );
        let children = out
            .sink
            .apply_for_test(graph.host_ctx(), vec![out.action])
            .await
            .unwrap();
        assert!(children[0].is_some());

        assert_eq!(
            graph.statements(),
            vec![
                "MATCH (n:`Meeting`) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS",
                "DROP CONSTRAINT `coco_uniq_Meeting__id` IF EXISTS",
                "CREATE CONSTRAINT `coco_uniq_Meeting__uuid` IF NOT EXISTS FOR (n:`Meeting`) REQUIRE n.`uuid` IS UNIQUE",
            ]
        );
    }

    #[tokio::test]
    async fn schema_only_change_is_lossy_and_keeps_the_table() {
        let old_schema = TableSchema::new([("id", ColumnDef::new("INTEGER"))], "id").unwrap();
        let new_schema = TableSchema::new(
            [
                ("id", ColumnDef::new("INTEGER")),
                ("title", ColumnDef::new("STRING")),
            ],
            "id",
        )
        .unwrap();
        let old_spec = TableSpec::table(
            "Meeting".to_string(),
            old_schema,
            crate::statediff::ManagedBy::System,
        );
        let new_spec = TableSpec::table(
            "Meeting".to_string(),
            new_schema,
            crate::statediff::ManagedBy::System,
        );
        let graph = RecordingGraph::new("neo4j");
        let handler = TableHandler::<RecordingGraph>::default();

        let out = handler
            .reconcile(
                meeting_table_key(),
                Some(new_spec),
                vec![MutualTrackingRecord::new(
                    old_spec,
                    crate::statediff::ManagedBy::System,
                )],
                false,
            )
            .unwrap()
            .unwrap();

        assert_eq!(
            out.child_invalidation,
            Some(crate::target_state::TargetChildInvalidation::Lossy)
        );
        let children = out
            .sink
            .apply_for_test(graph.host_ctx(), vec![out.action])
            .await
            .unwrap();
        assert!(children[0].is_some());

        // Nothing is deleted or dropped; the constraint DDL is idempotent.
        assert_eq!(
            graph.statements(),
            vec![
                "CREATE CONSTRAINT `coco_uniq_Meeting__id` IF NOT EXISTS FOR (n:`Meeting`) REQUIRE n.`id` IS UNIQUE",
            ]
        );
    }

    #[tokio::test]
    async fn falkordb_table_drop_deletes_nodes_then_index() {
        let schema = TableSchema::new([("id", ColumnDef::new("INTEGER"))], "id").unwrap();
        let spec = TableSpec::table(
            "Meeting".to_string(),
            schema,
            crate::statediff::ManagedBy::System,
        );
        let graph = RecordingGraph::new("falkordb");

        drop_table(&graph, &spec).await.unwrap();

        assert_eq!(
            graph.statements(),
            vec![
                "MATCH (n:`Meeting`) DETACH DELETE n",
                "DROP INDEX FOR (e:`Meeting`) ON (e.`id`)",
            ]
        );
        assert_eq!(graph.constraints(), vec!["DROP NODE Meeting.id"]);
    }

    #[tokio::test]
    async fn falkordb_relation_artifacts_are_created_and_dropped() {
        let endpoint = TableEndpoint {
            table_name: "Person".to_string(),
            primary_key: "id".to_string(),
        };
        let spec = TableSpec::relation(
            "ATTENDED".to_string(),
            endpoint.clone(),
            endpoint,
            TableSchema::schemaless("id").unwrap(),
            crate::statediff::ManagedBy::System,
        );
        let graph = RecordingGraph::new("falkordb");

        ensure_table(&graph, &spec).await.unwrap();
        drop_table(&graph, &spec).await.unwrap();

        assert_eq!(
            graph.statements(),
            vec![
                "CREATE INDEX FOR ()-[e:`ATTENDED`]-() ON (e.`id`)",
                "MATCH ()-[r:`ATTENDED`]->() DELETE r",
                "DROP INDEX FOR ()-[e:`ATTENDED`]-() ON (e.`id`)",
            ]
        );
        assert_eq!(
            graph.constraints(),
            vec![
                "CREATE RELATIONSHIP ATTENDED.id",
                "DROP RELATIONSHIP ATTENDED.id",
            ]
        );
    }

    #[tokio::test]
    async fn falkordb_primary_key_change_rebuilds_table() {
        let old_schema = TableSchema::new([("id", ColumnDef::new("INTEGER"))], "id").unwrap();
        let new_schema = TableSchema::new([("uuid", ColumnDef::new("STRING"))], "uuid").unwrap();
        let old_spec = TableSpec::table(
            "Meeting".to_string(),
            old_schema,
            crate::statediff::ManagedBy::System,
        );
        let new_spec = TableSpec::table(
            "Meeting".to_string(),
            new_schema,
            crate::statediff::ManagedBy::System,
        );
        let graph = RecordingGraph::new("falkordb");
        let handler = TableHandler::<RecordingGraph>::default();

        let out = handler
            .reconcile(
                meeting_table_key(),
                Some(new_spec),
                vec![MutualTrackingRecord::new(
                    old_spec,
                    crate::statediff::ManagedBy::System,
                )],
                false,
            )
            .unwrap()
            .unwrap();

        assert_eq!(
            out.child_invalidation,
            Some(crate::target_state::TargetChildInvalidation::Destructive)
        );
        let children = out
            .sink
            .apply_for_test(graph.host_ctx(), vec![out.action])
            .await
            .unwrap();
        assert!(children[0].is_some());

        assert_eq!(
            graph.statements(),
            vec![
                "MATCH (n:`Meeting`) DETACH DELETE n",
                "DROP INDEX FOR (e:`Meeting`) ON (e.`id`)",
                "CREATE INDEX FOR (e:`Meeting`) ON (e.`uuid`)",
            ]
        );
        assert_eq!(
            graph.constraints(),
            vec!["DROP NODE Meeting.id", "CREATE NODE Meeting.uuid"]
        );
    }

    #[test]
    fn user_managed_desired_table_keeps_child_without_schema_invalidation() {
        let schema = TableSchema::new([("id", ColumnDef::new("INTEGER"))], "id").unwrap();
        let system_spec = TableSpec::table(
            "Meeting".to_string(),
            schema.clone(),
            crate::statediff::ManagedBy::System,
        );
        let user_spec = TableSpec::table(
            "Meeting".to_string(),
            schema,
            crate::statediff::ManagedBy::User,
        );
        let handler = TableHandler::<FakeGraph>::default();
        let out = handler
            .reconcile(
                meeting_table_key(),
                Some(user_spec),
                vec![MutualTrackingRecord::new(
                    system_spec,
                    crate::statediff::ManagedBy::System,
                )],
                false,
            )
            .unwrap()
            .unwrap();
        assert_eq!(out.child_invalidation, None);
        assert_eq!(
            out.tracking_record.unwrap().managed_by,
            crate::statediff::ManagedBy::User
        );
    }

    #[test]
    fn user_managed_previous_table_is_not_dropped_when_target_disappears() {
        let schema = TableSchema::new([("id", ColumnDef::new("INTEGER"))], "id").unwrap();
        let user_spec = TableSpec::table(
            "Meeting".to_string(),
            schema,
            crate::statediff::ManagedBy::User,
        );
        let handler = TableHandler::<FakeGraph>::default();
        let out = handler
            .reconcile(
                meeting_table_key(),
                None,
                vec![MutualTrackingRecord::new(
                    user_spec,
                    crate::statediff::ManagedBy::User,
                )],
                false,
            )
            .unwrap();
        assert!(out.is_none());
    }

    #[tokio::test]
    async fn schemaless_table_writes_records_as_they_serialize() {
        let graph = RecordingGraph::new("neo4j");
        let tmp = tempfile::tempdir().unwrap();
        let app = recording_app(&graph, tmp.path()).await;

        app.run(|ctx| async move {
            let people = mount_table_target_with_options(
                &ctx,
                &RECORDING,
                "Person",
                TableSchema::schemaless("email")?,
                ManagedTargetOptions::default(),
            )
            .await?;
            people.declare_record(
                &ctx,
                "a@x.io",
                &serde_json::json!({ "email": "a@x.io", "name": "alice", "age": 30 }),
            )
        })
        .await
        .unwrap();

        // The table is keyed by the schemaless schema's primary key.
        assert_eq!(
            graph.statements(),
            vec![
                "CREATE CONSTRAINT `coco_uniq_Person__email` IF NOT EXISTS FOR (n:`Person`) REQUIRE n.`email` IS UNIQUE",
            ]
        );
        let calls = graph.param_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].0,
            "UNWIND $rows AS row MERGE (n:`Person` {`email`: row.key}) SET n += row.props"
        );
        assert_eq!(
            rows(&calls[0].1)[0]["props"],
            serde_json::json!({ "name": "alice", "age": 30 })
        );
    }

    #[tokio::test]
    async fn declaring_a_schema_for_a_schemaless_table_keeps_the_table() {
        let schemaless = TableSpec::table(
            "Meeting".to_string(),
            TableSchema::schemaless("id").unwrap(),
            crate::statediff::ManagedBy::System,
        );
        let with_schema = TableSpec::table(
            "Meeting".to_string(),
            TableSchema::new([("id", ColumnDef::new("INTEGER"))], "id").unwrap(),
            crate::statediff::ManagedBy::System,
        );
        let handler = TableHandler::<RecordingGraph>::default();

        let out = handler
            .reconcile(
                meeting_table_key(),
                Some(with_schema),
                vec![MutualTrackingRecord::new(
                    schemaless,
                    crate::statediff::ManagedBy::System,
                )],
                false,
            )
            .unwrap()
            .unwrap();

        // Same MERGE key: the records re-upsert, nothing is rebuilt.
        assert!(matches!(
            out.action,
            TargetAction::Update(TableAction {
                op: TableOp::Ensure(_),
                ..
            })
        ));
        assert_eq!(
            out.child_invalidation,
            Some(crate::target_state::TargetChildInvalidation::Lossy)
        );
    }

    /// Mounts `Person` and a `KNOWS` relation with `relation_schema`, then
    /// declares one relation from `a` to `b` carrying `record`.
    async fn run_knows_relation(
        graph: &RecordingGraph,
        relation_schema: TableSchema,
        record: JsonValue,
    ) -> Result<()> {
        let tmp = tempfile::tempdir().unwrap();
        let app = recording_app(graph, tmp.path()).await;
        app.run(move |ctx| {
            let relation_schema = relation_schema.clone();
            let record = record.clone();
            async move {
                let options = ManagedTargetOptions::default;
                let people = mount_table_target_with_options(
                    &ctx,
                    &RECORDING,
                    "Person",
                    TableSchema::schemaless("id")?,
                    options(),
                )
                .await?;
                let knows = mount_relation_target_with_options(
                    &ctx,
                    &RECORDING,
                    "KNOWS",
                    &people,
                    &people,
                    relation_schema,
                    options(),
                )
                .await?;
                knows.declare_relation_record(&ctx, "a", "b", &record)
            }
        })
        .await
        .map(|_| ())
    }

    #[tokio::test]
    async fn relation_record_is_keyed_by_its_primary_key() {
        let graph = RecordingGraph::new("neo4j");
        let schema = TableSchema::new(
            [
                ("edge_id", ColumnDef::new("STRING")),
                ("since", ColumnDef::new("INTEGER")),
            ],
            "edge_id",
        )
        .unwrap();

        run_knows_relation(
            &graph,
            schema,
            serde_json::json!({ "edge_id": "e1", "since": 2020 }),
        )
        .await
        .unwrap();

        assert!(graph.statements().contains(
            &"CREATE INDEX `coco_idx_rel_KNOWS__edge_id` IF NOT EXISTS FOR ()-[r:`KNOWS`]-() ON (r.`edge_id`)"
                .to_string()
        ));
        let calls = graph.param_calls();
        assert_eq!(calls.len(), 1);
        let (cypher, params) = &calls[0];
        assert_eq!(
            cypher,
            "UNWIND $rows AS row \
             MERGE (s:`Person` {`id`: row.from_id}) MERGE (t:`Person` {`id`: row.to_id}) \
             WITH s, t, row OPTIONAL MATCH (a)-[o:`KNOWS` {`edge_id`: row.rel_id}]->(b) WHERE a <> s OR b <> t \
             DELETE o WITH DISTINCT s, t, row \
             MERGE (s)-[r:`KNOWS` {`edge_id`: row.rel_id}]->(t) SET r += row.props"
        );
        // The record's key identifies the relationship and is not a property
        // to set.
        let row = &rows(params)[0];
        assert_eq!(row["rel_id"], JsonValue::from("e1"));
        assert_eq!(row["props"], serde_json::json!({ "since": 2020 }));
    }

    #[tokio::test]
    async fn relation_record_without_its_primary_key_gets_a_derived_id() {
        let graph = RecordingGraph::new("neo4j");

        run_knows_relation(
            &graph,
            TableSchema::schemaless("edge_id").unwrap(),
            serde_json::json!({ "since": 2020 }),
        )
        .await
        .unwrap();

        let calls = graph.param_calls();
        assert_eq!(calls.len(), 1);
        assert!(
            calls[0].0.contains("{`edge_id`: row.rel_id}"),
            "{}",
            calls[0].0
        );
        assert_eq!(
            rows(&calls[0].1)[0]["rel_id"],
            JsonValue::from(relation_key("Person", "a", "Person", "b"))
        );
    }

    #[tokio::test]
    async fn relation_record_rejects_a_primary_key_that_is_not_a_scalar_id() {
        let graph = RecordingGraph::new("neo4j");

        let err = run_knows_relation(
            &graph,
            TableSchema::schemaless("id").unwrap(),
            serde_json::json!({ "id": [1, 2] }),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(err.contains("must be a string or an integer"), "{err}");
        assert!(graph.param_calls().is_empty());
    }

    #[tokio::test]
    async fn relation_record_delete_matches_its_primary_key() {
        let graph = RecordingGraph::new("neo4j");
        let endpoint = TableEndpoint {
            table_name: "Person".to_string(),
            primary_key: "id".to_string(),
        };
        let spec = TableSpec::relation(
            "KNOWS".to_string(),
            endpoint.clone(),
            endpoint,
            TableSchema::schemaless("edge_id").unwrap(),
            crate::statediff::ManagedBy::System,
        );
        let action = RecordAction {
            record_id: KeyValue::Int(7),
            state: None,
        };

        apply_records(&graph, &spec, vec![action]).await.unwrap();

        let calls = graph.param_calls();
        assert_eq!(
            calls[0].0,
            "UNWIND $rows AS row MATCH ()-[r:`KNOWS` {`edge_id`: row.rel_id}]->() DELETE r"
        );
        assert_eq!(rows(&calls[0].1), [serde_json::json!({ "rel_id": 7 })]);
    }

    /// Runs an app that mounts `Doc` and declares one vector index per entry
    /// of `indexes` (`(name, field)`; `None` takes the default name).
    async fn run_vector_indexes(
        app: &crate::App,
        indexes: &'static [(Option<&'static str>, &'static str)],
    ) -> Result<()> {
        app.run(move |ctx| async move {
            let docs = mount_table_target_with_options(
                &ctx,
                &RECORDING,
                "Doc",
                TableSchema::schemaless("id")?,
                ManagedTargetOptions::default(),
            )
            .await?;
            for (name, field) in indexes {
                docs.declare_vector_index(&ctx, *name, field, 3, VectorMetric::Cosine)?;
            }
            Ok(())
        })
        .await
        .map(|_| ())
    }

    fn vector_index_statements(graph: &RecordingGraph) -> Vec<String> {
        graph
            .statements()
            .into_iter()
            .filter(|s| s.contains("coco_vec_"))
            .collect()
    }

    #[tokio::test]
    async fn renaming_a_vector_index_rebuilds_it_once() {
        let graph = RecordingGraph::new("neo4j");
        let tmp = tempfile::tempdir().unwrap();
        let app = recording_app(&graph, tmp.path()).await;
        let create = "CREATE VECTOR INDEX `coco_vec_Doc__embedding` IF NOT EXISTS FOR (n:`Doc`) ON n.`embedding` \
                      OPTIONS { indexConfig: { `vector.dimensions`: 3, `vector.similarity_function`: 'cosine' } }";
        let drop = "DROP INDEX `coco_vec_Doc__embedding` IF EXISTS";

        run_vector_indexes(&app, &[(None, "embedding")])
            .await
            .unwrap();
        assert_eq!(vector_index_statements(&graph), vec![drop, create]);

        // The same declaration under an explicit name: a create under the new
        // name and a drop under the old one, both of one database index. The
        // index is recreated and the old name's drop is skipped.
        run_vector_indexes(&app, &[(Some("doc_embedding"), "embedding")])
            .await
            .unwrap();
        assert_eq!(
            vector_index_statements(&graph),
            vec![drop, create, drop, create]
        );

        // No longer declared: the index is dropped.
        run_vector_indexes(&app, &[]).await.unwrap();
        assert_eq!(
            vector_index_statements(&graph),
            vec![drop, create, drop, create, drop]
        );
    }

    #[tokio::test]
    async fn repointing_a_named_vector_index_drops_the_old_field_index() {
        let graph = RecordingGraph::new("neo4j");
        let tmp = tempfile::tempdir().unwrap();
        let app = recording_app(&graph, tmp.path()).await;
        let drop_a = "DROP INDEX `coco_vec_Doc__a` IF EXISTS";
        let drop_b = "DROP INDEX `coco_vec_Doc__b` IF EXISTS";
        let is_create = |s: &String, field: &str| {
            s.starts_with(&format!("CREATE VECTOR INDEX `coco_vec_Doc__{field}`"))
        };

        run_vector_indexes(&app, &[(Some("emb"), "a")])
            .await
            .unwrap();
        let before = vector_index_statements(&graph).len();

        // One name, another field: the index on `a` is dropped and not
        // recreated; the index on `b` is created.
        run_vector_indexes(&app, &[(Some("emb"), "b")])
            .await
            .unwrap();
        let statements = vector_index_statements(&graph);
        let repoint = &statements[before..];
        assert_eq!(repoint.len(), 3, "{repoint:?}");
        assert_eq!(repoint[0], drop_a);
        assert_eq!(repoint[1], drop_b);
        assert!(is_create(&repoint[2], "b"), "{repoint:?}");

        // `emb` moves on to `c` in the run where `other` takes `b`: another
        // name recreates the released `b`, so it is not dropped for `emb`.
        let before = statements.len();
        run_vector_indexes(&app, &[(Some("emb"), "c"), (Some("other"), "b")])
            .await
            .unwrap();
        let statements = vector_index_statements(&graph);
        let takeover = &statements[before..];
        assert_eq!(
            takeover.iter().filter(|s| **s == drop_b).count(),
            1,
            "only the drop that precedes the create of `b`: {takeover:?}"
        );
        assert_eq!(
            takeover.iter().filter(|s| is_create(s, "b")).count(),
            1,
            "{takeover:?}"
        );
        assert!(takeover.iter().any(|s| is_create(s, "c")), "{takeover:?}");
    }

    #[tokio::test]
    async fn relation_endpoint_change_under_an_explicit_key_updates_one_edge() {
        let graph = RecordingGraph::new("neo4j");
        let tmp = tempfile::tempdir().unwrap();
        let app = recording_app(&graph, tmp.path()).await;

        // (a, b, e1), then (a, c, e1): one key, so one update of one edge.
        for to_id in ["b", "c"] {
            app.run(move |ctx| async move {
                let options = ManagedTargetOptions::default;
                let people = mount_table_target_with_options(
                    &ctx,
                    &RECORDING,
                    "Person",
                    TableSchema::schemaless("id")?,
                    options(),
                )
                .await?;
                let knows = mount_relation_target_with_options(
                    &ctx,
                    &RECORDING,
                    "KNOWS",
                    &people,
                    &people,
                    TableSchema::schemaless("edge_id")?,
                    options(),
                )
                .await?;
                knows.declare_relation_record(
                    &ctx,
                    "a",
                    to_id,
                    &serde_json::json!({ "edge_id": "e1" }),
                )
            })
            .await
            .unwrap();
        }

        let calls = graph.param_calls();
        assert_eq!(calls.len(), 2, "{calls:?}");
        let (cypher, params) = &calls[1];
        // The upsert removes the edge keyed `e1` between other endpoints
        // before it merges the edge between the new ones; nothing deletes by
        // key alone, which would also remove the new edge.
        assert!(
            cypher.contains(
                "OPTIONAL MATCH (a)-[o:`KNOWS` {`edge_id`: row.rel_id}]->(b) WHERE a <> s OR b <> t DELETE o"
            ),
            "{cypher}"
        );
        assert!(
            cypher
                .ends_with("MERGE (s)-[r:`KNOWS` {`edge_id`: row.rel_id}]->(t) SET r += row.props"),
            "{cypher}"
        );
        let row = &rows(params)[0];
        assert_eq!(row["rel_id"], JsonValue::from("e1"));
        assert_eq!(row["to_id"], JsonValue::from("c"));
    }

    #[tokio::test]
    async fn one_field_takes_one_vector_index() {
        let graph = RecordingGraph::new("neo4j");
        let tmp = tempfile::tempdir().unwrap();
        let app = recording_app(&graph, tmp.path()).await;

        let err = run_vector_indexes(&app, &[(Some("a"), "embedding"), (Some("b"), "embedding")])
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("\"a\" and \"b\" both index Doc.embedding"),
            "{err}"
        );
        assert!(vector_index_statements(&graph).is_empty());
    }

    #[tokio::test]
    async fn vector_index_rejects_a_zero_dimension_and_a_bad_name() {
        let graph = RecordingGraph::new("neo4j");
        let tmp = tempfile::tempdir().unwrap();
        let app = recording_app(&graph, tmp.path()).await;

        let err = app
            .run(|ctx| async move {
                let docs = mount_table_target_with_options(
                    &ctx,
                    &RECORDING,
                    "Doc",
                    TableSchema::schemaless("id")?,
                    ManagedTargetOptions::default(),
                )
                .await?;
                let bad_name =
                    docs.declare_vector_index(&ctx, Some("no-dash"), "v", 3, VectorMetric::Cosine);
                assert!(
                    bad_name
                        .unwrap_err()
                        .to_string()
                        .contains("vector index name")
                );
                docs.declare_vector_index(&ctx, None, "v", 0, VectorMetric::Cosine)
            })
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("Invalid vector dimension: 0"), "{err}");
    }
}
