//! Snowflake table target connector.
//!
//! Table targets reconcile declared rows against the previous run: changed rows
//! are upserted (`MERGE INTO`), unchanged rows are skipped, and rows no longer
//! declared are deleted. `managed_by` controls whether CocoIndex owns the table
//! DDL (database, schema and table creation, `ALTER TABLE` column changes,
//! `DROP TABLE`).
//!
//! Snowflake has no official Rust client, so this speaks the HTTP protocol the
//! official connectors (`snowflake-connector-python`, `gosnowflake`) use, via
//! `reqwest`: a password login against `/session/v1/login-request` that yields
//! a session token, statements with server-side `?` bindings through
//! `/queries/v1/query-request`, and a session close at the end of each batch.
//! Authentication is the same as Python's `snowflake.ConnectionConfig`:
//! account, user and password, with an optional warehouse and role.
//!
//! Use [`table_target`] to build a composable target state,
//! [`declare_table_target`] inside the current component, or
//! [`mount_table_target`] when rows must be declared immediately. The
//! `database`, `schema` and `managed_by` are set through
//! [`SnowflakeTableOptions`].

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue, json};

use crate::ctx::{ContextKey, ContextStore, Ctx};
use crate::error::{Error, Result};
use crate::sql_ident::validate_ident;
use crate::statediff::{
    CompositeTrackingRecord, DiffAction, ManagedBy, MutualTrackingRecord, diff, diff_composite,
    resolve_system_transition,
};
use crate::target_state::{
    ChildTargetDef, STATIC_ROOT_PROVIDERS, SinkCache, StableKey, StaticRootProvider, TargetAction,
    TargetActionSink, TargetChildInvalidation, TargetHandler, TargetReconcileOutput, TargetState,
    TargetStateProvider, declare_target_state, declare_target_state_with_child, mount_target,
};

const CLIENT_APP_ID: &str = "CocoIndexRust";
/// Snowflake answers these result codes while a statement is still running.
const QUERY_IN_PROGRESS_CODES: [&str; 2] = ["333333", "333334"];
const RESULT_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// Rows per `MERGE` / keys per `DELETE` statement.
const BATCH_SIZE: usize = 500;

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// Connection configuration. Mirrors Python's `snowflake.ConnectionConfig`.
#[derive(Clone)]
pub struct SnowflakeConfig {
    /// Account identifier, e.g. `ORGNAME-ACCOUNTNAME` (no
    /// `.snowflakecomputing.com` suffix).
    pub account: String,
    /// Login name.
    pub user: String,
    pub password: String,
    /// Warehouse that runs the DDL and DML.
    pub warehouse: Option<String>,
    /// Role for the session.
    pub role: Option<String>,
}

impl SnowflakeConfig {
    pub fn new(
        account: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            account: account.into(),
            user: user.into(),
            password: password.into(),
            warehouse: None,
            role: None,
        }
    }

    pub fn warehouse(mut self, warehouse: impl Into<String>) -> Self {
        self.warehouse = Some(warehouse.into());
        self
    }

    pub fn role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }

    /// The account's HTTPS endpoint. Underscores in an account identifier are
    /// not valid in a hostname; the official connectors replace them with `-`.
    fn base_url(&self) -> String {
        format!(
            "https://{}.snowflakecomputing.com",
            self.account.replace('_', "-")
        )
    }
}

impl fmt::Debug for SnowflakeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnowflakeConfig")
            .field("account", &self.account)
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .field("warehouse", &self.warehouse)
            .field("role", &self.role)
            .finish()
    }
}

struct ConnectionInner {
    http: reqwest::Client,
    base_url: String,
    config: SnowflakeConfig,
    state_id: String,
}

/// A Snowflake connection handle. Clone-cheap (the HTTP client is shared).
/// Each batch of statements logs in, runs, and closes its session. Register it
/// under a [`ContextKey`] and pass that key to the table-target constructors.
#[derive(Clone)]
pub struct SnowflakeConnection {
    inner: Arc<ConnectionInner>,
}

impl SnowflakeConnection {
    /// Build a connection. No request is made until the first statement.
    pub fn new(config: SnowflakeConfig) -> Self {
        let state_id = format!(
            "snowflake:{}:{}:{}:{}",
            config.account,
            config.user,
            config.warehouse.as_deref().unwrap_or_default(),
            config.role.as_deref().unwrap_or_default()
        );
        Self {
            inner: Arc::new(ConnectionInner {
                http: reqwest::Client::new(),
                base_url: config.base_url(),
                config,
                state_id,
            }),
        }
    }

    /// Override the account endpoint (default
    /// `https://<account>.snowflakecomputing.com`). Mainly for pointing the
    /// connection at a mock server in tests.
    pub fn with_base_url(self, base_url: impl Into<String>) -> Self {
        let inner = self.inner.as_ref();
        Self {
            inner: Arc::new(ConnectionInner {
                http: inner.http.clone(),
                base_url: base_url.into().trim_end_matches('/').to_string(),
                config: inner.config.clone(),
                state_id: inner.state_id.clone(),
            }),
        }
    }

    pub fn config(&self) -> &SnowflakeConfig {
        &self.inner.config
    }

    /// Stable identity (password excluded).
    pub fn state_id(&self) -> &str {
        &self.inner.state_id
    }

    /// Run one statement in a fresh session and return its result rows as
    /// objects keyed by column name. `FIXED`/`REAL`/`BOOLEAN` cells are decoded
    /// to JSON numbers/booleans; everything else is returned as the API's string
    /// form. Useful for reading a target table back.
    pub async fn query(&self, sql: &str) -> Result<Vec<Map<String, JsonValue>>> {
        let mut session = self.login().await?;
        let result = session.execute(sql, &[]).await;
        session.close().await;
        result
    }

    /// Open a session (`/session/v1/login-request`) with password auth.
    async fn login(&self) -> Result<Session> {
        let inner = &self.inner;
        let config = &inner.config;
        let mut query: Vec<(&str, String)> = vec![
            ("requestId", uuid::Uuid::new_v4().to_string()),
            ("request_guid", uuid::Uuid::new_v4().to_string()),
        ];
        if let Some(warehouse) = &config.warehouse {
            query.push(("warehouse", warehouse.clone()));
        }
        if let Some(role) = &config.role {
            query.push(("roleName", role.clone()));
        }
        let body = json!({
            "data": {
                "CLIENT_APP_ID": CLIENT_APP_ID,
                "CLIENT_APP_VERSION": env!("CARGO_PKG_VERSION"),
                "ACCOUNT_NAME": config.account,
                "LOGIN_NAME": config.user,
                "PASSWORD": config.password,
                "AUTHENTICATOR": "SNOWFLAKE",
                "CLIENT_ENVIRONMENT": {
                    "APPLICATION": CLIENT_APP_ID,
                    "OS": std::env::consts::OS,
                },
            }
        });
        let url = format!("{}/session/v1/login-request", inner.base_url);
        let response = send_json(inner.http.post(&url).query(&query).json(&body), None).await?;
        let data = check_success(&response, "login")?;
        let token = data
            .get("token")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| Error::engine("snowflake login response has no session token"))?
            .to_string();
        Ok(Session {
            conn: self.clone(),
            token,
            sequence_id: 0,
        })
    }
}

/// One logged-in session: a token and a statement sequence.
struct Session {
    conn: SnowflakeConnection,
    token: String,
    sequence_id: u64,
}

impl Session {
    /// Run one statement with positional `?` bindings and return its rows.
    async fn execute(
        &mut self,
        sql: &str,
        bindings: &[Binding],
    ) -> Result<Vec<Map<String, JsonValue>>> {
        let inner = &self.conn.inner;
        self.sequence_id += 1;
        let mut body = json!({
            "sqlText": sql,
            "asyncExec": false,
            "sequenceId": self.sequence_id,
            "isInternal": false,
            "parameters": {},
        });
        if !bindings.is_empty() {
            let map: Map<String, JsonValue> = bindings
                .iter()
                .enumerate()
                .map(|(index, binding)| ((index + 1).to_string(), binding.to_api()))
                .collect();
            body["bindings"] = JsonValue::Object(map);
        }
        let url = format!("{}/queries/v1/query-request", inner.base_url);
        let query = [
            ("requestId", uuid::Uuid::new_v4().to_string()),
            ("request_guid", uuid::Uuid::new_v4().to_string()),
        ];
        let mut response = send_json(
            inner.http.post(&url).query(&query).json(&body),
            Some(&self.token),
        )
        .await?;
        // A long statement answers "in progress" with a URL to poll for the
        // result.
        while response
            .get("code")
            .and_then(JsonValue::as_str)
            .is_some_and(|code| QUERY_IN_PROGRESS_CODES.contains(&code))
        {
            let result_url = response
                .get("data")
                .and_then(|d| d.get("getResultUrl"))
                .and_then(JsonValue::as_str)
                .ok_or_else(|| {
                    Error::engine("snowflake: query in progress without getResultUrl")
                })?;
            tokio::time::sleep(RESULT_POLL_INTERVAL).await;
            let url = format!("{}{result_url}", inner.base_url);
            response = send_json(inner.http.get(&url), Some(&self.token)).await?;
        }
        let data = check_success(&response, "statement")?;
        Ok(decode_rows(data))
    }

    /// Close the session. A failure here is logged, not returned: the work
    /// already committed, and the server expires an unclosed session on its
    /// own.
    async fn close(self) {
        let inner = &self.conn.inner;
        let url = format!("{}/session", inner.base_url);
        let query = [
            ("delete", "true".to_string()),
            ("requestId", uuid::Uuid::new_v4().to_string()),
            ("request_guid", uuid::Uuid::new_v4().to_string()),
        ];
        if let Err(e) = send_json(
            inner.http.post(&url).query(&query).json(&json!({})),
            Some(&self.token),
        )
        .await
        {
            tracing::debug!("snowflake session close failed: {e}");
        }
    }
}

async fn send_json(request: reqwest::RequestBuilder, token: Option<&str>) -> Result<JsonValue> {
    let mut request = request
        .header(reqwest::header::ACCEPT, "application/snowflake")
        .header(reqwest::header::USER_AGENT, CLIENT_APP_ID);
    if let Some(token) = token {
        request = request.header(
            reqwest::header::AUTHORIZATION,
            format!("Snowflake Token=\"{token}\""),
        );
    }
    let response = request
        .send()
        .await
        .map_err(|e| Error::engine(format!("snowflake request: {e}")))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|e| Error::engine(format!("snowflake response: {e}")))?;
    if !status.is_success() {
        return Err(Error::engine(format!(
            "snowflake: HTTP {status}: {}",
            truncate(&text, 500)
        )));
    }
    serde_json::from_str(&text)
        .map_err(|e| Error::engine(format!("snowflake: invalid JSON response: {e}")))
}

/// Return the `data` of a successful protocol response, or the server's
/// message as an error.
fn check_success<'a>(response: &'a JsonValue, what: &str) -> Result<&'a JsonValue> {
    if response.get("success").and_then(JsonValue::as_bool) == Some(true) {
        return Ok(response.get("data").unwrap_or(&JsonValue::Null));
    }
    let message = response
        .get("message")
        .and_then(JsonValue::as_str)
        .unwrap_or("unknown error");
    let code = response
        .get("code")
        .and_then(JsonValue::as_str)
        .map(|c| format!(" (code {c})"))
        .unwrap_or_default();
    Err(Error::engine(format!(
        "snowflake {what} failed{code}: {message}"
    )))
}

/// Decode a result's `rowset` (rows of string-or-null cells) into objects keyed
/// by the `rowtype` column names.
fn decode_rows(data: &JsonValue) -> Vec<Map<String, JsonValue>> {
    let columns: Vec<(&str, &str)> = data
        .get("rowtype")
        .and_then(JsonValue::as_array)
        .map(|cols| {
            cols.iter()
                .map(|c| {
                    (
                        c.get("name").and_then(JsonValue::as_str).unwrap_or(""),
                        c.get("type").and_then(JsonValue::as_str).unwrap_or(""),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let Some(rows) = data.get("rowset").and_then(JsonValue::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .map(|row| {
            let cells = row.as_array();
            columns
                .iter()
                .enumerate()
                .map(|(index, (name, ty))| {
                    let raw = cells
                        .and_then(|c| c.get(index))
                        .cloned()
                        .unwrap_or(JsonValue::Null);
                    ((*name).to_string(), decode_cell(ty, raw))
                })
                .collect()
        })
        .collect()
}

fn decode_cell(ty: &str, raw: JsonValue) -> JsonValue {
    let JsonValue::String(text) = &raw else {
        return raw;
    };
    match ty {
        "fixed" => text
            .parse::<i64>()
            .map(JsonValue::from)
            .or_else(|_| text.parse::<f64>().map(JsonValue::from))
            .unwrap_or(raw),
        "real" => text.parse::<f64>().map(JsonValue::from).unwrap_or(raw),
        "boolean" => match text.as_str() {
            "1" | "true" | "TRUE" => JsonValue::Bool(true),
            "0" | "false" | "FALSE" => JsonValue::Bool(false),
            _ => raw,
        },
        _ => raw,
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// A Snowflake column. `snowflake_type` is the SQL type as written in
/// `CREATE TABLE` (e.g. `NUMBER`, `VARCHAR`, `VARIANT`, `NUMBER(38, 0)`). A
/// column marked [`ColumnDef::parse_json`] receives its value JSON-serialized
/// and written through `PARSE_JSON`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnDef {
    pub snowflake_type: String,
    pub nullable: bool,
    pub use_parse_json: bool,
}

impl ColumnDef {
    pub fn new(snowflake_type: impl Into<String>) -> Self {
        Self {
            snowflake_type: snowflake_type.into().trim().to_string(),
            nullable: true,
            use_parse_json: false,
        }
    }

    /// Mark the column `NOT NULL`.
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Write the value as JSON text through `PARSE_JSON` (for `VARIANT`,
    /// `OBJECT` and `ARRAY` columns).
    pub fn parse_json(mut self) -> Self {
        self.use_parse_json = true;
        self
    }

    /// The `VARIANT` column every complex (collection / map / struct) field
    /// maps to.
    fn variant() -> Self {
        Self::new("VARIANT").parse_json()
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
            validate_sql_type(&def.snowflake_type)?;
            if out.insert(name.clone(), def).is_some() {
                return Err(Error::engine(format!(
                    "Snowflake table schema contains duplicate column {name:?}"
                )));
            }
        }
        let primary_key: Vec<String> = primary_key.into_iter().map(Into::into).collect();
        if primary_key.is_empty() {
            return Err(Error::engine("Snowflake table primary key cannot be empty"));
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

    /// Derive a schema from a `#[derive(SchemaFields)]` row type (the Rust
    /// analogue of Python's `TableSchema.from_class`), using the same leaf-type
    /// table as Python's `snowflake` connector.
    pub fn from_row<T: crate::row_schema::SchemaFields>(
        primary_key: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let columns = T::schema_fields()
            .into_iter()
            .map(|f| (f.name.clone(), snowflake_column_def(&f)));
        Self::new(columns, primary_key)
    }

    /// Columns in DDL order: primary-key columns first (in key order), then the
    /// rest by name.
    fn ordered_columns(&self) -> Vec<(&String, &ColumnDef)> {
        let pk: HashSet<&String> = self.primary_key.iter().collect();
        let mut out: Vec<(&String, &ColumnDef)> = self
            .primary_key
            .iter()
            .map(|name| (name, &self.columns[name]))
            .collect();
        out.extend(self.columns.iter().filter(|(name, _)| !pk.contains(name)));
        out
    }

    fn non_pk_columns(&self) -> Vec<&String> {
        let pk: HashSet<&String> = self.primary_key.iter().collect();
        self.ordered_columns()
            .into_iter()
            .map(|(name, _)| name)
            .filter(|name| !pk.contains(name))
            .collect()
    }
}

/// Map a connector-agnostic [`SchemaField`](crate::row_schema::SchemaField) to a
/// Snowflake [`ColumnDef`], mirroring Python's `snowflake` `_LEAF_TYPE_MAPPINGS`.
fn snowflake_column_def(field: &crate::row_schema::SchemaField) -> ColumnDef {
    use crate::row_schema::LogicalType as L;
    let mut def = match &field.logical_type {
        L::Bool => ColumnDef::new("BOOLEAN"),
        // Python maps `int`, `Decimal` and `timedelta` (seconds) to NUMBER.
        L::Int16 | L::Int32 | L::Int64 | L::Decimal | L::Duration => ColumnDef::new("NUMBER"),
        L::Float32 | L::Float64 => ColumnDef::new("FLOAT"),
        L::Text | L::Uuid => ColumnDef::new("VARCHAR"),
        L::Bytes => ColumnDef::new("BINARY"),
        L::Date => ColumnDef::new("DATE"),
        L::Time => ColumnDef::new("TIME"),
        L::DateTime => ColumnDef::new("TIMESTAMP_TZ"),
        L::Json => ColumnDef::variant(),
        L::Vector { .. } => ColumnDef::new("ARRAY").parse_json(),
        L::Custom(s) => ColumnDef::new(s.as_str()),
    };
    def.nullable = field.nullable;
    def
}

/// Options for the `*_with_options` table-target constructors.
#[derive(Clone, Debug, Default)]
pub struct SnowflakeTableOptions {
    /// Database holding the table (created if missing). `None` uses the
    /// session's current database.
    pub database: Option<String>,
    /// Schema holding the table (created if missing). `None` uses the session's
    /// current schema.
    pub schema: Option<String>,
    pub managed_by: ManagedBy,
}

// ---------------------------------------------------------------------------
// Public target API: constructor / declaration / mount split
// ---------------------------------------------------------------------------

/// A declarative Snowflake table target — a handle to declare rows on.
#[derive(Clone)]
pub struct TableTarget {
    table_name: Arc<str>,
    table_schema: TableSchema,
    rows: TargetStateProvider<RowState>,
}

/// Build a composable [`TargetState`] for a system-managed Snowflake table in
/// the session's current database and schema.
pub fn table_target(
    ctx: &Ctx,
    conn: &ContextKey<SnowflakeConnection>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
) -> Result<TargetState<TableSpec>> {
    table_target_with_options(
        ctx,
        conn,
        table_name,
        table_schema,
        SnowflakeTableOptions::default(),
    )
}

/// [`table_target`] with explicit [`SnowflakeTableOptions`] (`database`,
/// `schema`, `managed_by`).
pub fn table_target_with_options(
    ctx: &Ctx,
    conn: &ContextKey<SnowflakeConnection>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    options: SnowflakeTableOptions,
) -> Result<TargetState<TableSpec>> {
    let table_name = table_name.into();
    validate_ident(&table_name, "table name")?;
    if let Some(database) = &options.database {
        validate_ident(database, "database name")?;
    }
    if let Some(schema) = &options.schema {
        validate_ident(schema, "schema name")?;
    }
    let key = TableKey {
        conn_key: conn.name().to_string(),
        database: options.database,
        schema: options.schema,
        table_name,
    };
    let provider = TABLE_PROVIDER.get(ctx)?;
    let spec = TableSpec {
        key: key.clone(),
        table_schema,
        managed_by: options.managed_by,
    };
    Ok(provider.target_state(key.stable_key(), spec))
}

/// Declare a Snowflake table target in the **current** component (the row
/// child provider resolves at this component's commit) and return a handle.
pub fn declare_table_target(
    ctx: &Ctx,
    conn: &ContextKey<SnowflakeConnection>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
) -> Result<TableTarget> {
    declare_table_target_with_options(
        ctx,
        conn,
        table_name,
        table_schema,
        SnowflakeTableOptions::default(),
    )
}

/// [`declare_table_target`] with explicit [`SnowflakeTableOptions`].
pub fn declare_table_target_with_options(
    ctx: &Ctx,
    conn: &ContextKey<SnowflakeConnection>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    options: SnowflakeTableOptions,
) -> Result<TableTarget> {
    let ts = table_target_with_options(ctx, conn, table_name, table_schema, options)?;
    let spec = ts.value().clone();
    let rows = declare_target_state_with_child::<TableSpec, RowState>(ctx, ts)?;
    Ok(table_target_handle(spec, rows))
}

/// Mount a Snowflake table target **foreground** (rows can be declared
/// immediately) and return a handle.
pub async fn mount_table_target(
    ctx: &Ctx,
    conn: &ContextKey<SnowflakeConnection>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
) -> Result<TableTarget> {
    mount_table_target_with_options(
        ctx,
        conn,
        table_name,
        table_schema,
        SnowflakeTableOptions::default(),
    )
    .await
}

/// [`mount_table_target`] with explicit [`SnowflakeTableOptions`].
pub async fn mount_table_target_with_options(
    ctx: &Ctx,
    conn: &ContextKey<SnowflakeConnection>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    options: SnowflakeTableOptions,
) -> Result<TableTarget> {
    let ts = table_target_with_options(ctx, conn, table_name, table_schema, options)?;
    let spec = ts.value().clone();
    let rows = mount_target::<TableSpec, RowState>(ctx, ts).await?;
    Ok(table_target_handle(spec, rows))
}

fn table_target_handle(spec: TableSpec, rows: TargetStateProvider<RowState>) -> TableTarget {
    TableTarget {
        table_name: Arc::from(spec.key.table_name),
        table_schema: spec.table_schema,
        rows,
    }
}

impl TableTarget {
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Declare that `row` exists in the table. The row serializes to an object
    /// whose keys are column names; keys outside the schema are ignored, and
    /// every primary-key column must be present.
    pub fn declare_row<R: Serialize>(&self, ctx: &Ctx, row: &R) -> Result<()> {
        let fields = row_state(row, &self.table_schema)?;
        let key = pk_stable_key(&fields, self.table_schema.primary_key())?;
        declare_target_state(ctx, self.rows.target_state(key, RowState { fields }))
    }
}

// ---------------------------------------------------------------------------
// Internal specs / actions
// ---------------------------------------------------------------------------

/// A table target state's key: the connection's `ContextKey` name plus the
/// (optional) database and schema and the table name.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TableKey {
    conn_key: String,
    database: Option<String>,
    schema: Option<String>,
    table_name: String,
}

fn optional_stable_key(value: Option<String>) -> StableKey {
    match value {
        Some(value) => StableKey::Str(Arc::from(value)),
        None => StableKey::Null,
    }
}

fn optional_from_stable_key(key: &StableKey) -> Result<Option<String>> {
    match key {
        StableKey::Str(value) => Ok(Some(value.to_string())),
        StableKey::Null => Ok(None),
        other => Err(Error::engine(format!(
            "unexpected snowflake table target key part: {other:?}"
        ))),
    }
}

impl TableKey {
    fn stable_key(self) -> StableKey {
        StableKey::Array(Arc::from([
            StableKey::Str(Arc::from(self.conn_key)),
            optional_stable_key(self.database),
            optional_stable_key(self.schema),
            StableKey::Str(Arc::from(self.table_name)),
        ]))
    }

    fn from_stable_key(key: &StableKey) -> Result<Self> {
        if let StableKey::Array(parts) = key
            && let [
                StableKey::Str(conn_key),
                database,
                schema,
                StableKey::Str(table_name),
            ] = &parts[..]
        {
            return Ok(Self {
                conn_key: conn_key.to_string(),
                database: optional_from_stable_key(database)?,
                schema: optional_from_stable_key(schema)?,
                table_name: table_name.to_string(),
            });
        }
        Err(Error::engine(format!(
            "unexpected snowflake table target key: {key:?}"
        )))
    }

    fn qualified_table_name(&self) -> String {
        let mut parts = Vec::with_capacity(3);
        parts.extend(self.database.iter().map(|d| format!("\"{d}\"")));
        parts.extend(self.schema.iter().map(|s| format!("\"{s}\"")));
        parts.push(format!("\"{}\"", self.table_name));
        parts.join(".")
    }

    fn qualified_schema_name(&self, schema: &str) -> String {
        match &self.database {
            Some(database) => format!("\"{database}\".\"{schema}\""),
            None => format!("\"{schema}\""),
        }
    }
}

/// Spec for a Snowflake table (the declared container value).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSpec {
    key: TableKey,
    table_schema: TableSchema,
    #[serde(default)]
    managed_by: ManagedBy,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RowState {
    fields: Map<String, JsonValue>,
}

// Composite schema tracking (mirrors Python `connectors/snowflake/_target.py`):
// the `main` record is the primary-key signature; one `sub` record per non-PK
// column (type + nullability) drives incremental `ALTER TABLE`.

const COL_SUBKEY_PREFIX: &str = "col:";

fn col_subkey(col_name: &str) -> String {
    format!("{COL_SUBKEY_PREFIX}{col_name}")
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct PkColumnTrackingRecord {
    name: String,
    snowflake_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NonPkColumnTrackingRecord {
    snowflake_type: String,
    nullable: bool,
}

type TableCompositeRecord =
    CompositeTrackingRecord<Vec<PkColumnTrackingRecord>, String, NonPkColumnTrackingRecord>;

type TableTrackingRecord = MutualTrackingRecord<TableCompositeRecord>;

fn table_composite_record(schema: &TableSchema) -> TableCompositeRecord {
    let pk: HashSet<&String> = schema.primary_key().iter().collect();
    let main = schema
        .primary_key()
        .iter()
        .map(|name| PkColumnTrackingRecord {
            name: name.clone(),
            snowflake_type: schema.columns()[name].snowflake_type.clone(),
        })
        .collect();
    let sub: HashMap<String, NonPkColumnTrackingRecord> = schema
        .columns()
        .iter()
        .filter(|(name, _)| !pk.contains(*name))
        .map(|(name, col)| {
            (
                col_subkey(name),
                NonPkColumnTrackingRecord {
                    snowflake_type: col.snowflake_type.clone(),
                    nullable: col.nullable,
                },
            )
        })
        .collect();
    CompositeTrackingRecord::new(main, sub)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TableAction {
    key: TableKey,
    spec: Option<TableSpec>,
    main_action: Option<DiffAction>,
    column_actions: BTreeMap<String, DiffAction>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RowAction {
    pk: Vec<JsonValue>,
    state: Option<RowState>,
}

// ---------------------------------------------------------------------------
// Table container handler (root) + sink yielding row children
// ---------------------------------------------------------------------------

fn resolve_conn(host_ctx: &Arc<ContextStore>, conn_key: &str) -> Result<Arc<SnowflakeConnection>> {
    host_ctx
        .resolve::<SnowflakeConnection>(conn_key)
        .ok_or_else(|| {
            Error::engine(format!(
                "snowflake target: connection `{conn_key}` was not provided in the environment \
                 (call Environment::builder().provide_key(&KEY, conn))"
            ))
        })
}

#[linkme::distributed_slice(STATIC_ROOT_PROVIDERS)]
static TABLE_PROVIDER: StaticRootProvider =
    StaticRootProvider::new::<TableSpec, TableHandler>("cocoindex/snowflake/table");

/// Root handler for every Snowflake table target: stateless, the connection is
/// named by each target state's [`TableKey`].
#[derive(Default)]
struct TableHandler {
    sink: SinkCache<TableAction>,
}

impl TargetHandler<TableSpec> for TableHandler {
    type TrackingRecord = TableTrackingRecord;
    type Action = TableAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<TableSpec>,
        prev: Vec<TableTrackingRecord>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<TableAction, TableTrackingRecord>>> {
        let key = TableKey::from_stable_key(&key)?;
        match desired {
            Some(spec) => {
                let tracking = MutualTrackingRecord::new(
                    table_composite_record(&spec.table_schema),
                    spec.managed_by,
                );
                let resolved =
                    resolve_system_transition(Some(tracking.clone()), prev, prev_may_be_missing);
                // `insert` / `replace` build the table from the desired schema, so
                // every column is already correct. `upsert` means the table may or
                // may not exist: `CREATE TABLE IF NOT EXISTS` can land on a table
                // carrying the previous column set, so its columns still need
                // reconciling.
                let (main_action, column_transitions) = diff_composite(resolved.as_ref());
                let mut column_actions = BTreeMap::new();
                if matches!(main_action, None | Some(DiffAction::Upsert)) {
                    for (sub_key, transition) in &column_transitions {
                        if let Some(action) = diff(Some(transition)) {
                            column_actions.insert(sub_key.clone(), action);
                        }
                    }
                }
                let child_invalidation = if matches!(main_action, Some(DiffAction::Replace)) {
                    Some(TargetChildInvalidation::Destructive)
                } else if column_actions
                    .values()
                    .any(|a| !matches!(a, DiffAction::Insert))
                {
                    Some(TargetChildInvalidation::Lossy)
                } else {
                    None
                };
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Update(TableAction {
                        key,
                        spec: Some(spec),
                        main_action,
                        column_actions,
                    }),
                    sink: self.sink.get(table_sink),
                    tracking_record: Some(tracking),
                    child_invalidation,
                }))
            }
            None => {
                let resolved = resolve_system_transition(None, prev.clone(), prev_may_be_missing);
                if resolved.is_none() || !prev.iter().any(|v| v.managed_by.is_system()) {
                    return Ok(None);
                }
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Delete(TableAction {
                        key,
                        spec: None,
                        main_action: Some(DiffAction::Delete),
                        column_actions: BTreeMap::new(),
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
            // One session per connection for the whole batch, as Python opens
            // one connection per `_apply_actions`.
            let mut sessions: HashMap<String, Session> = HashMap::new();
            let mut result = Ok(());
            for action in actions {
                let (TargetAction::Create(a) | TargetAction::Update(a) | TargetAction::Delete(a)) =
                    action;
                let statements = table_action_statements(
                    &a.key,
                    a.spec.as_ref(),
                    a.main_action,
                    &a.column_actions,
                );
                if statements.is_empty() {
                    continue;
                }
                let session = match sessions.entry(a.key.conn_key.clone()) {
                    std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let conn = resolve_conn(&host_ctx, e.key())?;
                        e.insert(conn.login().await?)
                    }
                };
                for sql in &statements {
                    if let Err(e) = session.execute(sql, &[]).await {
                        result = Err(e);
                        break;
                    }
                }
                if result.is_err() {
                    break;
                }
            }
            for session in sessions.into_values() {
                session.close().await;
            }
            result
        },
        |a: &TableAction| {
            let spec = a
                .spec
                .clone()
                .ok_or_else(|| Error::engine("Snowflake table action missing spec"))?;
            Ok(ChildTargetDef::new::<RowState, _>(RowHandler {
                sink: SinkCache::default(),
                spec,
            }))
        },
    )
}

/// The DDL statements for a table action, in order. A user-managed table yields
/// none. A drop or structural rewrite removes the table first; `insert` /
/// `replace` create it from the desired schema, `upsert` creates it only if
/// missing and then reconciles columns. Mirrors Python's
/// `_TableHandler._apply_actions`.
fn table_action_statements(
    key: &TableKey,
    spec: Option<&TableSpec>,
    main_action: Option<DiffAction>,
    column_actions: &BTreeMap<String, DiffAction>,
) -> Vec<String> {
    let mut statements = Vec::new();
    if spec.is_some_and(|s| s.managed_by.is_user()) {
        return statements;
    }
    let table = key.qualified_table_name();
    if matches!(
        main_action,
        Some(DiffAction::Replace) | Some(DiffAction::Delete)
    ) {
        statements.push(format!("DROP TABLE IF EXISTS {table}"));
    }
    let Some(spec) = spec else {
        return statements;
    };
    let schema = &spec.table_schema;
    match main_action {
        Some(DiffAction::Insert | DiffAction::Replace) => {
            statements.extend(create_table_statements(key, schema, false));
            return statements;
        }
        Some(DiffAction::Upsert) => {
            statements.extend(create_table_statements(key, schema, true));
        }
        None | Some(DiffAction::Delete) => {}
    }
    for (sub_key, action) in column_actions {
        let Some(col_name) = sub_key.strip_prefix(COL_SUBKEY_PREFIX) else {
            continue;
        };
        if schema.primary_key().iter().any(|pk| pk == col_name) {
            continue;
        }
        let col = schema.columns().get(col_name);
        match (action, col) {
            (DiffAction::Delete, _) => {
                statements.push(format!(
                    "ALTER TABLE {table} DROP COLUMN IF EXISTS \"{col_name}\""
                ));
            }
            (DiffAction::Insert, Some(col)) => {
                statements.push(format!(
                    "ALTER TABLE {table} ADD COLUMN {}",
                    column_sql(col_name, col, false)
                ));
            }
            (DiffAction::Upsert, Some(col)) => {
                statements.push(format!(
                    "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {}",
                    column_sql(col_name, col, false)
                ));
            }
            (DiffAction::Replace, Some(col)) => {
                statements.push(format!(
                    "ALTER TABLE {table} DROP COLUMN IF EXISTS \"{col_name}\""
                ));
                statements.push(format!(
                    "ALTER TABLE {table} ADD COLUMN {}",
                    column_sql(col_name, col, false)
                ));
            }
            (_, None) => {}
        }
    }
    statements
}

fn create_table_statements(
    key: &TableKey,
    schema: &TableSchema,
    if_not_exists: bool,
) -> Vec<String> {
    let mut statements = Vec::new();
    if let Some(database) = &key.database {
        statements.push(format!("CREATE DATABASE IF NOT EXISTS \"{database}\""));
    }
    if let Some(schema_name) = &key.schema {
        statements.push(format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            key.qualified_schema_name(schema_name)
        ));
    }
    let pk: HashSet<&String> = schema.primary_key().iter().collect();
    let mut col_defs: Vec<String> = schema
        .ordered_columns()
        .into_iter()
        .map(|(name, col)| column_sql(name, col, pk.contains(name)))
        .collect();
    col_defs.push(format!(
        "PRIMARY KEY ({})",
        quoted_list(schema.primary_key())
    ));
    let if_not_exists = if if_not_exists { " IF NOT EXISTS" } else { "" };
    statements.push(format!(
        "CREATE TABLE{if_not_exists} {} ({})",
        key.qualified_table_name(),
        col_defs.join(", ")
    ));
    statements
}

fn column_sql(name: &str, col: &ColumnDef, is_pk: bool) -> String {
    let not_null = if col.nullable && !is_pk {
        ""
    } else {
        " NOT NULL"
    };
    format!("\"{name}\" {}{not_null}", col.snowflake_type)
}

fn quoted_list(names: &[String]) -> String {
    names
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ")
}

// ---------------------------------------------------------------------------
// Row handler (child) + sink
// ---------------------------------------------------------------------------

struct RowHandler {
    sink: SinkCache<RowAction>,
    spec: TableSpec,
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
        // Skip only when every previous fingerprint matches (Python's
        // `all(prev == target_fp ...)`).
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
            action: TargetAction::Update(RowAction { pk, state: desired }),
            sink: self.sink.get(|| self.row_sink()),
            tracking_record: desired_fp,
            child_invalidation: None,
        }))
    }
}

impl RowHandler {
    fn row_sink(&self) -> TargetActionSink<RowAction> {
        let spec = self.spec.clone();
        TargetActionSink::from_async_fn_with_ctx(
            move |host_ctx, actions: Vec<TargetAction<RowAction>>| {
                let spec = spec.clone();
                async move {
                    let conn = resolve_conn(&host_ctx, &spec.key.conn_key)?;
                    let mutations = actions
                        .into_iter()
                        .map(|action| {
                            let (TargetAction::Create(r)
                            | TargetAction::Update(r)
                            | TargetAction::Delete(r)) = action;
                            (r.pk, r.state)
                        })
                        .collect();
                    apply_rows(&conn, &spec, mutations).await
                }
            },
        )
    }
}

/// Apply row mutations in one session: declared rows are `MERGE`d in batches,
/// un-declared rows are deleted by primary key. Mirrors Python's
/// `_RowHandler._apply_actions`.
async fn apply_rows(
    conn: &SnowflakeConnection,
    spec: &TableSpec,
    mutations: Vec<(Vec<JsonValue>, Option<RowState>)>,
) -> Result<()> {
    if mutations.is_empty() {
        return Ok(());
    }
    let mut upserts: Vec<Map<String, JsonValue>> = Vec::new();
    let mut deletes: Vec<Vec<JsonValue>> = Vec::new();
    for (pk, state) in mutations {
        match state {
            Some(state) => upserts.push(state.fields),
            None => deletes.push(pk),
        }
    }
    let table = spec.key.qualified_table_name();
    let schema = &spec.table_schema;
    let mut session = conn.login().await?;
    let result = async {
        for chunk in upserts.chunks(BATCH_SIZE) {
            let (sql, bindings) = merge_statement(&table, schema, chunk)?;
            session.execute(&sql, &bindings).await?;
        }
        for chunk in deletes.chunks(BATCH_SIZE) {
            let (sql, bindings) = delete_statement(&table, schema, chunk)?;
            session.execute(&sql, &bindings).await?;
        }
        Ok(())
    }
    .await;
    session.close().await;
    result
}

/// One positional `?` binding. Snowflake carries every bound value as a string
/// (or `null`) tagged with a type.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Binding {
    bind_type: &'static str,
    value: Option<String>,
}

impl Binding {
    fn to_api(&self) -> JsonValue {
        json!({ "type": self.bind_type, "value": self.value })
    }
}

/// Bind a column's value. `PARSE_JSON` columns take JSON text (a string value
/// is taken to be JSON already, as in Python); a serde `Duration` becomes
/// seconds like Python's `timedelta.total_seconds()`; a byte array binds as
/// hex `BINARY`; other scalars bind as their own type.
fn binding(col: &ColumnDef, value: &JsonValue) -> Binding {
    let text = |bind_type: &'static str, value: String| Binding {
        bind_type,
        value: Some(value),
    };
    match value {
        JsonValue::Null => Binding {
            bind_type: "TEXT",
            value: None,
        },
        JsonValue::String(s) => text("TEXT", s.clone()),
        _ if col.use_parse_json => text("TEXT", value.to_string()),
        JsonValue::Bool(b) => text("BOOLEAN", b.to_string()),
        JsonValue::Number(n) => {
            if n.is_f64() {
                text("REAL", n.to_string())
            } else {
                text("FIXED", n.to_string())
            }
        }
        JsonValue::Array(_) if col.snowflake_type.eq_ignore_ascii_case("BINARY") => {
            match json_byte_array(value) {
                Some(bytes) => text("BINARY", hex(&bytes)),
                None => text("TEXT", value.to_string()),
            }
        }
        JsonValue::Object(_) | JsonValue::Array(_) => {
            match crate::row_schema::serialized_duration_parts(value) {
                Some((secs, nanos)) => {
                    text("REAL", (secs as f64 + f64::from(nanos) / 1e9).to_string())
                }
                None => text("TEXT", value.to_string()),
            }
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02X}")).collect()
}

/// `MERGE INTO` one batch of rows through a `USING (SELECT … UNION ALL SELECT
/// …)` source of `?` bindings. Mirrors Python's `_merge_sql` for one row.
fn merge_statement(
    table: &str,
    schema: &TableSchema,
    rows: &[Map<String, JsonValue>],
) -> Result<(String, Vec<Binding>)> {
    if rows.is_empty() {
        return Err(Error::engine("snowflake merge batch cannot be empty"));
    }
    let columns = schema.ordered_columns();
    let mut bindings = Vec::with_capacity(rows.len() * columns.len());
    let mut selects = Vec::with_capacity(rows.len());
    for row in rows {
        let mut exprs = Vec::with_capacity(columns.len());
        for (name, col) in &columns {
            let value = row.get(*name).unwrap_or(&JsonValue::Null);
            bindings.push(binding(col, value));
            let expr = if col.use_parse_json {
                "PARSE_JSON(?)"
            } else {
                "?"
            };
            exprs.push(format!("{expr} AS \"{name}\""));
        }
        selects.push(format!("SELECT {}", exprs.join(", ")));
    }
    let on_clause = schema
        .primary_key()
        .iter()
        .map(|c| format!("target.\"{c}\" = source.\"{c}\""))
        .collect::<Vec<_>>()
        .join(" AND ");
    let all_names: Vec<String> = columns.iter().map(|(n, _)| (*n).clone()).collect();
    let insert_cols = quoted_list(&all_names);
    let insert_values = all_names
        .iter()
        .map(|c| format!("source.\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!(
        "MERGE INTO {table} AS target USING ({}) AS source ON {on_clause}",
        selects.join(" UNION ALL ")
    );
    let non_pk = schema.non_pk_columns();
    if !non_pk.is_empty() {
        let updates = non_pk
            .iter()
            .map(|c| format!("\"{c}\" = source.\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&format!(" WHEN MATCHED THEN UPDATE SET {updates}"));
    }
    sql.push_str(&format!(
        " WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})"
    ));
    Ok((sql, bindings))
}

/// `DELETE` one batch of rows by primary key. Mirrors Python's `_delete_sql`:
/// `IN (…)` for a single-column key, OR-joined `( … AND … )` groups otherwise.
fn delete_statement(
    table: &str,
    schema: &TableSchema,
    keys: &[Vec<JsonValue>],
) -> Result<(String, Vec<Binding>)> {
    if keys.is_empty() {
        return Err(Error::engine("snowflake delete batch cannot be empty"));
    }
    let pk_cols = schema.primary_key();
    let mut bindings = Vec::with_capacity(keys.len() * pk_cols.len());
    let sql = if let [pk] = pk_cols {
        let col = &schema.columns()[pk];
        for key in keys {
            let [value] = &key[..] else {
                return Err(Error::engine("snowflake row key length mismatch"));
            };
            bindings.push(binding(col, value));
        }
        format!(
            "DELETE FROM {table} WHERE \"{pk}\" IN ({})",
            vec!["?"; keys.len()].join(", ")
        )
    } else {
        let mut groups = Vec::with_capacity(keys.len());
        for key in keys {
            if key.len() != pk_cols.len() {
                return Err(Error::engine("snowflake row key length mismatch"));
            }
            let mut parts = Vec::with_capacity(pk_cols.len());
            for (col_name, value) in pk_cols.iter().zip(key) {
                bindings.push(binding(&schema.columns()[col_name], value));
                parts.push(format!("\"{col_name}\" = ?"));
            }
            groups.push(format!("({})", parts.join(" AND ")));
        }
        format!("DELETE FROM {table} WHERE {}", groups.join(" OR "))
    };
    Ok((sql, bindings))
}

// ---------------------------------------------------------------------------
// Value / key helpers
// ---------------------------------------------------------------------------

fn row_state<R: Serialize>(row: &R, schema: &TableSchema) -> Result<Map<String, JsonValue>> {
    // Reject non-finite floats up front: `serde_json` would turn NaN/±Inf into
    // JSON null, silently corrupting the value (or failing a NOT NULL column).
    crate::finite::ensure_finite(row)
        .map_err(|e| Error::engine(format!("Snowflake target row has a {e}")))?;
    let value = serde_json::to_value(row)
        .map_err(|e| Error::engine(format!("serialize Snowflake target row: {e}")))?;
    let JsonValue::Object(mut fields) = value else {
        return Err(Error::engine(
            "Snowflake target row must serialize to an object",
        ));
    };
    fields.retain(|name, _| schema.columns().contains_key(name));
    for name in schema.columns().keys() {
        fields.entry(name.clone()).or_insert(JsonValue::Null);
    }
    Ok(fields)
}

fn json_byte_array(value: &JsonValue) -> Option<Vec<u8>> {
    let JsonValue::Array(items) = value else {
        return None;
    };
    items
        .iter()
        .map(|item| item.as_u64().filter(|b| *b <= 255).map(|b| b as u8))
        .collect()
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
            "unsupported Snowflake row key: {other:?}"
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

fn validate_sql_type(value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '(' | ')' | ',' | ' '))
    {
        return Err(Error::engine(format!("invalid Snowflake type: {value}")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(database: Option<&str>, schema: Option<&str>) -> TableKey {
        TableKey {
            conn_key: "sf".to_string(),
            database: database.map(str::to_string),
            schema: schema.map(str::to_string),
            table_name: "events".to_string(),
        }
    }

    fn schema() -> TableSchema {
        TableSchema::new(
            [
                ("id", ColumnDef::new("NUMBER").not_null()),
                ("payload", ColumnDef::new("VARIANT").parse_json()),
            ],
            ["id"],
        )
        .unwrap()
    }

    #[test]
    fn qualified_names_quote_each_part() {
        assert_eq!(
            key(Some("analytics"), Some("public")).qualified_table_name(),
            "\"analytics\".\"public\".\"events\""
        );
        assert_eq!(
            key(None, Some("public")).qualified_table_name(),
            "\"public\".\"events\""
        );
        assert_eq!(key(None, None).qualified_table_name(), "\"events\"");
    }

    #[test]
    fn table_key_round_trips_through_stable_key() {
        for (database, schema) in [(Some("db"), Some("s")), (None, Some("s")), (None, None)] {
            let original = key(database, schema);
            let decoded = TableKey::from_stable_key(&original.clone().stable_key()).unwrap();
            assert_eq!(decoded, original);
        }
    }

    #[test]
    fn config_debug_redacts_password() {
        let config = SnowflakeConfig::new("org-acct", "me", "hunter2");
        let debug = format!("{config:?}");
        assert!(!debug.contains("hunter2"), "{debug}");
        assert!(debug.contains("<redacted>"), "{debug}");
        assert_eq!(
            SnowflakeConfig::new("org_acct", "me", "pw").base_url(),
            "https://org-acct.snowflakecomputing.com"
        );
    }

    #[test]
    fn from_row_maps_logical_types_like_python() {
        #[derive(crate::SchemaFields)]
        #[allow(dead_code)]
        struct Row {
            id: i64,
            flag: bool,
            amount: f64,
            name: Option<String>,
            data: Vec<u8>,
            elapsed: std::time::Duration,
            #[coco(json)]
            payload: Vec<String>,
            #[coco(vector = 3)]
            embedding: Vec<f32>,
            #[coco(type = "NUMBER(38, 0)")]
            price: String,
        }
        let schema = TableSchema::from_row::<Row>(["id"]).unwrap();
        let col = |name: &str| schema.columns()[name].clone();
        assert_eq!(col("id"), ColumnDef::new("NUMBER").not_null());
        assert_eq!(col("flag"), ColumnDef::new("BOOLEAN").not_null());
        assert_eq!(col("amount"), ColumnDef::new("FLOAT").not_null());
        assert_eq!(col("name"), ColumnDef::new("VARCHAR"));
        assert_eq!(col("data"), ColumnDef::new("BINARY").not_null());
        assert_eq!(col("elapsed"), ColumnDef::new("NUMBER").not_null());
        assert_eq!(
            col("payload"),
            ColumnDef::new("VARIANT").parse_json().not_null()
        );
        assert_eq!(
            col("embedding"),
            ColumnDef::new("ARRAY").parse_json().not_null()
        );
        assert_eq!(col("price"), ColumnDef::new("NUMBER(38, 0)").not_null());
    }

    #[test]
    fn create_table_statements_match_python() {
        let statements =
            create_table_statements(&key(Some("analytics"), Some("public")), &schema(), false);
        assert_eq!(
            statements,
            vec![
                "CREATE DATABASE IF NOT EXISTS \"analytics\"".to_string(),
                "CREATE SCHEMA IF NOT EXISTS \"analytics\".\"public\"".to_string(),
                "CREATE TABLE \"analytics\".\"public\".\"events\" (\"id\" NUMBER NOT NULL, \
                 \"payload\" VARIANT, PRIMARY KEY (\"id\"))"
                    .to_string(),
            ]
        );
        let bare = create_table_statements(&key(None, None), &schema(), true);
        assert_eq!(bare.len(), 1);
        assert!(bare[0].starts_with("CREATE TABLE IF NOT EXISTS \"events\""));
    }

    #[test]
    fn table_action_statements_cover_replace_and_column_changes() {
        let spec = TableSpec {
            key: key(None, None),
            table_schema: schema(),
            managed_by: ManagedBy::System,
        };
        let statements = table_action_statements(
            &spec.key,
            Some(&spec),
            Some(DiffAction::Replace),
            &BTreeMap::from([(col_subkey("payload"), DiffAction::Insert)]),
        );
        assert_eq!(
            statements,
            vec![
                "DROP TABLE IF EXISTS \"events\"".to_string(),
                "CREATE TABLE \"events\" (\"id\" NUMBER NOT NULL, \"payload\" VARIANT, PRIMARY \
                 KEY (\"id\"))"
                    .to_string(),
            ]
        );

        let statements = table_action_statements(
            &spec.key,
            Some(&spec),
            None,
            &BTreeMap::from([
                (col_subkey("payload"), DiffAction::Replace),
                (col_subkey("old"), DiffAction::Delete),
                (col_subkey("id"), DiffAction::Replace),
            ]),
        );
        assert_eq!(
            statements,
            vec![
                "ALTER TABLE \"events\" DROP COLUMN IF EXISTS \"old\"".to_string(),
                "ALTER TABLE \"events\" DROP COLUMN IF EXISTS \"payload\"".to_string(),
                "ALTER TABLE \"events\" ADD COLUMN \"payload\" VARIANT".to_string(),
            ]
        );

        assert_eq!(
            table_action_statements(&spec.key, None, Some(DiffAction::Delete), &BTreeMap::new()),
            vec!["DROP TABLE IF EXISTS \"events\"".to_string()]
        );

        let user = TableSpec {
            managed_by: ManagedBy::User,
            ..spec.clone()
        };
        assert!(
            table_action_statements(
                &user.key,
                Some(&user),
                Some(DiffAction::Insert),
                &BTreeMap::new()
            )
            .is_empty()
        );
    }

    #[test]
    fn merge_statement_uses_parse_json_and_bindings() {
        let row = json!({"id": 1, "payload": {"b": 2, "a": [1]}});
        let (sql, bindings) = merge_statement(
            "\"analytics\".\"public\".\"events\"",
            &schema(),
            &[row.as_object().unwrap().clone()],
        )
        .unwrap();
        assert_eq!(
            sql,
            "MERGE INTO \"analytics\".\"public\".\"events\" AS target USING (SELECT ? AS \"id\", \
             PARSE_JSON(?) AS \"payload\") AS source ON target.\"id\" = source.\"id\" WHEN \
             MATCHED THEN UPDATE SET \"payload\" = source.\"payload\" WHEN NOT MATCHED THEN \
             INSERT (\"id\", \"payload\") VALUES (source.\"id\", source.\"payload\")"
        );
        assert_eq!(
            bindings,
            vec![
                Binding {
                    bind_type: "FIXED",
                    value: Some("1".into())
                },
                Binding {
                    bind_type: "TEXT",
                    value: Some("{\"a\":[1],\"b\":2}".into())
                },
            ]
        );
        assert_eq!(bindings[0].to_api(), json!({"type": "FIXED", "value": "1"}));
    }

    #[test]
    fn merge_statement_batches_rows_and_skips_update_without_non_pk_columns() {
        let schema = TableSchema::new([("id", ColumnDef::new("NUMBER"))], ["id"]).unwrap();
        let rows = [json!({"id": 1}), json!({"id": 2})].map(|v| v.as_object().unwrap().clone());
        let (sql, bindings) = merge_statement("\"events\"", &schema, &rows).unwrap();
        assert!(
            sql.contains("USING (SELECT ? AS \"id\" UNION ALL SELECT ? AS \"id\")"),
            "{sql}"
        );
        assert!(!sql.contains("WHEN MATCHED THEN UPDATE"), "{sql}");
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"), "{sql}");
        assert_eq!(bindings.len(), 2);
    }

    #[test]
    fn delete_statement_supports_single_and_composite_keys() {
        let (sql, bindings) = delete_statement(
            "\"events\"",
            &schema(),
            &[vec![json!(1)], vec![json!(2)], vec![json!(3)]],
        )
        .unwrap();
        assert_eq!(sql, "DELETE FROM \"events\" WHERE \"id\" IN (?, ?, ?)");
        assert_eq!(bindings[2].value.as_deref(), Some("3"));

        let composite = TableSchema::new(
            [
                ("tenant", ColumnDef::new("VARCHAR").not_null()),
                ("id", ColumnDef::new("NUMBER").not_null()),
            ],
            ["tenant", "id"],
        )
        .unwrap();
        let (sql, bindings) = delete_statement(
            "\"events\"",
            &composite,
            &[vec![json!("a"), json!(1)], vec![json!("b"), json!(2)]],
        )
        .unwrap();
        assert_eq!(
            sql,
            "DELETE FROM \"events\" WHERE (\"tenant\" = ? AND \"id\" = ?) OR (\"tenant\" = ? AND \
             \"id\" = ?)"
        );
        assert_eq!(bindings[0].bind_type, "TEXT");
        assert_eq!(bindings[1].bind_type, "FIXED");
    }

    #[test]
    fn bindings_encode_scalars_bytes_durations_and_nulls() {
        let plain = ColumnDef::new("FLOAT");
        assert_eq!(
            binding(&plain, &json!(1.5)),
            Binding {
                bind_type: "REAL",
                value: Some("1.5".into())
            }
        );
        assert_eq!(
            binding(&plain, &json!(true)),
            Binding {
                bind_type: "BOOLEAN",
                value: Some("true".into())
            }
        );
        assert_eq!(
            binding(&plain, &JsonValue::Null),
            Binding {
                bind_type: "TEXT",
                value: None
            }
        );
        assert_eq!(
            binding(&ColumnDef::new("BINARY"), &json!([104, 105])),
            Binding {
                bind_type: "BINARY",
                value: Some("6869".into())
            }
        );
        assert_eq!(
            binding(
                &ColumnDef::new("NUMBER"),
                &serde_json::to_value(Duration::new(2, 500_000_000)).unwrap()
            ),
            Binding {
                bind_type: "REAL",
                value: Some("2.5".into())
            }
        );
        // A string bound to a PARSE_JSON column is taken as JSON text already.
        assert_eq!(
            binding(&ColumnDef::new("VARIANT").parse_json(), &json!("{\"x\":1}")),
            Binding {
                bind_type: "TEXT",
                value: Some("{\"x\":1}".into())
            }
        );
        assert_eq!(
            binding(&plain, &json!({"x": 1})).value.as_deref(),
            Some("{\"x\":1}")
        );
    }

    #[test]
    fn row_state_keeps_schema_columns_and_rejects_non_finite() {
        #[derive(Serialize)]
        struct Row {
            id: i64,
            extra: &'static str,
            score: f64,
        }
        let schema = TableSchema::new(
            [
                ("id", ColumnDef::new("NUMBER")),
                ("missing", ColumnDef::new("VARCHAR")),
            ],
            ["id"],
        )
        .unwrap();
        let fields = row_state(
            &Row {
                id: 1,
                extra: "x",
                score: 0.5,
            },
            &schema,
        )
        .unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields["missing"], JsonValue::Null);
        let error = row_state(
            &Row {
                id: 1,
                extra: "x",
                score: f64::INFINITY,
            },
            &schema,
        )
        .unwrap_err();
        assert!(error.to_string().contains("non-finite"), "{error}");
    }

    #[test]
    fn decode_rows_types_cells_and_check_success_reports_errors() {
        let data = json!({
            "rowtype": [
                {"name": "id", "type": "fixed"},
                {"name": "score", "type": "real"},
                {"name": "ok", "type": "boolean"},
                {"name": "name", "type": "text"},
            ],
            "rowset": [["1", "0.5", "1", "a"], ["2", null, "0", null]],
        });
        assert_eq!(
            decode_rows(&data),
            vec![
                json!({"id": 1, "score": 0.5, "ok": true, "name": "a"})
                    .as_object()
                    .unwrap()
                    .clone(),
                json!({"id": 2, "score": null, "ok": false, "name": null})
                    .as_object()
                    .unwrap()
                    .clone(),
            ]
        );
        let failure =
            json!({"success": false, "code": "002003", "message": "Object does not exist"});
        let error = check_success(&failure, "statement")
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("002003") && error.contains("Object does not exist"),
            "{error}"
        );
    }

    #[test]
    fn identifiers_and_types_are_validated() {
        assert!(
            TableSchema::new([("bad-column", ColumnDef::new("VARCHAR"))], ["bad-column"]).is_err()
        );
        assert!(TableSchema::new([("id", ColumnDef::new("VARCHAR; DROP"))], ["id"]).is_err());
        assert!(TableSchema::new([("id", ColumnDef::new("NUMBER(38, 0)"))], ["id"]).is_ok());
        assert!(TableSchema::new([("id", ColumnDef::new("VARCHAR"))], ["other"]).is_err());
    }
}
