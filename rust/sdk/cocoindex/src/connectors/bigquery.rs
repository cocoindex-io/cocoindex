//! Google BigQuery table target connector.
//!
//! Table targets reconcile declared rows against the previous run: changed rows
//! are upserted (`MERGE`), unchanged rows are skipped, and rows no longer
//! declared are deleted. `managed_by` controls whether CocoIndex owns the table
//! DDL (dataset + table creation, `ALTER TABLE` column changes, `DROP TABLE`).
//!
//! BigQuery has no official Rust client, so this talks to the **BigQuery v2 REST
//! API** (`jobs.query` / `jobs.getQueryResults`) via `reqwest`, mirroring
//! Python's `bigquery` connector which drives the same statements through the
//! Google client library. Authentication follows the Python connector:
//!
//! * [`BigQueryConfig::credentials_path`] — a service-account JSON key file
//!   (RS256-signed JWT exchanged for an access token).
//! * Otherwise **Application Default Credentials**: the
//!   `GOOGLE_APPLICATION_CREDENTIALS` file, then the `gcloud auth
//!   application-default login` file (`authorized_user` refresh token or a
//!   service-account key), then the GCE metadata server.
//!
//! Use [`table_target`] to build a composable target state,
//! [`declare_table_target`] inside the current component, or
//! [`mount_table_target`] when rows must be declared immediately. The owning
//! `project` and `managed_by` are set through [`BigQueryTableOptions`].

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use cocoindex_utils::fingerprint::Fingerprint;
use rsa::RsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::signature::{SignatureEncoding, Signer};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue, json};
use sha2::Sha256;

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

const DEFAULT_BASE_URL: &str = "https://bigquery.googleapis.com/bigquery/v2";
const DEFAULT_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
const METADATA_TOKEN_URL: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";
const BIGQUERY_SCOPE: &str = "https://www.googleapis.com/auth/bigquery";
/// How long one `jobs.query` / `getQueryResults` call waits for the job.
const QUERY_TIMEOUT: Duration = Duration::from_secs(60);
/// Query parameters per statement. BigQuery caps a query at 10,000 parameters;
/// a `MERGE` binds one per column per row, so rows are batched to stay under
/// this.
const MAX_PARAMETERS_PER_STATEMENT: usize = 2_000;

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// Connection configuration. Mirrors Python's `bigquery.ConnectionConfig`.
#[derive(Clone, Debug, Default)]
pub struct BigQueryConfig {
    /// Google Cloud project that runs the query jobs (and owns tables declared
    /// without an explicit [`BigQueryTableOptions::project`]). When unset, the
    /// `GOOGLE_CLOUD_PROJECT` / `GCLOUD_PROJECT` environment variable, then the
    /// credentials file's project, is used.
    pub project: Option<String>,
    /// Path to a service-account JSON key file. When unset, Application
    /// Default Credentials are used.
    pub credentials_path: Option<PathBuf>,
    /// Job location, e.g. `US` or `EU`.
    pub location: Option<String>,
}

#[derive(Clone)]
enum Auth {
    ServiceAccount {
        client_email: String,
        private_key: String,
        token_uri: String,
    },
    AuthorizedUser {
        client_id: String,
        client_secret: String,
        refresh_token: String,
        token_uri: String,
    },
    /// The GCE / Cloud Run metadata server.
    Metadata,
    /// A pre-obtained bearer token (tests / custom auth flows).
    Static(String),
}

struct CachedToken {
    token: String,
    expires_at: Instant,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: u64,
}

struct ConnectionInner {
    http: reqwest::Client,
    base_url: String,
    auth: Auth,
    project: String,
    location: Option<String>,
    state_id: String,
    token_cache: tokio::sync::Mutex<Option<CachedToken>>,
}

/// A BigQuery connection handle. Clone-cheap (the HTTP client and token cache
/// are shared). Register it under a [`ContextKey`] and pass that key to the
/// table-target constructors.
#[derive(Clone)]
pub struct BigQueryConnection {
    inner: Arc<ConnectionInner>,
}

impl BigQueryConnection {
    /// Resolve credentials (see the module docs) and build a connection. No
    /// request is made until the first query.
    pub fn connect(config: BigQueryConfig) -> Result<Self> {
        let (auth, auth_id, project_hint) =
            resolve_credentials(config.credentials_path.as_deref())?;
        let project = resolve_project(config.project, project_hint)?;
        Ok(Self::build(auth, auth_id, project, config.location))
    }

    /// Build a connection from a pre-obtained OAuth bearer token. `config.project`
    /// is required (there is no credentials file to read it from). Useful for
    /// tests against a mock server or custom auth flows.
    pub fn from_static_token(config: BigQueryConfig, token: impl Into<String>) -> Result<Self> {
        let project = resolve_project(config.project, None)?;
        Ok(Self::build(
            Auth::Static(token.into()),
            "static_token".to_string(),
            project,
            config.location,
        ))
    }

    fn build(auth: Auth, auth_id: String, project: String, location: Option<String>) -> Self {
        let state_id = format!(
            "bigquery:{auth_id}:{project}:{}",
            location.as_deref().unwrap_or_default()
        );
        Self {
            inner: Arc::new(ConnectionInner {
                http: reqwest::Client::new(),
                base_url: DEFAULT_BASE_URL.to_string(),
                auth,
                project,
                location,
                state_id,
                token_cache: tokio::sync::Mutex::new(None),
            }),
        }
    }

    /// Override the REST base URL (default the public Google endpoint). Mainly
    /// for pointing the connection at a mock server in tests.
    pub fn with_base_url(self, base_url: impl Into<String>) -> Self {
        let inner = self.inner.as_ref();
        Self {
            inner: Arc::new(ConnectionInner {
                http: inner.http.clone(),
                base_url: base_url.into().trim_end_matches('/').to_string(),
                auth: inner.auth.clone(),
                project: inner.project.clone(),
                location: inner.location.clone(),
                state_id: inner.state_id.clone(),
                token_cache: tokio::sync::Mutex::new(None),
            }),
        }
    }

    /// The project that runs query jobs.
    pub fn project(&self) -> &str {
        &self.inner.project
    }

    pub fn location(&self) -> Option<&str> {
        self.inner.location.as_deref()
    }

    /// Stable identity (credentials excluded).
    pub fn state_id(&self) -> &str {
        &self.inner.state_id
    }

    /// Run a GoogleSQL statement and return its result rows as objects keyed by
    /// column name. Scalar `INTEGER`/`FLOAT`/`BOOLEAN` cells are decoded to JSON
    /// numbers/booleans; everything else is returned as the API's string form.
    /// Useful for reading a target table back.
    pub async fn query(&self, sql: &str) -> Result<Vec<Map<String, JsonValue>>> {
        self.run_query(sql, &[]).await
    }

    async fn token(&self) -> Result<String> {
        let inner = &self.inner;
        if let Auth::Static(token) = &inner.auth {
            return Ok(token.clone());
        }
        let mut guard = inner.token_cache.lock().await;
        if let Some(cached) = guard.as_ref()
            && cached.expires_at > Instant::now() + Duration::from_secs(60)
        {
            return Ok(cached.token.clone());
        }
        let response = match &inner.auth {
            Auth::ServiceAccount {
                client_email,
                private_key,
                token_uri,
            } => {
                let jwt = make_jwt(client_email, private_key, token_uri, unix_now())?;
                inner
                    .http
                    .post(token_uri)
                    .form(&[
                        ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                        ("assertion", jwt.as_str()),
                    ])
                    .send()
                    .await
            }
            Auth::AuthorizedUser {
                client_id,
                client_secret,
                refresh_token,
                token_uri,
            } => {
                inner
                    .http
                    .post(token_uri)
                    .form(&[
                        ("grant_type", "refresh_token"),
                        ("client_id", client_id.as_str()),
                        ("client_secret", client_secret.as_str()),
                        ("refresh_token", refresh_token.as_str()),
                    ])
                    .send()
                    .await
            }
            Auth::Metadata => {
                inner
                    .http
                    .get(METADATA_TOKEN_URL)
                    .header("Metadata-Flavor", "Google")
                    .send()
                    .await
            }
            Auth::Static(_) => unreachable!("handled above"),
        };
        let response = response
            .map_err(|e| match &inner.auth {
                Auth::Metadata => Error::engine(format!(
                    "bigquery: no credentials file was found (set \
                     BigQueryConfig.credentials_path or GOOGLE_APPLICATION_CREDENTIALS, or run \
                     `gcloud auth application-default login`) and the GCE metadata server is \
                     unreachable: {e}"
                )),
                _ => Error::engine(format!("bigquery token request: {e}")),
            })?
            .error_for_status()
            .map_err(|e| Error::engine(format!("bigquery token request failed: {e}")))?;
        let token: TokenResponse = response
            .json()
            .await
            .map_err(|e| Error::engine(format!("bigquery: parse token response: {e}")))?;
        *guard = Some(CachedToken {
            token: token.access_token.clone(),
            expires_at: Instant::now() + Duration::from_secs(token.expires_in.max(60)),
        });
        Ok(token.access_token)
    }

    /// Run one statement through `jobs.query`, waiting for completion (polling
    /// `jobs.getQueryResults` when the job outlives the request timeout) and
    /// following result pages.
    async fn run_query(
        &self,
        sql: &str,
        params: &[QueryParam],
    ) -> Result<Vec<Map<String, JsonValue>>> {
        let inner = &self.inner;
        let mut body = json!({
            "query": sql,
            "useLegacySql": false,
            "timeoutMs": QUERY_TIMEOUT.as_millis() as u64,
        });
        if !params.is_empty() {
            body["queryParameters"] =
                JsonValue::Array(params.iter().map(QueryParam::to_api).collect());
        }
        if let Some(location) = &inner.location {
            body["location"] = JsonValue::from(location.as_str());
        }
        let url = format!("{}/projects/{}/queries", inner.base_url, inner.project);
        let mut response = self.send(inner.http.post(&url).json(&body)).await?;

        let mut rows = Vec::new();
        loop {
            let complete = response
                .get("jobComplete")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false);
            if complete {
                if let Some(errors) = response.get("errors").and_then(JsonValue::as_array)
                    && let Some(first) = errors.first()
                {
                    return Err(Error::engine(format!(
                        "bigquery query failed: {}",
                        first
                            .get("message")
                            .and_then(JsonValue::as_str)
                            .unwrap_or("unknown error")
                    )));
                }
                decode_rows(&response, &mut rows);
                let Some(page_token) = response.get("pageToken").and_then(JsonValue::as_str) else {
                    return Ok(rows);
                };
                response = self.get_query_results(&response, Some(page_token)).await?;
            } else {
                response = self.get_query_results(&response, None).await?;
            }
        }
    }

    async fn get_query_results(
        &self,
        response: &JsonValue,
        page_token: Option<&str>,
    ) -> Result<JsonValue> {
        let inner = &self.inner;
        let job = response
            .get("jobReference")
            .ok_or_else(|| Error::engine("bigquery query response has no jobReference"))?;
        let job_id = job
            .get("jobId")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| Error::engine("bigquery query response has no jobId"))?;
        let url = format!(
            "{}/projects/{}/queries/{job_id}",
            inner.base_url, inner.project
        );
        let mut query: Vec<(&str, String)> =
            vec![("timeoutMs", (QUERY_TIMEOUT.as_millis() as u64).to_string())];
        if let Some(location) = job.get("location").and_then(JsonValue::as_str) {
            query.push(("location", location.to_string()));
        }
        if let Some(token) = page_token {
            query.push(("pageToken", token.to_string()));
        }
        self.send(inner.http.get(&url).query(&query)).await
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> Result<JsonValue> {
        let token = self.token().await?;
        let response = request
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| Error::engine(format!("bigquery request: {e}")))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| Error::engine(format!("bigquery response: {e}")))?;
        if !status.is_success() {
            let message = serde_json::from_str::<JsonValue>(&text)
                .ok()
                .and_then(|v| {
                    v.get("error")
                        .and_then(|e| e.get("message"))
                        .and_then(JsonValue::as_str)
                        .map(str::to_string)
                })
                .unwrap_or_else(|| truncate(&text, 500));
            return Err(Error::engine(format!("bigquery: HTTP {status}: {message}")));
        }
        serde_json::from_str(&text)
            .map_err(|e| Error::engine(format!("bigquery: invalid JSON response: {e}")))
    }
}

/// Decode a `jobs.query` / `getQueryResults` page's `rows` (`{"f": [{"v": …}]}`)
/// into objects keyed by the `schema.fields` names.
fn decode_rows(response: &JsonValue, out: &mut Vec<Map<String, JsonValue>>) {
    let fields: Vec<(&str, &str)> = response
        .get("schema")
        .and_then(|s| s.get("fields"))
        .and_then(JsonValue::as_array)
        .map(|fields| {
            fields
                .iter()
                .map(|f| {
                    (
                        f.get("name").and_then(JsonValue::as_str).unwrap_or(""),
                        f.get("type").and_then(JsonValue::as_str).unwrap_or(""),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let Some(rows) = response.get("rows").and_then(JsonValue::as_array) else {
        return;
    };
    for row in rows {
        let cells = row.get("f").and_then(JsonValue::as_array);
        let mut object = Map::new();
        for (index, (name, ty)) in fields.iter().enumerate() {
            let raw = cells
                .and_then(|c| c.get(index))
                .and_then(|c| c.get("v"))
                .cloned()
                .unwrap_or(JsonValue::Null);
            object.insert((*name).to_string(), decode_cell(ty, raw));
        }
        out.push(object);
    }
}

fn decode_cell(ty: &str, raw: JsonValue) -> JsonValue {
    let JsonValue::String(text) = &raw else {
        return raw;
    };
    match ty {
        "INTEGER" | "INT64" => text.parse::<i64>().map(JsonValue::from).unwrap_or(raw),
        "FLOAT" | "FLOAT64" => text.parse::<f64>().map(JsonValue::from).unwrap_or(raw),
        "BOOLEAN" | "BOOL" => match text.as_str() {
            "true" => JsonValue::Bool(true),
            "false" => JsonValue::Bool(false),
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
// Credentials
// ---------------------------------------------------------------------------

/// Resolve the credential source: an explicit key file, else Application
/// Default Credentials. Returns the auth, a credentials-derived stable id, and
/// the project the credentials name (if any).
fn resolve_credentials(credentials_path: Option<&Path>) -> Result<(Auth, String, Option<String>)> {
    if let Some(path) = credentials_path {
        return load_credentials_file(path);
    }
    if let Some(path) = std::env::var_os("GOOGLE_APPLICATION_CREDENTIALS").filter(|p| !p.is_empty())
    {
        return load_credentials_file(Path::new(&path));
    }
    if let Some(path) = gcloud_adc_path()
        && path.is_file()
    {
        return load_credentials_file(&path);
    }
    Ok((Auth::Metadata, "metadata".to_string(), None))
}

/// `gcloud auth application-default login` writes its credentials under the
/// gcloud config directory (`CLOUDSDK_CONFIG`, else `~/.config/gcloud`).
fn gcloud_adc_path() -> Option<PathBuf> {
    let config_dir = match std::env::var_os("CLOUDSDK_CONFIG") {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => {
            let home = std::env::var_os("HOME")
                .filter(|h| !h.is_empty())
                .or_else(|| std::env::var_os("USERPROFILE"))?;
            PathBuf::from(home).join(".config").join("gcloud")
        }
    };
    Some(config_dir.join("application_default_credentials.json"))
}

fn load_credentials_file(path: &Path) -> Result<(Auth, String, Option<String>)> {
    let raw = std::fs::read_to_string(path).map_err(|e| {
        Error::engine(format!(
            "bigquery: read credentials file {}: {e}",
            path.display()
        ))
    })?;
    let json: JsonValue = serde_json::from_str(&raw)
        .map_err(|e| Error::engine(format!("bigquery: parse credentials file: {e}")))?;
    let field = |key: &str| -> Result<String> {
        json.get(key)
            .and_then(JsonValue::as_str)
            .map(str::to_string)
            .ok_or_else(|| Error::engine(format!("bigquery: credentials file missing {key:?}")))
    };
    let optional = |key: &str| {
        json.get(key)
            .and_then(JsonValue::as_str)
            .map(str::to_string)
    };
    let token_uri = optional("token_uri").unwrap_or_else(|| DEFAULT_TOKEN_URI.to_string());
    match json.get("type").and_then(JsonValue::as_str) {
        Some("service_account") => {
            let client_email = field("client_email")?;
            let auth = Auth::ServiceAccount {
                client_email: client_email.clone(),
                private_key: field("private_key")?,
                token_uri,
            };
            Ok((
                auth,
                format!("service_account:{client_email}"),
                optional("project_id"),
            ))
        }
        Some("authorized_user") => {
            let client_id = field("client_id")?;
            let auth = Auth::AuthorizedUser {
                client_id: client_id.clone(),
                client_secret: field("client_secret")?,
                refresh_token: field("refresh_token")?,
                token_uri,
            };
            Ok((
                auth,
                format!("authorized_user:{client_id}"),
                optional("quota_project_id"),
            ))
        }
        other => Err(Error::engine(format!(
            "bigquery: unsupported credentials file type {other:?} (expected \
             \"service_account\" or \"authorized_user\")"
        ))),
    }
}

fn resolve_project(configured: Option<String>, hint: Option<String>) -> Result<String> {
    let from_env = || {
        ["GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT"]
            .iter()
            .find_map(|name| std::env::var(name).ok().filter(|v| !v.is_empty()))
    };
    let project = configured
        .filter(|p| !p.is_empty())
        .or_else(from_env)
        .or(hint)
        .ok_or_else(|| {
            Error::engine(
                "bigquery: no project configured; set BigQueryConfig.project or \
                 GOOGLE_CLOUD_PROJECT",
            )
        })?;
    validate_project_id(&project)?;
    Ok(project)
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Build and sign a service-account JWT assertion (RS256) for the BigQuery scope.
fn make_jwt(
    client_email: &str,
    private_key_pem: &str,
    token_uri: &str,
    now: u64,
) -> Result<String> {
    let header = json!({ "alg": "RS256", "typ": "JWT" });
    let claims = json!({
        "iss": client_email,
        "scope": BIGQUERY_SCOPE,
        "aud": token_uri,
        "iat": now,
        "exp": now + 3600,
    });
    let to_vec = |v: &JsonValue| -> Result<Vec<u8>> {
        serde_json::to_vec(v).map_err(|e| Error::engine(format!("encode JWT json: {e}")))
    };
    let header_b64 = URL_SAFE_NO_PAD.encode(to_vec(&header)?);
    let claims_b64 = URL_SAFE_NO_PAD.encode(to_vec(&claims)?);
    let signing_input = format!("{header_b64}.{claims_b64}");
    let key = RsaPrivateKey::from_pkcs8_pem(private_key_pem)
        .map_err(|e| Error::engine(format!("parse service-account private key: {e}")))?;
    let signing_key = SigningKey::<Sha256>::new(key);
    let signature = signing_key
        .try_sign(signing_input.as_bytes())
        .map_err(|e| Error::engine(format!("sign service-account JWT: {e}")))?;
    Ok(format!(
        "{signing_input}.{}",
        URL_SAFE_NO_PAD.encode(signature.to_bytes())
    ))
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// A BigQuery column. `bigquery_type` is the GoogleSQL type as written in
/// `CREATE TABLE` (e.g. `INT64`, `STRING`, `JSON`, `ARRAY<FLOAT64>`). A column
/// marked [`ColumnDef::parse_json`] receives its value JSON-serialized and
/// written through `PARSE_JSON`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnDef {
    pub bigquery_type: String,
    pub nullable: bool,
    pub use_parse_json: bool,
}

impl ColumnDef {
    pub fn new(bigquery_type: impl Into<String>) -> Self {
        Self {
            bigquery_type: bigquery_type.into().trim().to_string(),
            nullable: true,
            use_parse_json: false,
        }
    }

    /// Mark the column `NOT NULL`.
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Write the value as JSON text through `PARSE_JSON` (for `JSON` columns).
    pub fn parse_json(mut self) -> Self {
        self.use_parse_json = true;
        self
    }

    /// The `JSON` column every complex (collection / map / struct) field maps to.
    fn json() -> Self {
        Self::new("JSON").parse_json()
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
            validate_sql_type(&def.bigquery_type)?;
            if out.insert(name.clone(), def).is_some() {
                return Err(Error::engine(format!(
                    "BigQuery table schema contains duplicate column {name:?}"
                )));
            }
        }
        let primary_key: Vec<String> = primary_key.into_iter().map(Into::into).collect();
        if primary_key.is_empty() {
            return Err(Error::engine("BigQuery table primary key cannot be empty"));
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
    /// table as Python's `bigquery` connector.
    pub fn from_row<T: crate::row_schema::SchemaFields>(
        primary_key: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let columns = T::schema_fields()
            .into_iter()
            .map(|f| (f.name.clone(), bigquery_column_def(&f)));
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
/// BigQuery [`ColumnDef`], mirroring Python's `bigquery` `_LEAF_TYPE_MAPPINGS`.
fn bigquery_column_def(field: &crate::row_schema::SchemaField) -> ColumnDef {
    use crate::row_schema::LogicalType as L;
    let mut def = match &field.logical_type {
        L::Bool => ColumnDef::new("BOOL"),
        L::Int16 | L::Int32 | L::Int64 => ColumnDef::new("INT64"),
        // Python maps `timedelta` to FLOAT64 seconds.
        L::Float32 | L::Float64 | L::Duration => ColumnDef::new("FLOAT64"),
        L::Decimal => ColumnDef::new("NUMERIC"),
        L::Text | L::Uuid => ColumnDef::new("STRING"),
        L::Bytes => ColumnDef::new("BYTES"),
        L::Date => ColumnDef::new("DATE"),
        L::Time => ColumnDef::new("TIME"),
        L::DateTime => ColumnDef::new("TIMESTAMP"),
        L::Json => ColumnDef::json(),
        L::Vector { .. } => ColumnDef::new("ARRAY<FLOAT64>"),
        L::Custom(s) => ColumnDef::new(s.as_str()),
    };
    def.nullable = field.nullable;
    def
}

/// Options for the `*_with_options` table-target constructors.
#[derive(Clone, Debug, Default)]
pub struct BigQueryTableOptions {
    /// Project that owns the table. `None` means the connection's project, but
    /// the two are different target-state keys for the same table: declare each
    /// table through one form only.
    pub project: Option<String>,
    pub managed_by: ManagedBy,
}

// ---------------------------------------------------------------------------
// Public target API: constructor / declaration / mount split
// ---------------------------------------------------------------------------

/// A declarative BigQuery table target — a handle to declare rows on.
#[derive(Clone)]
pub struct TableTarget {
    dataset: Arc<str>,
    table_name: Arc<str>,
    table_schema: TableSchema,
    rows: TargetStateProvider<RowState>,
}

/// Build a composable [`TargetState`] for a system-managed BigQuery table in the
/// connection's project.
pub fn table_target(
    ctx: &Ctx,
    conn: &ContextKey<BigQueryConnection>,
    dataset: impl Into<String>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
) -> Result<TargetState<TableSpec>> {
    table_target_with_options(
        ctx,
        conn,
        dataset,
        table_name,
        table_schema,
        BigQueryTableOptions::default(),
    )
}

/// [`table_target`] with explicit [`BigQueryTableOptions`] (`project`,
/// `managed_by`).
pub fn table_target_with_options(
    ctx: &Ctx,
    conn: &ContextKey<BigQueryConnection>,
    dataset: impl Into<String>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    options: BigQueryTableOptions,
) -> Result<TargetState<TableSpec>> {
    let dataset = dataset.into();
    let table_name = table_name.into();
    validate_ident(&dataset, "dataset name")?;
    validate_ident(&table_name, "table name")?;
    if let Some(project) = &options.project {
        validate_project_id(project)?;
    }
    let key = TableKey {
        conn_key: conn.name().to_string(),
        project: options.project,
        dataset,
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

/// Declare a BigQuery table target in the **current** component (the row child
/// provider resolves at this component's commit) and return a handle.
pub fn declare_table_target(
    ctx: &Ctx,
    conn: &ContextKey<BigQueryConnection>,
    dataset: impl Into<String>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
) -> Result<TableTarget> {
    declare_table_target_with_options(
        ctx,
        conn,
        dataset,
        table_name,
        table_schema,
        BigQueryTableOptions::default(),
    )
}

/// [`declare_table_target`] with explicit [`BigQueryTableOptions`].
pub fn declare_table_target_with_options(
    ctx: &Ctx,
    conn: &ContextKey<BigQueryConnection>,
    dataset: impl Into<String>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    options: BigQueryTableOptions,
) -> Result<TableTarget> {
    let ts = table_target_with_options(ctx, conn, dataset, table_name, table_schema, options)?;
    let spec = ts.value().clone();
    let rows = declare_target_state_with_child::<TableSpec, RowState>(ctx, ts)?;
    Ok(table_target_handle(spec, rows))
}

/// Mount a BigQuery table target **foreground** (rows can be declared
/// immediately) and return a handle.
pub async fn mount_table_target(
    ctx: &Ctx,
    conn: &ContextKey<BigQueryConnection>,
    dataset: impl Into<String>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
) -> Result<TableTarget> {
    mount_table_target_with_options(
        ctx,
        conn,
        dataset,
        table_name,
        table_schema,
        BigQueryTableOptions::default(),
    )
    .await
}

/// [`mount_table_target`] with explicit [`BigQueryTableOptions`].
pub async fn mount_table_target_with_options(
    ctx: &Ctx,
    conn: &ContextKey<BigQueryConnection>,
    dataset: impl Into<String>,
    table_name: impl Into<String>,
    table_schema: TableSchema,
    options: BigQueryTableOptions,
) -> Result<TableTarget> {
    let ts = table_target_with_options(ctx, conn, dataset, table_name, table_schema, options)?;
    let spec = ts.value().clone();
    let rows = mount_target::<TableSpec, RowState>(ctx, ts).await?;
    Ok(table_target_handle(spec, rows))
}

fn table_target_handle(spec: TableSpec, rows: TargetStateProvider<RowState>) -> TableTarget {
    TableTarget {
        dataset: Arc::from(spec.key.dataset),
        table_name: Arc::from(spec.key.table_name),
        table_schema: spec.table_schema,
        rows,
    }
}

impl TableTarget {
    pub fn dataset(&self) -> &str {
        &self.dataset
    }

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
/// (optional) owning project, dataset and table name.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TableKey {
    conn_key: String,
    project: Option<String>,
    dataset: String,
    table_name: String,
}

impl TableKey {
    fn stable_key(self) -> StableKey {
        StableKey::Array(Arc::from([
            StableKey::Str(Arc::from(self.conn_key)),
            match self.project {
                Some(project) => StableKey::Str(Arc::from(project)),
                None => StableKey::Null,
            },
            StableKey::Str(Arc::from(self.dataset)),
            StableKey::Str(Arc::from(self.table_name)),
        ]))
    }

    fn from_stable_key(key: &StableKey) -> Result<Self> {
        if let StableKey::Array(parts) = key
            && let [
                StableKey::Str(conn_key),
                project,
                StableKey::Str(dataset),
                StableKey::Str(table_name),
            ] = &parts[..]
        {
            let project = match project {
                StableKey::Str(project) => Some(project.to_string()),
                StableKey::Null => None,
                other => {
                    return Err(Error::engine(format!(
                        "unexpected bigquery table target project key: {other:?}"
                    )));
                }
            };
            return Ok(Self {
                conn_key: conn_key.to_string(),
                project,
                dataset: dataset.to_string(),
                table_name: table_name.to_string(),
            });
        }
        Err(Error::engine(format!(
            "unexpected bigquery table target key: {key:?}"
        )))
    }

    fn qualified_table_name(&self) -> String {
        match &self.project {
            Some(project) => format!("`{project}.{}.{}`", self.dataset, self.table_name),
            None => format!("`{}.{}`", self.dataset, self.table_name),
        }
    }

    fn qualified_dataset_name(&self) -> String {
        match &self.project {
            Some(project) => format!("`{project}.{}`", self.dataset),
            None => format!("`{}`", self.dataset),
        }
    }
}

/// Spec for a BigQuery table (the declared container value).
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

// Composite schema tracking (mirrors Python `connectors/bigquery/_target.py`):
// the `main` record is the primary-key signature; one `sub` record per non-PK
// column (type + nullability) drives incremental `ALTER TABLE`.

const COL_SUBKEY_PREFIX: &str = "col:";

fn col_subkey(col_name: &str) -> String {
    format!("{COL_SUBKEY_PREFIX}{col_name}")
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct PkColumnTrackingRecord {
    name: String,
    bigquery_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NonPkColumnTrackingRecord {
    bigquery_type: String,
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
            bigquery_type: schema.columns()[name].bigquery_type.clone(),
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
                    bigquery_type: col.bigquery_type.clone(),
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

fn resolve_conn(host_ctx: &Arc<ContextStore>, conn_key: &str) -> Result<Arc<BigQueryConnection>> {
    host_ctx
        .resolve::<BigQueryConnection>(conn_key)
        .ok_or_else(|| {
            Error::engine(format!(
                "bigquery target: connection `{conn_key}` was not provided in the environment \
                 (call Environment::builder().provide_key(&KEY, conn))"
            ))
        })
}

#[linkme::distributed_slice(STATIC_ROOT_PROVIDERS)]
static TABLE_PROVIDER: StaticRootProvider =
    StaticRootProvider::new::<TableSpec, TableHandler>("cocoindex/bigquery/table");

/// Root handler for every BigQuery table target: stateless, the connection is
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
            for action in actions {
                let (TargetAction::Create(a) | TargetAction::Update(a) | TargetAction::Delete(a)) =
                    action;
                let conn = resolve_conn(&host_ctx, &a.key.conn_key)?;
                apply_table_action(&conn, a).await?;
            }
            Ok(())
        },
        |a: &TableAction| {
            let spec = a
                .spec
                .clone()
                .ok_or_else(|| Error::engine("BigQuery table action missing spec"))?;
            Ok(ChildTargetDef::new::<RowState, _>(RowHandler {
                sink: SinkCache::default(),
                spec,
            }))
        },
    )
}

/// Apply one resolved table action. Mirrors Python's `_TableHandler._apply_actions`.
async fn apply_table_action(conn: &BigQueryConnection, action: TableAction) -> Result<()> {
    let TableAction {
        key,
        spec,
        main_action,
        column_actions,
    } = action;
    for sql in table_action_statements(&key, spec.as_ref(), main_action, &column_actions) {
        conn.run_query(&sql, &[]).await?;
    }
    Ok(())
}

/// The DDL statements for a table action, in order. A user-managed table yields
/// none. A drop or structural rewrite removes the table first; `insert` /
/// `replace` create it from the desired schema, `upsert` creates it only if
/// missing and then reconciles columns.
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
                    "ALTER TABLE {table} DROP COLUMN IF EXISTS `{col_name}`"
                ));
            }
            (DiffAction::Insert, Some(col)) => {
                statements.push(format!(
                    "ALTER TABLE {table} ADD COLUMN {}",
                    added_column_sql(col_name, col)
                ));
            }
            (DiffAction::Upsert, Some(col)) => {
                statements.push(format!(
                    "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {}",
                    added_column_sql(col_name, col)
                ));
            }
            (DiffAction::Replace, Some(col)) => {
                statements.push(format!(
                    "ALTER TABLE {table} DROP COLUMN IF EXISTS `{col_name}`"
                ));
                statements.push(format!(
                    "ALTER TABLE {table} ADD COLUMN {}",
                    added_column_sql(col_name, col)
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
    let pk: HashSet<&String> = schema.primary_key().iter().collect();
    let mut col_defs: Vec<String> = schema
        .ordered_columns()
        .into_iter()
        .map(|(name, col)| column_sql(name, col, pk.contains(name)))
        .collect();
    col_defs.push(format!(
        "PRIMARY KEY ({}) NOT ENFORCED",
        quoted_list(schema.primary_key())
    ));
    let if_not_exists = if if_not_exists { " IF NOT EXISTS" } else { "" };
    vec![
        format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            key.qualified_dataset_name()
        ),
        format!(
            "CREATE TABLE{if_not_exists} {} ({})",
            key.qualified_table_name(),
            col_defs.join(", ")
        ),
    ]
}

fn column_sql(name: &str, col: &ColumnDef, is_pk: bool) -> String {
    let not_null = if col.nullable && !is_pk {
        ""
    } else {
        " NOT NULL"
    };
    format!("`{name}` {}{not_null}", col.bigquery_type)
}

/// BigQuery rejects `ALTER TABLE … ADD COLUMN` with a `NOT NULL` (REQUIRED)
/// column, so an added column is always nullable in the table whatever the
/// schema says.
fn added_column_sql(name: &str, col: &ColumnDef) -> String {
    format!("`{name}` {}", col.bigquery_type)
}

fn quoted_list(names: &[String]) -> String {
    names
        .iter()
        .map(|c| format!("`{c}`"))
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

/// Apply row mutations: declared rows are `MERGE`d in batches, un-declared rows
/// are deleted by primary key. Mirrors Python's `_RowHandler._apply_actions`.
async fn apply_rows(
    conn: &BigQueryConnection,
    spec: &TableSpec,
    mutations: Vec<(Vec<JsonValue>, Option<RowState>)>,
) -> Result<()> {
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
    let rows_per_merge = (MAX_PARAMETERS_PER_STATEMENT / schema.columns().len()).max(1);
    for chunk in upserts.chunks(rows_per_merge) {
        let (sql, params) = merge_statement(&table, schema, chunk)?;
        conn.run_query(&sql, &params).await?;
    }
    let keys_per_delete = (MAX_PARAMETERS_PER_STATEMENT / schema.primary_key().len()).max(1);
    for chunk in deletes.chunks(keys_per_delete) {
        let (sql, params) = delete_statement(&table, schema, chunk)?;
        conn.run_query(&sql, &params).await?;
    }
    Ok(())
}

/// One typed named query parameter (`@name`). Mirrors Python's `QueryParam`.
#[derive(Clone, Debug, PartialEq)]
struct QueryParam {
    name: String,
    param_type: String,
    value: JsonValue,
}

impl QueryParam {
    /// The REST `QueryParameter` representation.
    fn to_api(&self) -> JsonValue {
        let (parameter_type, parameter_value) = match self.param_type.strip_prefix("ARRAY<") {
            Some(rest) => {
                let inner = rest.trim_end_matches('>');
                let values = match &self.value {
                    JsonValue::Array(items) => items
                        .iter()
                        .map(|item| json!({ "value": scalar_param_value(inner, item) }))
                        .collect(),
                    _ => Vec::new(),
                };
                (
                    json!({ "type": "ARRAY", "arrayType": { "type": inner } }),
                    json!({ "arrayValues": values }),
                )
            }
            None => (
                json!({ "type": self.param_type }),
                json!({ "value": scalar_param_value(&self.param_type, &self.value) }),
            ),
        };
        json!({
            "name": self.name,
            "parameterType": parameter_type,
            "parameterValue": parameter_value,
        })
    }
}

/// The REST API carries every scalar parameter as a string (or `null`).
fn scalar_param_value(param_type: &str, value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Null => JsonValue::Null,
        JsonValue::String(s) => JsonValue::String(s.clone()),
        JsonValue::Bool(b) => JsonValue::String(b.to_string()),
        JsonValue::Number(n) => JsonValue::String(n.to_string()),
        JsonValue::Array(_) if param_type == "BYTES" => match json_byte_array(value) {
            Some(bytes) => JsonValue::String(STANDARD.encode(bytes)),
            None => JsonValue::String(value.to_string()),
        },
        JsonValue::Object(_) | JsonValue::Array(_) => {
            // A serde `Duration` (`{secs, nanos}`) becomes seconds, as Python's
            // `timedelta.total_seconds()`; anything else is its JSON text.
            match crate::row_schema::serialized_duration_parts(value) {
                Some((secs, nanos)) => {
                    JsonValue::String((secs as f64 + f64::from(nanos) / 1e9).to_string())
                }
                None => JsonValue::String(value.to_string()),
            }
        }
    }
}

/// The query-parameter type for a column. Mirrors Python's `_query_param_type`:
/// `PARSE_JSON` columns bind as `STRING`, known scalar and `ARRAY<…>` types bind
/// as themselves, anything else as `STRING` (BigQuery casts on insert).
fn query_param_type(col: &ColumnDef) -> String {
    if col.use_parse_json {
        return "STRING".to_string();
    }
    let ty = col.bigquery_type.to_ascii_uppercase();
    const SCALARS: [&str; 14] = [
        "BOOL",
        "BOOLEAN",
        "INT64",
        "INTEGER",
        "FLOAT64",
        "FLOAT",
        "NUMERIC",
        "BIGNUMERIC",
        "STRING",
        "BYTES",
        "DATE",
        "TIME",
        "DATETIME",
        "TIMESTAMP",
    ];
    if SCALARS.contains(&ty.as_str()) || (ty.starts_with("ARRAY<") && ty.ends_with('>')) {
        ty
    } else {
        "STRING".to_string()
    }
}

/// The value bound for a column: JSON text for `PARSE_JSON` columns (a string
/// value is taken to be JSON already, as in Python), the raw value otherwise.
fn encode_value(col: &ColumnDef, value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Null => JsonValue::Null,
        JsonValue::String(_) if col.use_parse_json => value.clone(),
        _ if col.use_parse_json => JsonValue::String(value.to_string()),
        _ => value.clone(),
    }
}

/// `MERGE` one batch of rows through a `USING (SELECT … UNION ALL SELECT …)`
/// source of named parameters. Mirrors Python's `_merge_sql` for one row.
fn merge_statement(
    table: &str,
    schema: &TableSchema,
    rows: &[Map<String, JsonValue>],
) -> Result<(String, Vec<QueryParam>)> {
    if rows.is_empty() {
        return Err(Error::engine("bigquery merge batch cannot be empty"));
    }
    let columns = schema.ordered_columns();
    let mut params = Vec::with_capacity(rows.len() * columns.len());
    let mut selects = Vec::with_capacity(rows.len());
    for row in rows {
        let mut exprs = Vec::with_capacity(columns.len());
        for (name, col) in &columns {
            let index = params.len();
            let value = row.get(*name).unwrap_or(&JsonValue::Null);
            params.push(QueryParam {
                name: format!("p{index}"),
                param_type: query_param_type(col),
                value: encode_value(col, value),
            });
            let expr = if col.use_parse_json {
                format!("PARSE_JSON(@p{index})")
            } else {
                format!("@p{index}")
            };
            exprs.push(format!("{expr} AS `{name}`"));
        }
        selects.push(format!("SELECT {}", exprs.join(", ")));
    }
    let on_clause = schema
        .primary_key()
        .iter()
        .map(|c| format!("target.`{c}` = source.`{c}`"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let all_names: Vec<String> = columns.iter().map(|(n, _)| (*n).clone()).collect();
    let insert_cols = quoted_list(&all_names);
    let insert_values = all_names
        .iter()
        .map(|c| format!("source.`{c}`"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!(
        "MERGE {table} AS target USING ({}) AS source ON {on_clause}",
        selects.join(" UNION ALL ")
    );
    let non_pk = schema.non_pk_columns();
    if !non_pk.is_empty() {
        let updates = non_pk
            .iter()
            .map(|c| format!("`{c}` = source.`{c}`"))
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&format!(" WHEN MATCHED THEN UPDATE SET {updates}"));
    }
    sql.push_str(&format!(
        " WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})"
    ));
    Ok((sql, params))
}

/// `DELETE` one batch of rows by primary key. Mirrors Python's `_delete_sql`:
/// `IN (…)` for a single-column key, OR-joined `( … AND … )` groups otherwise.
fn delete_statement(
    table: &str,
    schema: &TableSchema,
    keys: &[Vec<JsonValue>],
) -> Result<(String, Vec<QueryParam>)> {
    if keys.is_empty() {
        return Err(Error::engine("bigquery delete batch cannot be empty"));
    }
    let pk_cols = schema.primary_key();
    let mut params = Vec::with_capacity(keys.len() * pk_cols.len());
    let mut push_param = |col: &str, value: &JsonValue| -> String {
        let index = params.len();
        let def = &schema.columns()[col];
        params.push(QueryParam {
            name: format!("p{index}"),
            param_type: query_param_type(def),
            value: encode_value(def, value),
        });
        format!("@p{index}")
    };
    let sql = if let [pk] = pk_cols {
        let mut markers = Vec::with_capacity(keys.len());
        for key in keys {
            let [value] = &key[..] else {
                return Err(Error::engine("bigquery row key length mismatch"));
            };
            markers.push(push_param(pk, value));
        }
        format!(
            "DELETE FROM {table} WHERE `{pk}` IN ({})",
            markers.join(", ")
        )
    } else {
        let mut groups = Vec::with_capacity(keys.len());
        for key in keys {
            if key.len() != pk_cols.len() {
                return Err(Error::engine("bigquery row key length mismatch"));
            }
            let parts: Vec<String> = pk_cols
                .iter()
                .zip(key)
                .map(|(col, value)| format!("`{col}` = {}", push_param(col, value)))
                .collect();
            groups.push(format!("({})", parts.join(" AND ")));
        }
        format!("DELETE FROM {table} WHERE {}", groups.join(" OR "))
    };
    Ok((sql, params))
}

// ---------------------------------------------------------------------------
// Value / key helpers
// ---------------------------------------------------------------------------

fn row_state<R: Serialize>(row: &R, schema: &TableSchema) -> Result<Map<String, JsonValue>> {
    // Reject non-finite floats up front: `serde_json` would turn NaN/±Inf into
    // JSON null, silently corrupting the value (or failing a NOT NULL column).
    crate::finite::ensure_finite(row)
        .map_err(|e| Error::engine(format!("BigQuery target row has a {e}")))?;
    let value = serde_json::to_value(row)
        .map_err(|e| Error::engine(format!("serialize BigQuery target row: {e}")))?;
    let JsonValue::Object(mut fields) = value else {
        return Err(Error::engine(
            "BigQuery target row must serialize to an object",
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
            "unsupported BigQuery row key: {other:?}"
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

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// A Google Cloud project id: letters, digits, `_ . : -`, not starting with a
/// separator (Python's `_PROJECT_RE`).
fn validate_project_id(project: &str) -> Result<()> {
    let mut chars = project.chars();
    let valid = chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'));
    if valid {
        Ok(())
    } else {
        Err(Error::engine(format!(
            "invalid BigQuery project: {project}"
        )))
    }
}

fn validate_sql_type(value: &str) -> Result<()> {
    if value.is_empty()
        || !value.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '_' | '(' | ')' | ',' | '<' | '>' | ' ')
        })
    {
        return Err(Error::engine(format!("invalid BigQuery type: {value}")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(project: Option<&str>) -> TableKey {
        TableKey {
            conn_key: "bq".to_string(),
            project: project.map(str::to_string),
            dataset: "analytics".to_string(),
            table_name: "events".to_string(),
        }
    }

    fn schema() -> TableSchema {
        TableSchema::new(
            [
                ("id", ColumnDef::new("INT64").not_null()),
                ("payload", ColumnDef::new("JSON").parse_json()),
            ],
            ["id"],
        )
        .unwrap()
    }

    #[test]
    fn qualified_names_quote_the_whole_path() {
        assert_eq!(
            key(Some("demo-project")).qualified_table_name(),
            "`demo-project.analytics.events`"
        );
        assert_eq!(key(None).qualified_table_name(), "`analytics.events`");
        assert_eq!(
            key(Some("demo-project")).qualified_dataset_name(),
            "`demo-project.analytics`"
        );
    }

    #[test]
    fn table_key_round_trips_through_stable_key() {
        for project in [Some("demo-project"), None] {
            let original = key(project);
            let decoded = TableKey::from_stable_key(&original.clone().stable_key()).unwrap();
            assert_eq!(decoded, original);
        }
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
            #[coco(type = "NUMERIC")]
            price: String,
        }
        let schema = TableSchema::from_row::<Row>(["id"]).unwrap();
        let col = |name: &str| schema.columns()[name].clone();
        assert_eq!(col("id"), ColumnDef::new("INT64").not_null());
        assert_eq!(col("flag"), ColumnDef::new("BOOL").not_null());
        assert_eq!(col("amount"), ColumnDef::new("FLOAT64").not_null());
        assert_eq!(col("name"), ColumnDef::new("STRING"));
        assert_eq!(col("data"), ColumnDef::new("BYTES").not_null());
        assert_eq!(col("elapsed"), ColumnDef::new("FLOAT64").not_null());
        assert_eq!(
            col("payload"),
            ColumnDef::new("JSON").parse_json().not_null()
        );
        assert_eq!(
            col("embedding"),
            ColumnDef::new("ARRAY<FLOAT64>").not_null()
        );
        assert_eq!(col("price"), ColumnDef::new("NUMERIC").not_null());
    }

    #[test]
    fn create_table_statements_match_python() {
        let statements = create_table_statements(&key(Some("demo-project")), &schema(), false);
        assert_eq!(
            statements,
            vec![
                "CREATE SCHEMA IF NOT EXISTS `demo-project.analytics`".to_string(),
                "CREATE TABLE `demo-project.analytics.events` (`id` INT64 NOT NULL, `payload` \
                 JSON, PRIMARY KEY (`id`) NOT ENFORCED)"
                    .to_string(),
            ]
        );
        let upsert = create_table_statements(&key(None), &schema(), true);
        assert!(upsert[1].starts_with("CREATE TABLE IF NOT EXISTS `analytics.events`"));
    }

    #[test]
    fn table_action_statements_cover_replace_and_column_changes() {
        let spec = TableSpec {
            key: key(None),
            table_schema: schema(),
            managed_by: ManagedBy::System,
        };
        // Replace: drop, then create from the desired schema; no column actions.
        let statements = table_action_statements(
            &spec.key,
            Some(&spec),
            Some(DiffAction::Replace),
            &BTreeMap::from([(col_subkey("payload"), DiffAction::Insert)]),
        );
        assert_eq!(statements[0], "DROP TABLE IF EXISTS `analytics.events`");
        assert!(statements[2].starts_with("CREATE TABLE `analytics.events`"));
        assert_eq!(statements.len(), 3);

        // In-place column changes on an unchanged main record.
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
                "ALTER TABLE `analytics.events` DROP COLUMN IF EXISTS `old`".to_string(),
                "ALTER TABLE `analytics.events` DROP COLUMN IF EXISTS `payload`".to_string(),
                "ALTER TABLE `analytics.events` ADD COLUMN `payload` JSON".to_string(),
            ]
        );

        // Delete without a spec.
        assert_eq!(
            table_action_statements(&spec.key, None, Some(DiffAction::Delete), &BTreeMap::new()),
            vec!["DROP TABLE IF EXISTS `analytics.events`".to_string()]
        );

        // A user-managed table gets no DDL at all.
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
    fn merge_statement_uses_parse_json_and_named_params() {
        let row = json!({"id": 1, "payload": {"b": 2, "a": [1]}});
        let (sql, params) = merge_statement(
            "`demo-project.analytics.events`",
            &schema(),
            &[row.as_object().unwrap().clone()],
        )
        .unwrap();
        assert_eq!(
            sql,
            "MERGE `demo-project.analytics.events` AS target USING (SELECT @p0 AS `id`, \
             PARSE_JSON(@p1) AS `payload`) AS source ON target.`id` = source.`id` WHEN MATCHED \
             THEN UPDATE SET `payload` = source.`payload` WHEN NOT MATCHED THEN INSERT (`id`, \
             `payload`) VALUES (source.`id`, source.`payload`)"
        );
        assert_eq!(
            params,
            vec![
                QueryParam {
                    name: "p0".into(),
                    param_type: "INT64".into(),
                    value: json!(1)
                },
                QueryParam {
                    name: "p1".into(),
                    param_type: "STRING".into(),
                    value: json!("{\"a\":[1],\"b\":2}")
                },
            ]
        );
        assert_eq!(
            params[0].to_api(),
            json!({"name": "p0", "parameterType": {"type": "INT64"}, "parameterValue": {"value": "1"}})
        );
    }

    #[test]
    fn merge_statement_batches_rows_and_skips_update_without_non_pk_columns() {
        let schema = TableSchema::new([("id", ColumnDef::new("INT64"))], ["id"]).unwrap();
        let rows = [json!({"id": 1}), json!({"id": 2})].map(|v| v.as_object().unwrap().clone());
        let (sql, params) = merge_statement("`events`", &schema, &rows).unwrap();
        assert!(
            sql.contains("USING (SELECT @p0 AS `id` UNION ALL SELECT @p1 AS `id`)"),
            "{sql}"
        );
        assert!(!sql.contains("WHEN MATCHED THEN UPDATE"), "{sql}");
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"), "{sql}");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn delete_statement_supports_single_and_composite_keys() {
        let (sql, params) = delete_statement(
            "`events`",
            &schema(),
            &[vec![json!(1)], vec![json!(2)], vec![json!(3)]],
        )
        .unwrap();
        assert_eq!(sql, "DELETE FROM `events` WHERE `id` IN (@p0, @p1, @p2)");
        assert_eq!(params[2].to_api()["parameterValue"]["value"], json!("3"));

        let composite = TableSchema::new(
            [
                ("tenant", ColumnDef::new("STRING").not_null()),
                ("id", ColumnDef::new("INT64").not_null()),
            ],
            ["tenant", "id"],
        )
        .unwrap();
        let (sql, params) = delete_statement(
            "`events`",
            &composite,
            &[vec![json!("a"), json!(1)], vec![json!("b"), json!(2)]],
        )
        .unwrap();
        assert_eq!(
            sql,
            "DELETE FROM `events` WHERE (`tenant` = @p0 AND `id` = @p1) OR (`tenant` = @p2 AND \
             `id` = @p3)"
        );
        assert_eq!(params[0].param_type, "STRING");
        assert_eq!(params[1].param_type, "INT64");
    }

    #[test]
    fn query_params_encode_arrays_bytes_durations_and_nulls() {
        let array = QueryParam {
            name: "p0".into(),
            param_type: "ARRAY<FLOAT64>".into(),
            value: json!([1.5, 2.0]),
        };
        assert_eq!(
            array.to_api(),
            json!({
                "name": "p0",
                "parameterType": {"type": "ARRAY", "arrayType": {"type": "FLOAT64"}},
                "parameterValue": {"arrayValues": [{"value": "1.5"}, {"value": "2.0"}]},
            })
        );
        let bytes = QueryParam {
            name: "p1".into(),
            param_type: "BYTES".into(),
            value: json!([104, 105]),
        };
        assert_eq!(bytes.to_api()["parameterValue"]["value"], json!("aGk="));
        let duration = QueryParam {
            name: "p2".into(),
            param_type: "FLOAT64".into(),
            value: serde_json::to_value(Duration::new(2, 500_000_000)).unwrap(),
        };
        assert_eq!(duration.to_api()["parameterValue"]["value"], json!("2.5"));
        let null = QueryParam {
            name: "p3".into(),
            param_type: "STRING".into(),
            value: JsonValue::Null,
        };
        assert_eq!(null.to_api()["parameterValue"], json!({"value": null}));
    }

    #[test]
    fn query_param_type_falls_back_to_string_for_unknown_types() {
        assert_eq!(query_param_type(&ColumnDef::new("GEOGRAPHY")), "STRING");
        assert_eq!(
            query_param_type(&ColumnDef::new("json").parse_json()),
            "STRING"
        );
        assert_eq!(
            query_param_type(&ColumnDef::new("array<int64>")),
            "ARRAY<INT64>"
        );
        assert_eq!(query_param_type(&ColumnDef::new("Numeric")), "NUMERIC");
    }

    #[test]
    fn row_state_keeps_schema_columns_and_rejects_non_finite() {
        #[derive(Serialize)]
        struct Row {
            id: i64,
            payload: Vec<i64>,
            extra: &'static str,
            score: f64,
        }
        let schema = TableSchema::new(
            [
                ("id", ColumnDef::new("INT64")),
                ("payload", ColumnDef::new("JSON").parse_json()),
                ("missing", ColumnDef::new("STRING")),
            ],
            ["id"],
        )
        .unwrap();
        let fields = row_state(
            &Row {
                id: 1,
                payload: vec![1],
                extra: "x",
                score: 0.5,
            },
            &schema,
        )
        .unwrap();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields["missing"], JsonValue::Null);
        assert!(!fields.contains_key("extra"));
        let error = row_state(
            &Row {
                id: 1,
                payload: vec![],
                extra: "x",
                score: f64::NAN,
            },
            &schema,
        )
        .unwrap_err();
        assert!(error.to_string().contains("non-finite"), "{error}");
    }

    #[test]
    fn decode_rows_types_scalar_cells() {
        let response = json!({
            "schema": {"fields": [
                {"name": "id", "type": "INTEGER"},
                {"name": "score", "type": "FLOAT"},
                {"name": "ok", "type": "BOOLEAN"},
                {"name": "name", "type": "STRING"},
            ]},
            "rows": [{"f": [{"v": "1"}, {"v": "0.5"}, {"v": "true"}, {"v": "a"}]}],
        });
        let mut rows = Vec::new();
        decode_rows(&response, &mut rows);
        assert_eq!(
            rows,
            vec![
                json!({"id": 1, "score": 0.5, "ok": true, "name": "a"})
                    .as_object()
                    .unwrap()
                    .clone()
            ]
        );
    }

    #[test]
    fn identifiers_are_validated() {
        assert!(validate_project_id("demo-project").is_ok());
        assert!(validate_project_id("domain.com:proj").is_ok());
        assert!(validate_project_id("-bad").is_err());
        assert!(validate_project_id("bad project").is_err());
        assert!(
            TableSchema::new([("bad-column", ColumnDef::new("STRING"))], ["bad-column"]).is_err()
        );
        assert!(TableSchema::new([("id", ColumnDef::new("STRING; DROP"))], ["id"]).is_err());
        assert!(TableSchema::new([("id", ColumnDef::new("STRING"))], ["other"]).is_err());
    }

    #[test]
    fn jwt_has_three_parts() {
        use rsa::pkcs8::{EncodePrivateKey, LineEnding};
        let key = RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
        let pem = key.to_pkcs8_pem(LineEnding::LF).unwrap();
        let jwt = make_jwt("svc@example.iam", &pem, DEFAULT_TOKEN_URI, 1_700_000_000).unwrap();
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);
        let claims: JsonValue =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        assert_eq!(claims["scope"], json!(BIGQUERY_SCOPE));
        assert_eq!(claims["exp"], json!(1_700_003_600));
    }

    #[test]
    fn credentials_file_types_are_recognized() {
        let dir = tempfile::tempdir().unwrap();
        let sa = dir.path().join("sa.json");
        std::fs::write(
            &sa,
            json!({"type": "service_account", "client_email": "svc@example.iam", "private_key": "-----", "project_id": "demo"}).to_string(),
        )
        .unwrap();
        let (auth, id, project) = load_credentials_file(&sa).unwrap();
        assert!(matches!(auth, Auth::ServiceAccount { .. }));
        assert_eq!(id, "service_account:svc@example.iam");
        assert_eq!(project.as_deref(), Some("demo"));

        let user = dir.path().join("adc.json");
        std::fs::write(
            &user,
            json!({"type": "authorized_user", "client_id": "cid", "client_secret": "s", "refresh_token": "r", "quota_project_id": "q"}).to_string(),
        )
        .unwrap();
        let (auth, id, project) = load_credentials_file(&user).unwrap();
        assert!(matches!(auth, Auth::AuthorizedUser { .. }));
        assert_eq!(id, "authorized_user:cid");
        assert_eq!(project.as_deref(), Some("q"));

        let other = dir.path().join("other.json");
        std::fs::write(&other, json!({"type": "external_account"}).to_string()).unwrap();
        assert!(load_credentials_file(&other).is_err());
    }
}
