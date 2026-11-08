//! Azure Blob Storage source connector.
//!
//! Read-only listing and reading of the blobs in a container, mirroring
//! Python's `cocoindex.connectors.azure_blob`:
//!
//! - [`ContainerClient`] — a clone-cheap handle to one container. It speaks the
//!   Blob service REST API directly (there is no official Azure Rust SDK in the
//!   tree) and authenticates with a storage account key (Shared Key), a SAS
//!   token, a Microsoft Entra ID bearer token, or an Entra ID token it gets and
//!   renews itself from a service principal, a managed identity or the Azure
//!   CLI sign-in: see [`Credential`] and [`TokenCredential`].
//! - [`list_blobs`] returns an [`AzureBlobWalker`]; [`AzureBlobWalker::list`] /
//!   [`AzureBlobWalker::items`] enumerate matching blobs as [`AzureBlobFile`]s.
//!   Use each [`AzureBlobFile::key`] with `Ctx::mount_each` so per-file
//!   memoization handles edits and target reconciliation removes derived rows
//!   for deleted blobs.
//! - [`get_blob`] fetches a single blob's metadata; [`read`] reads a blob's
//!   content by name without fetching metadata first.
//!
//! Like the other object-store sources, [`AzureBlobFile`] serializes only stable
//! metadata for memo keys. The client and read cache are skipped by serde, while
//! the type still implements the shared async [`crate::resources::file::FileLike`]
//! trait.
//!
//! ```no_run
//! use cocoindex::connectors::azure_blob::{self, ContainerClient, Credential, ListOptions};
//!
//! # async fn demo() -> cocoindex::Result<()> {
//! let client = ContainerClient::new(
//!     "https://myaccount.blob.core.windows.net",
//!     "docs",
//!     Credential::AccountKey("<account-key>".to_string()),
//! )?;
//! let files = azure_blob::list_blobs(
//!     &client,
//!     ListOptions {
//!         prefix: "docs/".to_string(),
//!         ..Default::default()
//!     },
//! )
//! .list()
//! .await?;
//! for file in &files {
//!     let text = file.read_text().await?;
//!     println!("{}: {} chars", file.key(), text.len());
//! }
//! # Ok(())
//! # }
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use cocoindex_utils::fingerprint::Fingerprint;
use hmac::{Hmac, Mac};
use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH, ETAG, LAST_MODIFIED};
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::error::{Error, Result};
use crate::resources::file::{
    FileContentCache, FileLike, FileMetadata, FilePath, FilePathMatcher, FileSourceItem,
    MatchAllFilePathMatcher,
};

/// Blob service REST API version sent as `x-ms-version`. Bearer-token auth needs
/// 2017-11-09 or later.
const API_VERSION: &str = "2021-08-06";

// ---------------------------------------------------------------------------
// AzureBlobFilePath / AzureBlobFile — source items
// ---------------------------------------------------------------------------

/// Path of a blob: the storage account, the container, the full blob name, and
/// the path relative to the walker prefix. Mirrors Python's `AzureBlobFilePath`;
/// its memo key includes the account and container so the same relative path in
/// two containers stays distinct.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AzureBlobFilePath {
    account_name: String,
    container_name: String,
    /// Blob name relative to the walker prefix, exactly as stored, or the full
    /// blob name if no prefix was used.
    relative_path: String,
    /// Full blob name in the container (what [`resolve`](Self::resolve) returns).
    blob_name: String,
}

impl AzureBlobFilePath {
    /// The storage account name.
    pub fn account_name(&self) -> &str {
        &self.account_name
    }

    /// The container name.
    pub fn container_name(&self) -> &str {
        &self.container_name
    }

    /// Blob name relative to the walker prefix (forward slashes).
    pub fn path(&self) -> &str {
        &self.relative_path
    }

    /// The full blob name in the container.
    pub fn blob_name(&self) -> &str {
        &self.blob_name
    }

    /// The full blob name in the container (same as [`blob_name`](Self::blob_name)).
    pub fn resolve(&self) -> &str {
        &self.blob_name
    }

    /// Memo key: `(account_name, container_name, relative_path)`. Decoupled from
    /// the live connection (matches Python's `__coco_memo_key__`).
    pub fn memo_key(&self) -> impl Serialize + '_ {
        (
            &self.account_name,
            &self.container_name,
            &self.relative_path,
        )
    }
}

/// A blob discovered in or fetched from an Azure Blob Storage container.
///
/// Files returned by [`AzureBlobWalker`] and [`get_blob`] carry a clone-cheap
/// client handle so they can be read through the shared async [`FileLike`] API.
#[derive(Clone, Serialize, Deserialize)]
pub struct AzureBlobFile {
    file_path: AzureBlobFilePath,
    /// Blob size in bytes.
    pub size: u64,
    /// Last-modified time as Unix seconds (`None` if the server omitted it).
    pub modified_secs: Option<i64>,
    /// Blob ETag (content fingerprint), without surrounding quotes, if returned.
    pub etag: Option<String>,
    #[serde(skip)]
    client: Option<ContainerClient>,
    #[serde(skip, default = "default_file_cache")]
    cache: Arc<FileContentCache>,
}

impl AzureBlobFile {
    fn new(
        client: Option<ContainerClient>,
        file_path: AzureBlobFilePath,
        properties: BlobProperties,
    ) -> Self {
        let BlobProperties {
            size,
            modified_secs,
            etag,
        } = properties;
        let metadata = FileMetadata {
            size,
            modified: modified_time(modified_secs),
            content_fingerprint: etag_fingerprint(etag.as_deref()),
        };
        Self {
            file_path,
            size,
            modified_secs,
            etag,
            client,
            cache: Arc::new(FileContentCache::with_metadata(metadata)),
        }
    }

    /// Attach a client, so that a file restored by deserialization can be read.
    pub fn with_client(mut self, client: ContainerClient) -> Self {
        self.client = Some(client);
        self
    }

    /// Stable key for `Ctx::mount_each`: the blob name relative to the walker
    /// prefix, exactly as stored. Unique within a container and prefix.
    pub fn key(&self) -> String {
        self.file_path.relative_path.clone()
    }

    /// The blob's [`AzureBlobFilePath`].
    pub fn file_path(&self) -> &AzureBlobFilePath {
        &self.file_path
    }

    /// Read the blob's full content.
    pub async fn read(&self) -> Result<bytes::Bytes> {
        <Self as FileLike>::read(self).await
    }

    /// Read the first `size` bytes of the blob.
    pub async fn read_size(&self, size: usize) -> Result<bytes::Bytes> {
        <Self as FileLike>::read_size(self, size).await
    }

    /// Read the blob's content and decode it as text (BOM-aware).
    pub async fn read_text(&self) -> Result<String> {
        <Self as FileLike>::read_text(self).await
    }
}

impl std::fmt::Debug for AzureBlobFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureBlobFile")
            .field("file_path", &self.file_path)
            .field("size", &self.size)
            .field("modified_secs", &self.modified_secs)
            .field("etag", &self.etag)
            .finish()
    }
}

impl PartialEq for AzureBlobFile {
    fn eq(&self, other: &Self) -> bool {
        self.file_path == other.file_path
            && self.size == other.size
            && self.modified_secs == other.modified_secs
            && self.etag == other.etag
    }
}

impl Eq for AzureBlobFile {}

#[async_trait]
impl FileLike for AzureBlobFile {
    fn file_path(&self) -> FilePath {
        FilePath::with_base_dir(
            format!(
                "azure_blob://{}/{}",
                self.file_path.account_name, self.file_path.container_name
            ),
            PathBuf::new(),
            &self.file_path.relative_path,
        )
    }

    fn cache(&self) -> &FileContentCache {
        &self.cache
    }

    async fn fetch_metadata(&self) -> Result<FileMetadata> {
        Ok(FileMetadata {
            size: self.size,
            modified: modified_time(self.modified_secs),
            content_fingerprint: etag_fingerprint(self.etag.as_deref()),
        })
    }

    async fn read_impl(&self, size: Option<usize>) -> Result<Vec<u8>> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::engine("Azure Blob file is not attached to a ContainerClient"))?;
        read(client, &self.file_path.blob_name, size).await
    }
}

impl FileSourceItem for AzureBlobFile {
    fn key(&self) -> String {
        self.file_path.relative_path.clone()
    }
}

crate::memo::impl_file_memo_input!(AzureBlobFile);

fn default_file_cache() -> Arc<FileContentCache> {
    Arc::new(FileContentCache::new())
}

fn modified_time(secs: Option<i64>) -> SystemTime {
    match secs {
        Some(secs) if secs >= 0 => UNIX_EPOCH + Duration::from_secs(secs as u64),
        _ => UNIX_EPOCH,
    }
}

fn etag_fingerprint(etag: Option<&str>) -> Option<Fingerprint> {
    etag.and_then(|etag| Fingerprint::from(&etag).ok())
}

/// The metadata of a blob that the source keeps.
struct BlobProperties {
    size: u64,
    modified_secs: Option<i64>,
    etag: Option<String>,
}

// ---------------------------------------------------------------------------
// ContainerClient — connection handle (Clone-cheap)
// ---------------------------------------------------------------------------

/// How a [`ContainerClient`] authenticates its requests.
pub enum Credential {
    /// A storage account access key (base64, as shown in the Azure portal).
    /// Requests are signed with the Shared Key scheme.
    AccountKey(String),
    /// A shared access signature: the query string Azure generates, with or
    /// without the leading `?`. It needs the read and list permissions.
    SasToken(String),
    /// A Microsoft Entra ID OAuth 2.0 access token for the
    /// `https://storage.azure.com/` resource, such as the one
    /// `az account get-access-token --resource https://storage.azure.com/`
    /// prints. The identity needs `Storage Blob Data Reader`. The client does
    /// not renew the token.
    BearerToken(String),
    /// A Microsoft Entra ID token the client gets itself and renews as it
    /// expires, from one of the sources `DefaultAzureCredential` uses.
    TokenCredential(TokenCredential),
}

/// Where a [`Credential::TokenCredential`] gets its Entra ID token: the sources
/// of the Azure SDK's `DefaultAzureCredential` that need no interactive sign-in.
/// The token is for `https://storage.azure.com/`, and the identity needs
/// `Storage Blob Data Reader` on the container.
#[derive(Clone, Debug)]
pub enum TokenCredential {
    /// The first of these that is configured where the process runs:
    /// [`ClientSecret`](Self::ClientSecret) from `AZURE_TENANT_ID`,
    /// `AZURE_CLIENT_ID` and `AZURE_CLIENT_SECRET`;
    /// [`ManagedIdentity`](Self::ManagedIdentity) when the process runs on
    /// Azure; else [`AzureCli`](Self::AzureCli).
    Default,
    /// A service principal with a client secret (the `AZURE_*` environment
    /// variables of `DefaultAzureCredential`, given explicitly).
    ClientSecret {
        tenant_id: String,
        client_id: String,
        client_secret: String,
    },
    /// The managed identity of the Azure resource the process runs on: App
    /// Service, Functions and Container Apps through their identity endpoint
    /// (`IDENTITY_ENDPOINT` / `IDENTITY_HEADER`), a virtual machine through the
    /// instance metadata service. `client_id` picks a user-assigned identity;
    /// `None` is the system-assigned one.
    ManagedIdentity { client_id: Option<String> },
    /// The account signed in with `az login`, through
    /// `az account get-access-token`.
    AzureCli,
}

enum Auth {
    SharedKey { key: Vec<u8> },
    Sas { query: String },
    Bearer { header: String },
    Token(Box<TokenSource>),
}

struct ContainerClientInner {
    http: reqwest::Client,
    /// The container URL without a trailing slash:
    /// `https://{account}.blob.core.windows.net/{container}`.
    container_url: reqwest::Url,
    account_name: String,
    container_name: String,
    auth: Auth,
}

/// A connection to one Azure Blob Storage container. Clone-cheap (the
/// underlying HTTP client and credential are shared).
#[derive(Clone)]
pub struct ContainerClient {
    inner: Arc<ContainerClientInner>,
}

impl ContainerClient {
    /// Build a client for the container `container_name` of the storage account
    /// at `account_url` (`https://myaccount.blob.core.windows.net`).
    ///
    /// The account name is the first label of the URL's host. A URL with a path
    /// (the Azurite emulator's `http://127.0.0.1:10000/devstoreaccount1`) names
    /// the account by the first segment of its path instead.
    pub fn new(
        account_url: &str,
        container_name: impl Into<String>,
        credential: Credential,
    ) -> Result<Self> {
        let container_name = container_name.into();
        if container_name.is_empty() {
            return Err(Error::engine("Azure Blob container name is empty"));
        }
        let account_url = account_url.trim_end_matches('/');
        let parsed = reqwest::Url::parse(account_url).map_err(|e| {
            Error::engine(format!(
                "invalid Azure Blob account URL {account_url:?}: {e}"
            ))
        })?;
        let account_name = account_name_of(&parsed).ok_or_else(|| {
            Error::engine(format!(
                "Azure Blob account URL {account_url:?} does not name a storage account"
            ))
        })?;
        let container_url = format!("{account_url}/{}", percent_encode(&container_name, false));
        let container_url = reqwest::Url::parse(&container_url).map_err(|e| {
            Error::engine(format!(
                "invalid Azure Blob container URL {container_url:?}: {e}"
            ))
        })?;
        let auth = match credential {
            Credential::AccountKey(key) => Auth::SharedKey {
                key: BASE64.decode(key.trim()).map_err(|e| {
                    Error::engine(format!("Azure Blob account key is not valid base64: {e}"))
                })?,
            },
            Credential::SasToken(token) => Auth::Sas {
                query: token.trim().trim_start_matches('?').to_string(),
            },
            Credential::BearerToken(token) => Auth::Bearer {
                header: format!("Bearer {}", token.trim()),
            },
            Credential::TokenCredential(credential) => {
                Auth::Token(Box::new(TokenSource::new(credential)))
            }
        };
        Ok(Self {
            inner: Arc::new(ContainerClientInner {
                http: reqwest::Client::new(),
                container_url,
                account_name,
                container_name,
                auth,
            }),
        })
    }

    /// The storage account name.
    pub fn account_name(&self) -> &str {
        &self.inner.account_name
    }

    /// The container name.
    pub fn container_name(&self) -> &str {
        &self.inner.container_name
    }

    fn file_path(&self, relative_path: String, blob_name: String) -> AzureBlobFilePath {
        AzureBlobFilePath {
            account_name: self.inner.account_name.clone(),
            container_name: self.inner.container_name.clone(),
            relative_path,
            blob_name,
        }
    }

    /// Send an authenticated request to the container (`blob_name` `None`) or to
    /// one of its blobs. `query` holds unencoded `(name, value)` pairs, and
    /// `range` the value of an `x-ms-range` header. The response is returned
    /// whatever its status.
    async fn send(
        &self,
        method: Method,
        blob_name: Option<&str>,
        query: &[(&str, &str)],
        range: Option<&str>,
    ) -> Result<reqwest::Response> {
        let inner = self.inner.as_ref();
        let mut url = inner.container_url.clone();
        if let Some(blob_name) = blob_name {
            url.set_path(&format!(
                "{}/{}",
                url.path(),
                percent_encode(blob_name, true)
            ));
        }
        let mut query_string = query
            .iter()
            .map(|(name, value)| format!("{name}={}", percent_encode(value, false)))
            .collect::<Vec<_>>();
        if let Auth::Sas { query } = &inner.auth {
            query_string.push(query.clone());
        }
        if !query_string.is_empty() {
            url.set_query(Some(&query_string.join("&")));
        }

        // `x-ms-*` headers, in the lexicographic order Shared Key signs them in.
        let date = chrono::Utc::now()
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let mut ms_headers = vec![("x-ms-date", date.as_str())];
        if let Some(range) = range {
            ms_headers.push(("x-ms-range", range));
        }
        ms_headers.push(("x-ms-version", API_VERSION));

        let authorization = match &inner.auth {
            Auth::Token(source) => Some(format!("Bearer {}", source.token(&inner.http).await?)),
            Auth::SharedKey { key } => {
                let string_to_sign = shared_key_string_to_sign(
                    method.as_str(),
                    &ms_headers,
                    &inner.account_name,
                    url.path(),
                    query,
                );
                Some(format!(
                    "SharedKey {}:{}",
                    inner.account_name,
                    sign(key, &string_to_sign)?
                ))
            }
            Auth::Sas { .. } => None,
            Auth::Bearer { header } => Some(header.clone()),
        };

        let mut request = inner.http.request(method.clone(), url);
        for (name, value) in ms_headers {
            request = request.header(name, value);
        }
        if let Some(authorization) = authorization {
            request = request.header(AUTHORIZATION, authorization);
        }
        request.send().await.map_err(|e| {
            Error::engine(format!(
                "azure blob {method} {}: {e}",
                self.describe(blob_name)
            ))
        })
    }

    /// `{account}/{container}[/{blob}]`, for error messages.
    fn describe(&self, blob_name: Option<&str>) -> String {
        let inner = self.inner.as_ref();
        match blob_name {
            Some(blob_name) => format!(
                "{}/{}/{blob_name}",
                inner.account_name, inner.container_name
            ),
            None => format!("{}/{}", inner.account_name, inner.container_name),
        }
    }

    /// Turn a response with an error status into an error that names the
    /// operation, the status and the service's error code.
    fn check(
        &self,
        operation: &str,
        blob_name: Option<&str>,
        response: reqwest::Response,
    ) -> Result<reqwest::Response> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let error_code = response
            .headers()
            .get("x-ms-error-code")
            .and_then(|v| v.to_str().ok())
            .map(|code| format!(" ({code})"))
            .unwrap_or_default();
        Err(Error::engine(format!(
            "azure blob {operation} {} failed: {status}{error_code}",
            self.describe(blob_name)
        )))
    }
}

// ---------------------------------------------------------------------------
// Entra ID tokens
// ---------------------------------------------------------------------------

/// The resource the token is for.
const STORAGE_RESOURCE: &str = "https://storage.azure.com/";
/// Renew a token this long before it expires (the Azure SDK's refresh offset),
/// or at half its lifetime when the lifetime is shorter.
const TOKEN_REFRESH_OFFSET: Duration = Duration::from_secs(300);
/// How long the instance metadata service probe waits before deciding the
/// process does not run on an Azure virtual machine.
const IMDS_PROBE_TIMEOUT: Duration = Duration::from_secs(1);
/// How long `az account get-access-token` may take.
const AZURE_CLI_TIMEOUT: Duration = Duration::from_secs(10);
const IMDS_TOKEN_URL: &str = "http://169.254.169.254/metadata/identity/oauth2/token";
const DEFAULT_AUTHORITY_HOST: &str = "https://login.microsoftonline.com";

/// An Entra ID token and when to renew it.
struct CachedToken {
    token: String,
    renew_at: SystemTime,
}

/// Gets and caches the token of a [`TokenCredential`]; the source
/// [`TokenCredential::Default`] settled on is kept once found.
struct TokenSource {
    credential: TokenCredential,
    state: tokio::sync::Mutex<TokenState>,
}

#[derive(Default)]
struct TokenState {
    cached: Option<CachedToken>,
    /// The concrete source `Default` found, so the chain runs once.
    resolved: Option<TokenCredential>,
}

/// A token as a source returns it: the token and its lifetime.
struct FreshToken {
    token: String,
    expires_in: Duration,
}

impl FreshToken {
    fn cached(self) -> CachedToken {
        let offset = TOKEN_REFRESH_OFFSET.min(self.expires_in / 2);
        CachedToken {
            token: self.token,
            renew_at: SystemTime::now() + (self.expires_in - offset),
        }
    }
}

impl TokenSource {
    fn new(credential: TokenCredential) -> Self {
        Self {
            credential,
            state: tokio::sync::Mutex::new(TokenState::default()),
        }
    }

    /// The current token, renewed when it is about to expire.
    async fn token(&self, http: &reqwest::Client) -> Result<String> {
        let mut state = self.state.lock().await;
        if let Some(cached) = &state.cached
            && SystemTime::now() < cached.renew_at
        {
            return Ok(cached.token.clone());
        }
        let source = match &state.resolved {
            Some(source) => source.clone(),
            None => {
                let source = match &self.credential {
                    TokenCredential::Default => resolve_default(http).await?,
                    other => other.clone(),
                };
                state.resolved = Some(source.clone());
                source
            }
        };
        let fresh = fetch_token(http, &source).await?;
        let token = fresh.token.clone();
        state.cached = Some(fresh.cached());
        Ok(token)
    }
}

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

/// The identity endpoint of App Service, Functions and Container Apps, when
/// the process runs there.
fn app_service_identity_endpoint() -> Option<(String, String)> {
    Some((env_var("IDENTITY_ENDPOINT")?, env_var("IDENTITY_HEADER")?))
}

/// The first source [`TokenCredential::Default`] finds configured.
async fn resolve_default(http: &reqwest::Client) -> Result<TokenCredential> {
    if let (Some(tenant_id), Some(client_id), Some(client_secret)) = (
        env_var("AZURE_TENANT_ID"),
        env_var("AZURE_CLIENT_ID"),
        env_var("AZURE_CLIENT_SECRET"),
    ) {
        return Ok(TokenCredential::ClientSecret {
            tenant_id,
            client_id,
            client_secret,
        });
    }
    let managed = TokenCredential::ManagedIdentity {
        client_id: env_var("AZURE_CLIENT_ID"),
    };
    if app_service_identity_endpoint().is_some() {
        return Ok(managed);
    }
    // On a virtual machine the metadata service answers at once; elsewhere the
    // link-local address is unreachable.
    let probe = http
        .get(imds_token_url())
        .header("Metadata", "true")
        .timeout(IMDS_PROBE_TIMEOUT)
        .send()
        .await;
    if probe.is_ok() {
        return Ok(managed);
    }
    if azure_cli_available().await {
        return Ok(TokenCredential::AzureCli);
    }
    Err(Error::engine(
        "azure blob: no Entra ID credential is configured: set AZURE_TENANT_ID, \
         AZURE_CLIENT_ID and AZURE_CLIENT_SECRET, run on an Azure resource with a \
         managed identity, or sign in with `az login`",
    ))
}

fn imds_token_url() -> String {
    match env_var("AZURE_POD_IDENTITY_AUTHORITY_HOST") {
        Some(host) => format!(
            "{}/metadata/identity/oauth2/token",
            host.trim_end_matches('/')
        ),
        None => IMDS_TOKEN_URL.to_string(),
    }
}

async fn fetch_token(http: &reqwest::Client, source: &TokenCredential) -> Result<FreshToken> {
    match source {
        TokenCredential::ClientSecret {
            tenant_id,
            client_id,
            client_secret,
        } => client_secret_token(http, tenant_id, client_id, client_secret).await,
        TokenCredential::ManagedIdentity { client_id } => {
            managed_identity_token(http, client_id.as_deref()).await
        }
        TokenCredential::AzureCli => azure_cli_token().await,
        TokenCredential::Default => unreachable!("Default resolves to a concrete source"),
    }
}

/// A token response: `access_token` with `expires_in` (seconds, a number or a
/// string) or `expires_on` (Unix seconds, a number or a string).
fn parse_token_response(body: &str, what: &str) -> Result<FreshToken> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| Error::engine(format!("azure blob {what}: token response: {e}")))?;
    let number = |name: &str| -> Option<u64> {
        match json.get(name)? {
            serde_json::Value::Number(n) => n.as_u64(),
            serde_json::Value::String(s) => s.parse().ok(),
            _ => None,
        }
    };
    let token = json
        .get("access_token")
        .and_then(|v| v.as_str())
        .filter(|t| !t.is_empty())
        .ok_or_else(|| {
            Error::engine(format!(
                "azure blob {what}: token response has no access_token"
            ))
        })?
        .to_string();
    // A day at most: a huge value from the server would overflow `SystemTime`
    // (here or where the renewal time is computed); renewing early is harmless.
    let max = Duration::from_secs(86_400);
    let expires_in = match (number("expires_in"), number("expires_on")) {
        (Some(secs), _) => Duration::from_secs(secs).min(max),
        (None, Some(at)) => UNIX_EPOCH
            .checked_add(Duration::from_secs(at))
            .map_or(max, |at| {
                at.duration_since(SystemTime::now()).unwrap_or_default()
            })
            .min(max),
        (None, None) => {
            return Err(Error::engine(format!(
                "azure blob {what}: token response has no expires_in or expires_on"
            )));
        }
    };
    Ok(FreshToken { token, expires_in })
}

async fn token_request(request: reqwest::RequestBuilder, what: &str) -> Result<FreshToken> {
    let response = request
        .send()
        .await
        .map_err(|e| Error::engine(format!("azure blob {what}: {e}")))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| Error::engine(format!("azure blob {what}: {e}")))?;
    if !status.is_success() {
        return Err(Error::engine(format!(
            "azure blob {what} failed: {status} {body}"
        )));
    }
    parse_token_response(&body, what)
}

/// The client-credentials grant at the tenant's token endpoint
/// (`AZURE_AUTHORITY_HOST` overrides the public cloud's authority).
async fn client_secret_token(
    http: &reqwest::Client,
    tenant_id: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<FreshToken> {
    let authority = env_var("AZURE_AUTHORITY_HOST")
        .unwrap_or_else(|| DEFAULT_AUTHORITY_HOST.to_string())
        .trim_end_matches('/')
        .to_string();
    let url = format!("{authority}/{tenant_id}/oauth2/v2.0/token");
    token_request(
        http.post(&url).form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("scope", &format!("{STORAGE_RESOURCE}.default")),
        ]),
        "client secret token",
    )
    .await
}

/// A managed identity token: through the App Service identity endpoint when
/// the process runs there, else the instance metadata service.
async fn managed_identity_token(
    http: &reqwest::Client,
    client_id: Option<&str>,
) -> Result<FreshToken> {
    let mut query = vec![("resource", STORAGE_RESOURCE)];
    if let Some(client_id) = client_id {
        query.push(("client_id", client_id));
    }
    match app_service_identity_endpoint() {
        Some((endpoint, secret)) => {
            query.push(("api-version", "2019-08-01"));
            token_request(
                http.get(&endpoint)
                    .header("X-IDENTITY-HEADER", secret)
                    .query(&query),
                "managed identity token",
            )
            .await
        }
        None => {
            query.push(("api-version", "2018-02-01"));
            token_request(
                http.get(imds_token_url())
                    .header("Metadata", "true")
                    .query(&query),
                "managed identity token",
            )
            .await
        }
    }
}

fn azure_cli_command() -> tokio::process::Command {
    let mut command = tokio::process::Command::new(if cfg!(windows) { "az.cmd" } else { "az" });
    command
        .env("AZURE_CORE_NO_COLOR", "true")
        .stdin(std::process::Stdio::null());
    command
}

async fn azure_cli_available() -> bool {
    matches!(
        tokio::time::timeout(AZURE_CLI_TIMEOUT, azure_cli_command().arg("--version").output()).await,
        Ok(Ok(output)) if output.status.success()
    )
}

/// The signed-in account's token, from `az account get-access-token`.
async fn azure_cli_token() -> Result<FreshToken> {
    let output = tokio::time::timeout(
        AZURE_CLI_TIMEOUT,
        azure_cli_command()
            .args([
                "account",
                "get-access-token",
                "--output",
                "json",
                "--resource",
                STORAGE_RESOURCE,
            ])
            .output(),
    )
    .await
    .map_err(|_| Error::engine("azure blob: the Azure CLI did not answer in time"))?
    .map_err(|e| Error::engine(format!("azure blob: cannot run the Azure CLI: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(Error::engine(format!(
            "azure blob: `az account get-access-token` failed: {}",
            stderr.trim()
        )));
    }
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).map_err(|e| {
        Error::engine(format!(
            "azure blob: unexpected output from the Azure CLI: {e}"
        ))
    })?;
    let token = json
        .get("accessToken")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            Error::engine("azure blob: unexpected output from the Azure CLI: no accessToken")
        })?
        .to_string();
    let expires_at = match json.get("expires_on").and_then(|v| v.as_u64()) {
        Some(at) => UNIX_EPOCH + Duration::from_secs(at),
        None => {
            // The older field: a local time, `2026-11-06 12:00:00.000000`.
            let local = json
                .get("expiresOn")
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok())
                .and_then(|naive| naive.and_local_timezone(chrono::Local).single())
                .ok_or_else(|| {
                    Error::engine("azure blob: unexpected output from the Azure CLI: no expiry")
                })?;
            UNIX_EPOCH + Duration::from_secs(local.timestamp().max(0) as u64)
        }
    };
    Ok(FreshToken {
        token,
        expires_in: expires_at
            .duration_since(SystemTime::now())
            .unwrap_or_default(),
    })
}

/// The storage account an account URL names: the first path segment when the
/// URL has a path (emulator and IP-style endpoints), else the first host label.
fn account_name_of(account_url: &reqwest::Url) -> Option<String> {
    let from_path = account_url
        .path_segments()
        .and_then(|mut segments| segments.find(|segment| !segment.is_empty()));
    let name = match from_path {
        Some(segment) => segment,
        None => account_url.host_str()?.split('.').next()?,
    };
    (!name.is_empty()).then(|| name.to_string())
}

/// The Shared Key string-to-sign of a request without a body or standard
/// headers (the `x-ms-date` and `x-ms-range` headers stand in for `Date` and
/// `Range`).
///
/// `ms_headers` are the request's `x-ms-*` headers sorted by name, `url_path`
/// the percent-encoded path of the request URL, and `query` its unencoded
/// parameters (without a SAS).
fn shared_key_string_to_sign(
    method: &str,
    ms_headers: &[(&str, &str)],
    account_name: &str,
    url_path: &str,
    query: &[(&str, &str)],
) -> String {
    // VERB, then eleven standard headers the source never sends: Content-Encoding,
    // Content-Language, Content-Length, Content-MD5, Content-Type, Date,
    // If-Modified-Since, If-Match, If-None-Match, If-Unmodified-Since, Range.
    let mut out = format!("{method}\n\n\n\n\n\n\n\n\n\n\n\n");
    for (name, value) in ms_headers {
        out.push_str(&format!("{name}:{value}\n"));
    }
    out.push_str(&format!("/{account_name}{url_path}"));
    let mut params = query
        .iter()
        .map(|(name, value)| (name.to_lowercase(), *value))
        .collect::<Vec<_>>();
    params.sort();
    for (name, value) in params {
        out.push_str(&format!("\n{name}:{value}"));
    }
    out
}

/// Base64 of the HMAC-SHA256 of `string_to_sign` under the account key.
fn sign(key: &[u8], string_to_sign: &str) -> Result<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .map_err(|e| Error::engine(format!("Azure Blob account key is not usable: {e}")))?;
    mac.update(string_to_sign.as_bytes());
    Ok(BASE64.encode(mac.finalize().into_bytes()))
}

/// Percent-encode a string, leaving RFC 3986 unreserved characters intact. When
/// `keep_slash` is set, `/` is also left intact (blob names carry slashes as
/// literal path separators).
fn percent_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        let unreserved = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~');
        if unreserved || (keep_slash && b == b'/') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Decode the percent-encoding of a blob name that List Blobs returned with
/// `Encoded="true"` (a name with characters XML cannot carry).
fn percent_decode(s: &str) -> Result<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let decoded = (bytes[i] == b'%')
            .then(|| s.get(i + 1..i + 3))
            .flatten()
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match decoded {
            Some(byte) => {
                out.push(byte);
                i += 3;
            }
            None => {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8(out)
        .map_err(|e| Error::engine(format!("azure blob name {s:?} is not valid UTF-8: {e}")))
}

/// Parse an HTTP-date (RFC 1123, e.g. `Tue, 02 Jun 2026 12:00:00 GMT`), the
/// form of every Blob service timestamp the source reads, into Unix seconds.
fn parse_http_date_secs(s: &str) -> Option<i64> {
    // RFC 1123 uses the obsolete `GMT` zone; chrono's RFC 2822 parser wants a
    // numeric offset.
    let normalized = s.trim().replace(" GMT", " +0000");
    chrono::DateTime::parse_from_rfc2822(&normalized)
        .ok()
        .map(|dt| dt.timestamp())
}

/// An ETag without the quotes the `ETag` response header wraps it in. List
/// Blobs returns it bare, and both must give one fingerprint.
fn unquote_etag(etag: &str) -> String {
    etag.trim().trim_matches('"').to_string()
}

// ---------------------------------------------------------------------------
// get_blob / read
// ---------------------------------------------------------------------------

/// Fetch a single blob's metadata as an [`AzureBlobFile`] (Get Blob
/// Properties). Its relative path equals the full blob name. Errors if the blob
/// doesn't exist.
pub async fn get_blob(client: &ContainerClient, blob_name: &str) -> Result<AzureBlobFile> {
    let response = client
        .send(Method::HEAD, Some(blob_name), &[], None)
        .await?;
    let response = client.check("get blob properties", Some(blob_name), response)?;
    let headers = response.headers();
    let header = |name| headers.get(name).and_then(|v| v.to_str().ok());
    let size = header(CONTENT_LENGTH)
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or_else(|| {
            Error::engine(format!(
                "azure blob get blob properties {}: response has no Content-Length",
                client.describe(Some(blob_name))
            ))
        })?;
    let properties = BlobProperties {
        size,
        modified_secs: header(LAST_MODIFIED).and_then(parse_http_date_secs),
        etag: header(ETAG).map(unquote_etag),
    };
    Ok(AzureBlobFile::new(
        Some(client.clone()),
        client.file_path(blob_name.to_string(), blob_name.to_string()),
        properties,
    ))
}

/// Read blob content directly by name, without fetching metadata first: the
/// first `size` bytes, or the whole blob when `size` is `None`.
pub async fn read(
    client: &ContainerClient,
    blob_name: &str,
    size: Option<usize>,
) -> Result<Vec<u8>> {
    let range = match size {
        Some(0) => return Ok(Vec::new()),
        Some(size) => Some(format!("bytes=0-{}", size - 1)),
        None => None,
    };
    let response = client
        .send(Method::GET, Some(blob_name), &[], range.as_deref())
        .await?;
    // A range on an empty blob is not satisfiable; its first bytes are no bytes.
    if range.is_some() && response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
        return Ok(Vec::new());
    }
    let response = client.check("get blob", Some(blob_name), response)?;
    let bytes = response.bytes().await.map_err(|e| {
        Error::engine(format!(
            "azure blob read body {}: {e}",
            client.describe(Some(blob_name))
        ))
    })?;
    Ok(bytes.to_vec())
}

// ---------------------------------------------------------------------------
// AzureBlobWalker — list a container/prefix into AzureBlobFiles
// ---------------------------------------------------------------------------

/// Options for [`list_blobs`].
#[derive(Default)]
pub struct ListOptions {
    /// Only list blobs in this folder. A prefix names a folder: `"docs"` and
    /// `"docs/"` both list the blobs under `docs/`, and neither lists
    /// `docsa.md`. The folder is stripped from the relative paths of the files.
    pub prefix: String,
    /// Filter by relative path (after prefix stripping). Defaults to match-all.
    pub path_matcher: Option<Arc<dyn FilePathMatcher>>,
    /// Skip blobs larger than this many bytes.
    pub max_file_size: Option<u64>,
}

/// Lists the blobs of a container as [`AzureBlobFile`]s. Build with
/// [`list_blobs`].
pub struct AzureBlobWalker {
    client: ContainerClient,
    /// The key prefix of the listed folder: empty, or ending with `/`.
    folder: String,
    path_matcher: Arc<dyn FilePathMatcher>,
    max_file_size: Option<u64>,
}

/// List blobs in a container. Returns an [`AzureBlobWalker`]; call
/// [`AzureBlobWalker::list`] or [`AzureBlobWalker::items`] to enumerate the
/// matching blobs.
pub fn list_blobs(client: &ContainerClient, options: ListOptions) -> AzureBlobWalker {
    AzureBlobWalker {
        client: client.clone(),
        folder: folder_prefix(options.prefix),
        path_matcher: options
            .path_matcher
            .unwrap_or_else(|| Arc::new(MatchAllFilePathMatcher)),
        max_file_size: options.max_file_size,
    }
}

impl AzureBlobWalker {
    /// List all matching blobs (following the listing's continuation markers),
    /// skipping directory markers and applying the path matcher and the
    /// max-size filter.
    pub async fn list(&self) -> Result<Vec<AzureBlobFile>> {
        let mut out = Vec::new();
        let mut marker = String::new();
        loop {
            let mut query = vec![("restype", "container"), ("comp", "list")];
            if !self.folder.is_empty() {
                query.push(("prefix", &self.folder));
            }
            if !marker.is_empty() {
                query.push(("marker", &marker));
            }
            let response = self.client.send(Method::GET, None, &query, None).await?;
            let response = self.client.check("list blobs", None, response)?;
            let body = response.text().await.map_err(|e| {
                Error::engine(format!(
                    "azure blob list blobs {}: {e}",
                    self.client.describe(None)
                ))
            })?;
            let page: ListBlobsPage = quick_xml::de::from_str(&body).map_err(|e| {
                Error::engine(format!(
                    "azure blob list blobs {}: unreadable response: {e}",
                    self.client.describe(None)
                ))
            })?;

            for blob in page.blobs.blob {
                let blob_name = match blob.name.encoded {
                    Some(true) => percent_decode(&blob.name.value)?,
                    _ => blob.name.value,
                };
                let Some(relative) = relative_key(&blob_name, &self.folder) else {
                    continue;
                };
                if !self.path_matcher.is_file_included(&PathBuf::from(relative)) {
                    continue;
                }
                let size = blob.properties.content_length;
                if self.max_file_size.is_some_and(|max| size > max) {
                    continue;
                }
                let relative = relative.to_string();
                out.push(AzureBlobFile::new(
                    Some(self.client.clone()),
                    self.client.file_path(relative, blob_name),
                    BlobProperties {
                        size,
                        modified_secs: blob
                            .properties
                            .last_modified
                            .as_deref()
                            .and_then(parse_http_date_secs),
                        etag: blob.properties.etag.as_deref().map(unquote_etag),
                    },
                ));
            }

            match page.next_marker {
                Some(next) if !next.is_empty() => marker = next,
                _ => break,
            }
        }
        Ok(out)
    }

    /// Like [`list`](Self::list) but returns `(key, file)` pairs where `key` is
    /// the blob name relative to the prefix, exactly as stored — ready to hand
    /// to `Ctx::mount_each`.
    pub async fn items(&self) -> Result<Vec<(String, AzureBlobFile)>> {
        Ok(self
            .list()
            .await?
            .into_iter()
            .map(|f| (f.key(), f))
            .collect())
    }
}

/// The key prefix of the blobs in the folder `prefix`: `"docs"` and `"docs/"`
/// both select the blobs under `docs/`, and neither selects `docsa.md`.
fn folder_prefix(prefix: String) -> String {
    if prefix.is_empty() || prefix.ends_with('/') {
        prefix
    } else {
        prefix + "/"
    }
}

/// The item key of `blob_name` in `folder` (from [`folder_prefix`]): the rest of
/// its name, exactly as stored. `None` for a blob outside the folder and for a
/// folder marker (a name ending with `/`).
fn relative_key<'a>(blob_name: &'a str, folder: &str) -> Option<&'a str> {
    if blob_name.ends_with('/') {
        return None;
    }
    blob_name.strip_prefix(folder)
}

/// One page of a List Blobs response (`<EnumerationResults>`).
#[derive(Deserialize)]
struct ListBlobsPage {
    #[serde(rename = "Blobs", default)]
    blobs: ListedBlobs,
    #[serde(rename = "NextMarker", default)]
    next_marker: Option<String>,
}

#[derive(Deserialize, Default)]
struct ListedBlobs {
    #[serde(rename = "Blob", default)]
    blob: Vec<ListedBlob>,
}

#[derive(Deserialize)]
struct ListedBlob {
    #[serde(rename = "Name")]
    name: ListedBlobName,
    #[serde(rename = "Properties")]
    properties: ListedBlobProperties,
}

#[derive(Deserialize)]
struct ListedBlobName {
    /// Set when the service percent-encoded the name.
    #[serde(rename = "@Encoded", default)]
    encoded: Option<bool>,
    #[serde(rename = "$text", default)]
    value: String,
}

#[derive(Deserialize)]
struct ListedBlobProperties {
    #[serde(rename = "Last-Modified", default)]
    last_modified: Option<String>,
    #[serde(rename = "Etag", default)]
    etag: Option<String>,
    #[serde(rename = "Content-Length")]
    content_length: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folder_prefix_names_a_folder() {
        assert_eq!(folder_prefix(String::new()), "");
        assert_eq!(folder_prefix("docs".to_string()), "docs/");
        assert_eq!(folder_prefix("docs/".to_string()), "docs/");
    }

    #[test]
    fn relative_key_keeps_raw_names_and_skips_markers() {
        assert_eq!(relative_key("a/b.md", ""), Some("a/b.md"));
        assert_eq!(relative_key("docs/a/b.md", "docs/"), Some("a/b.md"));
        // Keys are not normalized.
        assert_eq!(relative_key("docs//b.md", "docs/"), Some("/b.md"));
        // A folder marker, and a blob outside the folder.
        assert_eq!(relative_key("docs/", "docs/"), None);
        assert_eq!(relative_key("docsa.md", "docs/"), None);
    }

    #[test]
    fn account_name_comes_from_host_or_path() {
        let name = |url: &str| account_name_of(&reqwest::Url::parse(url).unwrap());
        assert_eq!(
            name("https://myaccount.blob.core.windows.net").as_deref(),
            Some("myaccount")
        );
        assert_eq!(
            name("http://127.0.0.1:10000/devstoreaccount1").as_deref(),
            Some("devstoreaccount1")
        );
    }

    #[test]
    fn client_rejects_bad_configuration() {
        let key = || Credential::AccountKey(BASE64.encode("key"));
        assert!(ContainerClient::new("not a url", "docs", key()).is_err());
        assert!(ContainerClient::new("https://a.blob.core.windows.net", "", key()).is_err());
        assert!(
            ContainerClient::new(
                "https://a.blob.core.windows.net",
                "docs",
                Credential::AccountKey("not base64!".to_string()),
            )
            .is_err()
        );
    }

    /// The example request of the Shared Key documentation's "Blob, Queue, and
    /// File services" section shape: a container listing with an emulator-style
    /// path, checked against the string the specification prescribes.
    #[test]
    fn shared_key_string_to_sign_layout() {
        let string_to_sign = shared_key_string_to_sign(
            "GET",
            &[
                ("x-ms-date", "Fri, 26 Jun 2015 23:39:12 GMT"),
                ("x-ms-version", "2015-02-21"),
            ],
            "myaccount",
            "/mycontainer",
            &[
                ("restype", "container"),
                ("comp", "list"),
                ("prefix", "docs/"),
            ],
        );
        assert_eq!(
            string_to_sign,
            "GET\n\n\n\n\n\n\n\n\n\n\n\n\
             x-ms-date:Fri, 26 Jun 2015 23:39:12 GMT\n\
             x-ms-version:2015-02-21\n\
             /myaccount/mycontainer\n\
             comp:list\n\
             prefix:docs/\n\
             restype:container"
        );
    }

    #[test]
    fn list_page_parses_names_properties_and_marker() {
        let body = r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://a.blob.core.windows.net/" ContainerName="docs">
  <Prefix>docs/</Prefix>
  <Blobs>
    <Blob>
      <Name>docs/a &amp; b.md</Name>
      <Properties>
        <Creation-Time>Fri, 02 Jan 2026 03:04:05 GMT</Creation-Time>
        <Last-Modified>Fri, 02 Jan 2026 03:04:05 GMT</Last-Modified>
        <Etag>0x8DE49A1B2C3D4E5</Etag>
        <Content-Length>42</Content-Length>
        <Content-Type>text/markdown</Content-Type>
        <BlobType>BlockBlob</BlobType>
      </Properties>
      <OrMetadata />
    </Blob>
    <Blob>
      <Name Encoded="true">docs/b%EF%BF%BF.md</Name>
      <Properties><Content-Length>0</Content-Length></Properties>
    </Blob>
  </Blobs>
  <NextMarker>next-page</NextMarker>
</EnumerationResults>"#;
        let page: ListBlobsPage = quick_xml::de::from_str(body).unwrap();
        assert_eq!(page.next_marker.as_deref(), Some("next-page"));
        let [first, second] = page.blobs.blob.as_slice() else {
            panic!("expected two blobs");
        };
        assert_eq!(first.name.value, "docs/a & b.md");
        assert_eq!(first.name.encoded, None);
        assert_eq!(first.properties.content_length, 42);
        assert_eq!(first.properties.etag.as_deref(), Some("0x8DE49A1B2C3D4E5"));
        assert_eq!(
            first
                .properties
                .last_modified
                .as_deref()
                .and_then(parse_http_date_secs),
            Some(1_767_323_045)
        );
        assert_eq!(second.name.encoded, Some(true));
        assert_eq!(
            percent_decode(&second.name.value).unwrap(),
            "docs/b\u{ffff}.md"
        );

        let empty: ListBlobsPage = quick_xml::de::from_str(
            r#"<EnumerationResults ContainerName="docs"><Blobs /><NextMarker /></EnumerationResults>"#,
        )
        .unwrap();
        assert!(empty.blobs.blob.is_empty());
        assert!(empty.next_marker.is_none_or(|marker| marker.is_empty()));
    }

    #[test]
    fn etag_fingerprint_is_the_same_quoted_or_bare() {
        assert_eq!(unquote_etag("\"0x8DE49A1\""), unquote_etag("0x8DE49A1"));
    }

    #[test]
    fn file_path_accessors_and_memo_key() {
        let path = |container: &str| AzureBlobFilePath {
            account_name: "acct".to_string(),
            container_name: container.to_string(),
            relative_path: "a/b.md".to_string(),
            blob_name: "docs/a/b.md".to_string(),
        };
        let fp = path("c1");
        assert_eq!(fp.path(), "a/b.md");
        assert_eq!(fp.resolve(), "docs/a/b.md");
        assert_eq!(fp.blob_name(), "docs/a/b.md");
        // The memo key holds the account, the container and the relative path,
        // not the full blob name.
        assert_eq!(
            serde_json::to_value(fp.memo_key()).unwrap(),
            serde_json::json!(["acct", "c1", "a/b.md"])
        );
        assert_ne!(
            serde_json::to_value(fp.memo_key()).unwrap(),
            serde_json::to_value(path("c2").memo_key()).unwrap()
        );
    }

    #[tokio::test]
    async fn file_without_client_has_metadata_but_cannot_be_read() {
        let file = AzureBlobFile::new(
            None,
            AzureBlobFilePath {
                account_name: "acct".to_string(),
                container_name: "docs".to_string(),
                relative_path: "sub//doc.md".to_string(),
                blob_name: "prefix/sub//doc.md".to_string(),
            },
            BlobProperties {
                size: 42,
                modified_secs: Some(123),
                etag: Some("0x1".to_string()),
            },
        );
        // The mount key is the raw relative name, not a normalized path.
        assert_eq!(FileSourceItem::key(&file), "sub//doc.md");
        assert_eq!(FileLike::metadata(&file).await.unwrap().size, 42);
        assert!(
            FileLike::read(&file)
                .await
                .unwrap_err()
                .to_string()
                .contains("not attached")
        );
    }
}
