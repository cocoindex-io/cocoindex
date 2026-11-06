//! Azure Blob Storage source connector.
//!
//! Read-only listing and reading of the blobs in a container, mirroring
//! Python's `cocoindex.connectors.azure_blob`:
//!
//! - [`ContainerClient`] — a clone-cheap handle to one container. It speaks the
//!   Blob service REST API directly (there is no official Azure Rust SDK in the
//!   tree) and authenticates with a storage account key (Shared Key), a SAS
//!   token or a Microsoft Entra ID bearer token: see [`Credential`].
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
    pub async fn read(&self) -> Result<Vec<u8>> {
        <Self as FileLike>::read(self).await
    }

    /// Read the first `size` bytes of the blob.
    pub async fn read_size(&self, size: usize) -> Result<Vec<u8>> {
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
}

enum Auth {
    SharedKey { key: Vec<u8> },
    Sas { query: String },
    Bearer { header: String },
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
