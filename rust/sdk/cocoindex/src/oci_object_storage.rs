//! Oracle Cloud Infrastructure (OCI) Object Storage source connector.
//!
//! Read-only listing and reading of OCI Object Storage objects, mirroring
//! Python's `cocoindex.connectors.oci_object_storage`:
//!
//! - [`OciClient`] — a clone-cheap connection handle. [`OciClient::connect`]
//!   reads an `~/.oci/config` profile (the same file the OCI CLI / Python SDK
//!   use) and signs every request with OCI's RSA-SHA256 HTTP Signature scheme.
//! - [`list_objects`] returns an [`OciWalker`]; [`OciWalker::list`] /
//!   [`OciWalker::items`] enumerate matching objects as [`OciFile`]s. Use each
//!   [`OciFile::key`] with `Ctx::mount_each` so per-file memoization handles
//!   edits and target reconciliation removes derived rows for deleted objects.
//! - [`OciClient::get_object`] fetches a single object's metadata (via a `HEAD`);
//!   reads go through [`OciFile::read`] / [`OciFile::read_text`] or the
//!   compatibility methods on [`OciClient`].
//!
//! There is no official Oracle SDK for Rust, so this connector talks to the
//! Object Storage REST API directly (`GET`/`HEAD` under
//! `https://objectstorage.{region}.oraclecloud.com/n/{namespace}/b/{bucket}/o`).
//! Only `GET`/`HEAD` are used, so the signed header set is the minimal
//! `(request-target) host date` (the `x-content-sha256` / `content-length`
//! headers OCI requires are `PUT`/`POST`-only).
//!
//! Like the Amazon S3 and Google Drive sources, [`OciFile`] serializes only
//! stable metadata for memo keys. The clone-cheap client and read cache are
//! skipped by serde, while the public type still implements the shared async
//! [`crate::file::FileLike`] trait.
//!
//! Not yet supported (parity follow-ups): live bucket watching via OCI
//! Events/Streaming, and pass-phrase-encrypted private keys.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use cocoindex_utils::fingerprint::Fingerprint;
use rsa::RsaPrivateKey;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::signature::{SignatureEncoding, Signer};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::error::{Error, Result};
use crate::file::{
    FileContentCache, FileLike, FileMetadata, FilePath, FilePathMatcher, FileSourceItem,
    MatchAllFilePathMatcher, decode_bytes,
};
use crate::live_component::{LiveMapFeed, LiveMapSubscriber, LiveMapView};

/// Object metadata fields requested from the ListObjects API.
const LIST_FIELDS: &str = "name,size,md5,timeModified,etag";

// ---------------------------------------------------------------------------
// OciConfig — parsed ~/.oci/config profile
// ---------------------------------------------------------------------------

/// Credentials and endpoint for an OCI API-key principal, as found in an
/// `~/.oci/config` profile (the same file the OCI CLI and Python SDK read).
#[derive(Clone, Debug)]
pub struct OciConfig {
    /// Tenancy OCID.
    pub tenancy: String,
    /// User OCID.
    pub user: String,
    /// API-key fingerprint.
    pub fingerprint: String,
    /// Path to the PEM private key file.
    pub key_file: String,
    /// Region identifier, e.g. `us-ashburn-1`.
    pub region: String,
    /// Optional key pass-phrase. Encrypted keys are not yet supported; a set
    /// pass-phrase causes [`OciClient::from_config`] to error.
    pub pass_phrase: Option<String>,
}

impl OciConfig {
    /// Load a profile from an OCI config file (INI format). `profile` selects the
    /// `[PROFILE]` section (e.g. `DEFAULT`).
    pub fn from_file(path: impl AsRef<std::path::Path>, profile: &str) -> Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path).map_err(Error::Io)?;
        let mut section = parse_ini_section(&raw, profile).ok_or_else(|| {
            Error::engine(format!(
                "OCI config {}: profile [{profile}] not found",
                path.display()
            ))
        })?;
        let mut take = |key: &str| -> Result<String> {
            section.remove(key).ok_or_else(|| {
                Error::engine(format!(
                    "OCI config profile [{profile}] missing required key {key:?}"
                ))
            })
        };
        Ok(Self {
            tenancy: take("tenancy")?,
            user: take("user")?,
            fingerprint: take("fingerprint")?,
            key_file: expand_home(&take("key_file")?),
            region: take("region")?,
            pass_phrase: section
                .remove("pass_phrase")
                .or(section.remove("passphrase")),
        })
    }
}

/// Parse a single `[profile]` section of a simple INI file into a key→value map.
/// Comments (`#` / `;`) and blank lines are skipped; values are trimmed.
fn parse_ini_section(
    raw: &str,
    profile: &str,
) -> Option<std::collections::HashMap<String, String>> {
    let mut current: Option<String> = None;
    let mut map = std::collections::HashMap::new();
    let mut found = false;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(rest) = line.strip_prefix('[') {
            let name = rest.strip_suffix(']').unwrap_or(rest).trim();
            current = Some(name.to_string());
            if name == profile {
                found = true;
            }
            continue;
        }
        if current.as_deref() == Some(profile)
            && let Some((k, v)) = line.split_once('=')
        {
            map.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    found.then_some(map)
}

/// Expand a leading `~` in a path using `$HOME`.
fn expand_home(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/")
        && let Ok(home) = std::env::var("HOME")
    {
        return format!("{home}/{rest}");
    }
    path.to_string()
}

// ---------------------------------------------------------------------------
// OciFilePath / OciFile — source items (Serialize so `mount_each` + memo work)
// ---------------------------------------------------------------------------

/// Path of an OCI object: the namespace, bucket, full object name, and the path
/// relative to the walker prefix. Mirrors Python's `OCIFilePath`; its memo key
/// includes namespace + bucket so the same relative path in two buckets (or two
/// namespaces) stays distinct.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OciFilePath {
    namespace: String,
    bucket: String,
    /// Path relative to the walker prefix (forward slashes), or the full object
    /// name if no prefix was used. This is the user-facing display path /
    /// `mount_each` key.
    relative_path: String,
    /// Full object name (what [`resolve`](Self::resolve) returns).
    object_name: String,
}

impl OciFilePath {
    /// The OCI Object Storage namespace.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Path relative to the walker prefix (forward slashes).
    pub fn path(&self) -> &str {
        &self.relative_path
    }

    /// The full object name.
    pub fn resolve(&self) -> &str {
        &self.object_name
    }

    /// Memo key: `(namespace, bucket, relative_path)`. Stable across buckets and
    /// decoupled from the live connection (matches Python's `__coco_memo_key__`).
    pub fn memo_key(&self) -> impl Serialize + '_ {
        (&self.namespace, &self.bucket, &self.relative_path)
    }
}

/// A file discovered in or fetched from an OCI bucket.
///
/// Files returned by [`OciWalker`] carry a clone-cheap client handle so they can
/// be read through the shared async [`FileLike`] API.
#[derive(Clone, Serialize, Deserialize)]
pub struct OciFile {
    file_path: OciFilePath,
    /// Object size in bytes.
    pub size: u64,
    /// Last-modified time as Unix seconds (`None` if the server omitted it).
    pub modified_secs: Option<i64>,
    /// Base64 MD5 of the content, if returned (preferred content fingerprint).
    pub md5: Option<String>,
    /// Entity tag, if returned (fingerprint fallback when `md5` is absent, e.g.
    /// for multipart uploads).
    pub etag: Option<String>,
    #[serde(skip)]
    client: Option<OciClient>,
    #[serde(skip, default = "default_file_cache")]
    cache: Arc<FileContentCache>,
}

impl OciFile {
    fn new(
        client: Option<OciClient>,
        file_path: OciFilePath,
        size: u64,
        modified_secs: Option<i64>,
        md5: Option<String>,
        etag: Option<String>,
    ) -> Self {
        let metadata = FileMetadata {
            size,
            modified: modified_time(modified_secs),
            content_fingerprint: content_fingerprint(md5.as_deref(), etag.as_deref()),
        };
        Self {
            file_path,
            size,
            modified_secs,
            md5,
            etag,
            client,
            cache: Arc::new(FileContentCache::with_metadata(metadata)),
        }
    }

    /// Attach a client so the file can be read.
    pub fn with_client(mut self, client: OciClient) -> Self {
        self.client = Some(client);
        self
    }

    /// Stable key for `Ctx::mount_each`: the object's path relative to the walker
    /// prefix. Unique within a namespace+bucket+prefix.
    pub fn key(&self) -> String {
        self.file_path.relative_path.clone()
    }

    /// The object's [`OciFilePath`].
    pub fn file_path(&self) -> &OciFilePath {
        &self.file_path
    }

    pub async fn read(&self) -> Result<Vec<u8>> {
        <Self as FileLike>::read(self).await
    }

    pub async fn read_size(&self, size: usize) -> Result<Vec<u8>> {
        <Self as FileLike>::read_size(self, size).await
    }

    pub async fn read_text(&self) -> Result<String> {
        <Self as FileLike>::read_text(self).await
    }
}

impl std::fmt::Debug for OciFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OciFile")
            .field("file_path", &self.file_path)
            .field("size", &self.size)
            .field("modified_secs", &self.modified_secs)
            .field("md5", &self.md5)
            .field("etag", &self.etag)
            .finish()
    }
}

impl PartialEq for OciFile {
    fn eq(&self, other: &Self) -> bool {
        self.file_path == other.file_path
            && self.size == other.size
            && self.modified_secs == other.modified_secs
            && self.md5 == other.md5
            && self.etag == other.etag
    }
}

impl Eq for OciFile {}

#[async_trait]
impl FileLike for OciFile {
    fn file_path(&self) -> FilePath {
        FilePath::with_base_dir(
            format!(
                "oci://{}/{}",
                self.file_path.namespace, self.file_path.bucket
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
            content_fingerprint: content_fingerprint(self.md5.as_deref(), self.etag.as_deref()),
        })
    }

    async fn read_impl(&self, size: Option<usize>) -> Result<Vec<u8>> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::engine("OCI file is not attached to an OciClient"))?;
        match size {
            Some(0) => Ok(Vec::new()),
            Some(size) => client.read_range(self, size as u64).await,
            None => {
                client
                    .read_impl(
                        &self.file_path.namespace,
                        &self.file_path.bucket,
                        &self.file_path.object_name,
                        None,
                    )
                    .await
            }
        }
    }
}

impl FileSourceItem for OciFile {}

fn default_file_cache() -> Arc<FileContentCache> {
    Arc::new(FileContentCache::new())
}

fn modified_time(secs: Option<i64>) -> SystemTime {
    match secs {
        Some(secs) if secs >= 0 => UNIX_EPOCH + Duration::from_secs(secs as u64),
        _ => UNIX_EPOCH,
    }
}

/// Prefer the MD5 (present for single-part uploads), falling back to the ETag.
fn content_fingerprint(md5: Option<&str>, etag: Option<&str>) -> Option<Fingerprint> {
    md5.or(etag).and_then(|s| Fingerprint::from(&s).ok())
}

// ---------------------------------------------------------------------------
// OciClient — connection handle (Clone-cheap)
// ---------------------------------------------------------------------------

struct OciClientInner {
    http: reqwest::Client,
    /// `objectstorage.{region}.oraclecloud.com`
    host: String,
    /// `https://{host}`
    base_url: String,
    /// `{tenancy}/{user}/{fingerprint}`
    key_id: String,
    signing_key: SigningKey<Sha256>,
    state_id: String,
}

/// An OCI Object Storage connection. Clone-cheap (the underlying client and
/// signing key are shared).
#[derive(Clone)]
pub struct OciClient {
    inner: Arc<OciClientInner>,
}

impl OciClient {
    /// Build a client from an `~/.oci/config` profile. Honors `OCI_CONFIG_FILE`
    /// (default `~/.oci/config`) and `OCI_PROFILE` / `OCI_CLI_PROFILE` (default
    /// `DEFAULT`).
    pub async fn connect() -> Result<Self> {
        let config_file = std::env::var("OCI_CONFIG_FILE")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| expand_home("~/.oci/config"));
        let profile = std::env::var("OCI_PROFILE")
            .or_else(|_| std::env::var("OCI_CLI_PROFILE"))
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "DEFAULT".to_string());
        let config = OciConfig::from_file(&config_file, &profile)?;
        Self::from_config(config)
    }

    /// Build a client from an explicit [`OciConfig`].
    pub fn from_config(config: OciConfig) -> Result<Self> {
        if config.pass_phrase.is_some() {
            return Err(Error::engine(
                "OCI pass-phrase-encrypted private keys are not yet supported; \
                 use an unencrypted API key",
            ));
        }
        let pem = std::fs::read_to_string(&config.key_file)
            .map_err(|e| Error::engine(format!("read OCI key file {}: {e}", config.key_file)))?;
        let key = load_private_key(&pem)?;
        let signing_key = SigningKey::<Sha256>::new(key);
        let host = format!("objectstorage.{}.oraclecloud.com", config.region);
        let base_url = format!("https://{host}");
        let key_id = format!("{}/{}/{}", config.tenancy, config.user, config.fingerprint);
        let state_id = format!("oci-objectstorage:{}", config.region);
        Ok(Self {
            inner: Arc::new(OciClientInner {
                http: reqwest::Client::new(),
                host,
                base_url,
                key_id,
                signing_key,
                state_id,
            }),
        })
    }

    /// Override the Object Storage base URL (default the public OCI endpoint).
    /// Mainly for pointing the client at a mock server in tests; the signed
    /// `host` header is left unchanged.
    pub fn with_base_url(self, base_url: impl Into<String>) -> Self {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        let inner = self.inner.as_ref();
        Self {
            inner: Arc::new(OciClientInner {
                http: inner.http.clone(),
                host: inner.host.clone(),
                base_url,
                key_id: inner.key_id.clone(),
                signing_key: inner.signing_key.clone(),
                state_id: inner.state_id.clone(),
            }),
        }
    }

    /// Stable identity (for use as a `ContextKey` state id / memo dependency).
    /// Derived from the region; never the credentials.
    pub fn state_id(&self) -> &str {
        &self.inner.state_id
    }

    /// `HEAD` an object, returning its metadata, or `None` if it no longer
    /// exists (`404`). Used by the live view to confirm an event's object (the
    /// re-read is authoritative over the event type).
    async fn head_object_if_exists(
        &self,
        namespace: &str,
        bucket: &str,
        object_name: &str,
    ) -> Result<Option<ObjectHead>> {
        let path = object_path(namespace, bucket, object_name);
        let date = http_date_now();
        let authorization = build_authorization(
            &self.inner.signing_key,
            &self.inner.key_id,
            "HEAD",
            &path,
            &self.inner.host,
            &date,
        )?;
        let url = format!("{}{}", self.inner.base_url, path);
        let resp = self
            .inner
            .http
            .request(reqwest::Method::HEAD, &url)
            .header(reqwest::header::DATE, date)
            .header(reqwest::header::AUTHORIZATION, authorization)
            .send()
            .await
            .map_err(|e| Error::engine(format!("oci head {path}: {e}")))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(Error::engine(format!(
                "oci head {path} failed: {}",
                resp.status()
            )));
        }
        let headers = resp.headers();
        Ok(Some(ObjectHead {
            size: header_str(headers, "content-length")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0),
            modified_secs: header_str(headers, "last-modified").and_then(parse_http_date_secs),
            md5: header_str(headers, "opc-content-md5")
                .or_else(|| header_str(headers, "content-md5"))
                .map(str::to_string),
            etag: header_str(headers, "etag").map(str::to_string),
        }))
    }

    /// Fetch a single object's metadata as an [`OciFile`] (via a `HEAD`). Its
    /// relative path equals the full object name. Errors if the object is absent.
    pub async fn get_object(
        &self,
        namespace: &str,
        bucket: &str,
        object_name: &str,
    ) -> Result<OciFile> {
        let path = object_path(namespace, bucket, object_name);
        let resp = self
            .signed_request(reqwest::Method::HEAD, &path, None)
            .await?;
        let headers = resp.headers();
        let size = header_str(headers, "content-length")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let modified_secs = header_str(headers, "last-modified").and_then(parse_http_date_secs);
        let etag = header_str(headers, "etag").map(str::to_string);
        let md5 = header_str(headers, "opc-content-md5")
            .or_else(|| header_str(headers, "content-md5"))
            .map(str::to_string);
        Ok(OciFile::new(
            Some(self.clone()),
            OciFilePath {
                namespace: namespace.to_string(),
                bucket: bucket.to_string(),
                relative_path: object_name.to_string(),
                object_name: object_name.to_string(),
            },
            size,
            modified_secs,
            md5,
            etag,
        ))
    }

    /// [`get_object`](Self::get_object) addressed by an
    /// `oci://namespace/bucket/object` URI.
    pub async fn get_object_uri(&self, uri: &str) -> Result<OciFile> {
        let (namespace, bucket, object_name) = parse_oci_uri(uri)?;
        self.get_object(&namespace, &bucket, &object_name).await
    }

    /// Read an object's full content.
    pub async fn read(&self, file: &OciFile) -> Result<Vec<u8>> {
        self.read_impl(
            &file.file_path.namespace,
            &file.file_path.bucket,
            &file.file_path.object_name,
            None,
        )
        .await
    }

    /// Read the first `len` bytes of an object (a ranged `GET`).
    pub async fn read_range(&self, file: &OciFile, len: u64) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let range = format!("bytes=0-{}", len - 1);
        self.read_impl(
            &file.file_path.namespace,
            &file.file_path.bucket,
            &file.file_path.object_name,
            Some(range),
        )
        .await
    }

    /// Read an object's content and decode it as UTF-8 (BOM-aware, lossy on
    /// invalid UTF-8).
    pub async fn read_text(&self, file: &OciFile) -> Result<String> {
        Ok(decode_bytes(&self.read(file).await?))
    }

    async fn read_impl(
        &self,
        namespace: &str,
        bucket: &str,
        object_name: &str,
        range: Option<String>,
    ) -> Result<Vec<u8>> {
        let path = object_path(namespace, bucket, object_name);
        let extra = range
            .map(|r| vec![(reqwest::header::RANGE, r)])
            .unwrap_or_default();
        let resp = self
            .signed_request_with_headers(reqwest::Method::GET, &path, extra)
            .await?;
        let bytes = resp.bytes().await.map_err(|e| {
            Error::engine(format!(
                "oci read body {namespace}/{bucket}/{object_name}: {e}"
            ))
        })?;
        Ok(bytes.to_vec())
    }

    /// Send a signed request with no extra headers.
    async fn signed_request(
        &self,
        method: reqwest::Method,
        path_and_query: &str,
        _body: Option<()>,
    ) -> Result<reqwest::Response> {
        self.signed_request_with_headers(method, path_and_query, Vec::new())
            .await
    }

    /// Build, sign, and send a `GET`/`HEAD` request. Signs the minimal OCI header
    /// set `(request-target) host date`; `extra` headers (e.g. `Range`) are sent
    /// unsigned, which OCI permits for reads.
    async fn signed_request_with_headers(
        &self,
        method: reqwest::Method,
        path_and_query: &str,
        extra: Vec<(reqwest::header::HeaderName, String)>,
    ) -> Result<reqwest::Response> {
        let date = http_date_now();
        let authorization = build_authorization(
            &self.inner.signing_key,
            &self.inner.key_id,
            method.as_str(),
            path_and_query,
            &self.inner.host,
            &date,
        )?;
        let url = format!("{}{}", self.inner.base_url, path_and_query);
        let mut req = self
            .inner
            .http
            .request(method, &url)
            .header(reqwest::header::DATE, date)
            .header(reqwest::header::AUTHORIZATION, authorization);
        for (name, value) in extra {
            req = req.header(name, value);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| Error::engine(format!("oci request {path_and_query}: {e}")))?;
        resp.error_for_status()
            .map_err(|e| Error::engine(format!("oci request {path_and_query} failed: {e}")))
    }
}

/// Load an RSA private key from PEM, accepting either PKCS#8
/// (`BEGIN PRIVATE KEY`) or PKCS#1 (`BEGIN RSA PRIVATE KEY`) encodings.
fn load_private_key(pem: &str) -> Result<RsaPrivateKey> {
    RsaPrivateKey::from_pkcs8_pem(pem)
        .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
        .map_err(|e| {
            Error::engine(format!(
                "parse OCI private key (expected an unencrypted PKCS#8 or PKCS#1 PEM): {e}"
            ))
        })
}

/// The canonical signing string for an OCI `GET`/`HEAD`: the `(request-target)`,
/// `host`, and `date` pseudo/real headers, one `name: value` per line.
fn signing_string(method: &str, path_and_query: &str, host: &str, date: &str) -> String {
    format!(
        "(request-target): {} {}\nhost: {}\ndate: {}",
        method.to_ascii_lowercase(),
        path_and_query,
        host,
        date,
    )
}

/// Build the OCI `Authorization: Signature ...` header value.
fn build_authorization(
    signing_key: &SigningKey<Sha256>,
    key_id: &str,
    method: &str,
    path_and_query: &str,
    host: &str,
    date: &str,
) -> Result<String> {
    let to_sign = signing_string(method, path_and_query, host, date);
    let signature = signing_key
        .try_sign(to_sign.as_bytes())
        .map_err(|e| Error::engine(format!("sign OCI request: {e}")))?;
    let sig_b64 = STANDARD.encode(signature.to_bytes());
    Ok(format!(
        "Signature version=\"1\",keyId=\"{key_id}\",algorithm=\"rsa-sha256\",\
         headers=\"(request-target) host date\",signature=\"{sig_b64}\""
    ))
}

fn header_str<'a>(headers: &'a reqwest::header::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// Parse an HTTP-date (RFC 1123, e.g. `Tue, 02 Jun 2026 12:00:00 GMT`) into Unix
/// seconds. Returns `None` if unparseable.
fn parse_http_date_secs(s: &str) -> Option<i64> {
    // RFC 1123 uses the obsolete `GMT` zone; chrono's RFC 2822 parser wants a
    // numeric offset.
    let normalized = s.trim().replace(" GMT", " +0000");
    chrono::DateTime::parse_from_rfc2822(&normalized)
        .ok()
        .map(|dt| dt.timestamp())
}

/// Parse an ISO-8601 / RFC 3339 timestamp (the ListObjects `timeModified`) into
/// Unix seconds.
fn parse_rfc3339_secs(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp())
}

/// Current time formatted as an HTTP-date (RFC 1123, GMT) for the `date` header.
fn http_date_now() -> String {
    chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string()
}

/// Build the request path for an object: `/n/{ns}/b/{bucket}/o/{encoded object}`.
/// The object name is percent-encoded but keeps `/` as a literal path separator.
fn object_path(namespace: &str, bucket: &str, object_name: &str) -> String {
    format!(
        "/n/{}/b/{}/o/{}",
        percent_encode(namespace, false),
        percent_encode(bucket, false),
        percent_encode(object_name, true),
    )
}

/// Percent-encode a string, leaving RFC 3986 unreserved characters intact. When
/// `keep_slash` is set, `/` is also left intact (object names carry slashes as
/// literal path separators).
fn percent_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        let unreserved = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~');
        if unreserved || (keep_slash && b == b'/') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push_str(&format!("{b:02X}"));
        }
    }
    out
}

/// Build a query string from ordered `(name, value)` pairs, percent-encoding
/// values. Pairs with an empty value are skipped.
fn build_query(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .filter(|(_, v)| !v.is_empty())
        .map(|(k, v)| format!("{k}={}", percent_encode(v, false)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Parse an `oci://namespace/bucket/object` URI into *(namespace, bucket, object)*.
fn parse_oci_uri(uri: &str) -> Result<(String, String, String)> {
    let invalid = || {
        Error::engine(format!(
            "invalid OCI URI {uri:?}: expected 'oci://namespace/bucket/object'"
        ))
    };
    let rest = uri.strip_prefix("oci://").ok_or_else(invalid)?;
    let mut parts = rest.splitn(3, '/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(ns), Some(bucket), Some(object))
            if !ns.is_empty() && !bucket.is_empty() && !object.is_empty() =>
        {
            Ok((ns.to_string(), bucket.to_string(), object.to_string()))
        }
        _ => Err(invalid()),
    }
}

// ---------------------------------------------------------------------------
// OciWalker — list a namespace/bucket/prefix into OciFiles
// ---------------------------------------------------------------------------

/// Options for [`list_objects`].
#[derive(Default)]
pub struct ListOptions {
    /// Only list objects whose name starts with this prefix.
    pub prefix: String,
    /// Filter by relative path (after prefix stripping). Defaults to match-all.
    pub path_matcher: Option<Arc<dyn FilePathMatcher>>,
    /// Skip objects larger than this many bytes.
    pub max_file_size: Option<u64>,
}

/// Lists objects in an OCI bucket as [`OciFile`]s. Build with [`list_objects`].
pub struct OciWalker {
    client: OciClient,
    namespace: String,
    bucket: String,
    prefix: String,
    path_matcher: Arc<dyn FilePathMatcher>,
    max_file_size: Option<u64>,
}

/// List objects in an OCI bucket. Returns an [`OciWalker`]; call
/// [`OciWalker::list`] or [`OciWalker::items`] to enumerate matching objects.
pub fn list_objects(
    client: &OciClient,
    namespace: impl Into<String>,
    bucket: impl Into<String>,
    options: ListOptions,
) -> OciWalker {
    OciWalker {
        client: client.clone(),
        namespace: namespace.into(),
        bucket: bucket.into(),
        prefix: options.prefix,
        path_matcher: options
            .path_matcher
            .unwrap_or_else(|| Arc::new(MatchAllFilePathMatcher)),
        max_file_size: options.max_file_size,
    }
}

/// One entry of the ListObjects response.
#[derive(Deserialize)]
struct ObjectSummary {
    name: String,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    md5: Option<String>,
    #[serde(default, rename = "timeModified")]
    time_modified: Option<String>,
    #[serde(default)]
    etag: Option<String>,
}

/// The ListObjects response envelope.
#[derive(Deserialize)]
struct ListObjectsResponse {
    #[serde(default)]
    objects: Vec<ObjectSummary>,
    #[serde(default, rename = "nextStartWith")]
    next_start_with: Option<String>,
}

impl OciWalker {
    /// List all matching objects (paginating through the bucket), skipping
    /// directory markers, applying the prefix, path matcher, and max-size filter.
    pub async fn list(&self) -> Result<Vec<OciFile>> {
        let mut out = Vec::new();
        let mut start: Option<String> = None;
        let base_path = format!(
            "/n/{}/b/{}/o",
            percent_encode(&self.namespace, false),
            percent_encode(&self.bucket, false),
        );
        loop {
            let query = build_query(&[
                ("fields", LIST_FIELDS),
                ("prefix", &self.prefix),
                ("start", start.as_deref().unwrap_or("")),
            ]);
            let path_and_query = format!("{base_path}?{query}");
            let resp = self
                .client
                .signed_request(reqwest::Method::GET, &path_and_query, None)
                .await?;
            let page: ListObjectsResponse = resp.json().await.map_err(|e| {
                Error::engine(format!(
                    "oci list_objects {}/{}: parse response: {e}",
                    self.namespace, self.bucket
                ))
            })?;

            for obj in page.objects {
                let Some(relative_key) = relative_key(&self.prefix, &obj.name) else {
                    continue;
                };
                let size = obj.size.unwrap_or(0);
                if !self
                    .path_matcher
                    .is_file_included(&PathBuf::from(&relative_key))
                {
                    continue;
                }
                if self.max_file_size.is_some_and(|max| size > max) {
                    continue;
                }
                out.push(OciFile::new(
                    Some(self.client.clone()),
                    OciFilePath {
                        namespace: self.namespace.clone(),
                        bucket: self.bucket.clone(),
                        relative_path: relative_key,
                        object_name: obj.name,
                    },
                    size,
                    obj.time_modified.as_deref().and_then(parse_rfc3339_secs),
                    obj.md5,
                    obj.etag,
                ));
            }

            match page.next_start_with {
                Some(next) if !next.is_empty() => start = Some(next),
                _ => break,
            }
        }
        Ok(out)
    }

    /// Like [`list`](Self::list) but returns `(key, file)` pairs where `key` is the
    /// object's relative path — ready to hand to `Ctx::mount_each`.
    pub async fn items(&self) -> Result<Vec<(String, OciFile)>> {
        Ok(self
            .list()
            .await?
            .into_iter()
            .map(|f| (f.key(), f))
            .collect())
    }
}

/// Compute an object's path relative to `prefix`. Returns `None` for directory
/// markers (names ending in `/`) and for the prefix itself (empty relative path).
fn relative_key(prefix: &str, name: &str) -> Option<String> {
    if name.ends_with('/') {
        return None;
    }
    let relative = if prefix.is_empty() {
        name
    } else {
        name.strip_prefix(prefix)?.trim_start_matches('/')
    };
    if relative.is_empty() {
        None
    } else {
        Some(relative.to_string())
    }
}

// ---------------------------------------------------------------------------
// Live bucket-event view (the Rust analogue of Python's
// `list_objects(..., live_stream=...)`).
// ---------------------------------------------------------------------------

/// Object metadata returned by a `HEAD`, used to (re)build an [`OciFile`] for a
/// live event.
struct ObjectHead {
    size: u64,
    modified_secs: Option<i64>,
    md5: Option<String>,
    etag: Option<String>,
}

/// Clock-skew tolerance (seconds): live events whose `eventTime` precedes the
/// scan snapshot by more than this are dropped as already-covered by the scan.
/// Matches Python's `_SKEW_TOLERANCE`.
const OCI_LIVE_SKEW_SECS: i64 = 5;

/// The OCI Object Storage event envelope (`com.oraclecloud.objectstorage.*`).
#[derive(Deserialize)]
struct OciEvent {
    #[serde(rename = "eventType", default)]
    event_type: Option<String>,
    #[serde(rename = "eventTime", default)]
    event_time: Option<String>,
    #[serde(default)]
    data: Option<OciEventData>,
}

#[derive(Deserialize, Default)]
struct OciEventData {
    #[serde(rename = "resourceName", default)]
    resource_name: Option<String>,
    #[serde(rename = "additionalDetails", default)]
    additional_details: Option<OciEventDetails>,
}

#[derive(Deserialize, Default)]
struct OciEventDetails {
    #[serde(default)]
    namespace: Option<String>,
    #[serde(rename = "bucketName", default)]
    bucket_name: Option<String>,
}

/// A boxed event source: a stream of raw OCI event JSON payloads (one event per
/// item). The caller wires this to its event delivery mechanism (an OCI
/// Streaming/queue subscription, a webhook fan-in, …).
pub type OciEventStream = futures::stream::BoxStream<'static, Vec<u8>>;

/// A live view over an OCI bucket: an initial [`scan`](LiveMapView::scan) of
/// matching objects plus a [`watch`](LiveMapFeed::watch) that turns bucket
/// events into per-object updates/deletes. The Rust analogue of Python's
/// `list_objects(..., live_stream=...)`.
///
/// Each event is re-read with a `HEAD` (the live object state is authoritative
/// over the event type): a present object becomes an `update`, a `404` a
/// `delete`. Events are filtered by envelope type, namespace/bucket, the
/// prefix + path matcher, and an `eventTime` cutoff (snapshot time minus
/// [`OCI_LIVE_SKEW_SECS`]; missing/unparseable/future times pass through).
/// Feed it to [`Ctx::mount_each_live`](crate::Ctx::mount_each_live).
pub struct OciLiveWalker {
    client: OciClient,
    namespace: String,
    bucket: String,
    prefix: String,
    path_matcher: Arc<dyn FilePathMatcher>,
    max_file_size: Option<u64>,
    events: tokio::sync::Mutex<Option<OciEventStream>>,
    /// `eventTime` cutoff (Unix seconds), set at `scan` time.
    cutoff: tokio::sync::Mutex<i64>,
}

/// Build a live view over an OCI bucket, driven by `events` (a stream of raw OCI
/// event JSON payloads). See [`OciLiveWalker`].
pub fn list_objects_live(
    client: &OciClient,
    namespace: impl Into<String>,
    bucket: impl Into<String>,
    options: ListOptions,
    events: impl futures::Stream<Item = Vec<u8>> + Send + 'static,
) -> OciLiveWalker {
    OciLiveWalker {
        client: client.clone(),
        namespace: namespace.into(),
        bucket: bucket.into(),
        prefix: options.prefix,
        path_matcher: options
            .path_matcher
            .unwrap_or_else(|| Arc::new(MatchAllFilePathMatcher)),
        max_file_size: options.max_file_size,
        events: tokio::sync::Mutex::new(Some(Box::pin(events))),
        cutoff: tokio::sync::Mutex::new(0),
    }
}

impl OciLiveWalker {
    /// Process one event payload, dispatching an update/delete to `subscriber`.
    /// Returns `Ok(())` for skipped/filtered/transient events (never aborts the
    /// watch loop over a single bad event), matching Python's per-event
    /// resilience.
    async fn handle_event(
        &self,
        bytes: &[u8],
        cutoff: i64,
        subscriber: &LiveMapSubscriber<String, OciFile>,
    ) -> Result<()> {
        let Ok(event) = serde_json::from_slice::<OciEvent>(bytes) else {
            tracing::debug!("oci live: skipping malformed event payload");
            return Ok(());
        };
        // Envelope filter.
        let event_type = event.event_type.unwrap_or_default();
        if !event_type.starts_with("com.oraclecloud.objectstorage.") {
            return Ok(());
        }
        let Some(data) = event.data else {
            return Ok(());
        };
        let Some(object_name) = data.resource_name else {
            return Ok(());
        };
        // Namespace + bucket filter (drop cross-bucket events without a HEAD).
        let details = data.additional_details.unwrap_or_default();
        if details.namespace.as_deref() != Some(self.namespace.as_str())
            || details.bucket_name.as_deref() != Some(self.bucket.as_str())
        {
            return Ok(());
        }
        // Event-time cutoff: drop events that precede the scan snapshot. A
        // missing / unparseable / future-dated time falls through.
        if let Some(secs) = event.event_time.as_deref().and_then(parse_rfc3339_secs)
            && secs < cutoff
        {
            return Ok(());
        }
        // Prefix + path-matcher filter.
        let Some(relative_key) = relative_key(&self.prefix, &object_name) else {
            return Ok(());
        };
        if !self
            .path_matcher
            .is_file_included(&PathBuf::from(&relative_key))
        {
            return Ok(());
        }
        // Re-read: the live object state wins over the event type.
        match self
            .client
            .head_object_if_exists(&self.namespace, &self.bucket, &object_name)
            .await
        {
            Ok(Some(head)) => {
                if self.max_file_size.is_some_and(|max| head.size > max) {
                    return Ok(());
                }
                let file = OciFile::new(
                    Some(self.client.clone()),
                    OciFilePath {
                        namespace: self.namespace.clone(),
                        bucket: self.bucket.clone(),
                        relative_path: relative_key.clone(),
                        object_name,
                    },
                    head.size,
                    head.modified_secs,
                    head.md5,
                    head.etag,
                );
                subscriber.update(relative_key, file).await?;
            }
            Ok(None) => {
                subscriber.delete(relative_key).await?;
            }
            Err(e) => {
                tracing::warn!("oci live: HEAD {object_name} failed, skipping event: {e}");
            }
        }
        Ok(())
    }
}

#[crate::async_trait]
impl LiveMapView<String, OciFile> for OciLiveWalker {
    async fn scan(&self) -> Result<Vec<(String, OciFile)>> {
        // Snapshot the cutoff before listing so live events overlapping the scan
        // are reconciled rather than dropped.
        *self.cutoff.lock().await = chrono::Utc::now().timestamp() - OCI_LIVE_SKEW_SECS;
        let walker = list_objects(
            &self.client,
            self.namespace.clone(),
            self.bucket.clone(),
            ListOptions {
                prefix: self.prefix.clone(),
                path_matcher: Some(self.path_matcher.clone()),
                max_file_size: self.max_file_size,
            },
        );
        walker.items().await
    }
}

#[crate::async_trait]
impl LiveMapFeed<String, OciFile> for OciLiveWalker {
    async fn watch(&self, subscriber: LiveMapSubscriber<String, OciFile>) -> Result<()> {
        use futures::StreamExt;
        let mut events = self
            .events
            .lock()
            .await
            .take()
            .ok_or_else(|| Error::engine("oci live event stream already consumed"))?;
        let cutoff = *self.cutoff.lock().await;
        while let Some(bytes) = events.next().await {
            self.handle_event(&bytes, cutoff, &subscriber).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::{FileLike, PatternFilePathMatcher};
    use rsa::pkcs1v15::{Signature, VerifyingKey};
    use rsa::signature::Verifier;
    use rsa::{RsaPrivateKey, RsaPublicKey};

    #[test]
    fn parse_ini_selects_profile_and_trims() {
        let raw = "\
            [DEFAULT]\n\
            user = ocid1.user.oc1..aaaa\n\
            # a comment\n\
            tenancy=ocid1.tenancy.oc1..bbbb\n\
            region = us-ashburn-1\n\
            \n\
            [OTHER]\n\
            user = nope\n";
        let section = parse_ini_section(raw, "DEFAULT").unwrap();
        assert_eq!(section.get("user").unwrap(), "ocid1.user.oc1..aaaa");
        assert_eq!(section.get("tenancy").unwrap(), "ocid1.tenancy.oc1..bbbb");
        assert_eq!(section.get("region").unwrap(), "us-ashburn-1");
        assert!(parse_ini_section(raw, "MISSING").is_none());
        // Profile boundaries are respected.
        assert_eq!(
            parse_ini_section(raw, "OTHER")
                .unwrap()
                .get("user")
                .unwrap(),
            "nope"
        );
    }

    #[test]
    fn config_from_file_reports_missing_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config");
        std::fs::write(&path, "[DEFAULT]\nuser = u\n").unwrap();
        let err = OciConfig::from_file(&path, "DEFAULT")
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing required key"), "{err}");
    }

    #[test]
    fn expand_home_expands_tilde() {
        unsafe { std::env::set_var("HOME", "/home/test") };
        assert_eq!(expand_home("~/.oci/config"), "/home/test/.oci/config");
        assert_eq!(expand_home("/abs/path"), "/abs/path");
    }

    #[test]
    fn signing_string_is_canonical() {
        let s = signing_string(
            "GET",
            "/n/ns/b/bucket/o?fields=name&prefix=docs%2F",
            "objectstorage.us-ashburn-1.oraclecloud.com",
            "Tue, 02 Jun 2026 12:00:00 GMT",
        );
        assert_eq!(
            s,
            "(request-target): get /n/ns/b/bucket/o?fields=name&prefix=docs%2F\n\
             host: objectstorage.us-ashburn-1.oraclecloud.com\n\
             date: Tue, 02 Jun 2026 12:00:00 GMT"
        );
    }

    #[test]
    fn authorization_header_signs_and_verifies() {
        // A small key keeps the test fast; signing/verification exercise the real
        // RSA-SHA256 path used against OCI.
        let mut rng = rand::thread_rng();
        let key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let signing_key = SigningKey::<Sha256>::new(key.clone());
        let key_id = "ocid1.tenancy/ocid1.user/aa:bb:cc";
        let host = "objectstorage.us-ashburn-1.oraclecloud.com";
        let date = "Tue, 02 Jun 2026 12:00:00 GMT";
        let path = "/n/ns/b/bucket/o?fields=name";

        let header = build_authorization(&signing_key, key_id, "GET", path, host, date).unwrap();
        assert!(header.starts_with("Signature version=\"1\","));
        assert!(header.contains(&format!("keyId=\"{key_id}\"")));
        assert!(header.contains("algorithm=\"rsa-sha256\""));
        assert!(header.contains("headers=\"(request-target) host date\""));

        // Recover the base64 signature and verify it against the public key over
        // the exact signing string — proves the signature is correct.
        let sig_b64 = header
            .rsplit_once("signature=\"")
            .and_then(|(_, rest)| rest.strip_suffix('"'))
            .unwrap();
        let sig_bytes = STANDARD.decode(sig_b64).unwrap();
        let signature = Signature::try_from(sig_bytes.as_slice()).unwrap();
        let verifying_key = VerifyingKey::<Sha256>::new(RsaPublicKey::from(&key));
        let expected = signing_string("GET", path, host, date);
        verifying_key
            .verify(expected.as_bytes(), &signature)
            .unwrap();
    }

    #[test]
    fn percent_encode_keeps_slash_for_object_names() {
        assert_eq!(percent_encode("docs/a b.md", true), "docs/a%20b.md");
        assert_eq!(percent_encode("docs/a b.md", false), "docs%2Fa%20b.md");
        assert_eq!(percent_encode("name,size", false), "name%2Csize");
        assert_eq!(percent_encode("a-_.~z", false), "a-_.~z");
    }

    #[test]
    fn build_query_encodes_and_skips_empty() {
        let q = build_query(&[("fields", "name,size"), ("prefix", "docs/"), ("start", "")]);
        assert_eq!(q, "fields=name%2Csize&prefix=docs%2F");
    }

    #[test]
    fn object_path_encodes_components() {
        assert_eq!(
            object_path("my ns", "buck", "a/b c.md"),
            "/n/my%20ns/b/buck/o/a/b%20c.md"
        );
    }

    #[test]
    fn parse_uri_ok_and_errors() {
        assert_eq!(
            parse_oci_uri("oci://ns/my-bucket/data/config.json").unwrap(),
            (
                "ns".to_string(),
                "my-bucket".to_string(),
                "data/config.json".to_string()
            )
        );
        assert!(parse_oci_uri("ns/bucket/key").is_err()); // no scheme
        assert!(parse_oci_uri("oci://ns/bucket").is_err()); // no object
        assert!(parse_oci_uri("oci:///bucket/key").is_err()); // no namespace
    }

    #[test]
    fn parse_dates() {
        assert_eq!(
            parse_rfc3339_secs("1970-01-01T00:00:01Z").unwrap(),
            1,
            "rfc3339"
        );
        assert_eq!(
            parse_http_date_secs("Thu, 01 Jan 1970 00:00:01 GMT").unwrap(),
            1,
            "http-date"
        );
        assert!(parse_rfc3339_secs("not-a-date").is_none());
    }

    #[test]
    fn relative_key_strips_prefix_and_skips_markers() {
        assert_eq!(relative_key("", "a/b.md").as_deref(), Some("a/b.md"));
        assert_eq!(
            relative_key("data/", "data/a/b.md").as_deref(),
            Some("a/b.md")
        );
        assert_eq!(
            relative_key("data", "data/a/b.md").as_deref(),
            Some("a/b.md")
        );
        assert_eq!(relative_key("", "a/"), None); // directory marker
        assert_eq!(relative_key("data/", "data/"), None);
        assert_eq!(relative_key("data/x", "data/x"), None); // prefix object itself
        assert_eq!(relative_key("data/", "other/a.md"), None); // not under prefix
    }

    #[tokio::test]
    async fn oci_file_implements_shared_filelike_metadata_and_fingerprint() {
        let file = OciFile::new(
            None,
            OciFilePath {
                namespace: "ns".to_string(),
                bucket: "bucket".to_string(),
                relative_path: "docs/a.md".to_string(),
                object_name: "prefix/docs/a.md".to_string(),
            },
            42,
            Some(123),
            Some("md5-1".to_string()),
            Some("etag-1".to_string()),
        );

        assert_eq!(
            FileLike::file_path(&file).path(),
            std::path::Path::new("docs/a.md")
        );
        assert_eq!(FileSourceItem::key(&file), "docs/a.md");
        assert_eq!(FileLike::metadata(&file).await.unwrap().size, 42);
        assert_eq!(
            FileLike::content_fingerprint(&file).await.unwrap(),
            FileLike::content_fingerprint(&file).await.unwrap()
        );
        assert!(
            FileLike::read(&file)
                .await
                .unwrap_err()
                .to_string()
                .contains("not attached")
        );
    }

    #[tokio::test]
    async fn oci_file_is_recognized_by_memo_arg_helpers() {
        let file = OciFile::new(
            None,
            OciFilePath {
                namespace: "ns".to_string(),
                bucket: "bucket".to_string(),
                relative_path: "docs/a.md".to_string(),
                object_name: "prefix/docs/a.md".to_string(),
            },
            42,
            Some(123),
            Some("md5-1".to_string()),
            None,
        );

        let mut file_key = cocoindex_utils::fingerprint::Fingerprinter::default();
        crate::memo::write_key_fingerprint_part_for_arg(&mut file_key, &file).unwrap();

        let mut expected_key = cocoindex_utils::fingerprint::Fingerprinter::default();
        let path = FileLike::file_path(&file);
        crate::memo::write_key_fingerprint_part(&mut expected_key, &path.memo_key()).unwrap();

        assert_eq!(file_key.into_fingerprint(), expected_key.into_fingerprint());
        assert!(
            crate::memo::collect_memo_arg_state(&file, None)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn file_path_accessors_and_memo_key() {
        let fp = OciFilePath {
            namespace: "ns".to_string(),
            bucket: "b1".to_string(),
            relative_path: "a/b.md".to_string(),
            object_name: "data/a/b.md".to_string(),
        };
        assert_eq!(fp.namespace(), "ns");
        assert_eq!(fp.bucket(), "b1");
        assert_eq!(fp.path(), "a/b.md");
        assert_eq!(fp.resolve(), "data/a/b.md");
        let key = serde_json::to_value(fp.memo_key()).unwrap();
        assert_eq!(key, serde_json::json!(["ns", "b1", "a/b.md"]));
    }

    #[test]
    fn memo_key_differs_across_buckets_and_namespaces() {
        let mk = |ns: &str, bucket: &str| {
            let fp = OciFilePath {
                namespace: ns.to_string(),
                bucket: bucket.to_string(),
                relative_path: "a.md".to_string(),
                object_name: "a.md".to_string(),
            };
            serde_json::to_value(fp.memo_key()).unwrap()
        };
        assert_ne!(mk("ns", "b1"), mk("ns", "b2"));
        assert_ne!(mk("ns1", "b"), mk("ns2", "b"));
    }

    #[test]
    fn pattern_matcher_applies_to_relative_path() {
        let matcher = PatternFilePathMatcher::new(["**/*.md"], ["**/skip/**"]).unwrap();
        assert!(matcher.is_file_included(&PathBuf::from("a/b.md")));
        assert!(!matcher.is_file_included(&PathBuf::from("a/b.txt")));
        assert!(!matcher.is_file_included(&PathBuf::from("skip/b.md")));
    }
}
