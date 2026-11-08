//! Oracle Cloud Infrastructure (OCI) Object Storage source connector.
//!
//! Read-only listing and reading of OCI Object Storage objects, mirroring
//! Python's `cocoindex.connectors.oci_object_storage`:
//!
//! - [`OciClient`] — a clone-cheap connection handle. [`OciClient::connect`]
//!   reads an `~/.oci/config` profile (the same file the OCI CLI / Python SDK
//!   use; the key may be pass-phrase protected) and signs every request with
//!   OCI's RSA-SHA256 HTTP Signature scheme;
//!   [`OciClient::from_instance_principal`] runs as the compute instance itself,
//!   with a session token from the identity service that it renews as it expires.
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
//! [`crate::resources::file::FileLike`] trait.
//!
//! [`list_objects_live`] is the live view: an initial scan plus bucket events
//! delivered by a [`LiveStream`] of OCI event payloads (an OCI Streaming topic
//! read through the Kafka connector's `topic_as_stream(..).payloads()`), each
//! acknowledged to the stream once the object it names has synced.

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
use rsa::pkcs8::{DecodePrivateKey, EncodePublicKey};
use rsa::signature::{SignatureEncoding, Signer};
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::live_component::{
    LiveComponentOperator, LiveMapFeed, LiveMapSubscriber, LiveMapView, LiveStream,
    LiveStreamSubscriber, ReadyHandle, SingleWatcherGuard,
};
use crate::resources::file::{
    FileContentCache, FileLike, FileMetadata, FilePath, FilePathMatcher, FileSourceItem,
    MatchAllFilePathMatcher, decode_bytes,
};

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
    /// The key's pass-phrase, for a PKCS#8 encrypted key (`BEGIN ENCRYPTED
    /// PRIVATE KEY`, as `oci setup config` writes one).
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
    /// The verdict of [`exists`](Self::exists), kept for this handle's lifetime.
    #[serde(skip, default)]
    exists: Arc<tokio::sync::OnceCell<bool>>,
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
            exists: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    /// Whether this object currently exists in OCI.
    ///
    /// Performs a `HEAD` on the first call; the verdict is kept for the lifetime
    /// of this [`OciFile`] (not refreshed: fetch the object again for a fresh
    /// probe). Expected use: call `exists()` first and branch on the result,
    /// instead of letting [`read`](Self::read) fail on a 404.
    pub async fn exists(&self) -> Result<bool> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::engine("OCI file is not attached to an OciClient"))?;
        self.exists
            .get_or_try_init(|| async {
                let head = client
                    .head_object_if_exists(
                        &self.file_path.namespace,
                        &self.file_path.bucket,
                        &self.file_path.object_name,
                    )
                    .await?;
                Ok::<_, Error>(head.is_some())
            })
            .await
            .copied()
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

    pub async fn read(&self) -> Result<bytes::Bytes> {
        <Self as FileLike>::read(self).await
    }

    pub async fn read_size(&self, size: usize) -> Result<bytes::Bytes> {
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

crate::memo::impl_file_memo_input!(OciFile);

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
    auth: OciAuth,
    state_id: String,
}

/// What signs a request: the key id OCI knows the signer by, and the key.
#[derive(Clone)]
struct RequestSigner {
    key_id: String,
    signing_key: Arc<SigningKey<Sha256>>,
}

/// How the client authenticates.
enum OciAuth {
    /// An API key of a user: `{tenancy}/{user}/{fingerprint}` and its key.
    ApiKey(RequestSigner),
    /// The compute instance's own identity, through a session token.
    InstancePrincipal(Box<InstancePrincipal>),
}

/// Where an instance principal gets its identity from.
#[derive(Clone, Debug, Default)]
pub struct InstancePrincipalOptions {
    /// The instance metadata service, default `http://169.254.169.254/opc/v2`
    /// (the `OCI_METADATA_BASE_URL` environment variable overrides the default,
    /// as in the Python SDK).
    pub metadata_base_url: Option<String>,
    /// The identity service's federation endpoint, default
    /// `https://auth.{region}.oraclecloud.com/v1/x509` for the instance's region.
    pub federation_endpoint: Option<String>,
}

/// The default instance metadata service (IMDS v2) base URL.
const DEFAULT_METADATA_BASE_URL: &str = "http://169.254.169.254/opc/v2";

/// Refresh the session token this long before it expires (the Python SDK's
/// `DEFAULT_EXPIRY_JITTER_SECONDS`).
const SESSION_TOKEN_EXPIRY_MARGIN_SECS: i64 = 60;

/// A compute instance's identity: the certificate and key the instance
/// metadata service hands out, exchanged with the identity service for a
/// session token that signs requests as `ST$<token>` with a session key of
/// this process's own (the Python SDK's `InstancePrincipalsSecurityTokenSigner`).
struct InstancePrincipal {
    http: reqwest::Client,
    metadata_base_url: String,
    federation_endpoint: String,
    tenancy: String,
    session: tokio::sync::Mutex<Option<SessionToken>>,
}

/// A session token and the key that signs with it.
struct SessionToken {
    signer: RequestSigner,
    /// The token's `exp` claim (Unix seconds).
    expires_at: i64,
}

/// The identity material IMDS serves to an instance.
struct InstanceIdentity {
    leaf_cert_pem: String,
    leaf_key: RsaPrivateKey,
    intermediate_cert_pem: String,
}

/// The body of a PEM block: its base64 without the `BEGIN`/`END` lines and
/// newlines, as the federation request wants certificates and keys
/// (the Python SDK's `sanitize_certificate_string`).
fn pem_body(pem: &str) -> String {
    pem.lines()
        .filter(|line| !line.starts_with("-----"))
        .map(str::trim)
        .collect::<Vec<_>>()
        .join("")
}

/// A certificate's fingerprint as OCI's federation key id names it: SHA-1 over
/// the DER certificate, upper-case hex, colon-separated.
fn cert_fingerprint(cert_pem: &str) -> Result<String> {
    let der = STANDARD
        .decode(pem_body(cert_pem))
        .map_err(|e| Error::engine(format!("OCI instance certificate is not PEM: {e}")))?;
    Ok(Sha1::digest(&der)
        .iter()
        .map(|b| format!("{b:02X}"))
        .collect::<Vec<_>>()
        .join(":"))
}

/// The `exp` claim of a JWT, read without checking its signature (the token
/// is OCI's; this process only needs to know when to renew it).
fn jwt_expiry(token: &str) -> Result<i64> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| Error::engine("OCI session token is not a JWT"))?;
    let payload = URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .map_err(|e| Error::engine(format!("OCI session token payload: {e}")))?;
    let claims: serde_json::Value = serde_json::from_slice(&payload)
        .map_err(|e| Error::engine(format!("OCI session token claims: {e}")))?;
    claims
        .get("exp")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| Error::engine("OCI session token has no exp claim"))
}

impl InstancePrincipal {
    /// A text resource of the instance metadata service.
    async fn metadata(&self, path: &str) -> Result<String> {
        let url = format!("{}/{path}", self.metadata_base_url);
        let resp = self
            .http
            .get(&url)
            .header(reqwest::header::AUTHORIZATION, "Bearer Oracle")
            .send()
            .await
            .map_err(|e| Error::engine(format!("OCI instance metadata {url}: {e}")))?;
        if !resp.status().is_success() {
            return Err(Error::engine(format!(
                "OCI instance metadata {url} failed: {}",
                resp.status()
            )));
        }
        resp.text()
            .await
            .map_err(|e| Error::engine(format!("OCI instance metadata {url}: {e}")))
    }

    async fn identity(&self) -> Result<InstanceIdentity> {
        let leaf_cert_pem = self.metadata("identity/cert.pem").await?.trim().to_string();
        let key_pem = self.metadata("identity/key.pem").await?;
        let intermediate_cert_pem = self
            .metadata("identity/intermediate.pem")
            .await?
            .trim()
            .to_string();
        Ok(InstanceIdentity {
            leaf_cert_pem,
            leaf_key: load_private_key(&key_pem, None)?,
            intermediate_cert_pem,
        })
    }

    /// Exchange the instance's certificate for a session token: a new session
    /// key pair, and a federation request signed with the instance's key.
    async fn federate(&self) -> Result<SessionToken> {
        let identity = self.identity().await?;
        let session_key = RsaPrivateKey::new(&mut rand::thread_rng(), 2048)
            .map_err(|e| Error::engine(format!("generate OCI session key: {e}")))?;
        let public_pem = session_key
            .to_public_key()
            .to_public_key_pem(rsa::pkcs8::LineEnding::LF)
            .map_err(|e| Error::engine(format!("encode OCI session public key: {e}")))?;
        let body = serde_json::json!({
            "certificate": pem_body(&identity.leaf_cert_pem),
            "publicKey": pem_body(&public_pem),
            "intermediateCertificates": [pem_body(&identity.intermediate_cert_pem)],
        })
        .to_string();
        let url = reqwest::Url::parse(&self.federation_endpoint).map_err(|e| {
            Error::engine(format!(
                "OCI federation endpoint {}: {e}",
                self.federation_endpoint
            ))
        })?;
        let key_id = format!(
            "{}/fed-x509/{}",
            self.tenancy,
            cert_fingerprint(&identity.leaf_cert_pem)?
        );
        let date = http_date_now();
        let content_sha256 = STANDARD.encode(Sha256::digest(body.as_bytes()));
        let content_length = body.len().to_string();
        let request_target = format!("post {}", url.path());
        let signer = RequestSigner {
            key_id,
            signing_key: Arc::new(SigningKey::<Sha256>::new(identity.leaf_key)),
        };
        let authorization = sign_headers(
            &signer,
            &[
                ("date", &date),
                ("(request-target)", &request_target),
                ("content-length", &content_length),
                ("content-type", "application/json"),
                ("x-content-sha256", &content_sha256),
            ],
        )?;
        let resp = self
            .http
            .post(url)
            .header(reqwest::header::DATE, date)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::CONTENT_LENGTH, content_length)
            .header("x-content-sha256", content_sha256)
            .header(reqwest::header::AUTHORIZATION, authorization)
            .body(body)
            .send()
            .await
            .map_err(|e| Error::engine(format!("OCI federation request: {e}")))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| Error::engine(format!("OCI federation response: {e}")))?;
        if !status.is_success() {
            return Err(Error::engine(format!(
                "OCI federation request failed: {status} {text}"
            )));
        }
        let token = serde_json::from_str::<serde_json::Value>(&text)
            .ok()
            .and_then(|v| v.get("token").and_then(|t| t.as_str()).map(str::to_string))
            .ok_or_else(|| Error::engine("OCI federation response carries no token"))?;
        let expires_at = jwt_expiry(&token)?;
        Ok(SessionToken {
            signer: RequestSigner {
                key_id: format!("ST${token}"),
                signing_key: Arc::new(SigningKey::<Sha256>::new(session_key)),
            },
            expires_at,
        })
    }

    /// The current session's signer, renewed when the token is about to expire.
    async fn signer(&self) -> Result<RequestSigner> {
        let mut session = self.session.lock().await;
        let now = chrono::Utc::now().timestamp();
        if let Some(current) = session.as_ref()
            && now
                <= current
                    .expires_at
                    .saturating_sub(SESSION_TOKEN_EXPIRY_MARGIN_SECS)
        {
            return Ok(current.signer.clone());
        }
        let renewed = self.federate().await?;
        let signer = renewed.signer.clone();
        *session = Some(renewed);
        Ok(signer)
    }
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
        let pem = std::fs::read_to_string(&config.key_file)
            .map_err(|e| Error::engine(format!("read OCI key file {}: {e}", config.key_file)))?;
        let key = load_private_key(&pem, config.pass_phrase.as_deref())?;
        let key_id = format!("{}/{}/{}", config.tenancy, config.user, config.fingerprint);
        Ok(Self::with_auth(
            &config.region,
            OciAuth::ApiKey(RequestSigner {
                key_id,
                signing_key: Arc::new(SigningKey::<Sha256>::new(key)),
            }),
        ))
    }

    /// Build a client that authenticates as the compute instance it runs on
    /// (an *instance principal*): the instance's certificate from the metadata
    /// service is exchanged for a session token, renewed as it expires. The
    /// instance must be in a dynamic group with a policy that lets it read the
    /// bucket. The Python SDK's `InstancePrincipalsSecurityTokenSigner`.
    pub async fn from_instance_principal() -> Result<Self> {
        Self::from_instance_principal_with_options(InstancePrincipalOptions::default()).await
    }

    /// [`from_instance_principal`](Self::from_instance_principal) with the
    /// metadata and federation endpoints set explicitly.
    pub async fn from_instance_principal_with_options(
        options: InstancePrincipalOptions,
    ) -> Result<Self> {
        let metadata_base_url = options
            .metadata_base_url
            .or_else(|| {
                std::env::var("OCI_METADATA_BASE_URL")
                    .ok()
                    .filter(|s| !s.trim().is_empty())
            })
            .unwrap_or_else(|| DEFAULT_METADATA_BASE_URL.to_string())
            .trim_end_matches('/')
            .to_string();
        let http = reqwest::Client::new();
        let mut principal = InstancePrincipal {
            http,
            metadata_base_url,
            federation_endpoint: String::new(),
            tenancy: String::new(),
            session: tokio::sync::Mutex::new(None),
        };
        // The instance's region and tenancy, from its metadata document.
        let instance: serde_json::Value =
            serde_json::from_str(&principal.metadata("instance/").await?)
                .map_err(|e| Error::engine(format!("OCI instance metadata document: {e}")))?;
        let field = |name: &str| -> Result<String> {
            instance
                .get(name)
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .ok_or_else(|| {
                    Error::engine(format!("OCI instance metadata document has no {name:?}"))
                })
        };
        let region = field("canonicalRegionName").or_else(|_| field("region"))?;
        principal.tenancy = field("tenantId")?;
        principal.federation_endpoint = options
            .federation_endpoint
            .unwrap_or_else(|| format!("https://auth.{region}.oraclecloud.com/v1/x509"));
        // Fetch the first token now, so a misconfigured instance fails here
        // rather than on the first object read.
        let first = principal.federate().await?;
        *principal.session.get_mut() = Some(first);
        Ok(Self::with_auth(
            &region,
            OciAuth::InstancePrincipal(Box::new(principal)),
        ))
    }

    fn with_auth(region: &str, auth: OciAuth) -> Self {
        let host = format!("objectstorage.{region}.oraclecloud.com");
        let base_url = format!("https://{host}");
        let state_id = format!("oci-objectstorage:{region}");
        Self {
            inner: Arc::new(OciClientInner {
                http: reqwest::Client::new(),
                host,
                base_url,
                auth,
                state_id,
            }),
        }
    }

    /// The signer of the next request.
    async fn signer(&self) -> Result<RequestSigner> {
        match &self.inner.auth {
            OciAuth::ApiKey(signer) => Ok(signer.clone()),
            OciAuth::InstancePrincipal(principal) => principal.signer().await,
        }
    }

    /// Override the Object Storage base URL (default the public OCI endpoint).
    /// Mainly for pointing the client at a mock server in tests; the signed
    /// `host` header is left unchanged.
    pub fn with_base_url(self, base_url: impl Into<String>) -> Self {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        let inner = self.inner.as_ref();
        let auth = match &inner.auth {
            OciAuth::ApiKey(signer) => OciAuth::ApiKey(signer.clone()),
            OciAuth::InstancePrincipal(principal) => {
                OciAuth::InstancePrincipal(Box::new(InstancePrincipal {
                    http: principal.http.clone(),
                    metadata_base_url: principal.metadata_base_url.clone(),
                    federation_endpoint: principal.federation_endpoint.clone(),
                    tenancy: principal.tenancy.clone(),
                    // The session moves with the client: this one is consumed.
                    session: tokio::sync::Mutex::new(
                        principal.session.try_lock().ok().and_then(|mut s| s.take()),
                    ),
                }))
            }
        };
        Self {
            inner: Arc::new(OciClientInner {
                http: inner.http.clone(),
                host: inner.host.clone(),
                base_url,
                auth,
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
            &self.signer().await?,
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
            &self.signer().await?,
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

/// Load an RSA private key from PEM: PKCS#8 (`BEGIN PRIVATE KEY`), PKCS#1
/// (`BEGIN RSA PRIVATE KEY`), or PKCS#8 encrypted (`BEGIN ENCRYPTED PRIVATE
/// KEY`, as `oci setup config` writes) with its pass-phrase.
fn load_private_key(pem: &str, pass_phrase: Option<&str>) -> Result<RsaPrivateKey> {
    if pem.contains("-----BEGIN ENCRYPTED PRIVATE KEY-----") {
        let Some(pass_phrase) = pass_phrase else {
            return Err(Error::engine(
                "OCI private key is encrypted; set pass_phrase in the OCI config profile",
            ));
        };
        return RsaPrivateKey::from_pkcs8_encrypted_pem(pem, pass_phrase).map_err(|e| {
            Error::engine(format!(
                "decrypt OCI private key (is the pass_phrase right?): {e}"
            ))
        });
    }
    if pem.contains("Proc-Type: 4,ENCRYPTED") {
        return Err(Error::engine(
            "OCI private key uses the legacy OpenSSL encryption; re-encode it as an \
             encrypted PKCS#8 key (`openssl pkcs8 -topk8 -in key.pem -out key8.pem`)",
        ));
    }
    RsaPrivateKey::from_pkcs8_pem(pem)
        .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
        .map_err(|e| {
            Error::engine(format!(
                "parse OCI private key (expected a PKCS#8 or PKCS#1 PEM): {e}"
            ))
        })
}

/// Build the OCI `Authorization: Signature ...` header value of a `GET`/`HEAD`.
fn build_authorization(
    signer: &RequestSigner,
    method: &str,
    path_and_query: &str,
    host: &str,
    date: &str,
) -> Result<String> {
    let request_target = format!("{} {path_and_query}", method.to_ascii_lowercase());
    sign_headers(
        signer,
        &[
            ("(request-target)", &request_target),
            ("host", host),
            ("date", date),
        ],
    )
}

/// Build the OCI `Authorization: Signature ...` header value over `headers`,
/// signed in the order given (`name: value` lines joined by newlines).
fn sign_headers(signer: &RequestSigner, headers: &[(&str, &str)]) -> Result<String> {
    let to_sign = headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}"))
        .collect::<Vec<_>>()
        .join("\n");
    let signature = signer
        .signing_key
        .try_sign(to_sign.as_bytes())
        .map_err(|e| Error::engine(format!("sign OCI request: {e}")))?;
    let sig_b64 = STANDARD.encode(signature.to_bytes());
    let names = headers
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(" ");
    Ok(format!(
        "Signature version=\"1\",keyId=\"{}\",algorithm=\"rsa-sha256\",\
         headers=\"{names}\",signature=\"{sig_b64}\"",
        signer.key_id
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

/// A live view over an OCI bucket: an initial [`scan`](LiveMapView::scan) of
/// matching objects plus a [`watch`](LiveMapFeed::watch) that turns bucket
/// events into per-object updates/deletes. The Rust analogue of Python's
/// `list_objects(..., live_stream=...)`.
///
/// Each event is re-read with a `HEAD` (the live object state is authoritative
/// over the event type): a present object becomes an `update`, a `404` a
/// `delete`. Events are filtered by envelope type, namespace/bucket, the
/// prefix + path matcher, and an `eventTime` cutoff (snapshot time minus
/// `OCI_LIVE_SKEW_SECS`; missing/unparseable/future times pass through).
/// An event is acknowledged to its [`LiveStream`] once the update or delete it
/// caused has synced (at once when it was filtered out), so a durable stream
/// commits its position only past processed events. Feed it to
/// [`Ctx::mount_each_live`](crate::Ctx::mount_each_live).
pub struct OciLiveWalker {
    client: OciClient,
    namespace: String,
    bucket: String,
    prefix: String,
    path_matcher: Arc<dyn FilePathMatcher>,
    max_file_size: Option<u64>,
    events: Box<dyn LiveStream<Vec<u8>>>,
    /// `eventTime` cutoff (Unix seconds), set at `scan` time.
    cutoff: tokio::sync::Mutex<i64>,
    watch_guard: SingleWatcherGuard,
    /// Opt-in version for skipping the startup scan on reruns (see
    /// [`OciLiveWalker::logic_version`]). `None` always scans.
    logic_version: Option<String>,
    /// Decision recorded by `skip_initial_scan`, read by `watch` to know whether
    /// the scan ran (so the event cutoff and version write can branch on it).
    skip_scan: tokio::sync::Mutex<bool>,
}

/// Build a live view over an OCI bucket, driven by `events`: a [`LiveStream`]
/// of raw OCI event JSON payloads, typically an OCI Streaming topic the OCI
/// Events service publishes the bucket's events to, read with the Kafka
/// connector's `topic_as_stream(..).payloads()`. See [`OciLiveWalker`]; Python's
/// `list_objects(..., live_stream=...)`.
pub fn list_objects_live(
    client: &OciClient,
    namespace: impl Into<String>,
    bucket: impl Into<String>,
    options: ListOptions,
    events: impl LiveStream<Vec<u8>>,
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
        events: Box::new(events),
        cutoff: tokio::sync::Mutex::new(0),
        watch_guard: SingleWatcherGuard::new("OciLiveWalker"),
        logic_version: None,
        skip_scan: tokio::sync::Mutex::new(false),
    }
}

/// Committed-state key under which the live view records `logic_version` after a
/// successful full scan, read back on a later run to decide whether the startup
/// scan can be skipped.
const OCI_SCAN_VERSION_KEY: &str = "__coco_oci_scan_version__";

impl OciLiveWalker {
    /// Opt into skipping the startup full scan on reruns. When set, the live view
    /// records this version after a successful scan and, on a later run, skips the
    /// scan if the recorded version matches — relying on the durable event stream
    /// to replay the downtime backlog from its committed cursor.
    ///
    /// You MUST bump this whenever your processing logic changes (otherwise stale
    /// state is silently kept), and the stream MUST be durable (e.g. a Kafka
    /// consumer with a stable group and committed offsets). Leave unset to always
    /// scan on startup.
    pub fn logic_version(mut self, version: impl Into<String>) -> Self {
        self.logic_version = Some(version.into());
        self
    }

    /// Process one event payload, dispatching an update/delete to `subscriber`;
    /// the handle that acknowledges the event once that has synced. A skipped,
    /// filtered or transiently failing event is acknowledged at once (never
    /// aborts the watch over a single bad event), matching Python's per-event
    /// resilience.
    async fn handle_event(
        &self,
        bytes: &[u8],
        cutoff: i64,
        subscriber: &LiveMapSubscriber<String, OciFile>,
    ) -> Result<ReadyHandle> {
        let Ok(event) = serde_json::from_slice::<OciEvent>(bytes) else {
            tracing::debug!("oci live: skipping malformed event payload");
            return Ok(ReadyHandle::immediate());
        };
        // Envelope filter.
        let event_type = event.event_type.unwrap_or_default();
        if !event_type.starts_with("com.oraclecloud.objectstorage.") {
            return Ok(ReadyHandle::immediate());
        }
        let Some(data) = event.data else {
            return Ok(ReadyHandle::immediate());
        };
        let Some(object_name) = data.resource_name else {
            return Ok(ReadyHandle::immediate());
        };
        // Namespace + bucket filter (drop cross-bucket events without a HEAD).
        let details = data.additional_details.unwrap_or_default();
        if details.namespace.as_deref() != Some(self.namespace.as_str())
            || details.bucket_name.as_deref() != Some(self.bucket.as_str())
        {
            return Ok(ReadyHandle::immediate());
        }
        // Event-time cutoff: drop events that precede the scan snapshot. A
        // missing / unparseable / future-dated time falls through.
        if let Some(secs) = event.event_time.as_deref().and_then(parse_rfc3339_secs)
            && secs < cutoff
        {
            return Ok(ReadyHandle::immediate());
        }
        // Prefix + path-matcher filter.
        let Some(relative_key) = relative_key(&self.prefix, &object_name) else {
            return Ok(ReadyHandle::immediate());
        };
        if !self
            .path_matcher
            .is_file_included(&PathBuf::from(&relative_key))
        {
            return Ok(ReadyHandle::immediate());
        }
        // Re-read: the live object state wins over the event type.
        match self
            .client
            .head_object_if_exists(&self.namespace, &self.bucket, &object_name)
            .await
        {
            Ok(Some(head)) => {
                if self.max_file_size.is_some_and(|max| head.size > max) {
                    return Ok(ReadyHandle::immediate());
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
                subscriber.start_update(relative_key, file).await
            }
            Ok(None) => subscriber.start_delete(relative_key).await,
            Err(e) => {
                tracing::warn!("oci live: HEAD {object_name} failed, skipping event: {e}");
                Ok(ReadyHandle::immediate())
            }
        }
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
    async fn skip_initial_scan(&self, operator: &LiveComponentOperator) -> Result<bool> {
        let Some(version) = &self.logic_version else {
            return Ok(false);
        };
        let stored: Option<String> = operator.read_committed_state(OCI_SCAN_VERSION_KEY).await?;
        let skip = stored.as_deref() == Some(version.as_str());
        *self.skip_scan.lock().await = skip;
        Ok(skip)
    }

    async fn watch(&self, subscriber: LiveMapSubscriber<String, OciFile>) -> Result<()> {
        let _watch_token = self.watch_guard.enter()?;
        let skip_scan = *self.skip_scan.lock().await;
        // Record the version only after a real scan (which has already committed
        // — `mark_ready` ran before `watch`), so a later run skips only when the
        // bootstrap durably landed.
        if !skip_scan && let Some(version) = &self.logic_version {
            subscriber
                .write_committed_state(OCI_SCAN_VERSION_KEY, version)
                .await?;
        }
        // In skip-scan mode there is no scan to dedupe against, so the wall-clock
        // cutoff must not fire — the durable stream replays the downtime backlog
        // and every replayed event must be processed. `i64::MIN` drops nothing.
        let cutoff = if skip_scan {
            i64::MIN
        } else {
            *self.cutoff.lock().await
        };
        self.events
            .watch(&OciEventSubscriber {
                walker: self,
                map: &subscriber,
                cutoff,
            })
            .await
    }
}

/// Receives the bucket's event payloads from the [`LiveStream`] and turns each
/// into an update or delete of the live map (Python's `_OCIStreamSubscriber`).
struct OciEventSubscriber<'a> {
    walker: &'a OciLiveWalker,
    map: &'a LiveMapSubscriber<String, OciFile>,
    cutoff: i64,
}

#[crate::async_trait]
impl LiveStreamSubscriber<Vec<u8>> for OciEventSubscriber<'_> {
    async fn send(&self, message: Vec<u8>) -> Result<ReadyHandle> {
        self.walker
            .handle_event(&message, self.cutoff, self.map)
            .await
    }

    /// Readiness is the scan's, signalled before the watch starts; the stream's
    /// own readiness is not consumed.
    async fn mark_ready(&self) {}

    async fn read_committed_state(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.map.read_committed_state(key).await
    }

    async fn write_committed_state(&self, key: &str, value: &[u8]) -> Result<()> {
        self.map.write_committed_state(key, &value.to_vec()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::file::{FileLike, PatternFilePathMatcher};
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
    fn authorization_header_signs_and_verifies() {
        // A small key keeps the test fast; signing/verification exercise the real
        // RSA-SHA256 path used against OCI.
        let mut rng = rand::thread_rng();
        let key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let signer = RequestSigner {
            key_id: "ocid1.tenancy/ocid1.user/aa:bb:cc".to_string(),
            signing_key: Arc::new(SigningKey::<Sha256>::new(key.clone())),
        };
        let key_id = signer.key_id.as_str();
        let host = "objectstorage.us-ashburn-1.oraclecloud.com";
        let date = "Tue, 02 Jun 2026 12:00:00 GMT";
        let path = "/n/ns/b/bucket/o?fields=name";

        let header = build_authorization(&signer, "GET", path, host, date).unwrap();
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
        // The canonical string: `(request-target)` with the lower-cased method,
        // then `host` and `date`, one `name: value` per line.
        let expected = format!("(request-target): get {path}\nhost: {host}\ndate: {date}");
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
