//! Google Drive source connector.
//!
//! [`GoogleDriveSource::list_files`] recursively lists files under configured
//! root folders. Use each [`DriveFile::key`] with `Ctx::mount_each` so per-file
//! memoization handles edits and target reconciliation removes derived rows for
//! deleted files.
//!
//! The connector supports service-account auth and a personal Google account
//! signed in with OAuth ([`UserAccount`], experimental; the token file is the
//! one the Python SDK writes, so one sign-in serves both), optional MIME-type
//! filtering, Google-native export formats, and binary downloads for
//! non-native files.
//!
//! No C build dependencies: the service-account JWT is signed (RS256) with the
//! pure-Rust `rsa`/`sha2` crates, and HTTP uses `reqwest`. The Drive base URL and
//! auth are pluggable so the connector can be driven against a mock server.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use cocoindex_utils::fingerprint::Fingerprint;
use rsa::RsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::signature::{SignatureEncoding, Signer};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::error::{Error, Result};
use crate::resources::file::{
    FileContentCache, FileLike, FileMetadata, FilePath, FileSourceItem, decode_bytes,
};

const DRIVE_SCOPE: &str = "https://www.googleapis.com/auth/drive.readonly";
const DEFAULT_BASE_URL: &str = "https://www.googleapis.com/drive/v3";
const FOLDER_MIME: &str = "application/vnd.google-apps.folder";

/// Export MIME type for a Google-native document, or `None` for a binary file
/// that should be downloaded as-is via `alt=media`.
fn export_mime_for(mime_type: &str) -> Option<&'static str> {
    match mime_type {
        "application/vnd.google-apps.document" => Some("text/markdown"),
        "application/vnd.google-apps.spreadsheet" => Some("text/csv"),
        "application/vnd.google-apps.presentation" => Some("text/plain"),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// DriveFile — a source item
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct DriveFilePath {
    path: FilePath,
    file_id: String,
}

impl DriveFilePath {
    pub fn new(path: impl Into<std::path::PathBuf>, file_id: impl Into<String>) -> Self {
        Self {
            path: FilePath::new(path),
            file_id: file_id.into(),
        }
    }

    pub fn path(&self) -> &std::path::Path {
        self.path.path()
    }

    pub fn file_id(&self) -> &str {
        &self.file_id
    }

    pub fn resolve(&self) -> &str {
        &self.file_id
    }

    pub fn as_file_path(&self) -> FilePath {
        self.path.clone()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DriveFileInfo {
    pub file_id: String,
    pub name: String,
    pub path: String,
    pub mime_type: String,
    pub size: u64,
    pub modified_time: String,
    /// The MD5 of a binary file's content, as Drive reports it; `None` for a
    /// Google-native document.
    #[serde(default)]
    pub md5_checksum: Option<String>,
}

/// A file discovered in Google Drive.
///
/// The serializable fields are stable source metadata. Files returned by
/// [`GoogleDriveSource::list_files`] also carry a clone-cheap client so they can
/// be read through the shared [`FileLike`] API.
#[derive(Clone, Serialize, Deserialize)]
pub struct DriveFile {
    pub file_id: String,
    pub name: String,
    /// Path relative to the configured Drive root folder.
    #[serde(default)]
    pub path: String,
    pub mime_type: String,
    pub size: u64,
    /// RFC 3339 timestamp string as returned by the Drive API.
    pub modified_time: String,
    /// The MD5 of a binary file's content, as Drive reports it; `None` for a
    /// Google-native document. It is the file's content fingerprint, so an
    /// edit that keeps the content does not reprocess the file.
    #[serde(default)]
    pub md5_checksum: Option<String>,
    #[serde(skip)]
    client: Option<GoogleDriveClient>,
    #[serde(skip, default = "default_file_cache")]
    cache: Arc<FileContentCache>,
}

impl DriveFile {
    pub fn new(info: DriveFileInfo) -> Self {
        Self::from_info(None, info)
    }

    fn from_info(client: Option<GoogleDriveClient>, info: DriveFileInfo) -> Self {
        let metadata = FileMetadata {
            size: info.size,
            modified: parse_modified_time(&info.modified_time),
            content_fingerprint: md5_fingerprint(info.md5_checksum.as_deref()),
        };
        Self {
            file_id: info.file_id,
            name: info.name,
            path: info.path,
            mime_type: info.mime_type,
            size: info.size,
            modified_time: info.modified_time,
            md5_checksum: info.md5_checksum,
            client,
            cache: Arc::new(FileContentCache::with_metadata(metadata)),
        }
    }

    pub fn with_client(mut self, client: GoogleDriveClient) -> Self {
        self.client = Some(client);
        self
    }

    /// Stable key for `mount_each`. Drive names and paths need not be unique.
    pub fn key(&self) -> String {
        self.file_id.clone()
    }

    pub fn path(&self) -> &str {
        if self.path.is_empty() {
            &self.name
        } else {
            &self.path
        }
    }

    pub fn file_path(&self) -> DriveFilePath {
        DriveFilePath::new(self.path(), self.file_id.clone())
    }

    pub async fn read(&self) -> Result<Vec<u8>> {
        <Self as FileLike>::read(self).await
    }

    pub async fn read_text(&self) -> Result<String> {
        <Self as FileLike>::read_text(self).await
    }

    pub fn info(&self) -> DriveFileInfo {
        DriveFileInfo {
            file_id: self.file_id.clone(),
            name: self.name.clone(),
            path: self.path().to_string(),
            mime_type: self.mime_type.clone(),
            size: self.size,
            modified_time: self.modified_time.clone(),
            md5_checksum: self.md5_checksum.clone(),
        }
    }

    fn is_folder(&self) -> bool {
        self.mime_type == FOLDER_MIME
    }
}

#[async_trait]
impl FileLike for DriveFile {
    fn file_path(&self) -> FilePath {
        DriveFile::file_path(self).as_file_path()
    }

    fn cache(&self) -> &FileContentCache {
        &self.cache
    }

    async fn fetch_metadata(&self) -> Result<FileMetadata> {
        Ok(FileMetadata {
            size: self.size,
            modified: parse_modified_time(&self.modified_time),
            content_fingerprint: md5_fingerprint(self.md5_checksum.as_deref()),
        })
    }

    async fn read_impl(&self, size: Option<usize>) -> Result<Vec<u8>> {
        if size.is_some() {
            return Err(Error::engine(
                "partial reads are not supported for Google Drive files",
            ));
        }
        let client = self.client.as_ref().ok_or_else(|| {
            Error::engine("Google Drive file is not attached to a GoogleDriveClient")
        })?;
        client.read_file_bytes(self).await
    }
}

impl FileSourceItem for DriveFile {
    fn key(&self) -> String {
        DriveFile::key(self)
    }
}

#[async_trait::async_trait]
impl crate::memo::MemoInput for DriveFile {
    fn write_memo_key(&self, writer: &mut crate::memo::MemoKeyWriter<'_>) -> Result<()> {
        writer.write(&(&self.file_id, self.path()))
    }

    async fn memo_state(
        &self,
        previous: Option<&crate::memo::MemoStateValue>,
    ) -> Result<Option<crate::memo::MemoStateDecision>> {
        crate::memo::file_memo_state(self, previous).await.map(Some)
    }
}

fn default_file_cache() -> Arc<FileContentCache> {
    Arc::new(FileContentCache::new())
}

/// Drive's MD5 as the content fingerprint (the Python connector does the same).
fn md5_fingerprint(md5_checksum: Option<&str>) -> Option<Fingerprint> {
    md5_checksum
        .filter(|s| !s.is_empty())
        .map(|s| Fingerprint::from_bytes(s.as_bytes()))
}

/// Parse a Drive `files.list` response into its files and the next page token.
fn parse_file_list(json: &serde_json::Value) -> Result<(Vec<DriveFile>, Option<String>)> {
    let mut files = Vec::new();
    if let Some(arr) = json.get("files").and_then(|v| v.as_array()) {
        for f in arr {
            let file_id = f
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::engine("drive file entry missing id"))?
                .to_string();
            let name = f
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let mime_type = f
                .get("mimeType")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            // Drive returns `size` as a decimal string; folders/native docs omit it.
            let size = f
                .get("size")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let modified_time = f
                .get("modifiedTime")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let md5_checksum = f
                .get("md5Checksum")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            files.push(DriveFile::new(DriveFileInfo {
                file_id: file_id.clone(),
                name,
                path: String::new(),
                mime_type,
                size,
                modified_time,
                md5_checksum,
            }));
        }
    }
    let next = json
        .get("nextPageToken")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    Ok((files, next))
}

/// Reject anything that isn't a plausible Drive id, so it can't break out of the
/// `q` filter (Drive ids are URL-safe base64-ish tokens).
fn validate_drive_id(kind: &str, id: &str) -> Result<()> {
    if !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        Ok(())
    } else {
        Err(Error::engine(format!("invalid {kind}: {id:?}")))
    }
}

fn validate_folder_id(id: &str) -> Result<()> {
    validate_drive_id("Google Drive folder id", id)
}

fn parse_modified_time(value: &str) -> SystemTime {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|dt| {
            let seconds = dt.timestamp();
            if seconds >= 0 {
                UNIX_EPOCH
                    + Duration::from_secs(seconds as u64)
                    + Duration::from_nanos(dt.timestamp_subsec_nanos() as u64)
            } else {
                UNIX_EPOCH
            }
        })
        .unwrap_or(UNIX_EPOCH)
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum Auth {
    ServiceAccount {
        client_email: String,
        private_key: String,
        token_uri: String,
    },
    /// A pre-obtained bearer token (used for tests / custom auth flows).
    Static(String),
    /// A personal account's stored sign-in ([`UserAccount`]).
    User(Arc<UserCredentials>),
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

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Build and sign a service-account JWT assertion (RS256).
fn make_jwt(
    client_email: &str,
    private_key_pem: &str,
    token_uri: &str,
    scope: &str,
    now: u64,
) -> Result<String> {
    let header = serde_json::json!({ "alg": "RS256", "typ": "JWT" });
    let claims = serde_json::json!({
        "iss": client_email,
        "scope": scope,
        "aud": token_uri,
        "iat": now,
        "exp": now + 3600,
    });
    let to_vec = |v: &serde_json::Value| -> Result<Vec<u8>> {
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
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());
    Ok(format!("{signing_input}.{sig_b64}"))
}

// ---------------------------------------------------------------------------
// GoogleDriveClient — connection handle (Clone-cheap)
// ---------------------------------------------------------------------------

struct ClientInner {
    http: reqwest::Client,
    base_url: String,
    auth: Auth,
    state_id: String,
    token_cache: tokio::sync::Mutex<Option<CachedToken>>,
}

/// A Google Drive connection. Clone-cheap (the underlying client is shared).
#[derive(Clone)]
pub struct GoogleDriveClient {
    inner: Arc<ClientInner>,
}

impl GoogleDriveClient {
    /// Build a client from a service-account JSON key file (the standard
    /// `type: service_account` key downloaded from Google Cloud).
    pub fn from_service_account_file(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path).map_err(Error::Io)?;
        let json: serde_json::Value = serde_json::from_str(&raw)
            .map_err(|e| Error::engine(format!("parse service-account file: {e}")))?;
        let field = |k: &str| -> Result<String> {
            json.get(k)
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .ok_or_else(|| Error::engine(format!("service-account file missing {k:?}")))
        };
        let client_email = field("client_email")?;
        let private_key = field("private_key")?;
        let token_uri = json
            .get("token_uri")
            .and_then(|v| v.as_str())
            .unwrap_or("https://oauth2.googleapis.com/token")
            .to_string();
        Ok(Self::build(
            Auth::ServiceAccount {
                client_email: client_email.clone(),
                private_key,
                token_uri,
            },
            format!("service_account:{client_email}"),
        ))
    }

    /// Build a client that signs as the personal account stored by
    /// `account` ([`UserAccount::sign_in`]). The stored tokens are read now; an
    /// error says how to sign in when none are stored. The access token is
    /// refreshed as it expires and written back to the token file.
    ///
    /// The client's identity ([`state_id`](Self::state_id)) is the account's
    /// address, never its tokens: a refreshed token keeps memoized work, a
    /// different account invalidates it.
    pub fn from_user_account(account: &UserAccount) -> Result<Self> {
        let tokens = account.tokens()?;
        let state_id = format!("user_account:{}", tokens.email);
        Ok(Self::build(
            Auth::User(Arc::new(UserCredentials {
                token_path: account.token_path.clone(),
                token_url: account.endpoints.token.clone(),
                tokens: tokio::sync::Mutex::new(tokens),
            })),
            state_id,
        ))
    }

    /// Build a client from a pre-obtained OAuth bearer token. Useful for tests
    /// (against a mock server) or custom auth flows.
    pub fn from_static_token(token: impl Into<String>) -> Self {
        let token = token.into();
        Self::build(Auth::Static(token), "static_token".to_string())
    }

    fn build(auth: Auth, state_id: String) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                http: reqwest::Client::new(),
                base_url: DEFAULT_BASE_URL.to_string(),
                auth,
                state_id,
                token_cache: tokio::sync::Mutex::new(None),
            }),
        }
    }

    /// Override the Drive API base URL (default the public Google endpoint).
    /// Mainly for pointing the client at a mock server in tests.
    pub fn with_base_url(self, base_url: impl Into<String>) -> Self {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        let inner = self.inner.as_ref();
        Self {
            inner: Arc::new(ClientInner {
                http: inner.http.clone(),
                base_url,
                auth: inner.auth.clone(),
                state_id: inner.state_id.clone(),
                token_cache: tokio::sync::Mutex::new(None),
            }),
        }
    }

    /// Stable identity (for use as a `ContextKey` state id / memo dependency).
    pub fn state_id(&self) -> &str {
        &self.inner.state_id
    }

    async fn token(&self) -> Result<String> {
        match &self.inner.auth {
            Auth::Static(t) => Ok(t.clone()),
            Auth::User(credentials) => credentials.access_token(&self.inner.http).await,
            Auth::ServiceAccount {
                client_email,
                private_key,
                token_uri,
            } => {
                let mut guard = self.inner.token_cache.lock().await;
                if let Some(cached) = guard.as_ref()
                    && cached.expires_at > Instant::now() + Duration::from_secs(60)
                {
                    return Ok(cached.token.clone());
                }
                let jwt = make_jwt(
                    client_email,
                    private_key,
                    token_uri,
                    DRIVE_SCOPE,
                    unix_now(),
                )?;
                let resp = self
                    .inner
                    .http
                    .post(token_uri)
                    .form(&[
                        ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                        ("assertion", jwt.as_str()),
                    ])
                    .send()
                    .await
                    .map_err(|e| Error::engine(format!("token request: {e}")))?
                    .error_for_status()
                    .map_err(|e| Error::engine(format!("token request failed: {e}")))?;
                let token: TokenResponse = resp
                    .json()
                    .await
                    .map_err(|e| Error::engine(format!("parse token response: {e}")))?;
                let ttl = token.expires_in.max(60);
                *guard = Some(CachedToken {
                    token: token.access_token.clone(),
                    expires_at: Instant::now() + Duration::from_secs(ttl),
                });
                Ok(token.access_token)
            }
        }
    }

    /// List the direct children of a folder (one page at a time, following
    /// `nextPageToken`).
    async fn list_children(&self, folder_id: &str) -> Result<Vec<DriveFile>> {
        validate_folder_id(folder_id)?;
        let token = self.token().await?;
        let q = format!("'{folder_id}' in parents and trashed = false");
        let url = format!("{}/files", self.inner.base_url);
        let mut out = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut req = self.inner.http.get(&url).bearer_auth(&token).query(&[
                ("q", q.as_str()),
                (
                    "fields",
                    "nextPageToken, files(id, name, mimeType, size, modifiedTime, md5Checksum)",
                ),
            ]);
            if let Some(t) = &page_token {
                req = req.query(&[("pageToken", t.as_str())]);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| Error::engine(format!("drive list: {e}")))?
                .error_for_status()
                .map_err(|e| Error::engine(format!("drive list failed: {e}")))?;
            let json: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| Error::engine(format!("parse drive list: {e}")))?;
            let (mut files, next) = parse_file_list(&json)?;
            out.append(&mut files);
            match next {
                Some(t) => page_token = Some(t),
                None => break,
            }
        }
        Ok(out)
    }

    /// Download a file's raw bytes — exporting Google-native docs to text, or
    /// fetching binary content via `alt=media`.
    async fn read_file_bytes(&self, file: &DriveFile) -> Result<Vec<u8>> {
        validate_drive_id("Google Drive file id", &file.file_id)?;
        let token = self.token().await?;
        let req = match export_mime_for(&file.mime_type) {
            Some(export_mime) => self
                .inner
                .http
                .get(format!(
                    "{}/files/{}/export",
                    self.inner.base_url, file.file_id
                ))
                .query(&[("mimeType", export_mime)]),
            None => self
                .inner
                .http
                .get(format!("{}/files/{}", self.inner.base_url, file.file_id))
                .query(&[("alt", "media")]),
        };
        let resp = req
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| Error::engine(format!("drive download: {e}")))?
            .error_for_status()
            .map_err(|e| Error::engine(format!("drive download failed: {e}")))?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| Error::engine(format!("drive download body: {e}")))?;
        Ok(bytes.to_vec())
    }

    /// Download a file's raw bytes. Prefer [`DriveFile::read`] for files
    /// returned by [`GoogleDriveSource`], because it caches full reads.
    pub async fn read(&self, file: &DriveFile) -> Result<Vec<u8>> {
        self.read_file_bytes(file).await
    }

    /// Download a file and decode it as UTF-8 (lossless on valid UTF-8).
    pub async fn read_text(&self, file: &DriveFile) -> Result<String> {
        Ok(decode_bytes(&self.read(file).await?))
    }
}

#[derive(Clone)]
pub struct GoogleDriveSourceSpec {
    pub client: GoogleDriveClient,
    pub root_folder_ids: Vec<String>,
    pub mime_types: Option<Vec<String>>,
}

pub async fn list_files(spec: GoogleDriveSourceSpec) -> Result<Vec<DriveFile>> {
    GoogleDriveSource {
        client: spec.client,
        root_folder_ids: spec.root_folder_ids,
        mime_types: spec.mime_types,
    }
    .list_files()
    .await
}

// ---------------------------------------------------------------------------
// UserAccount — OAuth sign-in for a personal Google account (experimental)
// ---------------------------------------------------------------------------
//
// A `UserAccount` is a token file, shared with the Python SDK: the same format
// (`version`, `client_id`, `client_secret`, `refresh_token`, `access_token`,
// `expires_at`, `email`) at the same default location, so one sign-in serves
// both. `sign_in` fills it through the flow Google recommends for command-line
// applications: the authorization code grant with PKCE, redirected to a
// listener on the loopback interface, from a Desktop-app OAuth client the user
// created in their own Google Cloud project. A host with no browser prints the
// authorization link instead and accepts the address the browser lands on,
// pasted back. `GoogleDriveClient::from_user_account` then signs its requests
// with the stored access token and refreshes it with the refresh token as it
// expires, writing the new token back to the file.
//
// What the file holds is a credential, so this module keeps it out of
// everything that persists or prints: the file is created `0600` and refused
// when another user can read it; no token enters a memo key (an account is
// keyed by its address), a log line, an error message or the state database.

/// The Drive scope a sign-in asks for.
pub const DRIVE_READONLY_SCOPE: &str = DRIVE_SCOPE;

/// Bumped on an incompatible change to the token file's shape; an older file
/// means one sign-in again, never a migration. Shared with the Python SDK.
const TOKEN_FILE_VERSION: u64 = 1;

/// The whole interactive sign-in, including the person at the browser.
const SIGN_IN_TIMEOUT: Duration = Duration::from_secs(300);

/// How to create an OAuth client of one's own.
const OWN_CLIENT_DOCS: &str =
    "https://cocoindex.io/docs/connectors/google_drive#setting-up-the-oauth-client";

/// Google's OAuth and Drive endpoints. The default is Google; a test points
/// them at a mock server with [`UserAccount::with_endpoints`].
#[derive(Clone, Debug)]
pub struct GoogleEndpoints {
    pub authorization: String,
    pub token: String,
    pub revocation: String,
    /// Drive's `about.get`: the cheapest call that names the signed-in account
    /// and proves the Drive API is enabled for the client's project.
    pub about: String,
}

impl Default for GoogleEndpoints {
    fn default() -> Self {
        Self {
            authorization: "https://accounts.google.com/o/oauth2/v2/auth".to_string(),
            token: "https://oauth2.googleapis.com/token".to_string(),
            revocation: "https://oauth2.googleapis.com/revoke".to_string(),
            about: "https://www.googleapis.com/drive/v3/about".to_string(),
        }
    }
}

/// Where `UserAccount::new()` keeps its token: under the user's config directory.
pub fn default_token_path() -> Result<std::path::PathBuf> {
    let base = if cfg!(windows) {
        match std::env::var_os("APPDATA") {
            Some(appdata) => std::path::PathBuf::from(appdata),
            None => home_dir()?.join("AppData").join("Roaming"),
        }
    } else {
        match std::env::var("XDG_CONFIG_HOME") {
            Ok(xdg) if !xdg.trim().is_empty() => std::path::PathBuf::from(xdg),
            _ => home_dir()?.join(".config"),
        }
    };
    Ok(base.join("cocoindex").join("google_drive_token.json"))
}

fn home_dir() -> Result<std::path::PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(std::path::PathBuf::from)
        .ok_or_else(|| Error::engine("google drive: no home directory (HOME is not set)"))
}

/// What the token file holds. The Debug form names the account only.
#[derive(Clone, Serialize, Deserialize)]
struct Tokens {
    client_id: String,
    client_secret: String,
    refresh_token: String,
    access_token: String,
    /// POSIX seconds, as the Python SDK writes it.
    expires_at: f64,
    email: String,
}

impl std::fmt::Debug for Tokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tokens")
            .field("email", &self.email)
            .finish_non_exhaustive()
    }
}

#[derive(Serialize, Deserialize)]
struct TokenFile {
    version: u64,
    #[serde(flatten)]
    tokens: Tokens,
}

/// A Desktop-app OAuth client, as its downloaded JSON describes it.
#[derive(Clone)]
struct OAuthClient {
    client_id: String,
    client_secret: String,
}

/// Read the client JSON the Google Cloud console downloads for a Desktop app.
fn load_client(path: &std::path::Path) -> Result<OAuthClient> {
    let raw = std::fs::read_to_string(path).map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => {
            Error::engine(format!("No OAuth client file at {}.", path.display()))
        }
        _ => Error::engine(format!(
            "Cannot read the OAuth client file {}: {e}",
            path.display()
        )),
    })?;
    let document: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
        Error::engine(format!(
            "Cannot read the OAuth client file {}: {e}",
            path.display()
        ))
    })?;
    let Some(document) = document.as_object() else {
        return Err(Error::engine(format!(
            "{} is not an OAuth client file.",
            path.display()
        )));
    };
    if document.contains_key("web") && !document.contains_key("installed") {
        return Err(Error::engine(format!(
            "{} describes a Web application client; the sign-in needs a Desktop app \
             client (create one under APIs & Services > Credentials).",
            path.display()
        )));
    }
    let Some(installed) = document.get("installed").and_then(|v| v.as_object()) else {
        return Err(Error::engine(format!(
            "{} is not the JSON of a Desktop app OAuth client (it has no 'installed' section).",
            path.display()
        )));
    };
    match (
        installed.get("client_id").and_then(|v| v.as_str()),
        installed.get("client_secret").and_then(|v| v.as_str()),
    ) {
        (Some(client_id), Some(client_secret)) => Ok(OAuthClient {
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
        }),
        _ => Err(Error::engine(format!(
            "{} has no client_id and client_secret.",
            path.display()
        ))),
    }
}

// --- the token file ---------------------------------------------------------

/// Write `file` to `path` as JSON readable by this user only, atomically: the
/// file is created 0600 from the start (never written and then chmod'ed), in a
/// temp file beside the target that replaces it in one step.
fn write_private_json(path: &std::path::Path, file: &TokenFile) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(Error::Io)?;
    }
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let tmp = path.with_file_name(format!(".{name}.{}.tmp", std::process::id()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let write = (|| -> Result<()> {
        let handle = options.open(&tmp).map_err(Error::Io)?;
        serde_json::to_writer_pretty(&handle, file)
            .map_err(|e| Error::engine(format!("write the token file: {e}")))?;
        handle.sync_all().map_err(Error::Io)?;
        std::fs::rename(&tmp, path).map_err(Error::Io)
    })();
    if write.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    write
}

/// Refuse a token file another user can access (POSIX only: Windows has ACLs).
fn check_private(path: &std::path::Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)
            .map_err(Error::Io)?
            .permissions()
            .mode()
            & 0o777;
        if mode & 0o077 != 0 {
            return Err(Error::engine(format!(
                "The token file {} is accessible to other users (mode {mode:04o}); run \
                 `chmod 600` on it, or delete it and sign in again.",
                path.display()
            )));
        }
    }
    let _ = path;
    Ok(())
}

/// The stored tokens, or `None` when no usable sign-in is stored at `path`.
fn load_tokens(path: &std::path::Path) -> Result<Option<Tokens>> {
    if !path.exists() {
        return Ok(None);
    }
    check_private(path)?;
    let raw = std::fs::read_to_string(path).map_err(|e| {
        Error::engine(format!(
            "Cannot read the token file {}: {e}",
            path.display()
        ))
    })?;
    let document: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
        Error::engine(format!(
            "Cannot read the token file {}: {e}",
            path.display()
        ))
    })?;
    if document.get("version").and_then(|v| v.as_u64()) != Some(TOKEN_FILE_VERSION) {
        return Ok(None);
    }
    Ok(serde_json::from_value::<TokenFile>(document)
        .ok()
        .map(|f| f.tokens))
}

fn save_tokens(path: &std::path::Path, tokens: &Tokens) -> Result<()> {
    write_private_json(
        path,
        &TokenFile {
            version: TOKEN_FILE_VERSION,
            tokens: tokens.clone(),
        },
    )
}

// --- talking to Google ------------------------------------------------------

/// Google answered a token request with an OAuth error.
struct Refusal {
    code: String,
    description: String,
}

impl Refusal {
    fn detail(&self) -> String {
        if self.description.is_empty() {
            String::new()
        } else {
            format!(" ({})", self.description)
        }
    }
}

/// What the token endpoint answers when the client itself is the problem
/// (deleted, disabled, a wrong secret), not the user's grant.
fn is_client_refusal(code: &str) -> bool {
    matches!(code, "invalid_client" | "unauthorized_client")
}

/// POST `form` to the token endpoint; a refusal says what Google refused,
/// never what was sent.
async fn token_request(
    http: &reqwest::Client,
    token_url: &str,
    form: &[(&str, &str)],
) -> std::result::Result<serde_json::Value, TokenRequestError> {
    let response = http.post(token_url).form(form).send().await.map_err(|e| {
        TokenRequestError::Other(Error::engine(format!(
            "Cannot reach Google's token endpoint: {e}"
        )))
    })?;
    let status = response.status();
    let body: Option<serde_json::Value> = response.json().await.ok();
    if status.as_u16() >= 400 {
        let (code, description) = match body.as_ref().and_then(|b| b.as_object()) {
            Some(b) => (
                b.get("error")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("HTTP {status}")),
                b.get("error_description")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
            ),
            None => (format!("HTTP {status}"), String::new()),
        };
        return Err(TokenRequestError::Refused(Refusal { code, description }));
    }
    let Some(payload) = body else {
        return Err(TokenRequestError::Other(Error::engine(
            "Google's token endpoint returned no JSON.",
        )));
    };
    if payload
        .get("access_token")
        .and_then(|v| v.as_str())
        .is_none()
    {
        return Err(TokenRequestError::Other(Error::engine(
            "Google's token endpoint returned no access token.",
        )));
    }
    Ok(payload)
}

enum TokenRequestError {
    Refused(Refusal),
    Other(Error),
}

impl From<TokenRequestError> for Error {
    fn from(e: TokenRequestError) -> Self {
        match e {
            TokenRequestError::Refused(r) => Error::engine(format!(
                "Google refused the request: {}{}.",
                r.code,
                r.detail()
            )),
            TokenRequestError::Other(e) => e,
        }
    }
}

fn expires_at(payload: &serde_json::Value, now: f64) -> f64 {
    now + payload
        .get("expires_in")
        .and_then(|v| v.as_f64())
        .unwrap_or(3600.0)
}

fn posix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// The signed-in account's address, from the Drive API. Also the first call
/// that needs the Drive API enabled in the client's project, so its failure is
/// explained here.
async fn account_email(
    http: &reqwest::Client,
    about_url: &str,
    access_token: &str,
) -> Result<String> {
    let response = http
        .get(about_url)
        .query(&[("fields", "user(emailAddress)")])
        .bearer_auth(access_token)
        .send()
        .await
        .map_err(|e| Error::engine(format!("Cannot reach the Google Drive API: {e}")))?;
    let status = response.status().as_u16();
    if status == 403 {
        return Err(Error::engine(
            "The Google Drive API refused the signed-in account (HTTP 403). Enable the \
             Google Drive API for the OAuth client's project (APIs & Services > Library) \
             and sign in again.",
        ));
    }
    if status >= 400 {
        return Err(Error::engine(format!(
            "The Google Drive API refused the signed-in account (HTTP {status})."
        )));
    }
    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|_| Error::engine("The Google Drive API did not name the account."))?;
    match body
        .get("user")
        .and_then(|u| u.get("emailAddress"))
        .and_then(|v| v.as_str())
    {
        Some(email) if !email.is_empty() => Ok(email.to_string()),
        _ => Err(Error::engine(
            "The Google Drive API did not name the account.",
        )),
    }
}

/// A new access token from the refresh token. Google does not rotate refresh
/// tokens. An `invalid_grant` or a refused client means signing in again.
async fn refresh_tokens(
    http: &reqwest::Client,
    token_url: &str,
    tokens: &Tokens,
) -> Result<Tokens> {
    let payload = match token_request(
        http,
        token_url,
        &[
            ("grant_type", "refresh_token"),
            ("refresh_token", &tokens.refresh_token),
            ("client_id", &tokens.client_id),
            ("client_secret", &tokens.client_secret),
        ],
    )
    .await
    {
        Ok(payload) => payload,
        Err(TokenRequestError::Refused(r)) if r.code == "invalid_grant" => {
            return Err(Error::engine(format!(
                "Google refused the stored sign-in: invalid_grant{}. The consent may have \
                 expired (seven days for a client in Testing status), or the access was \
                 revoked. Sign in again.",
                r.detail()
            )));
        }
        Err(TokenRequestError::Refused(r)) if is_client_refusal(&r.code) => {
            return Err(Error::engine(format!(
                "Google no longer accepts the OAuth client this sign-in used: {}{}. It was \
                 deleted or disabled, or its secret changed. Sign in again with a working \
                 Desktop app client of your own.",
                r.code,
                r.detail()
            )));
        }
        Err(e) => return Err(e.into()),
    };
    let now = posix_now();
    Ok(Tokens {
        client_id: tokens.client_id.clone(),
        client_secret: tokens.client_secret.clone(),
        refresh_token: payload
            .get("refresh_token")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(&tokens.refresh_token)
            .to_string(),
        access_token: payload["access_token"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        expires_at: expires_at(&payload, now),
        email: tokens.email.clone(),
    })
}

/// Ask Google to revoke the refresh token; whether it accepted. Best effort.
async fn revoke(http: &reqwest::Client, revocation_url: &str, token: &str) -> bool {
    match http
        .post(revocation_url)
        .form(&[("token", token)])
        .send()
        .await
    {
        Ok(response) => response.status().as_u16() < 400,
        Err(_) => false,
    }
}

// --- the interactive flow ---------------------------------------------------

/// Whether a browser would show the sign-in page to the person at this
/// terminal. `BROWSER` set means they wired one up (VS Code's remote terminal
/// does, and forwards the loopback port with it). Over SSH, or on Linux with no
/// display, the host's browser would open on no screen they can see; the flow
/// then prints the link instead.
pub fn has_browser() -> bool {
    let env = |key: &str| std::env::var(key).ok().filter(|v| !v.is_empty());
    if env("BROWSER").is_some() {
        return true;
    }
    let displayed = env("DISPLAY").is_some() || env("WAYLAND_DISPLAY").is_some();
    if ["SSH_CONNECTION", "SSH_CLIENT", "SSH_TTY"]
        .iter()
        .any(|k| std::env::var_os(k).is_some())
    {
        return displayed;
    }
    if cfg!(target_os = "macos") || cfg!(windows) {
        return true;
    }
    displayed
}

/// Open `url` in the host's default browser, as Python's `webbrowser.open` does.
fn open_in_browser(url: &str) {
    let mut command = if cfg!(target_os = "macos") {
        std::process::Command::new("open")
    } else if cfg!(windows) {
        let mut c = std::process::Command::new("cmd");
        c.args(["/c", "start", ""]);
        c
    } else {
        std::process::Command::new("xdg-open")
    };
    let _ = command
        .arg(url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}

/// A callback that opens `url` for the person.
pub type OpenBrowserFn = Arc<dyn Fn(&str) + Send + Sync>;
/// A callback that reads one line the person typed, or `None` at end of input.
pub type ReadLineFn = Arc<dyn Fn() -> Option<String> + Send + Sync>;
/// A callback that shows the person a line they must act on.
pub type SayFn = Arc<dyn Fn(&str) + Send + Sync>;

/// How [`UserAccount::sign_in_with`] reaches the person and the host: each
/// default is the terminal and the host's browser, and a test replaces them.
#[derive(Clone)]
pub struct SignInOptions {
    /// Opens the authorization link; the host's default browser by default.
    pub open_browser: OpenBrowserFn,
    /// Reads a pasted redirect address; stdin by default.
    pub read_line: ReadLineFn,
    /// Shows instructions; stdout by default.
    pub say: SayFn,
    /// Whether a browser can show the sign-in page here; [`has_browser`]
    /// decides when `None`.
    pub browser_available: Option<bool>,
    /// How long to wait for the redirect, the person included.
    pub timeout: Duration,
}

impl Default for SignInOptions {
    fn default() -> Self {
        Self {
            open_browser: Arc::new(open_in_browser),
            read_line: Arc::new(|| {
                let mut line = String::new();
                match std::io::stdin().read_line(&mut line) {
                    Ok(0) | Err(_) => None,
                    Ok(_) => Some(line),
                }
            }),
            say: Arc::new(|message| println!("{message}")),
            browser_available: None,
            timeout: SIGN_IN_TIMEOUT,
        }
    }
}

fn url_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn url_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() + 0 && i + 2 <= bytes.len() - 1 => {
                match u8::from_str_radix(&value[i + 1..i + 3], 16) {
                    Ok(b) => {
                        out.push(b);
                        i += 2;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The parameters of a query string.
fn parse_query(query: &str) -> std::collections::BTreeMap<String, String> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (url_decode(k), url_decode(v))
        })
        .collect()
}

/// The query of a pasted redirect address, or `None` when the line is not one.
fn parse_pasted_redirect(text: &str) -> Option<std::collections::BTreeMap<String, String>> {
    let text = text.trim();
    let query = match text.split_once('?') {
        Some((_, query)) => query.split('#').next().unwrap_or(""),
        None if !text.contains("://") => text.trim_start_matches('?'),
        None => "",
    };
    let params = parse_query(query);
    (params.contains_key("code") || params.contains_key("error")).then_some(params)
}

fn instructions(url: &str, port: u16, browser_opened: bool) -> String {
    if browser_opened {
        return format!(
            "Finish the sign-in in the browser. If none opened, visit:\n\n  {url}\n\n\
             If the sign-in ends on a page that does not load, copy that page's address \
             from the address bar and paste it here. If Google shows an error page \
             instead of asking for consent, press Ctrl+C: the sign-in cannot finish."
        );
    }
    format!(
        "No browser can open on this host, so use one on another machine.\n\
         \n\
         1. Open this link there and sign in:\n\n  {url}\n\n   \
         The browser then lands on http://127.0.0.1:{port}/?code=...&state=... and fails to load:\n   \
         that address is the other machine, not this host.\n\
         \n   \
         If Google shows an error page instead of asking for consent (for\n   \
         example 'Access blocked'), the sign-in cannot finish here: press\n   \
         Ctrl+C. 'admin_policy_enforced' means a Google Workspace administrator\n   \
         blocks this app.\n\
         \n\
         2. Copy the whole address from the address bar and paste it here.\n   \
         (If instead the page says the sign-in completed, this port was \
         forwarded to this host and nothing is left to paste.)\n\
         \n\
         Paste the address here:"
    )
}

/// Answer one HTTP request on `socket`: the redirect the flow waits for (its
/// query, when it carries `code` or `error`); everything else is a 404.
async fn answer_redirect(
    mut socket: tokio::net::TcpStream,
) -> Option<std::collections::BTreeMap<String, String>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut head = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        let n = match socket.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        head.extend_from_slice(&buf[..n]);
        if head.windows(4).any(|w| w == b"\r\n\r\n") || head.len() > 64 * 1024 {
            break;
        }
    }
    let request_line = String::from_utf8_lossy(&head);
    let target = request_line
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");
    let params = target
        .split_once('?')
        .map(|(_, q)| parse_query(q))
        .unwrap_or_default();
    let found = params.contains_key("code") || params.contains_key("error");
    let (status, body) = if found {
        ("200 OK", "CocoIndex: signed in. You can close this tab.")
    } else {
        ("404 Not Found", "CocoIndex: waiting for Google's redirect.")
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = socket.write_all(response.as_bytes()).await;
    let _ = socket.shutdown().await;
    found.then_some(params)
}

/// Serve until the redirect arrives at the listener or is pasted, or time out.
/// stdin is read on a blocking thread because the read cannot be cancelled; a
/// pasted line that is not the redirect gets a hint and the wait goes on.
async fn await_redirect(
    listener: tokio::net::TcpListener,
    options: &SignInOptions,
) -> Result<std::collections::BTreeMap<String, String>> {
    let (tx, mut pasted) = tokio::sync::mpsc::unbounded_channel::<String>();
    let read_line = options.read_line.clone();
    std::thread::spawn(move || {
        while let Some(line) = read_line() {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    let deadline = tokio::time::sleep(options.timeout);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((socket, _)) = accepted else { continue };
                if let Some(params) = answer_redirect(socket).await {
                    return Ok(params);
                }
            }
            line = pasted.recv() => {
                let Some(line) = line else {
                    // stdin is closed: only the listener can end the wait now.
                    pasted = tokio::sync::mpsc::unbounded_channel().1;
                    continue;
                };
                if line.trim().is_empty() {
                    continue;
                }
                match parse_pasted_redirect(&line) {
                    Some(params) => return Ok(params),
                    None => (options.say)(
                        "That is not the address the sign-in ended on; it starts with \
                         http://127.0.0.1:... and contains code= and state=. Paste the whole address:",
                    ),
                }
            }
            _ = &mut deadline => {
                return Err(Error::engine(
                    "The sign-in timed out before Google redirected back.",
                ));
            }
        }
    }
}

fn random_urlsafe(bytes: usize) -> String {
    let mut raw = Vec::with_capacity(bytes);
    while raw.len() < bytes {
        raw.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    }
    raw.truncate(bytes);
    URL_SAFE_NO_PAD.encode(raw)
}

/// Compare two strings in time independent of where they differ.
fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut diff = (a.len() ^ b.len()) as u8;
    for i in 0..a.len().max(b.len()) {
        diff |= a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0);
    }
    diff == 0
}

/// The authorization code grant with PKCE over a loopback redirect; the token
/// response.
async fn run_consent(
    client_cfg: &OAuthClient,
    endpoints: &GoogleEndpoints,
    http: &reqwest::Client,
    options: &SignInOptions,
) -> Result<serde_json::Value> {
    use sha2::Digest;
    let verifier = random_urlsafe(64);
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let state = random_urlsafe(32);
    // Bound before the browser opens, so the redirect URI names a port this
    // process already holds.
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .map_err(Error::Io)?;
    let port = listener.local_addr().map_err(Error::Io)?.port();
    let redirect_uri = format!("http://127.0.0.1:{port}/");
    let query = [
        ("response_type", "code"),
        ("client_id", client_cfg.client_id.as_str()),
        ("redirect_uri", redirect_uri.as_str()),
        ("scope", DRIVE_READONLY_SCOPE),
        ("state", state.as_str()),
        ("code_challenge", challenge.as_str()),
        ("code_challenge_method", "S256"),
        // A refresh token on every sign-in, not only the first consent.
        ("access_type", "offline"),
        ("prompt", "consent"),
    ]
    .iter()
    .map(|(k, v)| format!("{}={}", url_encode(k), url_encode(v)))
    .collect::<Vec<_>>()
    .join("&");
    let url = format!("{}?{query}", endpoints.authorization);
    let browser_available = options.browser_available.unwrap_or_else(has_browser);
    if browser_available {
        (options.open_browser)(&url);
    }
    (options.say)(&instructions(&url, port, browser_available));
    let params = await_redirect(listener, options).await?;
    drop(url);

    if let Some(error) = params.get("error") {
        if error == "access_denied" {
            return Err(Error::engine(
                "The sign-in was cancelled on the consent screen.",
            ));
        }
        return Err(Error::engine(format!(
            "Google refused the sign-in: {error}."
        )));
    }
    if !constant_time_eq(
        params.get("state").map(String::as_str).unwrap_or(""),
        &state,
    ) {
        return Err(Error::engine(
            "The redirect does not belong to this sign-in (its state does not match); \
             nothing was stored. Start the sign-in again.",
        ));
    }
    let code = params.get("code").map(String::as_str).unwrap_or("");
    if code.is_empty() {
        return Err(Error::engine(
            "Google's redirect carried no authorization code.",
        ));
    }
    match token_request(
        http,
        &endpoints.token,
        &[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("code_verifier", &verifier),
            ("client_id", &client_cfg.client_id),
            ("client_secret", &client_cfg.client_secret),
            ("redirect_uri", &redirect_uri),
        ],
    )
    .await
    {
        Ok(payload) => Ok(payload),
        Err(TokenRequestError::Refused(r)) if is_client_refusal(&r.code) => {
            Err(Error::engine(format!(
                "Google refused the OAuth client in the client file: {}. Download the \
                 current JSON of the Desktop app client (its secret may have been reset) \
                 and sign in again (see {OWN_CLIENT_DOCS}).",
                r.code
            )))
        }
        Err(e) => Err(e.into()),
    }
}

// --- the public handle -------------------------------------------------------

/// A personal Google account signed in with OAuth, kept in a token file the
/// Python SDK reads and writes too.
///
/// Experimental. [`sign_in`](Self::sign_in) is the interactive step; run it once,
/// outside any CocoIndex run (`cocoindex google-drive sign-in` in the Python
/// CLI does the same from the shell). A run then builds a
/// [`GoogleDriveClient::from_user_account`], which reads the file and errors
/// when it holds no usable sign-in.
#[derive(Clone, Debug)]
pub struct UserAccount {
    token_path: std::path::PathBuf,
    endpoints: GoogleEndpoints,
}

impl UserAccount {
    /// The account whose token file is at [`default_token_path`].
    pub fn new() -> Result<Self> {
        Ok(Self::with_token_path(default_token_path()?))
    }

    /// The account whose token file is at `token_path`.
    pub fn with_token_path(token_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            token_path: token_path.into(),
            endpoints: GoogleEndpoints::default(),
        }
    }

    /// Point the sign-in, refresh and revocation at other endpoints (a mock
    /// server in tests).
    pub fn with_endpoints(mut self, endpoints: GoogleEndpoints) -> Self {
        self.endpoints = endpoints;
        self
    }

    pub fn token_path(&self) -> &std::path::Path {
        &self.token_path
    }

    /// Sign in interactively with a Desktop-app OAuth client; the account's
    /// address. `client_secrets_path` is the JSON downloaded for a client of
    /// your own (see the docs linked from the error when it is unusable).
    /// Opens the consent page in the browser, or prints the link when this
    /// host has none, and stores the tokens readable by this user only.
    pub async fn sign_in(
        &self,
        client_secrets_path: impl AsRef<std::path::Path>,
    ) -> Result<String> {
        self.sign_in_with(client_secrets_path, SignInOptions::default())
            .await
    }

    /// [`sign_in`](Self::sign_in) with the person and the host behind
    /// [`SignInOptions`], so a test can drive the flow against fake endpoints.
    pub async fn sign_in_with(
        &self,
        client_secrets_path: impl AsRef<std::path::Path>,
        options: SignInOptions,
    ) -> Result<String> {
        let client_cfg = load_client(client_secrets_path.as_ref())?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| Error::engine(format!("http client: {e}")))?;
        let payload = run_consent(&client_cfg, &self.endpoints, &http, &options).await?;
        let granted: Vec<&str> = payload
            .get("scope")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .split_whitespace()
            .collect();
        if !granted.is_empty() && !granted.contains(&DRIVE_READONLY_SCOPE) {
            return Err(Error::engine(
                "The consent did not grant read access to Google Drive; sign in again and \
                 keep the Drive permission selected.",
            ));
        }
        let Some(refresh_token) = payload
            .get("refresh_token")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        else {
            return Err(Error::engine(
                "Google returned no refresh token, so the sign-in could not be kept. Remove \
                 the app at https://myaccount.google.com/permissions and sign in again.",
            ));
        };
        let now = posix_now();
        let access_token = payload["access_token"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let email = account_email(&http, &self.endpoints.about, &access_token).await?;
        let tokens = Tokens {
            client_id: client_cfg.client_id,
            client_secret: client_cfg.client_secret,
            refresh_token: refresh_token.to_string(),
            access_token,
            expires_at: expires_at(&payload, now),
            email,
        };
        save_tokens(&self.token_path, &tokens)?;
        Ok(tokens.email)
    }

    /// Revoke the stored sign-in at Google and delete the token file; whether
    /// one existed. The file is deleted even when Google could not be reached:
    /// the stored tokens must not outlive the sign-out.
    pub async fn sign_out(&self) -> Result<bool> {
        if let Some(tokens) = load_tokens(&self.token_path)? {
            let http = reqwest::Client::new();
            if !revoke(&http, &self.endpoints.revocation, &tokens.refresh_token).await {
                tracing::warn!(
                    "Google did not confirm revoking the sign-in of {}; the token file is \
                     deleted anyway. To be sure, remove the app at \
                     https://myaccount.google.com/permissions.",
                    tokens.email
                );
            }
        }
        let existed = self.token_path.exists();
        match std::fs::remove_file(&self.token_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(Error::Io(e)),
        }
        Ok(existed)
    }

    /// The signed-in account's address; an error when none is signed in.
    pub fn email(&self) -> Result<String> {
        Ok(self.tokens()?.email)
    }

    pub fn is_signed_in(&self) -> bool {
        matches!(load_tokens(&self.token_path), Ok(Some(_)))
    }

    fn tokens(&self) -> Result<Tokens> {
        load_tokens(&self.token_path)?.ok_or_else(|| {
            Error::engine(format!(
                "No usable Google sign-in is stored at {}. Sign in once with \
                 `UserAccount::sign_in(client_secret.json)` or \
                 `cocoindex google-drive sign-in <client_secret.json>`.",
                self.token_path.display()
            ))
        })
    }
}

/// The credential a [`GoogleDriveClient`] built from a [`UserAccount`] signs
/// with: the tokens, refreshed as they expire and written back to the file so
/// the next process starts without a refresh.
struct UserCredentials {
    token_path: std::path::PathBuf,
    token_url: String,
    tokens: tokio::sync::Mutex<Tokens>,
}

impl UserCredentials {
    async fn access_token(&self, http: &reqwest::Client) -> Result<String> {
        let mut tokens = self.tokens.lock().await;
        if tokens.expires_at > posix_now() + 60.0 {
            return Ok(tokens.access_token.clone());
        }
        let refreshed = refresh_tokens(http, &self.token_url, &tokens).await?;
        save_tokens(&self.token_path, &refreshed)?;
        *tokens = refreshed;
        Ok(tokens.access_token.clone())
    }
}

// ---------------------------------------------------------------------------
// GoogleDriveSource
// ---------------------------------------------------------------------------

/// A Google Drive source: lists files (recursively) under a set of root folders.
#[derive(Clone)]
pub struct GoogleDriveSource {
    client: GoogleDriveClient,
    root_folder_ids: Vec<String>,
    mime_types: Option<Vec<String>>,
}

impl GoogleDriveSource {
    /// Create a source reading from the given root folder ids.
    pub fn new(client: GoogleDriveClient, root_folder_ids: Vec<String>) -> Self {
        Self {
            client,
            root_folder_ids,
            mime_types: None,
        }
    }

    /// Restrict listing to these MIME types (folders are always traversed).
    pub fn mime_types(mut self, mime_types: Vec<String>) -> Self {
        self.mime_types = Some(mime_types);
        self
    }

    /// The underlying client (e.g. to provide via a `ContextKey`).
    pub fn client(&self) -> &GoogleDriveClient {
        &self.client
    }

    /// List all (non-folder) files under the root folders, recursing into
    /// subfolders (breadth-first). Honors the optional MIME-type filter.
    pub async fn list_files(&self) -> Result<Vec<DriveFile>> {
        let mut pending: std::collections::VecDeque<(String, String)> = self
            .root_folder_ids
            .iter()
            .cloned()
            .map(|id| (id, String::new()))
            .collect();
        let mut seen_folders = std::collections::HashSet::new();
        let mut seen_files = std::collections::HashSet::new();
        let mut out = Vec::new();
        while let Some((folder_id, prefix)) = pending.pop_front() {
            if !seen_folders.insert(folder_id.clone()) {
                continue; // guard against cycles / shortcuts
            }
            for mut file in self.client.list_children(&folder_id).await? {
                let path = if prefix.is_empty() {
                    file.name.clone()
                } else {
                    format!("{prefix}/{}", file.name)
                };
                if file.is_folder() {
                    pending.push_back((file.file_id, path));
                    continue;
                }
                if let Some(allowed) = &self.mime_types
                    && !allowed.iter().any(|m| m == &file.mime_type)
                {
                    continue;
                }
                if !seen_files.insert(file.file_id.clone()) {
                    continue;
                }
                file.path = path;
                out.push(file.with_client(self.client.clone()));
            }
        }
        Ok(out)
    }

    pub async fn items(&self) -> Result<Vec<(String, DriveFile)>> {
        Ok(self
            .list_files()
            .await?
            .into_iter()
            .map(|file| (file.key(), file))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_mime_mapping() {
        assert_eq!(
            export_mime_for("application/vnd.google-apps.document"),
            Some("text/markdown")
        );
        assert_eq!(
            export_mime_for("application/vnd.google-apps.spreadsheet"),
            Some("text/csv")
        );
        assert_eq!(
            export_mime_for("application/vnd.google-apps.presentation"),
            Some("text/plain")
        );
        // Binary files have no export mapping (downloaded via alt=media).
        assert_eq!(export_mime_for("text/plain"), None);
        assert_eq!(export_mime_for("application/pdf"), None);
    }

    #[test]
    fn parse_list_extracts_files_and_token() {
        let json = serde_json::json!({
            "nextPageToken": "tok123",
            "files": [
                {"id": "a", "name": "doc.md", "mimeType": "text/markdown", "size": "42", "modifiedTime": "2024-01-01T00:00:00Z"},
                {"id": "b", "name": "sub", "mimeType": FOLDER_MIME, "modifiedTime": "2024-01-02T00:00:00Z"},
            ]
        });
        let (files, next) = parse_file_list(&json).unwrap();
        assert_eq!(next.as_deref(), Some("tok123"));
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].file_id, "a");
        assert_eq!(files[0].size, 42);
        assert_eq!(files[0].key(), "a");
        assert_eq!(files[0].path(), "doc.md");
        assert!(!files[0].is_folder());
        assert_eq!(files[1].size, 0); // folder omits size
        assert!(files[1].is_folder());
    }

    #[test]
    fn same_named_files_have_distinct_mount_keys() {
        let info = DriveFileInfo {
            file_id: "id-1".into(),
            name: "report.txt".into(),
            path: "folder/report.txt".into(),
            mime_type: "text/plain".into(),
            size: 0,
            modified_time: "2024-01-01T00:00:00Z".into(),
            md5_checksum: None,
        };
        let first = DriveFile::new(info.clone());
        let second = DriveFile::new(DriveFileInfo {
            file_id: "id-2".into(),
            ..info
        });
        assert_eq!(first.path(), second.path());
        assert_ne!(first.key(), second.key());

        let mut renamed = first.clone();
        renamed.name = "renamed.txt".into();
        renamed.path = "folder/renamed.txt".into();
        assert_eq!(first.key(), renamed.key());
    }

    #[test]
    fn parse_list_no_token_and_empty() {
        let (files, next) = parse_file_list(&serde_json::json!({"files": []})).unwrap();
        assert!(files.is_empty());
        assert_eq!(next, None);
        let (files, next) = parse_file_list(&serde_json::json!({})).unwrap();
        assert!(files.is_empty());
        assert_eq!(next, None);
    }

    #[test]
    fn folder_id_validation_rejects_injection() {
        assert!(validate_folder_id("1jn2OcXSviN8I2C2tSlxLlByW4twPLu7s").is_ok());
        assert!(validate_folder_id("abc-DEF_123").is_ok());
        assert!(validate_folder_id("").is_err());
        assert!(validate_folder_id("' in parents or '1' in parents").is_err());
        assert!(validate_folder_id("a'b").is_err());
    }

    #[test]
    fn file_id_validation_rejects_path_injection() {
        assert!(validate_drive_id("Google Drive file id", "file-ABC_123").is_ok());
        assert!(validate_drive_id("Google Drive file id", "../secret").is_err());
        assert!(validate_drive_id("Google Drive file id", "a/b").is_err());
    }

    #[test]
    fn base_url_override_is_safe_after_clone() {
        let client = GoogleDriveClient::from_static_token("test-token");
        let cloned = client.clone();
        let _ = cloned.with_base_url("http://localhost:1234/");
    }

    // A throwaway 2048-bit RSA key (PKCS#8 PEM), test-only — never used anywhere
    // else. Lets us exercise RS256 JWT signing without network or key generation.
    const TEST_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDoVeOAMVtE119k
5QWt+C/m/8tYT9RSE7rPC85KxWD0rTKAgzTj90XmoAff/eNoovluDeqIoqSDPxgO
nFxyPMb+twO0Qiu2cS+a39pubffFi4VBDkbEoOP6zy/2sv08UvNhUIUvc1lNldJf
wafpR8lwcLuWYSDiYUezndXUSxe/aoOcZblZo2rSGcLHqImWwXrTb/w1iodmgsI0
dgjbw4D3GH2hwglW9NtUIz6LimVTZrdG5HZ/AxBjJkMylY2Hn19gvQaMCLNIycVa
W97Uaa3nm2t/v3zkJ6FG0A6PatCQWTVxdltaLdYm9LXjo65TytR/kAc0T/jbXOAa
GTXRE6sNAgMBAAECggEBANwbwpRILjdh8zYa4u6WGou1+meH+ZZoqqpMfPvJUrl6
/EVUCF+Qe+Cp68wBM9iFzdi9xlv7+e99bsUozUxM2BmoORIlPlRxlrAbM007UWkN
bQjdBZ5y7olGkCIgIFluHLUtG4CAvzIJpmyhgvo20Fh99Lna+tR9ZPh9p36gRbdI
250BAPCz5tXsKz+1gEljvTffeXEGz9OgNcNmlzp0qE9kcLtBVIwp852k4VPt3k/r
pn7N+UC2VYKZhaU+H9p9p1z3QzxC6zXlx7pFxRnyIuALhjILU+cbMH2CDdpEiuGb
hqcXyKbML7FDXf1B5TUgHjit7DgN/vEnvRQo4/pAJOECgYEA9IljhxfGcmMXx76l
ByEIqgoU1B6bWB2X3MvUnFLWbgN4TS9C0JLUflT8KYfq8LRHxGtI19o7n2tJYOah
RuMch3viKGe+C1/cJRe5UTASY84koWck8lXroxuToIaTe+d3rBnfpp4VQS0QY4yM
l+31HQc1Rg4nR/JzFk1SvHQ4ACMCgYEA8zoTyTl/dWhuZh95RISMfSdP76ZBSC3F
OJVGX5E47dv9E2NqA06bAQjyge3BwEydwFBkEm2QKj5bUO9ZOS6ZMydkaovXLRGY
XlJ6chE6o2HOxNQWLdtCU+UqG+p8El3Yf1Y1smYMz/bxjUhYnpDTbUhX9qjnczKT
PeSADLIbww8CgYEA2B2UGJCqke2B1sZmkyZeweim/9EM+ZMt47VA8edEG3Z1m8Fp
C2y43+277fhxasnpo24tspbsmrf24ezyG/QcAqE5/vuwudy+cwnEfjw+BHbraLn/
rSzCVCTLE9PcBGVNHoy/XEHaBwAMu+47Uwq61izIqGFZ1fwwOkWcGXGdDIECgYEA
uTRdAplsq4sUnWCT54+Cpn37yUDwbrSje113U6fyEHS1tUC65b/CGbylZDgVk4cD
i//q43lYEEKhJ/TJHNiVwTTaqqLG+0NtoUzufdMOsn/0gT35kXtmexmBwfX/+cBJ
7VRI2QoJ8YVZEzqmeD9RLuKqUGD2tGorYjKPKpuothMCgYBkcXP0Cf7cfm5m3RT9
SMcGxsz4/epl3xeIABu/smUG26zYihigQSViDFfie/uWCvUeXCfbb6kfSvFqrj3m
rM3JvEZzqXNr6Lj+F9QXa33DkuhUbV+uxvy1MRfuu3OTCjgN6jgQvwj/zZQXhKnt
QYHyry6fOlqFrfEVtG39i5q60w==
-----END PRIVATE KEY-----";

    #[test]
    fn jwt_has_three_parts_and_signs() {
        let jwt = make_jwt(
            "svc@example.com",
            TEST_KEY_PEM,
            "https://oauth2.googleapis.com/token",
            DRIVE_SCOPE,
            1_700_000_000,
        )
        .unwrap();
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);
        // Header + claims decode to the expected JSON.
        let header: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
        assert_eq!(header["alg"], "RS256");
        let claims: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        assert_eq!(claims["iss"], "svc@example.com");
        assert_eq!(claims["scope"], DRIVE_SCOPE);
        assert_eq!(claims["exp"], 1_700_000_000u64 + 3600);
    }
}
