//! Google Drive source connector.
//!
//! [`GoogleDriveSource::list_files`] recursively lists files under configured
//! root folders. Use each [`DriveFile::key`] with `Ctx::mount_each` so per-file
//! memoization handles edits and target reconciliation removes derived rows for
//! deleted files.
//!
//! The connector supports service-account auth, optional MIME-type filtering,
//! Google-native export formats, and binary downloads for non-native files.
//!
//! No C build dependencies: the service-account JWT is signed (RS256) with the
//! pure-Rust `rsa`/`sha2` crates, and HTTP uses `reqwest`. The Drive base URL and
//! auth are pluggable so the connector can be driven against a mock server.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rsa::RsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::signature::{SignatureEncoding, Signer};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::error::{Error, Result};
use crate::file::{
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
    /// Path relative to the configured Drive root folder. `key()` uses this
    /// path, matching Python's Google Drive source item keys.
    #[serde(default)]
    pub path: String,
    pub mime_type: String,
    pub size: u64,
    /// RFC 3339 timestamp string as returned by the Drive API.
    pub modified_time: String,
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
            content_fingerprint: None,
        };
        Self {
            file_id: info.file_id,
            name: info.name,
            path: info.path,
            mime_type: info.mime_type,
            size: info.size,
            modified_time: info.modified_time,
            client,
            cache: Arc::new(FileContentCache::with_metadata(metadata)),
        }
    }

    pub fn with_client(mut self, client: GoogleDriveClient) -> Self {
        self.client = Some(client);
        self
    }

    /// Stable key for `mount_each`, matching Python's Google Drive source:
    /// the file's path under the configured Drive root.
    pub fn key(&self) -> String {
        self.path().to_string()
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
            content_fingerprint: None,
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

fn default_file_cache() -> Arc<FileContentCache> {
    Arc::new(FileContentCache::new())
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
            files.push(DriveFile::new(DriveFileInfo {
                file_id: file_id.clone(),
                name,
                path: String::new(),
                mime_type,
                size,
                modified_time,
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
            Auth::ServiceAccount {
                client_email,
                private_key,
                token_uri,
            } => {
                let mut guard = self.inner.token_cache.lock().await;
                if let Some(cached) = guard.as_ref() {
                    if cached.expires_at > Instant::now() + Duration::from_secs(60) {
                        return Ok(cached.token.clone());
                    }
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
                    "nextPageToken, files(id, name, mimeType, size, modifiedTime)",
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
                if let Some(allowed) = &self.mime_types {
                    if !allowed.iter().any(|m| m == &file.mime_type) {
                        continue;
                    }
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
        assert_eq!(files[0].key(), "doc.md");
        assert_eq!(files[0].path(), "doc.md");
        assert!(!files[0].is_folder());
        assert_eq!(files[1].size, 0); // folder omits size
        assert!(files[1].is_folder());
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
