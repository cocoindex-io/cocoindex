//! Amazon S3 (and S3-compatible, e.g. MinIO) object source connector.
//!
//! Read-only listing and reading of S3 objects, mirroring Python's
//! `cocoindex.connectors.amazon_s3`:
//!
//! - [`S3Client`] — a clone-cheap connection handle (`connect` reads standard
//!   AWS env: region/credentials and `AWS_ENDPOINT_URL` for MinIO).
//! - [`list_objects`] returns an [`S3Walker`]; [`S3Walker::list`] /
//!   [`S3Walker::items`] enumerate matching objects as [`S3File`]s. Use each
//!   [`S3File::key`] with `Ctx::mount_each` so per-file memoization handles edits
//!   and target reconciliation removes derived rows for deleted objects.
//! - [`S3Client::get_object`] fetches a single object's metadata; reads go
//!   through [`S3File::read`] / [`S3File::read_text`] or the compatibility
//!   methods on [`S3Client`].
//!
//! Like the Google Drive source, [`S3File`] serializes only stable metadata for
//! memo keys. The clone-cheap client and read cache are skipped by serde, while
//! the public type still implements the shared async [`crate::file::FileLike`]
//! trait.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use aws_sdk_s3::Client;
use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};

/// Re-export of the upstream [`aws_sdk_s3`] crate. The bucket and its objects are
/// user-managed (the source is read-only), so callers use this — together with
/// [`S3Client::client`] — to create/manage buckets and upload objects without
/// depending on `aws-sdk-s3` directly.
pub use aws_sdk_s3;

use crate::error::{Error, Result};
use crate::file::{
    FileContentCache, FileLike, FileMetadata, FilePath, FilePathMatcher, FileSourceItem,
    MatchAllFilePathMatcher, decode_bytes,
};

// ---------------------------------------------------------------------------
// S3FilePath / S3File — source items (Serialize so `mount_each` + memo work)
// ---------------------------------------------------------------------------

/// Path of an S3 object: the bucket, the full object key, and the path relative
/// to the walker prefix. Mirrors Python's `S3FilePath`; its memo key includes
/// the bucket so the same relative path in two buckets stays distinct.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct S3FilePath {
    bucket: String,
    /// Path relative to the walker prefix (forward slashes), or the full key if
    /// no prefix was used. This is the user-facing display path / `mount_each` key.
    relative_path: String,
    /// Full S3 object key (what [`resolve`](Self::resolve) returns).
    object_key: String,
}

impl S3FilePath {
    /// The S3 bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Path relative to the walker prefix (forward slashes).
    pub fn path(&self) -> &str {
        &self.relative_path
    }

    /// The full S3 object key.
    pub fn resolve(&self) -> &str {
        &self.object_key
    }

    /// Memo key: `(bucket, relative_path)`. Stable across buckets and decoupled
    /// from the live connection (matches Python's `__coco_memo_key__`).
    pub fn memo_key(&self) -> impl Serialize + '_ {
        (&self.bucket, &self.relative_path)
    }
}

/// A file discovered in or fetched from an S3 bucket.
///
/// Files returned by [`S3Walker`] carry a clone-cheap client handle so they can
/// be read through the shared async [`FileLike`] API.
#[derive(Clone, Serialize, Deserialize)]
pub struct S3File {
    file_path: S3FilePath,
    /// Object size in bytes.
    pub size: u64,
    /// Last-modified time as Unix seconds (`None` if the server omitted it).
    pub modified_secs: Option<i64>,
    /// S3 ETag (content fingerprint), if returned.
    pub etag: Option<String>,
    #[serde(skip)]
    client: Option<S3Client>,
    #[serde(skip, default = "default_file_cache")]
    cache: Arc<FileContentCache>,
}

impl S3File {
    fn new(
        client: Option<S3Client>,
        file_path: S3FilePath,
        size: u64,
        modified_secs: Option<i64>,
        etag: Option<String>,
    ) -> Self {
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

    pub fn with_client(mut self, client: S3Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Stable key for `Ctx::mount_each`: the object's path relative to the walker
    /// prefix. Unique within a bucket+prefix.
    pub fn key(&self) -> String {
        self.file_path.relative_path.clone()
    }

    /// The object's [`S3FilePath`].
    pub fn file_path(&self) -> &S3FilePath {
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

impl std::fmt::Debug for S3File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3File")
            .field("file_path", &self.file_path)
            .field("size", &self.size)
            .field("modified_secs", &self.modified_secs)
            .field("etag", &self.etag)
            .finish()
    }
}

impl PartialEq for S3File {
    fn eq(&self, other: &Self) -> bool {
        self.file_path == other.file_path
            && self.size == other.size
            && self.modified_secs == other.modified_secs
            && self.etag == other.etag
    }
}

impl Eq for S3File {}

#[async_trait]
impl FileLike for S3File {
    fn file_path(&self) -> FilePath {
        FilePath::with_base_dir(
            format!("s3://{}", self.file_path.bucket),
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
            .ok_or_else(|| Error::engine("S3 file is not attached to an S3Client"))?;
        match size {
            Some(0) => Ok(Vec::new()),
            Some(size) => client.read_range(self, size as u64).await,
            None => {
                client
                    .read_impl(&self.file_path.bucket, &self.file_path.object_key, None)
                    .await
            }
        }
    }
}

impl FileSourceItem for S3File {}

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

// ---------------------------------------------------------------------------
// S3Client — connection handle (Clone-cheap)
// ---------------------------------------------------------------------------

/// An Amazon S3 connection. Clone-cheap (the underlying client is shared).
#[derive(Clone)]
pub struct S3Client {
    inner: Arc<Client>,
    state_id: Arc<str>,
}

impl S3Client {
    /// Build a client from the standard AWS environment (region, credentials, and
    /// `AWS_ENDPOINT_URL`). When `AWS_ENDPOINT_URL` is set (e.g. for MinIO),
    /// path-style addressing is enabled automatically.
    pub async fn connect() -> Result<Self> {
        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .load()
            .await;
        let mut builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        let endpoint = std::env::var("AWS_ENDPOINT_URL")
            .ok()
            .filter(|s| !s.is_empty());
        if let Some(endpoint) = &endpoint {
            builder = builder.endpoint_url(endpoint).force_path_style(true);
        }
        let region = sdk_config
            .region()
            .map(|r| r.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        // Stable identity (endpoint for S3-compatible, else region) — used as a
        // ContextKey state id / memo dependency, never the credentials.
        let state_id = endpoint.unwrap_or_else(|| format!("aws-s3:{region}"));
        Ok(Self {
            inner: Arc::new(Client::from_conf(builder.build())),
            state_id: Arc::from(state_id),
        })
    }

    /// Wrap an already-built S3 client (advanced use / tests). `state_id` is the
    /// stable identity used for memo dependencies.
    pub fn from_client(state_id: impl Into<String>, client: Client) -> Self {
        Self {
            inner: Arc::new(client),
            state_id: Arc::from(state_id.into()),
        }
    }

    /// The underlying `aws_sdk_s3::Client`.
    pub fn client(&self) -> &Client {
        &self.inner
    }

    /// Stable identity (for use as a `ContextKey` state id / memo dependency).
    pub fn state_id(&self) -> &str {
        &self.state_id
    }

    /// Fetch a single object's metadata as an [`S3File`] (via `head_object`).
    /// Its relative path equals the full key. Errors if the object doesn't exist.
    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<S3File> {
        let head = self
            .inner
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| Error::engine(format!("s3 head_object {bucket}/{key}: {}", sdk_err(e))))?;
        Ok(S3File::new(
            Some(self.clone()),
            S3FilePath {
                bucket: bucket.to_string(),
                relative_path: key.to_string(),
                object_key: key.to_string(),
            },
            head.content_length().unwrap_or(0).max(0) as u64,
            head.last_modified().map(|d| d.secs()),
            head.e_tag().map(str::to_string),
        ))
    }

    /// [`get_object`](Self::get_object) addressed by an `s3://bucket/key` URI.
    pub async fn get_object_uri(&self, uri: &str) -> Result<S3File> {
        let (bucket, key) = parse_s3_uri(uri)?;
        self.get_object(&bucket, &key).await
    }

    /// Read an object's full content.
    pub async fn read(&self, file: &S3File) -> Result<Vec<u8>> {
        self.read_impl(&file.file_path.bucket, &file.file_path.object_key, None)
            .await
    }

    /// Read the first `len` bytes of an object (a ranged `get_object`).
    pub async fn read_range(&self, file: &S3File, len: u64) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let range = (len > 0).then(|| format!("bytes=0-{}", len - 1));
        self.read_impl(&file.file_path.bucket, &file.file_path.object_key, range)
            .await
    }

    /// Read an object's content and decode it as UTF-8 (BOM-aware, lossy on
    /// invalid UTF-8).
    pub async fn read_text(&self, file: &S3File) -> Result<String> {
        Ok(decode_bytes(&self.read(file).await?))
    }

    async fn read_impl(&self, bucket: &str, key: &str, range: Option<String>) -> Result<Vec<u8>> {
        let mut req = self.inner.get_object().bucket(bucket).key(key);
        if let Some(range) = range {
            req = req.range(range);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| Error::engine(format!("s3 get_object {bucket}/{key}: {}", sdk_err(e))))?;
        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| Error::engine(format!("s3 read body {bucket}/{key}: {e}")))?;
        Ok(data.into_bytes().to_vec())
    }
}

/// Format an AWS SDK error into a message that includes its source chain (the
/// `SdkError` `Display` alone is intentionally terse).
fn sdk_err<E, R>(err: aws_sdk_s3::error::SdkError<E, R>) -> String
where
    aws_sdk_s3::error::SdkError<E, R>: std::error::Error,
{
    let mut msg = err.to_string();
    let mut source = std::error::Error::source(&err);
    while let Some(e) = source {
        msg.push_str(&format!(": {e}"));
        source = e.source();
    }
    msg
}

/// Parse an `s3://bucket/key` URI into *(bucket, key)*.
fn parse_s3_uri(uri: &str) -> Result<(String, String)> {
    let rest = uri.strip_prefix("s3://").ok_or_else(|| {
        Error::engine(format!(
            "invalid S3 URI {uri:?}: expected 's3://bucket/key'"
        ))
    })?;
    match rest.split_once('/') {
        Some((bucket, key)) if !bucket.is_empty() && !key.is_empty() => {
            Ok((bucket.to_string(), key.to_string()))
        }
        _ => Err(Error::engine(format!(
            "invalid S3 URI {uri:?}: expected 's3://bucket/key'"
        ))),
    }
}

// ---------------------------------------------------------------------------
// S3Walker — list a bucket/prefix into S3Files
// ---------------------------------------------------------------------------

/// Options for [`list_objects`].
#[derive(Default)]
pub struct ListOptions {
    /// Only list objects whose key starts with this prefix.
    pub prefix: String,
    /// Filter by relative path (after prefix stripping). Defaults to match-all.
    pub path_matcher: Option<Arc<dyn FilePathMatcher>>,
    /// Skip objects larger than this many bytes.
    pub max_file_size: Option<u64>,
}

/// Lists objects in an S3 bucket as [`S3File`]s. Build with [`list_objects`].
pub struct S3Walker {
    client: S3Client,
    bucket: String,
    prefix: String,
    path_matcher: Arc<dyn FilePathMatcher>,
    max_file_size: Option<u64>,
}

/// List objects in an S3 bucket. Returns an [`S3Walker`]; call
/// [`S3Walker::list`] or [`S3Walker::items`] to enumerate matching objects.
pub fn list_objects(
    client: &S3Client,
    bucket: impl Into<String>,
    options: ListOptions,
) -> S3Walker {
    S3Walker {
        client: client.clone(),
        bucket: bucket.into(),
        prefix: options.prefix,
        path_matcher: options
            .path_matcher
            .unwrap_or_else(|| Arc::new(MatchAllFilePathMatcher)),
        max_file_size: options.max_file_size,
    }
}

impl S3Walker {
    /// List all matching objects (paginating through the bucket), skipping
    /// directory markers, applying the prefix, path matcher, and max-size filter.
    pub async fn list(&self) -> Result<Vec<S3File>> {
        let mut out = Vec::new();
        let mut continuation: Option<String> = None;
        loop {
            let mut req = self.client.inner.list_objects_v2().bucket(&self.bucket);
            if !self.prefix.is_empty() {
                req = req.prefix(&self.prefix);
            }
            if let Some(token) = &continuation {
                req = req.continuation_token(token);
            }
            let page = req.send().await.map_err(|e| {
                Error::engine(format!(
                    "s3 list_objects_v2 {}: {}",
                    self.bucket,
                    sdk_err(e)
                ))
            })?;

            for obj in page.contents() {
                let Some(key) = obj.key() else { continue };
                let Some(relative_key) = relative_key(&self.prefix, key) else {
                    continue;
                };
                let size = obj.size().unwrap_or(0).max(0) as u64;
                if !self
                    .path_matcher
                    .is_file_included(&PathBuf::from(&relative_key))
                {
                    continue;
                }
                if self.max_file_size.is_some_and(|max| size > max) {
                    continue;
                }
                out.push(S3File::new(
                    Some(self.client.clone()),
                    S3FilePath {
                        bucket: self.bucket.clone(),
                        relative_path: relative_key,
                        object_key: key.to_string(),
                    },
                    size,
                    obj.last_modified().map(|d| d.secs()),
                    obj.e_tag().map(str::to_string),
                ));
            }

            match page.next_continuation_token() {
                Some(token) if page.is_truncated().unwrap_or(false) => {
                    continuation = Some(token.to_string());
                }
                _ => break,
            }
        }
        Ok(out)
    }

    /// Like [`list`](Self::list) but returns `(key, file)` pairs where `key` is the
    /// object's relative path — ready to hand to `Ctx::mount_each`.
    pub async fn items(&self) -> Result<Vec<(String, S3File)>> {
        Ok(self
            .list()
            .await?
            .into_iter()
            .map(|f| (f.key(), f))
            .collect())
    }
}

/// Compute an object's path relative to `prefix`. Returns `None` for directory
/// markers (keys ending in `/`) and for the prefix itself (empty relative path).
fn relative_key(prefix: &str, key: &str) -> Option<String> {
    if key.ends_with('/') {
        return None;
    }
    let relative = if prefix.is_empty() {
        key
    } else {
        key.strip_prefix(prefix)?.trim_start_matches('/')
    };
    if relative.is_empty() {
        None
    } else {
        Some(relative.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::FileLike;
    use crate::fs::PatternFilePathMatcher;

    #[test]
    fn parse_uri_ok_and_errors() {
        assert_eq!(
            parse_s3_uri("s3://my-bucket/data/config.json").unwrap(),
            ("my-bucket".to_string(), "data/config.json".to_string())
        );
        assert!(parse_s3_uri("my-bucket/key").is_err()); // no scheme
        assert!(parse_s3_uri("s3://my-bucket").is_err()); // no key
        assert!(parse_s3_uri("s3:///key").is_err()); // no bucket
    }

    #[test]
    fn relative_key_strips_prefix_and_skips_markers() {
        // No prefix: relative == full key.
        assert_eq!(relative_key("", "a/b.md").as_deref(), Some("a/b.md"));
        // Prefix stripped (with and without a trailing slash on the prefix).
        assert_eq!(
            relative_key("data/", "data/a/b.md").as_deref(),
            Some("a/b.md")
        );
        assert_eq!(
            relative_key("data", "data/a/b.md").as_deref(),
            Some("a/b.md")
        );
        // Directory markers are skipped.
        assert_eq!(relative_key("", "a/"), None);
        assert_eq!(relative_key("data/", "data/"), None);
        // The prefix object itself (empty relative path) is skipped.
        assert_eq!(relative_key("data/x", "data/x"), None);
        // A key not under the prefix is skipped.
        assert_eq!(relative_key("data/", "other/a.md"), None);
    }

    #[tokio::test]
    async fn s3_file_implements_shared_filelike_metadata_and_fingerprint() {
        let file = S3File::new(
            None,
            S3FilePath {
                bucket: "bucket".to_string(),
                relative_path: "docs/a.md".to_string(),
                object_key: "prefix/docs/a.md".to_string(),
            },
            42,
            Some(123),
            Some("\"etag-1\"".to_string()),
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

    #[test]
    fn s3_file_path_accessors_and_memo_key() {
        let fp = S3FilePath {
            bucket: "b1".to_string(),
            relative_path: "a/b.md".to_string(),
            object_key: "data/a/b.md".to_string(),
        };
        assert_eq!(fp.bucket(), "b1");
        assert_eq!(fp.path(), "a/b.md");
        assert_eq!(fp.resolve(), "data/a/b.md"); // full key
        // Memo key is (bucket, relative_path) — independent of the full object key.
        let key = serde_json::to_value(fp.memo_key()).unwrap();
        assert_eq!(key, serde_json::json!(["b1", "a/b.md"]));
    }

    #[test]
    fn memo_key_differs_across_buckets_same_path() {
        let mk = |bucket: &str| {
            let fp = S3FilePath {
                bucket: bucket.to_string(),
                relative_path: "a.md".to_string(),
                object_key: "a.md".to_string(),
            };
            serde_json::to_value(fp.memo_key()).unwrap()
        };
        assert_ne!(mk("b1"), mk("b2"));
    }

    #[test]
    fn s3_file_key_is_relative_path() {
        let f = S3File::new(
            None,
            S3FilePath {
                bucket: "b".to_string(),
                relative_path: "sub/doc.md".to_string(),
                object_key: "prefix/sub/doc.md".to_string(),
            },
            10,
            Some(1),
            Some("\"abc\"".to_string()),
        );
        assert_eq!(f.key(), "sub/doc.md");
        assert_eq!(f.file_path().resolve(), "prefix/sub/doc.md");
    }

    // The walker's filtering uses the shared fs PatternFilePathMatcher on the
    // relative path; confirm include/exclude semantics line up with that type.
    #[test]
    fn pattern_matcher_applies_to_relative_path() {
        let matcher = PatternFilePathMatcher::new(["**/*.md"], ["**/skip/**"]).unwrap();
        assert!(matcher.is_file_included(&PathBuf::from("a/b.md")));
        assert!(!matcher.is_file_included(&PathBuf::from("a/b.txt")));
        assert!(!matcher.is_file_included(&PathBuf::from("skip/b.md")));
    }
}
