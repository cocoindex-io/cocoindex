//! Shared file source resources.
//!
//! File connectors use these types to expose stable logical paths, lazy
//! metadata, cached reads, content fingerprints, and keyed source items.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use cocoindex_utils::fingerprint::Fingerprint;
use globset::{Glob, GlobSet, GlobSetBuilder};
use serde::{Serialize, Serializer, ser::SerializeTuple};
use tokio::sync::Mutex;

use crate::error::{Error, Result};

/// A file path with an optional stable base key.
///
/// The resolved location may move, while [`FilePath::memo_key`] stays stable
/// when callers supply the same logical base key ([`FilePath::with_base_dir`],
/// or `FilePath::under_context_dir` for a directory the environment provides
/// under a `ContextKey`, as Python's `FilePath(path, base_dir=SOURCE_DIR)`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FilePath {
    base_key: Option<Arc<str>>,
    base_dir: Option<PathBuf>,
    path: PathBuf,
}

impl FilePath {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            base_key: None,
            base_dir: None,
            path: path.into(),
        }
    }

    pub fn with_base_dir(
        base_key: impl Into<Arc<str>>,
        base_dir: impl Into<PathBuf>,
        path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            base_key: Some(base_key.into()),
            base_dir: Some(base_dir.into()),
            path: path.into(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn base_key(&self) -> Option<&str> {
        self.base_key.as_deref()
    }

    pub fn resolve(&self) -> PathBuf {
        match &self.base_dir {
            Some(base) => base.join(&self.path),
            None => self.path.clone(),
        }
    }

    pub fn join(&self, child: impl AsRef<Path>) -> Self {
        Self {
            base_key: self.base_key.clone(),
            base_dir: self.base_dir.clone(),
            path: self.path.join(child),
        }
    }

    pub fn name(&self) -> &str {
        self.path.file_name().and_then(|s| s.to_str()).unwrap_or("")
    }

    pub fn stem(&self) -> &str {
        self.path.file_stem().and_then(|s| s.to_str()).unwrap_or("")
    }

    pub fn suffix(&self) -> String {
        self.path
            .extension()
            .and_then(|s| s.to_str())
            .map(|ext| format!(".{ext}"))
            .unwrap_or_default()
    }

    pub fn as_posix(&self) -> String {
        path_key(&self.path)
    }

    /// The path's components as strings (cf. Python `PurePath.parts`).
    pub fn parts(&self) -> Vec<String> {
        self.path
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect()
    }

    /// The logical parent path, preserving the base key/dir (cf. `PurePath.parent`).
    /// `None` at the root / for a bare filename.
    pub fn parent(&self) -> Option<FilePath> {
        let parent = self.path.parent()?;
        if parent.as_os_str().is_empty() {
            return None;
        }
        Some(self.with_path(parent.to_path_buf()))
    }

    /// The path's logical ancestors, nearest first (cf. `PurePath.parents`,
    /// without its final `.` entry).
    pub fn parents(&self) -> Vec<FilePath> {
        let mut parents = Vec::new();
        let mut current = self.parent();
        while let Some(parent) = current {
            current = parent.parent();
            parents.push(parent);
        }
        parents
    }

    /// Whether `base` is this path or one of its ancestors (cf.
    /// `PurePath.is_relative_to`).
    pub fn is_relative_to(&self, base: impl AsRef<Path>) -> bool {
        self.path.starts_with(base.as_ref())
    }

    /// Match the path against a glob `pattern` with `PurePath.match` semantics:
    /// a relative pattern is matched from the right, component by component
    /// (`*` and `?` stay within one component; `**` is not special); an
    /// absolute pattern must match the whole path. An invalid pattern never
    /// matches.
    pub fn matches(&self, pattern: &str) -> bool {
        let pattern = pattern.replace('\\', "/");
        let pattern_parts: Vec<&str> = pattern.split('/').filter(|p| !p.is_empty()).collect();
        if pattern_parts.is_empty() {
            return false;
        }
        let parts = self.parts();
        // `has_root`, not `is_absolute`: on Windows `/docs/a.md` has a root
        // but no drive, and pathlib anchors on the root alone.
        let anchored = pattern.starts_with('/');
        if anchored && (!self.path.has_root() || parts.len() != pattern_parts.len() + 1) {
            return false;
        }
        if parts.len() < pattern_parts.len() {
            return false;
        }
        parts
            .iter()
            .rev()
            .zip(pattern_parts.iter().rev())
            .all(|(part, glob)| Glob::new(glob).is_ok_and(|g| g.compile_matcher().is_match(part)))
    }

    /// All file-name suffixes, e.g. `a.tar.gz` -> `[".tar", ".gz"]` (cf.
    /// `PurePath.suffixes`). Leading dots (hidden files) are ignored.
    pub fn suffixes(&self) -> Vec<String> {
        let name = self.name().trim_start_matches('.');
        if !name.contains('.') {
            return Vec::new();
        }
        name.split('.').skip(1).map(|s| format!(".{s}")).collect()
    }

    /// A new path with the final component replaced (cf. `PurePath.with_name`).
    pub fn with_name(&self, name: impl AsRef<str>) -> FilePath {
        let base = self.path.parent().unwrap_or_else(|| Path::new(""));
        self.with_path(base.join(name.as_ref()))
    }

    /// A new path with the file stem replaced, keeping the suffix (cf.
    /// `PurePath.with_stem`).
    pub fn with_stem(&self, stem: impl AsRef<str>) -> FilePath {
        self.with_name(format!("{}{}", stem.as_ref(), self.suffix()))
    }

    /// A new path with the final suffix replaced (empty `suffix` removes it; a
    /// non-empty `suffix` should start with `.`). Cf. `PurePath.with_suffix`.
    pub fn with_suffix(&self, suffix: impl AsRef<str>) -> FilePath {
        self.with_name(format!("{}{}", self.stem(), suffix.as_ref()))
    }

    /// This path relative to `base`, preserving the base key/dir, or `None` if
    /// it is not under `base` (cf. `PurePath.relative_to`).
    pub fn relative_to(&self, base: impl AsRef<Path>) -> Option<FilePath> {
        self.path
            .strip_prefix(base.as_ref())
            .ok()
            .map(|p| self.with_path(p.to_path_buf()))
    }

    fn with_path(&self, path: PathBuf) -> FilePath {
        FilePath {
            base_key: self.base_key.clone(),
            base_dir: self.base_dir.clone(),
            path,
        }
    }

    pub fn memo_key(&self) -> impl Serialize + '_ {
        match self.base_key.as_deref() {
            Some(base_key) => FilePathMemoKey::Base {
                base_key,
                path: self.as_posix(),
            },
            None => FilePathMemoKey::Path(self.as_posix()),
        }
    }
}

impl<P: Into<PathBuf>> From<P> for FilePath {
    fn from(path: P) -> Self {
        Self::new(path)
    }
}

enum FilePathMemoKey<'a> {
    Path(String),
    Base { base_key: &'a str, path: String },
}

impl Serialize for FilePathMemoKey<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Path(path) => path.serialize(serializer),
            Self::Base { base_key, path } => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(base_key)?;
                tuple.serialize_element(path)?;
                tuple.end()
            }
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct FileMetadata {
    pub size: u64,
    #[serde(with = "system_time_serde")]
    pub modified: SystemTime,
    pub content_fingerprint: Option<Fingerprint>,
}

/// A file's full content, and when the read that produced it started.
struct CachedContent {
    bytes: Bytes,
    read_at: SystemTime,
}

/// Per-file cache of metadata, full content and content fingerprint.
///
/// The content is held as [`Bytes`], so every [`FileLike::read`] after the
/// first is a reference-count bump, not a copy of the file.
#[derive(Default)]
pub struct FileContentCache {
    metadata: Mutex<Option<FileMetadata>>,
    full_content: Mutex<Option<CachedContent>>,
    content_fingerprint: Mutex<Option<Fingerprint>>,
}

impl FileContentCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_metadata(metadata: FileMetadata) -> Self {
        Self {
            metadata: Mutex::new(Some(metadata)),
            full_content: Mutex::new(None),
            content_fingerprint: Mutex::new(None),
        }
    }

    /// When the read of the cached full content started, if it has been read.
    pub(crate) async fn full_content_read_at(&self) -> Option<SystemTime> {
        self.full_content
            .lock()
            .await
            .as_ref()
            .map(|content| content.read_at)
    }
}

#[async_trait]
pub trait FileLike: Send + Sync {
    fn file_path(&self) -> FilePath;

    fn cache(&self) -> &FileContentCache;

    async fn fetch_metadata(&self) -> Result<FileMetadata>;

    async fn read_impl(&self, size: Option<usize>) -> Result<Vec<u8>>;

    async fn metadata(&self) -> Result<FileMetadata> {
        {
            let guard = self.cache().metadata.lock().await;
            if let Some(metadata) = guard.as_ref() {
                return Ok(metadata.clone());
            }
        }
        let metadata = self.fetch_metadata().await?;
        *self.cache().metadata.lock().await = Some(metadata.clone());
        Ok(metadata)
    }

    /// The whole file, from the cache after the first call. Cloning the
    /// returned [`Bytes`] shares the buffer.
    async fn read(&self) -> Result<Bytes> {
        {
            let guard = self.cache().full_content.lock().await;
            if let Some(content) = guard.as_ref() {
                return Ok(content.bytes.clone());
            }
        }
        let read_at = SystemTime::now();
        let bytes = Bytes::from(self.read_impl(None).await?);
        *self.cache().full_content.lock().await = Some(CachedContent {
            bytes: bytes.clone(),
            read_at,
        });
        Ok(bytes)
    }

    /// The first `size` bytes: a slice of the cached content when the file
    /// was read before, a bounded read otherwise.
    async fn read_size(&self, size: usize) -> Result<Bytes> {
        {
            let guard = self.cache().full_content.lock().await;
            if let Some(content) = guard.as_ref() {
                return Ok(content.bytes.slice(..size.min(content.bytes.len())));
            }
        }
        Ok(Bytes::from(self.read_impl(Some(size)).await?))
    }

    /// The content as text: BOM-detected encoding (UTF-8/16/32), else UTF-8,
    /// with invalid sequences replaced by U+FFFD. The Python defaults.
    async fn read_text(&self) -> Result<String> {
        Ok(decode_bytes(&self.read().await?))
    }

    /// The content as text in `encoding` (a WHATWG label such as `"latin1"` or
    /// `"utf-16le"`, or `"utf-32le"`/`"utf-32be"`; `None` detects by BOM, else
    /// UTF-8), with `errors` deciding what an undecodable sequence does. Cf.
    /// Python `read_text(encoding=..., errors=...)`.
    async fn read_text_with(&self, encoding: Option<&str>, errors: DecodeErrors) -> Result<String> {
        decode_bytes_with(&self.read().await?, encoding, errors)
    }

    async fn content_fingerprint(&self) -> Result<Fingerprint> {
        {
            let guard = self.cache().content_fingerprint.lock().await;
            if let Some(fp) = guard.as_ref() {
                return Ok(*fp);
            }
        }
        let metadata = self.metadata().await?;
        let fp = match metadata.content_fingerprint {
            Some(fp) => fp,
            // One hasher update over the content, as the Python SDK's
            // `fingerprint_bytes` does; `Fingerprint::from` would serialize
            // the bytes one by one.
            None => Fingerprint::from_bytes(&self.read().await?),
        };
        *self.cache().content_fingerprint.lock().await = Some(fp);
        Ok(fp)
    }
}

pub trait FileSourceItem: FileLike {
    fn key(&self) -> String {
        self.file_path().as_posix()
    }
}

pub trait FilePathMatcher: Send + Sync {
    fn is_dir_included(&self, _path: &Path) -> bool {
        true
    }

    fn is_file_included(&self, path: &Path) -> bool;
}

#[derive(Clone, Debug, Default)]
pub struct MatchAllFilePathMatcher;

impl FilePathMatcher for MatchAllFilePathMatcher {
    fn is_file_included(&self, _path: &Path) -> bool {
        true
    }
}

#[derive(Clone, Debug)]
pub struct PatternFilePathMatcher {
    included: Option<GlobSet>,
    /// Regular (non-negated) exclusion patterns.
    excluded: Option<GlobSet>,
    /// Negation (`!`-prefixed in the original list, stored without the `!`)
    /// patterns compiled into a GlobSet. A path matching one of these is *not*
    /// excluded even if it matches `excluded`.
    negation: Option<GlobSet>,
    /// Brace-expanded raw negation strings, kept so `is_dir_included` can detect
    /// directories on the path to a negation-exempt file even when the directory
    /// itself would otherwise be pruned (e.g. `!dir1/dir2/a.yml` with `dir1/**`).
    negation_raw: Vec<String>,
}

impl Default for PatternFilePathMatcher {
    fn default() -> Self {
        Self::new(std::iter::empty::<&str>(), std::iter::empty::<&str>())
            .expect("empty glob sets should be valid")
    }
}

impl PatternFilePathMatcher {
    pub fn new<I, E>(included_patterns: I, excluded_patterns: E) -> Result<Self>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
        E: IntoIterator,
        E::Item: AsRef<str>,
    {
        let mut included_builder = GlobSetBuilder::new();
        let mut has_included = false;
        for pattern in included_patterns {
            included_builder.add(build_glob(pattern.as_ref())?);
            has_included = true;
        }
        let included = if has_included {
            Some(
                included_builder
                    .build()
                    .map_err(|e| Error::engine(format!("invalid glob set: {e}")))?,
            )
        } else {
            None
        };

        // Patterns in `excluded_patterns` that start with `!` are gitignore-style
        // negations: they un-exclude any path that would otherwise be excluded.
        let mut excluded_builder = GlobSetBuilder::new();
        let mut has_excluded = false;
        let mut negation_builder = GlobSetBuilder::new();
        let mut has_negation = false;
        let mut negation_raw = Vec::new();
        for pattern in excluded_patterns {
            let pattern = pattern.as_ref();
            if let Some(stripped) = pattern.strip_prefix('!') {
                negation_builder.add(build_glob(stripped)?);
                has_negation = true;
                negation_raw.extend(brace_expand(stripped));
            } else {
                excluded_builder.add(build_glob(pattern)?);
                has_excluded = true;
            }
        }
        let build_set = |builder: GlobSetBuilder, present: bool| -> Result<Option<GlobSet>> {
            if !present {
                return Ok(None);
            }
            builder
                .build()
                .map(Some)
                .map_err(|e| Error::engine(format!("invalid glob set: {e}")))
        };
        let excluded = build_set(excluded_builder, has_excluded)?;
        let negation = build_set(negation_builder, has_negation)?;

        Ok(Self {
            included,
            excluded,
            negation,
            negation_raw,
        })
    }

    pub fn include<I>(included_patterns: I) -> Result<Self>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        Self::new(included_patterns, std::iter::empty::<&str>())
    }
}

impl PatternFilePathMatcher {
    /// Whether `key` is excluded after applying both exclusion and negation patterns.
    fn is_excluded(&self, key: &str) -> bool {
        if !self.excluded.as_ref().is_some_and(|gs| gs.is_match(key)) {
            return false;
        }
        // Matches an exclusion pattern; a negation pattern can un-exclude it.
        !self.negation.as_ref().is_some_and(|gs| gs.is_match(key))
    }
}

impl FilePathMatcher for PatternFilePathMatcher {
    fn is_dir_included(&self, path: &Path) -> bool {
        let key = path_key(path);
        if !self.excluded.as_ref().is_some_and(|gs| gs.is_match(&key)) {
            return true;
        }
        // The directory matches an exclusion pattern. Traverse it anyway if a
        // negation pattern could apply to it or to a file inside it.
        if let Some(neg) = &self.negation {
            if neg.is_match(&key) {
                return true;
            }
            // Probe one level inside: catches glob negations like `!**/.github/**`.
            if neg.is_match(format!("{key}/__probe__").as_str()) {
                return true;
            }
        }
        // Raw-prefix check: catches exact-path negations like `!dir1/dir2/a.yml`
        // whose ancestor directories would otherwise be pruned before the exempt
        // file is reached.
        let dir_prefix = format!("{key}/");
        self.negation_raw.iter().any(|p| p.starts_with(&dir_prefix))
    }

    fn is_file_included(&self, path: &Path) -> bool {
        let key = path_key(path);
        self.included
            .as_ref()
            .is_none_or(|included| included.is_match(&key))
            && !self.is_excluded(&key)
    }
}

/// Expands brace alternations (`{a,b}`) in a glob into the full set of concrete
/// alternatives, e.g. `a/{b/c,d}/e` → `["a/b/c/e", "a/d/e"]`. Used only to feed
/// the prefix-ancestry check in [`PatternFilePathMatcher::is_dir_included`];
/// file-level matching stays delegated to `globset`, which already expands
/// braces. A pattern with no expandable (or malformed) braces returns itself.
fn brace_expand(pattern: &str) -> Vec<String> {
    let bytes = pattern.as_bytes();
    let mut in_class = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'[' => in_class = true,
            b']' => in_class = false,
            b'{' if !in_class => {
                let open = i;
                let mut depth = 1;
                let mut inner_class = false;
                let mut seg_start = open + 1;
                let mut alternatives: Vec<&str> = Vec::new();
                let mut j = open + 1;
                while j < bytes.len() && depth > 0 {
                    match bytes[j] {
                        b'[' => inner_class = true,
                        b']' => inner_class = false,
                        b'{' if !inner_class => depth += 1,
                        b'}' if !inner_class => {
                            depth -= 1;
                            if depth == 0 {
                                alternatives.push(&pattern[seg_start..j]);
                            }
                        }
                        b',' if !inner_class && depth == 1 => {
                            alternatives.push(&pattern[seg_start..j]);
                            seg_start = j + 1;
                        }
                        _ => {}
                    }
                    j += 1;
                }
                // Unbalanced braces: leave untouched so globset reports the error.
                if depth != 0 {
                    return vec![pattern.to_string()];
                }
                let prefix = &pattern[..open];
                let suffix = &pattern[j..]; // j is one past the closing '}'
                let mut out = Vec::new();
                for alt in alternatives {
                    out.extend(brace_expand(&format!("{prefix}{alt}{suffix}")));
                }
                return out;
            }
            _ => {}
        }
        i += 1;
    }
    vec![pattern.to_string()]
}

pub mod system_time_serde {
    use super::*;

    pub fn serialize<S: Serializer>(
        time: &SystemTime,
        ser: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let duration = time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        (duration.as_secs(), duration.subsec_nanos()).serialize(ser)
    }
}

/// What an undecodable byte sequence does when reading text (cf. Python's
/// `errors` argument to `bytes.decode`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DecodeErrors {
    /// Replace it with U+FFFD (the default).
    #[default]
    Replace,
    /// Drop it.
    Ignore,
    /// Fail the read.
    Strict,
}

/// Decode `bytes` as text, detecting the encoding by BOM (UTF-8/16/32) and
/// falling back to UTF-8, with invalid sequences replaced by U+FFFD.
pub fn decode_bytes(bytes: &[u8]) -> String {
    decode_bytes_with(bytes, None, DecodeErrors::Replace)
        .expect("replacing decode errors never fails")
}

/// Decode `bytes` as text in `encoding` (`None` detects by BOM, else UTF-8),
/// handling undecodable sequences per `errors`. See
/// [`FileLike::read_text_with`].
pub fn decode_bytes_with(
    bytes: &[u8],
    encoding: Option<&str>,
    errors: DecodeErrors,
) -> Result<String> {
    let Some(label) = encoding else {
        return decode_detected(bytes, errors);
    };
    let normalized = label.trim().to_ascii_lowercase().replace('_', "-");
    match normalized.as_str() {
        "utf-32" | "utf-32-le" | "utf-32le" => decode_utf32(bytes, false, errors),
        "utf-32-be" | "utf-32be" => decode_utf32(bytes, true, errors),
        // Python's `utf-8-sig` strips a BOM; encoding_rs's UTF-8 does not.
        "utf-8-sig" => decode_with(
            encoding_rs::UTF_8,
            bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes),
            errors,
        ),
        _ => {
            let encoding = encoding_rs::Encoding::for_label(normalized.as_bytes())
                .ok_or_else(|| Error::engine(format!("unknown text encoding {label:?}")))?;
            decode_with(encoding, bytes, errors)
        }
    }
}

fn decode_detected(bytes: &[u8], errors: DecodeErrors) -> Result<String> {
    if let Some(rest) = bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]) {
        return decode_with(encoding_rs::UTF_8, rest, errors);
    }
    // UTF-32 must be checked BEFORE UTF-16: a UTF-32LE BOM (FF FE 00 00) starts
    // with the UTF-16LE BOM (FF FE). `encoding_rs` has no UTF-32 decoder, so
    // decode it by hand.
    if let Some(rest) = bytes.strip_prefix(&[0xFF, 0xFE, 0x00, 0x00]) {
        return decode_utf32(rest, false, errors);
    }
    if let Some(rest) = bytes.strip_prefix(&[0x00, 0x00, 0xFE, 0xFF]) {
        return decode_utf32(rest, true, errors);
    }
    if let Some(rest) = bytes.strip_prefix(&[0xFF, 0xFE]) {
        return decode_with(encoding_rs::UTF_16LE, rest, errors);
    }
    if let Some(rest) = bytes.strip_prefix(&[0xFE, 0xFF]) {
        return decode_with(encoding_rs::UTF_16BE, rest, errors);
    }
    decode_with(encoding_rs::UTF_8, bytes, errors)
}

/// Decode with an `encoding_rs` encoding (BOM already stripped). `encoding_rs`
/// only replaces, so `Ignore` strips the replacement characters it inserted
/// and `Strict` fails when it inserted any.
fn decode_with(
    encoding: &'static encoding_rs::Encoding,
    bytes: &[u8],
    errors: DecodeErrors,
) -> Result<String> {
    let (text, had_errors) = encoding.decode_without_bom_handling(bytes);
    if !had_errors {
        return Ok(text.into_owned());
    }
    match errors {
        DecodeErrors::Replace => Ok(text.into_owned()),
        DecodeErrors::Ignore => Ok(text.chars().filter(|c| *c != '\u{FFFD}').collect()),
        DecodeErrors::Strict => Err(Error::engine(format!(
            "invalid {} byte sequence in text",
            encoding.name()
        ))),
    }
}

/// Decode UTF-32 (BOM already stripped) into a `String`; an invalid code point
/// or a trailing partial code unit is an undecodable sequence.
fn decode_utf32(bytes: &[u8], big_endian: bool, errors: DecodeErrors) -> Result<String> {
    let mut text = String::with_capacity(bytes.len() / 4);
    for chunk in bytes.chunks(4) {
        let decoded = match <[u8; 4]>::try_from(chunk) {
            Ok(unit) if big_endian => char::from_u32(u32::from_be_bytes(unit)),
            Ok(unit) => char::from_u32(u32::from_le_bytes(unit)),
            Err(_) => None,
        };
        match (decoded, errors) {
            (Some(c), _) => text.push(c),
            (None, DecodeErrors::Replace) => text.push('\u{FFFD}'),
            (None, DecodeErrors::Ignore) => {}
            (None, DecodeErrors::Strict) => {
                return Err(Error::engine("invalid UTF-32 code unit in text"));
            }
        }
    }
    Ok(text)
}

fn build_glob(pattern: &str) -> Result<globset::Glob> {
    Glob::new(pattern).map_err(|e| Error::engine(format!("invalid glob: {e}")))
}

pub(crate) fn path_key(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MockFile {
        path: FilePath,
        cache: FileContentCache,
        reads: AtomicUsize,
        metadata_reads: AtomicUsize,
        metadata_fp: Option<Fingerprint>,
    }

    impl MockFile {
        fn new(metadata_fp: Option<Fingerprint>) -> Self {
            Self {
                path: FilePath::new("mock.txt"),
                cache: FileContentCache::new(),
                reads: AtomicUsize::new(0),
                metadata_reads: AtomicUsize::new(0),
                metadata_fp,
            }
        }
    }

    #[async_trait]
    impl FileLike for MockFile {
        fn file_path(&self) -> FilePath {
            self.path.clone()
        }

        fn cache(&self) -> &FileContentCache {
            &self.cache
        }

        async fn fetch_metadata(&self) -> Result<FileMetadata> {
            self.metadata_reads.fetch_add(1, Ordering::SeqCst);
            Ok(FileMetadata {
                size: 11,
                modified: SystemTime::UNIX_EPOCH,
                content_fingerprint: self.metadata_fp,
            })
        }

        async fn read_impl(&self, size: Option<usize>) -> Result<Vec<u8>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let content = b"hello world";
            Ok(match size {
                Some(size) => content.iter().take(size).copied().collect(),
                None => content.to_vec(),
            })
        }
    }

    #[tokio::test]
    async fn file_like_caches_metadata_full_reads_and_fingerprints() {
        let file = MockFile::new(None);
        assert_eq!(file.metadata().await.unwrap().size, 11);
        assert_eq!(file.metadata().await.unwrap().size, 11);
        assert_eq!(file.metadata_reads.load(Ordering::SeqCst), 1);

        let first_read = file.read().await.unwrap();
        let second_read = file.read().await.unwrap();
        assert_eq!(first_read, b"hello world"[..]);
        assert_eq!(second_read, b"hello world"[..]);
        assert_eq!(file.read_size(5).await.unwrap(), b"hello"[..]);
        assert_eq!(file.read_size(100).await.unwrap(), b"hello world"[..]);
        assert_eq!(file.reads.load(Ordering::SeqCst), 1);
        // A cached read shares the buffer instead of copying it.
        assert_eq!(first_read.as_ptr(), second_read.as_ptr());

        let first = file.content_fingerprint().await.unwrap();
        let second = file.content_fingerprint().await.unwrap();
        assert_eq!(first, second);
        assert_eq!(first, Fingerprint::from_bytes(b"hello world"));
        assert_eq!(file.reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn file_like_uses_metadata_fingerprint_without_reading_content() {
        let fp = Fingerprint::from(&"etag").unwrap();
        let file = MockFile::new(Some(fp));
        assert_eq!(file.content_fingerprint().await.unwrap(), fp);
        assert_eq!(file.reads.load(Ordering::SeqCst), 0);
        assert_eq!(file.metadata_reads.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn filepath_memo_key_omits_empty_base_key() {
        let no_base = rmp_serde::to_vec(&FilePath::new("a/b.txt").memo_key()).unwrap();
        let with_base =
            rmp_serde::to_vec(&FilePath::with_base_dir("docs", "/tmp/a", "b.txt").memo_key())
                .unwrap();
        assert_ne!(no_base, with_base);
    }

    #[test]
    fn filepath_suffix_matches_python_pathlib() {
        assert_eq!(FilePath::new("docs/readme.md").suffix(), ".md");
        assert_eq!(FilePath::new("docs/readme").suffix(), "");
    }

    #[test]
    fn match_all_file_path_matcher_includes_everything() {
        let matcher = MatchAllFilePathMatcher;
        assert!(matcher.is_file_included(Path::new("anything.txt")));
        assert!(matcher.is_file_included(Path::new("nested/path/file.py")));
        assert!(matcher.is_dir_included(Path::new(".hidden")));
    }

    #[test]
    fn pattern_file_path_matcher_matches_core_python_semantics() {
        let basename = PatternFilePathMatcher::include(["*.py"]).unwrap();
        assert!(basename.is_file_included(Path::new("main.py")));
        assert!(basename.is_file_included(Path::new("src/main.py")));
        assert!(!basename.is_file_included(Path::new("main.rs")));

        let scoped = PatternFilePathMatcher::include(["src/*.py"]).unwrap();
        assert!(scoped.is_file_included(Path::new("src/main.py")));
        assert!(!scoped.is_file_included(Path::new("main.py")));
        assert!(!scoped.is_file_included(Path::new("lib/src/main.py")));

        let recursive = PatternFilePathMatcher::include(["**/*.py"]).unwrap();
        assert!(recursive.is_file_included(Path::new("main.py")));
        assert!(recursive.is_file_included(Path::new("a/b/c/main.py")));

        let excluded = PatternFilePathMatcher::new(["**/*.py"], ["**/test_*"]).unwrap();
        assert!(excluded.is_file_included(Path::new("main.py")));
        assert!(!excluded.is_file_included(Path::new("tests/test_main.py")));
        assert!(!excluded.is_file_included(Path::new("test_main.py")));

        let hidden = PatternFilePathMatcher::new(std::iter::empty::<&str>(), ["**/.*"]).unwrap();
        assert!(!hidden.is_dir_included(Path::new(".git")));
        assert!(!hidden.is_dir_included(Path::new("src/.hidden")));
        assert!(hidden.is_dir_included(Path::new("src")));

        let alternation = PatternFilePathMatcher::include(["**/*.{py,rs}"]).unwrap();
        assert!(alternation.is_file_included(Path::new("main.py")));
        assert!(alternation.is_file_included(Path::new("main.rs")));
        assert!(!alternation.is_file_included(Path::new("main.js")));
    }

    #[test]
    fn pattern_file_path_matcher_rejects_invalid_patterns() {
        assert!(PatternFilePathMatcher::include(["[invalid"]).is_err());
        assert!(PatternFilePathMatcher::new(std::iter::empty::<&str>(), ["[invalid"]).is_err());
    }

    #[test]
    fn pattern_file_path_matcher_negation() {
        // Gitignore-style exception: exclude all dot-entries but keep `.github`.
        let m =
            PatternFilePathMatcher::new(std::iter::empty::<&str>(), ["**/.*", "!**/.github/**"])
                .unwrap();
        assert!(!m.is_file_included(Path::new(".env")));
        assert!(m.is_file_included(Path::new(".github/workflows/ci.yml")));
        // The `.github` dir must be traversed even though `**/.*` excludes it.
        assert!(!m.is_dir_included(Path::new(".cache")));
        assert!(m.is_dir_included(Path::new(".github")));
        assert!(m.is_dir_included(Path::new(".github/workflows")));

        // Exact-path negation under a broad exclusion: ancestors stay traversable.
        let exact = PatternFilePathMatcher::new(
            std::iter::empty::<&str>(),
            ["dir1/**", "!dir1/dir2/a.yml"],
        )
        .unwrap();
        assert!(!exact.is_file_included(Path::new("dir1/other.txt")));
        assert!(exact.is_file_included(Path::new("dir1/dir2/a.yml")));
        assert!(exact.is_dir_included(Path::new("dir1")));
        assert!(exact.is_dir_included(Path::new("dir1/dir2")));

        // Brace-alternation negation contributes every branch to ancestry checks.
        let braced = PatternFilePathMatcher::new(
            std::iter::empty::<&str>(),
            ["a/**", "!a/{b/c,d}/keep.txt"],
        )
        .unwrap();
        assert!(braced.is_file_included(Path::new("a/b/c/keep.txt")));
        assert!(braced.is_file_included(Path::new("a/d/keep.txt")));
        assert!(!braced.is_file_included(Path::new("a/b/drop.txt")));
        assert!(braced.is_dir_included(Path::new("a/b")));
        assert!(braced.is_dir_included(Path::new("a/d")));
    }

    #[test]
    fn decode_bytes_detects_utf_boms() {
        assert_eq!(decode_bytes(b"\xEF\xBB\xBFhello"), "hello");
        assert_eq!(decode_bytes(b"\xFF\xFEh\0i\0"), "hi");
        assert_eq!(decode_bytes(b"a\xFFb"), "a\u{fffd}b");
    }

    #[test]
    fn decode_bytes_detects_utf32_before_utf16() {
        // UTF-32LE BOM is FF FE 00 00 — must not be mistaken for UTF-16LE (FF FE).
        assert_eq!(
            decode_bytes(b"\xFF\xFE\x00\x00h\x00\x00\x00i\x00\x00\x00"),
            "hi"
        );
        // UTF-32BE BOM is 00 00 FE FF.
        assert_eq!(
            decode_bytes(b"\x00\x00\xFE\xFF\x00\x00\x00h\x00\x00\x00i"),
            "hi"
        );
    }

    #[test]
    fn file_path_surface_methods() {
        let p = FilePath::new("docs/a/report.tar.gz");
        assert_eq!(p.parts(), vec!["docs", "a", "report.tar.gz"]);
        assert_eq!(p.name(), "report.tar.gz");
        assert_eq!(p.suffix(), ".gz");
        assert_eq!(p.suffixes(), vec![".tar", ".gz"]);
        assert_eq!(p.parent().unwrap().as_posix(), "docs/a");
        assert_eq!(p.parent().unwrap().parent().unwrap().as_posix(), "docs");
        assert_eq!(p.with_name("x.md").as_posix(), "docs/a/x.md");
        assert_eq!(p.with_suffix(".md").as_posix(), "docs/a/report.tar.md");
        assert_eq!(p.with_stem("final").as_posix(), "docs/a/final.gz");
        assert_eq!(p.relative_to("docs").unwrap().as_posix(), "a/report.tar.gz");
        assert!(p.relative_to("other").is_none());
        // A bare filename has no parent; a hidden file has no suffixes.
        assert!(FilePath::new("file.txt").parent().is_none());
        assert!(FilePath::new(".bashrc").suffixes().is_empty());
    }

    #[test]
    fn file_path_parents_is_relative_to_and_matches_follow_pathlib() {
        let p = FilePath::new("docs/a/report.md");
        let parents: Vec<String> = p.parents().iter().map(FilePath::as_posix).collect();
        assert_eq!(parents, vec!["docs/a", "docs"]);
        assert!(FilePath::new("file.txt").parents().is_empty());

        assert!(p.is_relative_to("docs"));
        assert!(p.is_relative_to("docs/a"));
        assert!(p.is_relative_to("docs/a/report.md"));
        assert!(!p.is_relative_to("doc"));
        assert!(!p.is_relative_to("other"));

        // A relative pattern matches from the right, one component at a time.
        assert!(p.matches("*.md"));
        assert!(p.matches("a/*.md"));
        assert!(p.matches("docs/a/report.md"));
        assert!(!p.matches("docs/*.md"));
        assert!(!p.matches("*.py"));
        assert!(!p.matches("x/docs/a/report.md"));
        // An absolute pattern must match the whole path.
        let abs = FilePath::new("/docs/a/report.md");
        assert!(abs.matches("/docs/a/*.md"));
        assert!(!abs.matches("/a/*.md"));
        assert!(!p.matches("/docs/a/report.md"));
        // `*` does not cross a separator, and an invalid pattern never matches.
        assert!(!p.matches("docs/*"));
        assert!(!p.matches("[invalid"));
    }

    #[test]
    fn decode_bytes_with_explicit_encoding_and_error_modes() {
        // An explicit encoding wins over detection.
        assert_eq!(
            decode_bytes_with(b"caf\xe9", Some("latin1"), DecodeErrors::Replace).unwrap(),
            "café"
        );
        assert_eq!(
            decode_bytes_with(b"h\0i\0", Some("utf-16le"), DecodeErrors::Replace).unwrap(),
            "hi"
        );
        assert_eq!(
            decode_bytes_with(b"\xEF\xBB\xBFhi", Some("utf-8-sig"), DecodeErrors::Strict).unwrap(),
            "hi"
        );
        assert_eq!(
            decode_bytes_with(b"h\0\0\0", Some("utf-32"), DecodeErrors::Strict).unwrap(),
            "h"
        );
        assert!(decode_bytes_with(b"x", Some("no-such-encoding"), DecodeErrors::Replace).is_err());

        // The error modes, Python's `errors=`.
        assert_eq!(
            decode_bytes_with(b"a\xFFb", None, DecodeErrors::Replace).unwrap(),
            "a\u{fffd}b"
        );
        assert_eq!(
            decode_bytes_with(b"a\xFFb", None, DecodeErrors::Ignore).unwrap(),
            "ab"
        );
        assert!(decode_bytes_with(b"a\xFFb", None, DecodeErrors::Strict).is_err());
        assert_eq!(
            decode_bytes_with(b"ok", None, DecodeErrors::Strict).unwrap(),
            "ok"
        );
        // A trailing partial UTF-32 unit is an error in strict mode.
        assert!(decode_bytes_with(b"\xFF\xFE\0\0h\0\0", None, DecodeErrors::Strict).is_err());
    }

    #[tokio::test]
    async fn file_like_read_text_with_uses_the_cached_content() {
        let file = MockFile::new(None);
        assert_eq!(file.read_text().await.unwrap(), "hello world");
        assert_eq!(
            file.read_text_with(Some("ascii"), DecodeErrors::Strict)
                .await
                .unwrap(),
            "hello world"
        );
        assert_eq!(file.reads.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn file_path_surface_preserves_base_key() {
        let p = FilePath::with_base_dir("docs", "/tmp/root", "a/b.md");
        assert_eq!(p.parent().unwrap().base_key(), Some("docs"));
        assert_eq!(p.with_suffix(".txt").base_key(), Some("docs"));
    }
}
