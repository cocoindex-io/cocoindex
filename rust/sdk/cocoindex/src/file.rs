//! Shared file source resources.
//!
//! File connectors use these types to expose stable logical paths, lazy
//! metadata, cached reads, content fingerprints, and keyed source items.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use cocoindex_utils::fingerprint::Fingerprint;
use globset::{Glob, GlobSet, GlobSetBuilder};
use serde::{Serialize, Serializer, ser::SerializeTuple};
use tokio::sync::Mutex;

use crate::error::{Error, Result};

/// A file path with an optional stable base key.
///
/// The resolved location may move, while [`FilePath::memo_key`] stays stable
/// when callers supply the same logical base key.
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

#[derive(Default)]
pub struct FileContentCache {
    metadata: Mutex<Option<FileMetadata>>,
    full_content: Mutex<Option<Vec<u8>>>,
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

    async fn read(&self) -> Result<Vec<u8>> {
        {
            let guard = self.cache().full_content.lock().await;
            if let Some(content) = guard.as_ref() {
                return Ok(content.clone());
            }
        }
        let content = self.read_impl(None).await?;
        *self.cache().full_content.lock().await = Some(content.clone());
        Ok(content)
    }

    async fn read_size(&self, size: usize) -> Result<Vec<u8>> {
        {
            let guard = self.cache().full_content.lock().await;
            if let Some(content) = guard.as_ref() {
                return Ok(content.iter().take(size).copied().collect());
            }
        }
        self.read_impl(Some(size)).await
    }

    async fn read_text(&self) -> Result<String> {
        Ok(decode_bytes(&self.read().await?))
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
            None => Fingerprint::from(&self.read().await?)
                .map_err(|e| Error::engine(format!("file fingerprint error: {e}")))?,
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
    excluded: GlobSet,
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

        let mut excluded_builder = GlobSetBuilder::new();
        for pattern in excluded_patterns {
            excluded_builder.add(build_glob(pattern.as_ref())?);
        }
        let excluded = excluded_builder
            .build()
            .map_err(|e| Error::engine(format!("invalid glob set: {e}")))?;

        Ok(Self { included, excluded })
    }

    pub fn include<I>(included_patterns: I) -> Result<Self>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        Self::new(included_patterns, std::iter::empty::<&str>())
    }
}

impl FilePathMatcher for PatternFilePathMatcher {
    fn is_dir_included(&self, path: &Path) -> bool {
        !self.excluded.is_match(path_key(path))
    }

    fn is_file_included(&self, path: &Path) -> bool {
        let key = path_key(path);
        !self.excluded.is_match(&key)
            && self
                .included
                .as_ref()
                .is_none_or(|included| included.is_match(key))
    }
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

pub fn decode_bytes(bytes: &[u8]) -> String {
    if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        let (text, _had_errors) = encoding_rs::UTF_8.decode_with_bom_removal(bytes);
        return text.into_owned();
    }
    // UTF-32 must be checked BEFORE UTF-16: a UTF-32LE BOM (FF FE 00 00) starts
    // with the UTF-16LE BOM (FF FE). `encoding_rs` has no UTF-32 decoder, so
    // decode it by hand.
    if bytes.starts_with(&[0xFF, 0xFE, 0x00, 0x00]) {
        return decode_utf32(&bytes[4..], false);
    }
    if bytes.starts_with(&[0x00, 0x00, 0xFE, 0xFF]) {
        return decode_utf32(&bytes[4..], true);
    }
    if bytes.starts_with(&[0xFF, 0xFE]) {
        let (text, _had_errors) = encoding_rs::UTF_16LE.decode_with_bom_removal(bytes);
        return text.into_owned();
    }
    if bytes.starts_with(&[0xFE, 0xFF]) {
        let (text, _had_errors) = encoding_rs::UTF_16BE.decode_with_bom_removal(bytes);
        return text.into_owned();
    }
    let (text, _encoding, _had_errors) = encoding_rs::UTF_8.decode(bytes);
    text.into_owned()
}

/// Decode UTF-32 (BOM already stripped) into a `String`, replacing invalid code
/// points with U+FFFD (mirrors lossy decoding of the other encodings).
fn decode_utf32(bytes: &[u8], big_endian: bool) -> String {
    bytes
        .chunks(4)
        .map(|chunk| {
            if chunk.len() < 4 {
                return '\u{FFFD}';
            }
            let code = if big_endian {
                u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])
            } else {
                u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])
            };
            char::from_u32(code).unwrap_or('\u{FFFD}')
        })
        .collect()
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

        assert_eq!(file.read().await.unwrap(), b"hello world");
        assert_eq!(file.read().await.unwrap(), b"hello world");
        assert_eq!(file.read_size(5).await.unwrap(), b"hello");
        assert_eq!(file.reads.load(Ordering::SeqCst), 1);

        let first = file.content_fingerprint().await.unwrap();
        let second = file.content_fingerprint().await.unwrap();
        assert_eq!(first, second);
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
    fn file_path_surface_preserves_base_key() {
        let p = FilePath::with_base_dir("docs", "/tmp/root", "a/b.md");
        assert_eq!(p.parent().unwrap().base_key(), Some("docs"));
        assert_eq!(p.with_suffix(".txt").base_key(), Some("docs"));
    }
}
