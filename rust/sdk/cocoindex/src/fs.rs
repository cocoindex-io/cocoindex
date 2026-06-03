//! Filesystem walking with shared file resources.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use crate::ctx::Ctx;
use crate::error::{Error, Result};
pub use crate::file::{
    FileContentCache, FileLike, FileMetadata, FilePath, FilePathMatcher, FileSourceItem,
    MatchAllFilePathMatcher, PatternFilePathMatcher, decode_bytes,
};
use crate::target_state::{
    StableKey, TargetAction, TargetActionSink, TargetHandler, TargetReconcileOutput,
    TargetStateProvider, declare_target_state, register_root_target_states_provider,
};

#[derive(Clone)]
pub struct DirWalker {
    root: FilePath,
    recursive: bool,
    matcher: Arc<dyn FilePathMatcher>,
}

impl DirWalker {
    pub fn new(path: impl Into<FilePath>) -> Self {
        Self {
            root: path.into(),
            recursive: false,
            matcher: Arc::new(MatchAllFilePathMatcher),
        }
    }

    pub fn recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    pub fn path_matcher(mut self, matcher: impl FilePathMatcher + 'static) -> Self {
        self.matcher = Arc::new(matcher);
        self
    }

    pub fn items(&self) -> Result<Vec<(String, FileEntry)>> {
        Ok(self
            .walk()?
            .into_iter()
            .map(|file| (file.key(), file))
            .collect())
    }

    pub fn walk(&self) -> Result<Vec<FileEntry>> {
        walk_internal(&self.root, self.recursive, self.matcher.as_ref())
    }
}

pub fn walk_dir(path: impl Into<FilePath>) -> DirWalker {
    DirWalker::new(path)
}

#[cfg(feature = "fs_live")]
pub use live::LiveDirWalker;

#[cfg(feature = "fs_live")]
mod live {
    use super::{DirWalker, FileEntry};
    use crate::error::{Error, Result};
    use crate::live_component::{LiveMapFeed, LiveMapSubscriber, LiveMapView};
    use async_trait::async_trait;
    use notify::{RecursiveMode, Watcher};
    use std::path::PathBuf;

    /// A live directory feed: a [`LiveMapView`] over a directory that re-scans on
    /// filesystem changes. Build it with [`DirWalker::live`] and feed it to
    /// [`Ctx::mount_each_live`](crate::Ctx::mount_each_live) — the catch-up scan
    /// walks the directory once, then each filesystem change triggers a re-scan
    /// so files are mounted/removed as they appear and disappear (the Rust
    /// analogue of Python's `walk_dir(..., live=True)`).
    pub struct LiveDirWalker {
        walker: DirWalker,
        watch_root: PathBuf,
        recursive: bool,
        poll_interval: std::time::Duration,
    }

    impl DirWalker {
        /// Turn this walker into a live [`LiveDirWalker`] that watches its root
        /// directory (default poll interval: 1s — see
        /// [`LiveDirWalker::poll_interval`]).
        pub fn live(self) -> LiveDirWalker {
            let resolved = self.root.resolve();
            // Canonicalize so the watcher matches event paths (e.g. macOS
            // FSEvents reports `/private/tmp/…` for a `/tmp/…` symlink).
            let watch_root = std::fs::canonicalize(&resolved).unwrap_or(resolved);
            let recursive = self.recursive;
            LiveDirWalker {
                walker: self,
                watch_root,
                recursive,
                poll_interval: std::time::Duration::from_secs(1),
            }
        }
    }

    impl LiveDirWalker {
        /// Set how often the directory is polled for changes (default 1s).
        pub fn poll_interval(mut self, interval: std::time::Duration) -> Self {
            self.poll_interval = interval;
            self
        }
    }

    #[async_trait]
    impl LiveMapView<String, FileEntry> for LiveDirWalker {
        async fn scan(&self) -> Result<Vec<(String, FileEntry)>> {
            self.walker.items()
        }
    }

    #[async_trait]
    impl LiveMapFeed<String, FileEntry> for LiveDirWalker {
        async fn watch(&self, subscriber: LiveMapSubscriber<String, FileEntry>) -> Result<()> {
            use notify::{Config, PollWatcher};

            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
            // A polling watcher (rather than the OS-native backend) so live
            // watching works uniformly across platforms and on filesystems where
            // FSEvents/inotify are unavailable (containers, network/Docker
            // volumes, sandboxes). The poll interval bounds change latency.
            let config = Config::default().with_poll_interval(self.poll_interval);
            let mut watcher = PollWatcher::new(
                move |res: notify::Result<notify::Event>| {
                    if res.is_ok() {
                        let _ = tx.send(());
                    }
                },
                config,
            )
            .map_err(|e| Error::engine(format!("fs watcher: {e}")))?;
            let mode = if self.recursive {
                RecursiveMode::Recursive
            } else {
                RecursiveMode::NonRecursive
            };
            watcher
                .watch(&self.watch_root, mode)
                .map_err(|e| Error::engine(format!("fs watch {:?}: {e}", self.watch_root)))?;
            // Re-scan on each (coalesced) batch of filesystem events: the live
            // component's `update_all` re-runs `scan`, and `mount_each`
            // reconciles added/removed files. The watcher is kept alive for the
            // duration of this loop; dropping the future stops watching.
            while rx.recv().await.is_some() {
                while rx.try_recv().is_ok() {}
                subscriber.update_all().await?;
            }
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// FileEntry
// ---------------------------------------------------------------------------

/// A walked file with stable path metadata and lazy content.
#[derive(Clone, Serialize)]
pub struct FileEntry {
    root: FilePath,
    relative: PathBuf,
    size: u64,
    #[serde(with = "crate::file::system_time_serde")]
    modified: SystemTime,
    #[serde(skip)]
    cache: Arc<FileContentCache>,
}

impl FileEntry {
    /// Logical file path for this entry.
    pub fn file_path(&self) -> FilePath {
        self.root.join(&self.relative)
    }

    /// Full filesystem path.
    pub fn path(&self) -> PathBuf {
        self.file_path().resolve()
    }

    /// Relative path from walk root.
    pub fn relative_path(&self) -> &Path {
        &self.relative
    }

    /// File stem (name without extension).
    pub fn stem(&self) -> &str {
        self.relative
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("")
    }

    /// Read file contents as bytes.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the file cannot be opened or read.
    pub fn content(&self) -> Result<Vec<u8>> {
        std::fs::read(self.path()).map_err(Error::Io)
    }

    /// Read file contents as UTF-8 string.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the file cannot be read. Invalid UTF-8 is
    /// decoded with replacement characters, matching [`FileLike::read_text`].
    pub fn content_str(&self) -> Result<String> {
        Ok(decode_bytes(&self.content()?))
    }

    /// Stable key for component paths (relative path, forward slashes).
    pub fn key(&self) -> String {
        self.relative.to_string_lossy().replace('\\', "/")
    }
}

#[async_trait]
impl FileLike for FileEntry {
    fn file_path(&self) -> FilePath {
        FileEntry::file_path(self)
    }

    fn cache(&self) -> &FileContentCache {
        &self.cache
    }

    async fn fetch_metadata(&self) -> Result<FileMetadata> {
        let metadata = tokio::fs::metadata(self.path()).await.map_err(Error::Io)?;
        Ok(FileMetadata {
            size: metadata.len(),
            modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            content_fingerprint: None,
        })
    }

    async fn read_impl(&self, size: Option<usize>) -> Result<Vec<u8>> {
        match size {
            None => tokio::fs::read(self.path()).await.map_err(Error::Io),
            Some(size) => {
                use tokio::io::AsyncReadExt;
                let mut file = tokio::fs::File::open(self.path())
                    .await
                    .map_err(Error::Io)?;
                let mut buf = vec![0; size];
                let read = file.read(&mut buf).await.map_err(Error::Io)?;
                buf.truncate(read);
                Ok(buf)
            }
        }
    }
}

impl FileSourceItem for FileEntry {}

/// Walk a directory matching multiple glob patterns. Returns all matching files
/// sorted by relative path.
///
/// # Examples
/// ```ignore
/// let files = cocoindex::fs::walk("./src", &["**/*.rs", "**/*.toml"])?;
/// ```
pub fn walk(dir: impl AsRef<Path>, patterns: &[&str]) -> Result<Vec<FileEntry>> {
    let matcher = PatternFilePathMatcher::include(patterns.iter().copied())?;
    walk_internal(&FilePath::new(dir.as_ref()), true, &matcher)
}

fn walk_internal(
    root: &FilePath,
    recursive: bool,
    matcher: &dyn FilePathMatcher,
) -> Result<Vec<FileEntry>> {
    let dir = root.resolve();
    let dir = dir.as_path();
    let canonical_dir = match std::fs::canonicalize(dir) {
        Ok(path) => path,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(Error::Io(err)),
    };
    let mut files = Vec::new();

    let walker = if recursive {
        WalkDir::new(dir)
    } else {
        WalkDir::new(dir).max_depth(1)
    };
    for entry in walker.into_iter().filter_entry(|entry| {
        if entry.depth() == 0 || !entry.file_type().is_dir() {
            return true;
        }
        let path = entry.path();
        let relative = path
            .strip_prefix(dir)
            .or_else(|_| path.strip_prefix(&canonical_dir));
        relative.is_ok_and(|relative| matcher.is_dir_included(relative))
    }) {
        let entry = entry.map_err(|err| {
            let message = err.to_string();
            match err.into_io_error() {
                Some(io_err) => Error::Io(io_err),
                None => Error::engine(message),
            }
        })?;
        let path = entry.path();
        let relative = relative_path(dir, &canonical_dir, path)?;
        let metadata = std::fs::metadata(path).map_err(Error::Io)?;
        if metadata.is_dir() {
            continue;
        }
        if !metadata.is_file() {
            continue;
        }
        if !matcher.is_file_included(&relative) {
            continue;
        }

        files.push(FileEntry {
            root: root.clone(),
            relative,
            size: metadata.len(),
            modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            cache: Arc::new(FileContentCache::with_metadata(FileMetadata {
                size: metadata.len(),
                modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                content_fingerprint: None,
            })),
        });
    }

    files.sort_by(|a, b| a.relative.cmp(&b.relative));
    Ok(files)
}

fn relative_path(root: &Path, canonical_root: &Path, path: &Path) -> Result<PathBuf> {
    path.strip_prefix(root)
        .or_else(|_| path.strip_prefix(canonical_root))
        .map_err(|_| {
            Error::engine(format!(
                "walk path '{}' is outside root directory '{}'",
                path.display(),
                root.display()
            ))
        })
        .map(Path::to_path_buf)
}

// ---------------------------------------------------------------------------
// DirTarget
// ---------------------------------------------------------------------------

/// A declarative directory target.
///
/// Files you [`declare_file`](DirTarget::declare_file) are reconciled against
/// the previous run via CocoIndex's target-state engine:
/// * new or changed files are written,
/// * unchanged files are skipped (no rewrite),
/// * files declared in a previous run but **not** this run (e.g. their source
///   was deleted) are removed from disk.
#[derive(Clone)]
pub struct DirTarget {
    provider: TargetStateProvider<Vec<u8>>,
    dir: PathBuf,
}

#[derive(Clone, Debug)]
pub struct DirTargetState {
    dir: PathBuf,
    create_parent_dirs: bool,
}

pub fn dir_target(dir: impl Into<PathBuf>) -> DirTargetState {
    DirTargetState {
        dir: dir.into(),
        create_parent_dirs: true,
    }
}

impl DirTargetState {
    pub fn create_parent_dirs(mut self, create_parent_dirs: bool) -> Self {
        self.create_parent_dirs = create_parent_dirs;
        self
    }
}

/// Mount a declarative directory target rooted at `dir`. Declared files are
/// synced to disk; orphaned files are deleted on later runs.
///
/// Must be called inside an `App::update()`/`App::run()` pipeline.
pub fn mount_dir_target(ctx: &Ctx, dir: impl Into<PathBuf>) -> Result<DirTarget> {
    declare_dir_target(ctx, dir_target(dir))
}

pub fn declare_dir_target(ctx: &Ctx, target: DirTargetState) -> Result<DirTarget> {
    let dir = target.dir;
    if target.create_parent_dirs {
        std::fs::create_dir_all(&dir).map_err(Error::Io)?;
    } else if !dir.is_dir() {
        return Err(Error::engine(format!(
            "directory target does not exist: {}",
            dir.display()
        )));
    }
    let provider = register_root_target_states_provider(
        ctx,
        format!("cocoindex/localfs/dir/{}", dir.to_string_lossy()),
        FileHandler { dir: dir.clone() },
    )?;
    Ok(DirTarget { provider, dir })
}

impl DirTarget {
    /// Mount a declarative directory target. See [`mount_dir_target`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::{ctx::Ctx, fs::DirTarget};
    /// # async fn doc(ctx: &Ctx) -> cocoindex::error::Result<()> {
    /// let target = DirTarget::mount(ctx, "./output")?;
    /// target.declare_file(ctx, "result.txt", b"final output")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn mount(ctx: &Ctx, dir: impl Into<PathBuf>) -> Result<Self> {
        mount_dir_target(ctx, dir)
    }

    /// The target directory path.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Declare that a file with `content` should exist at `name` (a relative
    /// path under the target directory). The actual write/skip/delete is decided
    /// by the engine during reconciliation.
    ///
    /// # Errors
    /// Returns an error if `name` is absolute or contains `..`, or if recording
    /// the target state fails.
    pub fn declare_file(&self, ctx: &Ctx, name: &str, content: &[u8]) -> Result<()> {
        validate_relative_name(name)?;
        declare_target_state(ctx, self.provider.target_state(name, content.to_vec()))
    }
}

fn validate_relative_name(name: &str) -> Result<()> {
    let path = Path::new(name);
    let has_parent = path
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir));
    let has_prefix = path
        .components()
        .any(|c| matches!(c, std::path::Component::Prefix(_)));
    if name.is_empty() || path.has_root() || has_prefix || has_parent {
        return Err(Error::engine(
            "declare_file name must be a non-empty relative path without '..'",
        ));
    }
    Ok(())
}

/// What the sink should do for one file: write `content`, or delete when `None`.
#[derive(Serialize, Deserialize)]
struct FileAction {
    name: String,
    content: Option<Vec<u8>>,
}

/// Directory target handler. Each file is one leaf target state under the
/// directory root provider.
struct FileHandler {
    dir: PathBuf,
}

impl TargetHandler<Vec<u8>> for FileHandler {
    type TrackingRecord = Fingerprint;
    type Action = FileAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<Vec<u8>>,
        prev: Vec<Fingerprint>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<FileAction, Fingerprint>>> {
        let StableKey::Str(name) = &key else {
            return Err(Error::engine(format!(
                "unexpected file target key: {key:?}"
            )));
        };
        let name = name.to_string();

        let desired_fp = match &desired {
            Some(content) => Some(Fingerprint::from(content).map_err(Error::from)?),
            None => None,
        };
        // Skip when nothing changed.
        let prev_same = desired_fp
            .as_ref()
            .is_some_and(|fp| prev.iter().any(|p| p == fp));
        if desired.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }
        Ok(Some(TargetReconcileOutput {
            action: TargetAction::Update(FileAction {
                name,
                content: desired,
            }),
            sink: self.dir_sink(),
            tracking_record: desired_fp,
            child_invalidation: None,
        }))
    }
}

impl FileHandler {
    fn dir_sink(&self) -> TargetActionSink<FileAction> {
        let dir = self.dir.clone();
        TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<FileAction>>| {
            let dir = dir.clone();
            async move {
                let result =
                    tokio::task::spawn_blocking(move || -> std::result::Result<(), String> {
                        for action in actions {
                            let file = match action {
                                TargetAction::Create(f)
                                | TargetAction::Update(f)
                                | TargetAction::Delete(f) => f,
                            };
                            let path = dir.join(&file.name);
                            match file.content {
                                Some(content) => {
                                    if let Some(parent) = path.parent() {
                                        std::fs::create_dir_all(parent)
                                            .map_err(|e| e.to_string())?;
                                    }
                                    std::fs::write(&path, &content).map_err(|e| e.to_string())?;
                                }
                                None => match std::fs::remove_file(&path) {
                                    Ok(()) => {}
                                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                                    Err(e) => return Err(e.to_string()),
                                },
                            }
                        }
                        Ok(())
                    })
                    .await;
                match result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(Error::engine(e)),
                    Err(e) => Err(Error::engine(format!("dir target sink task panicked: {e}"))),
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walk_finds_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), "hello").unwrap();
        std::fs::write(dir.path().join("b.rs"), "fn main() {}").unwrap();
        std::fs::create_dir_all(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub/c.rs"), "mod sub;").unwrap();

        let files = walk(dir.path(), &["**/*.rs"]).unwrap();
        assert_eq!(files.len(), 2);
        let keys: Vec<String> = files.iter().map(|f| f.key()).collect();
        assert!(keys.contains(&"b.rs".to_string()));
        assert!(keys.contains(&"sub/c.rs".to_string()));
    }

    #[test]
    fn walk_multiple_patterns() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.rs"), "").unwrap();
        std::fs::write(dir.path().join("b.py"), "").unwrap();
        std::fs::write(dir.path().join("c.txt"), "").unwrap();

        let files = walk(dir.path(), &["**/*.rs", "**/*.py"]).unwrap();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn walk_combined_patterns_include_root_and_nested_without_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("root.py"), "").unwrap();
        std::fs::create_dir_all(dir.path().join("pkg")).unwrap();
        std::fs::write(dir.path().join("pkg/nested.py"), "").unwrap();

        let files = walk(dir.path(), &["*.py", "**/*.py"]).unwrap();
        let keys: Vec<String> = files.iter().map(|f| f.key()).collect();
        assert_eq!(
            keys,
            vec!["pkg/nested.py".to_string(), "root.py".to_string()]
        );
    }

    #[test]
    fn walk_deduplicates() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.rs"), "").unwrap();

        // Both patterns match the same file
        let files = walk(dir.path(), &["**/*.rs", "a.*"]).unwrap();
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn file_entry_accessors() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("test.txt"), "hello world").unwrap();
        let files = walk(dir.path(), &["*.txt"]).unwrap();
        assert_eq!(files[0].stem(), "test");
        assert_eq!(files[0].content_str().unwrap(), "hello world");
        assert_eq!(files[0].content().unwrap(), b"hello world");
    }

    #[test]
    fn file_entry_text_decode_strips_utf8_bom_and_replaces_invalid_utf8() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("bom.txt"), b"\xEF\xBB\xBFhello").unwrap();
        std::fs::write(dir.path().join("utf16.txt"), b"\xFF\xFEh\0i\0").unwrap();
        std::fs::write(dir.path().join("bad.txt"), b"a\xFFb").unwrap();
        let files = walk(dir.path(), &["*.txt"]).unwrap();
        let by_key = files
            .iter()
            .map(|file| (file.key(), file.content_str().unwrap()))
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(by_key["bom.txt"], "hello");
        assert_eq!(by_key["utf16.txt"], "hi");
        assert_eq!(by_key["bad.txt"], "a\u{fffd}b");
    }

    #[tokio::test]
    async fn file_entry_uses_shared_async_filelike_api() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("test.txt"), "hello world").unwrap();
        let file = walk_dir(FilePath::with_base_dir(
            "source",
            dir.path(),
            PathBuf::new(),
        ))
        .path_matcher(PatternFilePathMatcher::include(["*.txt"]).unwrap())
        .walk()
        .unwrap()
        .pop()
        .unwrap();

        assert_eq!(file.file_path().path(), Path::new("test.txt"));
        assert_eq!(file.file_path().resolve(), dir.path().join("test.txt"));
        assert_eq!(FileSourceItem::key(&file), "test.txt");
        assert_eq!(FileLike::metadata(&file).await.unwrap().size, 11);
        assert_eq!(FileLike::read_size(&file, 5).await.unwrap(), b"hello");
        assert_eq!(FileLike::read(&file).await.unwrap(), b"hello world");
        assert_eq!(FileLike::read_size(&file, 5).await.unwrap(), b"hello");
        assert_eq!(FileLike::read_text(&file).await.unwrap(), "hello world");
        assert_eq!(
            FileLike::content_fingerprint(&file).await.unwrap(),
            FileLike::content_fingerprint(&file).await.unwrap()
        );
    }

    #[test]
    fn walk_rejects_paths_outside_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let canonical_root = std::fs::canonicalize(&root).unwrap();
        let outside = root.parent().unwrap().join("outside.txt");
        std::fs::write(&outside, b"bad").unwrap();

        let err = relative_path(&root, &canonical_root, &outside);
        assert!(err.is_err());
    }

    #[test]
    fn walk_missing_root_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");
        let files = walk(missing, &["**/*.txt"]).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn file_path_memo_key_is_stable_across_base_dir_moves() {
        let old = tempfile::tempdir().unwrap();
        let new = tempfile::tempdir().unwrap();
        let old_path = FilePath::with_base_dir("docs", old.path(), "guide/index.md");
        let new_path = FilePath::with_base_dir("docs", new.path(), "guide/index.md");

        let old_key = rmp_serde::to_vec(&old_path.memo_key()).unwrap();
        let new_key = rmp_serde::to_vec(&new_path.memo_key()).unwrap();
        assert_eq!(old_key, new_key);
        assert_ne!(old_path.resolve(), new_path.resolve());
    }

    #[test]
    fn pattern_file_path_matcher_includes_and_excludes_files_and_dirs() {
        let matcher =
            PatternFilePathMatcher::new(["**/*.rs", "*.toml"], ["target/**", "**/*.gen.rs"])
                .unwrap();

        assert!(matcher.is_file_included(Path::new("src/lib.rs")));
        assert!(matcher.is_file_included(Path::new("Cargo.toml")));
        assert!(!matcher.is_file_included(Path::new("src/generated.gen.rs")));
        assert!(!matcher.is_file_included(Path::new("target/debug/build.rs")));
        assert!(!matcher.is_dir_included(Path::new("target/debug")));
    }

    #[test]
    fn dir_walker_items_respects_recursive_and_matcher_options() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("root.rs"), "").unwrap();
        std::fs::write(dir.path().join("root.txt"), "").unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("src/lib.rs"), "").unwrap();
        std::fs::create_dir_all(dir.path().join("target/debug")).unwrap();
        std::fs::write(dir.path().join("target/debug/generated.rs"), "").unwrap();

        let nonrecursive = walk_dir(dir.path())
            .path_matcher(PatternFilePathMatcher::include(["**/*.rs"]).unwrap())
            .items()
            .unwrap();
        assert_eq!(
            nonrecursive
                .iter()
                .map(|(key, _)| key.as_str())
                .collect::<Vec<_>>(),
            vec!["root.rs"]
        );

        let recursive = walk_dir(dir.path())
            .recursive(true)
            .path_matcher(PatternFilePathMatcher::new(["**/*.rs"], ["target/**"]).unwrap())
            .items()
            .unwrap();
        assert_eq!(
            recursive
                .iter()
                .map(|(key, _)| key.as_str())
                .collect::<Vec<_>>(),
            vec!["root.rs", "src/lib.rs"]
        );
    }

    #[test]
    fn declare_dir_target_requires_existing_dir_when_parent_creation_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let app = crate::App::builder("declare_dir_target_existing_check")
            .db_path(dir.path().join("lmdb"))
            .build_blocking()
            .unwrap();
        let missing = dir.path().join("missing").join("out");
        let err = app
            .update_blocking(move |ctx| async move {
                declare_dir_target(&ctx, dir_target(&missing).create_parent_dirs(false))?;
                Ok(())
            })
            .unwrap_err();
        assert!(err.to_string().contains("directory target does not exist"));
    }

    #[test]
    fn dir_target_name_validation_rejects_traversal_and_absolute() {
        // Relative `..` traversal.
        assert!(validate_relative_name("../escape.txt").is_err());
        assert!(validate_relative_name("sub/../../escape.txt").is_err());
        // Absolute / rooted paths.
        assert!(validate_relative_name("/absolute/path").is_err());
        // Empty.
        assert!(validate_relative_name("").is_err());
        // Valid relative paths (including nested) are accepted.
        assert!(validate_relative_name("valid.txt").is_ok());
        assert!(validate_relative_name("sub/valid.txt").is_ok());

        let err = validate_relative_name("../x").unwrap_err();
        assert!(err.to_string().contains("relative path without '..'"));
    }
}
