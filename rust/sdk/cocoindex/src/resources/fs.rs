//! Filesystem walking with shared file resources.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use crate::ctx::{ContextKey, Ctx};
use crate::error::{Error, Result};
pub use crate::resources::file::{
    FileContentCache, FileLike, FileMetadata, FilePath, FilePathMatcher, FileSourceItem,
    MatchAllFilePathMatcher, PatternFilePathMatcher, decode_bytes,
};
use crate::target_state::{
    STATIC_ROOT_PROVIDERS, SinkCache, StableKey, StaticRootProvider, TargetAction,
    TargetActionSink, TargetHandler, TargetReconcileOutput, TargetStateProvider,
    declare_target_state,
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
    use crate::live_component::{LiveMapFeed, LiveMapSubscriber, LiveMapView, SingleWatcherGuard};
    use async_trait::async_trait;
    use notify::{Config, RecursiveMode, Watcher};
    use std::path::PathBuf;

    /// A live directory feed: a [`LiveMapView`] over a directory that re-scans on
    /// filesystem changes. Build it with [`DirWalker::live`] and feed it to
    /// [`Ctx::mount_each_live`](crate::Ctx::mount_each_live) — the catch-up scan
    /// walks the directory once, then each filesystem change triggers a re-scan
    /// so files are mounted/removed as they appear and disappear (the Rust
    /// analogue of Python's `walk_dir(..., live=True)`).
    ///
    /// Changes come from the platform's native file events (inotify, FSEvents,
    /// ReadDirectoryChangesW), as Python's `watchdog` observer does. Where the
    /// native watcher cannot be set up (a network or Docker volume, a sandbox),
    /// the directory is polled instead, every
    /// [`poll_interval`](LiveDirWalker::poll_interval).
    pub struct LiveDirWalker {
        walker: DirWalker,
        watch_root: PathBuf,
        recursive: bool,
        poll_interval: std::time::Duration,
        watch_guard: SingleWatcherGuard,
    }

    impl DirWalker {
        /// Turn this walker into a live [`LiveDirWalker`] that watches its root
        /// directory.
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
                watch_guard: SingleWatcherGuard::new("LiveDirWalker"),
            }
        }
    }

    impl LiveDirWalker {
        /// Set how often the directory is polled for changes when the native
        /// watcher is unavailable (default 1s).
        pub fn poll_interval(mut self, interval: std::time::Duration) -> Self {
            self.poll_interval = interval;
            self
        }
    }

    /// Watch `root`, sending `()` on every change, with the platform's native
    /// backend; when creating or arming it fails, the poller takes over and
    /// the reason is logged. The returned watcher stops when dropped.
    fn start_watcher(
        root: &std::path::Path,
        mode: RecursiveMode,
        poll_interval: std::time::Duration,
        tx: tokio::sync::mpsc::UnboundedSender<()>,
    ) -> Result<Box<dyn Watcher + Send>> {
        let handler = move |res: notify::Result<notify::Event>| {
            if res.is_ok() {
                let _ = tx.send(());
            }
        };
        let native = notify::RecommendedWatcher::new(handler.clone(), Config::default())
            .and_then(|mut watcher| watcher.watch(root, mode).map(|()| watcher));
        match native {
            Ok(watcher) => Ok(Box::new(watcher)),
            Err(e) => {
                tracing::warn!(
                    "fs watch {}: native file events unavailable ({e}); polling every {poll_interval:?}",
                    root.display()
                );
                let config = Config::default().with_poll_interval(poll_interval);
                let mut watcher = notify::PollWatcher::new(handler, config)
                    .map_err(|e| Error::engine(format!("fs watcher: {e}")))?;
                watcher
                    .watch(root, mode)
                    .map_err(|e| Error::engine(format!("fs watch {}: {e}", root.display())))?;
                Ok(Box::new(watcher))
            }
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
            let _watch_token = self.watch_guard.enter()?;
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
            let mode = if self.recursive {
                RecursiveMode::Recursive
            } else {
                RecursiveMode::NonRecursive
            };
            let _watcher = start_watcher(&self.watch_root, mode, self.poll_interval, tx)?;
            // The watcher reports only changes after the `watch` call just
            // above, while the framework's catch-up scan ran before this
            // method was called, so a file created or deleted between the two
            // would never produce an event: rescan once now that the watch is
            // armed, as the Python localfs source does.
            subscriber.update_all().await?;
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
    #[serde(with = "crate::resources::file::system_time_serde")]
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

crate::memo::impl_file_memo_input!(FileEntry);

/// Walk a directory matching multiple glob patterns. Returns all matching files
/// sorted by relative path.
///
/// # Examples
/// ```ignore
/// let files = cocoindex::resources::fs::walk("./src", &["**/*.rs", "**/*.toml"])?;
/// ```
pub fn walk(dir: impl AsRef<Path>, patterns: &[&str]) -> Result<Vec<FileEntry>> {
    let matcher = PatternFilePathMatcher::include(patterns.iter().copied())?;
    walk_internal(&FilePath::new(dir.as_ref()), true, &matcher)
}

/// Like [`walk`], but yields `(stable key, file)` pairs ready to feed to
/// `mount_each!`, which expects `(key, value)` items (each file's
/// [`FileEntry::key`] is its relative path).
pub fn walk_items(dir: impl AsRef<Path>, patterns: &[&str]) -> Result<Vec<(String, FileEntry)>> {
    Ok(walk(dir, patterns)?
        .into_iter()
        .map(|file| (file.key(), file))
        .collect())
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

impl FilePath {
    /// A path under the directory provided for `base_dir` (Python's
    /// `FilePath(path, base_dir=KEY)`): memo keys and target-state keys name
    /// the key, not the directory, so the whole tree can move between runs
    /// without invalidating memoized work or orphaning declared files.
    pub fn under_context_dir(
        ctx: &Ctx,
        base_dir: &ContextKey<PathBuf>,
        path: impl Into<PathBuf>,
    ) -> Result<Self> {
        let base = ctx.get_key(base_dir)?;
        Ok(Self::with_base_dir(base_dir.name(), base.clone(), path))
    }
}

/// A declarative directory target.
///
/// Files you [`declare_file`](DirTarget::declare_file) and subdirectories you
/// [`declare_dir_target`](DirTarget::declare_dir_target) are reconciled against
/// the previous run via CocoIndex's target-state engine:
/// * new or changed files are written, new directories are created,
/// * unchanged files are skipped (no rewrite),
/// * files and subdirectories declared in a previous run but **not** this run
///   (e.g. their source was deleted, or the directory is no longer mounted) are
///   removed from disk — a subdirectory with everything under it,
/// * [`App::drop_state`](crate::App::drop_state) removes every declared entry.
///
/// A directory target handle. As a `mount_each!` / `use_mount!` argument it
/// fingerprints by its path (its stable key) — the `provider` is a runtime
/// handle, not part of the component-memo identity.
#[derive(Clone, Serialize)]
pub struct DirTarget {
    #[serde(skip)]
    provider: TargetStateProvider<FsEntry>,
    /// The root directory target this handle belongs to.
    root: FilePath,
    /// This handle's directory, relative to `root` (empty for the root itself).
    sub: PathBuf,
}

#[derive(Clone, Debug)]
pub struct DirTargetState {
    dir: FilePath,
    create_parent_dirs: bool,
}

/// A directory target at `dir`: a path, or a [`FilePath`] under a context
/// directory ([`FilePath::under_context_dir`]). Parent directories are created
/// unless [`create_parent_dirs(false)`](DirTargetState::create_parent_dirs).
pub fn dir_target(dir: impl Into<FilePath>) -> DirTargetState {
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
pub fn mount_dir_target(ctx: &Ctx, dir: impl Into<FilePath>) -> Result<DirTarget> {
    declare_dir_target(ctx, dir_target(dir))
}

pub fn declare_dir_target(ctx: &Ctx, target: DirTargetState) -> Result<DirTarget> {
    let resolved = target.dir.resolve();
    if target.create_parent_dirs {
        std::fs::create_dir_all(&resolved).map_err(Error::Io)?;
    } else if !resolved.is_dir() {
        return Err(Error::engine(format!(
            "directory target does not exist: {}",
            resolved.display()
        )));
    }
    let provider = FILE_PROVIDER.get(ctx)?;
    Ok(DirTarget {
        provider,
        root: target.dir,
        sub: PathBuf::new(),
    })
}

/// Options for [`DirTarget::declare_file_with_options`] and
/// [`declare_file_with_options`].
#[derive(Clone, Copy, Debug, Default)]
pub struct FileOptions {
    /// Create the file's parent directories when they do not exist. Off by
    /// default: a missing parent is an error when the file is written.
    pub create_parent_dirs: bool,
}

impl DirTarget {
    /// Mount a declarative directory target. See [`mount_dir_target`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::{ctx::Ctx, resources::fs::DirTarget};
    /// # async fn doc(ctx: &Ctx) -> cocoindex::error::Result<()> {
    /// let target = DirTarget::mount(ctx, "./output")?;
    /// target.declare_file(ctx, "result.txt", b"final output")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn mount(ctx: &Ctx, dir: impl Into<FilePath>) -> Result<Self> {
        mount_dir_target(ctx, dir)
    }

    /// The target directory path, resolved.
    pub fn dir(&self) -> PathBuf {
        self.root.resolve().join(&self.sub)
    }

    /// Declare that a file with `content` should exist at `name` (a relative
    /// path under this directory). The actual write/skip/delete is decided by
    /// the engine during reconciliation. The file's parent directories must
    /// exist; see [`declare_file_with_options`](Self::declare_file_with_options).
    ///
    /// # Errors
    /// Returns an error if `name` is absolute, empty or leaves this directory,
    /// or if recording the target state fails.
    pub fn declare_file(&self, ctx: &Ctx, name: &str, content: &[u8]) -> Result<()> {
        self.declare_file_with_options(ctx, name, content, FileOptions::default())
    }

    /// [`declare_file`](Self::declare_file) with [`FileOptions`].
    pub fn declare_file_with_options(
        &self,
        ctx: &Ctx,
        name: &str,
        content: &[u8],
        options: FileOptions,
    ) -> Result<()> {
        let name = normalize_entry_name(name)?;
        declare_target_state(
            ctx,
            self.provider.target_state(
                self.entry_key(&name)?,
                FsEntry::File {
                    content: content.to_vec(),
                    create_parent_dirs: options.create_parent_dirs,
                },
            ),
        )
    }

    /// Declare a subdirectory target at `path` (relative to this directory) and
    /// return a handle for declaring entries in it. The subdirectory is a
    /// target state of its own: created when first declared, removed with
    /// everything under it when no longer declared. Its parent directories
    /// must exist unless `create_parent_dirs`.
    pub fn declare_dir_target(
        &self,
        ctx: &Ctx,
        path: &str,
        create_parent_dirs: bool,
    ) -> Result<DirTarget> {
        let path = normalize_entry_name(path)?;
        declare_target_state(
            ctx,
            self.provider.target_state(
                self.entry_key(&path)?,
                FsEntry::Dir { create_parent_dirs },
            ),
        )?;
        Ok(DirTarget {
            provider: self.provider.clone(),
            root: self.root.clone(),
            sub: self.sub.join(path),
        })
    }

    /// The target-state key of the entry `name` under this directory:
    /// `[base_key, root, path]`, from which the handler rebuilds the path.
    fn entry_key(&self, name: &Path) -> Result<StableKey> {
        entry_key(&self.root, &self.sub.join(name))
    }
}

/// Declare a single file target at `path` (a path, or a [`FilePath`] under a
/// context directory) without a directory target: the Rust analogue of Python's
/// standalone `localfs.declare_file`. The file is written, kept up to date, and
/// removed when no longer declared.
pub fn declare_file(ctx: &Ctx, path: impl Into<FilePath>, content: &[u8]) -> Result<()> {
    declare_file_with_options(ctx, path, content, FileOptions::default())
}

/// [`declare_file`] with [`FileOptions`].
pub fn declare_file_with_options(
    ctx: &Ctx,
    path: impl Into<FilePath>,
    content: &[u8],
    options: FileOptions,
) -> Result<()> {
    let path = path.into();
    let provider = FILE_PROVIDER.get(ctx)?;
    declare_target_state(
        ctx,
        provider.target_state(
            entry_key(&path, Path::new(""))?,
            FsEntry::File {
                content: content.to_vec(),
                create_parent_dirs: options.create_parent_dirs,
            },
        ),
    )
}

/// `[base_key, root, path]`: the base directory's context-key name (empty when
/// the root is a plain path), the root as written, and the entry's path
/// relative to the root (empty for the root itself).
fn entry_key(root: &FilePath, relative: &Path) -> Result<StableKey> {
    let to_str = |path: &Path| -> Result<Arc<str>> {
        path.to_str().map(Arc::from).ok_or_else(|| {
            Error::engine(format!(
                "directory target path is not valid UTF-8: {}",
                path.display()
            ))
        })
    };
    Ok(StableKey::Array(Arc::from([
        StableKey::Str(Arc::from(root.base_key().unwrap_or(""))),
        StableKey::Str(to_str(root.path())?),
        StableKey::Str(to_str(relative)?),
    ])))
}

/// Normalize an entry name: `.` segments drop out; the name must be relative,
/// non-empty and must not leave the directory.
fn normalize_entry_name(name: &str) -> Result<PathBuf> {
    let path = Path::new(name);
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(part) => out.push(part),
            std::path::Component::CurDir => {}
            _ => {
                return Err(Error::engine(format!(
                    "entry name must be a relative path inside the directory, without '..': {name:?}"
                )));
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(Error::engine(format!(
            "entry name must name an entry inside the directory, not the directory itself: {name:?}"
        )));
    }
    Ok(out)
}

/// What a directory target declares at an entry.
#[derive(Clone, Serialize, Deserialize)]
pub enum FsEntry {
    File {
        content: Vec<u8>,
        create_parent_dirs: bool,
    },
    Dir {
        create_parent_dirs: bool,
    },
}

/// What the sink should do for one entry.
#[derive(Serialize, Deserialize)]
struct FsAction {
    /// The context key the root directory is provided under, resolved when the
    /// action applies; `None` for a plain path.
    base_key: Option<String>,
    /// The entry's path: under the base directory, or as written.
    path: PathBuf,
    op: FsOp,
}

#[derive(Serialize, Deserialize)]
enum FsOp {
    WriteFile {
        content: Vec<u8>,
        create_parent_dirs: bool,
    },
    CreateDir {
        create_parent_dirs: bool,
    },
    RemoveFile,
    RemoveDir,
}

#[linkme::distributed_slice(STATIC_ROOT_PROVIDERS)]
static FILE_PROVIDER: StaticRootProvider =
    StaticRootProvider::new::<FsEntry, FileHandler>("cocoindex/localfs/file");

/// The tracking record of a declared directory (a file's is its content's
/// fingerprint).
fn dir_fingerprint() -> Fingerprint {
    Fingerprint::from_bytes(b"cocoindex/localfs/dir")
}

/// Directory target handler. Each entry is one leaf target state keyed by
/// `[base_key, root, path]`, so one handler serves every directory target.
#[derive(Default)]
struct FileHandler {
    sink: SinkCache<FsAction>,
}

impl TargetHandler<FsEntry> for FileHandler {
    type TrackingRecord = Fingerprint;
    type Action = FsAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<FsEntry>,
        prev: Vec<Fingerprint>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<FsAction, Fingerprint>>> {
        let (base_key, path) = entry_path(&key)?;

        let desired_fp = match &desired {
            Some(FsEntry::File { content, .. }) => {
                Some(Fingerprint::from(content).map_err(Error::from)?)
            }
            Some(FsEntry::Dir { .. }) => Some(dir_fingerprint()),
            None => None,
        };
        // Skip when nothing changed.
        let prev_same = desired_fp
            .as_ref()
            .is_some_and(|fp| !prev.is_empty() && prev.iter().all(|p| p == fp));
        if desired.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }
        let op = match desired {
            Some(FsEntry::File {
                content,
                create_parent_dirs,
            }) => FsOp::WriteFile {
                content,
                create_parent_dirs,
            },
            Some(FsEntry::Dir { create_parent_dirs }) => FsOp::CreateDir { create_parent_dirs },
            // A removed entry: a directory if it was tracked as one.
            None if prev.iter().any(|p| *p == dir_fingerprint()) => FsOp::RemoveDir,
            None => FsOp::RemoveFile,
        };
        Ok(Some(TargetReconcileOutput {
            action: TargetAction::Update(FsAction { base_key, path, op }),
            sink: self.sink.get(file_sink),
            tracking_record: desired_fp,
            child_invalidation: None,
        }))
    }
}

/// The base key and path an `[base_key, root, path]` target-state key names.
fn entry_path(key: &StableKey) -> Result<(Option<String>, PathBuf)> {
    if let StableKey::Array(parts) = key
        && let [StableKey::Str(base_key), StableKey::Str(root), StableKey::Str(relative)] =
            &parts[..]
    {
        let base_key = (!base_key.is_empty()).then(|| base_key.to_string());
        let root = PathBuf::from(&**root);
        // A standalone file target is its own root (an empty relative part);
        // joining "" would add a trailing separator and name a directory.
        let path = if relative.is_empty() {
            root
        } else {
            root.join(&**relative)
        };
        return Ok((base_key, path));
    }
    Err(Error::engine(format!(
        "unexpected file target key: {key:?}"
    )))
}

/// Apply a batch of entry actions: directories are created before the files
/// in them, and files are removed before the directories they were in.
fn apply_fs_actions(actions: Vec<(PathBuf, FsOp)>) -> std::io::Result<()> {
    let mut creates = Vec::new();
    let mut writes = Vec::new();
    let mut file_removes = Vec::new();
    let mut dir_removes = Vec::new();
    for (path, op) in actions {
        match op {
            FsOp::CreateDir { create_parent_dirs } => creates.push((path, create_parent_dirs)),
            FsOp::WriteFile {
                content,
                create_parent_dirs,
            } => writes.push((path, content, create_parent_dirs)),
            FsOp::RemoveFile => file_removes.push(path),
            FsOp::RemoveDir => dir_removes.push(path),
        }
    }
    creates.sort_by(|a, b| a.0.cmp(&b.0));
    for (path, create_parent_dirs) in creates {
        if create_parent_dirs {
            std::fs::create_dir_all(&path)?;
        } else if let Err(e) = std::fs::create_dir(&path)
            && !(e.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir())
        {
            return Err(e);
        }
    }
    for (path, content, create_parent_dirs) in writes {
        if create_parent_dirs && let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, &content)?;
    }
    for path in file_removes {
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    // Deepest first, so a removed tree's subdirectories go before it.
    dir_removes.sort_by(|a, b| b.cmp(a));
    for path in dir_removes {
        if path.is_dir() {
            std::fs::remove_dir_all(&path)?;
        }
    }
    Ok(())
}

fn file_sink() -> TargetActionSink<FsAction> {
    TargetActionSink::from_async_fn_with_ctx(
        move |host_ctx, actions: Vec<TargetAction<FsAction>>| async move {
            let mut resolved = Vec::with_capacity(actions.len());
            for action in actions {
                let entry = match action {
                    TargetAction::Create(f) | TargetAction::Update(f) | TargetAction::Delete(f) => {
                        f
                    }
                };
                let path = match &entry.base_key {
                    Some(base_key) => host_ctx
                        .resolve::<PathBuf>(base_key)
                        .ok_or_else(|| {
                            Error::engine(format!(
                                "directory target: base directory `{base_key}` was not provided \
                                 in the environment (call Environment::builder().provide_key(&KEY, dir))"
                            ))
                        })?
                        .join(&entry.path),
                    None => entry.path,
                };
                resolved.push((path, entry.op));
            }
            let result = tokio::task::spawn_blocking(move || apply_fs_actions(resolved)).await;
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(Error::Io(e)),
                Err(e) => Err(Error::engine(format!("dir target sink task panicked: {e}"))),
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(path: &str) -> StableKey {
        StableKey::Array(Arc::from([
            StableKey::Str("".into()),
            StableKey::Str("unused".into()),
            StableKey::Str(path.into()),
        ]))
    }

    fn file(content: &[u8]) -> FsEntry {
        FsEntry::File {
            content: content.to_vec(),
            create_parent_dirs: false,
        }
    }

    #[test]
    fn file_target_rewrites_unless_every_previous_state_matches() {
        let handler = FileHandler::default();
        let fp = |content: &[u8]| Fingerprint::from(&content.to_vec()).unwrap();

        let unchanged = handler
            .reconcile(key("a.txt"), Some(file(b"A")), vec![fp(b"A")], false)
            .unwrap();
        assert!(unchanged.is_none(), "an unchanged file is not rewritten");

        let maybe_missing = handler
            .reconcile(key("a.txt"), Some(file(b"A")), vec![fp(b"A")], true)
            .unwrap();
        assert!(maybe_missing.is_some(), "a possibly-missing file is rewritten");

        let ambiguous = handler
            .reconcile(key("a.txt"), Some(file(b"A")), vec![fp(b"A"), fp(b"B")], false)
            .unwrap();
        assert!(ambiguous.is_some(), "an ambiguous previous state is rewritten");

        let changed = handler
            .reconcile(key("a.txt"), Some(file(b"B")), vec![fp(b"A")], false)
            .unwrap();
        assert!(changed.is_some(), "a changed file is rewritten");
    }

    #[test]
    fn removed_entries_are_removed_as_what_they_were() {
        let handler = FileHandler::default();
        let out = handler
            .reconcile(key("sub"), None, vec![dir_fingerprint()], false)
            .unwrap()
            .unwrap();
        let TargetAction::Update(action) = out.action else {
            panic!("expected an update action");
        };
        assert!(matches!(action.op, FsOp::RemoveDir));
        assert_eq!(action.path, Path::new("unused/sub"));
        assert_eq!(action.base_key, None);

        let out = handler
            .reconcile(key("a.txt"), None, vec![Fingerprint::from_bytes(b"A")], false)
            .unwrap()
            .unwrap();
        let TargetAction::Update(action) = out.action else {
            panic!("expected an update action");
        };
        assert!(matches!(action.op, FsOp::RemoveFile));

        let known_absent = handler.reconcile(key("a.txt"), None, vec![], false).unwrap();
        assert!(known_absent.is_none());
    }

    #[test]
    fn entry_names_are_normalized_and_contained() {
        assert_eq!(normalize_entry_name("./a/./b.txt").unwrap(), Path::new("a/b.txt"));
        assert!(normalize_entry_name("../b.txt").is_err());
        assert!(normalize_entry_name("a/../../b.txt").is_err());
        assert!(normalize_entry_name("/abs.txt").is_err());
        assert!(normalize_entry_name("").is_err());
        assert!(normalize_entry_name(".").is_err());
    }

    #[test]
    fn a_batch_creates_dirs_before_files_and_removes_files_before_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        apply_fs_actions(vec![
            (
                root.join("sub/a.txt"),
                FsOp::WriteFile {
                    content: b"A".to_vec(),
                    create_parent_dirs: false,
                },
            ),
            (
                root.join("sub"),
                FsOp::CreateDir {
                    create_parent_dirs: false,
                },
            ),
        ])
        .unwrap();
        assert_eq!(std::fs::read(root.join("sub/a.txt")).unwrap(), b"A");

        // A missing parent is an error without create_parent_dirs, and fine with.
        let missing = apply_fs_actions(vec![(
            root.join("nowhere/b.txt"),
            FsOp::WriteFile {
                content: b"B".to_vec(),
                create_parent_dirs: false,
            },
        )]);
        assert!(missing.is_err());
        apply_fs_actions(vec![(
            root.join("deep/er/b.txt"),
            FsOp::WriteFile {
                content: b"B".to_vec(),
                create_parent_dirs: true,
            },
        )])
        .unwrap();
        assert_eq!(std::fs::read(root.join("deep/er/b.txt")).unwrap(), b"B");

        apply_fs_actions(vec![
            (root.join("sub"), FsOp::RemoveDir),
            (root.join("sub/a.txt"), FsOp::RemoveFile),
            (root.join("deep"), FsOp::RemoveDir),
        ])
        .unwrap();
        assert!(!root.join("sub").exists());
        assert!(!root.join("deep").exists());
    }
}
