//! Filesystem walking with shared file resources.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};

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

    /// `(key, file)` pairs for `mount_each`, sorted by key. Blocks on the
    /// directory walk; see [`walk`](Self::walk).
    pub fn items(&self) -> Result<Vec<(String, FileEntry)>> {
        Ok(self
            .walk()?
            .into_iter()
            .map(|file| (file.key(), file))
            .collect())
    }

    /// Walk the directory and return its matching files, sorted by relative
    /// path. This blocks on filesystem I/O: call it before the pipeline starts
    /// or from a blocking context. A live feed ([`live`](Self::live)) walks on
    /// a blocking thread.
    ///
    /// Symlinked directories are followed (a cycle is not entered twice), and
    /// a directory this process cannot read is skipped, as in the Python SDK.
    pub fn walk(&self) -> Result<Vec<FileEntry>> {
        let mut files = walk_files(&self.root, Path::new(""), self.recursive, &*self.matcher)?;
        files.sort_by(|a, b| a.relative.cmp(&b.relative));
        Ok(files)
    }
}

pub fn walk_dir(path: impl Into<FilePath>) -> DirWalker {
    DirWalker::new(path)
}

#[cfg(feature = "fs_live")]
pub use live::LiveDirWalker;

#[cfg(feature = "fs_live")]
mod live {
    use super::{DirWalker, FileEntry, walk_files};
    use crate::error::{Error, Result};
    use crate::live_component::{LiveMapFeed, LiveMapSubscriber, LiveMapView, SingleWatcherGuard};
    use async_trait::async_trait;
    use notify::{Config, EventKind, RecursiveMode, Watcher};
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::sync::mpsc;

    /// How long after the first file event the feed keeps collecting further
    /// events before it applies them, so that a burst (an editor's temp file
    /// and rename, a `cp -r`) is applied once per path.
    const EVENT_DEBOUNCE: Duration = Duration::from_millis(100);

    /// The default [`LiveDirWalker::rescan_interval`], as in the Python SDK.
    const DEFAULT_RESCAN_INTERVAL: Duration = Duration::from_secs(60 * 60);

    /// A live directory feed: a [`LiveMapView`] over a directory that follows
    /// filesystem changes. Build it with [`DirWalker::live`] and feed it to
    /// [`Ctx::mount_each_live`](crate::Ctx::mount_each_live) — the catch-up scan
    /// walks the directory once, then each file event updates or deletes that
    /// file's component alone (the Rust analogue of Python's
    /// `walk_dir(..., live=True)`): a created or modified file is mounted or
    /// re-processed, a deleted or moved-away file (or directory) has its
    /// components removed, and a directory that appears is walked and its files
    /// mounted. Only an event queue overflow and the periodic
    /// [`rescan_interval`](LiveDirWalker::rescan_interval) walk the whole tree.
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
        poll_interval: Duration,
        rescan_interval: Option<Duration>,
        watch_guard: SingleWatcherGuard,
        /// The keys currently fed to the subscriber: what the last scan found,
        /// kept up to date by every update and delete since. A path that
        /// disappears is resolved against it (a moved-away directory removes
        /// every key under it) without walking the tree.
        known_keys: Mutex<BTreeSet<String>>,
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
                poll_interval: Duration::from_secs(1),
                rescan_interval: Some(DEFAULT_RESCAN_INTERVAL),
                watch_guard: SingleWatcherGuard::new("LiveDirWalker"),
                known_keys: Mutex::new(BTreeSet::new()),
            }
        }
    }

    impl LiveDirWalker {
        /// Set how often the directory is polled for changes when the native
        /// watcher is unavailable (default 1s).
        pub fn poll_interval(mut self, interval: Duration) -> Self {
            self.poll_interval = interval;
            self
        }

        /// Set the interval between periodic full rescans, which recreate the
        /// OS-level watcher and walk the whole directory, as a defence against
        /// events the platform dropped (e.g. macOS FSEvents silently stopping).
        /// Defaults to one hour; `None` disables the rescan.
        pub fn rescan_interval(mut self, interval: Option<Duration>) -> Self {
            self.rescan_interval = interval;
            self
        }

        fn known_keys(&self) -> std::sync::MutexGuard<'_, BTreeSet<String>> {
            self.known_keys.lock().unwrap_or_else(|e| e.into_inner())
        }

        /// The subscriber's key for an event path, or `None` when the path is
        /// not a file this walker can yield: outside the root, the root itself,
        /// below the top level of a non-recursive walk, or under a directory
        /// the matcher prunes.
        fn relative_key(&self, path: &Path) -> Option<(PathBuf, String)> {
            let relative = path
                .strip_prefix(&self.watch_root)
                .or_else(|_| path.strip_prefix(self.walker.root.resolve()))
                .ok()?;
            if relative.as_os_str().is_empty() {
                return None;
            }
            let mut ancestors = relative
                .ancestors()
                .skip(1)
                .filter(|p| !p.as_os_str().is_empty());
            if !self.recursive && ancestors.next().is_some() {
                return None;
            }
            if !ancestors.all(|dir| self.walker.matcher.is_dir_included(dir)) {
                return None;
            }
            Some((
                relative.to_path_buf(),
                crate::resources::file::path_key(relative),
            ))
        }

        /// Walk the files under `relative` (a directory) on a blocking thread.
        async fn walk_subtree(&self, relative: PathBuf) -> Result<Vec<FileEntry>> {
            let root = self.walker.root.clone();
            let recursive = self.recursive;
            let matcher = self.walker.matcher.clone();
            tokio::task::spawn_blocking(move || walk_files(&root, &relative, recursive, &*matcher))
                .await
                .map_err(|e| Error::engine(format!("directory walk task panicked: {e}")))?
        }

        /// Resolve a batch of changed paths into per-key updates and deletes
        /// and apply them to the subscriber.
        async fn apply_changes(
            &self,
            subscriber: &LiveMapSubscriber<String, FileEntry>,
            paths: BTreeSet<PathBuf>,
        ) -> Result<()> {
            let mut updates: BTreeMap<String, FileEntry> = BTreeMap::new();
            let mut deletes: BTreeSet<String> = BTreeSet::new();
            for path in paths {
                let Some((relative, key)) = self.relative_key(&path) else {
                    continue;
                };
                match tokio::fs::metadata(&path).await {
                    Ok(meta) if meta.is_file() => {
                        if self.walker.matcher.is_file_included(&relative) {
                            let entry =
                                FileEntry::from_metadata(self.walker.root.clone(), relative, &meta);
                            updates.insert(key, entry);
                        }
                    }
                    Ok(meta) if meta.is_dir() => {
                        // A directory that appeared (created, or moved in): its
                        // files produced no events of their own, so walk it.
                        if self.recursive && self.walker.matcher.is_dir_included(&relative) {
                            for entry in self.walk_subtree(relative).await? {
                                updates.insert(entry.key(), entry);
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        // Gone: the file under that key, or every file under
                        // that directory.
                        let known = self.known_keys();
                        if known.contains(&key) {
                            deletes.insert(key);
                        } else {
                            let prefix = format!("{key}/");
                            deletes.extend(
                                known
                                    .range(prefix.clone()..)
                                    .take_while(|k| k.starts_with(&prefix))
                                    .cloned(),
                            );
                        }
                    }
                    Err(e) => {
                        tracing::debug!("fs watch: cannot stat {}: {e}", path.display());
                    }
                }
            }
            for key in deletes {
                if updates.contains_key(&key) {
                    continue;
                }
                subscriber.delete(key.clone()).await?;
                self.known_keys().remove(&key);
            }
            // Every event re-runs its file's component, as in the Python SDK:
            // an equal size and mtime is not proof of equal content (a rewrite
            // in the mtime's own tick keeps it), so whether the work can be
            // skipped is the file memo state's decision, not the feed's.
            for (key, entry) in updates {
                subscriber.update(key.clone(), entry).await?;
                self.known_keys().insert(key);
            }
            Ok(())
        }
    }

    /// What the watcher reports to the feed's loop.
    enum FsChange {
        /// The platform dropped events (an inotify queue overflow): walk the
        /// whole tree.
        Rescan,
        /// Something changed at this path; what it is now is decided by a stat
        /// after the debounce window.
        Path(PathBuf),
    }

    /// Map a platform event to the changes the feed acts on. Access events
    /// (an open or a close for reading) are dropped: on Linux the inotify
    /// watch includes `IN_OPEN`, so the feed's own reads of the files it
    /// processes would otherwise feed back as changes.
    fn changes_of(event: notify::Event) -> Vec<FsChange> {
        if event.need_rescan() {
            return vec![FsChange::Rescan];
        }
        match event.kind {
            EventKind::Access(_) => Vec::new(),
            EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_) => {
                event.paths.into_iter().map(FsChange::Path).collect()
            }
            EventKind::Any | EventKind::Other => {
                event.paths.into_iter().map(FsChange::Path).collect()
            }
        }
    }

    /// Watch `root`, sending each change to `tx`, with the platform's native
    /// backend; when creating or arming it fails, the poller takes over and
    /// the reason is logged. The returned watcher stops when dropped.
    fn start_watcher(
        root: &Path,
        mode: RecursiveMode,
        poll_interval: Duration,
        tx: mpsc::UnboundedSender<FsChange>,
    ) -> Result<Box<dyn Watcher + Send>> {
        let handler = move |res: notify::Result<notify::Event>| {
            if let Ok(event) = res {
                for change in changes_of(event) {
                    let _ = tx.send(change);
                }
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
            let walker = self.walker.clone();
            let items = tokio::task::spawn_blocking(move || walker.items())
                .await
                .map_err(|e| Error::engine(format!("directory walk task panicked: {e}")))??;
            *self.known_keys() = items.iter().map(|(key, _)| key.clone()).collect();
            Ok(items)
        }
    }

    #[async_trait]
    impl LiveMapFeed<String, FileEntry> for LiveDirWalker {
        async fn watch(&self, subscriber: LiveMapSubscriber<String, FileEntry>) -> Result<()> {
            let _watch_token = self.watch_guard.enter()?;
            let (tx, mut rx) = mpsc::unbounded_channel::<FsChange>();
            let mode = if self.recursive {
                RecursiveMode::Recursive
            } else {
                RecursiveMode::NonRecursive
            };
            let mut watcher =
                start_watcher(&self.watch_root, mode, self.poll_interval, tx.clone())?;
            // The watcher reports only changes after the `watch` call just
            // above, while the framework's catch-up scan ran before this
            // method was called, so a file created or deleted between the two
            // would never produce an event: rescan once now that the watch is
            // armed, as the Python localfs source does.
            subscriber.update_all().await?;
            let mut next_rescan = self
                .rescan_interval
                .map(|interval| tokio::time::Instant::now() + interval);
            // The watcher is kept alive for the duration of this loop; dropping
            // the future stops watching.
            loop {
                let first = match next_rescan {
                    Some(deadline) => match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(change) => change,
                        Err(_elapsed) => {
                            // Periodic rescan: a fresh OS-level watcher (the
                            // old one may have silently stopped), stale events
                            // dropped, then a full walk to catch whatever the
                            // old watcher missed.
                            drop(watcher);
                            while rx.try_recv().is_ok() {}
                            watcher = start_watcher(
                                &self.watch_root,
                                mode,
                                self.poll_interval,
                                tx.clone(),
                            )?;
                            subscriber.update_all().await?;
                            next_rescan = self
                                .rescan_interval
                                .map(|interval| tokio::time::Instant::now() + interval);
                            continue;
                        }
                    },
                    None => rx.recv().await,
                };
                let Some(first) = first else { break };
                // Collect the burst the first event belongs to.
                let mut rescan = false;
                let mut paths = BTreeSet::new();
                let mut note = |change: FsChange| match change {
                    FsChange::Rescan => rescan = true,
                    FsChange::Path(path) => {
                        paths.insert(path);
                    }
                };
                note(first);
                let window = tokio::time::sleep(EVENT_DEBOUNCE);
                tokio::pin!(window);
                loop {
                    tokio::select! {
                        () = &mut window => break,
                        change = rx.recv() => match change {
                            Some(change) => note(change),
                            None => break,
                        },
                    }
                }
                if rescan {
                    subscriber.update_all().await?;
                } else {
                    self.apply_changes(&subscriber, paths).await?;
                }
            }
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn event(kind: EventKind, path: &str) -> notify::Event {
            notify::Event::new(kind).add_path(PathBuf::from(path))
        }

        #[test]
        fn access_events_are_dropped_and_changes_keep_their_path() {
            use notify::event::{AccessKind, AccessMode, CreateKind, ModifyKind, RemoveKind};
            assert!(
                changes_of(event(
                    EventKind::Access(AccessKind::Open(AccessMode::Any)),
                    "/r/a"
                ))
                .is_empty()
            );
            assert!(
                changes_of(event(
                    EventKind::Access(AccessKind::Close(AccessMode::Read)),
                    "/r/a"
                ))
                .is_empty()
            );
            for kind in [
                EventKind::Create(CreateKind::File),
                EventKind::Modify(ModifyKind::Any),
                EventKind::Remove(RemoveKind::Folder),
            ] {
                let changes = changes_of(event(kind, "/r/a"));
                assert!(
                    matches!(changes.as_slice(), [FsChange::Path(p)] if p == Path::new("/r/a"))
                );
            }
            let overflow = event(EventKind::Other, "/r").set_flag(notify::event::Flag::Rescan);
            assert!(matches!(
                changes_of(overflow).as_slice(),
                [FsChange::Rescan]
            ));
        }

        #[test]
        fn relative_key_prunes_what_the_walk_would_not_yield() {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().canonicalize().unwrap();
            let matcher =
                super::super::PatternFilePathMatcher::new(std::iter::empty::<&str>(), ["**/.*"])
                    .unwrap();
            let live = super::super::walk_dir(root.clone())
                .recursive(true)
                .path_matcher(matcher.clone())
                .live();
            assert_eq!(
                live.relative_key(&root.join("a/b.txt")).unwrap().1,
                "a/b.txt"
            );
            assert!(live.relative_key(&root).is_none(), "the root itself");
            assert!(
                live.relative_key(&root.join(".git/objects/ab")).is_none(),
                "a pruned dir"
            );
            assert!(
                live.relative_key(Path::new("/elsewhere/x")).is_none(),
                "outside the root"
            );

            let flat = super::super::walk_dir(root.clone()).live();
            assert_eq!(flat.relative_key(&root.join("b.txt")).unwrap().1, "b.txt");
            assert!(
                flat.relative_key(&root.join("a/b.txt")).is_none(),
                "below a flat walk"
            );
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
    fn from_metadata(root: FilePath, relative: PathBuf, metadata: &std::fs::Metadata) -> Self {
        let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        Self {
            root,
            relative,
            size: metadata.len(),
            modified,
            cache: Arc::new(FileContentCache::with_metadata(FileMetadata {
                size: metadata.len(),
                modified,
                content_fingerprint: None,
            })),
        }
    }

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

    /// Read file contents as bytes, blocking the calling thread. In an async
    /// function prefer [`FileLike::read`], which reads on the runtime's
    /// blocking pool and caches the content.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the file cannot be opened or read.
    pub fn content(&self) -> Result<Vec<u8>> {
        std::fs::read(self.path()).map_err(Error::Io)
    }

    /// Read file contents as UTF-8 string, blocking the calling thread (see
    /// [`content`](Self::content)); [`FileLike::read_text`] is the async form.
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
/// sorted by relative path. Blocks on the walk, like [`DirWalker::walk`].
///
/// # Examples
/// ```ignore
/// let files = cocoindex::resources::fs::walk("./src", &["**/*.rs", "**/*.toml"])?;
/// ```
pub fn walk(dir: impl AsRef<Path>, patterns: &[&str]) -> Result<Vec<FileEntry>> {
    let matcher = PatternFilePathMatcher::include(patterns.iter().copied())?;
    DirWalker::new(dir.as_ref())
        .recursive(true)
        .path_matcher(matcher)
        .walk()
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

/// The identity of a directory, for not entering a symlink cycle twice.
#[cfg(unix)]
#[derive(Clone, Copy, PartialEq, Eq)]
struct DirId {
    dev: u64,
    ino: u64,
}

#[cfg(unix)]
impl DirId {
    fn of(metadata: &std::fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;
        Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
        }
    }
}

/// Walk the files under `start` (a directory relative to `root`; empty for
/// the root itself), in directory order, with the matcher applied to paths
/// relative to `root`. Mirrors the Python SDK's walk:
/// * a symlink to a directory is followed, except into a directory that is
///   already an ancestor (a cycle); a symlink to a file is a file,
/// * a directory the process cannot read, and an entry that vanishes or
///   cannot be stat-ed, are skipped,
/// * the matcher decides on an entry's path before it is stat-ed, so an
///   excluded file costs no system call beyond the directory listing.
///
/// A missing `start` yields no files; `start` not being a directory is an
/// error.
fn walk_files(
    root: &FilePath,
    start: &Path,
    recursive: bool,
    matcher: &dyn FilePathMatcher,
) -> Result<Vec<FileEntry>> {
    let root_fs = root.resolve();
    let start_fs = root_fs.join(start);
    let start_metadata = match std::fs::metadata(&start_fs) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(Error::Io(err)),
    };
    if !start_metadata.is_dir() {
        return Err(Error::engine(format!(
            "path is not a directory: {}",
            start_fs.display()
        )));
    }

    struct PendingDir {
        path: PathBuf,
        relative: PathBuf,
        #[cfg(unix)]
        ancestors: Vec<DirId>,
    }

    let mut files = Vec::new();
    let mut pending = vec![PendingDir {
        path: start_fs,
        relative: start.to_path_buf(),
        #[cfg(unix)]
        ancestors: vec![DirId::of(&start_metadata)],
    }];
    while let Some(dir) = pending.pop() {
        let entries = match std::fs::read_dir(&dir.path) {
            Ok(entries) => entries,
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::NotFound
                ) =>
            {
                continue;
            }
            Err(err) => return Err(Error::Io(err)),
        };
        let mut subdirs = Vec::new();
        for entry in entries {
            let Ok(entry) = entry else { continue };
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            let path = entry.path();
            let relative = dir.relative.join(entry.file_name());
            // The listing says what a non-symlink is; a symlink is what it
            // points to (dangling: skipped), as Python's `is_dir`/`is_file`.
            let (is_dir, is_file, metadata) = if file_type.is_symlink() {
                match std::fs::metadata(&path) {
                    Ok(metadata) => (metadata.is_dir(), metadata.is_file(), Some(metadata)),
                    Err(_) => continue,
                }
            } else {
                (file_type.is_dir(), file_type.is_file(), None)
            };
            if is_dir {
                if !recursive || !matcher.is_dir_included(&relative) {
                    continue;
                }
                #[cfg(unix)]
                let ancestors = {
                    let metadata = match metadata {
                        Some(metadata) => metadata,
                        None => match std::fs::metadata(&path) {
                            Ok(metadata) => metadata,
                            Err(_) => continue,
                        },
                    };
                    let id = DirId::of(&metadata);
                    if dir.ancestors.contains(&id) {
                        continue;
                    }
                    let mut ancestors = dir.ancestors.clone();
                    ancestors.push(id);
                    ancestors
                };
                #[cfg(not(unix))]
                if file_type.is_symlink() {
                    // Without a stable directory identity a symlink cycle
                    // cannot be detected: do not follow symlinked directories.
                    continue;
                }
                subdirs.push(PendingDir {
                    path,
                    relative,
                    #[cfg(unix)]
                    ancestors,
                });
            } else if is_file {
                if !matcher.is_file_included(&relative) {
                    continue;
                }
                let metadata = match metadata {
                    Some(metadata) => metadata,
                    None => match std::fs::metadata(&path) {
                        Ok(metadata) => metadata,
                        Err(_) => continue,
                    },
                };
                files.push(FileEntry::from_metadata(root.clone(), relative, &metadata));
            }
        }
        // Push in reverse so the stack visits subdirectories in listing order.
        pending.extend(subdirs.into_iter().rev());
    }
    Ok(files)
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
            self.provider
                .target_state(self.entry_key(&path)?, FsEntry::Dir { create_parent_dirs }),
        )?;
        Ok(DirTarget {
            provider: self.provider.clone(),
            root: self.root.clone(),
            sub: self.sub.join(path),
        })
    }

    /// The target-state key of the entry `name` under this directory:
    /// `[root, path]` (or `[base_key, root, path]` under a context directory), from which the handler rebuilds the path.
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

/// `[root, path]` for a root that is a plain path, `[base_key, root, path]`
/// for one under a context directory: the base directory's context-key name,
/// the root as written, and the entry's path relative to the root (empty for
/// the root itself). A plain root keeps the two-element key so states stored
/// before context roots existed stay matched.
fn entry_key(root: &FilePath, relative: &Path) -> Result<StableKey> {
    let to_str = |path: &Path| -> Result<Arc<str>> {
        path.to_str().map(Arc::from).ok_or_else(|| {
            Error::engine(format!(
                "directory target path is not valid UTF-8: {}",
                path.display()
            ))
        })
    };
    let root_and_path = [
        StableKey::Str(to_str(root.path())?),
        StableKey::Str(to_str(relative)?),
    ];
    Ok(StableKey::Array(match root.base_key() {
        Some(base_key) => Arc::from(
            std::iter::once(StableKey::Str(Arc::from(base_key)))
                .chain(root_and_path)
                .collect::<Vec<_>>(),
        ),
        None => Arc::from(root_and_path),
    }))
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
/// `[root, path]` or `[base_key, root, path]`, so one handler serves every directory target.
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
            // One hasher update over the content, as the Python target's
            // `fingerprint_bytes`; `Fingerprint::from` would serialize the
            // bytes one by one.
            Some(FsEntry::File { content, .. }) => Some(Fingerprint::from_bytes(content)),
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

/// The base key and path a `[root, path]` or `[base_key, root, path]`
/// target-state key names.
fn entry_path(key: &StableKey) -> Result<(Option<String>, PathBuf)> {
    if let StableKey::Array(parts) = key
        && let (base_key, [StableKey::Str(root), StableKey::Str(relative)]) = match &parts[..] {
            [StableKey::Str(base_key), rest @ ..] if parts.len() == 3 => {
                (Some(base_key.to_string()), rest)
            }
            rest => (None, rest),
        }
    {
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
        let fp = Fingerprint::from_bytes;

        let unchanged = handler
            .reconcile(key("a.txt"), Some(file(b"A")), vec![fp(b"A")], false)
            .unwrap();
        assert!(unchanged.is_none(), "an unchanged file is not rewritten");

        let maybe_missing = handler
            .reconcile(key("a.txt"), Some(file(b"A")), vec![fp(b"A")], true)
            .unwrap();
        assert!(
            maybe_missing.is_some(),
            "a possibly-missing file is rewritten"
        );

        let ambiguous = handler
            .reconcile(
                key("a.txt"),
                Some(file(b"A")),
                vec![fp(b"A"), fp(b"B")],
                false,
            )
            .unwrap();
        assert!(
            ambiguous.is_some(),
            "an ambiguous previous state is rewritten"
        );

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
            .reconcile(
                key("a.txt"),
                None,
                vec![Fingerprint::from_bytes(b"A")],
                false,
            )
            .unwrap()
            .unwrap();
        let TargetAction::Update(action) = out.action else {
            panic!("expected an update action");
        };
        assert!(matches!(action.op, FsOp::RemoveFile));

        let known_absent = handler
            .reconcile(key("a.txt"), None, vec![], false)
            .unwrap();
        assert!(known_absent.is_none());
    }

    fn keys(files: &[FileEntry]) -> Vec<String> {
        files.iter().map(FileEntry::key).collect()
    }

    #[test]
    fn walk_follows_symlinked_dirs_once_and_skips_unreadable_ones() {
        let tmp = tempfile::tempdir().unwrap();
        let root = &tmp.path().join("root");
        std::fs::create_dir_all(root.join("a/b")).unwrap();
        std::fs::write(root.join("a/b/deep.txt"), b"deep").unwrap();
        std::fs::write(root.join("top.txt"), b"top").unwrap();
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("linked.txt"), b"linked").unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::{PermissionsExt, symlink};
            // A symlink to a directory elsewhere is followed; one back to an
            // ancestor is a cycle and is not entered; a symlink to a file is a
            // file; a dangling one is skipped.
            symlink(&outside, root.join("a/link")).unwrap();
            symlink(root, root.join("a/b/loop")).unwrap();
            symlink(root.join("top.txt"), root.join("a/alias.txt")).unwrap();
            symlink(root.join("missing"), root.join("a/dangling")).unwrap();
            // A directory this process cannot list is skipped, not an error.
            std::fs::create_dir(root.join("locked")).unwrap();
            std::fs::write(root.join("locked/hidden.txt"), b"hidden").unwrap();
            std::fs::set_permissions(root.join("locked"), std::fs::Permissions::from_mode(0o000))
                .unwrap();

            let files = walk_dir(root).recursive(true).walk().unwrap();
            std::fs::set_permissions(root.join("locked"), std::fs::Permissions::from_mode(0o755))
                .unwrap();
            assert_eq!(
                keys(&files),
                [
                    "a/alias.txt",
                    "a/b/deep.txt",
                    "a/link/linked.txt",
                    "top.txt"
                ]
            );
        }

        // Non-recursive: the top level only.
        let flat = walk_dir(root).walk().unwrap();
        assert_eq!(keys(&flat), ["top.txt"]);

        // A matcher prunes directories before they are listed and files before
        // they are stat-ed.
        let matched = walk_dir(root)
            .recursive(true)
            .path_matcher(PatternFilePathMatcher::new(["**/*.txt"], ["a/b"]).unwrap())
            .walk()
            .unwrap();
        assert!(!keys(&matched).iter().any(|k| k.starts_with("a/b/")));
        assert!(keys(&matched).contains(&"top.txt".to_string()));

        // A missing root yields nothing; a file as root is an error.
        assert!(walk_dir(root.join("nope")).walk().unwrap().is_empty());
        assert!(walk_dir(root.join("top.txt")).walk().is_err());
    }

    #[test]
    fn entry_names_are_normalized_and_contained() {
        assert_eq!(
            normalize_entry_name("./a/./b.txt").unwrap(),
            Path::new("a/b.txt")
        );
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
