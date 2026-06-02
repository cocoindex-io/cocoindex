//! Filesystem walking with fingerprinting.

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use cocoindex_core::engine::target_state::{TargetReconcileOutput, TargetStateProvider};
use cocoindex_core::state::stable_path::StableKey;
use cocoindex_utils::fingerprint::Fingerprint;
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::profile::{Action, BoxedHandler, BoxedSink, RustProfile, Value};

// ---------------------------------------------------------------------------
// FileEntry
// ---------------------------------------------------------------------------

/// A walked file with eager fingerprint and lazy content.
#[derive(Clone, Serialize)]
pub struct FileEntry {
    root: PathBuf,
    relative: PathBuf,
    size: u64,
    #[serde(with = "system_time_serde")]
    modified: SystemTime,
}

mod system_time_serde {
    use serde::{Serialize, Serializer};
    use std::time::SystemTime;

    pub fn serialize<S: Serializer>(time: &SystemTime, ser: S) -> Result<S::Ok, S::Error> {
        let duration = time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        (duration.as_secs(), duration.subsec_nanos()).serialize(ser)
    }
}

impl FileEntry {
    /// Full filesystem path.
    pub fn path(&self) -> PathBuf {
        self.root.join(&self.relative)
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

    /// Fingerprint for memoization keys (size + mtime).
    /// Returns a serializable value suitable as a cache key.
    pub fn fingerprint(&self) -> impl Serialize + '_ {
        (
            self.relative.to_string_lossy(),
            self.size,
            self.modified
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        )
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
    /// Returns [`Error::Io`] if the file cannot be read, or if it contains
    /// invalid UTF-8 data.
    pub fn content_str(&self) -> Result<String> {
        std::fs::read_to_string(self.path()).map_err(Error::Io)
    }

    /// Stable key for component paths (relative path, forward slashes).
    pub fn key(&self) -> String {
        self.relative.to_string_lossy().replace('\\', "/")
    }
}

/// Walk a directory matching multiple glob patterns. Returns all matching files
/// sorted by relative path.
///
/// # Examples
/// ```ignore
/// let files = cocoindex::fs::walk("./src", &["**/*.rs", "**/*.toml"])?;
/// ```
pub fn walk(dir: impl AsRef<Path>, patterns: &[&str]) -> Result<Vec<FileEntry>> {
    let dir = dir.as_ref();
    let canonical_dir = match std::fs::canonicalize(dir) {
        Ok(path) => path,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(Error::Io(err)),
    };
    let matcher = compile_globset(patterns)?;
    let mut files = Vec::new();

    for entry in WalkDir::new(dir) {
        let entry = entry.map_err(|err| {
            let message = err.to_string();
            match err.into_io_error() {
                Some(io_err) => Error::Io(io_err),
                None => Error::engine(message),
            }
        })?;
        let path = entry.path();
        let relative = relative_path(dir, &canonical_dir, path)?;
        let relative_key = relative.to_string_lossy().replace('\\', "/");
        if !matcher.is_match(&relative_key) {
            continue;
        }

        let metadata = std::fs::metadata(path).map_err(Error::Io)?;
        if !metadata.is_file() {
            continue;
        }

        files.push(FileEntry {
            root: dir.to_path_buf(),
            relative,
            size: metadata.len(),
            modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        });
    }

    files.sort_by(|a, b| a.relative.cmp(&b.relative));
    Ok(files)
}

fn compile_globset(patterns: &[&str]) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob = GlobBuilder::new(pattern)
            .literal_separator(true)
            .build()
            .map_err(|e| Error::engine(format!("invalid glob: {e}")))?;
        builder.add(glob);
    }

    builder
        .build()
        .map_err(|e| Error::engine(format!("invalid glob set: {e}")))
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

/// A declarative directory target — the Rust analogue of Python's `localfs`
/// directory target.
///
/// Files you [`declare_file`](DirTarget::declare_file) are reconciled against
/// the previous run via CocoIndex's target-state engine:
/// * new or changed files are written,
/// * unchanged files are skipped (no rewrite),
/// * files declared in a previous run but **not** this run (e.g. their source
///   was deleted) are removed from disk.
#[derive(Clone)]
pub struct DirTarget {
    provider: TargetStateProvider<RustProfile>,
    dir: PathBuf,
}

/// Mount a declarative directory target rooted at `dir`. Declared files are
/// synced to disk; orphaned files are deleted on later runs.
///
/// Must be called inside an `App::update()`/`App::run()` pipeline.
pub fn mount_dir_target(ctx: &Ctx, dir: impl Into<PathBuf>) -> Result<DirTarget> {
    let dir = dir.into();
    std::fs::create_dir_all(&dir).map_err(Error::Io)?;
    let provider = ctx.register_root_target_provider(
        format!("cocoindex/localfs/dir/{}", dir.to_string_lossy()),
        dir_handler(dir.clone()),
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
        let key = StableKey::Str(Arc::from(name));
        ctx.declare_target_state(
            self.provider.clone(),
            key,
            Value::from_serializable(&content.to_vec())?,
        )
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

fn dir_handler(dir: PathBuf) -> BoxedHandler {
    let sink = dir_sink(dir);
    BoxedHandler::new(move |key, desired, prev, prev_may_be_missing| {
        let StableKey::Str(name) = &key else {
            return Err(cocoindex_utils::error::Error::internal_msg(format!(
                "unexpected file target key: {key:?}"
            )));
        };
        let name = name.to_string();

        let desired_content: Option<Vec<u8>> = desired
            .map(Value::deserialize::<Vec<u8>>)
            .transpose()
            .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
        let desired_fp: Option<Fingerprint> = match &desired_content {
            Some(content) => Some(
                Fingerprint::from(content)
                    .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?,
            ),
            None => None,
        };

        // Skip when nothing changed.
        let prev_same = desired_fp.as_ref().is_some_and(|fp| {
            prev.iter()
                .filter_map(|v| v.deserialize::<Fingerprint>().ok())
                .any(|p| &p == fp)
        });
        if desired_content.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired_content.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }

        let tracking_record = match &desired_fp {
            Some(fp) => Some(
                Value::from_serializable(fp)
                    .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?,
            ),
            None => None,
        };
        let action = FileAction {
            name,
            content: desired_content,
        };
        Ok(Some(TargetReconcileOutput {
            action: Action::Update(
                Value::from_serializable(&action)
                    .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?,
            ),
            sink: sink.clone(),
            tracking_record,
            child_invalidation: None,
        }))
    })
}

fn dir_sink(dir: PathBuf) -> BoxedSink {
    BoxedSink::new(move |actions| {
        let dir = dir.clone();
        Box::pin(async move {
            let result = tokio::task::spawn_blocking(move || -> std::result::Result<(), String> {
                for action in actions {
                    let inner = match action {
                        Action::Create(v) | Action::Update(v) | Action::Delete(v) => v,
                    };
                    let file: FileAction = inner.deserialize().map_err(|e| e.to_string())?;
                    let path = dir.join(&file.name);
                    match file.content {
                        Some(content) => {
                            if let Some(parent) = path.parent() {
                                std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
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
                Ok(Ok(())) => Ok(None),
                Ok(Err(e)) => Err(cocoindex_utils::error::Error::internal_msg(e)),
                Err(e) => Err(cocoindex_utils::error::Error::internal_msg(format!(
                    "dir target sink task panicked: {e}"
                ))),
            }
        }) as Pin<Box<_>>
    })
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
