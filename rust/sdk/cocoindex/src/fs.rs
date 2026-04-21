//! Filesystem walking with fingerprinting.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use serde::Serialize;
use walkdir::WalkDir;

use crate::error::{Error, Result};

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

/// Declares files that should exist in a directory.
/// CocoIndex auto-syncs: files not declared this run are deleted.
#[derive(Clone)]
pub struct DirTarget {
    dir: PathBuf,
}

impl DirTarget {
    /// Create a directory target.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the directory or its parents cannot be created.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(Error::Io)?;
        Ok(Self { dir })
    }

    /// Declare a file that should exist. Content is fingerprinted.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::fs::DirTarget;
    /// # fn main() -> cocoindex::error::Result<()> {
    /// let target = DirTarget::new("./output")?;
    /// target.declare_file("result.txt", b"final output")?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the parent directory cannot be created or if
    /// the file cannot be written to disk.
    pub fn declare_file(&self, name: &str, content: &[u8]) -> Result<()> {
        let path = Path::new(name);
        let has_parent_dir = path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir));
        #[cfg(windows)]
        let has_prefix = path
            .components()
            .any(|component| matches!(component, std::path::Component::Prefix(_)));
        #[cfg(not(windows))]
        let has_prefix = false;

        if path.has_root() || has_prefix || has_parent_dir {
            return Err(Error::engine(
                "declare_file path must be relative and cannot contain '..'",
            ));
        }

        let path = self.dir.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(Error::Io)?;
        }
        std::fs::write(&path, content).map_err(Error::Io)?;
        Ok(())
    }

    /// Get the target directory path.
    pub fn dir(&self) -> &Path {
        &self.dir
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
    fn dir_target_declare_file_path_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let target = DirTarget::new(dir.path()).unwrap();

        // Check relative traversal
        let err = target.declare_file("../escape.txt", b"bad").unwrap_err();
        assert!(
            err.to_string()
                .contains("must be relative and cannot contain '..'")
        );

        let err = target
            .declare_file("sub/../../escape.txt", b"bad")
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("must be relative and cannot contain '..'")
        );

        // Check rooted / absolute traversal
        let err = target.declare_file("/absolute/path", b"bad").unwrap_err();
        assert!(
            err.to_string()
                .contains("must be relative and cannot contain '..'")
        );

        #[cfg(windows)]
        {
            let err = target
                .declare_file(r"C:\absolute\path.txt", b"bad")
                .unwrap_err();
            assert!(
                err.to_string()
                    .contains("must be relative and cannot contain '..'")
            );

            let err = target
                .declare_file(r"C:drive-relative.txt", b"bad")
                .unwrap_err();
            assert!(
                err.to_string()
                    .contains("must be relative and cannot contain '..'")
            );
        }

        // Valid path works
        assert!(target.declare_file("sub/valid.txt", b"ok").is_ok());
    }
}
