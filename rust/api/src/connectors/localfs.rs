//! Filesystem connector: walk directories and declare file targets.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::Serialize;

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// WalkOpts
// ---------------------------------------------------------------------------

/// Options for walking a directory.
pub struct WalkOpts {
    pattern: String,
    excludes: Vec<String>,
}

impl WalkOpts {
    pub fn new(pattern: &str) -> Self {
        Self {
            pattern: pattern.to_owned(),
            excludes: Vec::new(),
        }
    }

    /// Exclude files matching this glob pattern.
    pub fn exclude(mut self, pattern: &str) -> Self {
        self.excludes.push(pattern.to_owned());
        self
    }
}

impl From<&str> for WalkOpts {
    fn from(pattern: &str) -> Self {
        WalkOpts::new(pattern)
    }
}

// ---------------------------------------------------------------------------
// FileRef
// ---------------------------------------------------------------------------

/// Reference to a walked file. Implements Serialize for memoization keys.
/// The serialized form includes path + size + mtime, so changes are detected.
#[derive(Clone, Serialize)]
pub struct FileRef {
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

impl FileRef {
    /// Read the file contents as UTF-8 text.
    pub fn read_text(&self) -> Result<String> {
        std::fs::read_to_string(self.path()).map_err(Error::Io)
    }

    /// Read the file contents as raw bytes.
    pub fn read_bytes(&self) -> Result<Vec<u8>> {
        std::fs::read(self.path()).map_err(Error::Io)
    }

    /// Stable key for component paths (relative path, forward slashes).
    pub fn key(&self) -> String {
        self.relative
            .to_string_lossy()
            .replace('\\', "/")
            .to_string()
    }

    /// Full filesystem path.
    pub fn path(&self) -> PathBuf {
        self.root.join(&self.relative)
    }

    /// Relative path from walk root.
    pub fn relative_path(&self) -> &Path {
        &self.relative
    }
}

// ---------------------------------------------------------------------------
// walk_dir
// ---------------------------------------------------------------------------

/// Walk a directory matching a glob pattern. Returns all matching files.
///
/// # Examples
/// ```ignore
/// let files = walk_dir("./docs", "**/*.md")?;
/// let files = walk_dir("./src", WalkOpts::new("**/*.rs").exclude("**/target/**"))?;
/// ```
pub fn walk_dir(dir: impl AsRef<Path>, opts: impl Into<WalkOpts>) -> Result<Vec<FileRef>> {
    let dir = dir.as_ref();
    let opts = opts.into();

    let pattern = format!("{}/{}", dir.display(), opts.pattern);
    let exclude_patterns: Vec<glob::Pattern> = opts
        .excludes
        .iter()
        .filter_map(|p| glob::Pattern::new(p).ok())
        .collect();

    let mut files = Vec::new();
    let entries = glob::glob(&pattern).map_err(|e| Error::engine(format!("invalid glob: {e}")))?;

    for entry in entries {
        let path = entry.map_err(|e| Error::Io(e.into_error()))?;
        if !path.is_file() {
            continue;
        }

        // Check excludes
        let relative = path
            .strip_prefix(dir)
            .unwrap_or(&path)
            .to_path_buf();
        let rel_str = relative.to_string_lossy();
        if exclude_patterns.iter().any(|p| p.matches(&rel_str)) {
            continue;
        }

        let metadata = std::fs::metadata(&path).map_err(Error::Io)?;
        files.push(FileRef {
            root: dir.to_path_buf(),
            relative,
            size: metadata.len(),
            modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        });
    }

    files.sort_by(|a, b| a.relative.cmp(&b.relative));
    Ok(files)
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
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(Error::Io)?;
        Ok(Self { dir })
    }

    /// Declare a file that should exist. Content is fingerprinted.
    pub fn declare_file(&self, name: &str, content: &[u8]) -> Result<()> {
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
    fn walk_dir_finds_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), "hello").unwrap();
        std::fs::write(dir.path().join("b.rs"), "fn main() {}").unwrap();
        std::fs::create_dir_all(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub/c.rs"), "mod sub;").unwrap();

        let files = walk_dir(dir.path(), "**/*.rs").unwrap();
        assert_eq!(files.len(), 2);
        let keys: Vec<String> = files.iter().map(|f| f.key()).collect();
        assert!(keys.contains(&"b.rs".to_string()));
        assert!(keys.contains(&"sub/c.rs".to_string()));
    }

    #[test]
    fn walk_dir_exclude() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.rs"), "").unwrap();
        std::fs::create_dir_all(dir.path().join("target")).unwrap();
        std::fs::write(dir.path().join("target/b.rs"), "").unwrap();

        let files = walk_dir(
            dir.path(),
            WalkOpts::new("**/*.rs").exclude("target/**"),
        )
        .unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].key(), "a.rs");
    }

    #[test]
    fn fileref_read_text() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("test.txt"), "hello world").unwrap();
        let files = walk_dir(dir.path(), "*.txt").unwrap();
        assert_eq!(files[0].read_text().unwrap(), "hello world");
    }

    #[test]
    fn dir_target_declare() {
        let dir = tempfile::tempdir().unwrap();
        let target = DirTarget::new(dir.path().join("out")).unwrap();
        target.declare_file("hello.txt", b"world").unwrap();
        let content = std::fs::read_to_string(dir.path().join("out/hello.txt")).unwrap();
        assert_eq!(content, "world");
    }
}
