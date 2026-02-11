//! ContentStore: checkpoint/rollback built on LMDB.
//! Forge's primary integration point.

use std::path::Path;

use cocoindex_utils::fingerprint::Fingerprinter;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

// LMDB constants
const MAX_DBS: u32 = 16;
const LMDB_MAP_SIZE: usize = 0x1000_0000; // 256 MiB — sufficient for content store

// ---------------------------------------------------------------------------
// Report types
// ---------------------------------------------------------------------------

pub struct SyncReport {
    pub added: usize,
    pub modified: usize,
    pub removed: usize,
    pub unchanged: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointInfo {
    pub name: String,
    pub description: String,
    pub entry_count: usize,
    pub created_at: String,
}

pub struct RestoreReport {
    pub written: usize,
    pub removed: usize,
}

pub struct ImportReport {
    pub imported: usize,
    pub skipped: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub path: String,
    pub content: Vec<u8>,
    pub fingerprint: [u8; 16],
}

pub enum DiffEntry {
    Added(String),
    Removed(String),
    Modified(String),
}

impl std::fmt::Display for DiffEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiffEntry::Added(p) => write!(f, "+ {p}"),
            DiffEntry::Removed(p) => write!(f, "- {p}"),
            DiffEntry::Modified(p) => write!(f, "~ {p}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Internal types stored in LMDB
// ---------------------------------------------------------------------------

/// Metadata for a stored file entry.
#[derive(Serialize, Deserialize)]
struct StoredEntry {
    fingerprint: [u8; 16],
    content: Vec<u8>,
}

// ---------------------------------------------------------------------------
// ContentStore
// ---------------------------------------------------------------------------

pub struct ContentStore {
    env: heed::Env,
    /// "entries" db: path -> StoredEntry
    entries_db: heed::Database<heed::types::Str, heed::types::Bytes>,
    /// "checkpoints" db: name -> serialized checkpoint data
    checkpoints_db: heed::Database<heed::types::Str, heed::types::Bytes>,
    /// "checkpoint_entries" db: "checkpoint_name\0path" -> StoredEntry
    checkpoint_entries_db: heed::Database<heed::types::Str, heed::types::Bytes>,
}

impl ContentStore {
    /// Open or create a content store at the given path.
    pub fn open(db_path: impl AsRef<Path>) -> Result<Self> {
        let db_path = db_path.as_ref();
        std::fs::create_dir_all(db_path).map_err(Error::Io)?;

        let env = unsafe {
            heed::EnvOpenOptions::new()
                .max_dbs(MAX_DBS)
                .map_size(LMDB_MAP_SIZE)
                .open(db_path)
        }
        .map_err(Error::Db)?;

        let mut wtxn = env.write_txn().map_err(Error::Db)?;
        let entries_db = env
            .create_database(&mut wtxn, Some("entries"))
            .map_err(Error::Db)?;
        let checkpoints_db = env
            .create_database(&mut wtxn, Some("checkpoints"))
            .map_err(Error::Db)?;
        let checkpoint_entries_db = env
            .create_database(&mut wtxn, Some("checkpoint_entries"))
            .map_err(Error::Db)?;
        wtxn.commit().map_err(Error::Db)?;

        Ok(Self {
            env,
            entries_db,
            checkpoints_db,
            checkpoint_entries_db,
        })
    }

    /// Sync a directory to LMDB. Fingerprint-based change detection.
    pub fn sync(&self, dir: impl AsRef<Path>) -> Result<SyncReport> {
        let dir = dir.as_ref();
        let mut added = 0;
        let mut modified = 0;
        let mut unchanged = 0;

        // Collect all files from the directory
        let mut current_files = std::collections::HashMap::new();
        walk_files(dir, dir, &mut current_files)?;

        let mut wtxn = self.env.write_txn().map_err(Error::Db)?;

        // Track existing paths for removal detection
        let mut existing_paths = std::collections::HashSet::new();
        {
            let iter = self.entries_db.iter(&wtxn).map_err(Error::Db)?;
            for result in iter {
                let (key, _) = result.map_err(Error::Db)?;
                existing_paths.insert(key.to_string());
            }
        }

        // Update/add entries
        for (rel_path, content) in &current_files {
            let fp = fingerprint_bytes(content);

            // Check if entry exists and is unchanged
            if let Some(existing_data) = self.entries_db.get(&wtxn, rel_path).map_err(Error::Db)? {
                let existing: StoredEntry = rmp_serde::from_slice(existing_data)?;
                if existing.fingerprint == fp {
                    unchanged += 1;
                    existing_paths.remove(rel_path.as_str());
                    continue;
                }
                modified += 1;
            } else {
                added += 1;
            }

            let entry = StoredEntry {
                fingerprint: fp,
                content: content.clone(),
            };
            let encoded = rmp_serde::to_vec_named(&entry)?;
            self.entries_db
                .put(&mut wtxn, rel_path, &encoded)
                .map_err(Error::Db)?;
            existing_paths.remove(rel_path.as_str());
        }

        // Remove entries no longer on disk
        let removed = existing_paths.len();
        for path in &existing_paths {
            self.entries_db
                .delete(&mut wtxn, path)
                .map_err(Error::Db)?;
        }

        wtxn.commit().map_err(Error::Db)?;

        Ok(SyncReport {
            added,
            modified,
            removed,
            unchanged,
        })
    }

    /// Save current state as a named checkpoint.
    pub fn checkpoint(&self, name: &str, description: &str) -> Result<CheckpointInfo> {
        let rtxn = self.env.read_txn().map_err(Error::Db)?;
        let mut wtxn = self.env.write_txn().map_err(Error::Db)?;

        // Copy all entries to checkpoint
        let mut entry_count = 0;
        let iter = self.entries_db.iter(&rtxn).map_err(Error::Db)?;
        for result in iter {
            let (path, data) = result.map_err(Error::Db)?;
            let cp_key = format!("{name}\0{path}");
            self.checkpoint_entries_db
                .put(&mut wtxn, &cp_key, data)
                .map_err(Error::Db)?;
            entry_count += 1;
        }
        drop(rtxn);

        let info = CheckpointInfo {
            name: name.to_string(),
            description: description.to_string(),
            entry_count,
            created_at: chrono::Utc::now().to_rfc3339(),
        };
        let info_bytes = rmp_serde::to_vec_named(&info)?;
        self.checkpoints_db
            .put(&mut wtxn, name, &info_bytes)
            .map_err(Error::Db)?;

        wtxn.commit().map_err(Error::Db)?;
        Ok(info)
    }

    /// Restore files from a checkpoint.
    pub fn restore(&self, name: &str, dir: impl AsRef<Path>) -> Result<RestoreReport> {
        let dir = dir.as_ref();
        let rtxn = self.env.read_txn().map_err(Error::Db)?;

        // Verify checkpoint exists
        if self
            .checkpoints_db
            .get(&rtxn, name)
            .map_err(Error::Db)?
            .is_none()
        {
            return Err(Error::engine(format!("checkpoint not found: {name}")));
        }

        // Collect current files for removal
        let mut current_files = std::collections::HashSet::new();
        if dir.exists() {
            let mut files = std::collections::HashMap::new();
            walk_files(dir, dir, &mut files)?;
            current_files = files.keys().cloned().collect();
        }

        // Restore from checkpoint
        let prefix = format!("{name}\0");
        let mut written = 0;
        let mut restored_paths = std::collections::HashSet::new();

        let iter = self
            .checkpoint_entries_db
            .prefix_iter(&rtxn, &prefix)
            .map_err(Error::Db)?;
        for result in iter {
            let (key, data) = result.map_err(Error::Db)?;
            let rel_path = &key[prefix.len()..];
            let entry: StoredEntry = rmp_serde::from_slice(data)?;

            let full_path = dir.join(rel_path);
            if let Some(parent) = full_path.parent() {
                std::fs::create_dir_all(parent).map_err(Error::Io)?;
            }
            std::fs::write(&full_path, &entry.content).map_err(Error::Io)?;
            restored_paths.insert(rel_path.to_string());
            written += 1;
        }

        // Remove files that existed but are not in the checkpoint
        let mut removed = 0;
        for path in &current_files {
            if !restored_paths.contains(path) {
                let full_path = dir.join(path);
                if full_path.exists() {
                    std::fs::remove_file(&full_path).map_err(Error::Io)?;
                    removed += 1;
                }
            }
        }

        Ok(RestoreReport { written, removed })
    }

    /// List all checkpoints.
    pub fn list(&self) -> Result<Vec<CheckpointInfo>> {
        let rtxn = self.env.read_txn().map_err(Error::Db)?;
        let mut result = Vec::new();
        let iter = self.checkpoints_db.iter(&rtxn).map_err(Error::Db)?;
        for entry in iter {
            let (_, data) = entry.map_err(Error::Db)?;
            let info: CheckpointInfo = rmp_serde::from_slice(data)?;
            result.push(info);
        }
        Ok(result)
    }

    /// Diff two checkpoints (or "current" for the current state).
    pub fn diff(&self, from: &str, to: &str) -> Result<Vec<DiffEntry>> {
        let rtxn = self.env.read_txn().map_err(Error::Db)?;
        let from_entries = self.collect_entries(&rtxn, from)?;
        let to_entries = self.collect_entries(&rtxn, to)?;

        let mut result = Vec::new();

        // Find added/modified
        for (path, to_fp) in &to_entries {
            match from_entries.get(path) {
                Some(from_fp) if from_fp == to_fp => {} // unchanged
                Some(_) => result.push(DiffEntry::Modified(path.clone())),
                None => result.push(DiffEntry::Added(path.clone())),
            }
        }

        // Find removed
        for path in from_entries.keys() {
            if !to_entries.contains_key(path) {
                result.push(DiffEntry::Removed(path.clone()));
            }
        }

        result.sort_by(|a, b| {
            let path_a = match a {
                DiffEntry::Added(p) | DiffEntry::Removed(p) | DiffEntry::Modified(p) => p,
            };
            let path_b = match b {
                DiffEntry::Added(p) | DiffEntry::Removed(p) | DiffEntry::Modified(p) => p,
            };
            path_a.cmp(path_b)
        });

        Ok(result)
    }

    /// Delete a checkpoint.
    pub fn delete_checkpoint(&self, name: &str) -> Result<()> {
        let mut wtxn = self.env.write_txn().map_err(Error::Db)?;

        // Delete checkpoint metadata
        self.checkpoints_db
            .delete(&mut wtxn, name)
            .map_err(Error::Db)?;

        // Delete all checkpoint entries
        let prefix = format!("{name}\0");
        let mut keys_to_delete = Vec::new();
        {
            let iter = self
                .checkpoint_entries_db
                .prefix_iter(&wtxn, &prefix)
                .map_err(Error::Db)?;
            for result in iter {
                let (key, _) = result.map_err(Error::Db)?;
                keys_to_delete.push(key.to_string());
            }
        }
        for key in &keys_to_delete {
            self.checkpoint_entries_db
                .delete(&mut wtxn, key)
                .map_err(Error::Db)?;
        }

        wtxn.commit().map_err(Error::Db)?;
        Ok(())
    }

    /// Export entries matching a glob pattern.
    pub fn export(&self, pattern: &str) -> Result<Vec<Entry>> {
        let rtxn = self.env.read_txn().map_err(Error::Db)?;
        let pat = glob::Pattern::new(pattern)
            .map_err(|e| Error::engine(format!("invalid glob: {e}")))?;

        let mut entries = Vec::new();
        let iter = self.entries_db.iter(&rtxn).map_err(Error::Db)?;
        for result in iter {
            let (path, data) = result.map_err(Error::Db)?;
            if pat.matches(path) {
                let stored: StoredEntry = rmp_serde::from_slice(data)?;
                entries.push(Entry {
                    path: path.to_string(),
                    content: stored.content,
                    fingerprint: stored.fingerprint,
                });
            }
        }
        Ok(entries)
    }

    /// Import entries from another store.
    pub fn import(&self, entries: &[Entry]) -> Result<ImportReport> {
        let mut wtxn = self.env.write_txn().map_err(Error::Db)?;
        let mut imported = 0;
        let mut skipped = 0;

        for entry in entries {
            // Check if already exists with same fingerprint
            if let Some(existing_data) = self
                .entries_db
                .get(&wtxn, &entry.path)
                .map_err(Error::Db)?
            {
                let existing: StoredEntry = rmp_serde::from_slice(existing_data)?;
                if existing.fingerprint == entry.fingerprint {
                    skipped += 1;
                    continue;
                }
            }

            let stored = StoredEntry {
                fingerprint: entry.fingerprint,
                content: entry.content.clone(),
            };
            let encoded = rmp_serde::to_vec_named(&stored)?;
            self.entries_db
                .put(&mut wtxn, &entry.path, &encoded)
                .map_err(Error::Db)?;
            imported += 1;
        }

        wtxn.commit().map_err(Error::Db)?;
        Ok(ImportReport { imported, skipped })
    }

    // --- Private helpers ---

    fn collect_entries(
        &self,
        rtxn: &heed::RoTxn,
        name: &str,
    ) -> Result<std::collections::HashMap<String, [u8; 16]>> {
        let mut entries = std::collections::HashMap::new();

        if name == "current" {
            let iter = self.entries_db.iter(rtxn).map_err(Error::Db)?;
            for result in iter {
                let (path, data) = result.map_err(Error::Db)?;
                let stored: StoredEntry = rmp_serde::from_slice(data)?;
                entries.insert(path.to_string(), stored.fingerprint);
            }
        } else {
            let prefix = format!("{name}\0");
            let iter = self
                .checkpoint_entries_db
                .prefix_iter(rtxn, &prefix)
                .map_err(Error::Db)?;
            for result in iter {
                let (key, data) = result.map_err(Error::Db)?;
                let rel_path = &key[prefix.len()..];
                let stored: StoredEntry = rmp_serde::from_slice(data)?;
                entries.insert(rel_path.to_string(), stored.fingerprint);
            }
        }

        Ok(entries)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn fingerprint_bytes(data: &[u8]) -> [u8; 16] {
    let mut fp = Fingerprinter::default();
    fp.write_raw_bytes(data);
    fp.into_fingerprint().0
}

fn walk_files(
    base: &Path,
    dir: &Path,
    out: &mut std::collections::HashMap<String, Vec<u8>>,
) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    let entries = std::fs::read_dir(dir).map_err(Error::Io)?;
    for entry in entries {
        let entry = entry.map_err(Error::Io)?;
        let path = entry.path();
        if path.is_dir() {
            let name = path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy();
            if !name.starts_with('.') {
                walk_files(base, &path, out)?;
            }
        } else if path.is_file() {
            let rel = path
                .strip_prefix(base)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            let content = std::fs::read(&path).map_err(Error::Io)?;
            out.insert(rel, content);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (tempfile::TempDir, tempfile::TempDir, ContentStore) {
        let db_dir = tempfile::tempdir().unwrap();
        let data_dir = tempfile::tempdir().unwrap();
        let store = ContentStore::open(db_dir.path()).unwrap();
        (db_dir, data_dir, store)
    }

    #[test]
    fn sync_roundtrip() {
        let (_db, data_dir, store) = setup();
        std::fs::write(data_dir.path().join("a.txt"), "hello").unwrap();
        std::fs::write(data_dir.path().join("b.txt"), "world").unwrap();

        let report = store.sync(data_dir.path()).unwrap();
        assert_eq!(report.added, 2);
        assert_eq!(report.modified, 0);
        assert_eq!(report.unchanged, 0);

        // Second sync — no changes
        let report = store.sync(data_dir.path()).unwrap();
        assert_eq!(report.added, 0);
        assert_eq!(report.modified, 0);
        assert_eq!(report.unchanged, 2);
    }

    #[test]
    fn sync_detect_modification() {
        let (_db, data_dir, store) = setup();
        std::fs::write(data_dir.path().join("a.txt"), "v1").unwrap();
        store.sync(data_dir.path()).unwrap();

        std::fs::write(data_dir.path().join("a.txt"), "v2").unwrap();
        let report = store.sync(data_dir.path()).unwrap();
        assert_eq!(report.modified, 1);
    }

    #[test]
    fn sync_detect_removal() {
        let (_db, data_dir, store) = setup();
        std::fs::write(data_dir.path().join("a.txt"), "hello").unwrap();
        std::fs::write(data_dir.path().join("b.txt"), "world").unwrap();
        store.sync(data_dir.path()).unwrap();

        std::fs::remove_file(data_dir.path().join("b.txt")).unwrap();
        let report = store.sync(data_dir.path()).unwrap();
        assert_eq!(report.removed, 1);
        assert_eq!(report.unchanged, 1);
    }

    #[test]
    fn checkpoint_and_restore() {
        let (_db, data_dir, store) = setup();
        std::fs::write(data_dir.path().join("a.txt"), "hello").unwrap();
        store.sync(data_dir.path()).unwrap();

        let cp = store.checkpoint("v1", "initial").unwrap();
        assert_eq!(cp.entry_count, 1);

        // Modify file
        std::fs::write(data_dir.path().join("a.txt"), "modified").unwrap();
        assert_eq!(
            std::fs::read_to_string(data_dir.path().join("a.txt")).unwrap(),
            "modified"
        );

        // Restore
        let report = store.restore("v1", data_dir.path()).unwrap();
        assert_eq!(report.written, 1);

        assert_eq!(
            std::fs::read_to_string(data_dir.path().join("a.txt")).unwrap(),
            "hello"
        );
    }

    #[test]
    fn diff_checkpoints() {
        let (_db, data_dir, store) = setup();

        std::fs::write(data_dir.path().join("a.txt"), "hello").unwrap();
        std::fs::write(data_dir.path().join("b.txt"), "world").unwrap();
        store.sync(data_dir.path()).unwrap();
        store.checkpoint("v1", "first").unwrap();

        std::fs::write(data_dir.path().join("a.txt"), "changed").unwrap();
        std::fs::remove_file(data_dir.path().join("b.txt")).unwrap();
        std::fs::write(data_dir.path().join("c.txt"), "new").unwrap();
        store.sync(data_dir.path()).unwrap();
        store.checkpoint("v2", "second").unwrap();

        let diff = store.diff("v1", "v2").unwrap();
        let added: Vec<_> = diff
            .iter()
            .filter(|d| matches!(d, DiffEntry::Added(_)))
            .collect();
        let removed: Vec<_> = diff
            .iter()
            .filter(|d| matches!(d, DiffEntry::Removed(_)))
            .collect();
        let modified: Vec<_> = diff
            .iter()
            .filter(|d| matches!(d, DiffEntry::Modified(_)))
            .collect();

        assert_eq!(added.len(), 1);
        assert_eq!(removed.len(), 1);
        assert_eq!(modified.len(), 1);
    }

    #[test]
    fn export_import() {
        let (db_dir1, data_dir, store1) = setup();
        std::fs::write(data_dir.path().join("a.txt"), "hello").unwrap();
        std::fs::write(data_dir.path().join("b.rs"), "fn main() {}").unwrap();
        store1.sync(data_dir.path()).unwrap();

        let entries = store1.export("*.txt").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "a.txt");

        // Import into a new store
        let db_dir2 = tempfile::tempdir().unwrap();
        let store2 = ContentStore::open(db_dir2.path()).unwrap();
        let report = store2.import(&entries).unwrap();
        assert_eq!(report.imported, 1);
        assert_eq!(report.skipped, 0);

        // Import again — should skip
        let report = store2.import(&entries).unwrap();
        assert_eq!(report.imported, 0);
        assert_eq!(report.skipped, 1);

        // Suppress unused variable warning
        drop(db_dir1);
    }

    #[test]
    fn list_checkpoints() {
        let (_db, data_dir, store) = setup();
        std::fs::write(data_dir.path().join("a.txt"), "hello").unwrap();
        store.sync(data_dir.path()).unwrap();

        store.checkpoint("v1", "first").unwrap();
        store.checkpoint("v2", "second").unwrap();

        let cps = store.list().unwrap();
        assert_eq!(cps.len(), 2);
    }

    #[test]
    fn delete_checkpoint() {
        let (_db, data_dir, store) = setup();
        std::fs::write(data_dir.path().join("a.txt"), "hello").unwrap();
        store.sync(data_dir.path()).unwrap();
        store.checkpoint("v1", "first").unwrap();

        store.delete_checkpoint("v1").unwrap();
        let cps = store.list().unwrap();
        assert_eq!(cps.len(), 0);
    }

    #[test]
    fn diff_with_current() {
        let (_db, data_dir, store) = setup();

        std::fs::write(data_dir.path().join("a.txt"), "hello").unwrap();
        store.sync(data_dir.path()).unwrap();
        store.checkpoint("v1", "first").unwrap();

        std::fs::write(data_dir.path().join("a.txt"), "changed").unwrap();
        store.sync(data_dir.path()).unwrap();

        let diff = store.diff("v1", "current").unwrap();
        assert_eq!(diff.len(), 1);
        assert!(matches!(&diff[0], DiffEntry::Modified(p) if p == "a.txt"));
    }
}
