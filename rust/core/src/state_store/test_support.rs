//! Shared test-only helpers for constructing in-process stores.

use super::AppStore;
use tempfile::TempDir;

/// Open a fresh in-process LMDB environment and return an `AppStore`
/// backed by it. The caller must keep `TempDir` alive for the duration
/// of the test; dropping it removes the directory.
pub(crate) async fn make_test_store() -> (AppStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("mdb");
    std::fs::create_dir_all(&db_path).unwrap();
    let env = unsafe {
        heed::EnvOpenOptions::new()
            .read_txn_without_tls()
            .max_dbs(4)
            .map_size(1 << 22) // 4 MiB
            .open(&db_path)
    }
    .unwrap();
    let mut wtxn = env.write_txn().unwrap();
    let db = env.create_database(&mut wtxn, Some("test_app")).unwrap();
    wtxn.commit().unwrap();
    let storage = super::Storage::from_env(env.clone());
    (AppStore::new(db, env, storage), dir)
}
