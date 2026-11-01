//! Shared test-only helpers for constructing in-process stores and steering
//! their write batches.

use super::{AppStore, Storage};
use crate::prelude::*;
use tempfile::TempDir;
use tokio::sync::Notify;

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
    let storage = Storage::from_env(env);
    (AppStore::new(db, storage), dir)
}

/// A write batch held open by [`hold_write_batch`].
pub(crate) struct HeldWriteBatch {
    holder: tokio::task::JoinHandle<Result<()>>,
    release: Arc<Notify>,
}

impl HeldWriteBatch {
    /// Lets the held batch commit, and waits until it has.
    pub(crate) async fn release(self) {
        self.release.notify_one();
        self.holder.await.unwrap().unwrap();
    }
}

/// Holds a write batch of `storage` open until [`HeldWriteBatch::release`],
/// so the `run_txn` calls made meanwhile queue into the next batch, which
/// runs their bodies in call order. Polling such a call once queues it.
pub(crate) async fn hold_write_batch(storage: &Storage) -> HeldWriteBatch {
    let held = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let holder = tokio::spawn({
        let (storage, held, release) = (storage.clone(), held.clone(), release.clone());
        async move {
            storage
                .run_txn(move |_wtxn| {
                    let (held, release) = (held.clone(), release.clone());
                    Box::pin(async move {
                        held.notify_one();
                        release.notified().await;
                        Ok(())
                    })
                })
                .await
        }
    });
    held.notified().await;
    HeldWriteBatch { holder, release }
}
