//! Per-environment storage handle: opens the underlying LMDB env, hosts
//! the [`TxnBatcher`], and exposes per-app [`AppStore`] creation.
//!
//! `Storage` is the env-level analog of the per-app [`AppStore`]. Both are
//! cheaply clonable (internally `Arc`-backed) so callers can move them
//! into spawned threads for inspection-style streaming reads.

use crate::prelude::*;
use crate::state_store::app_store::{AppStore, Database};
use crate::state_store::txn::ReadTxn;
use crate::state_store::txn_batcher::TxnBatcher;

use cocoindex_utils::retryable;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_MAX_DBS: u32 = 1024;
const DEFAULT_MAP_SIZE: usize = 0x1_0000_0000; // 4GiB

/// Phase 1: short timeout to handle transient concurrency.
static READ_TXN_RETRY_PHASE1: retryable::RetryOptions = retryable::RetryOptions {
    retry_timeout: Some(Duration::from_secs(3)),
    initial_backoff: Duration::from_millis(10),
    max_backoff: Duration::from_secs(1),
};

/// Phase 2: after clearing stale readers, retry indefinitely.
static READ_TXN_RETRY_PHASE2: retryable::RetryOptions = retryable::RetryOptions {
    retry_timeout: None,
    initial_backoff: Duration::from_millis(10),
    max_backoff: Duration::from_secs(1),
};

fn default_max_dbs() -> u32 {
    DEFAULT_MAX_DBS
}

fn default_map_size() -> usize {
    DEFAULT_MAP_SIZE
}

/// Configuration for opening the storage environment.
///
/// The on-disk schema (field names, defaults) is the public configuration
/// surface deserialized from user settings.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StorageSettings {
    pub db_path: PathBuf,
    #[serde(default = "default_max_dbs")]
    pub lmdb_max_dbs: u32,
    #[serde(default = "default_map_size")]
    pub lmdb_map_size: usize,
}

#[derive(Clone)]
pub struct Storage {
    inner: Arc<StorageInner>,
}

struct StorageInner {
    db_env: heed::Env<heed::WithoutTls>,
    txn_batcher: TxnBatcher,
}

impl Storage {
    pub fn new(settings: &StorageSettings) -> Result<Self> {
        let db_path = settings.db_path.join("mdb");
        std::fs::create_dir_all(&db_path)?;
        // Backward compatibility: migrate files from old layout into mdb/.
        Self::migrate_legacy_db_files(&settings.db_path, &db_path)?;
        if settings.lmdb_max_dbs < 1 {
            client_bail!("lmdb_max_dbs must be >= 1, got {}", settings.lmdb_max_dbs);
        }
        if settings.lmdb_map_size == 0 {
            client_bail!("lmdb_map_size must be > 0, got {}", settings.lmdb_map_size);
        }
        let db_env = unsafe {
            heed::EnvOpenOptions::new()
                .read_txn_without_tls()
                .max_dbs(settings.lmdb_max_dbs)
                .map_size(settings.lmdb_map_size)
                .open(db_path)
        }?;
        let cleared_count = db_env.clear_stale_readers()?;
        if cleared_count > 0 {
            info!("Cleared {cleared_count} stale readers");
        }
        let txn_batcher = TxnBatcher::new(db_env.clone());
        Ok(Self {
            inner: Arc::new(StorageInner {
                db_env,
                txn_batcher,
            }),
        })
    }

    /// Migrate legacy files from the old layout (directly in `base_path`)
    /// into the new `db_path` subdirectory.
    fn migrate_legacy_db_files(base_path: &Path, db_path: &Path) -> Result<()> {
        let legacy_files: Vec<PathBuf> = ["data.mdb", "lock.mdb"]
            .iter()
            .map(|name| base_path.join(name))
            .filter(|path| path.exists())
            .collect();
        if legacy_files.is_empty() {
            return Ok(());
        }
        info!(
            "Migrating legacy storage files from {} to {}",
            base_path.display(),
            db_path.display()
        );
        for src in legacy_files {
            let dst = db_path.join(src.file_name().unwrap());
            std::fs::rename(&src, &dst)?;
        }
        Ok(())
    }

    pub fn txn_batcher(&self) -> &TxnBatcher {
        &self.inner.txn_batcher
    }

    /// Create the per-app sub-database and wrap it in an `AppStore`.
    pub fn create_app_store(&self, app_name: &str) -> Result<AppStore> {
        let mut wtxn = self.inner.db_env.write_txn()?;
        let db = self
            .inner
            .db_env
            .create_database(&mut wtxn, Some(app_name))?;
        wtxn.commit()?;
        Ok(AppStore::new(db))
    }

    /// Open the per-app sub-database by name, or `None` if it doesn't exist.
    /// Opens an internal read transaction for the lookup.
    pub fn open_app_store_by_name(&self, app_name: &str) -> Result<Option<AppStore>> {
        let rtxn = self.inner.db_env.read_txn()?;
        let db: Option<Database> = self.inner.db_env.open_database(&rtxn, Some(app_name))?;
        Ok(db.map(AppStore::new))
    }

    /// Open a read transaction with automatic retry on `MDB_READERS_FULL`.
    ///
    /// Two-phase strategy:
    /// 1. Retry with a short timeout — handles transient reader slot contention.
    /// 2. If phase 1 times out, call `clear_stale_readers()` to reclaim slots
    ///    from dead processes, then retry indefinitely.
    pub async fn read_txn(&self) -> Result<ReadTxn<'_>> {
        let db_env = &self.inner.db_env;
        let try_read_txn = || async {
            match db_env.read_txn() {
                Ok(txn) => retryable::Ok(txn),
                Err(heed::Error::Mdb(heed::MdbError::ReadersFull)) => {
                    warn!("Storage readers full, retrying");
                    Err(retryable::Error::retryable(internal_error!(
                        "Storage readers full"
                    )))
                }
                Err(e) => Err(retryable::Error::not_retryable(e)),
            }
        };

        // Phase 1: short timeout for transient concurrency.
        match retryable::run(&try_read_txn, &READ_TXN_RETRY_PHASE1).await {
            Ok(txn) => return Ok(ReadTxn::new(txn)),
            Err(e) if !e.is_retryable => return Err(e.into()),
            Err(_) => {}
        }

        // Phase 2: clear stale readers, then retry indefinitely.
        let cleared = db_env.clear_stale_readers()?;
        if cleared > 0 {
            warn!("Cleared {cleared} stale storage readers");
        }
        retryable::run(&try_read_txn, &READ_TXN_RETRY_PHASE2)
            .await
            .map(ReadTxn::new)
            .map_err(Into::into)
    }

    /// Synchronous, non-retrying read txn for inspection use. Callers tolerate
    /// `MDB_READERS_FULL` at a higher level rather than going through the
    /// engine's two-phase retry path.
    pub fn read_txn_for_inspect(&self) -> Result<ReadTxn<'_>> {
        Ok(ReadTxn::new(self.inner.db_env.read_txn()?))
    }

    /// Internal accessor for the spawn-iter pattern in `state_store::ops`,
    /// which needs to move the env handle into a dedicated thread because
    /// LMDB read transactions and cursors are `!Send`.
    pub(crate) fn heed_env(&self) -> &heed::Env<heed::WithoutTls> {
        &self.inner.db_env
    }
}
