//! Per-environment storage handle: opens the underlying LMDB env, batches
//! write transactions, and exposes per-app [`AppStore`] creation.
//!
//! `Storage` is the env-level analog of the per-app [`AppStore`]. Both are
//! cheaply clonable (internally `Arc`-backed) so callers can move them
//! into spawned threads for inspection-style streaming reads.

use crate::prelude::*;
use crate::state::db_schema::{
    ChildExistenceInfo, DbEntryKey, StablePathEntryKey, StablePathNodeType,
};
use crate::state::stable_path::{StablePath, StablePathPrefix, StablePathRef};
use crate::state_store::app_store::{AppStore, Database};
use crate::state_store::txn::WriteTxn;

use cocoindex_utils::batching::{BatchQueue, Batcher, BatchingOptions, Runner};
use cocoindex_utils::deser::from_msgpack_slice;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const DEFAULT_MAX_DBS: u32 = 1024;
const DEFAULT_MAP_SIZE: usize = 0x1_0000_0000; // 4GiB

/// Sync sibling of [`AppStore::read_txn`]'s `MDB_READERS_FULL` retry,
/// for use inside `spawn_blocking` where the async retry helper isn't
/// reachable. Same two-phase policy.
fn open_read_txn_sync_with_retry(
    env: &heed::Env<heed::WithoutTls>,
) -> Result<heed::RoTxn<'_, heed::WithoutTls>> {
    use std::time::{Duration, Instant};

    const INITIAL_BACKOFF: Duration = Duration::from_millis(10);
    const MAX_BACKOFF: Duration = Duration::from_secs(1);
    const PHASE1_TIMEOUT: Duration = Duration::from_secs(3);

    // Phase 1: short timeout for transient concurrency.
    let phase1_start = Instant::now();
    let mut backoff = INITIAL_BACKOFF;
    loop {
        match env.read_txn() {
            Ok(txn) => return Ok(txn),
            Err(heed::Error::Mdb(heed::MdbError::ReadersFull)) => {
                if phase1_start.elapsed() >= PHASE1_TIMEOUT {
                    break;
                }
                warn!("LMDB readers full, retrying");
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
            Err(e) => return Err(e.into()),
        }
    }

    // Phase 2: clear stale readers, then retry indefinitely.
    let cleared = env.clear_stale_readers()?;
    if cleared > 0 {
        warn!("Cleared {cleared} stale LMDB readers");
    }
    backoff = INITIAL_BACKOFF;
    loop {
        match env.read_txn() {
            Ok(txn) => return Ok(txn),
            Err(heed::Error::Mdb(heed::MdbError::ReadersFull)) => {
                warn!("LMDB readers still full after clearing stale readers, retrying");
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
            Err(e) => return Err(e.into()),
        }
    }
}

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
    batcher: Batcher<TxnRunner>,
}

/// Type-erased body for a batched write transaction. Each body returns a
/// future that runs against the shared `WriteTxn` and resolves to a boxed
/// output. The future is bound to the borrow of the txn (`'a`).
type TxnBody = Box<
    dyn for<'a, 'env> FnOnce(&'a mut WriteTxn<'env>) -> BoxFuture<'a, Result<Box<dyn Any + Send>>>
        + Send,
>;

/// `Runner` implementation that opens a single write txn, runs each batched
/// body against it (awaiting in turn), then commits.
struct TxnRunner {
    db_env: heed::Env<heed::WithoutTls>,
}

#[async_trait]
impl Runner for TxnRunner {
    type Input = TxnBody;
    type Output = Box<dyn Any + Send>;

    async fn run(
        &self,
        inputs: Vec<TxnBody>,
    ) -> Result<impl ExactSizeIterator<Item = Box<dyn Any + Send>>> {
        let mut outputs = Vec::with_capacity(inputs.len());
        let mut wtxn = WriteTxn::new(self.db_env.write_txn()?);
        for body in inputs {
            outputs.push(body(&mut wtxn).await?);
        }
        wtxn.into_inner().commit()?;
        Ok(outputs.into_iter())
    }
}

impl Storage {
    pub async fn new(settings: &StorageSettings) -> Result<Self> {
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
        let batcher = Batcher::new(
            TxnRunner {
                db_env: db_env.clone(),
            },
            Arc::new(BatchQueue::new()),
            BatchingOptions::default(),
        );
        Ok(Self {
            inner: Arc::new(StorageInner { db_env, batcher }),
        })
    }

    /// Construct a `Storage` from an already-open `heed::Env`. Used in tests
    /// where the env is created directly without going through `StorageSettings`.
    pub(crate) fn from_env(db_env: heed::Env<heed::WithoutTls>) -> Self {
        let batcher = Batcher::new(
            TxnRunner {
                db_env: db_env.clone(),
            },
            Arc::new(BatchQueue::new()),
            BatchingOptions::default(),
        );
        Self {
            inner: Arc::new(StorageInner { db_env, batcher }),
        }
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

    /// Run `body` inside a batched write transaction.
    ///
    /// `body` receives `&mut WriteTxn` and returns a `Send` future (typically
    /// `Box::pin(async move { … })`). Multiple concurrent callers' bodies are
    /// coalesced into a single underlying write txn for throughput. FIFO:
    /// the first caller executes inline; concurrent callers queue up and are
    /// flushed together once the current batch commits. Bodies within a
    /// batch are awaited sequentially against the same txn. If any body
    /// resolves to `Err`, the whole batch is rolled back (the `WriteTxn` is
    /// dropped without committing) and every caller in the batch receives
    /// an error.
    ///
    /// The future must be boxed (`BoxFuture<'a, _>` = `Pin<Box<dyn Future +
    /// Send + 'a>>`) because stable Rust can't yet express a `Send` bound on
    /// the future returned by an `AsyncFnOnce` borrowing from the txn.
    pub async fn run_txn<T, F>(&self, body: F) -> Result<T>
    where
        T: Send + 'static,
        F: for<'a, 'env> FnOnce(&'a mut WriteTxn<'env>) -> BoxFuture<'a, Result<T>>
            + Send
            + 'static,
    {
        let erased: TxnBody = Box::new(move |wtxn| {
            Box::pin(async move {
                let value = body(wtxn).await?;
                Ok(Box::new(value) as Box<dyn Any + Send>)
            })
        });
        let output = self.inner.batcher.run(erased).await?;
        output
            .downcast::<T>()
            .map(|b| *b)
            .map_err(|_| internal_error!("Storage::run_txn: output type mismatch"))
    }

    /// Create the per-app sub-database and wrap it in an `AppStore`.
    pub async fn create_app_store(&self, app_name: &str) -> Result<AppStore> {
        let mut wtxn = self.inner.db_env.write_txn()?;
        let db = self
            .inner
            .db_env
            .create_database(&mut wtxn, Some(app_name))?;
        wtxn.commit()?;
        Ok(AppStore::new(db, self.inner.db_env.clone(), self.clone()))
    }

    /// Open the per-app sub-database by name, or `None` if it doesn't exist.
    /// Opens an internal read transaction for the lookup.
    pub async fn open_app_store_by_name(&self, app_name: &str) -> Result<Option<AppStore>> {
        let rtxn = self.inner.db_env.read_txn()?;
        let db: Option<Database> = self.inner.db_env.open_database(&rtxn, Some(app_name))?;
        let env = self.inner.db_env.clone();
        let storage = self.clone();
        Ok(db.map(|db| AppStore::new(db, env, storage.clone())))
    }

    /// Drop an app's data from this LMDB environment. heed 0.22 doesn't
    /// expose `mdb_drop`, so the sub-database stays registered in the
    /// env's catalog but is emptied. `list_app_names` filters out
    /// empty sub-databases, so the app is effectively gone.
    /// Idempotent: dropping a non-existent app is a no-op.
    pub async fn drop_app(&self, app_name: &str) -> Result<()> {
        let rtxn = self.inner.db_env.read_txn()?;
        let db: Option<Database> = self.inner.db_env.open_database(&rtxn, Some(app_name))?;
        drop(rtxn);
        let Some(db) = db else {
            return Ok(());
        };
        let mut wtxn = self.inner.db_env.write_txn()?;
        db.clear(&mut wtxn)?;
        wtxn.commit()?;
        Ok(())
    }

    /// Stream every `(StablePath, node_type)` entry from `app_store` via
    /// a channel. Runs on `tokio::task::spawn_blocking` because the LMDB
    /// cursor (`RoPrefix`) wraps a raw `*mut MDB_cursor` and is `!Send`,
    /// so the cursor can't be held across an `.await`. The sync loop on
    /// the blocking-pool thread uses `blocking_send` for backpressure.
    /// The rtxn open uses the same `MDB_READERS_FULL` retry policy as
    /// [`AppStore::read_txn`], but sync (since we're off the runtime).
    pub async fn spawn_stable_path_iter(
        &self,
        app_store: AppStore,
    ) -> tokio::sync::mpsc::Receiver<Result<(StablePath, StablePathNodeType)>> {
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        tokio::task::spawn_blocking(move || {
            let result: Result<()> = (|| {
                let encoded_key_prefix =
                    DbEntryKey::StablePathPrefixPrefix(StablePathPrefix::default()).encode()?;
                let txn = open_read_txn_sync_with_retry(&app_store.env)?;
                let db = app_store.db();

                let mut last_prefix: Option<Vec<u8>> = None;
                for entry in db.prefix_iter(&txn, &encoded_key_prefix)? {
                    let (raw_key, _) = entry?;
                    if let Some(last_prefix) = &last_prefix
                        && raw_key.starts_with(last_prefix)
                    {
                        continue;
                    }
                    let key: DbEntryKey = DbEntryKey::decode(raw_key)?;
                    let path = match key {
                        DbEntryKey::StablePath(path, _) => path,
                        other => {
                            return Err(internal_error!("Expected StablePath, got {other:?}"));
                        }
                    };
                    last_prefix = Some(DbEntryKey::StablePathPrefix(path.as_ref()).encode()?);

                    let node_type = if path.as_ref().is_empty() {
                        StablePathNodeType::Component
                    } else {
                        let path_ref: StablePathRef<'_> = path.as_ref();
                        if let Some((parent_ref, key)) = path_ref.split_parent() {
                            let parent_owned: StablePath = parent_ref.into();
                            let info = {
                                let key_encoded = DbEntryKey::StablePath(
                                    parent_owned,
                                    StablePathEntryKey::ChildExistence(key.clone()),
                                )
                                .encode()?;
                                db.get(&txn, &key_encoded)?
                                    .map(from_msgpack_slice::<ChildExistenceInfo>)
                                    .transpose()?
                            };
                            info.map(|i| i.node_type)
                                .unwrap_or(StablePathNodeType::Directory)
                        } else {
                            StablePathNodeType::Component
                        }
                    };

                    if tx.blocking_send(Ok((path, node_type))).is_err() {
                        break;
                    }
                }
                Ok(())
            })();
            if let Err(err) = result {
                let _ = tx.blocking_send(Err(err));
            }
        });

        rx
    }

    /// Resolves the app store by name, then spawns the stable-path iteration
    /// thread. Returns `None` if the app's database doesn't exist.
    pub async fn spawn_stable_path_iter_by_name(
        &self,
        app_name: &str,
    ) -> Result<Option<tokio::sync::mpsc::Receiver<Result<(StablePath, StablePathNodeType)>>>> {
        let app_store = self.open_app_store_by_name(app_name).await?;
        Ok(match app_store {
            Some(store) => Some(self.spawn_stable_path_iter(store).await),
            None => None,
        })
    }

    /// List every non-empty named app sub-store in this storage environment.
    /// The "unnamed database" is LMDB's catalog of named sub-databases.
    pub async fn list_app_names(&self) -> Result<Vec<String>> {
        let db_env = &self.inner.db_env;
        let rtxn = db_env.read_txn()?;
        let unnamed: heed::Database<heed::types::Str, heed::types::DecodeIgnore> = db_env
            .open_database(&rtxn, None)?
            .expect("the unnamed database always exists");

        let mut names = Vec::new();
        for result in unnamed.iter(&rtxn)? {
            let (name, ()) = result?;
            if let Ok(Some(db)) =
                db_env.open_database::<heed::types::Bytes, heed::types::Bytes>(&rtxn, Some(name))
                && db.first(&rtxn)?.is_some()
            {
                names.push(name.to_string());
            }
        }
        Ok(names)
    }
}
