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
const MAP_SIZE_GROWTH_FACTOR: usize = 2;

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

/// Round `requested` up to the next multiple of the OS page size.
///
/// heed/LMDB require `map_size` to be a multiple of the system page size
/// (4 KiB on most Linux, 16 KiB on Apple Silicon), rejecting other values
/// with a hard error. Users shouldn't have to know their page size, so we
/// align for them here. Rounding *up* only raises the cap on how far the
/// memory map may grow — it never shrinks the user's request. We read the
/// page size via the same `page_size` crate heed validates against, so the
/// aligned value is guaranteed to satisfy heed.
fn align_map_size_to_page(requested: usize) -> usize {
    let page = page_size::get();
    // `page` is a power of two on every supported platform, so it can't be 0
    // and `div_ceil` won't divide by zero. `saturating_mul` guards the
    // (practically impossible) case of `requested` being within one page of
    // `usize::MAX`.
    requested.div_ceil(page).saturating_mul(page)
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
///
/// `Fn + Sync` (not `FnOnce`) so the batcher can retry the entire batch on
/// `MDB_MAP_FULL`: the env is resized between attempts, then every body is
/// called again with a fresh write transaction. Callers must therefore
/// ensure their closures are side-effect–free on the captured state (i.e.
/// they may be invoked more than once). In practice all callers clone `Arc`
/// handles inside the closure and do not move-out of captures, so this is
/// already satisfied.
///
/// `Sync` is required because `try_run_once` holds `&[TxnBody]` across
/// `await` points; for `&T` to be `Send`, `T` must be `Sync`.
type TxnBody = Box<
    dyn for<'a, 'env> Fn(&'a mut WriteTxn<'env>) -> BoxFuture<'a, Result<Box<dyn Any + Send>>>
        + Send
        + Sync,
>;

/// Returns `true` if `err` is an LMDB `MDB_MAP_FULL` error.
fn is_map_full(err: &Error) -> bool {
    let inner = err.without_contexts();
    if let Error::Internal(anyhow_err) = inner {
        return matches!(
            anyhow_err.downcast_ref::<heed::Error>(),
            Some(heed::Error::Mdb(heed::MdbError::MapFull))
        );
    }
    false
}

/// `Runner` implementation that opens a single write txn, runs each batched
/// body against it (awaiting in turn), then commits.
///
/// When a `MDB_MAP_FULL` error occurs (either from a put inside a body or
/// from the final commit), the write txn is automatically dropped (aborted),
/// the map size is doubled via `env.resize`, and the whole batch is retried.
/// Safety: `resize` requires no active write transactions; by the time we
/// call it the failed write txn has already been dropped. Read transactions
/// may be active concurrently, but for a single-process LMDB environment
/// (which CocoIndex always is) this is safe.
struct TxnRunner {
    db_env: heed::Env<heed::WithoutTls>,
    /// Current map size in bytes. Updated atomically when the env is resized.
    current_map_size: Arc<std::sync::Mutex<usize>>,
}

impl TxnRunner {
    /// Attempts one write-txn pass over `inputs`. If any body or the final
    /// commit returns an error the write txn is aborted and the error
    /// propagates. On `MapFull` the caller should resize and retry.
    async fn try_run_once(
        &self,
        inputs: &[TxnBody],
    ) -> Result<Vec<Box<dyn Any + Send>>> {
        let mut outputs = Vec::with_capacity(inputs.len());
        let mut wtxn = WriteTxn::new(self.db_env.write_txn()?);
        for body in inputs {
            outputs.push(body(&mut wtxn).await?);
        }
        wtxn.into_inner().commit()?;
        Ok(outputs)
    }

    /// Doubles the tracked map size (aligned to page), stores it, and
    /// returns the new value.
    fn next_map_size(&self) -> usize {
        let mut size = self.current_map_size.lock().unwrap();
        let new_size =
            align_map_size_to_page(size.saturating_mul(MAP_SIZE_GROWTH_FACTOR));
        *size = new_size;
        new_size
    }
}

#[async_trait]
impl Runner for TxnRunner {
    type Input = TxnBody;
    type Output = Box<dyn Any + Send>;

    async fn run(
        &self,
        inputs: Vec<TxnBody>,
    ) -> Result<impl ExactSizeIterator<Item = Box<dyn Any + Send>>> {
        loop {
            match self.try_run_once(&inputs).await {
                Ok(outputs) => return Ok(outputs.into_iter()),
                Err(e) if is_map_full(&e) => {
                    let new_size = self.next_map_size();
                    warn!(
                        "LMDB map full, auto-resizing to {} bytes and retrying",
                        new_size
                    );
                    // Safety: the write txn that hit MapFull was already
                    // dropped (error path of try_run_once). We are the sole
                    // writer (enforced by the Batcher), so no write
                    // transactions are active right now.
                    unsafe { self.db_env.resize(new_size)?; }
                }
                Err(e) => return Err(e),
            }
        }
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
        let map_size = align_map_size_to_page(settings.lmdb_map_size);
        if map_size != settings.lmdb_map_size {
            debug!(
                "Rounded lmdb_map_size up from {} to {} to match the system page size ({})",
                settings.lmdb_map_size,
                map_size,
                page_size::get()
            );
        }
        let db_env = unsafe {
            heed::EnvOpenOptions::new()
                .read_txn_without_tls()
                .max_dbs(settings.lmdb_max_dbs)
                .map_size(map_size)
                .open(db_path)
        }?;
        let cleared_count = db_env.clear_stale_readers()?;
        if cleared_count > 0 {
            info!("Cleared {cleared_count} stale readers");
        }
        let current_map_size = Arc::new(std::sync::Mutex::new(map_size));
        let batcher = Batcher::new(
            TxnRunner {
                db_env: db_env.clone(),
                current_map_size,
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
        let current_map_size = Arc::new(std::sync::Mutex::new(db_env.info().map_size));
        let batcher = Batcher::new(
            TxnRunner {
                db_env: db_env.clone(),
                current_map_size,
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
        F: for<'a, 'env> Fn(&'a mut WriteTxn<'env>) -> BoxFuture<'a, Result<T>>
            + Send
            + Sync
            + 'static,
    {
        // Call `body(wtxn)` rather than wrapping in `async move { body(wtxn).await }`.
        // The latter would move `body` into the async block, making the outer closure
        // `FnOnce`. By calling `body(wtxn)` directly we borrow `body` (via its `Fn`
        // impl) and only move the returned `future` into the mapping async block,
        // keeping the outer closure `Fn` (retryable on `MDB_MAP_FULL`).
        let erased: TxnBody = Box::new(move |wtxn| {
            let future = body(wtxn);
            Box::pin(async move {
                let value = future.await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn align_map_size_rounds_up_to_page_multiple() {
        let page = page_size::get();
        // Exact multiples are left untouched.
        assert_eq!(align_map_size_to_page(page), page);
        assert_eq!(align_map_size_to_page(4 * page), 4 * page);
        // Anything below a full page rounds up to a single page.
        assert_eq!(align_map_size_to_page(1), page);
        assert_eq!(align_map_size_to_page(page - 1), page);
        // A value just past a page boundary rounds up to the next page.
        assert_eq!(align_map_size_to_page(page + 1), 2 * page);

        // The value from the original bug report (10 KiB) becomes a valid
        // page multiple no smaller than what was requested.
        let aligned = align_map_size_to_page(10 * 1024);
        assert_eq!(aligned % page, 0);
        assert!(aligned >= 10 * 1024);
    }

    /// Regression test for the user-facing failure: a `lmdb_map_size` that
    /// isn't a multiple of the system page size used to surface heed's hard
    /// error ("map size (N) must be a multiple of the system page size").
    /// We now align it up transparently, so opening the env just works.
    #[tokio::test]
    async fn new_accepts_unaligned_map_size() {
        let dir = TempDir::new().unwrap();
        let settings = StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: DEFAULT_MAX_DBS,
            // 4 MiB + 1 byte: deliberately not a multiple of any page size,
            // yet large enough to back a real env on both 4 KiB and 16 KiB
            // page platforms once aligned up.
            lmdb_map_size: 4 * 1024 * 1024 + 1,
        };
        Storage::new(&settings).await.unwrap();
    }
}
