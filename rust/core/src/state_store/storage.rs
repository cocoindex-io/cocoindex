//! Per-environment storage handle: selects the backend, batches write
//! transactions, and exposes per-app [`AppStore`] creation.
//!
//! LMDB remains the default. A `postgres://`/`postgresql://` value in
//! [`StorageSettings::db_path`] selects the optional Postgres adapter.
//! `Storage` is cheaply clonable (internally `Arc`-backed) so callers can
//! move it into spawned tasks.

use crate::prelude::*;
use crate::state::db_schema::{
    ChildExistenceInfo, DbEntryKey, StablePathEntryKey, StablePathNodeType,
};
use crate::state::stable_path::{StablePath, StablePathPrefix, StablePathRef};
use crate::state_store::app_store::AppStore;
#[cfg(feature = "postgres")]
use crate::state_store::backend::PostgresBackend;
use crate::state_store::backend::{
    LmdbBackend, LmdbDatabase, StorageBackend, open_read_txn_on_env_with_retry,
};
use crate::state_store::txn::{WriteTxn, WriteTxnInner};

use cocoindex_utils::batching::{BatchQueue, Batcher, BatchingOptions, Runner};
use cocoindex_utils::deser::from_msgpack_slice;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
#[cfg(feature = "postgres")]
use sqlx::Acquire;
use std::any::Any;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const DEFAULT_MAX_DBS: u32 = 1024;
const DEFAULT_MAP_SIZE: usize = 0x1_0000_0000; // 4GiB
const MAP_SIZE_GROWTH_FACTOR: usize = 2;

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
#[derive(Clone, Serialize, Deserialize)]
pub struct StorageSettings {
    /// LMDB directory, or an explicit `postgres://`/`postgresql://` URL.
    ///
    /// Keeping URLs in the historical field means existing Rust struct
    /// literals and the Python flat wire format remain source-compatible.
    pub db_path: PathBuf,
    #[serde(default = "default_max_dbs")]
    pub lmdb_max_dbs: u32,
    #[serde(default = "default_map_size")]
    pub lmdb_map_size: usize,
}

impl std::fmt::Debug for StorageSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let db_path = self.db_path.to_string_lossy();
        let displayed = if db_path.contains("://") {
            redact_url(&db_path)
        } else {
            db_path.into_owned()
        };
        f.debug_struct("StorageSettings")
            .field("db_path", &displayed)
            .field("lmdb_max_dbs", &self.lmdb_max_dbs)
            .field("lmdb_map_size", &self.lmdb_map_size)
            .finish()
    }
}

impl StorageSettings {
    fn backend_name(&self) -> Result<&'static str> {
        let Some(url) = self.db_path.to_str() else {
            return Ok("lmdb");
        };
        let Some((scheme, _)) = url.split_once("://") else {
            return Ok("lmdb");
        };
        if scheme.eq_ignore_ascii_case("postgres") || scheme.eq_ignore_ascii_case("postgresql") {
            Ok("postgres")
        } else {
            client_bail!(
                "unsupported storage backend URL scheme: {}",
                redact_url(url)
            )
        }
    }
}

/// Remove credentials from a URL before it reaches logs or `Debug` output.
pub fn redact_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let authority_start = scheme_end + 3;
    let authority_end = url[authority_start..]
        .find(['/', '?', '#'])
        .map(|i| authority_start + i)
        .unwrap_or(url.len());
    let authority = &url[authority_start..authority_end];
    let redacted_authority = match authority.rsplit_once('@') {
        Some((_credentials, host)) => format!("***@{host}"),
        None => authority.to_string(),
    };
    let suffix = &url[authority_end..];
    let Some(query_start) = suffix.find('?') else {
        return format!(
            "{}{}{}",
            &url[..authority_start],
            redacted_authority,
            suffix
        );
    };

    let mut redacted = String::with_capacity(url.len());
    redacted.push_str(&url[..authority_start]);
    redacted.push_str(&redacted_authority);
    redacted.push_str(&suffix[..=query_start]);

    let query_and_fragment = &suffix[query_start + 1..];
    let (query, fragment) = query_and_fragment
        .split_once('#')
        .map_or((query_and_fragment, None), |(query, fragment)| {
            (query, Some(fragment))
        });
    for (index, part) in query.split('&').enumerate() {
        if index > 0 {
            redacted.push('&');
        }
        let key = part.split_once('=').map_or(part, |(key, _)| key);
        if key.to_ascii_lowercase().ends_with("password") {
            redacted.push_str(key);
            redacted.push_str("=***");
        } else {
            redacted.push_str(part);
        }
    }
    if let Some(fragment) = fragment {
        redacted.push('#');
        redacted.push_str(fragment);
    }
    redacted
}

#[derive(Clone)]
pub struct Storage {
    inner: Arc<StorageInner>,
}

struct StorageInner {
    backend: Arc<dyn StorageBackend>,
    coord: Arc<tokio::sync::RwLock<()>>,
    batcher: Batcher<TxnRunner>,
}

/// Type-erased body for a batched write transaction. Each body returns a
/// future that runs against the shared `WriteTxn` and resolves to a boxed
/// output. The future is bound to the borrow of the txn (`'a`).
///
/// `Fn` (not `FnOnce`) so the batcher can retry the entire batch on
/// backend-transient failures: LMDB `MDB_MAP_FULL` (after resizing) and
/// Postgres serialization/deadlock failures (after rollback). Every body is
/// called again with a fresh write transaction, and only the last attempt's
/// outputs are returned. Callers must therefore ensure their closures are
/// side-effect-free on the captured state (i.e. they may be invoked more than
/// once): clone captures inside the closure rather than moving them out, and
/// never accumulate into shared state from inside the body — a body that
/// reports through a shared slot overwrites it on each run, and the caller acts
/// on it after `run_txn` returns.
type TxnBody = Box<
    dyn for<'a, 'env> Fn(&'a mut WriteTxn<'env>) -> BoxFuture<'a, Result<Box<dyn Any + Send>>>
        + Send
        + Sync,
>;

/// Returns `true` if `err` is an LMDB `MDB_MAP_FULL` error.
#[cfg(feature = "postgres")]
fn postgres_error(e: sqlx::Error, context: &'static str) -> Error {
    Error::internal(anyhow::Error::from(e).context(context))
}

#[cfg(feature = "postgres")]
fn is_retryable_postgres_error(err: &Error) -> bool {
    let Error::Internal(anyhow_err) = err.without_contexts() else {
        return false;
    };
    let Some(sqlx_err) = anyhow_err.downcast_ref::<sqlx::Error>() else {
        return false;
    };
    let Some(db_err) = sqlx_err.as_database_error() else {
        return false;
    };
    matches!(db_err.code().as_deref(), Some("40001" | "40P01"))
}

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

/// When a `MDB_MAP_FULL` error occurs (either from a put inside a body or
/// from the final commit), the write txn and its coordinator read guard are
/// dropped, the coordinator write guard is acquired, the map size is doubled
/// via `env.resize`, and the whole batch is retried.
///
/// Safety: `resize` is only called while holding the coordinator write guard,
/// which guarantees no read or write LMDB transaction opened through this
/// coordinator is active in the current process.
#[derive(Clone)]
enum TxnRunner {
    Lmdb {
        db_env: heed::Env<heed::WithoutTls>,
        coord: Arc<tokio::sync::RwLock<()>>,
    },
    #[cfg(feature = "postgres")]
    Postgres(Arc<PostgresBackend>),
}

impl TxnRunner {
    /// Runs `inputs` in one write txn, retrying the whole batch on a
    /// backend-specific transient failure.
    async fn run_with_retry(&self, inputs: &[TxnBody]) -> Result<Vec<Box<dyn Any + Send>>> {
        match self {
            Self::Lmdb { db_env, coord } => {
                let runner = Self::Lmdb {
                    db_env: db_env.clone(),
                    coord: coord.clone(),
                };
                loop {
                    match runner.try_run_once_lmdb(inputs).await {
                        Ok(outputs) => return Ok(outputs),
                        Err(e) if is_map_full(&e) => {
                            runner.resize_on_map_full().await?;
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(backend) => {
                let mut backoff = std::time::Duration::from_millis(10);
                loop {
                    match Self::try_run_once_postgres(backend, inputs).await {
                        Ok(outputs) => return Ok(outputs),
                        Err(e) if is_retryable_postgres_error(&e) => {
                            warn!(
                                "Postgres state transaction retrying after serialization failure"
                            );
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(std::time::Duration::from_secs(1));
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }
    }

    /// Attempts one LMDB write-txn pass over `inputs`. If any body or the
    /// final commit returns an error the write txn and coordinator read guard
    /// are dropped before the error propagates. On `MapFull` the caller should
    /// resize (under the coordinator write guard) and retry.
    async fn try_run_once_lmdb(&self, inputs: &[TxnBody]) -> Result<Vec<Box<dyn Any + Send>>> {
        let (db_env, coord) = match self {
            Self::Lmdb { db_env, coord } => (db_env, coord),
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => {
                client_bail!("LMDB transaction runner received a non-LMDB backend")
            }
        };
        let _read_guard = coord.read().await;
        let mut outputs = Vec::with_capacity(inputs.len());
        let mut wtxn = WriteTxn::lmdb(db_env.write_txn()?);
        for body in inputs {
            outputs.push(body(&mut wtxn).await?);
        }
        match wtxn.into_inner() {
            WriteTxnInner::Lmdb(txn) => txn.commit()?,
            #[cfg(feature = "postgres")]
            WriteTxnInner::Postgres(_) => unreachable!("LMDB runner produced a Postgres txn"),
        }
        Ok(outputs)
    }

    #[cfg(feature = "postgres")]
    async fn try_run_once_postgres(
        backend: &PostgresBackend,
        inputs: &[TxnBody],
    ) -> Result<Vec<Box<dyn Any + Send>>> {
        let _write_guard = backend.write_lock().await;
        let mut conn = backend
            .pool()
            .acquire()
            .await
            .map_err(|e| postgres_error(e, "failed to acquire Postgres state connection"))?;
        let mut txn = conn
            .begin()
            .await
            .map_err(|e| postgres_error(e, "failed to begin Postgres state transaction"))?;
        // SERIALIZABLE catches read-modify-write races. The advisory lock
        // serializes all CocoIndex state transactions in this database so the
        // first implementation is correct even for callers that build
        // multi-row invariants; retries handle deadlock/serialization errors.
        sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .execute(&mut *txn)
            .await
            .map_err(|e| postgres_error(e, "failed to set Postgres state isolation level"))?;
        sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
            .bind(0x434f434f_i32)
            .bind(1_i32)
            .execute(&mut *txn)
            .await
            .map_err(|e| postgres_error(e, "failed to lock Postgres state transaction"))?;

        let mut wtxn = WriteTxn::postgres(txn);
        let mut outputs = Vec::with_capacity(inputs.len());
        for body in inputs {
            match body(&mut wtxn).await {
                Ok(output) => outputs.push(output),
                Err(e) => {
                    if let WriteTxnInner::Postgres(txn) = wtxn.into_inner() {
                        let _ = txn.rollback().await;
                    }
                    return Err(e);
                }
            }
        }
        match wtxn.into_inner() {
            WriteTxnInner::Postgres(txn) => txn
                .commit()
                .await
                .map_err(|e| postgres_error(e, "failed to commit Postgres state transaction"))?,
            WriteTxnInner::Lmdb(_) => unreachable!("Postgres runner produced an LMDB txn"),
        }
        Ok(outputs)
    }

    /// Doubles the env's current map size (aligned to page). Caller must hold
    /// the coordinator write guard before calling `Env::resize`.
    fn next_map_size(db_env: &heed::Env<heed::WithoutTls>) -> Result<usize> {
        let current = db_env.info().map_size;
        let doubled = current.checked_mul(MAP_SIZE_GROWTH_FACTOR).ok_or_else(|| {
            internal_error!("LMDB map size overflow while doubling: current={current} bytes")
        })?;
        Ok(align_map_size_to_page(doubled))
    }

    async fn resize_on_map_full(&self) -> Result<usize> {
        let (db_env, coord) = match self {
            Self::Lmdb { db_env, coord } => (db_env, coord),
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => client_bail!("LMDB resize attempted on a non-LMDB backend"),
        };
        let resize_guard = coord.write().await;
        let new_size = Self::next_map_size(db_env)?;
        warn!(
            "LMDB map full, auto-resizing to {} bytes and retrying",
            new_size
        );
        // Safety: `resize_guard` excludes all coordinator-participating LMDB
        // transactions in this process; the failed write txn and its read
        // guard were dropped before this path runs.
        unsafe {
            db_env.resize(new_size)?;
        }
        drop(resize_guard);
        Ok(new_size)
    }
}

#[async_trait]
impl Runner for TxnRunner {
    type Input = TxnBody;
    type Output = Box<dyn Any + Send>;

    /// LMDB ties a write transaction to the OS thread that began it: only that
    /// thread can release the writer lock, and LMDB ignores a failed release.
    /// A write txn that begins on one runtime worker and commits or aborts on
    /// another — which work-stealing allows at any `.await` that suspends —
    /// leaves the lock held for good and blocks every later writer.
    ///
    /// So the whole LMDB batch runs on one blocking-pool thread, where
    /// `block_on` polls the bodies instead of the runtime's workers. Postgres
    /// transactions are ordinary async transactions and can run directly.
    async fn run(
        &self,
        inputs: Vec<TxnBody>,
    ) -> Result<impl ExactSizeIterator<Item = Box<dyn Any + Send>>> {
        match self {
            Self::Lmdb { .. } => {
                let runner = self.clone();
                let runtime = tokio::runtime::Handle::current();
                let span = Span::current();
                let outputs = tokio::task::spawn_blocking(move || {
                    runtime.block_on(runner.run_with_retry(&inputs).instrument(span))
                })
                .await??;
                Ok(outputs.into_iter())
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => {
                let outputs = self.run_with_retry(&inputs).await?;
                Ok(outputs.into_iter())
            }
        }
    }
}

impl Storage {
    pub async fn new(settings: &StorageSettings) -> Result<Self> {
        let backend_name = settings.backend_name()?;
        let coord = Arc::new(tokio::sync::RwLock::new(()));
        let (backend, batcher): (Arc<dyn StorageBackend>, Batcher<TxnRunner>) = match backend_name {
            "lmdb" => {
                if settings.db_path.as_os_str().is_empty() {
                    client_bail!("Settings.db_path must be provided for the LMDB backend");
                }
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
                let backend = Arc::new(LmdbBackend::new(db_env.clone(), coord.clone()))
                    as Arc<dyn StorageBackend>;
                let batcher = Batcher::new(
                    TxnRunner::Lmdb {
                        db_env: db_env.clone(),
                        coord: coord.clone(),
                    },
                    Arc::new(BatchQueue::new()),
                    BatchingOptions::default(),
                );
                (backend, batcher)
            }
            "postgres" => {
                #[cfg(feature = "postgres")]
                {
                    let url = settings
                        .db_path
                        .to_str()
                        .ok_or_else(|| client_error!("Postgres state URL must be valid UTF-8"))?;
                    let pg = Arc::new(PostgresBackend::connect(url).await?);
                    let backend: Arc<dyn StorageBackend> = pg.clone();
                    let batcher = Batcher::new(
                        TxnRunner::Postgres(pg),
                        Arc::new(BatchQueue::new()),
                        BatchingOptions::default(),
                    );
                    (backend, batcher)
                }
                #[cfg(not(feature = "postgres"))]
                {
                    client_bail!(
                        "Postgres state backend support is not compiled in; rebuild with --features postgres"
                    );
                }
            }
            _ => unreachable!("backend_name validates all schemes"),
        };
        Ok(Self {
            inner: Arc::new(StorageInner {
                backend,
                coord,
                batcher,
            }),
        })
    }

    /// Construct a `Storage` from an already-open `heed::Env`. Used in unit
    /// tests that open an env directly without going through `StorageSettings`.
    #[cfg(test)]
    pub(crate) fn from_env(db_env: heed::Env<heed::WithoutTls>) -> Self {
        let coord = Arc::new(tokio::sync::RwLock::new(()));
        let backend =
            Arc::new(LmdbBackend::new(db_env.clone(), coord.clone())) as Arc<dyn StorageBackend>;
        let batcher = Batcher::new(
            TxnRunner::Lmdb {
                db_env: db_env.clone(),
                coord: coord.clone(),
            },
            Arc::new(BatchQueue::new()),
            BatchingOptions::default(),
        );
        Self {
            inner: Arc::new(StorageInner {
                backend,
                coord,
                batcher,
            }),
        }
    }

    pub(crate) fn txn_coordinator(&self) -> Arc<tokio::sync::RwLock<()>> {
        self.inner.coord.clone()
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
    /// Transient backend failures retry the whole batch on a fresh txn:
    /// LMDB `MDB_MAP_FULL` after resizing the map, and Postgres
    /// serialization/deadlock errors after rollback. Only the last attempt's
    /// output is returned. So `body` may run more than once per call and must
    /// be replay-safe. `Fn` rules out moving out of captures, but nothing
    /// checks for side effects outside the txn, which a failed attempt doesn't
    /// roll back: hand results out through the return value (or a shared slot
    /// each run overwrites), never by accumulating into shared state.
    ///
    /// A batch — opening the txn, every body, the commit or rollback — runs on
    /// one blocking-pool thread, because LMDB requires a write txn to begin
    /// and end on the same OS thread. The writer lock is held throughout, so
    /// a body should only await work that belongs inside the txn.
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

    /// Create the per-app sub-store and wrap it in an `AppStore`.
    pub async fn create_app_store(&self, app_name: &str) -> Result<AppStore> {
        self.inner.backend.create_app(app_name).await?;
        let handle = self
            .inner
            .backend
            .open_app(app_name)
            .await?
            .ok_or_else(|| internal_error!("app store disappeared after creation: {app_name}"))?;
        Ok(AppStore::new(
            handle,
            self.inner.backend.clone(),
            self.clone(),
        ))
    }

    /// Open the per-app sub-store by name, or `None` if it doesn't exist.
    pub async fn open_app_store_by_name(&self, app_name: &str) -> Result<Option<AppStore>> {
        let handle = self.inner.backend.open_app(app_name).await?;
        Ok(handle.map(|handle| AppStore::new(handle, self.inner.backend.clone(), self.clone())))
    }

    /// Drop an app's data. Idempotent: dropping a non-existent app is a no-op.
    pub async fn drop_app(&self, app_name: &str) -> Result<()> {
        self.inner.backend.drop_app(app_name).await
    }

    /// Run `f` with `app_store`'s `(db, txn, sender)` on a
    /// `tokio::task::spawn_blocking` thread, streaming items over the
    /// returned channel. LMDB cursors (`RoPrefix`) wrap a raw
    /// `*mut MDB_cursor` and are `!Send`, so iteration can't be held across
    /// an `.await`; the sync loop on the blocking-pool thread should use
    /// `blocking_send` for backpressure and stop when it fails (receiver
    /// dropped). The rtxn open uses the same `MDB_READERS_FULL` retry policy
    /// as [`AppStore::read_txn`], but sync (since we're off the runtime).
    /// An `Err` from `f` is sent as the final item.
    pub(crate) async fn spawn_read_txn_receiver<T, F>(
        &self,
        app_store: AppStore,
        f: F,
    ) -> tokio::sync::mpsc::Receiver<Result<T>>
    where
        T: Send + 'static,
        F: FnOnce(
                &LmdbDatabase,
                &heed::RoTxn<'_, heed::WithoutTls>,
                &tokio::sync::mpsc::Sender<Result<T>>,
            ) -> Result<()>
            + Send
            + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        let Some(env) = app_store.lmdb_env_handle() else {
            let _ = tx.try_send(Err(client_error!(
                "streaming inspection is not yet supported by the Postgres state backend"
            )));
            return rx;
        };
        let db = app_store
            .db()
            .expect("lmdb_env_handle returned Some for a non-LMDB app");
        let coord = self.inner.coord.clone();
        tokio::task::spawn_blocking(move || {
            let result: Result<()> = (|| {
                let _guard = coord.blocking_read();
                let txn = open_read_txn_on_env_with_retry(&env)?;
                f(&db, &txn, &tx)
            })();
            if let Err(err) = result {
                let _ = tx.blocking_send(Err(err));
            }
        });

        rx
    }

    /// Stream every `(StablePath, node_type)` entry from `app_store` via
    /// a channel (see [`Self::spawn_read_txn_receiver`] for the threading shape).
    pub async fn spawn_stable_path_iter(
        &self,
        app_store: AppStore,
    ) -> tokio::sync::mpsc::Receiver<Result<(StablePath, StablePathNodeType)>> {
        self.spawn_read_txn_receiver(app_store, |db, txn, tx| {
            Self::for_each_stable_path_in_txn(db, txn, |path, node_type| {
                Ok(tx.blocking_send(Ok((path, node_type))).is_ok())
            })
        })
        .await
    }

    /// Walk every stable path in `db` within an open read txn, calling
    /// `emit(path, node_type)` per path in stored order. `emit` returns
    /// `false` to stop early (e.g. when a channel receiver is gone).
    /// Shared by the stable-path streaming above and the detail streaming
    /// in `inspect::db_inspect`, which folds per-path reads into the same
    /// txn.
    pub(crate) fn for_each_stable_path_in_txn(
        db: &LmdbDatabase,
        txn: &heed::RoTxn<'_, heed::WithoutTls>,
        mut emit: impl FnMut(StablePath, StablePathNodeType) -> Result<bool>,
    ) -> Result<()> {
        let encoded_key_prefix =
            DbEntryKey::StablePathPrefixPrefix(StablePathPrefix::default()).encode()?;

        let mut last_prefix: Option<Vec<u8>> = None;
        for entry in db.prefix_iter(txn, &encoded_key_prefix)? {
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
                        db.get(txn, &key_encoded)?
                            .map(from_msgpack_slice::<ChildExistenceInfo>)
                            .transpose()?
                    };
                    info.map(|i| i.node_type)
                        .unwrap_or(StablePathNodeType::Directory)
                } else {
                    StablePathNodeType::Component
                }
            };

            if !emit(path, node_type)? {
                break;
            }
        }
        Ok(())
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

    /// List every non-empty app sub-store in this storage environment.
    pub async fn list_app_names(&self) -> Result<Vec<String>> {
        self.inner.backend.list_app_names().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn postgres_url_without_feature_errors() {
        let settings = StorageSettings {
            db_path: PathBuf::from("postgresql://user:secret@db.example/cocoindex"),
            lmdb_max_dbs: DEFAULT_MAX_DBS,
            lmdb_map_size: DEFAULT_MAP_SIZE,
        };
        let err = match Storage::new(&settings).await {
            Ok(_) => panic!("Postgres URL unexpectedly opened without the postgres feature"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("not compiled in"));
        assert!(!err.to_string().contains("secret"));
    }

    #[test]
    fn storage_url_detection_and_redaction() {
        let pg = StorageSettings {
            db_path: PathBuf::from("postgresql://user:secret@db.example/cocoindex"),
            lmdb_max_dbs: DEFAULT_MAX_DBS,
            lmdb_map_size: DEFAULT_MAP_SIZE,
        };
        assert_eq!(pg.backend_name().unwrap(), "postgres");
        assert!(!format!("{pg:?}").contains("secret"));
        assert!(format!("{pg:?}").contains("***@db.example"));

        let uppercase = StorageSettings {
            db_path: PathBuf::from("POSTGRESQL://user:secret@db.example/cocoindex"),
            lmdb_max_dbs: DEFAULT_MAX_DBS,
            lmdb_map_size: DEFAULT_MAP_SIZE,
        };
        assert_eq!(uppercase.backend_name().unwrap(), "postgres");
        assert!(!format!("{uppercase:?}").contains("secret"));

        let unsupported = StorageSettings {
            db_path: PathBuf::from("mysql://user:secret@db.example/cocoindex"),
            lmdb_max_dbs: DEFAULT_MAX_DBS,
            lmdb_map_size: DEFAULT_MAP_SIZE,
        };
        assert!(unsupported.backend_name().is_err());

        let lmdb = StorageSettings {
            db_path: PathBuf::from("/tmp/cocoindex"),
            lmdb_max_dbs: DEFAULT_MAX_DBS,
            lmdb_map_size: DEFAULT_MAP_SIZE,
        };
        assert_eq!(lmdb.backend_name().unwrap(), "lmdb");
    }

    #[test]
    fn redact_url_removes_credentials() {
        assert_eq!(
            redact_url("postgres://user:secret@db.example:5432/cocoindex?sslmode=require"),
            "postgres://***@db.example:5432/cocoindex?sslmode=require"
        );
        assert_eq!(
            redact_url(
                "postgres://user:secret@db.example:5432/cocoindex?sslmode=require&password=query-secret&sslpassword=ssl-secret"
            ),
            "postgres://***@db.example:5432/cocoindex?sslmode=require&password=***&sslpassword=***"
        );
        assert_eq!(redact_url("/tmp/cocoindex.db"), "/tmp/cocoindex.db");
    }

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

    /// Integration test for `MDB_MAP_FULL` auto-resize on the `Storage::run_txn`
    /// path: one batched write txn is filled past the map limit, the runner
    /// doubles the map, retries the same body, and commits.
    #[tokio::test]
    async fn auto_resizes_on_map_full() {
        let dir = TempDir::new().unwrap();
        let page = page_size::get();
        // Deliberately tiny, page-aligned map. Large enough for env metadata and
        // `create_app_store` (direct write txn, not the batcher).
        let initial_map_size = align_map_size_to_page(page * 16);

        let settings = StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: 8,
            lmdb_map_size: initial_map_size,
        };
        let storage = Storage::new(&settings).await.unwrap();
        let app_store = storage.create_app_store("resize_test").await.unwrap();
        assert_eq!(
            app_store.lmdb_env().info().map_size,
            initial_map_size,
            "initial map size should match configured value"
        );

        // One payload pattern; 16 KiB per key. Total raw value bytes exceed the
        // initial map once LMDB btree/metadata overhead is included, so a single
        // `run_txn` body should hit MapFull on put or commit, trigger resize,
        // and succeed only after retry.
        const PAYLOAD_LEN: usize = 16 * 1024;
        const WRITE_COUNT: usize = 64;
        let payload = vec![0xAB_u8; PAYLOAD_LEN];
        let entries: Vec<(String, Vec<u8>)> = (0..WRITE_COUNT)
            .map(|i| (format!("key_{i:04}"), payload.clone()))
            .collect();

        let app_store_for_txn = app_store.clone();
        let entries_for_txn = entries.clone();
        storage
            .run_txn(move |wtxn| {
                let app_store = app_store_for_txn.clone();
                let entries = entries_for_txn.clone();
                Box::pin(async move {
                    for (key, value) in &entries {
                        app_store
                            .put_raw_in_txn(wtxn, key.as_bytes(), value)
                            .await?;
                    }
                    Ok(())
                })
            })
            .await
            .expect("single run_txn should succeed after MapFull resize-and-retry");

        let final_map_size = app_store.lmdb_env().info().map_size;
        let expected_min_final = align_map_size_to_page(initial_map_size * MAP_SIZE_GROWTH_FACTOR);
        assert!(
            final_map_size > initial_map_size,
            "map size must grow after MapFull: initial={initial_map_size}, final={final_map_size}"
        );
        assert!(
            final_map_size >= expected_min_final,
            "map size should at least double: initial={initial_map_size}, \
             final={final_map_size}, expected>={expected_min_final}"
        );

        eprintln!(
            "auto_resizes_on_map_full: initial_map_size={initial_map_size} \
             final_map_size={final_map_size} writes={WRITE_COUNT} \
             bytes_per_key={PAYLOAD_LEN}"
        );

        // Read back first, middle, and last keys; verify full payload bytes.
        for key in ["key_0000", "key_0031", "key_0063"] {
            let bytes = app_store
                .get_raw(key.as_bytes())
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("{key} should exist after successful commit"));
            assert_eq!(
                bytes.as_slice(),
                payload.as_slice(),
                "{key} payload should match what was written"
            );
        }
    }

    /// Verifies the coordinator blocks `Env::resize` until every guarded read
    /// transaction has ended:
    ///
    /// 1. Open a [`ReadTxn`] and keep it alive.
    /// 2. Start a concurrent `Storage::run_txn` write large enough to hit MapFull.
    /// 3. Confirm the write has not finished while the read txn is still open.
    /// 4. Drop the read txn.
    /// 5. Confirm the write completes, the map grows, and data is intact.
    #[tokio::test]
    async fn resize_waits_for_active_reader() {
        use tokio::sync::oneshot;

        let dir = TempDir::new().unwrap();
        let page = page_size::get();
        let initial_map_size = align_map_size_to_page(page * 16);
        let settings = StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: 8,
            lmdb_map_size: initial_map_size,
        };
        let storage = Storage::new(&settings).await.unwrap();
        let app_store = storage.create_app_store("coord_test").await.unwrap();
        let coord = storage.txn_coordinator();

        // Step 1: hold a guarded read transaction open.
        let reader = app_store.read_txn().await.unwrap();

        const PAYLOAD_LEN: usize = 16 * 1024;
        const WRITE_COUNT: usize = 64;
        let payload = vec![0xCD_u8; PAYLOAD_LEN];
        let entries: Vec<(String, Vec<u8>)> = (0..WRITE_COUNT)
            .map(|i| (format!("key_{i:04}"), payload.clone()))
            .collect();

        // Step 2: concurrent write that will trigger MapFull + resize.
        let (write_started_tx, write_started_rx) = oneshot::channel();
        let storage_for_write = storage.clone();
        let app_store_for_write = app_store.clone();
        let entries_for_write = entries.clone();
        let write_handle = tokio::spawn(async move {
            write_started_tx.send(()).ok();
            storage_for_write
                .run_txn(move |wtxn| {
                    let app_store = app_store_for_write.clone();
                    let entries = entries_for_write.clone();
                    Box::pin(async move {
                        for (key, value) in &entries {
                            app_store
                                .put_raw_in_txn(wtxn, key.as_bytes(), value)
                                .await?;
                        }
                        Ok(())
                    })
                })
                .await
        });

        write_started_rx.await.unwrap();

        // Step 3: wait until the resize path holds (or waits for) the coordinator
        // write lock — impossible while our read guard is still alive.
        let mut resize_blocked = false;
        while !write_handle.is_finished() {
            if coord.try_write().is_err() {
                resize_blocked = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            resize_blocked,
            "write should reach MapFull and block on resize while reader is held"
        );
        assert!(
            !write_handle.is_finished(),
            "write should not finish before the read txn is dropped"
        );

        // Step 4: release the read transaction (txn drops before its guard).
        drop(reader);

        // Step 5: write completes, map grows, data is readable.
        write_handle
            .await
            .expect("write task panicked")
            .expect("write should succeed after reader released");

        let final_map_size = app_store.lmdb_env().info().map_size;
        let expected_min_final = align_map_size_to_page(initial_map_size * MAP_SIZE_GROWTH_FACTOR);
        assert!(
            final_map_size > initial_map_size,
            "map size must grow: initial={initial_map_size}, final={final_map_size}"
        );
        assert!(
            final_map_size >= expected_min_final,
            "map size should at least double: initial={initial_map_size}, \
             final={final_map_size}, expected>={expected_min_final}"
        );

        eprintln!(
            "resize_waits_for_active_reader: initial_map_size={initial_map_size} \
             final_map_size={final_map_size}"
        );

        for key in ["key_0000", "key_0031", "key_0063"] {
            let bytes = app_store
                .get_raw(key.as_bytes())
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("{key} should exist after successful write"));
            assert_eq!(
                bytes.as_slice(),
                payload.as_slice(),
                "{key} payload should match what was written"
            );
        }
    }

    /// Regression test for #2424. LMDB's writer lock belongs to the OS thread
    /// that began the write txn; on Linux a release from any other thread
    /// fails silently and wedges every later writer. A body that really
    /// suspends lets a multi-thread runtime resume its task on another
    /// worker, so the runner has to keep the whole txn on one thread.
    #[test]
    fn write_txn_stays_on_one_thread_when_a_body_suspends() {
        use std::time::Duration;

        const ROUNDS: usize = 8;

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_time()
            .build()
            .unwrap();
        let dir = TempDir::new().unwrap();
        let settings = StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: DEFAULT_MAX_DBS,
            lmdb_map_size: 4 * 1024 * 1024,
        };

        let rounds = rt.block_on(async {
            let storage = Storage::new(&settings).await.unwrap();
            let mut rounds = Vec::new();
            for _ in 0..ROUNDS {
                // A txn that ended on the wrong thread leaves the writer lock
                // held, so the next round would block forever rather than fail.
                let round = tokio::time::timeout(
                    Duration::from_secs(30),
                    storage.run_txn(|_wtxn| {
                        Box::pin(async {
                            let before = std::thread::current().id();
                            tokio::time::sleep(Duration::from_millis(5)).await;
                            Ok((before, std::thread::current().id()))
                        })
                    }),
                )
                .await;
                let timed_out = round.is_err();
                rounds.push(round);
                if timed_out {
                    break;
                }
            }
            rounds
        });
        // A wedged writer blocks its thread for good; dropping the runtime
        // would wait on it and turn the failure below into a hang.
        rt.shutdown_background();

        for round in rounds {
            let (before, after) = round
                .expect("write txn blocked on a writer lock leaked by an earlier txn")
                .unwrap();
            assert_eq!(before, after, "write txn resumed on a different thread");
        }
    }
}
