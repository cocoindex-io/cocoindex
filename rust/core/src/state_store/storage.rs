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
use crate::state_store::txn::{Env, EnvSlot, WriteTxn, opened_env};

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
/// reachable. Same two-phase policy. Caller must already hold a coordinator
/// read guard before opening the LMDB read transaction.
fn open_read_txn_on_env_with_retry(env: &Env) -> Result<heed::RoTxn<'_, heed::WithoutTls>> {
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
    env: Arc<EnvSlot>,
    batcher: Batcher<TxnRunner>,
}

/// Type-erased body for a batched write transaction. Each body returns a
/// future that runs against the shared `WriteTxn` and resolves to a boxed
/// output. The future is bound to the borrow of the txn (`'a`).
///
/// `Fn` (not `FnOnce`) because the runner may call a body again, with a
/// fresh write transaction, after aborting the txn it ran in — on
/// `MDB_MAP_FULL`, once the env is resized, or when another body sharing
/// the txn fails (see [`TxnRunner::run_isolating_failures`]) — and returns
/// only the last call's output. Callers must therefore ensure their
/// closures are side-effect–free on the captured state (i.e. they may be
/// invoked more than once): clone captures inside the closure rather than
/// moving them out, and never accumulate into shared state from inside the
/// body — a body that reports through a shared slot overwrites it on each
/// run, and the caller acts on it after `run_txn` returns.
type TxnBody = Box<
    dyn for<'a, 'env> Fn(&'a mut WriteTxn<'env>) -> BoxFuture<'a, Result<Box<dyn Any + Send>>>
        + Send,
>;

/// Outcome of one write-txn pass over a sub-batch of bodies.
enum TxnPass {
    /// Every body succeeded and the txn committed: one output per body.
    Committed(Vec<Box<dyn Any + Send>>),
    /// The body at `index` failed with `err`. The txn was aborted, and the
    /// bodies after it did not run.
    BodyFailed { index: usize, err: Error },
}

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

/// When a `MDB_MAP_FULL` error occurs (either from a put inside a body or
/// from the final commit), the write txn and its coordinator read guard are
/// dropped, the coordinator write guard is acquired, the map size is doubled
/// via `env.resize`, and every body of that txn is run again.
///
/// Safety: `resize` is only called while holding the coordinator write guard,
/// which guarantees no read or write LMDB transaction opened through this
/// coordinator is active in the current process.
#[derive(Clone)]
struct TxnRunner {
    env: Arc<EnvSlot>,
}

impl TxnRunner {
    /// Runs `bodies` in order and returns each body's own outcome, so a body
    /// that fails fails only its own caller. Must be polled on a single OS
    /// thread from start to finish — see [`Runner::run`].
    ///
    /// The bodies share one write txn unless one of them fails. LMDB has no
    /// savepoints, so the writes a failing body made before it failed can
    /// only be dropped by aborting the txn, which drops the writes of the
    /// bodies ahead of it too. Those bodies ran cleanly: they run again and
    /// commit in a txn of their own, then the bodies behind the failing one
    /// continue in the next txn, so the bodies still commit in input order.
    /// Committing the bodies ahead on their own, rather than running them
    /// again with the rest, keeps each body to at most two runs however many
    /// of its batch mates fail — barring `MDB_MAP_FULL` retries, and bodies
    /// that fail only when run again.
    ///
    /// A failure that is no one body's doing — opening the txn, growing the
    /// map, committing — goes to every body of that txn.
    async fn run_isolating_failures(&self, bodies: &[TxnBody]) -> Vec<Result<Box<dyn Any + Send>>> {
        let mut outcomes: Vec<Option<Result<Box<dyn Any + Send>>>> =
            std::iter::repeat_with(|| None).take(bodies.len()).collect();
        // Sub-batches still to run, as index ranges over `bodies`. The
        // earliest one is on top, so they run (and commit) in input order.
        let mut pending = vec![0..bodies.len()];
        while let Some(sub) = pending.pop() {
            if sub.is_empty() {
                continue;
            }
            match self.run_with_resize_retry(&bodies[sub.clone()]).await {
                Ok(TxnPass::Committed(outputs)) => {
                    for (outcome, output) in outcomes[sub].iter_mut().zip(outputs) {
                        *outcome = Some(Ok(output));
                    }
                }
                Ok(TxnPass::BodyFailed { index, err }) => {
                    if sub.len() > 1 {
                        debug!(
                            "Body {} of a {}-body write txn failed; committing the others \
                             without it: {err}",
                            index + 1,
                            sub.len()
                        );
                    }
                    let failed = sub.start + index;
                    outcomes[failed] = Some(Err(err));
                    pending.push(failed + 1..sub.end);
                    pending.push(sub.start..failed);
                }
                Err(err) => {
                    let (last, rest) = outcomes[sub]
                        .split_last_mut()
                        .expect("an empty sub-batch is skipped above");
                    for outcome in rest {
                        *outcome = Some(Err(err.replica()));
                    }
                    *last = Some(Err(err));
                }
            }
        }
        outcomes
            .into_iter()
            .map(|outcome| outcome.expect("every body is assigned an outcome"))
            .collect()
    }

    /// Runs `bodies` in one write txn, resizing the map and running them all
    /// again on `MDB_MAP_FULL`.
    async fn run_with_resize_retry(&self, bodies: &[TxnBody]) -> Result<TxnPass> {
        loop {
            match self.try_run_once(bodies).await {
                Err(e) if is_map_full(&e) => {
                    self.resize_on_map_full().await?;
                }
                result => return result,
            }
        }
    }

    /// Attempts one write-txn pass over `bodies`, committing only if every
    /// body succeeds. A body's own failure is reported as
    /// [`TxnPass::BodyFailed`], any other failure (`MapFull` from a body
    /// included) as `Err`; either way the write txn and coordinator read guard
    /// are dropped before this returns. On `MapFull` the caller should resize
    /// (under the coordinator write guard) and retry.
    async fn try_run_once(&self, bodies: &[TxnBody]) -> Result<TxnPass> {
        let env_guard = self.env.read().await;
        let env = opened_env(&env_guard)?;
        let mut outputs = Vec::with_capacity(bodies.len());
        let mut wtxn = WriteTxn::new(env.write_txn()?);
        for (index, body) in bodies.iter().enumerate() {
            match body(&mut wtxn).await {
                Ok(output) => outputs.push(output),
                // The map filled up under the txn's writes as a whole, not
                // this body's alone.
                Err(err) if is_map_full(&err) => return Err(err),
                Err(err) => return Ok(TxnPass::BodyFailed { index, err }),
            }
        }
        wtxn.into_inner().commit()?;
        Ok(TxnPass::Committed(outputs))
    }

    /// Doubles the env's current map size (aligned to page). Caller must hold
    /// the coordinator write guard before calling `Env::resize`.
    fn next_map_size(env: &Env) -> Result<usize> {
        let current = env.info().map_size;
        let doubled = current.checked_mul(MAP_SIZE_GROWTH_FACTOR).ok_or_else(|| {
            internal_error!("LMDB map size overflow while doubling: current={current} bytes")
        })?;
        Ok(align_map_size_to_page(doubled))
    }

    async fn resize_on_map_full(&self) -> Result<usize> {
        let resize_guard = self.env.write().await;
        let env = opened_env(&resize_guard)?;
        let new_size = Self::next_map_size(env)?;
        warn!(
            "LMDB map full, auto-resizing to {} bytes and retrying",
            new_size
        );
        // Safety: `resize_guard` excludes all coordinator-participating LMDB
        // transactions in this process; the failed write txn and its read
        // guard were dropped before this path runs.
        unsafe {
            env.resize(new_size)?;
        }
        drop(resize_guard);
        Ok(new_size)
    }
}

#[async_trait]
impl Runner for TxnRunner {
    type Input = TxnBody;
    /// Per-body outcome. Sharing a write txn is an optimization that must not
    /// couple the bodies' fates, so a body's failure is reported to its own
    /// caller rather than as a batch-level `Err`, which the batcher would fan
    /// out to every caller of the batch.
    type Output = Result<Box<dyn Any + Send>>;

    /// LMDB ties a write transaction to the OS thread that began it: only that
    /// thread can release the writer lock, and LMDB ignores a failed release.
    /// A write txn that begins on one runtime worker and commits or aborts on
    /// another — which work-stealing allows at any `.await` that suspends —
    /// leaves the lock held for good and blocks every later writer.
    ///
    /// So the whole batch — every write txn it takes — runs on one
    /// blocking-pool thread, where `block_on` polls the bodies instead of the
    /// runtime's workers.
    async fn run(
        &self,
        inputs: Vec<TxnBody>,
    ) -> Result<impl ExactSizeIterator<Item = Self::Output>> {
        let runner = self.clone();
        let runtime = tokio::runtime::Handle::current();
        let span = Span::current();
        let outcomes = tokio::task::spawn_blocking(move || {
            runtime.block_on(runner.run_isolating_failures(&inputs).instrument(span))
        })
        .await?;
        Ok(outcomes.into_iter())
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
        Ok(Self::from_env(db_env))
    }

    /// Construct a `Storage` that takes ownership of an already-open
    /// `heed::Env`. Unit tests use it directly to open an env without going
    /// through `StorageSettings`; no other clone of `db_env` may outlive the
    /// call, or [`Self::close`] can't close it.
    pub(crate) fn from_env(db_env: Env) -> Self {
        let env = Arc::new(EnvSlot::new(Some(db_env)));
        let batcher = Batcher::new(
            TxnRunner { env: env.clone() },
            Arc::new(BatchQueue::new()),
            BatchingOptions::default(),
        );
        Self {
            inner: Arc::new(StorageInner { env, batcher }),
        }
    }

    pub(crate) fn txn_coordinator(&self) -> Arc<EnvSlot> {
        self.inner.env.clone()
    }

    /// Close the LMDB env once the transactions in flight finish, so the same
    /// db path can be opened again in this process even while clones of this
    /// `Storage` are still alive. Every later operation fails with a client
    /// error. Idempotent.
    pub async fn close(&self) {
        let Some(env) = self.inner.env.write().await.take() else {
            return;
        };
        // Only the slot owns the env: transactions borrow it, or own a clone
        // only while they hold a coordinator read guard, which the write
        // guard above has excluded. So this drop closes it.
        let closing = env.prepare_for_closing();
        debug_assert!(
            closing.wait_timeout(std::time::Duration::ZERO),
            "LMDB env still referenced after close"
        );
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
    /// batch are awaited sequentially against the same txn, in call order.
    ///
    /// A failing body fails only its own caller: if `body` resolves to `Err`,
    /// this call returns that error and none of the body's writes commit,
    /// while every other caller in the batch still gets its own body's
    /// result. LMDB can only drop a body's writes by aborting the whole txn,
    /// so the bodies that ran ahead of a failing one run again in a new txn.
    /// A failure that no body caused, such as a failed commit, still reaches
    /// every caller whose body was in that txn.
    ///
    /// That re-run, and the one after `MDB_MAP_FULL` from any body or the
    /// commit (the map is grown and the txn's bodies re-run on a fresh txn),
    /// mean `body` may run more than once per call, with only its last run's
    /// output returned, so it must be replay-safe. `Fn` rules out moving out
    /// of captures, but nothing checks for side effects outside the txn,
    /// which an aborted run doesn't roll back: hand results out through the
    /// return value (or a shared slot each run overwrites), never by
    /// accumulating into shared state.
    ///
    /// A batch — opening each txn, every body, each commit or rollback — runs
    /// on one blocking-pool thread, because LMDB requires a write txn to begin
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
        // keeping the outer closure `Fn` (re-runnable, see `TxnBody`).
        let erased: TxnBody = Box::new(move |wtxn| {
            let future = body(wtxn);
            Box::pin(async move {
                let value = future.await?;
                Ok(Box::new(value) as Box<dyn Any + Send>)
            })
        });
        // The outer `Result` is the batcher's own failure (e.g. a body that
        // panicked); the inner one is this body's outcome.
        let output = self.inner.batcher.run(erased).await??;
        output
            .downcast::<T>()
            .map(|b| *b)
            .map_err(|_| internal_error!("Storage::run_txn: output type mismatch"))
    }

    /// Create the per-app sub-database and wrap it in an `AppStore`.
    pub async fn create_app_store(&self, app_name: &str) -> Result<AppStore> {
        let env_guard = self.inner.env.read().await;
        let env = opened_env(&env_guard)?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database(&mut wtxn, Some(app_name))?;
        wtxn.commit()?;
        Ok(AppStore::new(db, self.clone()))
    }

    /// Open the per-app sub-database by name, or `None` if it doesn't exist.
    /// Opens an internal read transaction for the lookup.
    pub async fn open_app_store_by_name(&self, app_name: &str) -> Result<Option<AppStore>> {
        let env_guard = self.inner.env.read().await;
        let env = opened_env(&env_guard)?;
        let rtxn = env.read_txn()?;
        let db: Option<Database> = env.open_database(&rtxn, Some(app_name))?;
        // The dbi handle opened in a read txn only becomes usable by other
        // transactions after this txn commits; dropping (aborting) it instead
        // leaves the handle invalid and later reads fail with EINVAL when the
        // sub-database was created by another process.
        rtxn.commit()?;
        Ok(db.map(|db| AppStore::new(db, self.clone())))
    }

    /// Drop an app's data from this LMDB environment. heed 0.22 doesn't
    /// expose `mdb_drop`, so the sub-database stays registered in the
    /// env's catalog but is emptied. `list_app_names` filters out
    /// empty sub-databases, so the app is effectively gone.
    /// Idempotent: dropping a non-existent app is a no-op.
    pub async fn drop_app(&self, app_name: &str) -> Result<()> {
        let env_guard = self.inner.env.read().await;
        let env = opened_env(&env_guard)?;
        let rtxn = env.read_txn()?;
        let db =
            env.open_database::<heed::types::Bytes, heed::types::Bytes>(&rtxn, Some(app_name))?;
        // See `open_app_store_by_name`: commit so the dbi handle stays
        // valid for the write txn below.
        rtxn.commit()?;
        let Some(db) = db else {
            return Ok(());
        };
        let mut wtxn = env.write_txn()?;
        db.clear(&mut wtxn)?;
        wtxn.commit()?;
        Ok(())
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
                &Database,
                &heed::RoTxn<'_, heed::WithoutTls>,
                &tokio::sync::mpsc::Sender<Result<T>>,
            ) -> Result<()>
            + Send
            + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        let env_slot = self.inner.env.clone();
        tokio::task::spawn_blocking(move || {
            let result: Result<()> = (|| {
                let env_guard = env_slot.blocking_read();
                let txn = open_read_txn_on_env_with_retry(opened_env(&env_guard)?)?;
                let db = app_store.db();
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
        db: &Database,
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

    /// List every non-empty named app sub-store in this storage environment.
    /// The "unnamed database" is LMDB's catalog of named sub-databases.
    pub async fn list_app_names(&self) -> Result<Vec<String>> {
        let env_guard = self.inner.env.read().await;
        let db_env = opened_env(&env_guard)?;
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
    use crate::state_store::test_support::{hold_write_batch, make_test_store};
    use std::sync::atomic::{AtomicUsize, Ordering};
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

    /// `close` releases the env while clones of the `Storage` and its
    /// `AppStore`s are still alive: the same path opens again, and the old
    /// handles fail cleanly instead of touching a closed env.
    #[tokio::test]
    async fn close_lets_the_same_path_open_again() {
        let dir = TempDir::new().unwrap();
        let settings = StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: 8,
            lmdb_map_size: DEFAULT_MAP_SIZE,
        };
        let storage = Storage::new(&settings).await.unwrap();
        let app_store = storage.create_app_store("app").await.unwrap();
        let db = app_store.db();
        storage
            .run_txn(move |wtxn| {
                Box::pin(async move {
                    db.put(wtxn, b"key", b"value")?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let error = Storage::new(&settings).await.err().expect("still open");
        assert!(format!("{error:?}").contains("already open"), "{error:?}");

        storage.close().await;
        storage.close().await;

        let closed = |error: Error| format!("{error}").contains("environment is closed");
        assert!(closed(app_store.read_txn().await.err().unwrap()));
        assert!(closed(storage.list_app_names().await.unwrap_err()));
        assert!(closed(
            storage
                .run_txn(|_wtxn| Box::pin(async { Ok(()) }))
                .await
                .unwrap_err()
        ));

        let reopened = Storage::new(&settings).await.unwrap();
        let app_store = reopened
            .open_app_store_by_name("app")
            .await
            .unwrap()
            .unwrap();
        let rtxn = app_store.read_txn().await.unwrap();
        assert_eq!(
            app_store.db().get(&rtxn, b"key").unwrap(),
            Some(&b"value"[..])
        );
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
            app_store.env().info().map_size,
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
                        app_store.db().put(wtxn, key.as_bytes(), value)?;
                    }
                    Ok(())
                })
            })
            .await
            .expect("single run_txn should succeed after MapFull resize-and-retry");

        let final_map_size = app_store.env().info().map_size;
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
        let rtxn = app_store.read_txn().await.unwrap();
        for key in ["key_0000", "key_0031", "key_0063"] {
            let bytes = app_store
                .db()
                .get(&*rtxn, key.as_bytes())
                .unwrap()
                .unwrap_or_else(|| panic!("{key} should exist after successful commit"));
            assert_eq!(
                bytes.as_ref(),
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
                            app_store.db().put(wtxn, key.as_bytes(), value)?;
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

        let final_map_size = app_store.env().info().map_size;
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

        let rtxn = app_store.read_txn().await.unwrap();
        for key in ["key_0000", "key_0031", "key_0063"] {
            let bytes = app_store
                .db()
                .get(&*rtxn, key.as_bytes())
                .unwrap()
                .unwrap_or_else(|| panic!("{key} should exist after successful write"));
            assert_eq!(
                bytes.as_ref(),
                payload.as_slice(),
                "{key} payload should match what was written"
            );
        }
    }

    /// Bodies that share a write batch fail independently: a failing body
    /// fails only its own caller and none of its writes commit, while every
    /// other body commits, in call order, without seeing the failed bodies'
    /// writes.
    #[tokio::test]
    async fn failing_bodies_fail_only_their_own_callers() {
        const KEYS: [&str; 5] = ["ok_0", "fail_1", "ok_2", "fail_3", "ok_4"];
        let (app_store, _dir) = make_test_store().await;
        let storage = app_store.storage.clone();
        let batch = hold_write_batch(&storage).await;

        // Queue one call per key into the next batch. Each body writes its
        // key, then fails if the key says so, or else returns the keys it
        // found already written.
        let runs: [Arc<AtomicUsize>; KEYS.len()] = Default::default();
        let mut calls: Vec<_> = std::iter::zip(KEYS, &runs)
            .map(|(key, runs)| {
                let (app_store, runs) = (app_store.clone(), runs.clone());
                Box::pin(storage.run_txn(move |wtxn| {
                    let (app_store, runs) = (app_store.clone(), runs.clone());
                    Box::pin(async move {
                        runs.fetch_add(1, Ordering::SeqCst);
                        let mut written = Vec::new();
                        for other in KEYS {
                            if app_store.db().get(&**wtxn, other.as_bytes())?.is_some() {
                                written.push(other);
                            }
                        }
                        app_store.db().put(wtxn, key.as_bytes(), b"")?;
                        if key.starts_with("fail") {
                            client_bail!("{key} rejected");
                        }
                        Ok(written)
                    })
                }))
            })
            .collect();
        for call in &mut calls {
            assert!(futures::poll!(call.as_mut()).is_pending());
        }
        batch.release().await;

        let [ok_0, fail_1, ok_2, fail_3, ok_4] =
            futures::future::join_all(calls).await.try_into().unwrap();
        assert_eq!(ok_0.unwrap(), Vec::<&str>::new());
        assert_eq!(ok_2.unwrap(), ["ok_0"]);
        assert_eq!(ok_4.unwrap(), ["ok_0", "ok_2"]);
        for (outcome, key) in [(fail_1, "fail_1"), (fail_3, "fail_3")] {
            let err = outcome.expect_err("a failing body fails its own caller");
            assert!(
                err.to_string().contains(&format!("{key} rejected")),
                "{key}'s caller got another body's error: {err}"
            );
        }

        let rtxn = app_store.read_txn().await.unwrap();
        for key in KEYS {
            assert_eq!(
                app_store
                    .db()
                    .get(&*rtxn, key.as_bytes())
                    .unwrap()
                    .is_some(),
                key.starts_with("ok"),
                "{key} must commit iff its body succeeded"
            );
        }
        // `ok_0` and `ok_2` ran ahead of a failing body, so they ran again to
        // commit without it: once more each, although `ok_0` was batched with
        // two failing bodies.
        let runs = runs.map(|runs| runs.load(Ordering::SeqCst));
        assert_eq!(runs, [2, 1, 2, 1, 1], "runs per body of {KEYS:?}");
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
