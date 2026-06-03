//! Per-app handle within a [`Storage`](super::Storage).
//!
//! An `AppStore` is a cheap-clone token that carries the per-app heed
//! `Database` plus a clone of the parent `Env` so standalone read
//! methods can open their own `RoTxn` (with `MDB_READERS_FULL` retry)
//! without the caller having to manage the transaction.
//!
//! Read methods come in two flavors:
//!
//! * **`*_in_txn(wtxn, ...)`** — reads inside a write transaction; see
//!   uncommitted writes in the same txn. Used by `pre_commit` and
//!   friends inside `run_txn` bodies.
//! * **Standalone `read_*(...)` / `list_*(...)`** — open their own snapshot
//!   internally. Used by callers that aren't inside a write txn (memo
//!   lookups, GC sweeps, inspection).
//!
//! Only operations actually invoked from both contexts in production
//! expose both shapes (today: just `read_component_memo`). Methods
//! invoked from one context only get only the corresponding flavor.
//!
//! All I/O methods are `async fn`. LMDB is synchronous internally — the
//! returned futures never yield except where the standalone reader's
//! `MDB_READERS_FULL` retry pauses — but the async signature
//! future-proofs the API.

use futures::future::BoxFuture;

use cocoindex_utils::deser::from_msgpack_slice;
use cocoindex_utils::fingerprint::Fingerprint;

use crate::prelude::*;
use crate::state::db_schema::{
    ChildExistenceInfo, DbEntryKey, FunctionMemoizationEntry, IdSequencerInfo, StablePathEntryKey,
    StablePathNodeType, TargetStateOwnerInfo,
};
use crate::state::stable_path::{StableKey, StablePath, StablePathPrefix, StablePathRef};
use crate::state::target_state_path::TargetStatePath;
use crate::state_store::txn::WriteTxn;

/// LMDB database handle. Keys and values are opaque bytes; logical
/// key/value schemas live in [`crate::state::db_schema`].
pub(crate) type Database = heed::Database<heed::types::Bytes, heed::types::Bytes>;

/// Per-app handle within a `Storage`. Carries the `Database`, a clone
/// of the parent `Env` (so standalone read methods can open their own
/// `RoTxn` without the caller having to do so), and a clone of the
/// parent `Storage` (so the session backend can route writes through
/// `Storage::run_txn_boxed`'s single-writer batcher — bypassing it
/// would serialize every per-session write through heed's writer
/// mutex with no amortization).
#[derive(Clone)]
pub struct AppStore {
    pub(crate) db: Database,
    pub(crate) env: heed::Env<heed::WithoutTls>,
    pub(crate) storage: super::storage::Storage,
}

impl AppStore {
    pub(crate) fn new(
        db: Database,
        env: heed::Env<heed::WithoutTls>,
        storage: super::storage::Storage,
    ) -> Self {
        Self { db, env, storage }
    }

    /// Internal accessor for cursor-iteration code (e.g.
    /// `Storage::spawn_stable_path_iter`) that needs the
    /// raw heed handle.
    pub(crate) fn db(&self) -> Database {
        self.db
    }

    /// Run `body` inside a write txn driven by the single-writer
    /// batcher. Concurrent callers coalesce into one underlying
    /// `heed::RwTxn`; bodies within a batch are awaited sequentially.
    /// Every LMDB write path (session writes and the standalone
    /// methods alike) goes through this so that no caller opens
    /// `env.write_txn()` directly — bypassing the batcher would
    /// serialize each call through heed's writer mutex with no
    /// amortization.
    pub(super) async fn run_in_batcher<F>(&self, body: F) -> Result<()>
    where
        F: for<'a, 'env> FnOnce(&'a mut WriteTxn<'env>) -> BoxFuture<'a, Result<()>>
            + Send
            + 'static,
    {
        self.run_in_batcher_typed::<(), _>(move |wtxn| Box::pin(async move { body(wtxn).await }))
            .await
    }

    /// Generic variant of [`Self::run_in_batcher`] that returns a
    /// typed value out of the batched body. Used by methods like
    /// `reserve_id_range` whose batched work computes a fresh value.
    pub(super) async fn run_in_batcher_typed<T, F>(&self, body: F) -> Result<T>
    where
        T: Send + 'static,
        F: for<'a, 'env> FnOnce(&'a mut WriteTxn<'env>) -> BoxFuture<'a, Result<T>>
            + Send
            + 'static,
    {
        self.storage.run_txn(body).await
    }

    /// Open a fresh LMDB read transaction with `MDB_READERS_FULL` retry
    /// (two-phase: short retry → clear stale readers → retry
    /// indefinitely). Used by the standalone read methods and by the
    /// streaming inspection iter.
    pub async fn read_txn(&self) -> Result<heed::RoTxn<'_, heed::WithoutTls>> {
        let env = &self.env;
        let try_open = || async {
            match env.read_txn() {
                Ok(txn) => cocoindex_utils::retryable::Ok(txn),
                Err(heed::Error::Mdb(heed::MdbError::ReadersFull)) => {
                    warn!("LMDB readers full, retrying");
                    Err(cocoindex_utils::retryable::Error::retryable(
                        internal_error!("LMDB readers full"),
                    ))
                }
                Err(e) => Err(cocoindex_utils::retryable::Error::not_retryable(e)),
            }
        };

        // Phase 1: short timeout for transient concurrency.
        match cocoindex_utils::retryable::run(&try_open, &READ_TXN_RETRY_PHASE1).await {
            Ok(txn) => return Ok(txn),
            Err(e) if !e.is_retryable => return Err(e.into()),
            Err(_) => {}
        }

        // Phase 2: clear stale readers, then retry indefinitely.
        let cleared = env.clear_stale_readers()?;
        if cleared > 0 {
            warn!("Cleared {cleared} stale LMDB readers");
        }
        cocoindex_utils::retryable::run(&try_open, &READ_TXN_RETRY_PHASE2)
            .await
            .map_err(Into::into)
    }
}

static READ_TXN_RETRY_PHASE1: cocoindex_utils::retryable::RetryOptions =
    cocoindex_utils::retryable::RetryOptions {
        retry_timeout: Some(std::time::Duration::from_secs(3)),
        initial_backoff: std::time::Duration::from_millis(10),
        max_backoff: std::time::Duration::from_secs(1),
    };

static READ_TXN_RETRY_PHASE2: cocoindex_utils::retryable::RetryOptions =
    cocoindex_utils::retryable::RetryOptions {
        retry_timeout: None,
        initial_backoff: std::time::Duration::from_millis(10),
        max_backoff: std::time::Duration::from_secs(1),
    };

// --- Key encoding helpers (internal) -------------------------------------

fn key_tracking_info(path: &StablePath) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(path.clone(), StablePathEntryKey::TrackingInfo).encode()
}

fn key_component_memo(path: &StablePath) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(path.clone(), StablePathEntryKey::ComponentMemoization).encode()
}

fn key_fn_memo(path: &StablePath, fp: Fingerprint) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(path.clone(), StablePathEntryKey::FunctionMemoization(fp)).encode()
}

fn key_fn_memo_prefix(path: &StablePath) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(path.clone(), StablePathEntryKey::FunctionMemoizationPrefix).encode()
}

fn key_child_existence(parent: &StablePath, child_key: &StableKey) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(
        parent.clone(),
        StablePathEntryKey::ChildExistence(child_key.clone()),
    )
    .encode()
}

fn key_child_existence_prefix(parent: &StablePath) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(parent.clone(), StablePathEntryKey::ChildExistencePrefix).encode()
}

fn key_tombstone(parent: &StablePath, relative_path: &StablePath) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(
        parent.clone(),
        StablePathEntryKey::ChildComponentTombstone(relative_path.clone()),
    )
    .encode()
}

fn key_tombstone_prefix(parent: &StablePath) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(
        parent.clone(),
        StablePathEntryKey::ChildComponentTombstonePrefix,
    )
    .encode()
}

fn key_target_state_owner(path: &TargetStatePath) -> Result<Vec<u8>> {
    DbEntryKey::TargetState(path.clone()).encode()
}

fn key_id_sequencer(key: &StableKey) -> Result<Vec<u8>> {
    DbEntryKey::IdSequencer(key.clone()).encode()
}

fn key_user_state(path: &StablePath, user_key: &StableKey) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(
        path.clone(),
        StablePathEntryKey::UserState(user_key.clone()),
    )
    .encode()
}

fn key_user_state_prefix(path: &StablePath) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(path.clone(), StablePathEntryKey::UserStatePrefix).encode()
}

// --- Tracking info -------------------------------------------------------

impl AppStore {
    /// Read raw tracking-info bytes inside an open write txn. Returns
    /// owned bytes (`Vec<u8>`) so the caller can deserialize from a
    /// local buffer and avoid keeping the txn borrowed for the
    /// deserialized struct's lifetime. Callers typically then do
    /// `from_msgpack_slice::<StablePathEntryTrackingInfo>(&bytes)`.
    pub async fn read_tracking_info_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
    ) -> Result<Option<Vec<u8>>> {
        let key = key_tracking_info(path)?;
        Ok(self.db().get(&**txn, &key)?.map(<[u8]>::to_vec))
    }

    /// Standalone snapshot read of raw tracking-info bytes — no
    /// caller-managed txn. Engine `Committer` uses this to fetch the
    /// post-pre_commit tracking_info for prune+converge, then hands
    /// the new bytes to [`AppStoreTrait::commit`](super::AppStoreTrait::commit)
    /// via the plan.
    pub async fn read_tracking_info(&self, path: &StablePath) -> Result<Option<Vec<u8>>> {
        let rtxn = self.read_txn().await?;
        let key = key_tracking_info(path)?;
        Ok(self.db().get(&rtxn, &key)?.map(<[u8]>::to_vec))
    }

    /// Write pre-serialized tracking info. Callers serialize externally so
    /// the txn can be re-borrowed mutably after the read-modify-write
    /// pattern used in `pre_commit` (the deserialized `tracking_info`
    /// borrows from the write txn and must be released before writing back).
    pub async fn write_tracking_info_raw(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
        encoded: &[u8],
    ) -> Result<()> {
        let key = key_tracking_info(path)?;
        self.db().put(&mut **txn, &key, encoded)?;
        Ok(())
    }

    pub async fn delete_tracking_info(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
    ) -> Result<()> {
        let key = key_tracking_info(path)?;
        self.db().delete(&mut **txn, &key)?;
        Ok(())
    }

    /// Cleanup primitive: read the blob, clear `pending_process_token`
    /// iff it equals `self_token`, write back. Opens its own write txn
    /// (standalone — no caller-provided handle). Idempotent.
    pub async fn clear_staged_tracking(&self, path: &StablePath, self_token: u128) -> Result<()> {
        let env = self.env.clone();
        let path = path.clone();
        let db = self.db();
        let key = key_tracking_info(&path)?;
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut wtxn = env.write_txn()?;
            let Some(bytes) = db.get(&wtxn, &key)? else {
                return Ok(());
            };
            let mut info: crate::state::db_schema::StablePathEntryTrackingInfo<'_> =
                cocoindex_utils::deser::from_msgpack_slice(bytes)?;
            if info.pending_process_token != Some(self_token) {
                return Ok(());
            }
            info.pending_process_token = None;
            let new_bytes = rmp_serde::to_vec_named(&info)?;
            db.put(&mut wtxn, &key, &new_bytes)?;
            wtxn.commit()?;
            Ok(())
        })
        .await
        .map_err(|e| internal_error!("clear_staged_tracking join: {e}"))?
    }

    /// Standalone Phase 5 sweep: delete one tombstone. Routed through
    /// the single-writer batcher so concurrent callers coalesce into
    /// one underlying write txn (opening `env.write_txn()` here would
    /// serialize every per-component sweep through heed's writer mutex
    /// with no amortization). Idempotent — `delete` on a missing key
    /// is a no-op for heed.
    pub async fn cleanup_tombstone_standalone(
        &self,
        parent: &StablePath,
        relative: &StablePath,
    ) -> Result<()> {
        let app_store = self.clone();
        let parent = parent.clone();
        let relative = relative.clone();
        self.run_in_batcher(move |wtxn| {
            Box::pin(async move { app_store.delete_tombstone(wtxn, &parent, &relative).await })
        })
        .await
    }

    /// Standalone existence-chain upsert. Writes the leaf
    /// `__cex(parent_of_leaf, leaf_key, Component)` row; missing
    /// ancestor `Directory` rows are filled in by
    /// [`Self::ensure_path_node_type`]'s recursion, which stops as
    /// soon as it finds an existing row.
    ///
    /// Routed through the single-writer batcher (see
    /// [`Self::cleanup_tombstone_standalone`] for the rationale).
    pub async fn ensure_existence_chain_standalone(&self, path: &StablePath) -> Result<()> {
        let Some((_, _)) = path.as_ref().split_parent() else {
            return Ok(());
        };
        let app_store = self.clone();
        let path = path.clone();
        self.run_in_batcher(move |wtxn| {
            Box::pin(async move {
                let Some((parent, key)) = path.as_ref().split_parent() else {
                    return Ok(());
                };
                let parent_owned: StablePath = parent.into();
                app_store
                    .ensure_path_node_type(
                        wtxn,
                        parent_owned.as_ref(),
                        key,
                        StablePathNodeType::Component,
                    )
                    .await
            })
        })
        .await
    }

    /// Standalone Phase 6: upsert the component memo. Routed through
    /// the single-writer batcher (see [`Self::cleanup_tombstone_standalone`]
    /// for the rationale).
    pub async fn finalize_memoization_standalone(
        &self,
        component_path: &StablePath,
        encoded: &[u8],
    ) -> Result<()> {
        let app_store = self.clone();
        let path = component_path.clone();
        let encoded = encoded.to_vec();
        self.run_in_batcher(move |wtxn| {
            Box::pin(async move {
                app_store
                    .write_component_memo_raw(wtxn, &path, &encoded)
                    .await
            })
        })
        .await
    }

    /// Delete the component-memo row outside a caller-supplied txn.
    /// Routed through the single-writer batcher so concurrent
    /// callers coalesce into one underlying write txn (the same
    /// invariant the LMDB precommit/commit phases rely on; opening
    /// `env.write_txn()` here would serialize every Delete-mode
    /// preflight through heed's writer mutex with no amortization).
    pub async fn delete_component_memo(&self, path: &StablePath) -> Result<()> {
        let app_store = self.clone();
        let path = path.clone();
        self.run_in_batcher(move |wtxn| {
            Box::pin(async move { app_store.delete_component_memo_in_txn(wtxn, &path).await })
        })
        .await
    }

    /// Standalone snapshot read of the `(parent_path, key)` node type.
    pub async fn read_path_node_type(
        &self,
        parent_path: StablePathRef<'_>,
        key: &StableKey,
    ) -> Result<Option<StablePathNodeType>> {
        let rtxn = self.read_txn().await?;
        let parent_owned: StablePath = parent_path.into();
        let cex_key = key_child_existence(&parent_owned, key)?;
        let Some(bytes) = self.db().get(&rtxn, &cex_key)? else {
            return Ok(None);
        };
        let info: ChildExistenceInfo = from_msgpack_slice(bytes)?;
        Ok(Some(info.node_type))
    }

    /// Reserve an ID range outside a caller-supplied txn. Routed
    /// through the single-writer batcher so concurrent callers
    /// coalesce. Returns the first reserved ID.
    pub async fn reserve_id_range(&self, key: &StableKey, count: u64) -> Result<u64> {
        let app_store = self.clone();
        let key = key.clone();
        self.run_in_batcher_typed(move |wtxn| {
            Box::pin(async move { app_store.reserve_id_range_in_txn(wtxn, &key, count).await })
        })
        .await
    }
}

// --- Component memoization -----------------------------------------------

impl AppStore {
    /// Read raw component-memo bytes inside an open write txn. Sees
    /// uncommitted writes in the same txn. Used by the engine's memo
    /// invalidation path.
    pub async fn read_component_memo_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
    ) -> Result<Option<Vec<u8>>> {
        let key = key_component_memo(path)?;
        Ok(self.db().get(&**txn, &key)?.map(<[u8]>::to_vec))
    }

    /// Read raw component-memo bytes from a fresh snapshot. Used by the
    /// memoization-check fast path outside `run_txn`.
    pub async fn read_component_memo(&self, path: &StablePath) -> Result<Option<Vec<u8>>> {
        let rtxn = self.read_txn().await?;
        let key = key_component_memo(path)?;
        Ok(self.db().get(&rtxn, &key)?.map(<[u8]>::to_vec))
    }

    /// Write a pre-serialized component memo. Callers serialize externally
    /// for the read-modify-write pattern (see `update_component_memo_states`
    /// in engine code).
    pub async fn write_component_memo_raw(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
        encoded: &[u8],
    ) -> Result<()> {
        let key = key_component_memo(path)?;
        self.db().put(&mut **txn, &key, encoded)?;
        Ok(())
    }

    pub async fn delete_component_memo_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
    ) -> Result<()> {
        let key = key_component_memo(path)?;
        self.db().delete(&mut **txn, &key)?;
        Ok(())
    }
}

// --- Function memoization ------------------------------------------------

impl AppStore {
    /// List every function memo under `path` from a fresh snapshot. Used
    /// by the per-component `fn_memos` loader outside `run_txn`.
    pub async fn list_fn_memos(&self, path: &StablePath) -> Result<Vec<(Fingerprint, Vec<u8>)>> {
        let rtxn = self.read_txn().await?;
        let prefix = key_fn_memo_prefix(path)?;
        let db = self.db();
        let mut out = Vec::new();
        for entry in db.prefix_iter(&rtxn, &prefix)? {
            let (raw_key, raw_val) = entry?;
            let fp: Fingerprint = storekey::decode(raw_key[prefix.len()..].as_ref())?;
            out.push((fp, raw_val.to_vec()));
        }
        Ok(out)
    }

    pub async fn write_fn_memo(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
        fp: Fingerprint,
        entry: &FunctionMemoizationEntry<'_>,
    ) -> Result<()> {
        let value = rmp_serde::to_vec_named(entry)?;
        self.write_fn_memo_raw(txn, path, fp, &value).await
    }

    pub async fn write_fn_memo_raw(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
        fp: Fingerprint,
        encoded: &[u8],
    ) -> Result<()> {
        let key = key_fn_memo(path, fp)?;
        self.db().put(&mut **txn, &key, encoded)?;
        Ok(())
    }

    pub async fn delete_fn_memo(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
        fp: Fingerprint,
    ) -> Result<()> {
        let key = key_fn_memo(path, fp)?;
        self.db().delete(&mut **txn, &key)?;
        Ok(())
    }

    /// Prefix-delete every function memo under `path`. Used when the cache
    /// was not populated (full_reprocess, delete mode) — see
    /// `FnMemoCache::flush_to_db`.
    pub async fn delete_all_fn_memos(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
    ) -> Result<()> {
        let prefix = key_fn_memo_prefix(path)?;
        let db = self.db();
        let mut iter = db.prefix_iter_mut(&mut **txn, &prefix)?;
        while iter.next().transpose()?.is_some() {
            // Safety: we drop the borrowed key/value before the next `next()`.
            unsafe {
                iter.del_current()?;
            }
        }
        Ok(())
    }
}

// --- Child existence -----------------------------------------------------

impl AppStore {
    pub async fn read_child_existence_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        parent: &StablePath,
        child_key: &StableKey,
    ) -> Result<Option<ChildExistenceInfo>> {
        let key = key_child_existence(parent, child_key)?;
        let data = self.db().get(&**txn, &key)?;
        data.map(from_msgpack_slice).transpose().map_err(Into::into)
    }

    pub async fn write_child_existence(
        &self,
        txn: &mut WriteTxn<'_>,
        parent: &StablePath,
        child_key: &StableKey,
        info: &ChildExistenceInfo,
    ) -> Result<()> {
        let key = key_child_existence(parent, child_key)?;
        let value = rmp_serde::to_vec_named(info)?;
        self.db().put(&mut **txn, &key, &value)?;
        Ok(())
    }

    pub async fn delete_child_existence(
        &self,
        txn: &mut WriteTxn<'_>,
        parent: &StablePath,
        child_key: &StableKey,
    ) -> Result<()> {
        let key = key_child_existence(parent, child_key)?;
        self.db().delete(&mut **txn, &key)?;
        Ok(())
    }

    /// All child-existence entries for `parent`, in sorted-key order (which
    /// matches `BTreeMap<StableKey, _>` iteration order because the on-disk
    /// encoding via `storekey` is order-preserving). Used by
    /// `Committer::update_existence` for the sorted-merge against the
    /// in-memory declared children.
    pub async fn list_child_existence_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        parent: &StablePath,
    ) -> Result<Vec<(StableKey, ChildExistenceInfo)>> {
        let prefix = key_child_existence_prefix(parent)?;
        let mut out = Vec::new();
        for entry in self.db().prefix_iter(&**txn, &prefix)? {
            let (raw_key, raw_value) = entry?;
            let stable_key: StableKey = storekey::decode(raw_key[prefix.len()..].as_ref())?;
            let info: ChildExistenceInfo = from_msgpack_slice(raw_value)?;
            out.push((stable_key, info));
        }
        Ok(out)
    }
}

// --- Tombstones ----------------------------------------------------------

impl AppStore {
    pub async fn write_tombstone(
        &self,
        txn: &mut WriteTxn<'_>,
        parent: &StablePath,
        relative_path: &StablePath,
    ) -> Result<()> {
        let key = key_tombstone(parent, relative_path)?;
        self.db().put(&mut **txn, &key, &[])?;
        Ok(())
    }

    pub async fn delete_tombstone(
        &self,
        txn: &mut WriteTxn<'_>,
        parent: &StablePath,
        relative_path: &StablePath,
    ) -> Result<()> {
        let key = key_tombstone(parent, relative_path)?;
        self.db().delete(&mut **txn, &key)?;
        Ok(())
    }

    /// Relative paths of all tombstones for `parent`, from a fresh
    /// snapshot. Used by `Committer::launch_child_component_gc` to find
    /// which children need GC.
    pub async fn list_tombstones(&self, parent: &StablePath) -> Result<Vec<StablePath>> {
        let rtxn = self.read_txn().await?;
        let prefix = key_tombstone_prefix(parent)?;
        let mut out = Vec::new();
        for entry in self.db().prefix_iter(&rtxn, &prefix)? {
            let (raw_key, _) = entry?;
            let relative: StablePath = storekey::decode(raw_key[prefix.len()..].as_ref())?;
            out.push(relative);
        }
        Ok(out)
    }

    /// Atomic existence-removal + tombstone-write, matching the contract of
    /// `LiveComponentController::delete`'s synchronous step.
    pub async fn remove_child_with_tombstone(
        &self,
        txn: &mut WriteTxn<'_>,
        parent: &StablePath,
        child_key: &StableKey,
        owner_path: &StablePath,
        relative_child: &StablePath,
    ) -> Result<()> {
        self.delete_child_existence(txn, parent, child_key).await?;
        self.write_tombstone(txn, owner_path, relative_child)
            .await?;
        Ok(())
    }
}

// --- Inverted target-state owner index -----------------------------------

impl AppStore {
    pub async fn read_target_state_owner_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &TargetStatePath,
    ) -> Result<Option<TargetStateOwnerInfo>> {
        let key = key_target_state_owner(path)?;
        let data = self.db().get(&**txn, &key)?;
        data.map(from_msgpack_slice).transpose().map_err(Into::into)
    }

    pub async fn upsert_target_state_owner(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &TargetStatePath,
        owner: &StablePath,
    ) -> Result<()> {
        let key = key_target_state_owner(path)?;
        let value = rmp_serde::to_vec_named(&TargetStateOwnerInfo {
            component_path: owner.clone(),
        })?;
        self.db().put(&mut **txn, &key, &value)?;
        Ok(())
    }

    pub async fn delete_target_state_owner(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &TargetStatePath,
    ) -> Result<()> {
        let key = key_target_state_owner(path)?;
        self.db().delete(&mut **txn, &key)?;
        Ok(())
    }
}

// --- ID sequencer --------------------------------------------------------

impl AppStore {
    pub async fn peek_id_sequence_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        key: &StableKey,
    ) -> Result<Option<u64>> {
        let db_key = key_id_sequencer(key)?;
        let data = self.db().get(&**txn, &db_key)?;
        match data {
            None => Ok(None),
            Some(bytes) => {
                let info: IdSequencerInfo = from_msgpack_slice(bytes)?;
                Ok(Some(info.next_id))
            }
        }
    }

    pub async fn write_id_sequence(
        &self,
        txn: &mut WriteTxn<'_>,
        key: &StableKey,
        next_id: u64,
    ) -> Result<()> {
        let db_key = key_id_sequencer(key)?;
        let info = IdSequencerInfo { next_id };
        let value = rmp_serde::to_vec_named(&info)?;
        self.db().put(&mut **txn, &db_key, &value)?;
        Ok(())
    }

    /// Atomically reserve `count` consecutive IDs starting from the next
    /// available ID. Returns the first reserved ID. IDs start at 1
    /// (0 is reserved).
    pub async fn reserve_id_range_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        key: &StableKey,
        count: u64,
    ) -> Result<u64> {
        let current_next_id = self
            .peek_id_sequence_in_txn(&mut *txn, key)
            .await?
            .unwrap_or(1);
        self.write_id_sequence(txn, key, current_next_id + count)
            .await?;
        Ok(current_next_id)
    }
}

// --- App-level -----------------------------------------------------------

impl AppStore {
    pub async fn clear_all(&self, txn: &mut WriteTxn<'_>) -> Result<()> {
        self.db().clear(&mut **txn)?;
        Ok(())
    }
}

// --- Path node type ------------------------------------------------------

impl AppStore {
    /// Looks up the node type of `parent_path/key` by reading the parent's
    /// child-existence entry. Used by `pre_commit` path-existence checks.
    pub async fn read_path_node_type_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        parent_path: StablePathRef<'_>,
        key: &StableKey,
    ) -> Result<Option<StablePathNodeType>> {
        let parent_owned: StablePath = parent_path.into();
        let info = self
            .read_child_existence_in_txn(txn, &parent_owned, key)
            .await?;
        Ok(info.map(|i| i.node_type))
    }

    /// Ensures `parent_path/key` is recorded with `target_node_type`.
    /// Recurses up the ancestor chain creating directory entries as needed.
    ///
    /// Promotion rule:
    /// - missing → write `target_node_type`
    /// - `Directory` + target=`Component` → upgrade to Component
    /// - anything else → no-op
    pub async fn ensure_path_node_type(
        &self,
        txn: &mut WriteTxn<'_>,
        parent_path: StablePathRef<'_>,
        key: &StableKey,
        target_node_type: StablePathNodeType,
    ) -> Result<()> {
        let parent_owned: StablePath = parent_path.into();
        let existing = self
            .read_child_existence_in_txn(txn, &parent_owned, key)
            .await?;
        let existing_node_type = existing.as_ref().map(|i| i.node_type);
        match (existing_node_type, target_node_type) {
            (None, _) | (Some(StablePathNodeType::Directory), StablePathNodeType::Component) => {
                self.write_child_existence(
                    txn,
                    &parent_owned,
                    key,
                    &ChildExistenceInfo {
                        node_type: target_node_type,
                    },
                )
                .await?;
            }
            _ => {
                // No-op for all other cases
            }
        }
        if existing_node_type.is_none()
            && let Some((parent, key)) = parent_path.split_parent()
        {
            return Box::pin(self.ensure_path_node_type(
                txn,
                parent,
                key,
                StablePathNodeType::Directory,
            ))
            .await;
        }
        Ok(())
    }
}

// --- User state ----------------------------------------------------------

impl AppStore {
    /// List all user-state entries for `path` from a fresh snapshot.
    /// Returns `(user_key, value_bytes)` pairs in on-disk key order.
    pub async fn list_user_states(&self, path: &StablePath) -> Result<Vec<(StableKey, Vec<u8>)>> {
        let rtxn = self.read_txn().await?;
        let prefix = key_user_state_prefix(path)?;
        let db = self.db();
        let mut out = Vec::new();
        for entry in db.prefix_iter(&rtxn, &prefix)? {
            let (raw_key, raw_val) = entry?;
            let user_key: StableKey = storekey::decode(raw_key[prefix.len()..].as_ref())?;
            out.push((user_key, raw_val.to_vec()));
        }
        Ok(out)
    }

    pub async fn write_user_state(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
        user_key: &StableKey,
        value: &[u8],
    ) -> Result<()> {
        let key = key_user_state(path, user_key)?;
        self.db().put(&mut **txn, &key, value)?;
        Ok(())
    }

    pub async fn delete_user_state(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
        user_key: &StableKey,
    ) -> Result<()> {
        let key = key_user_state(path, user_key)?;
        self.db().delete(&mut **txn, &key)?;
        Ok(())
    }

    /// Delete every user-state entry under `path`. Used during component GC.
    pub async fn delete_all_user_states(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
    ) -> Result<()> {
        let prefix = key_user_state_prefix(path)?;
        let db = self.db();
        let mut iter = db.prefix_iter_mut(&mut **txn, &prefix)?;
        while iter.next().transpose()?.is_some() {
            // Safety: key/value borrows are dropped before the next iteration.
            unsafe {
                iter.del_current()?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::AppStore;
    use crate::state::stable_path::{StableKey, StablePath};
    use crate::state_store::txn::WriteTxn;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Open a fresh in-process LMDB environment and return an `AppStore`
    /// backed by it. The caller must keep `TempDir` alive for the duration
    /// of the test; dropping it removes the directory.
    async fn make_test_store() -> (AppStore, TempDir) {
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
        let storage = crate::state_store::Storage::from_env(env.clone());
        (AppStore::new(db, env, storage), dir)
    }

    fn comp_path(name: &str) -> StablePath {
        StablePath(Arc::from(vec![StableKey::Str(Arc::from(name))]))
    }

    fn sym(s: &str) -> StableKey {
        StableKey::Symbol(Arc::from(s))
    }

    fn to_map(pairs: Vec<(StableKey, Vec<u8>)>) -> HashMap<StableKey, Vec<u8>> {
        pairs.into_iter().collect()
    }

    // --- list_user_states --------------------------------------------------

    #[tokio::test]
    async fn list_user_states_empty_on_fresh_path() {
        let (store, _dir) = make_test_store().await;
        let result = store.list_user_states(&comp_path("comp")).await.unwrap();
        assert!(result.is_empty());
    }

    // --- write_user_state + list -------------------------------------------

    #[tokio::test]
    async fn write_then_list_returns_all_entries() {
        let (store, _dir) = make_test_store().await;
        let p = comp_path("comp");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, &sym("count"), b"42")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, &sym("name"), b"hello")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, &sym("flag"), b"true")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let entries = to_map(store.list_user_states(&p).await.unwrap());
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[&sym("count")], b"42");
        assert_eq!(entries[&sym("name")], b"hello");
        assert_eq!(entries[&sym("flag")], b"true");
    }

    #[tokio::test]
    async fn write_overwrites_existing_entry() {
        let (store, _dir) = make_test_store().await;
        let p = comp_path("comp");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, &sym("k"), b"v1")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, &sym("k"), b"v2")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let entries = to_map(store.list_user_states(&p).await.unwrap());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[&sym("k")], b"v2");
    }

    // --- delete_user_state -------------------------------------------------

    #[tokio::test]
    async fn delete_selective_within_flush_txn() {
        // A, B, C are written; a second txn updates A and deletes B in one
        // atomic operation; C is untouched.
        let (store, _dir) = make_test_store().await;
        let p = comp_path("comp");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, &sym("a"), b"old_a")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, &sym("b"), b"b_val")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, &sym("c"), b"c_val")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        // write and delete are atomic within the same txn.
        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, &sym("a"), b"new_a")
            .await
            .unwrap();
        store
            .delete_user_state(&mut wtxn, &p, &sym("b"))
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let entries = to_map(store.list_user_states(&p).await.unwrap());
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[&sym("a")], b"new_a");
        assert!(!entries.contains_key(&sym("b")));
        assert_eq!(entries[&sym("c")], b"c_val");
    }

    // --- delete_all_user_states --------------------------------------------

    #[tokio::test]
    async fn delete_all_then_write_within_flush_txn() {
        // A, B, C are written; a second txn calls delete_all then writes
        // A (new value) and D (new key) — all atomically. B and C must be gone.
        let (store, _dir) = make_test_store().await;
        let p = comp_path("comp");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, &sym("a"), b"old_a")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, &sym("b"), b"b_val")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, &sym("c"), b"c_val")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        // delete_all and subsequent writes are atomic within the same txn.
        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store.delete_all_user_states(&mut wtxn, &p).await.unwrap();
        store
            .write_user_state(&mut wtxn, &p, &sym("a"), b"new_a")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, &sym("d"), b"d_val")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let entries = to_map(store.list_user_states(&p).await.unwrap());
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[&sym("a")], b"new_a");
        assert!(!entries.contains_key(&sym("b")));
        assert!(!entries.contains_key(&sym("c")));
        assert_eq!(entries[&sym("d")], b"d_val");
    }

    // --- isolation ---------------------------------------------------------

    #[tokio::test]
    async fn user_states_isolated_by_path() {
        let (store, _dir) = make_test_store().await;
        let p1 = comp_path("comp_a");
        let p2 = comp_path("comp_b");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p1, &sym("k"), b"from_a")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p2, &sym("k"), b"from_b")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let r1 = to_map(store.list_user_states(&p1).await.unwrap());
        let r2 = to_map(store.list_user_states(&p2).await.unwrap());
        assert_eq!(r1.len(), 1);
        assert_eq!(r2.len(), 1);
        assert_eq!(r1[&sym("k")], b"from_a");
        assert_eq!(r2[&sym("k")], b"from_b");
    }
}

// --- Inspection (cross-component scans within one app) -------------------

impl AppStore {
    /// Scan all stable-path entries in this app and return one path per
    /// component / directory, from a fresh snapshot.
    pub async fn list_all_stable_paths(&self) -> Result<Vec<StablePath>> {
        let rtxn = self.read_txn().await?;
        let encoded_key_prefix =
            DbEntryKey::StablePathPrefixPrefix(StablePathPrefix::default()).encode()?;
        let db = self.db();
        let mut out = Vec::new();
        let mut last_prefix: Option<Vec<u8>> = None;
        for entry in db.prefix_iter(&rtxn, &encoded_key_prefix)? {
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
            out.push(path);
        }
        Ok(out)
    }
}

// --- Submit lifecycle (engine-facing shapes) ----------------------------
//
// Convenience aliases for the `*_standalone` helpers above, named to
// match how engine code refers to these operations.

impl AppStore {
    /// Standalone Phase 5 tombstone sweep. See
    /// [`Self::cleanup_tombstone_standalone`].
    pub async fn cleanup_tombstone(
        &self,
        parent_path: &StablePath,
        relative_path: &StablePath,
    ) -> Result<()> {
        self.cleanup_tombstone_standalone(parent_path, relative_path)
            .await
    }

    /// Standalone Phase 6 component-memo persist. See
    /// [`Self::finalize_memoization_standalone`].
    pub async fn finalize_memoization(
        &self,
        component_path: &StablePath,
        encoded: &[u8],
    ) -> Result<()> {
        self.finalize_memoization_standalone(component_path, encoded)
            .await
    }

    /// Standalone existence-chain upsert. `_known_parent_path` is
    /// unused on LMDB — `ensure_path_node_type`'s recursion already
    /// short-circuits at the first existing row — but kept for
    /// signature parity with how engine code calls this.
    pub async fn ensure_existence_chain(
        &self,
        path: &StablePath,
        _known_parent_path: &StablePath,
    ) -> Result<()> {
        self.ensure_existence_chain_standalone(path).await
    }

    /// Spawn a background task that streams every `(StablePath,
    /// StablePathNodeType)` pair in this app's store, in stable-path
    /// order. Iteration runs on a dedicated `spawn_blocking` thread
    /// because the LMDB cursor is `!Send`. Forwards to
    /// [`crate::state_store::Storage::spawn_stable_path_iter`].
    pub async fn spawn_stable_path_iter(
        &self,
    ) -> tokio::sync::mpsc::Receiver<Result<(StablePath, StablePathNodeType)>> {
        self.storage.spawn_stable_path_iter(self.clone()).await
    }
}
