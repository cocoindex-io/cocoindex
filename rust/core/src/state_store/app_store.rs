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

/// Per-app handle within a `Storage`. Carries both the `Database` and a
/// clone of the parent `Env` so standalone read methods can open their
/// own `RoTxn` without the caller having to do so.
#[derive(Clone)]
pub struct AppStore {
    pub(crate) db: Database,
    pub(crate) env: heed::Env<heed::WithoutTls>,
}

impl AppStore {
    pub(crate) fn new(db: Database, env: heed::Env<heed::WithoutTls>) -> Self {
        Self { db, env }
    }

    /// Internal accessor for cursor-iteration code (e.g.
    /// `Storage::spawn_iter_stable_paths_with_node_type`) that needs the
    /// raw heed handle.
    pub(crate) fn db(&self) -> Database {
        self.db
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

    pub async fn delete_component_memo(
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
        let key = key_fn_memo(path, fp)?;
        let value = rmp_serde::to_vec_named(entry)?;
        self.db().put(&mut **txn, &key, &value)?;
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
    pub async fn reserve_id_range(
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
