//! Per-app handle within a [`Storage`](super::Storage).
//!
//! An `AppStore` is a cheap-clone token that identifies which app's entries
//! a typed I/O operation reads or writes. Methods are paired with a
//! [`WriteTxn`](super::WriteTxn) or [`ReadTxn`](super::ReadTxn) (or
//! anything implementing [`AnyTxn`](super::AnyTxn) for the read-only ops),
//! and the txn parameter always comes first.
//!
//! All I/O methods are `async fn`. The current LMDB implementation is
//! synchronous internally — the returned futures never yield — but the
//! async signature future-proofs the API.

use std::collections::HashSet;

use cocoindex_utils::deser::from_msgpack_slice;
use cocoindex_utils::fingerprint::Fingerprint;

use crate::prelude::*;
use crate::state::db_schema::{
    ChildExistenceInfo, DbEntryKey, FunctionMemoizationEntry, IdSequencerInfo, StablePathEntryKey,
    StablePathNodeType, TargetStateOwnerInfo,
};
use crate::state::stable_path::{StableKey, StablePath, StablePathPrefix, StablePathRef};
use crate::state::target_state_path::TargetStatePath;
use crate::state_store::txn::{AnyTxn, ReadTxn, WriteTxn};

/// LMDB database handle. Keys and values are opaque bytes; logical
/// key/value schemas live in [`crate::state::db_schema`].
pub(crate) type Database = heed::Database<heed::types::Bytes, heed::types::Bytes>;

/// Per-app handle within a `Storage`.
#[derive(Clone)]
pub struct AppStore {
    pub(crate) db: Database,
}

impl AppStore {
    pub(crate) fn new(db: Database) -> Self {
        Self { db }
    }

    /// Internal accessor for typed-entity methods. External callers reach
    /// the underlying state through method calls, not the raw `Database`.
    pub(crate) fn db(&self) -> Database {
        self.db
    }
}

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
    /// Read raw tracking-info bytes. Returns owned bytes (`Vec<u8>`) so the
    /// caller can deserialize from a local buffer and avoid keeping the txn
    /// borrowed for the deserialized struct's lifetime. Callers typically
    /// then do `from_msgpack_slice::<StablePathEntryTrackingInfo>(&bytes)`.
    pub async fn read_tracking_info<T: AnyTxn>(
        &self,
        txn: &mut T,
        path: &StablePath,
    ) -> Result<Option<Vec<u8>>> {
        let key = key_tracking_info(path)?;
        Ok(txn.db_get_bytes(self.db(), &key)?.map(<[u8]>::to_vec))
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
    /// Read raw component-memo bytes. See [`Self::read_tracking_info`] for
    /// the owned-bytes return rationale.
    pub async fn read_component_memo<T: AnyTxn>(
        &self,
        txn: &mut T,
        path: &StablePath,
    ) -> Result<Option<Vec<u8>>> {
        let key = key_component_memo(path)?;
        Ok(txn.db_get_bytes(self.db(), &key)?.map(<[u8]>::to_vec))
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

// --- Function memoization (per-component, via FnMemoAccessor) -----------

/// Per-component handle that mediates function-memoization reads, writes,
/// and retention during a single component build.
///
/// Engine code routes all per-component function-memo I/O through this type
/// rather than calling [`AppStore`] methods directly, so storage backends
/// that benefit from prefetching (e.g. a future Postgres backend that wants
/// to load all entries for a component in one prefix scan and serve all
/// reads from memory) can introduce a per-build buffer here without
/// touching the engine call sites.
///
/// The accessor is constructed once per component build and held by
/// [`ComponentProcessorContext`](crate::engine::context::ComponentProcessorContext)
/// — engine code reaches it via `comp_ctx.fn_memo_accessor()`, which
/// returns a borrow rather than a fresh value, so any per-build state
/// (a future buffer) persists across the many fn-memo lookups inside a
/// single component's processing phase.
///
/// The current LMDB implementation is a pure passthrough — every call
/// delegates directly to the corresponding [`AppStore`] method, since LMDB's
/// random reads are already memory speed.
pub struct FnMemoAccessor {
    app_store: AppStore,
    component_path: StablePath,
}

impl FnMemoAccessor {
    pub fn new(app_store: AppStore, component_path: StablePath) -> Self {
        Self {
            app_store,
            component_path,
        }
    }

    /// Read raw function-memo bytes for the given fingerprint. Returns
    /// owned bytes; see [`AppStore::read_tracking_info`] for the rationale.
    pub async fn read<T: AnyTxn>(&self, txn: &mut T, fp: Fingerprint) -> Result<Option<Vec<u8>>> {
        let key = key_fn_memo(&self.component_path, fp)?;
        Ok(txn
            .db_get_bytes(self.app_store.db(), &key)?
            .map(<[u8]>::to_vec))
    }

    pub async fn write(
        &self,
        wtxn: &mut WriteTxn<'_>,
        fp: Fingerprint,
        entry: &FunctionMemoizationEntry<'_>,
    ) -> Result<()> {
        let key = key_fn_memo(&self.component_path, fp)?;
        let value = rmp_serde::to_vec_named(entry)?;
        self.app_store.db().put(&mut **wtxn, &key, &value)?;
        Ok(())
    }

    /// GC: delete all function memos for this component whose fingerprint
    /// is NOT in `keep`.
    pub async fn retain(&self, wtxn: &mut WriteTxn<'_>, keep: &HashSet<Fingerprint>) -> Result<()> {
        let prefix = key_fn_memo_prefix(&self.component_path)?;
        let db = self.app_store.db();
        let mut iter = db.prefix_iter_mut(&mut **wtxn, &prefix)?;
        while let Some((raw_key, _)) = iter.next().transpose()? {
            let fp: Fingerprint = storekey::decode(raw_key[prefix.len()..].as_ref())?;
            if keep.contains(&fp) {
                continue;
            }
            // Safety: we drop the borrowed key before the next `next()` call.
            unsafe {
                iter.del_current()?;
            }
        }
        Ok(())
    }
}

// --- Child existence -----------------------------------------------------

impl AppStore {
    pub async fn read_child_existence<T: AnyTxn>(
        &self,
        txn: &mut T,
        parent: &StablePath,
        child_key: &StableKey,
    ) -> Result<Option<ChildExistenceInfo>> {
        let key = key_child_existence(parent, child_key)?;
        let data = txn.db_get_bytes(self.db(), &key)?;
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
    pub async fn list_child_existence<T: AnyTxn>(
        &self,
        txn: &mut T,
        parent: &StablePath,
    ) -> Result<Vec<(StableKey, ChildExistenceInfo)>> {
        let prefix = key_child_existence_prefix(parent)?;
        let mut out = Vec::new();
        for entry in txn.db_prefix_iter(self.db(), &prefix)? {
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

    /// Relative paths of all tombstones for `parent`. Used by
    /// `Committer::launch_child_component_gc` to find which children need GC.
    pub async fn list_tombstones<T: AnyTxn>(
        &self,
        txn: &mut T,
        parent: &StablePath,
    ) -> Result<Vec<StablePath>> {
        let prefix = key_tombstone_prefix(parent)?;
        let mut out = Vec::new();
        for entry in txn.db_prefix_iter(self.db(), &prefix)? {
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
    pub async fn read_target_state_owner<T: AnyTxn>(
        &self,
        txn: &mut T,
        path: &TargetStatePath,
    ) -> Result<Option<TargetStateOwnerInfo>> {
        let key = key_target_state_owner(path)?;
        let data = txn.db_get_bytes(self.db(), &key)?;
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
    pub async fn peek_id_sequence<T: AnyTxn>(
        &self,
        txn: &mut T,
        key: &StableKey,
    ) -> Result<Option<u64>> {
        let db_key = key_id_sequencer(key)?;
        let data = txn.db_get_bytes(self.db(), &db_key)?;
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
        let current_next_id = self.peek_id_sequence(&mut *txn, key).await?.unwrap_or(1);
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
    pub async fn read_path_node_type<T: AnyTxn>(
        &self,
        txn: &mut T,
        parent_path: StablePathRef<'_>,
        key: &StableKey,
    ) -> Result<Option<StablePathNodeType>> {
        let parent_owned: StablePath = parent_path.into();
        let info = self.read_child_existence(txn, &parent_owned, key).await?;
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
        let existing = self.read_child_existence(txn, &parent_owned, key).await?;
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
    /// component / directory.
    pub async fn list_all_stable_paths(&self, txn: &mut ReadTxn<'_>) -> Result<Vec<StablePath>> {
        let encoded_key_prefix =
            DbEntryKey::StablePathPrefixPrefix(StablePathPrefix::default()).encode()?;
        let db = self.db();
        let mut result = Vec::new();
        let mut last_prefix: Option<Vec<u8>> = None;
        for entry in db.prefix_iter(&**txn, &encoded_key_prefix)? {
            let (raw_key, _) = entry?;
            if let Some(last_prefix) = &last_prefix
                && raw_key.starts_with(last_prefix)
            {
                continue;
            }
            let key: DbEntryKey = DbEntryKey::decode(raw_key)?;
            let DbEntryKey::StablePath(path, _) = key else {
                internal_bail!("Expected StablePath, got {key:?}");
            };
            last_prefix = Some(DbEntryKey::StablePathPrefix(path.as_ref()).encode()?);
            result.push(path);
        }
        Ok(result)
    }
}
