//! Per-app handle within a [`Storage`](super::Storage).
//!
//! An `AppStore` is a cheap-clone token that identifies which app's entries
//! a typed I/O operation reads or writes. Methods are paired with a
//! [`WriteTxn`](super::WriteTxn) or [`ReadTxn`](super::ReadTxn) (or
//! anything implementing [`AnyTxn`](super::AnyTxn) for the read-only ops),
//! and the txn parameter always comes first.

use std::collections::HashSet;

use cocoindex_utils::deser::from_msgpack_slice;
use cocoindex_utils::fingerprint::Fingerprint;

use crate::prelude::*;
use crate::state::db_schema::{
    ChildExistenceInfo, ComponentMemoizationInfo, DbEntryKey, FunctionMemoizationEntry,
    IdSequencerInfo, StablePathEntryKey, StablePathEntryTrackingInfo, StablePathNodeType,
    TargetStateOwnerInfo,
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
    pub fn read_tracking_info<'a, T: AnyTxn>(
        &self,
        txn: &'a T,
        path: &StablePath,
    ) -> Result<Option<StablePathEntryTrackingInfo<'a>>> {
        let key = key_tracking_info(path)?;
        let data = txn.db_get_bytes(self.db(), &key)?;
        data.map(from_msgpack_slice).transpose().map_err(Into::into)
    }

    /// Write pre-serialized tracking info. Callers serialize externally so
    /// the txn can be re-borrowed mutably after the read-modify-write
    /// pattern used in `pre_commit` (the deserialized `tracking_info`
    /// borrows from the write txn and must be released before writing back).
    pub fn write_tracking_info_raw(
        &self,
        txn: &mut WriteTxn,
        path: &StablePath,
        encoded: &[u8],
    ) -> Result<()> {
        let key = key_tracking_info(path)?;
        self.db().put(&mut **txn, &key, encoded)?;
        Ok(())
    }

    pub fn delete_tracking_info(&self, txn: &mut WriteTxn, path: &StablePath) -> Result<()> {
        let key = key_tracking_info(path)?;
        self.db().delete(&mut **txn, &key)?;
        Ok(())
    }
}

// --- Component memoization -----------------------------------------------

impl AppStore {
    pub fn read_component_memo<'a, T: AnyTxn>(
        &self,
        txn: &'a T,
        path: &StablePath,
    ) -> Result<Option<ComponentMemoizationInfo<'a>>> {
        let key = key_component_memo(path)?;
        let data = txn.db_get_bytes(self.db(), &key)?;
        data.map(from_msgpack_slice).transpose().map_err(Into::into)
    }

    /// Write a pre-serialized component memo. Callers serialize externally
    /// for the read-modify-write pattern (see `update_component_memo_states`
    /// in engine code).
    pub fn write_component_memo_raw(
        &self,
        txn: &mut WriteTxn,
        path: &StablePath,
        encoded: &[u8],
    ) -> Result<()> {
        let key = key_component_memo(path)?;
        self.db().put(&mut **txn, &key, encoded)?;
        Ok(())
    }

    pub fn delete_component_memo(&self, txn: &mut WriteTxn, path: &StablePath) -> Result<()> {
        let key = key_component_memo(path)?;
        self.db().delete(&mut **txn, &key)?;
        Ok(())
    }
}

// --- Function memoization ------------------------------------------------

impl AppStore {
    pub fn read_fn_memo<'a, T: AnyTxn>(
        &self,
        txn: &'a T,
        path: &StablePath,
        fp: Fingerprint,
    ) -> Result<Option<FunctionMemoizationEntry<'a>>> {
        let key = key_fn_memo(path, fp)?;
        let data = txn.db_get_bytes(self.db(), &key)?;
        data.map(from_msgpack_slice).transpose().map_err(Into::into)
    }

    pub fn write_fn_memo(
        &self,
        txn: &mut WriteTxn,
        path: &StablePath,
        fp: Fingerprint,
        entry: &FunctionMemoizationEntry<'_>,
    ) -> Result<()> {
        let key = key_fn_memo(path, fp)?;
        let value = rmp_serde::to_vec_named(entry)?;
        self.db().put(&mut **txn, &key, &value)?;
        Ok(())
    }

    /// GC: delete all function memos for `path` whose fingerprint is NOT in `keep`.
    pub fn retain_fn_memos(
        &self,
        txn: &mut WriteTxn,
        path: &StablePath,
        keep: &HashSet<Fingerprint>,
    ) -> Result<()> {
        let prefix = key_fn_memo_prefix(path)?;
        let db = self.db();
        let mut iter = db.prefix_iter_mut(&mut **txn, &prefix)?;
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
    pub fn read_child_existence<T: AnyTxn>(
        &self,
        txn: &T,
        parent: &StablePath,
        child_key: &StableKey,
    ) -> Result<Option<ChildExistenceInfo>> {
        let key = key_child_existence(parent, child_key)?;
        let data = txn.db_get_bytes(self.db(), &key)?;
        data.map(from_msgpack_slice).transpose().map_err(Into::into)
    }

    pub fn write_child_existence(
        &self,
        txn: &mut WriteTxn,
        parent: &StablePath,
        child_key: &StableKey,
        info: &ChildExistenceInfo,
    ) -> Result<()> {
        let key = key_child_existence(parent, child_key)?;
        let value = rmp_serde::to_vec_named(info)?;
        self.db().put(&mut **txn, &key, &value)?;
        Ok(())
    }

    pub fn delete_child_existence(
        &self,
        txn: &mut WriteTxn,
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
    pub fn list_child_existence<T: AnyTxn>(
        &self,
        txn: &T,
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
    pub fn write_tombstone(
        &self,
        txn: &mut WriteTxn,
        parent: &StablePath,
        relative_path: &StablePath,
    ) -> Result<()> {
        let key = key_tombstone(parent, relative_path)?;
        self.db().put(&mut **txn, &key, &[])?;
        Ok(())
    }

    pub fn delete_tombstone(
        &self,
        txn: &mut WriteTxn,
        parent: &StablePath,
        relative_path: &StablePath,
    ) -> Result<()> {
        let key = key_tombstone(parent, relative_path)?;
        self.db().delete(&mut **txn, &key)?;
        Ok(())
    }

    /// Relative paths of all tombstones for `parent`. Used by
    /// `Committer::launch_child_component_gc` to find which children need GC.
    pub fn list_tombstones<T: AnyTxn>(
        &self,
        txn: &T,
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
    pub fn remove_child_with_tombstone(
        &self,
        txn: &mut WriteTxn,
        parent: &StablePath,
        child_key: &StableKey,
        owner_path: &StablePath,
        relative_child: &StablePath,
    ) -> Result<()> {
        self.delete_child_existence(txn, parent, child_key)?;
        self.write_tombstone(txn, owner_path, relative_child)?;
        Ok(())
    }
}

// --- Inverted target-state owner index -----------------------------------

impl AppStore {
    pub fn read_target_state_owner<T: AnyTxn>(
        &self,
        txn: &T,
        path: &TargetStatePath,
    ) -> Result<Option<TargetStateOwnerInfo>> {
        let key = key_target_state_owner(path)?;
        let data = txn.db_get_bytes(self.db(), &key)?;
        data.map(from_msgpack_slice).transpose().map_err(Into::into)
    }

    pub fn upsert_target_state_owner(
        &self,
        txn: &mut WriteTxn,
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

    pub fn delete_target_state_owner(
        &self,
        txn: &mut WriteTxn,
        path: &TargetStatePath,
    ) -> Result<()> {
        let key = key_target_state_owner(path)?;
        self.db().delete(&mut **txn, &key)?;
        Ok(())
    }
}

// --- ID sequencer --------------------------------------------------------

impl AppStore {
    pub fn peek_id_sequence<T: AnyTxn>(&self, txn: &T, key: &StableKey) -> Result<Option<u64>> {
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

    pub fn write_id_sequence(
        &self,
        txn: &mut WriteTxn,
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
    pub fn reserve_id_range(&self, txn: &mut WriteTxn, key: &StableKey, count: u64) -> Result<u64> {
        let current_next_id = self.peek_id_sequence(txn, key)?.unwrap_or(1);
        self.write_id_sequence(txn, key, current_next_id + count)?;
        Ok(current_next_id)
    }
}

// --- App-level -----------------------------------------------------------

impl AppStore {
    pub fn clear_all(&self, txn: &mut WriteTxn) -> Result<()> {
        self.db().clear(&mut **txn)?;
        Ok(())
    }
}

// --- Path node type ------------------------------------------------------

impl AppStore {
    /// Looks up the node type of `parent_path/key` by reading the parent's
    /// child-existence entry. Used by `pre_commit` path-existence checks.
    pub fn read_path_node_type<T: AnyTxn>(
        &self,
        txn: &T,
        parent_path: StablePathRef<'_>,
        key: &StableKey,
    ) -> Result<Option<StablePathNodeType>> {
        let parent_owned: StablePath = parent_path.into();
        let info = self.read_child_existence(txn, &parent_owned, key)?;
        Ok(info.map(|i| i.node_type))
    }

    /// Ensures `parent_path/key` is recorded with `target_node_type`.
    /// Recurses up the ancestor chain creating directory entries as needed.
    ///
    /// Promotion rule:
    /// - missing → write `target_node_type`
    /// - `Directory` + target=`Component` → upgrade to Component
    /// - anything else → no-op
    pub fn ensure_path_node_type(
        &self,
        txn: &mut WriteTxn,
        parent_path: StablePathRef<'_>,
        key: &StableKey,
        target_node_type: StablePathNodeType,
    ) -> Result<()> {
        let parent_owned: StablePath = parent_path.into();
        let existing = self.read_child_existence(txn, &parent_owned, key)?;
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
                )?;
            }
            _ => {
                // No-op for all other cases
            }
        }
        if existing_node_type.is_none()
            && let Some((parent, key)) = parent_path.split_parent()
        {
            return self.ensure_path_node_type(txn, parent, key, StablePathNodeType::Directory);
        }
        Ok(())
    }
}

// --- Inspection (cross-component scans within one app) -------------------

impl AppStore {
    /// Scan all stable-path entries in this app and return one path per
    /// component / directory.
    pub fn list_all_stable_paths(&self, txn: &ReadTxn) -> Result<Vec<StablePath>> {
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
