//! Typed per-entity I/O operations.
//!
//! Each function is a thin wrapper around `(key encode) + (msgpack serde) +
//! (heed get/put/delete/iter)`. Signatures are heed-free so the engine never
//! sees LMDB types.
//!
//! Lifetime contract: read functions return schema types parameterized by
//! the transaction lifetime, borrowing from the LMDB mmap pages held by
//! the read transaction.

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
use crate::state_store::app_store::AppStore;
use crate::state_store::storage::Storage;
use crate::state_store::txn::{AnyTxn, ReadTxn, WriteTxn};

// --- Key encoding helpers (internal) ---

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

// --- Tracking info ---

pub fn read_tracking_info<'a, T: AnyTxn>(
    txn: &'a T,
    app_store: &AppStore,
    path: &StablePath,
) -> Result<Option<StablePathEntryTrackingInfo<'a>>> {
    let key = key_tracking_info(path)?;
    let data = txn.db_get_bytes(app_store.db(), &key)?;
    data.map(from_msgpack_slice).transpose().map_err(Into::into)
}

/// Write pre-serialized tracking info. Callers serialize externally so the
/// txn can be re-borrowed mutably after the read-modify-write pattern used
/// in `pre_commit` (where the deserialized `tracking_info` borrows from the
/// write txn and must be released before writing back).
pub fn write_tracking_info_raw(
    txn: &mut WriteTxn,
    app_store: &AppStore,
    path: &StablePath,
    encoded: &[u8],
) -> Result<()> {
    let key = key_tracking_info(path)?;
    app_store.db().put(&mut **txn, &key, encoded)?;
    Ok(())
}

pub fn delete_tracking_info(
    txn: &mut WriteTxn,
    app_store: &AppStore,
    path: &StablePath,
) -> Result<()> {
    let key = key_tracking_info(path)?;
    app_store.db().delete(&mut **txn, &key)?;
    Ok(())
}

// --- Component memoization ---

pub fn read_component_memo<'a, T: AnyTxn>(
    txn: &'a T,
    app_store: &AppStore,
    path: &StablePath,
) -> Result<Option<ComponentMemoizationInfo<'a>>> {
    let key = key_component_memo(path)?;
    let data = txn.db_get_bytes(app_store.db(), &key)?;
    data.map(from_msgpack_slice).transpose().map_err(Into::into)
}

/// Write a pre-serialized component memo. Callers serialize externally so
/// the txn can be re-borrowed mutably after the read-modify-write pattern
/// in [`update_component_memo_states`] (engine code, not in this module).
pub fn write_component_memo_raw(
    txn: &mut WriteTxn,
    app_store: &AppStore,
    path: &StablePath,
    encoded: &[u8],
) -> Result<()> {
    let key = key_component_memo(path)?;
    app_store.db().put(&mut **txn, &key, encoded)?;
    Ok(())
}

pub fn delete_component_memo(
    txn: &mut WriteTxn,
    app_store: &AppStore,
    path: &StablePath,
) -> Result<()> {
    let key = key_component_memo(path)?;
    app_store.db().delete(&mut **txn, &key)?;
    Ok(())
}

// --- Function memoization ---

pub fn read_fn_memo<'a, T: AnyTxn>(
    txn: &'a T,
    app_store: &AppStore,
    path: &StablePath,
    fp: Fingerprint,
) -> Result<Option<FunctionMemoizationEntry<'a>>> {
    let key = key_fn_memo(path, fp)?;
    let data = txn.db_get_bytes(app_store.db(), &key)?;
    data.map(from_msgpack_slice).transpose().map_err(Into::into)
}

pub fn write_fn_memo(
    txn: &mut WriteTxn,
    app_store: &AppStore,
    path: &StablePath,
    fp: Fingerprint,
    entry: &FunctionMemoizationEntry<'_>,
) -> Result<()> {
    let key = key_fn_memo(path, fp)?;
    let value = rmp_serde::to_vec_named(entry)?;
    app_store.db().put(&mut **txn, &key, &value)?;
    Ok(())
}

/// GC: delete all function memos for `path` whose fingerprint is NOT in `keep`.
pub fn retain_fn_memos(
    txn: &mut WriteTxn,
    app_store: &AppStore,
    path: &StablePath,
    keep: &HashSet<Fingerprint>,
) -> Result<()> {
    let prefix = key_fn_memo_prefix(path)?;
    let db = app_store.db();
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

// --- Child existence ---

pub fn read_child_existence<T: AnyTxn>(
    txn: &T,
    app_store: &AppStore,
    parent: &StablePath,
    child_key: &StableKey,
) -> Result<Option<ChildExistenceInfo>> {
    let key = key_child_existence(parent, child_key)?;
    let data = txn.db_get_bytes(app_store.db(), &key)?;
    data.map(from_msgpack_slice).transpose().map_err(Into::into)
}

pub fn write_child_existence(
    txn: &mut WriteTxn,
    app_store: &AppStore,
    parent: &StablePath,
    child_key: &StableKey,
    info: &ChildExistenceInfo,
) -> Result<()> {
    let key = key_child_existence(parent, child_key)?;
    let value = rmp_serde::to_vec_named(info)?;
    app_store.db().put(&mut **txn, &key, &value)?;
    Ok(())
}

pub fn delete_child_existence(
    txn: &mut WriteTxn,
    app_store: &AppStore,
    parent: &StablePath,
    child_key: &StableKey,
) -> Result<()> {
    let key = key_child_existence(parent, child_key)?;
    app_store.db().delete(&mut **txn, &key)?;
    Ok(())
}

/// All child-existence entries for `parent`, in sorted-key order (which
/// matches `BTreeMap<StableKey, _>` iteration order because the on-disk
/// encoding via `storekey` is order-preserving). Used by
/// `Committer::update_existence` for the sorted-merge against the in-memory
/// declared children.
pub fn list_child_existence<T: AnyTxn>(
    txn: &T,
    app_store: &AppStore,
    parent: &StablePath,
) -> Result<Vec<(StableKey, ChildExistenceInfo)>> {
    let prefix = key_child_existence_prefix(parent)?;
    let mut out = Vec::new();
    for entry in txn.db_prefix_iter(app_store.db(), &prefix)? {
        let (raw_key, raw_value) = entry?;
        let stable_key: StableKey = storekey::decode(raw_key[prefix.len()..].as_ref())?;
        let info: ChildExistenceInfo = from_msgpack_slice(raw_value)?;
        out.push((stable_key, info));
    }
    Ok(out)
}

// --- Tombstones ---

pub fn write_tombstone(
    txn: &mut WriteTxn,
    app_store: &AppStore,
    parent: &StablePath,
    relative_path: &StablePath,
) -> Result<()> {
    let key = key_tombstone(parent, relative_path)?;
    app_store.db().put(&mut **txn, &key, &[])?;
    Ok(())
}

pub fn delete_tombstone(
    txn: &mut WriteTxn,
    app_store: &AppStore,
    parent: &StablePath,
    relative_path: &StablePath,
) -> Result<()> {
    let key = key_tombstone(parent, relative_path)?;
    app_store.db().delete(&mut **txn, &key)?;
    Ok(())
}

/// Relative paths of all tombstones for `parent`. Used by
/// `Committer::launch_child_component_gc` to find which children need GC.
pub fn list_tombstones<T: AnyTxn>(
    txn: &T,
    app_store: &AppStore,
    parent: &StablePath,
) -> Result<Vec<StablePath>> {
    let prefix = key_tombstone_prefix(parent)?;
    let mut out = Vec::new();
    for entry in txn.db_prefix_iter(app_store.db(), &prefix)? {
        let (raw_key, _) = entry?;
        let relative: StablePath = storekey::decode(raw_key[prefix.len()..].as_ref())?;
        out.push(relative);
    }
    Ok(out)
}

/// Atomic existence-removal + tombstone-write, matching the contract of
/// `LiveComponentController::delete`'s synchronous step.
pub fn remove_child_with_tombstone(
    txn: &mut WriteTxn,
    app_store: &AppStore,
    parent: &StablePath,
    child_key: &StableKey,
    owner_path: &StablePath,
    relative_child: &StablePath,
) -> Result<()> {
    delete_child_existence(txn, app_store, parent, child_key)?;
    write_tombstone(txn, app_store, owner_path, relative_child)?;
    Ok(())
}

// --- Inverted target-state owner index ---

pub fn read_target_state_owner<T: AnyTxn>(
    txn: &T,
    app_store: &AppStore,
    path: &TargetStatePath,
) -> Result<Option<TargetStateOwnerInfo>> {
    let key = key_target_state_owner(path)?;
    let data = txn.db_get_bytes(app_store.db(), &key)?;
    data.map(from_msgpack_slice).transpose().map_err(Into::into)
}

pub fn upsert_target_state_owner(
    txn: &mut WriteTxn,
    app_store: &AppStore,
    path: &TargetStatePath,
    owner: &StablePath,
) -> Result<()> {
    let key = key_target_state_owner(path)?;
    let value = rmp_serde::to_vec_named(&TargetStateOwnerInfo {
        component_path: owner.clone(),
    })?;
    app_store.db().put(&mut **txn, &key, &value)?;
    Ok(())
}

pub fn delete_target_state_owner(
    txn: &mut WriteTxn,
    app_store: &AppStore,
    path: &TargetStatePath,
) -> Result<()> {
    let key = key_target_state_owner(path)?;
    app_store.db().delete(&mut **txn, &key)?;
    Ok(())
}

// --- ID sequencer ---

pub fn peek_id_sequence<T: AnyTxn>(
    txn: &T,
    app_store: &AppStore,
    key: &StableKey,
) -> Result<Option<u64>> {
    let db_key = key_id_sequencer(key)?;
    let data = txn.db_get_bytes(app_store.db(), &db_key)?;
    match data {
        None => Ok(None),
        Some(bytes) => {
            let info: IdSequencerInfo = from_msgpack_slice(bytes)?;
            Ok(Some(info.next_id))
        }
    }
}

pub fn write_id_sequence(
    txn: &mut WriteTxn,
    app_store: &AppStore,
    key: &StableKey,
    next_id: u64,
) -> Result<()> {
    let db_key = key_id_sequencer(key)?;
    let info = IdSequencerInfo { next_id };
    let value = rmp_serde::to_vec_named(&info)?;
    app_store.db().put(&mut **txn, &db_key, &value)?;
    Ok(())
}

/// Atomically reserve `count` consecutive IDs starting from the next
/// available ID. Returns the first reserved ID. IDs start at 1 (0 is reserved).
pub fn reserve_id_range(
    txn: &mut WriteTxn,
    app_store: &AppStore,
    key: &StableKey,
    count: u64,
) -> Result<u64> {
    let current_next_id = peek_id_sequence(txn, app_store, key)?.unwrap_or(1);
    write_id_sequence(txn, app_store, key, current_next_id + count)?;
    Ok(current_next_id)
}

// --- App-level ---

pub fn clear_all(txn: &mut WriteTxn, app_store: &AppStore) -> Result<()> {
    app_store.db().clear(&mut **txn)?;
    Ok(())
}

// --- Path node type helpers (LMDB-encoded via ChildExistence on parent) ---

/// Looks up the node type of `parent_path/key` by reading the parent's
/// child-existence entry. Used by `pre_commit` path-existence checks.
pub fn read_path_node_type<T: AnyTxn>(
    txn: &T,
    app_store: &AppStore,
    parent_path: StablePathRef<'_>,
    key: &StableKey,
) -> Result<Option<StablePathNodeType>> {
    let parent_owned: StablePath = parent_path.into();
    let info = read_child_existence(txn, app_store, &parent_owned, key)?;
    Ok(info.map(|i| i.node_type))
}

/// Ensures `parent_path/key` is recorded with `target_node_type`. Recurses
/// up the ancestor chain creating directory entries as needed.
///
/// Promotion rule (matching the legacy `ensure_path_node_type` behavior):
/// - missing → write `target_node_type`
/// - Directory + target=Component → upgrade to Component
/// - anything else → no-op
pub fn ensure_path_node_type(
    txn: &mut WriteTxn,
    app_store: &AppStore,
    parent_path: StablePathRef<'_>,
    key: &StableKey,
    target_node_type: StablePathNodeType,
) -> Result<()> {
    let parent_owned: StablePath = parent_path.into();
    let existing = read_child_existence(txn, app_store, &parent_owned, key)?;
    let existing_node_type = existing.as_ref().map(|i| i.node_type);
    match (existing_node_type, target_node_type) {
        (None, _) | (Some(StablePathNodeType::Directory), StablePathNodeType::Component) => {
            write_child_existence(
                txn,
                app_store,
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
        return ensure_path_node_type(txn, app_store, parent, key, StablePathNodeType::Directory);
    }
    Ok(())
}

// --- Inspection (cross-component scans) ---

/// Scan all stable-path entries and return one path per component / directory.
pub fn list_all_stable_paths(txn: &ReadTxn, app_store: &AppStore) -> Result<Vec<StablePath>> {
    let encoded_key_prefix =
        DbEntryKey::StablePathPrefixPrefix(StablePathPrefix::default()).encode()?;
    let db = app_store.db();
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

/// Resolves the app_store by app name then spawns the stable-path iteration
/// thread. Returns `None` if the app's database doesn't exist.
pub fn spawn_iter_stable_paths_with_node_type_for_app_name(
    storage: Storage,
    app_name: &str,
) -> Result<Option<tokio::sync::mpsc::Receiver<Result<(StablePath, StablePathNodeType)>>>> {
    let app_store = storage.open_app_store_by_name(app_name)?;
    Ok(app_store.map(|app_store| spawn_iter_stable_paths_with_node_type(app_store, storage)))
}

/// Spawn a thread to stream every `(StablePath, node_type)` entry from the
/// app_store via a channel. Used by `inspect::iter_stable_paths`; the thread
/// model is needed because LMDB read transactions/cursors are `!Send`.
pub fn spawn_iter_stable_paths_with_node_type(
    app_store: AppStore,
    storage: Storage,
) -> tokio::sync::mpsc::Receiver<Result<(StablePath, StablePathNodeType)>> {
    let (tx, rx) = tokio::sync::mpsc::channel(128);

    std::thread::spawn(move || {
        let result: Result<()> = (|| {
            let encoded_key_prefix =
                DbEntryKey::StablePathPrefixPrefix(StablePathPrefix::default()).encode()?;
            let txn = storage.heed_env().read_txn()?;
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
                    other => return Err(internal_error!("Expected StablePath, got {other:?}")),
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

/// List every non-empty named app sub-app_store in the storage environment.
/// The "unnamed database" is LMDB's catalog of named sub-databases.
pub fn list_app_names(storage: &Storage) -> Result<Vec<String>> {
    let db_env = storage.heed_env();
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
