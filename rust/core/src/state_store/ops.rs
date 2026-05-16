//! Typed per-entity I/O operations.
//!
//! Each function is a thin wrapper around `(key encode) + (msgpack serde) +
//! (heed get/put/delete/iter)`. Signatures are heed-free so the engine never
//! sees LMDB types. The same signatures are expected to exist verbatim in
//! the enterprise (Postgres) repo with PG-specific bodies.
//!
//! Lifetime contract: read functions return schema types parameterized by
//! the transaction lifetime, borrowing from LMDB mmap pages (or, in the PG
//! backend, from Row buffers retained by the read transaction). See
//! `specs/core/state_store_refactor.md` §3 "borrow-on-read".

use std::collections::HashSet;

use cocoindex_utils::deser::from_msgpack_slice;
use cocoindex_utils::fingerprint::Fingerprint;

use crate::prelude::*;
use crate::state::db_schema::{
    ChildExistenceInfo, ComponentMemoizationInfo, DbEntryKey, FunctionMemoizationEntry,
    IdSequencerInfo, StablePathEntryKey, StablePathNodeType, TargetStateOwnerInfo,
};
use crate::state::stable_path::{StableKey, StablePath, StablePathPrefix, StablePathRef};
use crate::state::target_state_path::TargetStatePath;
use crate::state_store::store::{AnyTxn, Database, ReadTxn, Store, WriteTxn};

// --- Key encoding helpers (internal) ---

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

fn key_tombstone(parent: &StablePath, relative_path: &StablePath) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(
        parent.clone(),
        StablePathEntryKey::ChildComponentTombstone(relative_path.clone()),
    )
    .encode()
}

fn key_target_state_owner(path: &TargetStatePath) -> Result<Vec<u8>> {
    DbEntryKey::TargetState(path.clone()).encode()
}

fn key_id_sequencer(key: &StableKey) -> Result<Vec<u8>> {
    DbEntryKey::IdSequencer(key.clone()).encode()
}

// --- Component memoization ---

pub fn read_component_memo<'a, T: AnyTxn>(
    txn: &'a T,
    store: &Store,
    path: &StablePath,
) -> Result<Option<ComponentMemoizationInfo<'a>>> {
    let key = key_component_memo(path)?;
    let data = txn.db_get_bytes(store.db(), &key)?;
    data.map(from_msgpack_slice).transpose().map_err(Into::into)
}

/// Write a pre-serialized component memo. Callers serialize externally so
/// the txn can be re-borrowed mutably after the read-modify-write pattern
/// in [`update_component_memo_states`] (engine code, not in this module).
pub fn write_component_memo_raw(
    txn: &mut WriteTxn,
    store: &Store,
    path: &StablePath,
    encoded: &[u8],
) -> Result<()> {
    let key = key_component_memo(path)?;
    store.db().put(&mut **txn, &key, encoded)?;
    Ok(())
}

pub fn delete_component_memo(txn: &mut WriteTxn, store: &Store, path: &StablePath) -> Result<()> {
    let key = key_component_memo(path)?;
    store.db().delete(&mut **txn, &key)?;
    Ok(())
}

// --- Function memoization ---

pub fn read_fn_memo<'a, T: AnyTxn>(
    txn: &'a T,
    store: &Store,
    path: &StablePath,
    fp: Fingerprint,
) -> Result<Option<FunctionMemoizationEntry<'a>>> {
    let key = key_fn_memo(path, fp)?;
    let data = txn.db_get_bytes(store.db(), &key)?;
    data.map(from_msgpack_slice).transpose().map_err(Into::into)
}

pub fn write_fn_memo(
    txn: &mut WriteTxn,
    store: &Store,
    path: &StablePath,
    fp: Fingerprint,
    entry: &FunctionMemoizationEntry<'_>,
) -> Result<()> {
    let key = key_fn_memo(path, fp)?;
    let value = rmp_serde::to_vec_named(entry)?;
    store.db().put(&mut **txn, &key, &value)?;
    Ok(())
}

/// GC: delete all function memos for `path` whose fingerprint is NOT in `keep`.
pub fn retain_fn_memos(
    txn: &mut WriteTxn,
    store: &Store,
    path: &StablePath,
    keep: &HashSet<Fingerprint>,
) -> Result<()> {
    let prefix = key_fn_memo_prefix(path)?;
    let db = store.db();
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

// --- Child existence (internal — public ops compose these) ---

fn read_child_existence<T: AnyTxn>(
    txn: &T,
    store: &Store,
    parent: &StablePath,
    child_key: &StableKey,
) -> Result<Option<ChildExistenceInfo>> {
    let key = key_child_existence(parent, child_key)?;
    let data = txn.db_get_bytes(store.db(), &key)?;
    data.map(from_msgpack_slice).transpose().map_err(Into::into)
}

fn write_child_existence(
    txn: &mut WriteTxn,
    store: &Store,
    parent: &StablePath,
    child_key: &StableKey,
    info: &ChildExistenceInfo,
) -> Result<()> {
    let key = key_child_existence(parent, child_key)?;
    let value = rmp_serde::to_vec_named(info)?;
    store.db().put(&mut **txn, &key, &value)?;
    Ok(())
}

fn delete_child_existence(
    txn: &mut WriteTxn,
    store: &Store,
    parent: &StablePath,
    child_key: &StableKey,
) -> Result<()> {
    let key = key_child_existence(parent, child_key)?;
    store.db().delete(&mut **txn, &key)?;
    Ok(())
}

// --- Tombstones ---

fn write_tombstone(
    txn: &mut WriteTxn,
    store: &Store,
    parent: &StablePath,
    relative_path: &StablePath,
) -> Result<()> {
    let key = key_tombstone(parent, relative_path)?;
    store.db().put(&mut **txn, &key, &[])?;
    Ok(())
}

pub fn delete_tombstone(
    txn: &mut WriteTxn,
    store: &Store,
    parent: &StablePath,
    relative_path: &StablePath,
) -> Result<()> {
    let key = key_tombstone(parent, relative_path)?;
    store.db().delete(&mut **txn, &key)?;
    Ok(())
}

/// Atomic existence-removal + tombstone-write, matching the contract of
/// `LiveComponentController::delete`'s synchronous step.
pub fn remove_child_with_tombstone(
    txn: &mut WriteTxn,
    store: &Store,
    parent: &StablePath,
    child_key: &StableKey,
    owner_path: &StablePath,
    relative_child: &StablePath,
) -> Result<()> {
    delete_child_existence(txn, store, parent, child_key)?;
    write_tombstone(txn, store, owner_path, relative_child)?;
    Ok(())
}

// --- Inverted target-state owner index ---

pub fn read_target_state_owner<T: AnyTxn>(
    txn: &T,
    store: &Store,
    path: &TargetStatePath,
) -> Result<Option<TargetStateOwnerInfo>> {
    let key = key_target_state_owner(path)?;
    let data = txn.db_get_bytes(store.db(), &key)?;
    data.map(from_msgpack_slice).transpose().map_err(Into::into)
}

pub fn delete_target_state_owner(
    txn: &mut WriteTxn,
    store: &Store,
    path: &TargetStatePath,
) -> Result<()> {
    let key = key_target_state_owner(path)?;
    store.db().delete(&mut **txn, &key)?;
    Ok(())
}

// --- ID sequencer ---

pub fn peek_id_sequence<T: AnyTxn>(txn: &T, store: &Store, key: &StableKey) -> Result<Option<u64>> {
    let db_key = key_id_sequencer(key)?;
    let data = txn.db_get_bytes(store.db(), &db_key)?;
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
    store: &Store,
    key: &StableKey,
    next_id: u64,
) -> Result<()> {
    let db_key = key_id_sequencer(key)?;
    let info = IdSequencerInfo { next_id };
    let value = rmp_serde::to_vec_named(&info)?;
    store.db().put(&mut **txn, &db_key, &value)?;
    Ok(())
}

/// Atomically reserve `count` consecutive IDs starting from the next
/// available ID. Returns the first reserved ID. IDs start at 1 (0 is reserved).
pub fn reserve_id_range(
    txn: &mut WriteTxn,
    store: &Store,
    key: &StableKey,
    count: u64,
) -> Result<u64> {
    let current_next_id = peek_id_sequence(txn, store, key)?.unwrap_or(1);
    write_id_sequence(txn, store, key, current_next_id + count)?;
    Ok(current_next_id)
}

// --- App-level ---

pub fn clear_all(txn: &mut WriteTxn, store: &Store) -> Result<()> {
    store.db().clear(&mut **txn)?;
    Ok(())
}

// --- Path node type helpers (LMDB-encoded via ChildExistence on parent) ---

/// Looks up the node type of `parent_path/key` by reading the parent's
/// child-existence entry. Used by `pre_commit` path-existence checks.
pub fn read_path_node_type<T: AnyTxn>(
    txn: &T,
    store: &Store,
    parent_path: StablePathRef<'_>,
    key: &StableKey,
) -> Result<Option<StablePathNodeType>> {
    let parent_owned: StablePath = parent_path.into();
    let info = read_child_existence(txn, store, &parent_owned, key)?;
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
    store: &Store,
    parent_path: StablePathRef<'_>,
    key: &StableKey,
    target_node_type: StablePathNodeType,
) -> Result<()> {
    let parent_owned: StablePath = parent_path.into();
    let existing = read_child_existence(txn, store, &parent_owned, key)?;
    let existing_node_type = existing.as_ref().map(|i| i.node_type);
    match (existing_node_type, target_node_type) {
        (None, _) | (Some(StablePathNodeType::Directory), StablePathNodeType::Component) => {
            write_child_existence(
                txn,
                store,
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
        return ensure_path_node_type(txn, store, parent, key, StablePathNodeType::Directory);
    }
    Ok(())
}

// --- Inspection (cross-component scans) ---

/// Scan all stable-path entries and return one path per component / directory.
///
/// On Postgres this becomes a partition-wise scan across all hash partitions
/// — acceptable cost for an inspection tool, see `specs/core/internal_states.md`.
pub fn list_all_stable_paths(txn: &ReadTxn, store: &Store) -> Result<Vec<StablePath>> {
    let encoded_key_prefix =
        DbEntryKey::StablePathPrefixPrefix(StablePathPrefix::default()).encode()?;
    let db = store.db();
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

/// Open a `ReadTxn` for inspection ops that don't go through the engine's
/// retry-on-`MDB_READERS_FULL` path. Inspection tools are best-effort and
/// callers can retry at a higher level if a read txn fails to open.
pub fn open_read_txn_for_inspect(db_env: &heed::Env<heed::WithoutTls>) -> Result<ReadTxn<'_>> {
    let rtxn = db_env.read_txn()?;
    Ok(ReadTxn::new(rtxn))
}

/// Resolves the store by app name then spawns the stable-path iteration
/// thread. Returns `None` if the app's database doesn't exist.
pub fn spawn_iter_stable_paths_with_node_type_for_app_name(
    db_env: heed::Env<heed::WithoutTls>,
    app_name: &str,
) -> Result<Option<tokio::sync::mpsc::Receiver<Result<(StablePath, StablePathNodeType)>>>> {
    let rtxn = db_env.read_txn()?;
    let store = open_app_store_by_name(&db_env, &rtxn, app_name)?;
    drop(rtxn);
    Ok(store.map(|store| spawn_iter_stable_paths_with_node_type(store, db_env)))
}

/// Spawn a thread to stream every `(StablePath, node_type)` entry from the
/// store via a channel. Used by `inspect::iter_stable_paths`; the thread
/// model is needed because LMDB read transactions/cursors are `!Send`.
pub fn spawn_iter_stable_paths_with_node_type(
    store: Store,
    db_env: heed::Env<heed::WithoutTls>,
) -> tokio::sync::mpsc::Receiver<Result<(StablePath, StablePathNodeType)>> {
    let (tx, rx) = tokio::sync::mpsc::channel(128);

    std::thread::spawn(move || {
        let result: Result<()> = (|| {
            let encoded_key_prefix =
                DbEntryKey::StablePathPrefixPrefix(StablePathPrefix::default()).encode()?;
            let txn = db_env.read_txn()?;
            let db = store.db();

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

/// Open a logical app sub-database within the LMDB environment by name,
/// returning `None` if it doesn't exist. Used by
/// `spawn_iter_stable_paths_with_node_type_for_app_name`.
fn open_app_store_by_name(
    db_env: &heed::Env<heed::WithoutTls>,
    rtxn: &heed::RoTxn<'_, heed::WithoutTls>,
    app_name: &str,
) -> Result<Option<Store>> {
    let db: Option<Database> = db_env.open_database(rtxn, Some(app_name))?;
    Ok(db.map(Store::new))
}

/// List every non-empty named app database in the environment. LMDB-specific
/// — the unnamed database is its catalog of named sub-databases.
pub fn list_app_names(db_env: &heed::Env<heed::WithoutTls>) -> Result<Vec<String>> {
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
