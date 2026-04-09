use crate::engine::component::ComponentProcessor;
use crate::prelude::*;

use std::borrow::Cow;
use std::cmp::{Ord, Ordering};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque, btree_map};

use heed::{RoTxn, RwTxn};

use crate::engine::context::{
    ComponentProcessingAction, ComponentProcessingMode, ComponentProcessorContext,
    DeclaredTargetState, FnCallMemo, MemoStatesPayload, TARGET_ID_KEY,
};
use crate::engine::context::{FnCallContext, FnCallMemoEntry};
use crate::engine::id_sequencer::IdReservation;
use crate::engine::logic_registry;
use crate::engine::profile::{EngineProfile, Persist};
use crate::engine::target_state::{
    ChildInvalidation, TargetActionSink, TargetHandler, TargetStateProvider,
    TargetStateProviderRegistry,
};
use crate::state::stable_path::{StableKey, StablePath, StablePathRef};
use crate::state::stable_path_set::{ChildStablePathSet, StablePathSet};
use crate::state::target_state_path::{
    TargetStatePath, TargetStatePathWithProviderId, TargetStateProviderGeneration,
};
use cocoindex_utils::deser::from_msgpack_slice;
use cocoindex_utils::fingerprint::Fingerprint;

/// Deserialize a `Vec<MemoizedValue>` into a `Vec<Prof::FunctionData>`.
fn deserialize_memo_values<Prof: EngineProfile>(
    values: &[db_schema::MemoizedValue<'_>],
) -> Result<Vec<Prof::FunctionData>> {
    values
        .iter()
        .map(|s| {
            let bytes = match s {
                db_schema::MemoizedValue::Inlined(b) => b,
            };
            Prof::FunctionData::from_bytes(bytes.as_ref())
        })
        .collect()
}

/// Serialize a `&[Prof::FunctionData]` into a `Vec<MemoizedValue<'static>>`.
/// The returned values own their bytes (`Cow::Owned`), so they're independent of
/// the input lifetime.
fn serialize_memo_values<Prof: EngineProfile>(
    values: &[Prof::FunctionData],
) -> Result<Vec<db_schema::MemoizedValue<'static>>> {
    values
        .iter()
        .map(|s| {
            let bytes = s.to_bytes()?;
            Ok(db_schema::MemoizedValue::Inlined(Cow::Owned(bytes.into())))
        })
        .collect()
}

/// Deserialize the context-borne memo states (fp-tagged list of value blobs).
fn deserialize_context_memo_states<Prof: EngineProfile>(
    entries: &[(Fingerprint, Vec<db_schema::MemoizedValue<'_>>)],
) -> Result<Vec<(Fingerprint, Vec<Prof::FunctionData>)>> {
    entries
        .iter()
        .map(|(fp, values)| Ok((*fp, deserialize_memo_values::<Prof>(values)?)))
        .collect()
}

/// Serialize the context-borne memo states into the on-disk representation.
fn serialize_context_memo_states<Prof: EngineProfile>(
    entries: &[(Fingerprint, Vec<Prof::FunctionData>)],
) -> Result<Vec<(Fingerprint, Vec<db_schema::MemoizedValue<'static>>)>> {
    entries
        .iter()
        .map(|(fp, values)| Ok((*fp, serialize_memo_values::<Prof>(values)?)))
        .collect()
}

pub(crate) async fn use_or_invalidate_component_memoization<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    processor_fp: Option<Fingerprint>,
) -> Result<Option<(Prof::FunctionData, MemoStatesPayload<Prof>)>> {
    // Short-circuit to miss under full_reprocess
    if comp_ctx.full_reprocess() {
        return Ok(None);
    }

    let key = db_schema::DbEntryKey::StablePath(
        comp_ctx.stable_path().clone(),
        db_schema::StablePathEntryKey::ComponentMemoization,
    )
    .encode()?;

    let db = comp_ctx.app_ctx().db();
    {
        let rtxn = comp_ctx.app_ctx().env().read_txn().await?;
        let Some(data) = db.get(&rtxn, key.as_slice())? else {
            return Ok(None);
        };
        if let Some(processor_fp) = processor_fp {
            let memo_info: db_schema::ComponentMemoizationInfo<'_> = from_msgpack_slice(&data)?;
            if memo_info.processor_fp == processor_fp
                && logic_registry::all_contained_with_env(
                    &memo_info.logic_deps,
                    comp_ctx.app_ctx().env(),
                )
            {
                let bytes = match memo_info.return_value {
                    db_schema::MemoizedValue::Inlined(b) => b,
                };
                let ret = Prof::FunctionData::from_bytes(bytes.as_ref());
                match ret {
                    Ok(ret) => {
                        let memo_states = deserialize_memo_values::<Prof>(&memo_info.memo_states)?;
                        let context_memo_states = deserialize_context_memo_states::<Prof>(
                            &memo_info.context_memo_states,
                        )?;
                        return Ok(Some((
                            ret,
                            MemoStatesPayload {
                                positional: memo_states,
                                by_context_fp: context_memo_states,
                            },
                        )));
                    }
                    Err(e) => {
                        warn!(
                            "Skip memoized return value because it failed in deserialization: {:?}",
                            e
                        );
                    }
                }
            }
        }
    }

    // Invalidate the memoization.
    {
        let db = comp_ctx.app_ctx().db().clone();
        comp_ctx
            .app_ctx()
            .env()
            .txn_batcher()
            .run(move |wtxn| {
                db.delete(wtxn, key.as_slice())?;
                Ok(())
            })
            .await?;
    }

    Ok(None)
}

/// Update only the memo states of an existing component memoization entry.
///
/// Used when memo state validation indicates `can_reuse=true` but states have changed
/// (e.g. mtime changed but content fingerprint is unchanged). Reads the existing entry,
/// replaces the `memo_states` / `context_memo_states` fields, and writes it back —
/// preserving `processor_fp`, `return_value`, and `logic_deps`.
pub(crate) async fn update_component_memo_states<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    new_states: &MemoStatesPayload<Prof>,
) -> Result<()> {
    let key = db_schema::DbEntryKey::StablePath(
        comp_ctx.stable_path().clone(),
        db_schema::StablePathEntryKey::ComponentMemoization,
    )
    .encode()?;

    let db = comp_ctx.app_ctx().db().clone();

    // Serialize new states
    let memo_states_serialized = serialize_memo_values::<Prof>(&new_states.positional)?;
    let context_memo_states_serialized =
        serialize_context_memo_states::<Prof>(&new_states.by_context_fp)?;

    // Read existing entry and write back with updated states in one transaction
    comp_ctx
        .app_ctx()
        .env()
        .txn_batcher()
        .run(move |wtxn| {
            let Some(data) = db.get(wtxn, key.as_slice())? else {
                return Ok(());
            };
            let existing: db_schema::ComponentMemoizationInfo<'_> = from_msgpack_slice(data)?;
            let memo_info = db_schema::ComponentMemoizationInfo {
                processor_fp: existing.processor_fp,
                return_value: existing.return_value,
                logic_deps: existing.logic_deps,
                memo_states: memo_states_serialized,
                context_memo_states: context_memo_states_serialized,
            };
            let encoded = rmp_serde::to_vec_named(&memo_info)?;
            db.put(wtxn, key.as_slice(), encoded.as_slice())?;
            Ok(())
        })
        .await?;
    Ok(())
}

fn write_fn_call_memo<Prof: EngineProfile>(
    wtxn: &mut RwTxn<'_>,
    db: &db_schema::Database,
    comp_ctx: &ComponentProcessorContext<Prof>,
    memo_fp: Fingerprint,
    memo: FnCallMemo<Prof>,
) -> Result<()> {
    let key = db_schema::DbEntryKey::StablePath(
        comp_ctx.stable_path().clone(),
        db_schema::StablePathEntryKey::FunctionMemoization(memo_fp),
    )
    .encode()?;
    let ret_bytes = memo.ret.to_bytes()?;
    let memo_states_serialized = serialize_memo_values::<Prof>(&memo.memo_states)?;
    let context_memo_states_serialized =
        serialize_context_memo_states::<Prof>(&memo.context_memo_states)?;
    let fn_call_memo = db_schema::FunctionMemoizationEntry {
        return_value: db_schema::MemoizedValue::Inlined(Cow::Borrowed(ret_bytes.as_ref())),
        child_components: vec![],
        target_state_paths: memo.target_state_paths,
        dependency_memo_entries: memo.dependency_memo_entries.into_iter().collect(),
        logic_deps: memo.logic_deps.into_iter().collect(),
        memo_states: memo_states_serialized,
        context_memo_states: context_memo_states_serialized,
    };
    let encoded = rmp_serde::to_vec_named(&fn_call_memo)?;
    db.put(wtxn, key.as_slice(), encoded.as_slice())?;
    Ok(())
}

fn read_fn_call_memo_with_txn<Prof: EngineProfile>(
    rtxn: &RoTxn,
    db: &db_schema::Database,
    comp_ctx: &ComponentProcessorContext<Prof>,
    memo_fp: Fingerprint,
) -> Result<Option<FnCallMemo<Prof>>> {
    let key = db_schema::DbEntryKey::StablePath(
        comp_ctx.stable_path().clone(),
        db_schema::StablePathEntryKey::FunctionMemoization(memo_fp),
    )
    .encode()?;

    let data = db.get(rtxn, key.as_slice())?;
    let Some(data) = data else {
        return Ok(None);
    };
    let fn_call_memo: db_schema::FunctionMemoizationEntry<'_> = from_msgpack_slice(&data)?;
    if !logic_registry::all_contained_with_env(&fn_call_memo.logic_deps, comp_ctx.app_ctx().env()) {
        return Ok(None);
    }
    if !fn_call_memo.child_components.is_empty() {
        // Legacy entry stored child component paths. Invalidate it so the function re-runs,
        // detects the child components, logs a warning, and the entry is cleaned up.
        return Ok(None);
    }
    let return_value_bytes = match fn_call_memo.return_value {
        db_schema::MemoizedValue::Inlined(b) => b,
    };
    let ret = Prof::FunctionData::from_bytes(return_value_bytes.as_ref())?;
    let memo_states = deserialize_memo_values::<Prof>(&fn_call_memo.memo_states)?;
    let context_memo_states =
        deserialize_context_memo_states::<Prof>(&fn_call_memo.context_memo_states)?;
    Ok(Some(FnCallMemo {
        ret,
        target_state_paths: fn_call_memo.target_state_paths,
        dependency_memo_entries: fn_call_memo.dependency_memo_entries.into_iter().collect(),
        logic_deps: fn_call_memo.logic_deps.into_iter().collect(),
        memo_states,
        context_memo_states,
        already_stored: true,
    }))
}

pub(crate) async fn read_fn_call_memo<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    memo_fp: Fingerprint,
) -> Result<Option<FnCallMemo<Prof>>> {
    // Short-circuit to miss under full_reprocess
    if comp_ctx.full_reprocess() {
        return Ok(None);
    }
    let rtxn = comp_ctx.app_ctx().env().read_txn().await?;
    read_fn_call_memo_with_txn(&rtxn, comp_ctx.app_ctx().db(), comp_ctx, memo_fp)
}

pub fn declare_target_state<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    fn_ctx: &FnCallContext,
    provider: TargetStateProvider<Prof>,
    key: StableKey,
    value: Prof::TargetStateValue,
) -> Result<()> {
    let target_state_path = provider.target_state_path().concat(&key);
    let declared_target_state = DeclaredTargetState {
        provider,
        item_key: key,
        value,
        child_provider: None,
    };
    comp_ctx.update_building_state(|building_state| {
        match building_state
            .target_states
            .declared_target_states
            .entry(target_state_path.clone())
        {
            btree_map::Entry::Occupied(entry) => {
                client_bail!(
                    "Target state already declared with key: {:?}",
                    entry.get().item_key
                );
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(declared_target_state);
            }
        }
        Ok(())
    })?;
    fn_ctx.update(|inner| inner.target_state_paths.push(target_state_path));
    Ok(())
}

pub fn declare_target_state_with_child<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    fn_ctx: &FnCallContext,
    provider: TargetStateProvider<Prof>,
    key: StableKey,
    value: Prof::TargetStateValue,
) -> Result<TargetStateProvider<Prof>> {
    let child_provider = comp_ctx.update_building_state(|building_state| {
        let child_provider = building_state
            .target_states
            .provider_registry
            .register_lazy(&provider, key.clone())?;
        let declared_target_state = DeclaredTargetState {
            provider,
            item_key: key,
            value,
            child_provider: Some(child_provider.clone()),
        };
        match building_state
            .target_states
            .declared_target_states
            .entry(child_provider.target_state_path().clone())
        {
            btree_map::Entry::Occupied(entry) => {
                client_bail!(
                    "Target state already declared with key: {:?}",
                    entry.get().item_key
                );
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(declared_target_state);
            }
        }
        Ok(child_provider)
    })?;
    fn_ctx.update(|inner| {
        inner
            .target_state_paths
            .push(child_provider.target_state_path().clone());
    });
    Ok(child_provider)
}

struct ChildrenPathInfo {
    path: StablePath,
    child_path_set: Option<ChildStablePathSet>,
}

struct ChildPathInfo {
    encoded_db_key: Vec<u8>,
    encoded_db_value: Vec<u8>,
    stable_key: StableKey,
    path_set: StablePathSet,
}

struct Committer<Prof: EngineProfile> {
    component_ctx: ComponentProcessorContext<Prof>,
    db: db_schema::Database,
    target_states_providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,

    component_path: StablePath,

    encoded_tombstone_key_prefix: Vec<u8>,

    existence_processing_queue: VecDeque<ChildrenPathInfo>,
    buffered_paths_for_tombstone: Vec<StablePath>,

    demote_component_only: bool,
}

impl<Prof: EngineProfile> Committer<Prof> {
    fn new(
        component_ctx: &ComponentProcessorContext<Prof>,
        target_states_providers: &rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
        demote_component_only: bool,
    ) -> Result<Self> {
        let component_path = component_ctx.stable_path().clone();
        let tombstone_key_prefix = db_schema::DbEntryKey::StablePath(
            component_path.clone(),
            db_schema::StablePathEntryKey::ChildComponentTombstonePrefix,
        );
        let encoded_tombstone_key_prefix = tombstone_key_prefix.encode()?;
        Ok(Self {
            component_ctx: component_ctx.clone(),
            db: component_ctx.app_ctx().db().clone(),
            target_states_providers: target_states_providers.clone(),
            component_path,
            encoded_tombstone_key_prefix,
            existence_processing_queue: VecDeque::new(),
            buffered_paths_for_tombstone: Vec::new(),
            demote_component_only,
        })
    }

    /// Run all DB write operations inside a provided write transaction.
    /// Returns `self` so the caller can use it for post-commit work (e.g. GC).
    fn commit_in_txn(
        mut self,
        wtxn: &mut RwTxn<'_>,
        child_path_set: Option<ChildStablePathSet>,
        encoded_target_state_info_key: &[u8],
        all_memo_fps: &HashSet<Fingerprint>,
        memos_without_mounts_to_store: Vec<(Fingerprint, FnCallMemo<Prof>)>,
        curr_version: Option<u64>,
    ) -> Result<Self> {
        {
            if self.component_ctx.mode() == ComponentProcessingMode::Delete {
                self.db
                    .delete(&mut *wtxn, encoded_target_state_info_key.as_ref())?;
            } else {
                let curr_version = curr_version
                    .ok_or_else(|| internal_error!("curr_version is required for Build mode"))?;
                let mut tracking_info: db_schema::StablePathEntryTrackingInfo = self
                    .db
                    .get(&*wtxn, encoded_target_state_info_key.as_ref())?
                    .map(|data| from_msgpack_slice(data))
                    .transpose()?
                    .ok_or_else(|| internal_error!("tracking info not found for commit"))?;

                for item in tracking_info.target_state_items.values_mut() {
                    item.states.retain(|(version, state)| {
                        *version > curr_version || *version == curr_version && !state.is_deleted()
                    });
                }
                // Prune entries with empty states and collect their paths for
                // inverted tracking cleanup (deferred until tracking_info is dropped).
                let mut pruned_paths: HashSet<TargetStatePath> = HashSet::new();
                tracking_info
                    .target_state_items
                    .retain(|path_with_pid, item| {
                        if item.states.is_empty() {
                            pruned_paths.insert(path_with_pid.target_state_path.clone());
                            false
                        } else {
                            true
                        }
                    });
                // Don't delete inverted tracking if a surviving entry shares the same
                // target_state_path (can happen when provider_id changed — old entry
                // pruned, new entry survives under different provider_id).
                if !pruned_paths.is_empty() {
                    for path_with_pid in tracking_info.target_state_items.keys() {
                        pruned_paths.remove(&path_with_pid.target_state_path);
                    }
                }
                for (path_with_pid, item) in tracking_info.target_state_items.iter_mut() {
                    if let Some(parent_provider) = self
                        .target_states_providers
                        .get(path_with_pid.target_state_path.provider_path())
                    {
                        if let Some(pg) = parent_provider.provider_generation() {
                            item.provider_schema_version = pg.provider_schema_version;
                        }
                    }
                }

                let is_version_converged =
                    tracking_info.target_state_items.iter().all(|(_, item)| {
                        item.states
                            .iter()
                            .all(|(version, _)| *version == curr_version)
                    });
                if is_version_converged {
                    tracking_info.version = 1;
                    for item in tracking_info.target_state_items.values_mut() {
                        for (version, _) in item.states.iter_mut() {
                            *version = 1;
                        }
                    }
                }

                let data_bytes = rmp_serde::to_vec_named(&tracking_info)?;
                drop(tracking_info); // Release borrow before mutable operations.
                self.db.put(
                    &mut *wtxn,
                    encoded_target_state_info_key.as_ref(),
                    data_bytes.as_slice(),
                )?;

                // Clean up inverted tracking for pruned entries.
                for path in &pruned_paths {
                    delete_target_state_owner(&mut *wtxn, &self.db, path)?;
                }
            }

            // Write memos.
            for (fp, memo) in memos_without_mounts_to_store {
                write_fn_call_memo(&mut *wtxn, &self.db, &self.component_ctx, fp, memo)?;
            }

            // Delete all function memo entries that are not in the all_memo_fps.
            {
                let fn_memo_key_prefix = db_schema::DbEntryKey::StablePath(
                    self.component_path.clone(),
                    db_schema::StablePathEntryKey::FunctionMemoizationPrefix,
                );
                let encoded_fn_memo_key_prefix = fn_memo_key_prefix.encode()?;
                let mut fn_memo_key_prefix_iter = self
                    .db
                    .prefix_iter_mut(&mut *wtxn, encoded_fn_memo_key_prefix.as_ref())?;
                while let Some((key, _)) = fn_memo_key_prefix_iter.next().transpose()? {
                    // Decode key
                    let decoded_fp: Fingerprint =
                        storekey::decode(key[encoded_fn_memo_key_prefix.len()..].as_ref())?;
                    if all_memo_fps.contains(&decoded_fp) {
                        continue;
                    }
                    unsafe {
                        fn_memo_key_prefix_iter.del_current()?;
                    }
                }
            }

            if !self.demote_component_only {
                self.update_existence(&mut *wtxn, child_path_set)?;
            }
        }

        Ok(self)
    }

    async fn commit(
        self,
        child_path_set: Option<ChildStablePathSet>,
        target_state_info_key: db_schema::DbEntryKey<'_>,
        all_memo_fps: HashSet<Fingerprint>,
        memos_without_mounts_to_store: Vec<(Fingerprint, FnCallMemo<Prof>)>,
        curr_version: Option<u64>,
    ) -> Result<()> {
        let encoded_target_state_info_key = target_state_info_key.encode()?;
        // Single cheap Arc clone so we can call txn_batcher() / db_env() after self moves into the closure.
        let app_ctx = self.component_ctx.app_ctx().clone();
        let committer = app_ctx
            .env()
            .txn_batcher()
            .run(move |wtxn| {
                self.commit_in_txn(
                    wtxn,
                    child_path_set,
                    &encoded_target_state_info_key,
                    &all_memo_fps,
                    memos_without_mounts_to_store,
                    curr_version,
                )
            })
            .await?;
        // Transaction committed — open a read txn so GC sees the committed tombstones.
        let rtxn = app_ctx.env().read_txn().await?;
        committer.launch_child_component_gc(&rtxn)
    }

    fn update_existence(
        &mut self,
        wtxn: &mut RwTxn<'_>,
        child_path_set: Option<ChildStablePathSet>,
    ) -> Result<()> {
        self.existence_processing_queue.push_back(ChildrenPathInfo {
            path: self.component_path.clone(),
            child_path_set,
        });
        while let Some(path_info) = self.existence_processing_queue.pop_front() {
            let mut children_to_add: Vec<ChildPathInfo> = Vec::new();
            {
                let mut curr_iter = path_info
                    .child_path_set
                    .into_iter()
                    .flat_map(|set| set.children.into_iter());

                let mut curr_iter_next = || -> Result<Option<ChildPathInfo>> {
                    let v = if let Some((stable_key, path_set)) = curr_iter.next() {
                        let db_key = db_schema::DbEntryKey::StablePath(
                            path_info.path.clone(),
                            db_schema::StablePathEntryKey::ChildExistence(stable_key.clone()),
                        );
                        Some(ChildPathInfo {
                            encoded_db_key: db_key.encode()?,
                            encoded_db_value: Self::encode_child_existence_info(&path_set)?,
                            stable_key,
                            path_set,
                        })
                    } else {
                        None
                    };
                    Ok(v)
                };

                let mut curr_next = curr_iter_next()?;

                let db_key_prefix = db_schema::DbEntryKey::StablePath(
                    path_info.path.clone(),
                    db_schema::StablePathEntryKey::ChildExistencePrefix,
                );
                let encoded_db_key_prefix = db_key_prefix.encode()?;
                let mut db_prefix_iter = self
                    .db
                    .prefix_iter_mut(wtxn, encoded_db_key_prefix.as_ref())?;
                let mut db_next = db_prefix_iter.next().transpose()?;

                loop {
                    let Some(db_next_entry) = db_next else {
                        // All remaining children are new.
                        curr_next.map(|v| children_to_add.push(v));
                        while let Some(entry) = curr_iter_next()? {
                            children_to_add.push(entry);
                        }
                        break;
                    };
                    let Some(curr_next_v) = &curr_next else {
                        // All remaining children should be deleted.
                        let mut db_next_entry = db_next_entry;
                        loop {
                            self.del_child(db_next_entry, &path_info.path, &encoded_db_key_prefix)?;
                            unsafe {
                                db_prefix_iter.del_current()?;
                            }
                            db_next_entry =
                                if let Some(entry) = db_prefix_iter.next().transpose()? {
                                    entry
                                } else {
                                    break;
                                };
                        }
                        break;
                    };
                    match Ord::cmp(curr_next_v.encoded_db_key.as_slice(), db_next_entry.0) {
                        Ordering::Less => {
                            // New child.
                            children_to_add.push(curr_next.ok_or_else(invariance_violation)?);
                            curr_next = curr_iter_next()?;
                        }
                        Ordering::Greater => {
                            // Child to delete.
                            self.del_child(db_next_entry, &path_info.path, &encoded_db_key_prefix)?;
                            unsafe {
                                db_prefix_iter.del_current()?;
                            }
                            db_next = db_prefix_iter.next().transpose()?;
                        }
                        Ordering::Equal => {
                            let curr_next_v = curr_next.ok_or_else(invariance_violation)?;

                            // Update the child existence info if it has changed.
                            if curr_next_v.encoded_db_value.as_slice() != db_next_entry.1 {
                                unsafe {
                                    db_prefix_iter.put_current(
                                        curr_next_v.encoded_db_key.as_slice(),
                                        curr_next_v.encoded_db_value.as_slice(),
                                    )?;
                                }
                            }

                            match curr_next_v.path_set {
                                StablePathSet::Directory(curr_dir_set) => {
                                    let db_value: db_schema::ChildExistenceInfo =
                                        from_msgpack_slice(db_next_entry.1)?;
                                    if db_value.node_type
                                        == db_schema::StablePathNodeType::Component
                                    {
                                        self.buffered_paths_for_tombstone.push(
                                            self.relative_path(path_info.path.as_ref())?
                                                .concat_part(curr_next_v.stable_key.clone()),
                                        );
                                    }
                                    self.existence_processing_queue.push_back(ChildrenPathInfo {
                                        path: path_info
                                            .path
                                            .concat_part(curr_next_v.stable_key.clone()),
                                        child_path_set: Some(curr_dir_set),
                                    });
                                }
                                StablePathSet::Component => {
                                    // No-op. Everything should be handled by the sub component.
                                }
                            }

                            curr_next = curr_iter_next()?;
                            db_next = db_prefix_iter.next().transpose()?;
                        }
                    }
                }
            }

            for child_to_add in children_to_add {
                if let StablePathSet::Directory(child_path_set) = child_to_add.path_set {
                    self.existence_processing_queue.push_back(ChildrenPathInfo {
                        path: path_info.path.concat_part(child_to_add.stable_key),
                        child_path_set: Some(child_path_set),
                    });
                }
                self.db.put(
                    wtxn,
                    child_to_add.encoded_db_key.as_slice(),
                    child_to_add.encoded_db_value.as_slice(),
                )?;
            }

            self.flush_component_tombstones(wtxn)?;
        }
        Ok(())
    }

    fn launch_child_component_gc(&self, rtxn: &RoTxn<'_>) -> Result<()> {
        let tombstone_key_prefix_iter = self
            .db
            .prefix_iter(rtxn, self.encoded_tombstone_key_prefix.as_ref())?;
        for tombstone_entry in tombstone_key_prefix_iter {
            let (ts_key, _) = tombstone_entry?;
            let relative_path: StablePath =
                storekey::decode(&ts_key[self.encoded_tombstone_key_prefix.len()..])?;
            let stable_path = self.component_path.concat(relative_path.as_ref());
            let component = self.component_ctx.component().get_child(stable_path);
            let delete_ctx = component.new_processor_context_for_delete(
                self.target_states_providers.clone(),
                Some(&self.component_ctx),
                self.component_ctx.processing_stats().clone(),
                self.component_ctx.host_ctx().clone(),
            );
            let _ = component.delete(delete_ctx, None)?;
        }
        Ok(())
    }

    fn del_child(
        &mut self,
        db_entry: (&[u8], &[u8]),
        path: &StablePath,
        encoded_db_key_prefix: &[u8],
    ) -> Result<()> {
        let (raw_db_key, raw_db_value) = db_entry;
        let stable_key: StableKey =
            storekey::decode(raw_db_key[encoded_db_key_prefix.len()..].as_ref())?;
        let db_value: db_schema::ChildExistenceInfo = from_msgpack_slice(raw_db_value)?;
        match db_value.node_type {
            db_schema::StablePathNodeType::Directory => {
                self.existence_processing_queue.push_back(ChildrenPathInfo {
                    path: path.concat_part(stable_key),
                    child_path_set: None,
                });
            }
            db_schema::StablePathNodeType::Component => {
                self.buffered_paths_for_tombstone
                    .push(self.relative_path(path.as_ref())?.concat_part(stable_key));
            }
        }
        Ok(())
    }

    fn flush_component_tombstones(&mut self, wtxn: &mut RwTxn<'_>) -> Result<()> {
        if self.buffered_paths_for_tombstone.is_empty() {
            return Ok(());
        }
        let mut encoded_tombstone_key = self.encoded_tombstone_key_prefix.clone();
        let prefix_len = encoded_tombstone_key.len();
        for stable_path in std::mem::take(&mut self.buffered_paths_for_tombstone) {
            encoded_tombstone_key.truncate(prefix_len);
            storekey::encode(&mut encoded_tombstone_key, &stable_path)?;
            self.db.put(wtxn, encoded_tombstone_key.as_slice(), &[])?;
        }
        Ok(())
    }

    fn encode_child_existence_info(path_set: &StablePathSet) -> Result<Vec<u8>> {
        let existence_info = match path_set {
            StablePathSet::Directory(_) => db_schema::ChildExistenceInfo {
                node_type: db_schema::StablePathNodeType::Directory,
            },
            StablePathSet::Component => db_schema::ChildExistenceInfo {
                node_type: db_schema::StablePathNodeType::Component,
            },
        };
        Ok(rmp_serde::to_vec_named(&existence_info)?)
    }

    fn relative_path<'p>(&self, path: StablePathRef<'p>) -> Result<StablePathRef<'p>> {
        path.strip_parent(self.component_path.as_ref())
    }
}

struct SinkInput<Prof: EngineProfile> {
    actions: Vec<Prof::TargetAction>,
    child_providers: Option<Vec<Option<TargetStateProvider<Prof>>>>,
}

impl<Prof: EngineProfile> Default for SinkInput<Prof> {
    fn default() -> Self {
        Self {
            actions: Vec::new(),
            child_providers: None,
        }
    }
}

impl<Prof: EngineProfile> SinkInput<Prof> {
    fn add_action(
        &mut self,
        action: Prof::TargetAction,
        child_provider: Option<TargetStateProvider<Prof>>,
    ) {
        self.actions.push(action);
        if let Some(child_providers) = self.child_providers.as_mut() {
            child_providers.push(child_provider);
        } else if let Some(child_provider) = child_provider {
            let mut v = Vec::with_capacity(self.actions.len());
            v.extend(std::iter::repeat(None).take(self.actions.len() - 1));
            v.push(Some(child_provider));
            self.child_providers = Some(v);
        }
    }
}

struct PreCommitOutput<Prof: EngineProfile> {
    curr_version: Option<u64>,
    previously_exists: bool,
    demote_component_only: bool,
    actions_by_sinks: HashMap<Prof::TargetActionSink, SinkInput<Prof>>,
    /// Name of the processor to be deleted; caller passes it to `collect_processor_name_name_for_del`.
    processor_name_for_del: Option<String>,
}

// --- Inverted tracking (TargetStatePath → owning component) helpers ---

fn read_target_state_owner(
    wtxn: &heed::RwTxn<'_>,
    db: &db_schema::Database,
    target_state_path: &TargetStatePath,
) -> Result<Option<db_schema::TargetStateOwnerInfo>> {
    let key = db_schema::DbEntryKey::TargetState(target_state_path.clone()).encode()?;
    Ok(db
        .get(wtxn, key.as_slice())?
        .map(|data| from_msgpack_slice(data))
        .transpose()?)
}

/// Encode an inverted tracking upsert as a deferred write (key, Some(value)).
fn encode_owner_upsert(
    target_state_path: &TargetStatePath,
    component_path: &StablePath,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let key = db_schema::DbEntryKey::TargetState(target_state_path.clone()).encode()?;
    let value = rmp_serde::to_vec_named(&db_schema::TargetStateOwnerInfo {
        component_path: component_path.clone(),
    })?;
    Ok((key, value))
}

fn delete_target_state_owner(
    wtxn: &mut heed::RwTxn<'_>,
    db: &db_schema::Database,
    target_state_path: &TargetStatePath,
) -> Result<()> {
    let key = db_schema::DbEntryKey::TargetState(target_state_path.clone()).encode()?;
    db.delete(wtxn, key.as_slice())?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn pre_commit<Prof: EngineProfile>(
    wtxn: &mut heed::RwTxn<'_>,
    db: &db_schema::Database,
    comp_mode: ComponentProcessingMode,
    stable_path: &StablePath,
    full_reprocess: bool,
    processor_name: Option<&str>,
    encoded_target_state_info_key: &[u8],
    memo_del_key: &[u8],
    contained_target_state_paths: &HashSet<TargetStatePath>,
    target_states_providers: &rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
    declared_target_states: BTreeMap<TargetStatePath, DeclaredTargetState<Prof>>,
) -> Result<Option<PreCommitOutput<Prof>>> {
    let mut actions_by_sinks = HashMap::<Prof::TargetActionSink, SinkInput<Prof>>::new();
    let mut demote_component_only = false;
    let mut processor_name_for_del: Option<String> = None;

    if comp_mode == ComponentProcessingMode::Delete {
        db.delete(wtxn, memo_del_key)?;
    }

    if let Some((parent_path, key)) = stable_path.as_ref().split_parent() {
        match comp_mode {
            ComponentProcessingMode::Build => {
                ensure_path_node_type(
                    db,
                    wtxn,
                    parent_path,
                    key,
                    db_schema::StablePathNodeType::Component,
                )?;
            }
            ComponentProcessingMode::Delete => {
                let node_type = get_path_node_type(db, wtxn, parent_path, key)?;
                match node_type {
                    Some(db_schema::StablePathNodeType::Component) => {
                        return Ok(None);
                    }
                    Some(db_schema::StablePathNodeType::Directory) => {
                        demote_component_only = true;
                    }
                    None => {}
                }
            }
        }
    }

    let mut id_reservation = IdReservation::new(&TARGET_ID_KEY);
    let mut tracking_info: Option<db_schema::StablePathEntryTrackingInfo> = db
        .get(wtxn, encoded_target_state_info_key)?
        .map(|data| from_msgpack_slice(data))
        .transpose()?;
    // Deferred DB writes that will be flushed after tracking_info is dropped,
    // since tracking_info borrows from wtxn and prevents mutable DB operations.
    // Each entry is (encoded_key, optional_encoded_value); None value means delete.
    let mut deferred_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let previously_exists = tracking_info.is_some();
    if let Some(tracking_info) = &mut tracking_info {
        if let Some(processor_name) = processor_name {
            tracking_info.processor_name = Cow::Borrowed(processor_name);
        } else {
            processor_name_for_del = Some(tracking_info.processor_name.as_ref().to_owned());
        }
    } else if let Some(processor_name) = processor_name {
        tracking_info = Some(db_schema::StablePathEntryTrackingInfo::new(Cow::Borrowed(
            processor_name,
        )));
    }
    let curr_version = if let Some(mut tracking_info) = tracking_info {
        let curr_version = tracking_info.version + 1;
        tracking_info.version = curr_version;

        // Entries to insert/re-insert into target_state_items after both phases.
        // Collected separately so Phase 2 doesn't see items added by Phase 1.
        let mut items_to_insert: Vec<(
            TargetStatePathWithProviderId,
            db_schema::TargetStateInfoItem,
        )> = Vec::new();

        // Phase 1: Insert + Update — iterate declared target states.
        // For each declared target state, find and remove any existing tracked entry,
        // then reconcile. This unifies the insert and update code paths.
        for (target_state_path, declared_target_state) in declared_target_states {
            // Look up existing tracked entry using exact key (provider_id from current providers).
            let parent_provider_gen = target_states_providers
                .get(target_state_path.provider_path())
                .and_then(|p| p.provider_generation());
            let parent_provider_id = parent_provider_gen.map(|g| g.provider_id);
            let lookup_key = TargetStatePathWithProviderId {
                target_state_path: target_state_path.clone(),
                provider_id: parent_provider_id,
            };
            let existing_item = tracking_info.target_state_items.remove(&lookup_key);

            // Whether this target state path is new to this component's forward tracking
            // (either fresh insert or preempted from another component).
            // When provider_id changed, the old entry (under old_pid) stays for Phase 2
            // to skip (stale) and commit to prune.
            let is_new_to_component = existing_item.is_none();

            // Obtain prev_item: either from this component's existing entry or via preempt.
            let mut prev_item = if let Some(existing_item) = existing_item {
                Some(existing_item)
            } else {
                // Insert path: check inverted tracking for ownership preempt.
                match read_target_state_owner(wtxn, db, &target_state_path)? {
                    Some(owner_info) if owner_info.component_path != *stable_path => {
                        let old_owner_key = db_schema::DbEntryKey::StablePath(
                            owner_info.component_path.clone(),
                            db_schema::StablePathEntryKey::TrackingInfo,
                        )
                        .encode()?;
                        if let Some(data) = db.get(wtxn, old_owner_key.as_slice())? {
                            let mut old_tracking: db_schema::StablePathEntryTrackingInfo =
                                from_msgpack_slice(data)?;
                            let len_before = old_tracking.target_state_items.len();
                            // Look up the entry matching current provider_id.
                            let prev_item = old_tracking
                                .target_state_items
                                .remove(&lookup_key)
                                .map(|mut item| {
                                    // Reset version numbers so the new component's commit
                                    // retention prunes them. The old owner's versions are from
                                    // a different version space and may collide with
                                    // curr_version.
                                    for (version, _) in item.states.iter_mut() {
                                        *version = 0;
                                    }
                                    item
                                });
                            // Also remove any stale entries (different provider_ids)
                            // to prevent them from clobbering inverted tracking on prune.
                            old_tracking
                                .target_state_items
                                .retain(|k, _| k.target_state_path != target_state_path);
                            if old_tracking.target_state_items.len() < len_before {
                                // Write back old owner's modified tracking info — deferred.
                                let old_data = rmp_serde::to_vec_named(&old_tracking)?;
                                deferred_writes.push((old_owner_key, old_data));
                            }
                            prev_item
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            };

            // Compute prev_states and prev_may_be_missing uniformly from prev_item.
            let (prev_states, prev_may_be_missing) = if let Some(ref prev_item) = prev_item {
                let schema_version_mismatch = match parent_provider_gen {
                    Some(pg) => prev_item.provider_schema_version != pg.provider_schema_version,
                    None => false,
                };
                let prev_may_be_missing = full_reprocess
                    || schema_version_mismatch
                    || prev_item.states.iter().any(|(_, s)| s.is_deleted());
                let prev_states = prev_item
                    .states
                    .iter()
                    .filter_map(|(_, s)| s.as_ref())
                    .map(|s_bytes| Prof::TargetStateTrackingRecord::from_bytes(s_bytes))
                    .collect::<Result<Vec<_>>>()?;
                (prev_states, prev_may_be_missing)
            } else {
                (vec![], true)
            };

            let target_state_key_bytes = storekey::encode_vec(&declared_target_state.item_key)
                .map_err(|e| internal_error!("Failed to encode StableKey: {e}"))?;
            let recon_output = declared_target_state
                .provider
                .handler()
                .ok_or_else(|| {
                    internal_error!(
                        "provider not ready for target state with key {:?}",
                        declared_target_state.item_key
                    )
                })?
                .reconcile(
                    declared_target_state.item_key,
                    Some(declared_target_state.value),
                    &prev_states,
                    prev_may_be_missing,
                )?;

            if let Some(recon_output) = recon_output {
                let mut provider_generation = prev_item
                    .as_ref()
                    .and_then(|item| item.provider_generation.clone());

                if let Some(child_provider) = &declared_target_state.child_provider {
                    let existing_gen = provider_generation.clone().unwrap_or_default();
                    let new_gen = match recon_output.child_invalidation {
                        Some(ChildInvalidation::Destructive) => {
                            let new_id = id_reservation.next_id(wtxn, db)?;
                            TargetStateProviderGeneration {
                                provider_id: new_id,
                                provider_schema_version: 0,
                            }
                        }
                        Some(ChildInvalidation::Lossy) => TargetStateProviderGeneration {
                            provider_id: existing_gen.provider_id,
                            provider_schema_version: existing_gen.provider_schema_version + 1,
                        },
                        None => existing_gen,
                    };
                    provider_generation = Some(new_gen.clone());
                    child_provider.set_provider_generation(new_gen)?;
                }

                actions_by_sinks
                    .entry(recon_output.sink)
                    .or_default()
                    .add_action(recon_output.action, declared_target_state.child_provider);

                let new_state_bytes = recon_output
                    .tracking_record
                    .map(|s| s.to_bytes())
                    .transpose()?;

                if let Some(item) = &mut prev_item {
                    // Update existing item.
                    item.provider_generation = provider_generation;
                    item.states.push((
                        curr_version,
                        match new_state_bytes {
                            Some(s) => {
                                db_schema::TargetStateInfoItemState::Existing(Cow::Owned(s.into()))
                            }
                            None => db_schema::TargetStateInfoItemState::Deleted,
                        },
                    ));
                } else if let Some(new_state) = new_state_bytes {
                    // Insert new item.
                    prev_item = Some(db_schema::TargetStateInfoItem {
                        key: Cow::Owned(target_state_key_bytes.into()),
                        states: vec![
                            (0, db_schema::TargetStateInfoItemState::Deleted),
                            (
                                curr_version,
                                db_schema::TargetStateInfoItemState::Existing(Cow::Owned(
                                    new_state.into(),
                                )),
                            ),
                        ],
                        provider_schema_version: 0,
                        provider_generation,
                    });
                }
            } else if let Some(item) = &mut prev_item {
                // No change — bump version on existing item.
                for (version, _) in item.states.iter_mut() {
                    *version = curr_version;
                }
            }

            // Collect item for re-insertion after Phase 2.
            if let Some(item) = prev_item {
                // Write inverted tracking for entries new to this component — deferred.
                if is_new_to_component {
                    deferred_writes.push(encode_owner_upsert(&target_state_path, stable_path)?);
                }
                items_to_insert.push((lookup_key, item));
            }
        }

        // Phase 2: Delete + Contained — iterate remaining tracked entries not matched above.
        for (target_state_path_with_pid, item) in tracking_info.target_state_items.iter_mut() {
            // Skip stale entries — commit() will prune them via version retention.
            let parent_provider_gen = target_states_providers
                .get(target_state_path_with_pid.target_state_path.provider_path())
                .and_then(|p| p.provider_generation());
            if target_state_path_with_pid.provider_id.unwrap_or(0)
                != parent_provider_gen.map(|pg| pg.provider_id).unwrap_or(0)
            {
                continue;
            }

            // Contained entries: still referenced by a parent, just bump version.
            if contained_target_state_paths.contains(&target_state_path_with_pid.target_state_path)
            {
                for (version, _) in item.states.iter_mut() {
                    *version = curr_version;
                }
                continue;
            }

            // Delete: target state is no longer declared.
            let Some(target_states_provider) = target_states_providers
                .get(target_state_path_with_pid.target_state_path.provider_path())
            else {
                trace!(
                    "skip deleting target states with path {target_state_path_with_pid} in {} because target states provider not found",
                    stable_path
                );
                continue;
            };
            let target_state_key: StableKey = storekey::decode(item.key.as_ref())?;
            let schema_version_mismatch = match parent_provider_gen {
                Some(pg) => item.provider_schema_version != pg.provider_schema_version,
                None => false,
            };
            let prev_may_be_missing = if full_reprocess || schema_version_mismatch {
                true
            } else {
                item.states.iter().any(|(_, s)| s.is_deleted())
            };
            let prev_states = item
                .states
                .iter()
                .filter_map(|(_, s)| s.as_ref())
                .map(|s_bytes| Prof::TargetStateTrackingRecord::from_bytes(s_bytes))
                .collect::<Result<Vec<_>>>()?;

            let recon_output = target_states_provider
                .handler()
                .ok_or_else(|| {
                    internal_error!(
                        "provider not ready for target state with key {target_state_key:?}"
                    )
                })?
                .reconcile(target_state_key, None, &prev_states, prev_may_be_missing)?;
            if let Some(recon_output) = recon_output {
                actions_by_sinks
                    .entry(recon_output.sink)
                    .or_default()
                    .add_action(recon_output.action, None);
                item.states.push((
                    curr_version,
                    match recon_output
                        .tracking_record
                        .map(|s| s.to_bytes())
                        .transpose()?
                    {
                        Some(s) => {
                            db_schema::TargetStateInfoItemState::Existing(Cow::Owned(s.into()))
                        }
                        None => db_schema::TargetStateInfoItemState::Deleted,
                    },
                ));
            } else {
                for (version, _) in item.states.iter_mut() {
                    *version = curr_version;
                }
            }
        }

        // Insert/re-insert items collected during Phase 1.
        for (path_with_pid, item) in items_to_insert {
            tracking_info.target_state_items.insert(path_with_pid, item);
        }

        let data_bytes = rmp_serde::to_vec_named(&tracking_info)?;
        drop(tracking_info); // Release borrow before mutable operations.
        db.put(wtxn, encoded_target_state_info_key, data_bytes.as_slice())?;
        Some(curr_version)
    } else {
        None
    };

    // Flush deferred writes now that tracking_info is dropped.
    for (key, value) in &deferred_writes {
        db.put(wtxn, key.as_slice(), value.as_slice())?;
    }

    id_reservation.commit(wtxn, db)?;
    Ok(Some(PreCommitOutput {
        curr_version,
        previously_exists,
        demote_component_only,
        actions_by_sinks,
        processor_name_for_del,
    }))
}

pub(crate) struct SubmitOutput<Prof: EngineProfile> {
    pub built_target_states_providers: Option<TargetStateProviderRegistry<Prof>>,
    pub touched_previous_states: bool,
}

#[instrument(name = "submit", skip_all)]
pub(crate) async fn submit<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    processor: Option<&Prof::ComponentProc>,
    collect_processor_name_name_for_del: impl FnOnce(&str) -> (),
) -> Result<SubmitOutput<Prof>> {
    let processor_name = processor.map(|p| p.processor_info().name.as_str());

    let mut built_target_states_providers: Option<TargetStateProviderRegistry<Prof>> = None;
    let (
        target_states_providers,
        declared_target_states,
        child_path_set,
        mut finalized_fn_call_memos,
    ) = match comp_ctx.processing_state() {
        ComponentProcessingAction::Build(build_ctx) => {
            // Extract from MutexGuard in a block so the guard is dropped before `.await`.
            let building_state = {
                let mut guard = build_ctx.state.lock().unwrap();
                let Some(state) = guard.take() else {
                    internal_bail!(
                        "Processing for the component at {} is already finished",
                        comp_ctx.stable_path()
                    );
                };
                state
            };

            let child_path_set = building_state.child_path_set;
            let finalized_fn_call_memos =
                finalize_fn_call_memoization(comp_ctx, building_state.fn_call_memos).await?;
            (
                &built_target_states_providers
                    .get_or_insert(building_state.target_states.provider_registry)
                    .providers,
                building_state.target_states.declared_target_states,
                Some(child_path_set),
                finalized_fn_call_memos,
            )
        }
        ComponentProcessingAction::Delete(delete_context) => (
            &delete_context.providers,
            Default::default(),
            None,
            Default::default(),
        ),
    };

    let db = comp_ctx.app_ctx().db().clone();
    let comp_mode = comp_ctx.mode();
    let stable_path = comp_ctx.stable_path().clone();
    let full_reprocess = comp_ctx.full_reprocess();
    let processor_name_owned: Option<String> = processor_name.map(|s| s.to_owned());

    let target_state_info_key = db_schema::DbEntryKey::StablePath(
        stable_path.clone(),
        db_schema::StablePathEntryKey::TrackingInfo,
    );
    let encoded_target_state_info_key = target_state_info_key.encode()?;
    let memo_del_key = db_schema::DbEntryKey::StablePath(
        stable_path.clone(),
        db_schema::StablePathEntryKey::ComponentMemoization,
    )
    .encode()?;
    let contained_target_state_paths =
        std::mem::take(&mut finalized_fn_call_memos.contained_target_state_paths);
    let target_states_providers_owned = target_states_providers.clone();

    // Reconcile and pre-commit target states
    let pre_commit_out = comp_ctx
        .app_ctx()
        .env()
        .txn_batcher()
        .run(move |wtxn| {
            pre_commit(
                wtxn,
                &db,
                comp_mode,
                &stable_path,
                full_reprocess,
                processor_name_owned.as_deref(),
                &encoded_target_state_info_key,
                &memo_del_key,
                &contained_target_state_paths,
                &target_states_providers_owned,
                declared_target_states,
            )
        })
        .await?;

    let Some(pre_commit_out) = pre_commit_out else {
        return Ok(SubmitOutput {
            built_target_states_providers: None,
            touched_previous_states: false,
        });
    };
    if let Some(ref name) = pre_commit_out.processor_name_for_del {
        collect_processor_name_name_for_del(name);
    }
    let curr_version = pre_commit_out.curr_version;
    let touched_previous_states = pre_commit_out.previously_exists;
    let demote_component_only = pre_commit_out.demote_component_only;
    let actions_by_sinks = pre_commit_out.actions_by_sinks;

    // Apply actions
    let host_runtime_ctx = comp_ctx.app_ctx().env().host_runtime_ctx();
    for (sink, input) in actions_by_sinks {
        let handlers = sink
            .apply(
                host_runtime_ctx,
                Arc::clone(comp_ctx.host_ctx()),
                input.actions,
            )
            .await?;
        if let Some(child_providers) = input.child_providers {
            let Some(handlers) = handlers else {
                client_bail!("expect child providers returned by Sink");
            };
            if handlers.len() != child_providers.len() {
                client_bail!(
                    "expect child providers returned by Sink to be the same length as the actions ({}), got {}",
                    child_providers.len(),
                    handlers.len(),
                );
            }
            for (child_target_state_def, child_provider) in
                std::iter::zip(handlers, child_providers)
            {
                if let Some(child_provider) = child_provider {
                    if let Some(child_target_state_def) = child_target_state_def {
                        child_provider.fulfill_handler(child_target_state_def.handler)?;
                    } else {
                        client_bail!("expect child provider returned by Sink to be fulfilled");
                    }
                }
            }
        }
    }

    let committer = Committer::new(comp_ctx, &target_states_providers, demote_component_only)?;
    committer
        .commit(
            child_path_set,
            target_state_info_key,
            finalized_fn_call_memos.all_memos_fps,
            finalized_fn_call_memos.memos_without_mounts_to_store,
            curr_version,
        )
        .await?;

    Ok(SubmitOutput {
        built_target_states_providers,
        touched_previous_states,
    })
}

#[instrument(name = "post_submit_after_ready", skip_all)]
pub(crate) async fn post_submit_for_build<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    comp_memo: Option<(
        Fingerprint,
        &'_ Prof::FunctionData,
        &'_ MemoStatesPayload<Prof>,
    )>,
) -> Result<()> {
    let Some((fp, ret, memo_states)) = comp_memo else {
        return Ok(());
    };

    // Serialize outside the closure (no transaction needed for serialization).
    let key = db_schema::DbEntryKey::StablePath(
        comp_ctx.stable_path().clone(),
        db_schema::StablePathEntryKey::ComponentMemoization,
    )
    .encode()?;
    let ret_bytes = ret.to_bytes()?;
    let memo_states_serialized = serialize_memo_values::<Prof>(&memo_states.positional)?;
    let context_memo_states_serialized =
        serialize_context_memo_states::<Prof>(&memo_states.by_context_fp)?;
    let memo_info = db_schema::ComponentMemoizationInfo {
        processor_fp: fp,
        return_value: db_schema::MemoizedValue::Inlined(Cow::Borrowed(ret_bytes.as_ref())),
        logic_deps: comp_ctx.take_logic_deps(),
        memo_states: memo_states_serialized,
        context_memo_states: context_memo_states_serialized,
    };
    let encoded = rmp_serde::to_vec_named(&memo_info)?;

    let db = comp_ctx.app_ctx().db().clone();
    comp_ctx
        .app_ctx()
        .env()
        .txn_batcher()
        .run(move |wtxn| {
            db.put(wtxn, key.as_slice(), encoded.as_slice())?;
            Ok(())
        })
        .await
}

pub(crate) async fn cleanup_tombstone<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
) -> Result<()> {
    let Some(parent) = comp_ctx.component().parent() else {
        return Ok(());
    };
    let owner_path = parent.stable_path();
    let relative_path = comp_ctx
        .stable_path()
        .as_ref()
        .strip_parent(owner_path.as_ref())?;
    let tombstone_key = db_schema::DbEntryKey::StablePath(
        owner_path.clone(),
        db_schema::StablePathEntryKey::ChildComponentTombstone(relative_path.into()),
    );
    let encoded_tombstone_key = tombstone_key.encode()?;

    let db = comp_ctx.app_ctx().db().clone();
    comp_ctx
        .app_ctx()
        .env()
        .txn_batcher()
        .run(move |wtxn| {
            db.delete(wtxn, encoded_tombstone_key.as_ref())?;
            Ok(())
        })
        .await
}

fn ensure_path_node_type(
    db: &db_schema::Database,
    wtxn: &mut RwTxn<'_>,
    parent_path: StablePathRef<'_>,
    key: &StableKey,
    target_node_type: db_schema::StablePathNodeType,
) -> Result<()> {
    let db_key = db_schema::DbEntryKey::StablePath(
        parent_path.into(),
        db_schema::StablePathEntryKey::ChildExistence(key.clone()),
    );
    let encoded_db_key = db_key.encode()?;

    let existing_node_type = get_path_node_type_with_raw_key(db, wtxn, encoded_db_key.as_slice())?;
    match (existing_node_type, target_node_type) {
        (None, _)
        | (
            Some(db_schema::StablePathNodeType::Directory),
            db_schema::StablePathNodeType::Component,
        ) => {
            let encoded_db_value = rmp_serde::to_vec_named(&db_schema::ChildExistenceInfo {
                node_type: target_node_type,
            })?;
            db.put(wtxn, encoded_db_key.as_slice(), encoded_db_value.as_slice())?;
        }
        _ => {
            // No-op for all other cases
        }
    }
    if existing_node_type.is_none()
        && let Some((parent, key)) = parent_path.split_parent()
    {
        return ensure_path_node_type(
            db,
            wtxn,
            parent,
            key,
            db_schema::StablePathNodeType::Directory,
        );
    }
    Ok(())
}

fn get_path_node_type(
    db: &db_schema::Database,
    rtxn: &RoTxn<'_>,
    parent_path: StablePathRef<'_>,
    key: &StableKey,
) -> Result<Option<db_schema::StablePathNodeType>> {
    let encoded_db_key = db_schema::DbEntryKey::StablePath(
        parent_path.into(),
        db_schema::StablePathEntryKey::ChildExistence(key.clone()),
    )
    .encode()?;
    get_path_node_type_with_raw_key(db, rtxn, encoded_db_key.as_slice())
}

fn get_path_node_type_with_raw_key(
    db: &db_schema::Database,
    rtxn: &RoTxn<'_>,
    raw_key: &[u8],
) -> Result<Option<db_schema::StablePathNodeType>> {
    let db_value = db.get(rtxn, raw_key)?;
    let Some(db_value) = db_value else {
        return Ok(None);
    };
    let child_existence_info: db_schema::ChildExistenceInfo = from_msgpack_slice(db_value)?;
    Ok(Some(child_existence_info.node_type))
}

#[derive(Default)]
struct FinalizedFnCallMemoization<Prof: EngineProfile> {
    memos_without_mounts_to_store: Vec<(Fingerprint, FnCallMemo<Prof>)>,
    // Fingerprints of all memos, including dependencies that is not populated in the current processing.
    all_memos_fps: HashSet<Fingerprint>,
    // Target state paths covered by memos but not explicitly declared in the current run, because of contained by memos that already stored, including dependency memos of already stored ones.
    // We collect them to avoid GC of these target states.
    contained_target_state_paths: HashSet<TargetStatePath>,
}

async fn finalize_fn_call_memoization<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    fn_call_memos: HashMap<Fingerprint, Arc<tokio::sync::RwLock<FnCallMemoEntry<Prof>>>>,
) -> Result<FinalizedFnCallMemoization<Prof>> {
    let mut result = FinalizedFnCallMemoization::default();

    let mut deps_to_process: VecDeque<Fingerprint> = VecDeque::new();

    // Extract memos from the in-memory map.
    for (fp, memo_lock) in fn_call_memos.iter() {
        let mut guard = memo_lock
            .try_write()
            .map_err(|_| internal_error!("fn call memo entry is locked during finalize"))?;
        let FnCallMemoEntry::Ready(Some(memo)) = std::mem::take(&mut *guard) else {
            continue;
        };

        result.all_memos_fps.insert(*fp);

        if memo.already_stored {
            result
                .contained_target_state_paths
                .extend(memo.target_state_paths.into_iter());
            deps_to_process.extend(memo.dependency_memo_entries.into_iter());
        } else {
            result.memos_without_mounts_to_store.push((*fp, memo));
        }
        // For non-stored memos, their dependencies were already resolved in this run,
        // so they exist in `fn_call_memos` and will be visited by the outer loop.
    }

    // Transitively expand deps of already-stored memos (read from DB).
    // Collect their target_state_paths so those target states are not GC'd.
    // Use a single read transaction for all DB reads.
    if !deps_to_process.is_empty() {
        let rtxn = comp_ctx.app_ctx().env().read_txn().await?;
        let db = comp_ctx.app_ctx().db();
        while let Some(fp) = deps_to_process.pop_front() {
            if !result.all_memos_fps.insert(fp) {
                continue;
            }
            let Some(memo) = read_fn_call_memo_with_txn(&rtxn, db, comp_ctx, fp)? else {
                continue;
            };
            result
                .contained_target_state_paths
                .extend(memo.target_state_paths.into_iter());
            deps_to_process.extend(memo.dependency_memo_entries.into_iter());
        }
    }
    Ok(result)
}
