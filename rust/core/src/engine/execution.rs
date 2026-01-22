use crate::engine::component::ComponentProcessor;
use crate::prelude::*;

use std::borrow::Cow;
use std::cmp::{Ord, Ordering};
use std::collections::{HashMap, HashSet, VecDeque, btree_map};

use heed::{RoTxn, RwTxn};

use crate::engine::context::{
    ComponentProcessingAction, ComponentProcessingMode, ComponentProcessorContext, DeclaredEffect,
    FnCallMemo,
};
use crate::engine::context::{FnCallContext, FnCallMemoEntry};
use crate::engine::profile::{EngineProfile, Persist, StableFingerprint};
use crate::engine::target_state::{
    TargetActionSink, TargetHandler, TargetStateProvider, TargetStateProviderRegistry,
};
use crate::state::stable_path::{StableKey, StablePath, StablePathRef};
use crate::state::stable_path_set::{ChildStablePathSet, StablePathSet};
use crate::state::target_state_path::TargetStatePath;
use cocoindex_utils::deser::from_msgpack_slice;
use cocoindex_utils::fingerprint::Fingerprint;

pub(crate) fn use_or_invalidate_component_memoization<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    processor_fp: Option<Fingerprint>,
) -> Result<Option<Prof::FunctionData>> {
    let key = db_schema::DbEntryKey::StablePath(
        comp_ctx.stable_path().clone(),
        db_schema::StablePathEntryKey::ComponentMemoization,
    )
    .encode()?;

    let db_env = comp_ctx.app_ctx().env().db_env();
    let db = comp_ctx.app_ctx().db();
    {
        let rtxn = db_env.read_txn()?;
        let Some(data) = db.get(&rtxn, key.as_slice())? else {
            return Ok(None);
        };
        if let Some(processor_fp) = processor_fp {
            let memo_info: db_schema::ComponentMemoizationInfo<'_> = from_msgpack_slice(&data)?;
            if memo_info.processor_fp == processor_fp {
                let bytes = match memo_info.return_value {
                    db_schema::MemoizedValue::Inlined(b) => b,
                };
                let ret = Prof::FunctionData::from_bytes(bytes.as_ref());
                match ret {
                    Ok(ret) => return Ok(Some(ret)),
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
        let mut wtxn = db_env.write_txn()?;
        db.delete(&mut wtxn, key.as_slice())?;
        wtxn.commit()?;
    }

    Ok(None)
}

fn delete_component_memoization<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    wtxn: &mut RwTxn<'_>,
) -> Result<()> {
    let db = comp_ctx.app_ctx().db();
    let key = db_schema::DbEntryKey::StablePath(
        comp_ctx.stable_path().clone(),
        db_schema::StablePathEntryKey::ComponentMemoization,
    )
    .encode()?;
    db.delete(wtxn, key.as_slice())?;
    Ok(())
}

fn write_component_memoization<Prof: EngineProfile>(
    wtxn: &mut RwTxn<'_>,
    db: &db_schema::Database,
    comp_ctx: &ComponentProcessorContext<Prof>,
    processor_fp: Fingerprint,
    return_value: &Prof::FunctionData,
) -> Result<()> {
    let key = db_schema::DbEntryKey::StablePath(
        comp_ctx.stable_path().clone(),
        db_schema::StablePathEntryKey::ComponentMemoization,
    )
    .encode()?;

    let bytes = return_value.to_bytes()?;
    let memo_info = db_schema::ComponentMemoizationInfo {
        processor_fp,
        return_value: db_schema::MemoizedValue::Inlined(Cow::Borrowed(bytes.as_ref())),
    };
    let encoded = rmp_serde::to_vec_named(&memo_info)?;
    db.put(wtxn, key.as_slice(), encoded.as_slice())?;
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
    let fn_call_memo = db_schema::FunctionMemoizationEntry {
        return_value: db_schema::MemoizedValue::Inlined(Cow::Borrowed(ret_bytes.as_ref())),
        child_components: memo.child_components,
        target_state_paths: memo.target_state_paths,
        dependency_memo_entries: memo.dependency_memo_entries.into_iter().collect(),
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
    let return_value_bytes = match fn_call_memo.return_value {
        db_schema::MemoizedValue::Inlined(b) => b,
    };
    let ret = Prof::FunctionData::from_bytes(return_value_bytes.as_ref())?;
    Ok(Some(FnCallMemo {
        ret,
        child_components: fn_call_memo.child_components,
        target_state_paths: fn_call_memo.target_state_paths,
        dependency_memo_entries: fn_call_memo.dependency_memo_entries.into_iter().collect(),
        already_stored: true,
    }))
}

pub(crate) fn read_fn_call_memo<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    memo_fp: Fingerprint,
) -> Result<Option<FnCallMemo<Prof>>> {
    let db_env = comp_ctx.app_ctx().env().db_env();
    let rtxn = db_env.read_txn()?;
    read_fn_call_memo_with_txn(&rtxn, comp_ctx.app_ctx().db(), comp_ctx, memo_fp)
}

pub fn declare_target_state<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    fn_ctx: &FnCallContext,
    provider: TargetStateProvider<Prof>,
    key: Prof::TargetStateKey,
    value: Prof::TargetStateValue,
) -> Result<()> {
    let target_state_path = make_target_state_path(&provider, &key);
    let declared_effect = DeclaredEffect {
        provider,
        key,
        value,
        child_provider: None,
    };
    comp_ctx.update_building_state(|building_state| {
        match building_state
            .target_states
            .declared_effects
            .entry(target_state_path.clone())
        {
            btree_map::Entry::Occupied(entry) => {
                client_bail!(
                    "Target state already declared with key: {:?}",
                    entry.get().key
                );
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(declared_effect);
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
    key: Prof::TargetStateKey,
    value: Prof::TargetStateValue,
) -> Result<TargetStateProvider<Prof>> {
    let target_state_path = make_target_state_path(&provider, &key);
    let child_provider = comp_ctx.update_building_state(|building_state| {
        let child_provider = building_state
            .target_states
            .provider_registry
            .register_lazy(target_state_path.clone())?;
        let declared_effect = DeclaredEffect {
            provider,
            key,
            value,
            child_provider: Some(child_provider.clone()),
        };
        match building_state
            .target_states
            .declared_effects
            .entry(target_state_path.clone())
        {
            btree_map::Entry::Occupied(entry) => {
                client_bail!(
                    "Target state already declared with key: {:?}",
                    entry.get().key
                );
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(declared_effect);
            }
        }
        Ok(child_provider)
    })?;
    fn_ctx.update(|inner| {
        inner.target_state_paths.push(target_state_path);
    });
    Ok(child_provider)
}

fn make_target_state_path<Prof: EngineProfile>(
    provider: &TargetStateProvider<Prof>,
    key: &Prof::TargetStateKey,
) -> TargetStatePath {
    let fp = key.stable_fingerprint();
    provider.target_state_path().concat(fp)
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

struct Committer<'a, Prof: EngineProfile> {
    component_ctx: &'a ComponentProcessorContext<Prof>,
    db: &'a db_schema::Database,
    target_states_providers: &'a rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,

    component_path: &'a StablePath,

    encoded_tombstone_key_prefix: Vec<u8>,

    existence_processing_queue: VecDeque<ChildrenPathInfo>,
    buffered_paths_for_tombstone: Vec<StablePath>,

    demote_component_only: bool,
}

impl<'a, Prof: EngineProfile> Committer<'a, Prof> {
    fn new(
        component_ctx: &'a ComponentProcessorContext<Prof>,
        target_states_providers: &'a rpds::HashTrieMapSync<
            TargetStatePath,
            TargetStateProvider<Prof>,
        >,
        demote_component_only: bool,
    ) -> Result<Self> {
        let component_path = component_ctx.stable_path();
        let tombstone_key_prefix = db_schema::DbEntryKey::StablePath(
            component_path.clone(),
            db_schema::StablePathEntryKey::ChildComponentTombstonePrefix,
        );
        let encoded_tombstone_key_prefix = tombstone_key_prefix.encode()?;
        Ok(Self {
            component_ctx,
            db: component_ctx.app_ctx().db(),
            target_states_providers,
            component_path,
            encoded_tombstone_key_prefix,
            existence_processing_queue: VecDeque::new(),
            buffered_paths_for_tombstone: Vec::new(),
            demote_component_only,
        })
    }

    fn commit(
        &mut self,
        child_path_set: Option<ChildStablePathSet>,
        effect_info_key: &db_schema::DbEntryKey,
        all_memo_fps: &HashSet<Fingerprint>,
        memos_without_mounts_to_store: Vec<(Fingerprint, FnCallMemo<Prof>)>,
        curr_version: Option<u64>,
    ) -> Result<()> {
        let encoded_effect_info_key = effect_info_key.encode()?;
        let db_env = self.component_ctx.app_ctx().env().db_env();
        {
            let mut wtxn = db_env.write_txn()?;
            if self.component_ctx.mode() == ComponentProcessingMode::Delete {
                self.db
                    .delete(&mut wtxn, encoded_effect_info_key.as_ref())?;
            } else {
                let curr_version = curr_version
                    .ok_or_else(|| internal_error!("curr_version is required for Build mode"))?;
                let mut tracking_info: db_schema::StablePathEntryTrackingInfo = self
                    .db
                    .get(&wtxn, encoded_effect_info_key.as_ref())?
                    .map(|data| from_msgpack_slice(&data))
                    .transpose()?
                    .ok_or_else(|| internal_error!("tracking info not found for commit"))?;

                for item in tracking_info.effect_items.values_mut() {
                    item.states.retain(|(version, state)| {
                        *version > curr_version || *version == curr_version && !state.is_deleted()
                    });
                }
                tracking_info
                    .effect_items
                    .retain(|_, item| !item.states.is_empty());
                let is_version_converged = tracking_info.effect_items.iter().all(|(_, item)| {
                    item.states
                        .iter()
                        .all(|(version, _)| *version == curr_version)
                });
                if is_version_converged {
                    tracking_info.version = 1;
                    for item in tracking_info.effect_items.values_mut() {
                        for (version, _) in item.states.iter_mut() {
                            *version = 1;
                        }
                    }
                }

                let data_bytes = rmp_serde::to_vec_named(&tracking_info)?;
                self.db.put(
                    &mut wtxn,
                    encoded_effect_info_key.as_ref(),
                    data_bytes.as_slice(),
                )?;
            }

            // Write memos.
            for (fp, memo) in memos_without_mounts_to_store {
                write_fn_call_memo(&mut wtxn, self.db, self.component_ctx, fp, memo)?;
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
                    .prefix_iter_mut(&mut wtxn, encoded_fn_memo_key_prefix.as_ref())?;
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
                self.update_existence(&mut wtxn, child_path_set)?;
            }
            wtxn.commit()?;
        }

        {
            let rtxn = db_env.read_txn()?;
            self.launch_child_component_gc(&rtxn)?;
        }

        Ok(())
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
            );
            let _ = component.delete(delete_ctx)?;
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

pub(crate) struct SubmitOutput<Prof: EngineProfile> {
    pub built_target_states_providers: Option<TargetStateProviderRegistry<Prof>>,
    pub memos_with_mounts_to_store: Vec<(Fingerprint, FnCallMemo<Prof>)>,
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
    let mut memos_with_mounts_to_store: Vec<(Fingerprint, FnCallMemo<Prof>)> = Vec::new();
    let (target_states_providers, declared_effects, child_path_set, finalized_fn_call_memos) =
        match comp_ctx.processing_state() {
            ComponentProcessingAction::Build(building_state) => {
                let mut building_state = building_state.lock().unwrap();
                let Some(building_state) = building_state.take() else {
                    internal_bail!(
                        "Processing for the component at {} is already finished",
                        comp_ctx.stable_path()
                    );
                };

                let mut child_path_set = building_state.child_path_set;
                let finalized_fn_call_memos = finalize_fn_call_memoization(
                    comp_ctx,
                    building_state.fn_call_memos,
                    &mut memos_with_mounts_to_store,
                    &mut child_path_set,
                )?;
                (
                    &built_target_states_providers
                        .get_or_insert(building_state.target_states.provider_registry)
                        .providers,
                    building_state.target_states.declared_effects,
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

    let db_env = comp_ctx.app_ctx().env().db_env();
    let db = comp_ctx.app_ctx().db();

    let effect_info_key = db_schema::DbEntryKey::StablePath(
        comp_ctx.stable_path().clone(),
        db_schema::StablePathEntryKey::TrackingInfo,
    );

    let mut actions_by_sinks = HashMap::<Prof::TargetActionSink, SinkInput<Prof>>::new();
    let mut demote_component_only = false;

    // Reconcile and pre-commit target states
    let (curr_version, touched_previous_states) = {
        let mut wtxn = db_env.write_txn()?;

        if comp_ctx.mode() == ComponentProcessingMode::Delete {
            delete_component_memoization(comp_ctx, &mut wtxn)?;
        }

        if let Some((parent_path, key)) = comp_ctx.stable_path().as_ref().split_parent() {
            match comp_ctx.mode() {
                ComponentProcessingMode::Build => {
                    ensure_path_node_type(
                        db,
                        &mut wtxn,
                        parent_path,
                        key,
                        db_schema::StablePathNodeType::Component,
                    )?;
                }
                ComponentProcessingMode::Delete => {
                    let node_type = get_path_node_type(db, &wtxn, parent_path, key)?;
                    match node_type {
                        Some(db_schema::StablePathNodeType::Component) => {
                            return Ok(SubmitOutput {
                                built_target_states_providers: None,
                                memos_with_mounts_to_store,
                                touched_previous_states: false,
                            });
                        }
                        Some(db_schema::StablePathNodeType::Directory) => {
                            demote_component_only = true;
                        }
                        None => {}
                    }
                }
            }
        }

        let mut tracking_info: Option<db_schema::StablePathEntryTrackingInfo> = db
            .get(&wtxn, effect_info_key.encode()?.as_ref())?
            .map(|data| from_msgpack_slice(&data))
            .transpose()?;
        let previously_exists = tracking_info.is_some();
        if let Some(tracking_info) = &mut tracking_info {
            if let Some(processor_name) = processor_name {
                tracking_info.processor_name = Cow::Borrowed(processor_name);
            } else {
                collect_processor_name_name_for_del(tracking_info.processor_name.as_ref());
            }
        } else if let Some(processor_name) = processor_name {
            tracking_info = Some(db_schema::StablePathEntryTrackingInfo::new(Cow::Borrowed(
                processor_name,
            )));
        }
        let curr_version = if let Some(mut tracking_info) = tracking_info {
            let curr_version = tracking_info.version + 1;
            tracking_info.version = curr_version;
            let mut declared_target_states_to_process = declared_effects;

            // Deal with existing target states
            for (target_state_path, item) in tracking_info.effect_items.iter_mut() {
                let prev_may_be_missing = item.states.iter().any(|(_, s)| s.is_deleted());
                let prev_states = item
                    .states
                    .iter()
                    .filter_map(|(_, s)| s.as_ref())
                    .map(|s_bytes| Prof::TargetStateTrackingRecord::from_bytes(s_bytes))
                    .collect::<Result<Vec<_>>>()?;

                let declared_target_state =
                    declared_target_states_to_process.remove(target_state_path);
                let (target_states_provider, effect_key, declared_decl, child_provider) =
                    match declared_target_state {
                        Some(declared_effect) => (
                            Cow::Owned(declared_effect.provider),
                            declared_effect.key,
                            Some(declared_effect.value),
                            declared_effect.child_provider,
                        ),
                        None => {
                            if finalized_fn_call_memos
                                .contained_target_state_paths
                                .contains(target_state_path)
                            {
                                for (version, _) in item.states.iter_mut() {
                                    *version = curr_version;
                                }
                                continue;
                            }
                            let Some(target_states_provider) =
                                target_states_providers.get(target_state_path.provider_path())
                            else {
                                // TODO: Verify the parent is gone.
                                trace!(
                                    "skip deleting target states with path {target_state_path} in {} because target states provider not found",
                                    comp_ctx.stable_path()
                                );
                                continue;
                            };
                            let effect_key = Prof::TargetStateKey::from_bytes(item.key.as_ref())?;
                            (
                                Cow::Borrowed(target_states_provider),
                                effect_key,
                                None,
                                None,
                            )
                        }
                    };
                let recon_output = target_states_provider
                    .handler()
                    .ok_or_else(|| {
                        internal_error!(
                            "provider not ready for target state with key {effect_key:?}"
                        )
                    })?
                    .reconcile(effect_key, declared_decl, &prev_states, prev_may_be_missing)?;
                if let Some(recon_output) = recon_output {
                    actions_by_sinks
                        .entry(recon_output.sink)
                        .or_default()
                        .add_action(recon_output.action, child_provider);
                    item.states.push((
                        curr_version,
                        match recon_output
                            .tracking_record
                            .map(|s| s.to_bytes())
                            .transpose()?
                        {
                            Some(s) => {
                                db_schema::EffectInfoItemState::Existing(Cow::Owned(s.into()))
                            }
                            None => db_schema::EffectInfoItemState::Deleted,
                        },
                    ));
                } else {
                    for (version, _) in item.states.iter_mut() {
                        *version = curr_version;
                    }
                }
            }

            // Deal with new target states
            for (target_state_path, target_state) in declared_target_states_to_process {
                let effect_key_bytes = target_state.key.to_bytes()?;
                let Some(recon_output) = target_state
                    .provider
                    .handler()
                    .ok_or_else(|| {
                        internal_error!(
                            "provider not ready for target state with key {:?}",
                            target_state.key
                        )
                    })?
                    .reconcile(
                        target_state.key,
                        Some(target_state.value),
                        /*&prev_states=*/ &[],
                        /*prev_may_be_missing=*/ true,
                    )?
                else {
                    continue;
                };
                actions_by_sinks
                    .entry(recon_output.sink)
                    .or_default()
                    .add_action(recon_output.action, target_state.child_provider);
                let Some(new_state) = recon_output
                    .tracking_record
                    .map(|s| s.to_bytes())
                    .transpose()?
                    .map(|s| Cow::Owned(s.into()))
                else {
                    continue;
                };
                let item = db_schema::EffectInfoItem {
                    key: Cow::Owned(effect_key_bytes.into()),
                    states: vec![
                        (0, db_schema::EffectInfoItemState::Deleted),
                        (
                            curr_version,
                            db_schema::EffectInfoItemState::Existing(new_state),
                        ),
                    ],
                };
                tracking_info.effect_items.insert(target_state_path, item);
            }

            let data_bytes = rmp_serde::to_vec_named(&tracking_info)?;
            db.put(
                &mut wtxn,
                effect_info_key.encode()?.as_ref(),
                data_bytes.as_slice(),
            )?;
            Some(curr_version)
        } else {
            None
        };

        wtxn.commit()?;
        (curr_version, previously_exists)
    };

    // Apply actions
    let host_runtime_ctx = comp_ctx.app_ctx().env().host_runtime_ctx();
    for (sink, input) in actions_by_sinks {
        let handlers = sink.apply(host_runtime_ctx, input.actions).await?;
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
            for (child_effect_def, child_provider) in std::iter::zip(handlers, child_providers) {
                if let Some(child_provider) = child_provider {
                    if let Some(child_effect_def) = child_effect_def {
                        child_provider.fulfill_handler(child_effect_def.handler)?;
                    } else {
                        client_bail!("expect child provider returned by Sink to be fulfilled");
                    }
                }
            }
        }
    }

    let mut committer = Committer::new(comp_ctx, &target_states_providers, demote_component_only)?;
    committer.commit(
        child_path_set,
        &effect_info_key,
        &finalized_fn_call_memos.all_memos_fps,
        finalized_fn_call_memos.memos_without_mounts_to_store,
        curr_version,
    )?;

    Ok(SubmitOutput {
        built_target_states_providers,
        memos_with_mounts_to_store,
        touched_previous_states,
    })
}

#[instrument(name = "post_submit_after_ready", skip_all)]
pub(crate) async fn post_submit_for_build<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    comp_memo: Option<(Fingerprint, &'_ Prof::FunctionData)>,
    memos_with_mounts_to_store: Vec<(Fingerprint, FnCallMemo<Prof>)>,
) -> Result<()> {
    if comp_memo.is_none() && memos_with_mounts_to_store.is_empty() {
        return Ok(());
    }
    let db_env = comp_ctx.app_ctx().env().db_env();
    let mut wtxn = db_env.write_txn()?;
    let db = comp_ctx.app_ctx().db();

    if let Some((fp, ret)) = comp_memo {
        write_component_memoization(&mut wtxn, db, comp_ctx, fp, &ret)?;
    }
    for (fp, memo) in memos_with_mounts_to_store {
        write_fn_call_memo(&mut wtxn, db, comp_ctx, fp, memo)?;
    }
    wtxn.commit()?;
    Ok(())
}

pub(crate) fn cleanup_tombstone<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
) -> Result<()> {
    let Some(parent_ctx) = comp_ctx.parent_context() else {
        return Ok(());
    };
    let parent_path = parent_ctx.stable_path();
    let relative_path = comp_ctx
        .stable_path()
        .as_ref()
        .strip_parent(parent_path.as_ref())?;
    let tombstone_key = db_schema::DbEntryKey::StablePath(
        parent_path.clone(),
        db_schema::StablePathEntryKey::ChildComponentTombstone(relative_path.into()),
    );
    let encoded_tombstone_key = tombstone_key.encode()?;

    let db_env = comp_ctx.app_ctx().env().db_env();
    let db = comp_ctx.app_ctx().db();
    {
        let mut wtxn = db_env.write_txn()?;
        db.delete(&mut wtxn, encoded_tombstone_key.as_ref())?;
        wtxn.commit()?;
    }
    Ok(())
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

fn finalize_fn_call_memoization<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    fn_call_memos: HashMap<Fingerprint, Arc<tokio::sync::RwLock<FnCallMemoEntry<Prof>>>>,
    memos_with_mounts_to_store: &mut Vec<(Fingerprint, FnCallMemo<Prof>)>,
    stable_path_set: &mut ChildStablePathSet,
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
            for child_component in &memo.child_components {
                stable_path_set.add_child(child_component.as_ref(), StablePathSet::Component)?;
            }
            result
                .contained_target_state_paths
                .extend(memo.target_state_paths.into_iter());
            deps_to_process.extend(memo.dependency_memo_entries.into_iter());
        } else if memo.child_components.is_empty() {
            result.memos_without_mounts_to_store.push((*fp, memo));
        } else {
            memos_with_mounts_to_store.push((*fp, memo));
        }
        // For non-stored memos, their dependencies were already resolved in this run,
        // so they exist in `fn_call_memos` and will be visited by the outer loop.
    }

    // Transitively expand deps of already-stored memos (read from DB).
    // Collect their target_state_paths so those target states are not GC'd.
    // Use a single read transaction for all DB reads.
    let db_env = comp_ctx.app_ctx().env().db_env();
    let rtxn = db_env.read_txn()?;
    let db = comp_ctx.app_ctx().db();
    while let Some(fp) = deps_to_process.pop_front() {
        if !result.all_memos_fps.insert(fp) {
            continue;
        }
        let Some(memo) = read_fn_call_memo_with_txn(&rtxn, db, comp_ctx, fp)? else {
            continue;
        };
        for child_component in &memo.child_components {
            stable_path_set.add_child(child_component.as_ref(), StablePathSet::Component)?;
        }
        result
            .contained_target_state_paths
            .extend(memo.target_state_paths.into_iter());
        deps_to_process.extend(memo.dependency_memo_entries.into_iter());
    }
    Ok(result)
}
