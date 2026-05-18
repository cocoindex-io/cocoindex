use crate::engine::component::ComponentProcessor;
use crate::prelude::*;

use std::borrow::Cow;
use std::cmp::{Ord, Ordering};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque, btree_map};

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
use crate::state_store::{AnyTxn, AppStore, WriteTxn};
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

    let app_store = comp_ctx.app_ctx().app_store();
    let path = comp_ctx.stable_path();
    {
        let mut rtxn = comp_ctx.app_ctx().env().read_txn().await?;
        let Some(memo_bytes) = app_store.read_component_memo(&mut rtxn, path).await? else {
            return Ok(None);
        };
        let memo_info: db_schema::ComponentMemoizationInfo<'_> = from_msgpack_slice(&memo_bytes)?;
        if let Some(processor_fp) = processor_fp {
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
        let app_store = comp_ctx.app_ctx().app_store().clone();
        let path = path.clone();
        comp_ctx
            .app_ctx()
            .env()
            .run_txn(move |wtxn| {
                Box::pin(async move { app_store.delete_component_memo(wtxn, &path).await })
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
    let app_store = comp_ctx.app_ctx().app_store().clone();
    let path = comp_ctx.stable_path().clone();

    // Serialize new states
    let memo_states_serialized = serialize_memo_values::<Prof>(&new_states.positional)?;
    let context_memo_states_serialized =
        serialize_context_memo_states::<Prof>(&new_states.by_context_fp)?;

    // Read existing entry and write back with updated states in one
    // transaction. The deserialized memo_info borrows from wtxn, so we
    // serialize the modified struct to bytes (releasing the borrow) before
    // writing back.
    comp_ctx
        .app_ctx()
        .env()
        .run_txn(move |wtxn| {
            Box::pin(async move {
                let encoded = {
                    let Some(existing_bytes) = app_store.read_component_memo(wtxn, &path).await?
                    else {
                        return Ok(());
                    };
                    let existing: db_schema::ComponentMemoizationInfo<'_> =
                        from_msgpack_slice(&existing_bytes)?;
                    let new_info = db_schema::ComponentMemoizationInfo {
                        processor_fp: existing.processor_fp,
                        return_value: existing.return_value,
                        logic_deps: existing.logic_deps,
                        memo_states: memo_states_serialized,
                        context_memo_states: context_memo_states_serialized,
                    };
                    rmp_serde::to_vec_named(&new_info)?
                };
                app_store
                    .write_component_memo_raw(wtxn, &path, &encoded)
                    .await
            })
        })
        .await?;
    Ok(())
}

async fn write_fn_call_memo<Prof: EngineProfile>(
    wtxn: &mut WriteTxn<'_>,
    comp_ctx: &ComponentProcessorContext<Prof>,
    memo_fp: Fingerprint,
    memo: FnCallMemo<Prof>,
) -> Result<()> {
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
    comp_ctx
        .fn_memo_accessor()
        .write(wtxn, memo_fp, &fn_call_memo)
        .await
}

async fn read_fn_call_memo_with_txn<Prof: EngineProfile, T: AnyTxn>(
    rtxn: &mut T,
    comp_ctx: &ComponentProcessorContext<Prof>,
    memo_fp: Fingerprint,
) -> Result<Option<FnCallMemo<Prof>>> {
    let Some(bytes) = comp_ctx.fn_memo_accessor().read(rtxn, memo_fp).await? else {
        return Ok(None);
    };
    let fn_call_memo: db_schema::FunctionMemoizationEntry<'_> = from_msgpack_slice(&bytes)?;
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
    let mut rtxn = comp_ctx.app_ctx().env().read_txn().await?;
    read_fn_call_memo_with_txn(&mut rtxn, comp_ctx, memo_fp).await
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

struct Committer<Prof: EngineProfile> {
    component_ctx: ComponentProcessorContext<Prof>,
    app_store: AppStore,
    target_states_providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,

    component_path: StablePath,

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
        Ok(Self {
            component_ctx: component_ctx.clone(),
            app_store: component_ctx.app_ctx().app_store().clone(),
            target_states_providers: target_states_providers.clone(),
            component_path,
            existence_processing_queue: VecDeque::new(),
            buffered_paths_for_tombstone: Vec::new(),
            demote_component_only,
        })
    }

    /// Run all DB write operations inside a provided write transaction.
    /// Returns `self` so the caller can use it for post-commit work (e.g. GC).
    async fn commit_in_txn(
        mut self,
        wtxn: &mut WriteTxn<'_>,
        child_path_set: Option<ChildStablePathSet>,
        all_memo_fps: &HashSet<Fingerprint>,
        memos_without_mounts_to_store: Vec<(Fingerprint, FnCallMemo<Prof>)>,
        curr_version: Option<u64>,
    ) -> Result<Self> {
        {
            if self.component_ctx.mode() == ComponentProcessingMode::Delete {
                self.app_store
                    .delete_tracking_info(wtxn, &self.component_path)
                    .await?;
            } else {
                let curr_version = curr_version
                    .ok_or_else(|| internal_error!("curr_version is required for Build mode"))?;
                let tracking_info_bytes = self
                    .app_store
                    .read_tracking_info(&mut *wtxn, &self.component_path)
                    .await?
                    .ok_or_else(|| internal_error!("tracking info not found for commit"))?;
                let mut tracking_info: db_schema::StablePathEntryTrackingInfo<'_> =
                    from_msgpack_slice(&tracking_info_bytes)?;

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
                self.app_store
                    .write_tracking_info_raw(wtxn, &self.component_path, &data_bytes)
                    .await?;

                // Clean up inverted tracking for pruned entries.
                for path in &pruned_paths {
                    self.app_store.delete_target_state_owner(wtxn, path).await?;
                }
            }

            // Write memos.
            for (fp, memo) in memos_without_mounts_to_store {
                write_fn_call_memo(wtxn, &self.component_ctx, fp, memo).await?;
            }

            // Delete all function memo entries that are not in the all_memo_fps.
            self.component_ctx
                .fn_memo_accessor()
                .retain(wtxn, all_memo_fps)
                .await?;

            if !self.demote_component_only {
                self.update_existence(&mut *wtxn, child_path_set).await?;
            }
        }

        Ok(self)
    }

    async fn commit(
        self,
        child_path_set: Option<ChildStablePathSet>,
        all_memo_fps: HashSet<Fingerprint>,
        memos_without_mounts_to_store: Vec<(Fingerprint, FnCallMemo<Prof>)>,
        curr_version: Option<u64>,
    ) -> Result<()> {
        // Single cheap Arc clone so we can call run_txn() / read_txn() after self moves into the closure.
        let app_ctx = self.component_ctx.app_ctx().clone();
        let committer = app_ctx
            .env()
            .run_txn(move |wtxn| {
                Box::pin(async move {
                    self.commit_in_txn(
                        wtxn,
                        child_path_set,
                        &all_memo_fps,
                        memos_without_mounts_to_store,
                        curr_version,
                    )
                    .await
                })
            })
            .await?;
        // Transaction committed — open a read txn so GC sees the committed tombstones.
        let mut rtxn = app_ctx.env().read_txn().await?;
        committer.launch_child_component_gc(&mut rtxn).await
    }

    async fn update_existence(
        &mut self,
        wtxn: &mut WriteTxn<'_>,
        child_path_set: Option<ChildStablePathSet>,
    ) -> Result<()> {
        self.existence_processing_queue.push_back(ChildrenPathInfo {
            path: self.component_path.clone(),
            child_path_set,
        });
        while let Some(path_info) = self.existence_processing_queue.pop_front() {
            // Sorted merge between the declared children (in-memory BTreeMap
            // iteration, sorted by StableKey) and the existing on-disk
            // entries (storekey-encoded byte order matches StableKey Ord).
            let mut curr_iter = path_info
                .child_path_set
                .into_iter()
                .flat_map(|set| set.children.into_iter());
            let existing_children = self
                .app_store
                .list_child_existence(&mut *wtxn, &path_info.path)
                .await?;
            let mut existing_iter = existing_children.into_iter();

            let mut curr_next = curr_iter.next();
            let mut existing_next = existing_iter.next();
            let mut children_to_add: Vec<(StableKey, StablePathSet)> = Vec::new();

            loop {
                match (&curr_next, &existing_next) {
                    (None, None) => break,
                    (Some(_), None) => {
                        // All remaining declared children are new.
                        if let Some(entry) = curr_next.take() {
                            children_to_add.push(entry);
                        }
                        children_to_add.extend(curr_iter.by_ref());
                        break;
                    }
                    (None, Some(_)) => {
                        // All remaining existing children should be deleted.
                        if let Some((key, info)) = existing_next.take() {
                            self.app_store
                                .delete_child_existence(wtxn, &path_info.path, &key)
                                .await?;
                            self.del_child(&key, &info, &path_info.path)?;
                        }
                        for (key, info) in existing_iter.by_ref() {
                            self.app_store
                                .delete_child_existence(wtxn, &path_info.path, &key)
                                .await?;
                            self.del_child(&key, &info, &path_info.path)?;
                        }
                        break;
                    }
                    (Some((curr_key, _)), Some((existing_key, _))) => {
                        match curr_key.cmp(existing_key) {
                            Ordering::Less => {
                                // New child.
                                children_to_add
                                    .push(curr_next.take().ok_or_else(invariance_violation)?);
                                curr_next = curr_iter.next();
                            }
                            Ordering::Greater => {
                                // Existing child no longer declared — delete.
                                let (key, info) =
                                    existing_next.take().ok_or_else(invariance_violation)?;
                                self.app_store
                                    .delete_child_existence(wtxn, &path_info.path, &key)
                                    .await?;
                                self.del_child(&key, &info, &path_info.path)?;
                                existing_next = existing_iter.next();
                            }
                            Ordering::Equal => {
                                let (curr_key, curr_path_set) =
                                    curr_next.take().ok_or_else(invariance_violation)?;
                                let (_, existing_info) =
                                    existing_next.take().ok_or_else(invariance_violation)?;
                                let new_node_type = node_type_for(&curr_path_set);

                                // Update the child existence info if the node type changed.
                                if existing_info.node_type != new_node_type {
                                    self.app_store
                                        .write_child_existence(
                                            wtxn,
                                            &path_info.path,
                                            &curr_key,
                                            &db_schema::ChildExistenceInfo {
                                                node_type: new_node_type,
                                            },
                                        )
                                        .await?;
                                }

                                if let StablePathSet::Directory(curr_dir_set) = curr_path_set {
                                    // Demotion: existing was a Component, now becoming a Directory
                                    // (its descendants have replaced the leaf). The old component
                                    // needs a tombstone so its target states get cleaned up.
                                    if existing_info.node_type
                                        == db_schema::StablePathNodeType::Component
                                    {
                                        self.buffered_paths_for_tombstone.push(
                                            self.relative_path(path_info.path.as_ref())?
                                                .concat_part(curr_key.clone()),
                                        );
                                    }
                                    self.existence_processing_queue.push_back(ChildrenPathInfo {
                                        path: path_info.path.concat_part(curr_key),
                                        child_path_set: Some(curr_dir_set),
                                    });
                                }
                                // StablePathSet::Component case: no-op (sub-component handles itself).
                                curr_next = curr_iter.next();
                                existing_next = existing_iter.next();
                            }
                        }
                    }
                }
            }

            for (stable_key, path_set) in children_to_add {
                let node_type = node_type_for(&path_set);
                self.app_store
                    .write_child_existence(
                        wtxn,
                        &path_info.path,
                        &stable_key,
                        &db_schema::ChildExistenceInfo { node_type },
                    )
                    .await?;
                if let StablePathSet::Directory(child_path_set) = path_set {
                    self.existence_processing_queue.push_back(ChildrenPathInfo {
                        path: path_info.path.concat_part(stable_key),
                        child_path_set: Some(child_path_set),
                    });
                }
            }

            self.flush_component_tombstones(wtxn).await?;
        }
        Ok(())
    }

    async fn launch_child_component_gc<T: AnyTxn>(&self, rtxn: &mut T) -> Result<()> {
        for relative_path in self
            .app_store
            .list_tombstones(rtxn, &self.component_path)
            .await?
        {
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
        stable_key: &StableKey,
        info: &db_schema::ChildExistenceInfo,
        parent_path: &StablePath,
    ) -> Result<()> {
        match info.node_type {
            db_schema::StablePathNodeType::Directory => {
                self.existence_processing_queue.push_back(ChildrenPathInfo {
                    path: parent_path.concat_part(stable_key.clone()),
                    child_path_set: None,
                });
            }
            db_schema::StablePathNodeType::Component => {
                self.buffered_paths_for_tombstone.push(
                    self.relative_path(parent_path.as_ref())?
                        .concat_part(stable_key.clone()),
                );
            }
        }
        Ok(())
    }

    async fn flush_component_tombstones(&mut self, wtxn: &mut WriteTxn<'_>) -> Result<()> {
        for relative_path in std::mem::take(&mut self.buffered_paths_for_tombstone) {
            self.app_store
                .write_tombstone(wtxn, &self.component_path, &relative_path)
                .await?;
        }
        Ok(())
    }

    fn relative_path<'p>(&self, path: StablePathRef<'p>) -> Result<StablePathRef<'p>> {
        path.strip_parent(self.component_path.as_ref())
    }
}

fn node_type_for(path_set: &StablePathSet) -> db_schema::StablePathNodeType {
    match path_set {
        StablePathSet::Directory(_) => db_schema::StablePathNodeType::Directory,
        StablePathSet::Component => db_schema::StablePathNodeType::Component,
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

/// Write deferred to after `pre_commit` finishes inspecting `tracking_info`.
///
/// The two cases mix in one queue because both arise during the ownership
/// preemption flow: when a target state moves from component A to B, we
/// need to (a) rewrite A's tracking info with the entry removed, and (b)
/// point the inverted index at B. Both writes must happen after the
/// borrowed `tracking_info` in `pre_commit` is dropped — hence "deferred".
enum DeferredWrite {
    /// Pre-serialized tracking info. Stored as bytes because the typed
    /// value borrows from the write txn (the read returns `*Info<'txn>`);
    /// serializing at deferral time releases that borrow so the eventual
    /// flush can take `&mut WriteTxn` for the write.
    TrackingInfoRaw { path: StablePath, encoded: Vec<u8> },
    /// Inverted-index upsert pointing `target_state_path` at `component_path`.
    OwnerUpsert {
        target_state_path: TargetStatePath,
        component_path: StablePath,
    },
}

impl DeferredWrite {
    async fn flush(self, wtxn: &mut WriteTxn<'_>, app_store: &AppStore) -> Result<()> {
        match self {
            DeferredWrite::TrackingInfoRaw { path, encoded } => {
                app_store
                    .write_tracking_info_raw(wtxn, &path, &encoded)
                    .await
            }
            DeferredWrite::OwnerUpsert {
                target_state_path,
                component_path,
            } => {
                app_store
                    .upsert_target_state_owner(wtxn, &target_state_path, &component_path)
                    .await
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn pre_commit<Prof: EngineProfile>(
    wtxn: &mut WriteTxn<'_>,
    app_store: &AppStore,
    comp_mode: ComponentProcessingMode,
    stable_path: &StablePath,
    full_reprocess: bool,
    processor_name: Option<&str>,
    contained_target_state_paths: &HashSet<TargetStatePath>,
    target_states_providers: &rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
    declared_target_states: BTreeMap<TargetStatePath, DeclaredTargetState<Prof>>,
) -> Result<Option<PreCommitOutput<Prof>>> {
    let mut actions_by_sinks = HashMap::<Prof::TargetActionSink, SinkInput<Prof>>::new();
    let mut demote_component_only = false;
    let mut processor_name_for_del: Option<String> = None;

    if comp_mode == ComponentProcessingMode::Delete {
        app_store.delete_component_memo(wtxn, stable_path).await?;
    }

    if let Some((parent_path, key)) = stable_path.as_ref().split_parent() {
        match comp_mode {
            ComponentProcessingMode::Build => {
                ensure_path_node_type(
                    app_store,
                    wtxn,
                    parent_path,
                    key,
                    db_schema::StablePathNodeType::Component,
                )
                .await?;
            }
            ComponentProcessingMode::Delete => {
                let node_type = get_path_node_type(app_store, wtxn, parent_path, key).await?;
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
    let tracking_info_bytes = app_store.read_tracking_info(wtxn, stable_path).await?;
    let mut tracking_info: Option<db_schema::StablePathEntryTrackingInfo<'_>> = tracking_info_bytes
        .as_deref()
        .map(from_msgpack_slice)
        .transpose()?;
    // Deferred DB writes that will be flushed after tracking_info is dropped,
    // since tracking_info borrows from wtxn and prevents mutable DB operations.
    let mut deferred_writes: Vec<DeferredWrite> = Vec::new();
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
                match app_store
                    .read_target_state_owner(wtxn, &target_state_path)
                    .await?
                {
                    Some(owner_info) if owner_info.component_path != *stable_path => {
                        let old_owner_path = owner_info.component_path.clone();
                        if let Some(old_tracking_bytes) =
                            app_store.read_tracking_info(wtxn, &old_owner_path).await?
                        {
                            let mut old_tracking: db_schema::StablePathEntryTrackingInfo<'_> =
                                from_msgpack_slice(&old_tracking_bytes)?;
                            let len_before = old_tracking.target_state_items.len();
                            // Look up the entry matching current provider_id.
                            // `into_owned()` releases the borrow on `old_tracking_bytes` so
                            // `prev_item` can outlive this `if let` block.
                            let prev_item = old_tracking
                                .target_state_items
                                .remove(&lookup_key)
                                .map(|item| {
                                    let mut item = item.into_owned();
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
                                // Write back old owner's modified tracking info — deferred
                                // because the deserialized struct borrows from wtxn.
                                let old_data = rmp_serde::to_vec_named(&old_tracking)?;
                                deferred_writes.push(DeferredWrite::TrackingInfoRaw {
                                    path: old_owner_path,
                                    encoded: old_data,
                                });
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
                            let new_id = id_reservation.next_id(wtxn, app_store).await?;
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
                    deferred_writes.push(DeferredWrite::OwnerUpsert {
                        target_state_path: target_state_path.clone(),
                        component_path: stable_path.clone(),
                    });
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
        app_store
            .write_tracking_info_raw(wtxn, stable_path, &data_bytes)
            .await?;
        Some(curr_version)
    } else {
        None
    };

    // Flush deferred writes now that tracking_info is dropped.
    for dw in deferred_writes {
        dw.flush(wtxn, app_store).await?;
    }

    id_reservation.commit(wtxn, app_store).await?;
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

    let app_store = comp_ctx.app_ctx().app_store().clone();
    let comp_mode = comp_ctx.mode();
    let stable_path = comp_ctx.stable_path().clone();
    let full_reprocess = comp_ctx.full_reprocess();
    let processor_name_owned: Option<String> = processor_name.map(|s| s.to_owned());

    let contained_target_state_paths =
        std::mem::take(&mut finalized_fn_call_memos.contained_target_state_paths);

    let mut pending_fulfillments: Vec<(TargetStateProvider<Prof>, Prof::TargetHdl)> = Vec::new();
    let target_states_providers_owned = target_states_providers.clone();

    // Reconcile and pre-commit target states
    let pre_commit_out = comp_ctx
        .app_ctx()
        .env()
        .run_txn(move |wtxn| {
            Box::pin(async move {
                pre_commit(
                    wtxn,
                    &app_store,
                    comp_mode,
                    &stable_path,
                    full_reprocess,
                    processor_name_owned.as_deref(),
                    &contained_target_state_paths,
                    &target_states_providers_owned,
                    declared_target_states,
                )
                .await
            })
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

    // Apply actions and collect child handlers to fulfill.
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
                        pending_fulfillments.push((child_provider, child_target_state_def.handler));
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
            finalized_fn_call_memos.all_memos_fps,
            finalized_fn_call_memos.memos_without_mounts_to_store,
            curr_version,
        )
        .await?;

    // Fulfill child handlers and register their attachment providers.
    // Done after commit so the immutable borrow on providers is released.
    if let Some(ref mut registry) = built_target_states_providers {
        for (child_provider, handler) in pending_fulfillments {
            child_provider.fulfill_handler(handler, registry)?;
        }
    }

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

    let app_store = comp_ctx.app_ctx().app_store().clone();
    let path = comp_ctx.stable_path().clone();
    comp_ctx
        .app_ctx()
        .env()
        .run_txn(move |wtxn| {
            Box::pin(async move {
                app_store
                    .write_component_memo_raw(wtxn, &path, &encoded)
                    .await
            })
        })
        .await
}

pub(crate) async fn cleanup_tombstone<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
) -> Result<()> {
    let Some(parent) = comp_ctx.component().parent() else {
        return Ok(());
    };
    let owner_path: StablePath = parent.stable_path().clone();
    let relative_path: StablePath = comp_ctx
        .stable_path()
        .as_ref()
        .strip_parent(owner_path.as_ref())?
        .into();
    let app_store = comp_ctx.app_ctx().app_store().clone();
    comp_ctx
        .app_ctx()
        .env()
        .run_txn(move |wtxn| {
            Box::pin(async move {
                app_store
                    .delete_tombstone(wtxn, &owner_path, &relative_path)
                    .await
            })
        })
        .await
}

pub(crate) async fn ensure_path_node_type(
    app_store: &AppStore,
    wtxn: &mut WriteTxn<'_>,
    parent_path: StablePathRef<'_>,
    key: &StableKey,
    target_node_type: db_schema::StablePathNodeType,
) -> Result<()> {
    app_store
        .ensure_path_node_type(wtxn, parent_path, key, target_node_type)
        .await
}

async fn get_path_node_type<T: AnyTxn>(
    app_store: &AppStore,
    rtxn: &mut T,
    parent_path: StablePathRef<'_>,
    key: &StableKey,
) -> Result<Option<db_schema::StablePathNodeType>> {
    app_store.read_path_node_type(rtxn, parent_path, key).await
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
        let mut rtxn = comp_ctx.app_ctx().env().read_txn().await?;
        while let Some(fp) = deps_to_process.pop_front() {
            if !result.all_memos_fps.insert(fp) {
                continue;
            }
            let Some(memo) = read_fn_call_memo_with_txn(&mut rtxn, comp_ctx, fp).await? else {
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
