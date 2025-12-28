use crate::prelude::*;

use std::borrow::Cow;
use std::cmp::{Ord, Ordering};
use std::collections::{HashMap, VecDeque, btree_map};

use heed::{RoTxn, RwTxn};

use crate::engine::context::{
    ComponentProcessingAction, ComponentProcessingMode, ComponentProcessorContext, DeclaredEffect,
};
use crate::engine::effect::{EffectHandler, EffectProvider, EffectProviderRegistry, EffectSink};
use crate::engine::profile::{EngineProfile, Persist, StableFingerprint};
use crate::state::effect_path::EffectPath;
use crate::state::stable_path::{StableKey, StablePath, StablePathRef};
use crate::state::stable_path_set::{ChildStablePathSet, StablePathSet};

pub fn declare_effect<Prof: EngineProfile>(
    context: &ComponentProcessorContext<Prof>,
    provider: EffectProvider<Prof>,
    key: Prof::EffectKey,
    value: Prof::EffectValue,
) -> Result<()> {
    let effect_path = make_effect_path(&provider, &key);
    let declared_effect = DeclaredEffect {
        provider,
        key,
        value,
        child_provider: None,
    };
    context.update_building_state(|building_state| {
        match building_state.effect.declared_effects.entry(effect_path) {
            btree_map::Entry::Occupied(entry) => {
                client_bail!("Effect already declared with key: {:?}", entry.get().key);
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(declared_effect);
            }
        }
        Ok(())
    })
}

pub fn declare_effect_with_child<Prof: EngineProfile>(
    context: &ComponentProcessorContext<Prof>,
    provider: EffectProvider<Prof>,
    key: Prof::EffectKey,
    value: Prof::EffectValue,
) -> Result<EffectProvider<Prof>> {
    let effect_path = make_effect_path(&provider, &key);
    context.update_building_state(|building_state| {
        let child_provider = building_state
            .effect
            .provider_registry
            .register_lazy(effect_path.clone())?;
        let declared_effect = DeclaredEffect {
            provider,
            key,
            value,
            child_provider: Some(child_provider.clone()),
        };
        match building_state.effect.declared_effects.entry(effect_path) {
            btree_map::Entry::Occupied(entry) => {
                client_bail!("Effect already declared with key: {:?}", entry.get().key);
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(declared_effect);
            }
        }
        Ok(child_provider)
    })
}

fn make_effect_path<Prof: EngineProfile>(
    provider: &EffectProvider<Prof>,
    key: &Prof::EffectKey,
) -> EffectPath {
    let fp = key.stable_fingerprint();
    provider.effect_path().concat(fp)
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
    effect_providers: &'a rpds::HashTrieMapSync<EffectPath, EffectProvider<Prof>>,

    component_path: &'a StablePath,

    encoded_tombstone_key_prefix: Vec<u8>,

    existence_processing_queue: VecDeque<ChildrenPathInfo>,
    buffered_paths_for_tombstone: Vec<StablePath>,

    demote_component_only: bool,
}

impl<'a, Prof: EngineProfile> Committer<'a, Prof> {
    fn new(
        component_ctx: &'a ComponentProcessorContext<Prof>,
        component_path: &'a StablePath,
        effect_providers: &'a rpds::HashTrieMapSync<EffectPath, EffectProvider<Prof>>,
        demote_component_only: bool,
    ) -> Result<Self> {
        let tombstone_key_prefix = db_schema::DbEntryKey::StablePath(
            component_path.clone(),
            db_schema::StablePathEntryKey::ChildComponentTombstonePrefix,
        );
        let encoded_tombstone_key_prefix = tombstone_key_prefix.encode()?;
        Ok(Self {
            component_ctx,
            db: component_ctx.app_ctx().db(),
            effect_providers,
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
        curr_version: u64,
    ) -> Result<()> {
        let encoded_effect_info_key = effect_info_key.encode()?;
        let db_env = self.component_ctx.app_ctx().env().db_env();
        {
            let mut wtxn = db_env.write_txn()?;
            if self.component_ctx.mode() == ComponentProcessingMode::Delete {
                self.db
                    .delete(&mut wtxn, encoded_effect_info_key.as_ref())?;
            } else {
                let mut effect_info: db_schema::StablePathEntryEffectInfo = self
                    .db
                    .get(&wtxn, encoded_effect_info_key.as_ref())?
                    .map(|data| rmp_serde::from_slice(&data))
                    .transpose()?
                    .unwrap_or_default();

                for item in effect_info.items.values_mut() {
                    item.states.retain(|(version, state)| {
                        *version > curr_version || *version == curr_version && state.is_some()
                    });
                }
                effect_info.items.retain(|_, item| !item.states.is_empty());
                let is_version_converged = effect_info.items.iter().all(|(_, item)| {
                    item.states
                        .iter()
                        .all(|(version, _)| *version == curr_version)
                });
                if is_version_converged {
                    effect_info.version = 1;
                    for item in effect_info.items.values_mut() {
                        for (version, _) in item.states.iter_mut() {
                            *version = 1;
                        }
                    }
                }

                let data_bytes = rmp_serde::to_vec(&effect_info)?;
                self.db.put(
                    &mut wtxn,
                    encoded_effect_info_key.as_ref(),
                    data_bytes.as_slice(),
                )?;
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
                                        rmp_serde::from_slice(db_next_entry.1)?;
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
            component.delete(
                Some(self.component_ctx.clone()),
                self.effect_providers.clone(),
            )?;
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
        let db_value: db_schema::ChildExistenceInfo = rmp_serde::from_slice(raw_db_value)?;
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
        Ok(rmp_serde::to_vec(&existence_info)?)
    }

    fn relative_path<'p>(&self, path: StablePathRef<'p>) -> Result<StablePathRef<'p>> {
        path.strip_parent(self.component_path.as_ref())
    }
}

struct SinkInput<Prof: EngineProfile> {
    actions: Vec<Prof::EffectAction>,
    child_providers: Option<Vec<Option<EffectProvider<Prof>>>>,
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
        action: Prof::EffectAction,
        child_provider: Option<EffectProvider<Prof>>,
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

#[instrument(name = "submit", skip_all)]
pub(crate) async fn submit<Prof: EngineProfile>(
    context: &ComponentProcessorContext<Prof>,
) -> Result<Option<EffectProviderRegistry<Prof>>> {
    let mut provider_registry: Option<EffectProviderRegistry<Prof>> = None;
    let (effect_providers, declared_effects, child_path_set) = match context.processing_state() {
        ComponentProcessingAction::Build(building_state) => {
            let mut building_state = building_state.lock().unwrap();
            let Some(building_state) = building_state.take() else {
                internal_bail!(
                    "Processing for the component at {} is already finished",
                    context.stable_path()
                );
            };
            (
                &provider_registry
                    .insert(building_state.effect.provider_registry)
                    .providers,
                building_state.effect.declared_effects,
                Some(building_state.child_path_set),
            )
        }
        ComponentProcessingAction::Delete(delete_context) => {
            (&delete_context.providers, Default::default(), None)
        }
    };

    let db_env = context.app_ctx().env().db_env();
    let db = context.app_ctx().db();

    let effect_info_key = db_schema::DbEntryKey::StablePath(
        context.stable_path().clone(),
        db_schema::StablePathEntryKey::Effects,
    );

    let mut actions_by_sinks = HashMap::<Prof::EffectSink, SinkInput<Prof>>::new();
    let mut demote_component_only = false;

    // Reconcile and pre-commit effects
    let curr_version = {
        let mut wtxn = db_env.write_txn()?;

        if let Some((parent_path, key)) = context.stable_path().as_ref().split_parent() {
            match context.mode() {
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
                        Some(db_schema::StablePathNodeType::Component) => return Ok(None),
                        Some(db_schema::StablePathNodeType::Directory) => {
                            demote_component_only = true;
                        }
                        None => {}
                    }
                }
            }
        }

        let mut effect_info: db_schema::StablePathEntryEffectInfo = db
            .get(&wtxn, effect_info_key.encode()?.as_ref())?
            .map(|data| rmp_serde::from_slice(&data))
            .transpose()?
            .unwrap_or_default();
        let curr_version = effect_info.version + 1;
        effect_info.version = curr_version;

        let mut declared_effects_to_process = declared_effects;

        // Deal with existing effects
        for (effect_path, item) in effect_info.items.iter_mut() {
            let prev_may_be_missing = item.states.iter().any(|(_, s)| s.is_none());
            let prev_states = item
                .states
                .iter()
                .filter_map(|(_, s)| s.as_ref())
                .map(|s_bytes| Prof::EffectState::from_bytes(s_bytes.as_ref()))
                .collect::<Result<Vec<_>>>()?;

            let declared_effect = declared_effects_to_process.remove(effect_path);
            let (effect_provider, effect_key, declared_decl, child_provider) = match declared_effect
            {
                Some(declared_effect) => (
                    Cow::Owned(declared_effect.provider),
                    declared_effect.key,
                    Some(declared_effect.value),
                    declared_effect.child_provider,
                ),
                None => {
                    let Some(effect_provider) = effect_providers.get(effect_path.provider_path())
                    else {
                        // TODO: Verify the parent is gone.
                        trace!(
                            "skip deleting effect with path {effect_path} in {} because effect provider not found",
                            context.stable_path()
                        );
                        continue;
                    };
                    let effect_key = Prof::EffectKey::from_bytes(item.key.as_ref())?;
                    (Cow::Borrowed(effect_provider), effect_key, None, None)
                }
            };
            let recon_output = effect_provider
                .handler()
                .ok_or_else(|| {
                    anyhow::anyhow!("effect provider not ready for effect with key {effect_key:?}")
                })?
                .reconcile(effect_key, declared_decl, &prev_states, prev_may_be_missing)?;
            if let Some(recon_output) = recon_output {
                actions_by_sinks
                    .entry(recon_output.sink)
                    .or_default()
                    .add_action(recon_output.action, child_provider);
                item.states.push((
                    curr_version,
                    recon_output
                        .state
                        .map(|s| s.to_bytes())
                        .transpose()?
                        .map(|s| Cow::Owned(s.into())),
                ));
            } else {
                for (version, _) in item.states.iter_mut() {
                    *version = curr_version;
                }
            }
        }

        // Deal with new effects
        for (effect_path, effect) in declared_effects_to_process {
            let effect_key_bytes = effect.key.to_bytes()?;
            let Some(recon_output) = effect
                .provider
                .handler()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "effect provider not ready for effect with key {:?}",
                        effect.key
                    )
                })?
                .reconcile(
                    effect.key,
                    Some(effect.value),
                    /*&prev_states=*/ &[],
                    /*prev_may_be_missing=*/ true,
                )?
            else {
                continue;
            };
            actions_by_sinks
                .entry(recon_output.sink)
                .or_default()
                .add_action(recon_output.action, effect.child_provider);
            let Some(new_state) = recon_output
                .state
                .map(|s| s.to_bytes())
                .transpose()?
                .map(|s| Cow::Owned(s.into()))
            else {
                continue;
            };
            let item = db_schema::EffectInfoItem {
                key: Cow::Owned(effect_key_bytes.into()),
                states: vec![(0, None), (curr_version, Some(new_state))],
            };
            effect_info.items.insert(effect_path, item);
        }

        let data_bytes = rmp_serde::to_vec(&effect_info)?;
        db.put(
            &mut wtxn,
            effect_info_key.encode()?.as_ref(),
            data_bytes.as_slice(),
        )?;
        wtxn.commit()?;

        curr_version
    };

    // Apply actions
    for (sink, input) in actions_by_sinks {
        let handlers = sink.apply(input.actions).await?;
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

    let mut committer = Committer::new(
        context,
        context.stable_path(),
        &effect_providers,
        demote_component_only,
    )?;
    committer.commit(child_path_set, &effect_info_key, curr_version)?;

    Ok(provider_registry)
}

pub(crate) fn cleanup_tombstone<Prof: EngineProfile>(
    context: &ComponentProcessorContext<Prof>,
) -> Result<()> {
    let Some(parent_ctx) = context.parent_context() else {
        return Ok(());
    };
    let parent_path = parent_ctx.stable_path();
    let relative_path = context
        .stable_path()
        .as_ref()
        .strip_parent(parent_path.as_ref())?;
    let tombstone_key = db_schema::DbEntryKey::StablePath(
        parent_path.clone(),
        db_schema::StablePathEntryKey::ChildComponentTombstone(relative_path.into()),
    );
    let encoded_tombstone_key = tombstone_key.encode()?;

    let db_env = context.app_ctx().env().db_env();
    let db = context.app_ctx().db();
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
            db.put(
                wtxn,
                encoded_db_key.as_slice(),
                &rmp_serde::to_vec(&db_schema::ChildExistenceInfo {
                    node_type: target_node_type,
                })?,
            )?;
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
    let child_existence_info: db_schema::ChildExistenceInfo = rmp_serde::from_slice(db_value)?;
    Ok(Some(child_existence_info.node_type))
}
