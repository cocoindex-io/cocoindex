use crate::prelude::*;

use std::borrow::Cow;
use std::cmp::{Ord, Ordering};
use std::collections::{HashMap, VecDeque, btree_map};

use heed::{RoTxn, RwTxn};

use crate::engine::context::{
    ComponentProcessingAction, ComponentProcessorContext, DeclaredEffect,
};
use crate::engine::effect::{EffectProvider, EffectProviderRegistry, EffectReconciler, EffectSink};
use crate::engine::profile::{EngineProfile, Persist, StableFingerprint};
use crate::state::effect_path::EffectPath;
use crate::state::state_path::{StateKey, StatePath, StatePathRef};
use crate::state::state_path_set::{ChildStatePathSet, StatePathSet};

pub fn declare_effect<Prof: EngineProfile>(
    context: &ComponentProcessorContext<Prof>,
    provider: EffectProvider<Prof>,
    key: Prof::EffectKey,
    decl: Prof::EffectDecl,
) -> Result<()> {
    let effect_path = make_effect_path(&provider, &key);
    let declared_effect = DeclaredEffect {
        provider,
        key,
        decl,
        child_provider: None,
    };
    context.update_building_state(|building_state| {
        match building_state.effect.declared_effects.entry(effect_path) {
            btree_map::Entry::Occupied(entry) => {
                bail!("Effect already declared with key: {:?}", entry.get().key);
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
    decl: Prof::EffectDecl,
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
            decl,
            child_provider: Some(child_provider.clone()),
        };
        match building_state.effect.declared_effects.entry(effect_path) {
            btree_map::Entry::Occupied(entry) => {
                bail!("Effect already declared with key: {:?}", entry.get().key);
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
    path: StatePath,
    child_path_set: Option<ChildStatePathSet>,
}

struct ChildPathInfo {
    encoded_db_key: Vec<u8>,
    encoded_db_value: Vec<u8>,
    state_key: StateKey,
    state_path_set: StatePathSet,
}

struct Committer<'a, Prof: EngineProfile> {
    component_ctx: &'a ComponentProcessorContext<Prof>,
    db: &'a db_schema::Database,
    effect_providers: &'a rpds::HashTrieMapSync<EffectPath, EffectProvider<Prof>>,

    component_state_path: &'a StatePath,

    encoded_tombstone_key_prefix: Vec<u8>,

    existence_processing_queue: VecDeque<ChildrenPathInfo>,
    buffered_state_paths_for_tombstone: Vec<StatePath>,
}

impl<'a, Prof: EngineProfile> Committer<'a, Prof> {
    fn new(
        component_ctx: &'a ComponentProcessorContext<Prof>,
        component_state_path: &'a StatePath,
        effect_providers: &'a rpds::HashTrieMapSync<EffectPath, EffectProvider<Prof>>,
    ) -> Result<Self> {
        let tombstone_key_prefix = db_schema::DbEntryKey::State(
            component_state_path.clone(),
            db_schema::StateEntryKey::ChildComponentTombstonePrefix,
        );
        let encoded_tombstone_key_prefix = tombstone_key_prefix.encode()?;
        Ok(Self {
            component_ctx,
            db: component_ctx.app_ctx().db(),
            effect_providers,
            component_state_path,
            encoded_tombstone_key_prefix,
            existence_processing_queue: VecDeque::new(),
            buffered_state_paths_for_tombstone: Vec::new(),
        })
    }

    fn commit(
        &mut self,
        child_state_path_set: Option<ChildStatePathSet>,
        effect_info_key: &db_schema::DbEntryKey,
        curr_version: u64,
    ) -> Result<()> {
        let db_env = self.component_ctx.app_ctx().env().db_env();
        {
            let mut wtxn = db_env.write_txn()?;
            let mut effect_info: db_schema::StateEntryEffectInfo = self
                .db
                .get(&wtxn, effect_info_key.encode()?.as_ref())?
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
                effect_info_key.encode()?.as_ref(),
                data_bytes.as_slice(),
            )?;

            self.update_existence(&mut wtxn, child_state_path_set)?;
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
        child_path_set: Option<ChildStatePathSet>,
    ) -> Result<()> {
        self.existence_processing_queue.push_back(ChildrenPathInfo {
            path: self.component_state_path.clone(),
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
                    let v = if let Some((state_key, state_path_set)) = curr_iter.next() {
                        let db_key = db_schema::DbEntryKey::State(
                            path_info.path.clone(),
                            db_schema::StateEntryKey::ChildExistence(state_key.clone()),
                        );
                        Some(ChildPathInfo {
                            encoded_db_key: db_key.encode()?,
                            encoded_db_value: Self::encode_child_existence_info(&state_path_set)?,
                            state_key,
                            state_path_set,
                        })
                    } else {
                        None
                    };
                    Ok(v)
                };

                let mut curr_next = curr_iter_next()?;

                let db_key_prefix = db_schema::DbEntryKey::State(
                    path_info.path.clone(),
                    db_schema::StateEntryKey::ChildExistencePrefix,
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

                            match curr_next_v.state_path_set {
                                StatePathSet::Directory(curr_dir_set) => {
                                    let db_value: db_schema::ChildExistenceInfo =
                                        rmp_serde::from_slice(db_next_entry.1)?;
                                    if db_value.node_type == db_schema::StatePathNodeType::Component
                                    {
                                        self.buffered_state_paths_for_tombstone.push(
                                            self.relative_path(path_info.path.as_ref())?
                                                .concat_part(curr_next_v.state_key.clone()),
                                        );
                                    }
                                    self.existence_processing_queue.push_back(ChildrenPathInfo {
                                        path: path_info
                                            .path
                                            .concat_part(curr_next_v.state_key.clone()),
                                        child_path_set: Some(curr_dir_set),
                                    });
                                }
                                StatePathSet::Component => {
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
                if let StatePathSet::Directory(child_path_set) = child_to_add.state_path_set {
                    self.existence_processing_queue.push_back(ChildrenPathInfo {
                        path: path_info.path.concat_part(child_to_add.state_key),
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
            let relative_state_path: StatePath =
                storekey::decode(&ts_key[self.encoded_tombstone_key_prefix.len()..])?;
            let state_path = self
                .component_state_path
                .concat(relative_state_path.as_ref());
            let component = self.component_ctx.component().get_child(state_path);
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
        path: &StatePath,
        encoded_db_key_prefix: &[u8],
    ) -> Result<()> {
        let (raw_db_key, raw_db_value) = db_entry;
        let state_key: StateKey =
            storekey::decode(raw_db_key[encoded_db_key_prefix.len()..].as_ref())?;
        let db_value: db_schema::ChildExistenceInfo = rmp_serde::from_slice(raw_db_value)?;
        match db_value.node_type {
            db_schema::StatePathNodeType::Directory => {
                self.existence_processing_queue.push_back(ChildrenPathInfo {
                    path: path.concat_part(state_key),
                    child_path_set: None,
                });
            }
            db_schema::StatePathNodeType::Component => {
                self.buffered_state_paths_for_tombstone
                    .push(self.relative_path(path.as_ref())?.concat_part(state_key));
            }
        }
        Ok(())
    }

    fn flush_component_tombstones(&mut self, wtxn: &mut RwTxn<'_>) -> Result<()> {
        if self.buffered_state_paths_for_tombstone.is_empty() {
            return Ok(());
        }
        let mut encoded_tombstone_key = self.encoded_tombstone_key_prefix.clone();
        let prefix_len = encoded_tombstone_key.len();
        for state_path in std::mem::take(&mut self.buffered_state_paths_for_tombstone) {
            encoded_tombstone_key.truncate(prefix_len);
            storekey::encode(&mut encoded_tombstone_key, &state_path)?;
            self.db.put(wtxn, encoded_tombstone_key.as_slice(), &[])?;
        }
        Ok(())
    }

    fn encode_child_existence_info(state_path_set: &StatePathSet) -> Result<Vec<u8>> {
        let existence_info = match state_path_set {
            StatePathSet::Directory(_) => db_schema::ChildExistenceInfo {
                node_type: db_schema::StatePathNodeType::Directory,
            },
            StatePathSet::Component => db_schema::ChildExistenceInfo {
                node_type: db_schema::StatePathNodeType::Component,
            },
        };
        Ok(rmp_serde::to_vec(&existence_info)?)
    }

    fn relative_path<'p>(&self, path: StatePathRef<'p>) -> Result<StatePathRef<'p>> {
        path.strip_parent(self.component_state_path.as_ref())
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

pub(crate) async fn submit<Prof: EngineProfile>(
    context: &ComponentProcessorContext<Prof>,
) -> Result<Option<EffectProviderRegistry<Prof>>> {
    let mut provider_registry: Option<EffectProviderRegistry<Prof>> = None;
    let (effect_providers, declared_effects, child_state_path_set) =
        match context.processing_state() {
            ComponentProcessingAction::Build(building_state) => {
                let mut building_state = building_state.lock().unwrap();
                let Some(building_state) = building_state.take() else {
                    bail!(
                        "Processing for the component at {} is already finished",
                        context.state_path()
                    );
                };
                (
                    &provider_registry
                        .insert(building_state.effect.provider_registry)
                        .providers,
                    building_state.effect.declared_effects,
                    Some(building_state.child_state_path_set),
                )
            }
            ComponentProcessingAction::Delete(delete_context) => {
                (&delete_context.providers, Default::default(), None)
            }
        };

    let db_env = context.app_ctx().env().db_env();
    let db = context.app_ctx().db();

    let effect_info_key = db_schema::DbEntryKey::State(
        context.state_path().clone(),
        db_schema::StateEntryKey::Effects,
    );

    let mut actions_by_sinks = HashMap::<Prof::EffectSink, SinkInput<Prof>>::new();

    // Reconcile and pre-commit effects
    let curr_version = {
        let mut wtxn = db_env.write_txn()?;

        let mut effect_info: db_schema::StateEntryEffectInfo = db
            .get(&wtxn, effect_info_key.encode()?.as_ref())?
            .map(|data| rmp_serde::from_slice(&data))
            .transpose()?
            .unwrap_or_default();
        let curr_version = effect_info.version + 1;

        let mut declared_effects_to_process = declared_effects;

        // Deal with existing effects
        for (effect_path, item) in effect_info.items.iter_mut() {
            let prev_may_be_missing = item.states.iter().any(|(_, s)| s.is_none());
            let prev_states = item
                .states
                .iter()
                .filter_map(|(_, s)| s.as_ref())
                .map(|s_bytes| Prof::EffectState::from_bytes(s_bytes.as_ref()))
                .collect::<Result<Vec<_>, Prof::Error>>()?;

            let declared_effect = declared_effects_to_process.remove(effect_path);
            let (effect_provider, effect_key, declared_decl, child_provider) = match declared_effect
            {
                Some(declared_effect) => (
                    Cow::Owned(declared_effect.provider),
                    declared_effect.key,
                    Some(declared_effect.decl),
                    declared_effect.child_provider,
                ),
                None => {
                    let Some(effect_provider) = effect_providers.get(effect_path.provider_path())
                    else {
                        // TODO: Verify the parent is gone.
                        trace!(
                            "skipping effect with path {effect_path} because effect provider not found"
                        );
                        continue;
                    };
                    let effect_key = Prof::EffectKey::from_bytes(item.key.as_ref())?;
                    (Cow::Borrowed(effect_provider), effect_key, None, None)
                }
            };
            let recon_output = effect_provider
                .reconciler()
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
                .reconciler()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "effect provider not ready for effect with key {:?}",
                        effect.key
                    )
                })?
                .reconcile(
                    effect.key,
                    Some(effect.decl),
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
                states: vec![(curr_version, Some(new_state))],
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
        let recons = sink.apply(input.actions).await?;
        if let Some(child_providers) = input.child_providers {
            let Some(recons) = recons else {
                bail!("expect child providers returned by Sink");
            };
            if recons.len() != child_providers.len() {
                bail!(
                    "expect child providers returned by Sink to be the same length as the actions ({}), got {}",
                    child_providers.len(),
                    recons.len(),
                );
            }
            for (recon, child_provider) in std::iter::zip(recons, child_providers) {
                if let Some(child_provider) = child_provider {
                    if let Some(recon) = recon {
                        child_provider.fulfill_reconciler(recon)?;
                    } else {
                        bail!("expect child provider returned by Sink to be fulfilled");
                    }
                }
            }
        }
    }

    let mut committer = Committer::new(context, context.state_path(), &effect_providers)?;
    committer.commit(child_state_path_set, &effect_info_key, curr_version)?;

    Ok(provider_registry)
}
