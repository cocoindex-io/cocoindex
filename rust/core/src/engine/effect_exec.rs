use std::borrow::Cow;
use std::collections::HashMap;

use crate::prelude::*;

use crate::engine::context::{ComponentProcessorContext, DeclaredEffect};
use crate::engine::effect::{EffectProvider, EffectReconciler, EffectSink};
use crate::engine::profile::{EngineProfile, Persist, StableFingerprint};
use crate::state::effect_path::EffectPath;

pub fn declare_effect<Prof: EngineProfile>(
    context: &ComponentProcessorContext<Prof>,
    provider: EffectProvider<Prof>,
    key: Prof::EffectKey,
    decl: Prof::EffectDecl,
) -> Result<()> {
    let effect_path = make_effect_path(&provider, &key);
    let mut effect_context = context.inner.effect.lock().unwrap();
    let declared_effect = DeclaredEffect {
        provider,
        key,
        decl,
        child_provider: None,
    };
    let existing = effect_context
        .declared_effects
        .insert(effect_path, declared_effect);
    if let Some(existing) = existing {
        bail!("Effect already declared with key: {:?}", existing.key);
    }
    Ok(())
}

pub fn declare_effect_with_child<Prof: EngineProfile>(
    context: &ComponentProcessorContext<Prof>,
    provider: EffectProvider<Prof>,
    key: Prof::EffectKey,
    decl: Prof::EffectDecl,
) -> Result<EffectProvider<Prof>> {
    let effect_path = make_effect_path(&provider, &key);
    let mut effect_context = context.inner.effect.lock().unwrap();
    let child_provider = effect_context
        .provider_registry
        .register_lazy(effect_path.clone())?;
    let declared_effect = DeclaredEffect {
        provider,
        key,
        decl,
        child_provider: Some(child_provider.clone()),
    };
    let existing = effect_context
        .declared_effects
        .insert(effect_path, declared_effect);
    if let Some(existing) = existing {
        bail!("Effect already declared with key: {:?}", existing.key);
    }
    Ok(child_provider)
}

fn make_effect_path<Prof: EngineProfile>(
    provider: &EffectProvider<Prof>,
    key: &Prof::EffectKey,
) -> EffectPath {
    let fp = key.stable_fingerprint();
    provider.effect_path().concat(fp)
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

pub(crate) async fn commit_effects<Prof: EngineProfile>(
    context: &ComponentProcessorContext<Prof>,
) -> Result<()> {
    let (declared_effects, effect_providers) = {
        let mut effect_context = context.inner.effect.lock().unwrap();
        let declared_effects = std::mem::take(&mut effect_context.declared_effects);
        let effect_providers = std::mem::take(&mut effect_context.provider_registry);
        (declared_effects, effect_providers)
    };

    let db_env = context.app_ctx().env.db_env();
    let db = context.app_ctx().db;

    let effect_info_key = db_schema::DbEntryKey::State(
        context.state_path().clone(),
        db_schema::StateEntryKey::Effects,
    );

    let mut actions_by_sinks = HashMap::<Prof::EffectSink, SinkInput<Prof>>::new();

    // Reconcile and precommit effects
    let curr_version = {
        let mut wtxn = db_env.write_txn()?;

        let mut effect_info: db_schema::StateEntryEffectInfo = db
            .get(&wtxn, &effect_info_key)?
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
                    let effect_provider = effect_providers
                        .providers
                        .get(effect_path.provider_path())
                        .ok_or_else(|| {
                            anyhow::anyhow!("Effect provider not found for path: {effect_path}")
                        })?;
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
        db.put(&mut wtxn, &effect_info_key, data_bytes.as_slice())?;
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

    // Commit effects
    {
        let mut wtxn = db_env.write_txn()?;
        let mut effect_info: db_schema::StateEntryEffectInfo = db
            .get(&wtxn, &effect_info_key)?
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
        db.put(&mut wtxn, &effect_info_key, data_bytes.as_slice())?;
        wtxn.commit()?;
    }

    // Merge new providers back to the parent context registry
    if let Some(parent_context) = &context.inner.parent_context {
        let mut parent_effect_context = parent_context.effect.lock().unwrap();
        for effect_path in effect_providers.curr_effect_paths {
            let Some(provider) = effect_providers.providers.get(&effect_path) else {
                continue;
            };
            if !provider.is_orphaned() {
                parent_effect_context
                    .provider_registry
                    .add(effect_path, provider.clone())?;
            }
        }
    }

    Ok(())
}
