use std::borrow::Cow;
use std::collections::HashMap;

use crate::prelude::*;

use crate::engine::context::{ComponentBuilderContext, DeclaredEffect};
use crate::engine::effect::{EffectProvider, EffectProviderInner, EffectReconciler, EffectSink};
use crate::engine::profile::{EngineProfile, Persist, StableFingerprint};

pub fn declare_effect<Prof: EngineProfile>(
    context: &ComponentBuilderContext<Prof>,
    declared_effect: DeclaredEffect<Prof>,
    child_reconciler: Option<Prof::EffectRcl>,
) -> Result<Option<EffectProvider<Prof>>> {
    let fp = declared_effect.key.stable_fingerprint();
    let state_path = declared_effect.provider.effect_path().concat(fp);
    let child_provider = child_reconciler.map(|r| EffectProvider {
        inner: Arc::new(EffectProviderInner {
            effect_path: state_path.clone(),
            reconciler: r,
        }),
    });

    {
        let mut declared_effects = context.inner.declared_effects.lock().unwrap();
        let existing = declared_effects.insert(state_path, declared_effect);
        if let Some(existing) = existing {
            bail!("Effect already declared with key: {:?}", existing.key);
        }
    }

    Ok(child_provider)
}

pub(crate) async fn commit_effects<Prof: EngineProfile>(
    context: &ComponentBuilderContext<Prof>,
) -> Result<()> {
    let declared_effects = {
        let mut declared_effects = context.inner.declared_effects.lock().unwrap();
        std::mem::take(&mut *declared_effects)
    };

    let db_env = context.app_ctx().env.db_env();
    let db = context.app_ctx().db;
    let effect_providers = context.app_ctx().env.effect_providers();

    let effect_info_key = db_schema::DbEntryKey::State(
        context.state_path().clone(),
        db_schema::StateEntryKey::Effects,
    );

    let mut actions_by_sinks = HashMap::<Prof::EffectSink, Vec<Prof::EffectAction>>::new();

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

            let declared_effect = declared_effects_to_process.remove(&effect_path);
            let (effect_provider, effect_key, declared_decl) = match declared_effect {
                Some(declared_effect) => (
                    declared_effect.provider,
                    declared_effect.key,
                    Some(declared_effect.decl),
                ),
                None => {
                    let effect_provider =
                        effect_providers.get_provider(&effect_path).ok_or_else(|| {
                            anyhow::anyhow!("Effect provider not found for path: {effect_path}")
                        })?;
                    let effect_key = Prof::EffectKey::from_bytes(item.key.as_ref())?;
                    (effect_provider, effect_key, None)
                }
            };
            let recon_output = effect_provider.reconciler().reconcile(
                effect_key,
                declared_decl,
                &prev_states,
                prev_may_be_missing,
            )?;
            actions_by_sinks
                .entry(recon_output.sink)
                .or_default()
                .push(recon_output.action);
            item.states.push((
                curr_version,
                recon_output
                    .state
                    .map(|s| s.to_bytes())
                    .transpose()?
                    .map(|s| Cow::Owned(s.into())),
            ));
        }

        // Deal with new effects
        for (effect_path, effect) in declared_effects_to_process {
            let effect_key_bytes = effect.key.to_bytes()?;
            let recon_output = effect.provider.reconciler().reconcile(
                effect.key,
                Some(effect.decl),
                /*&prev_states=*/ &[],
                /*prev_may_be_missing=*/ true,
            )?;
            actions_by_sinks
                .entry(recon_output.sink)
                .or_default()
                .push(recon_output.action);
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
    for (sink, actions) in actions_by_sinks {
        sink.apply(actions).await?;
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

    Ok(())
}
