use std::collections::HashMap;

use crate::prelude::*;

use crate::engine::context::{ComponentBuilderContext, DeclaredEffect};
use crate::engine::effect::{EffectProvider, EffectProviderInner, EffectReconciler, EffectSink};
use crate::engine::profile::{EngineProfile, StableFingerprint};
use crate::state::state_path::StateKey;

pub fn declare_effect<Prof: EngineProfile>(
    context: &ComponentBuilderContext<Prof>,
    declared_effect: DeclaredEffect<Prof>,
    child_reconciler: Option<Prof::EffectRcl>,
) -> Result<Option<EffectProvider<Prof>>> {
    let fp = declared_effect.key.stable_fingerprint();
    let state_path = declared_effect
        .provider
        .effect_state_path()
        .concat(StateKey::Fingerprint(fp));
    let child_provider = child_reconciler.map(|r| EffectProvider {
        inner: Arc::new(EffectProviderInner {
            effect_state_path: state_path.clone(),
            reconciler: r,
        }),
    });

    {
        let mut declared_effects = context.inner.declared_effects.lock().unwrap();
        declared_effects.insert(state_path, declared_effect);
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

    let mut actions_by_sinks = HashMap::<Prof::EffectSink, Vec<Prof::EffectAction>>::new();

    for (_, effect) in declared_effects {
        let recon_output = effect.provider.reconciler().reconcile(
            effect.key,
            Some(effect.decl),
            &[],
            /*prev_may_be_missing=*/ true,
        )?;
        actions_by_sinks
            .entry(recon_output.sink)
            .or_default()
            .push(recon_output.action);
    }

    for (sink, actions) in actions_by_sinks {
        sink.apply(actions).await?;
    }
    Ok(())
}
