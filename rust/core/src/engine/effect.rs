use crate::{
    engine::{context::ComponentBuilderContext, profile::EngineProfile},
    prelude::*,
    state::state_path::{StateKey, StatePath},
};

use std::{collections::HashMap, hash::Hash, ops::Deref};

pub trait EffectSink<Prof: EngineProfile>: Send + Sync + Eq + Hash + 'static {
    // TODO: Add method to expose function info and arguments, for tracing purpose & no-change detection.

    /// Run the logic to apply the action.
    ///
    /// We expect the implementation of this method to spawn the logic to a separate thread or task when needed.
    #[allow(async_fn_in_trait)]
    async fn apply(&self, actions: Vec<Prof::EffectAction>) -> Result<()>;
}

pub struct EffectReconcileOutput<Prof: EngineProfile> {
    pub state: Prof::EffectState,
    pub action: Prof::EffectAction,
    pub sink: Prof::EffectSink,
    // TODO: Add fields to indicate compatibility, especially for containers (tables)
    // - Whether or not irreversible (e.g. delete a column from a table)
    // - Whether or not destructive (all children effect should be deleted)
}

pub trait EffectReconciler<Prof: EngineProfile>: Send + Sync + Sized + 'static {
    fn reconcile(
        &self,
        key: Prof::EffectKey,
        desired_effect: Option<Prof::EffectDecl>,
        prev_possible_states: &[Prof::EffectState],
        prev_may_be_missing: bool,
    ) -> Result<EffectReconcileOutput<Prof>>;
}

pub(crate) struct EffectProviderInner<Prof: EngineProfile> {
    pub effect_state_path: StatePath,
    pub reconciler: Prof::EffectRcl,
}

#[derive(Clone)]
pub struct EffectProvider<Prof: EngineProfile> {
    pub(crate) inner: Arc<EffectProviderInner<Prof>>,
}

impl<Prof: EngineProfile> EffectProvider<Prof> {
    pub(crate) fn new(inner: EffectProviderInner<Prof>) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl<Prof: EngineProfile> EffectProvider<Prof> {
    pub fn effect_state_path(&self) -> &StatePath {
        &self.inner.effect_state_path
    }

    pub fn reconciler(&self) -> &Prof::EffectRcl {
        &self.inner.reconciler
    }
}

#[derive(Clone)]
pub struct RootEffectProviderRegistry<Prof: EngineProfile> {
    providers: Arc<Mutex<HashMap<String, EffectProvider<Prof>>>>,
}

impl<Prof: EngineProfile> RootEffectProviderRegistry<Prof> {
    pub fn new() -> Self {
        Self {
            providers: Default::default(),
        }
    }

    pub fn register(
        &self,
        name: String,
        reconciler: Prof::EffectRcl,
    ) -> Result<EffectProvider<Prof>> {
        let provider = EffectProvider::new(EffectProviderInner {
            effect_state_path: StatePath::root().concat(StateKey::Str(Arc::new(name.clone()))),
            reconciler,
        });
        let mut providers = self.providers.lock().unwrap();
        providers.insert(name, provider.clone());
        Ok(provider)
    }
}

pub fn declare_effect<Prof: EngineProfile>(
    state_path: &StatePath,
    context: &ComponentBuilderContext<Prof>,
    provider: &EffectProvider<Prof>,
    decl: Prof::EffectDecl,
    key: Prof::EffectKey,
    child_reconciler: Option<Prof::EffectRcl>,
) -> Result<Option<EffectProvider<Prof>>> {
    unimplemented!()
}
