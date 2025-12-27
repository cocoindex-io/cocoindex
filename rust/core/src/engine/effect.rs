use crate::prelude::*;

use crate::{engine::profile::EngineProfile, state::effect_path::EffectPath};

use std::hash::Hash;

pub struct ChildEffectDef<Prof: EngineProfile> {
    pub handler: Prof::EffectHdl,
}

#[async_trait]
pub trait EffectSink<Prof: EngineProfile>: Send + Sync + Eq + Hash + 'static {
    // TODO: Add method to expose function info and arguments, for tracing purpose & no-change detection.

    /// Run the logic to apply the action.
    ///
    /// We expect the implementation of this method to spawn the logic to a separate thread or task when needed.
    async fn apply(
        &self,
        actions: Vec<Prof::EffectAction>,
    ) -> Result<Option<Vec<Option<ChildEffectDef<Prof>>>>>;
}

pub struct EffectReconcileOutput<Prof: EngineProfile> {
    pub action: Prof::EffectAction,
    pub sink: Prof::EffectSink,
    pub state: Option<Prof::EffectState>,
    // TODO: Add fields to indicate compatibility, especially for containers (tables)
    // - Whether or not irreversible (e.g. delete a column from a table)
    // - Whether or not destructive (all children effect should be deleted)
}

pub trait EffectHandler<Prof: EngineProfile>: Send + Sync + Sized + 'static {
    fn reconcile(
        &self,
        key: Prof::EffectKey,
        desired_effect: Option<Prof::EffectValue>,
        prev_possible_states: &[Prof::EffectState],
        prev_may_be_missing: bool,
    ) -> Result<Option<EffectReconcileOutput<Prof>>>;
}

pub(crate) struct EffectProviderInner<Prof: EngineProfile> {
    effect_path: EffectPath,
    handler: OnceLock<Prof::EffectHdl>,
    orphaned: OnceLock<()>,
}

#[derive(Clone)]
pub struct EffectProvider<Prof: EngineProfile> {
    pub(crate) inner: Arc<EffectProviderInner<Prof>>,
}

impl<Prof: EngineProfile> EffectProvider<Prof> {
    pub fn new(effect_path: EffectPath) -> Self {
        Self {
            inner: Arc::new(EffectProviderInner {
                effect_path,
                handler: OnceLock::new(),
                orphaned: OnceLock::new(),
            }),
        }
    }
    pub fn effect_path(&self) -> &EffectPath {
        &self.inner.effect_path
    }

    pub fn handler(&self) -> Option<&Prof::EffectHdl> {
        self.inner.handler.get()
    }

    pub fn fulfill_handler(&self, handler: Prof::EffectHdl) -> Result<()> {
        self.inner
            .handler
            .set(handler)
            .map_err(|_| internal_error!("Handler is already fulfilled"))
    }

    pub fn is_orphaned(&self) -> bool {
        self.inner.orphaned.get().is_some()
    }
}

#[derive(Default)]
pub struct EffectProviderRegistry<Prof: EngineProfile> {
    pub(crate) providers: rpds::HashTrieMapSync<EffectPath, EffectProvider<Prof>>,
    pub(crate) curr_effect_paths: Vec<EffectPath>,
}

impl<Prof: EngineProfile> EffectProviderRegistry<Prof> {
    pub fn new(providers: rpds::HashTrieMapSync<EffectPath, EffectProvider<Prof>>) -> Self {
        Self {
            providers,
            curr_effect_paths: Vec::new(),
        }
    }

    pub fn add(&mut self, effect_path: EffectPath, provider: EffectProvider<Prof>) -> Result<()> {
        if self.providers.contains_key(&effect_path) {
            client_bail!(
                "Effect provider already registered for path: {:?}",
                effect_path
            );
        }
        self.curr_effect_paths.push(effect_path.clone());
        self.providers.insert_mut(effect_path, provider);
        Ok(())
    }

    pub fn register(
        &mut self,
        effect_path: EffectPath,
        handler: Prof::EffectHdl,
    ) -> Result<EffectProvider<Prof>> {
        let provider = EffectProvider {
            inner: Arc::new(EffectProviderInner {
                effect_path: effect_path.clone(),
                handler: OnceLock::from(handler),
                orphaned: OnceLock::new(),
            }),
        };
        self.add(effect_path, provider.clone())?;
        Ok(provider)
    }

    pub fn register_lazy(&mut self, effect_path: EffectPath) -> Result<EffectProvider<Prof>> {
        let provider = EffectProvider {
            inner: Arc::new(EffectProviderInner {
                effect_path: effect_path.clone(),
                handler: OnceLock::new(),
                orphaned: OnceLock::new(),
            }),
        };
        self.add(effect_path, provider.clone())?;
        Ok(provider)
    }
}
