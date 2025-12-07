use cocoindex_utils::fingerprint::Fingerprint;

use crate::{engine::profile::EngineProfile, prelude::*, state::effect_path::EffectPath};

use std::hash::Hash;

pub trait EffectSink<Prof: EngineProfile>: Send + Sync + Eq + Hash + 'static {
    // TODO: Add method to expose function info and arguments, for tracing purpose & no-change detection.

    /// Run the logic to apply the action.
    ///
    /// We expect the implementation of this method to spawn the logic to a separate thread or task when needed.
    #[allow(async_fn_in_trait)]
    async fn apply(&self, actions: Vec<Prof::EffectAction>) -> Result<(), Prof::Error>;
}

pub struct EffectReconcileOutput<Prof: EngineProfile> {
    pub action: Prof::EffectAction,
    pub sink: Prof::EffectSink,
    pub state: Option<Prof::EffectState>,
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
    ) -> Result<Option<EffectReconcileOutput<Prof>>, Prof::Error>;
}

pub(crate) struct EffectProviderInner<Prof: EngineProfile> {
    pub effect_path: EffectPath,
    pub reconciler: OnceLock<Prof::EffectRcl>,
    pub orphaned: OnceLock<()>,
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
                reconciler: OnceLock::new(),
                orphaned: OnceLock::new(),
            }),
        }
    }
    pub fn effect_path(&self) -> &EffectPath {
        &self.inner.effect_path
    }

    pub fn reconciler(&self) -> Option<&Prof::EffectRcl> {
        self.inner.reconciler.get()
    }
}

#[derive(Default)]
pub struct EffectProviderRegistry<Prof: EngineProfile> {
    pub(crate) providers: rpds::HashTrieMapSync<EffectPath, EffectProvider<Prof>>,
    pub(crate) curr_effect_paths: Vec<EffectPath>,
}

impl<Prof: EngineProfile> EffectProviderRegistry<Prof> {
    pub fn new(parent_registry: Option<&Self>) -> Self {
        Self {
            providers: parent_registry
                .map(|r| r.providers.clone())
                .unwrap_or_default(),
            curr_effect_paths: Vec::new(),
        }
    }

    pub fn register(
        &mut self,
        effect_path: EffectPath,
        reconciler: Prof::EffectRcl,
    ) -> Result<EffectProvider<Prof>> {
        let provider = EffectProvider {
            inner: Arc::new(EffectProviderInner {
                effect_path: effect_path.clone(),
                reconciler: OnceLock::from(reconciler),
                orphaned: OnceLock::new(),
            }),
        };
        self.curr_effect_paths.push(effect_path.clone());
        self.providers.insert_mut(effect_path, provider.clone());
        Ok(provider)
    }

    pub fn get_provider(&self, effect_path: &[Fingerprint]) -> Option<EffectProvider<Prof>> {
        self.providers.get(effect_path).cloned()
    }
}
