use crate::prelude::*;

use cocoindex_utils::fingerprint::Fingerprint;

use crate::{engine::profile::EngineProfile, state::effect_path::EffectPath};

use std::hash::Hash;

#[async_trait]
pub trait EffectSink<Prof: EngineProfile>: Send + Sync + Eq + Hash + 'static {
    // TODO: Add method to expose function info and arguments, for tracing purpose & no-change detection.

    /// Run the logic to apply the action.
    ///
    /// We expect the implementation of this method to spawn the logic to a separate thread or task when needed.
    async fn apply(
        &self,
        actions: Vec<Prof::EffectAction>,
    ) -> Result<Option<Vec<Option<Prof::EffectRcl>>>, Prof::Error>;
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
    effect_path: EffectPath,
    reconciler: OnceLock<Prof::EffectRcl>,
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

    pub fn fulfill_reconciler(&self, reconciler: Prof::EffectRcl) -> Result<()> {
        self.inner
            .reconciler
            .set(reconciler)
            .map_err(|_| anyhow!("Reconciler is already fulfilled"))
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
            bail!(
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
        reconciler: Prof::EffectRcl,
    ) -> Result<EffectProvider<Prof>> {
        let provider = EffectProvider {
            inner: Arc::new(EffectProviderInner {
                effect_path: effect_path.clone(),
                reconciler: OnceLock::from(reconciler),
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
                reconciler: OnceLock::new(),
                orphaned: OnceLock::new(),
            }),
        };
        self.add(effect_path, provider.clone())?;
        Ok(provider)
    }

    pub fn get_provider(&self, effect_path: &[Fingerprint]) -> Option<EffectProvider<Prof>> {
        self.providers.get(effect_path).cloned()
    }
}
