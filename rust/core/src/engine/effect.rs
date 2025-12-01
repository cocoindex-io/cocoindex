use cocoindex_utils::fingerprint::Fingerprint;

use crate::{engine::profile::EngineProfile, prelude::*, state::effect_path::EffectPath};

use std::{collections::HashMap, hash::Hash};

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
    ) -> Result<EffectReconcileOutput<Prof>, Prof::Error>;
}

pub(crate) struct EffectProviderInner<Prof: EngineProfile> {
    pub effect_path: EffectPath,
    pub reconciler: Prof::EffectRcl,
}

#[derive(Clone)]
pub struct EffectProvider<Prof: EngineProfile> {
    pub(crate) inner: Arc<EffectProviderInner<Prof>>,
}

impl<Prof: EngineProfile> EffectProvider<Prof> {
    pub fn effect_path(&self) -> &EffectPath {
        &self.inner.effect_path
    }

    pub fn reconciler(&self) -> &Prof::EffectRcl {
        &self.inner.reconciler
    }
}

#[derive(Clone)]
pub struct RootEffectProviderRegistry<Prof: EngineProfile> {
    providers: Arc<Mutex<HashMap<Fingerprint, EffectProvider<Prof>>>>,
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
        let fp = Fingerprint::from(&name)?;
        let provider = EffectProvider {
            inner: Arc::new(EffectProviderInner {
                effect_path: EffectPath::new(fp),
                reconciler,
            }),
        };
        let mut providers = self.providers.lock().unwrap();
        providers.insert(fp, provider.clone());
        Ok(provider)
    }

    pub fn get_provider(&self, effect_path: &EffectPath) -> Option<EffectProvider<Prof>> {
        effect_path
            .as_slice()
            .first()
            .and_then(|fp| self.providers.lock().unwrap().get(fp).cloned())
    }
}
