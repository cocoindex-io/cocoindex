use crate::prelude::*;

use crate::{
    engine::{context::ComponentProcessorContext, profile::EngineProfile},
    state::{
        stable_path::StableKey,
        target_state_path::{TargetStatePath, TargetStateProviderGeneration},
    },
};

use std::hash::Hash;

pub struct ChildTargetDef<Prof: EngineProfile> {
    pub handler: Prof::TargetHdl,
}

#[async_trait]
pub trait TargetActionSink<Prof: EngineProfile>: Send + Sync + Eq + Hash + 'static {
    // TODO: Add method to expose function info and arguments, for tracing purpose & no-change detection.

    /// Run the logic to apply the action.
    ///
    /// We expect the implementation of this method to spawn the logic to a separate thread or task when needed.
    async fn apply(
        &self,
        host_runtime_ctx: &Prof::HostRuntimeCtx,
        host_ctx: Arc<Prof::HostCtx>,
        actions: Vec<Prof::TargetAction>,
    ) -> Result<Option<Vec<Option<ChildTargetDef<Prof>>>>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildInvalidation {
    Destructive,
    Lossy,
}

pub struct TargetReconcileOutput<Prof: EngineProfile> {
    pub action: Prof::TargetAction,
    pub sink: Prof::TargetActionSink,
    pub tracking_record: Option<Prof::TargetStateTrackingRecord>,
    pub child_invalidation: Option<ChildInvalidation>,
}

pub trait TargetHandler<Prof: EngineProfile>: Send + Sync + Sized + 'static {
    fn reconcile(
        &self,
        key: StableKey,
        desired_target_state: Option<Prof::TargetStateValue>,
        prev_possible_records: &[Prof::TargetStateTrackingRecord],
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<Prof>>>;

    fn attachment(&self, _att_type: &str) -> Result<Option<Prof::TargetHdl>> {
        Ok(None)
    }
}

pub(crate) struct TargetStateProviderInner<Prof: EngineProfile> {
    parent_provider: Option<TargetStateProvider<Prof>>,
    stable_key: StableKey,
    target_state_path: TargetStatePath,
    handler: OnceLock<Prof::TargetHdl>,
    orphaned: OnceLock<()>,
    provider_generation: OnceLock<TargetStateProviderGeneration>,
    attachments: Mutex<HashMap<Arc<str>, TargetStateProvider<Prof>>>,
}

#[derive(Clone)]
pub struct TargetStateProvider<Prof: EngineProfile> {
    pub(crate) inner: Arc<TargetStateProviderInner<Prof>>,
}

impl<Prof: EngineProfile> TargetStateProvider<Prof> {
    pub fn target_state_path(&self) -> &TargetStatePath {
        &self.inner.target_state_path
    }

    pub fn handler(&self) -> Option<&Prof::TargetHdl> {
        self.inner.handler.get()
    }

    pub fn fulfill_handler(&self, handler: Prof::TargetHdl) -> Result<()> {
        self.inner
            .handler
            .set(handler)
            .map_err(|_| internal_error!("Handler is already fulfilled"))
    }

    pub fn stable_key_chain(&self) -> Vec<StableKey> {
        let mut chain = vec![self.inner.stable_key.clone()];
        let mut current = self;
        while let Some(parent) = &current.inner.parent_provider {
            chain.push(parent.inner.stable_key.clone());
            current = parent;
        }
        chain.reverse();
        chain
    }

    pub fn is_orphaned(&self) -> bool {
        self.inner.orphaned.get().is_some()
    }

    pub fn provider_generation(&self) -> Option<&TargetStateProviderGeneration> {
        self.inner.provider_generation.get()
    }

    pub fn set_provider_generation(&self, generation: TargetStateProviderGeneration) -> Result<()> {
        self.inner
            .provider_generation
            .set(generation)
            .map_err(|_| internal_error!("Provider generation already set"))
    }

    pub fn register_attachment_provider(
        &self,
        comp_ctx: &ComponentProcessorContext<Prof>,
        att_type: &str,
    ) -> Result<TargetStateProvider<Prof>> {
        let mut attachments = self.inner.attachments.lock().unwrap();
        if let Some(existing) = attachments.get(att_type) {
            return Ok(existing.clone());
        }

        let handler = self
            .handler()
            .ok_or_else(|| client_error!("Cannot register attachment on unfulfilled provider"))?
            .attachment(att_type)?
            .ok_or_else(|| {
                client_error!("Handler does not support attachment type: {att_type:?}")
            })?;

        let symbol_key = StableKey::Symbol(att_type.into());
        let target_state_path = self.target_state_path().concat(&symbol_key);

        let provider_generation = self
            .provider_generation()
            .ok_or_else(|| {
                internal_error!(
                    "Parent provider generation must be set before registering attachment"
                )
            })?
            .clone();

        let provider = TargetStateProvider {
            inner: Arc::new(TargetStateProviderInner {
                parent_provider: Some(self.clone()),
                stable_key: symbol_key,
                target_state_path: target_state_path.clone(),
                handler: OnceLock::from(handler),
                orphaned: OnceLock::new(),
                provider_generation: OnceLock::from(provider_generation),
                attachments: Mutex::new(HashMap::new()),
            }),
        };

        comp_ctx.update_building_state(|building_state| {
            building_state
                .target_states
                .provider_registry
                .add(target_state_path, provider.clone())
        })?;

        attachments.insert(att_type.into(), provider.clone());
        Ok(provider)
    }
}

#[derive(Default)]
pub struct TargetStateProviderRegistry<Prof: EngineProfile> {
    pub(crate) providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
    pub(crate) curr_target_state_paths: Vec<TargetStatePath>,
}

impl<Prof: EngineProfile> TargetStateProviderRegistry<Prof> {
    pub fn new(
        providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
    ) -> Self {
        Self {
            providers,
            curr_target_state_paths: Vec::new(),
        }
    }

    pub fn add(
        &mut self,
        target_state_path: TargetStatePath,
        provider: TargetStateProvider<Prof>,
    ) -> Result<()> {
        if self.providers.contains_key(&target_state_path) {
            client_bail!(
                "Target state provider already registered for path: {:?}",
                target_state_path
            );
        }
        self.curr_target_state_paths.push(target_state_path.clone());
        self.providers.insert_mut(target_state_path, provider);
        Ok(())
    }

    pub fn register_root(
        &mut self,
        name: String,
        handler: Prof::TargetHdl,
    ) -> Result<TargetStateProvider<Prof>> {
        let target_state_path =
            TargetStatePath::new(utils::fingerprint::Fingerprint::from(&name)?, None);
        let provider = TargetStateProvider {
            inner: Arc::new(TargetStateProviderInner {
                parent_provider: None,
                stable_key: StableKey::Symbol(name.into()),
                target_state_path: target_state_path.clone(),
                handler: OnceLock::from(handler),
                orphaned: OnceLock::new(),
                provider_generation: OnceLock::new(),
                attachments: Mutex::new(HashMap::new()),
            }),
        };
        self.add(target_state_path, provider.clone())?;
        Ok(provider)
    }

    pub fn register_lazy(
        &mut self,
        parent_provider: &TargetStateProvider<Prof>,
        stable_key: StableKey,
    ) -> Result<TargetStateProvider<Prof>> {
        let target_state_path = parent_provider.target_state_path().concat(&stable_key);
        let provider = TargetStateProvider {
            inner: Arc::new(TargetStateProviderInner {
                parent_provider: Some(parent_provider.clone()),
                stable_key,
                target_state_path: target_state_path.clone(),
                handler: OnceLock::new(),
                orphaned: OnceLock::new(),
                provider_generation: OnceLock::new(),
                attachments: Mutex::new(HashMap::new()),
            }),
        };
        self.add(target_state_path, provider.clone())?;
        Ok(provider)
    }
}
