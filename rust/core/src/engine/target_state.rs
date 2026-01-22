use crate::prelude::*;

use crate::{engine::profile::EngineProfile, state::target_state_path::TargetStatePath};

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
        actions: Vec<Prof::TargetAction>,
    ) -> Result<Option<Vec<Option<ChildTargetDef<Prof>>>>>;
}

pub struct TargetReconcileOutput<Prof: EngineProfile> {
    pub action: Prof::TargetAction,
    pub sink: Prof::TargetActionSink,
    pub tracking_record: Option<Prof::TargetStateTrackingRecord>,
    // TODO: Add fields to indicate compatibility, especially for containers (tables)
    // - Whether or not irreversible (e.g. delete a column from a table)
    // - Whether or not destructive (all children target states should be deleted)
}

pub trait TargetHandler<Prof: EngineProfile>: Send + Sync + Sized + 'static {
    fn reconcile(
        &self,
        key: Prof::TargetStateKey,
        desired_target_state: Option<Prof::TargetStateValue>,
        prev_possible_states: &[Prof::TargetStateTrackingRecord],
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<Prof>>>;
}

pub(crate) struct TargetStateProviderInner<Prof: EngineProfile> {
    target_state_path: TargetStatePath,
    handler: OnceLock<Prof::TargetHdl>,
    orphaned: OnceLock<()>,
}

#[derive(Clone)]
pub struct TargetStateProvider<Prof: EngineProfile> {
    pub(crate) inner: Arc<TargetStateProviderInner<Prof>>,
}

impl<Prof: EngineProfile> TargetStateProvider<Prof> {
    pub fn new(target_state_path: TargetStatePath) -> Self {
        Self {
            inner: Arc::new(TargetStateProviderInner {
                target_state_path,
                handler: OnceLock::new(),
                orphaned: OnceLock::new(),
            }),
        }
    }
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

    pub fn is_orphaned(&self) -> bool {
        self.inner.orphaned.get().is_some()
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

    pub fn register(
        &mut self,
        target_state_path: TargetStatePath,
        handler: Prof::TargetHdl,
    ) -> Result<TargetStateProvider<Prof>> {
        let provider = TargetStateProvider {
            inner: Arc::new(TargetStateProviderInner {
                target_state_path: target_state_path.clone(),
                handler: OnceLock::from(handler),
                orphaned: OnceLock::new(),
            }),
        };
        self.add(target_state_path, provider.clone())?;
        Ok(provider)
    }

    pub fn register_lazy(
        &mut self,
        target_state_path: TargetStatePath,
    ) -> Result<TargetStateProvider<Prof>> {
        let provider = TargetStateProvider {
            inner: Arc::new(TargetStateProviderInner {
                target_state_path: target_state_path.clone(),
                handler: OnceLock::new(),
                orphaned: OnceLock::new(),
            }),
        };
        self.add(target_state_path, provider.clone())?;
        Ok(provider)
    }
}
