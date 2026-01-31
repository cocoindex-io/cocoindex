use std::collections::{BTreeMap, HashSet};

use cocoindex_utils::fingerprint::Fingerprint;

use crate::engine::component::{Component, ComponentBgChildReadiness};
use crate::engine::id_sequencer::IdSequencerManager;
use crate::engine::profile::EngineProfile;
use crate::engine::stats::ProcessingStats;
use crate::engine::target_state::{TargetStateProvider, TargetStateProviderRegistry};
use crate::prelude::*;

use crate::state::stable_path::StableKey;
use crate::state::stable_path_set::ChildStablePathSet;
use crate::state::target_state_path::TargetStatePath;
use crate::{
    engine::environment::{AppRegistration, Environment},
    state::stable_path::StablePath,
};

struct AppContextInner<Prof: EngineProfile> {
    env: Environment<Prof>,
    db: db_schema::Database,
    app_reg: AppRegistration<Prof>,
    id_sequencer_manager: IdSequencerManager,
}

#[derive(Clone)]
pub struct AppContext<Prof: EngineProfile> {
    inner: Arc<AppContextInner<Prof>>,
}

impl<Prof: EngineProfile> AppContext<Prof> {
    pub fn new(
        env: Environment<Prof>,
        db: db_schema::Database,
        app_reg: AppRegistration<Prof>,
    ) -> Self {
        Self {
            inner: Arc::new(AppContextInner {
                env,
                db,
                app_reg,
                id_sequencer_manager: IdSequencerManager::new(),
            }),
        }
    }

    pub fn env(&self) -> &Environment<Prof> {
        &self.inner.env
    }

    pub fn db(&self) -> &db_schema::Database {
        &self.inner.db
    }

    pub fn app_reg(&self) -> &AppRegistration<Prof> {
        &self.inner.app_reg
    }

    /// Get the next ID for the given key.
    ///
    /// IDs are allocated in batches for efficiency. The key can be `None` for a default sequencer.
    pub fn next_id(&self, key: Option<&StableKey>) -> Result<u64> {
        let default_key = StableKey::Null;
        let key = key.unwrap_or(&default_key);
        self.inner
            .id_sequencer_manager
            .next_id(self.inner.env.db_env(), &self.inner.db, key)
    }
}

pub(crate) struct DeclaredEffect<Prof: EngineProfile> {
    pub provider: TargetStateProvider<Prof>,
    pub key: Prof::TargetStateKey,
    pub value: Prof::TargetStateValue,
    pub child_provider: Option<TargetStateProvider<Prof>>,
}

pub(crate) struct ComponentTargetStatesContext<Prof: EngineProfile> {
    pub declared_effects: BTreeMap<TargetStatePath, DeclaredEffect<Prof>>,
    pub provider_registry: TargetStateProviderRegistry<Prof>,
}

pub struct FnCallMemo<Prof: EngineProfile> {
    pub ret: Prof::FunctionData,
    pub(crate) child_components: Vec<StablePath>,
    pub(crate) target_state_paths: Vec<TargetStatePath>,
    pub(crate) dependency_memo_entries: HashSet<Fingerprint>,
    pub(crate) already_stored: bool,
}

pub enum FnCallMemoEntry<Prof: EngineProfile> {
    /// Memoization result is pending, i.e. the function call is not finished yet.
    Pending,
    /// Memoization result is ready. None means memoization is disabled, e.g. it creates target states providers.
    Ready(Option<FnCallMemo<Prof>>),
}

impl<Prof: EngineProfile> Default for FnCallMemoEntry<Prof> {
    fn default() -> Self {
        Self::Pending
    }
}

pub(crate) struct ComponentBuildingState<Prof: EngineProfile> {
    pub target_states: ComponentTargetStatesContext<Prof>,
    pub child_path_set: ChildStablePathSet,
    pub fn_call_memos: HashMap<Fingerprint, Arc<tokio::sync::RwLock<FnCallMemoEntry<Prof>>>>,
}

pub(crate) struct ComponentDeleteContext<Prof: EngineProfile> {
    pub providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ComponentProcessingMode {
    Build,
    Delete,
}

pub(crate) enum ComponentProcessingAction<Prof: EngineProfile> {
    Build(Mutex<Option<ComponentBuildingState<Prof>>>),
    Delete(ComponentDeleteContext<Prof>),
}

struct ComponentProcessorContextInner<Prof: EngineProfile> {
    component: Component<Prof>,
    parent_context: Option<ComponentProcessorContext<Prof>>,
    processing_action: ComponentProcessingAction<Prof>,
    components_readiness: ComponentBgChildReadiness,

    processing_stats: ProcessingStats,
    // TODO: Add fields to record states, children components, etc.
}

#[derive(Clone)]
pub struct ComponentProcessorContext<Prof: EngineProfile> {
    inner: Arc<ComponentProcessorContextInner<Prof>>,
}

impl<Prof: EngineProfile> ComponentProcessorContext<Prof> {
    pub(crate) fn new(
        component: Component<Prof>,
        providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
        parent_context: Option<ComponentProcessorContext<Prof>>,
        processing_stats: ProcessingStats,
        mode: ComponentProcessingMode,
    ) -> Self {
        let processing_state = if mode == ComponentProcessingMode::Build {
            ComponentProcessingAction::Build(Mutex::new(Some(ComponentBuildingState {
                target_states: ComponentTargetStatesContext {
                    declared_effects: Default::default(),
                    provider_registry: TargetStateProviderRegistry::new(providers),
                },
                child_path_set: Default::default(),
                fn_call_memos: Default::default(),
            })))
        } else {
            ComponentProcessingAction::Delete(ComponentDeleteContext { providers })
        };
        Self {
            inner: Arc::new(ComponentProcessorContextInner {
                component,
                parent_context,
                processing_action: processing_state,
                components_readiness: Default::default(),
                processing_stats,
            }),
        }
    }

    pub fn component(&self) -> &Component<Prof> {
        &self.inner.component
    }

    pub fn app_ctx(&self) -> &AppContext<Prof> {
        self.inner.component.app_ctx()
    }

    pub fn stable_path(&self) -> &StablePath {
        self.inner.component.stable_path()
    }

    pub(crate) fn parent_context(&self) -> Option<&ComponentProcessorContext<Prof>> {
        self.inner.parent_context.as_ref()
    }

    pub(crate) fn update_building_state<T>(
        &self,
        f: impl FnOnce(&mut ComponentBuildingState<Prof>) -> Result<T>,
    ) -> Result<T> {
        match &self.inner.processing_action {
            ComponentProcessingAction::Build(building_state) => {
                let mut building_state = building_state.lock().unwrap();
                let Some(building_state) = &mut *building_state else {
                    internal_bail!(
                        "Processing for the component at {} is already finished",
                        self.stable_path()
                    );
                };
                f(building_state)
            }
            ComponentProcessingAction::Delete { .. } => {
                internal_bail!(
                    "Processing for the component at {} is for deletion only",
                    self.stable_path()
                )
            }
        }
    }

    pub(crate) fn processing_state(&self) -> &ComponentProcessingAction<Prof> {
        &self.inner.processing_action
    }

    pub(crate) fn components_readiness(&self) -> &ComponentBgChildReadiness {
        &self.inner.components_readiness
    }

    pub(crate) fn mode(&self) -> ComponentProcessingMode {
        match &self.inner.processing_action {
            ComponentProcessingAction::Build(_) => ComponentProcessingMode::Build,
            ComponentProcessingAction::Delete { .. } => ComponentProcessingMode::Delete,
        }
    }

    pub fn join_fn_call(&self, _fn_ctx: &FnCallContext) {
        // Nothing needs to be incorporated for now
    }

    pub fn processing_stats(&self) -> &ProcessingStats {
        &self.inner.processing_stats
    }
}

#[derive(Default)]
pub struct FnCallContextInner {
    /// Target states that are declared by the function.
    pub target_state_paths: Vec<TargetStatePath>,
    /// Dependency entries that are declared by the function. Only needs to keep dependencies with side effects (child components / target states / dependency entries with side effects).
    pub dependency_memo_entries: HashSet<Fingerprint>,

    pub child_components: Vec<StablePath>,
}

#[derive(Default)]
pub struct FnCallContext {
    pub(crate) inner: Mutex<FnCallContextInner>,
}

impl FnCallContext {
    pub fn join_child(&self, child_fn_ctx: &FnCallContext) {
        // Take the child's inner first to keep lock scope small (and avoid deadlock).
        let child_inner = child_fn_ctx.update(std::mem::take);
        self.update(|inner| {
            inner
                .target_state_paths
                .extend(child_inner.target_state_paths);
            inner
                .dependency_memo_entries
                .extend(child_inner.dependency_memo_entries);
            inner.child_components.extend(child_inner.child_components);
        });
    }

    pub fn update<T>(&self, f: impl FnOnce(&mut FnCallContextInner) -> T) -> T {
        let mut guard = self.inner.lock().unwrap();
        f(&mut guard)
    }
}
