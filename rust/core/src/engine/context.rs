use std::collections::{BTreeMap, HashSet};

use cocoindex_utils::fingerprint::Fingerprint;

use crate::engine::component::{Component, ComponentBgChildReadiness};
use crate::engine::profile::EngineProfile;
use crate::engine::stats::ProcessingStats;
use crate::engine::target_state::{TargetStateProvider, TargetStateProviderRegistry};
use crate::prelude::*;

use crate::state::effect_path::EffectPath;
use crate::state::stable_path_set::ChildStablePathSet;
use crate::{
    engine::environment::{AppRegistration, Environment},
    state::stable_path::StablePath,
};

struct AppContextInner<Prof: EngineProfile> {
    env: Environment<Prof>,
    db: db_schema::Database,
    app_reg: AppRegistration<Prof>,
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
            inner: Arc::new(AppContextInner { env, db, app_reg }),
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
}

pub(crate) struct DeclaredEffect<Prof: EngineProfile> {
    pub provider: TargetStateProvider<Prof>,
    pub key: Prof::TargetStateKey,
    pub value: Prof::TargetStateValue,
    pub child_provider: Option<TargetStateProvider<Prof>>,
}

pub(crate) struct ComponentEffectContext<Prof: EngineProfile> {
    pub declared_effects: BTreeMap<EffectPath, DeclaredEffect<Prof>>,
    pub provider_registry: TargetStateProviderRegistry<Prof>,
}

pub struct FnCallMemo<Prof: EngineProfile> {
    pub ret: Prof::FunctionData,
    pub(crate) child_components: Vec<StablePath>,
    pub(crate) effect_paths: Vec<EffectPath>,
    pub(crate) dependency_memo_entries: HashSet<Fingerprint>,
    pub(crate) already_stored: bool,
}

pub enum FnCallMemoEntry<Prof: EngineProfile> {
    /// Memoization result is pending, i.e. the function call is not finished yet.
    Pending,
    /// Memoization result is ready. None means memoization is disabled, e.g. it creates effect providers.
    Ready(Option<FnCallMemo<Prof>>),
}

impl<Prof: EngineProfile> Default for FnCallMemoEntry<Prof> {
    fn default() -> Self {
        Self::Pending
    }
}

pub(crate) struct ComponentBuildingState<Prof: EngineProfile> {
    pub effect: ComponentEffectContext<Prof>,
    pub child_path_set: ChildStablePathSet,
    pub fn_call_memos: HashMap<Fingerprint, Arc<tokio::sync::RwLock<FnCallMemoEntry<Prof>>>>,
}

pub(crate) struct ComponentDeleteContext<Prof: EngineProfile> {
    pub providers: rpds::HashTrieMapSync<EffectPath, TargetStateProvider<Prof>>,
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
        providers: rpds::HashTrieMapSync<EffectPath, TargetStateProvider<Prof>>,
        parent_context: Option<ComponentProcessorContext<Prof>>,
        processing_stats: ProcessingStats,
        mode: ComponentProcessingMode,
    ) -> Self {
        let processing_state = if mode == ComponentProcessingMode::Build {
            ComponentProcessingAction::Build(Mutex::new(Some(ComponentBuildingState {
                effect: ComponentEffectContext {
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
    /// Effects that are declared by the function.
    pub effect_paths: Vec<EffectPath>,
    /// Dependency entries that are declared by the function. Only needs to keep dependencies with side effects (child components / effects / dependency entries with side effects).
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
            inner.effect_paths.extend(child_inner.effect_paths);
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
