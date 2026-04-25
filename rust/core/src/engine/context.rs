use std::collections::{BTreeMap, HashSet};

use cocoindex_utils::fingerprint::Fingerprint;

use crate::engine::component::{Component, ComponentBgChildReadiness};
use crate::engine::id_sequencer::IdSequencerManager;
use crate::engine::profile::EngineProfile;
use crate::engine::stats::ProcessingStats;
use crate::engine::target_state::{TargetStateProvider, TargetStateProviderRegistry};
use crate::prelude::*;

use crate::state::stable_path::StableKey;

pub(crate) static TARGET_ID_KEY: LazyLock<StableKey> =
    LazyLock::new(|| StableKey::Symbol("cocoindex/_internal/target_id".into()));
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
    inflight_semaphore: Option<Arc<tokio::sync::Semaphore>>,
    cancellation_token: tokio_util::sync::CancellationToken,
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
        max_inflight_components: Option<usize>,
    ) -> Self {
        let inflight_semaphore =
            max_inflight_components.map(|n| Arc::new(tokio::sync::Semaphore::new(n)));
        Self {
            inner: Arc::new(AppContextInner {
                env,
                db,
                app_reg,
                id_sequencer_manager: IdSequencerManager::new(),
                inflight_semaphore,
                cancellation_token: tokio_util::sync::CancellationToken::new(),
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

    pub fn inflight_semaphore(&self) -> Option<&Arc<tokio::sync::Semaphore>> {
        self.inner.inflight_semaphore.as_ref()
    }

    pub fn cancellation_token(&self) -> &tokio_util::sync::CancellationToken {
        &self.inner.cancellation_token
    }

    /// Get the next ID for the given key.
    ///
    /// IDs are allocated in batches for efficiency. The key can be `None` for a default sequencer.
    pub async fn next_id(&self, key: Option<&StableKey>) -> Result<u64> {
        let default_key = StableKey::Null;
        let key = key.unwrap_or(&default_key);
        self.inner
            .id_sequencer_manager
            .next_id(self.inner.env.txn_batcher(), &self.inner.db, key)
            .await
    }
}

pub(crate) struct DeclaredTargetState<Prof: EngineProfile> {
    pub provider: TargetStateProvider<Prof>,
    pub item_key: StableKey,
    pub value: Prof::TargetStateValue,
    pub child_provider: Option<TargetStateProvider<Prof>>,
}

pub(crate) struct ComponentTargetStatesContext<Prof: EngineProfile> {
    pub declared_target_states: BTreeMap<TargetStatePath, DeclaredTargetState<Prof>>,
    pub provider_registry: TargetStateProviderRegistry<Prof>,
}

pub struct FnCallMemo<Prof: EngineProfile> {
    pub ret: Prof::FunctionData,
    pub(crate) target_state_paths: Vec<TargetStatePath>,
    pub(crate) dependency_memo_entries: HashSet<Fingerprint>,
    pub(crate) logic_deps: HashSet<Fingerprint>,
    pub memo_states: Vec<Prof::FunctionData>,
    /// Context-borne memo states, keyed by tracked-context value fingerprint.
    /// See `db_schema::FunctionMemoizationEntry::context_memo_states`.
    pub context_memo_states: Vec<(Fingerprint, Vec<Prof::FunctionData>)>,
    pub(crate) already_stored: bool,
}

/// Combined payload of positional and context-borne memo states.
///
/// Used to thread both halves through the `ComponentProcessor::handle_memo_states`
/// callback and through function-level memoization APIs. The core crate treats
/// the context fingerprints and values as opaque data — both come from Python in
/// the Python profile and are round-tripped through the Python state handler.
pub struct MemoStatesPayload<Prof: EngineProfile> {
    pub positional: Vec<Prof::FunctionData>,
    pub by_context_fp: Vec<(Fingerprint, Vec<Prof::FunctionData>)>,
}

impl<Prof: EngineProfile> Default for MemoStatesPayload<Prof> {
    fn default() -> Self {
        Self {
            positional: Vec::new(),
            by_context_fp: Vec::new(),
        }
    }
}

impl<Prof: EngineProfile> MemoStatesPayload<Prof> {
    pub fn is_empty(&self) -> bool {
        self.positional.is_empty() && self.by_context_fp.is_empty()
    }
}

pub enum FnCallMemoEntry<Prof: EngineProfile> {
    /// Memoization result is pending, i.e. the function call is not finished yet.
    Pending,
    /// Memoization result is ready. None means memoization is disabled, e.g. it mounts child components.
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

pub(crate) struct ComponentBuildContext<Prof: EngineProfile> {
    pub state: Mutex<Option<ComponentBuildingState<Prof>>>,
    pub full_reprocess: bool,
    pub live: bool,
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
    Build(ComponentBuildContext<Prof>),
    Delete(ComponentDeleteContext<Prof>),
}

impl<Prof: EngineProfile> ComponentProcessingAction<Prof> {
    pub fn new_build(
        providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
        full_reprocess: bool,
        live: bool,
    ) -> Self {
        Self::Build(ComponentBuildContext {
            state: Mutex::new(Some(ComponentBuildingState {
                target_states: ComponentTargetStatesContext {
                    declared_target_states: Default::default(),
                    provider_registry: TargetStateProviderRegistry::new(providers),
                },
                child_path_set: Default::default(),
                fn_call_memos: Default::default(),
            })),
            full_reprocess,
            live,
        })
    }
}

struct ComponentProcessorContextInner<Prof: EngineProfile> {
    component: Component<Prof>,
    parent_context: Option<ComponentProcessorContext<Prof>>,
    processing_action: ComponentProcessingAction<Prof>,
    components_readiness: ComponentBgChildReadiness,

    processing_stats: ProcessingStats,
    inflight_permit: Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,

    /// Logic fingerprints accumulated from function calls and child components.
    logic_deps: Mutex<HashSet<Fingerprint>>,

    /// Opaque per-operation context (e.g. ContextProvider on the Python side).
    host_ctx: Arc<Prof::HostCtx>,
}

#[derive(Clone)]
pub struct ComponentProcessorContext<Prof: EngineProfile> {
    inner: Arc<ComponentProcessorContextInner<Prof>>,
}

impl<Prof: EngineProfile> ComponentProcessorContext<Prof> {
    pub(crate) fn new(
        component: Component<Prof>,
        parent_context: Option<ComponentProcessorContext<Prof>>,
        processing_stats: ProcessingStats,
        host_ctx: Arc<Prof::HostCtx>,
        processing_action: ComponentProcessingAction<Prof>,
    ) -> Self {
        Self {
            inner: Arc::new(ComponentProcessorContextInner {
                component,
                parent_context,
                processing_action,
                components_readiness: Default::default(),
                processing_stats,
                inflight_permit: Mutex::new(None),
                logic_deps: Mutex::new(HashSet::new()),
                host_ctx,
            }),
        }
    }

    pub fn host_ctx(&self) -> &Arc<Prof::HostCtx> {
        &self.inner.host_ctx
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

    /// Access the building state under a single lock acquisition.
    /// Callers should access all needed fields within `f` rather than wrapping
    /// this in convenience methods — the caller decides the lock granularity.
    pub(crate) fn update_building_state<T>(
        &self,
        f: impl FnOnce(&mut ComponentBuildingState<Prof>) -> Result<T>,
    ) -> Result<T> {
        match &self.inner.processing_action {
            ComponentProcessingAction::Build(build_ctx) => {
                let mut building_state = build_ctx.state.lock().unwrap();
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

    pub fn components_readiness(&self) -> &ComponentBgChildReadiness {
        &self.inner.components_readiness
    }

    pub(crate) fn mode(&self) -> ComponentProcessingMode {
        match &self.inner.processing_action {
            ComponentProcessingAction::Build(_) => ComponentProcessingMode::Build,
            ComponentProcessingAction::Delete { .. } => ComponentProcessingMode::Delete,
        }
    }

    pub fn join_fn_call(&self, fn_ctx: &FnCallContext) {
        let (fn_logic_deps, context_change_deps) = fn_ctx.update(|inner| {
            (
                inner.fn_logic_deps.clone(),
                inner.context_change_deps.clone(),
            )
        });
        let mut deps = self.inner.logic_deps.lock().unwrap();
        deps.extend(fn_logic_deps);
        deps.extend(context_change_deps);
    }

    /// Merge additional logic deps (e.g. from child components) into this component's set.
    pub(crate) fn merge_logic_deps(&self, deps: impl IntoIterator<Item = Fingerprint>) {
        self.inner.logic_deps.lock().unwrap().extend(deps);
    }

    /// Take the accumulated logic deps as a sorted Vec for deterministic storage.
    pub(crate) fn take_logic_deps(&self) -> Vec<Fingerprint> {
        let deps = std::mem::take(&mut *self.inner.logic_deps.lock().unwrap());
        let mut v: Vec<_> = deps.into_iter().collect();
        v.sort();
        v
    }

    /// Collect initial memo states for the change-detection context fingerprints
    /// observed so far (stored in this component's `logic_deps`), by looking
    /// them up in the env's eager-initial-states registry.
    ///
    /// Used by the Python memoization layer on cache miss to populate a new
    /// entry's `context_memo_states` in a single Rust call, without
    /// snapshotting `logic_deps` to Python and doing per-entry lookups there.
    pub fn collect_context_initial_states(&self) -> Vec<(Fingerprint, Vec<Prof::FunctionData>)> {
        let deps = self.inner.logic_deps.lock().unwrap();
        self.app_ctx()
            .env()
            .collect_context_initial_states(deps.iter())
    }

    pub(crate) fn set_inflight_permit(&self, permit: tokio::sync::OwnedSemaphorePermit) {
        *self.inner.inflight_permit.lock().unwrap() = Some(permit);
    }

    /// Release the inflight permit if held. No-op after first call.
    pub(crate) fn release_inflight_permit(&self) {
        *self.inner.inflight_permit.lock().unwrap() = None;
    }

    pub fn processing_stats(&self) -> &ProcessingStats {
        &self.inner.processing_stats
    }

    pub fn full_reprocess(&self) -> bool {
        match &self.inner.processing_action {
            ComponentProcessingAction::Build(build_ctx) => build_ctx.full_reprocess,
            ComponentProcessingAction::Delete { .. } => false,
        }
    }

    pub fn live(&self) -> bool {
        match &self.inner.processing_action {
            ComponentProcessingAction::Build(build_ctx) => build_ctx.live,
            ComponentProcessingAction::Delete { .. } => false,
        }
    }
}

#[derive(Default)]
pub struct FnCallContextInner {
    /// Target states that are declared by the function.
    pub target_state_paths: Vec<TargetStatePath>,
    /// Dependency entries that are declared by the function. Only needs to keep dependencies with side effects (target states / dependency entries with side effects).
    pub dependency_memo_entries: HashSet<Fingerprint>,

    /// Whether the function (directly or transitively) mounted any child components.
    /// If true, function-level memoization is disabled for this call.
    pub has_child_components: bool,

    /// Function logic fingerprints (mode-controlled propagation via `propagate_children_fn_logic`).
    pub fn_logic_deps: HashSet<Fingerprint>,
    /// Context key fingerprints (always propagate regardless of logic_tracking mode).
    pub context_change_deps: HashSet<Fingerprint>,
}

pub struct FnCallContext {
    pub(crate) inner: Mutex<FnCallContextInner>,
    /// Whether to merge children's `fn_logic_deps` into this context.
    /// `true` for "full" mode, `false` for "self" or `None` mode.
    propagate_children_fn_logic: bool,
}

impl Default for FnCallContext {
    fn default() -> Self {
        Self {
            inner: Mutex::new(FnCallContextInner::default()),
            propagate_children_fn_logic: true,
        }
    }
}

impl FnCallContext {
    pub fn new(propagate_children_fn_logic: bool) -> Self {
        Self {
            inner: Mutex::new(FnCallContextInner::default()),
            propagate_children_fn_logic,
        }
    }

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
            inner.has_child_components |= child_inner.has_child_components;
            // Context change deps always propagate.
            inner
                .context_change_deps
                .extend(child_inner.context_change_deps);
            // Function logic deps conditionally propagate.
            if self.propagate_children_fn_logic {
                inner.fn_logic_deps.extend(child_inner.fn_logic_deps);
            }
        });
    }

    pub fn add_fn_logic_dep(&self, fp: Fingerprint) {
        self.update(|inner| {
            inner.fn_logic_deps.insert(fp);
        });
    }

    pub fn add_context_change_dep(&self, fp: Fingerprint) {
        self.update(|inner| {
            inner.context_change_deps.insert(fp);
        });
    }

    /// Collect initial memo states for the change-detection context fingerprints
    /// observed so far in this function-call context, by looking them up in
    /// the given env's eager-initial-states registry.
    ///
    /// Parallel to `ComponentProcessorContext::collect_context_initial_states`;
    /// used on cache miss in the function-level memoization path (where we
    /// only have an `FnCallContext`, not a `ComponentProcessorContext`).
    pub fn collect_context_initial_states<Prof: EngineProfile>(
        &self,
        env: &Environment<Prof>,
    ) -> Vec<(Fingerprint, Vec<Prof::FunctionData>)> {
        self.update(|inner| env.collect_context_initial_states(inner.context_change_deps.iter()))
    }

    pub fn update<T>(&self, f: impl FnOnce(&mut FnCallContextInner) -> T) -> T {
        let mut guard = self.inner.lock().unwrap();
        f(&mut guard)
    }
}
