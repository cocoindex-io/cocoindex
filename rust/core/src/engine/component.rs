use crate::engine::runtime::get_runtime;
use crate::prelude::*;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Weak;
use std::sync::atomic::AtomicU64;

use crate::engine::context::FnCallContext;
use crate::engine::context::{
    AppContext, ComponentDeleteContext, ComponentProcessingAction, ComponentProcessingMode,
    ComponentProcessorContext, MemoStatesPayload,
};
use crate::engine::execution::{
    cleanup_tombstone, post_submit_for_build, submit, update_component_memo_states,
    use_or_invalidate_component_memoization,
};
use crate::engine::profile::EngineProfile;
use crate::engine::stats::ProcessingStats;
use crate::engine::target_state::{TargetStateProvider, TargetStateProviderRegistry};
use crate::state::stable_path::{StablePath, StablePathRef};
use crate::state::stable_path_set::StablePathSet;
use crate::state::target_state_path::TargetStatePath;
use cocoindex_utils::error::{SharedError, SharedResult, SharedResultExt};
use cocoindex_utils::fingerprint::Fingerprint;

#[derive(Debug, Clone)]
pub struct ComponentProcessorInfo {
    pub name: String,
}

impl ComponentProcessorInfo {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

pub trait ComponentProcessor<Prof: EngineProfile>: Send + Sync + 'static {
    // TODO: Add method to expose function info and arguments, for tracing purpose & no-change detection.

    /// Run the logic to build the component.
    ///
    /// We expect the implementation of this method to spawn the logic to a separate thread or task when needed.
    fn process(
        &self,
        host_runtime_ctx: &Prof::HostRuntimeCtx,
        comp_ctx: &ComponentProcessorContext<Prof>,
    ) -> Result<impl Future<Output = Result<Prof::FunctionData>> + Send + 'static>;

    /// Fingerprint of the memoization key. When matching, re-processing can be skipped.
    /// When None, memoization is not enabled for the component.
    fn memo_key_fingerprint(&self) -> Option<Fingerprint>;

    fn processor_info(&self) -> &ComponentProcessorInfo;

    /// Whether this processor has a memo state handler for post-fingerprint validation.
    fn has_memo_state_handler(&self) -> bool {
        false
    }

    /// Validate or collect memo states after a fingerprint match.
    /// `stored_states`: `Some(payload)` on cache hit, `None` on cache miss (collect initial states).
    /// Returns `(new_states, can_reuse, states_changed)`:
    /// - `can_reuse`: when true, the cached value is valid and can be returned without re-execution.
    /// - `states_changed`: when true, the new states differ from stored states and must be persisted.
    ///   This can be true even when `can_reuse` is true (e.g. mtime changed but content hash unchanged).
    ///
    /// The payload carries both positional (argument-borne) and context-borne memo states.
    /// The core crate treats everything inside as opaque blobs — state functions themselves
    /// live Python-side in the Python profile.
    fn handle_memo_states(
        &self,
        host_runtime_ctx: &Prof::HostRuntimeCtx,
        comp_ctx: &ComponentProcessorContext<Prof>,
        stored_states: Option<MemoStatesPayload<Prof>>,
    ) -> Result<impl Future<Output = Result<(MemoStatesPayload<Prof>, bool, bool)>> + Send + 'static>
    {
        let _ = (host_runtime_ctx, comp_ctx, stored_states);
        Ok(async { Ok((MemoStatesPayload::default(), true, false)) })
    }
}

struct ComponentInner<Prof: EngineProfile> {
    app_ctx: AppContext<Prof>,
    stable_path: StablePath,

    /// Strong reference to the parent component. Keeps the parent (and its
    /// ancestors) alive as long as this child is alive. On Drop, removes
    /// this child's Weak entry from the parent's active_children.
    parent: Option<Component<Prof>>,

    /// Semaphore to ensure `process()` and `commit_effects()` calls cannot happen in parallel.
    build_semaphore: tokio::sync::Semaphore,
    last_memo_fp: Mutex<Option<Fingerprint>>,

    /// Latest invocation sequence number for "latest wins" ordering.
    latest_seq: AtomicU64,

    /// Active child components, keyed by their full StablePath.
    /// Uses Weak references — children are kept alive by their spawned tasks
    /// and LiveComponentController references, not by this map. When a child's
    /// last strong reference is dropped, its Drop impl removes the entry here.
    active_children: Mutex<HashMap<StablePath, Weak<ComponentInner<Prof>>>>,

    /// Shared state for a live component running at this path.
    live_state: Mutex<Option<Arc<crate::engine::live_component::LiveComponentState>>>,
}

impl<Prof: EngineProfile> Drop for ComponentInner<Prof> {
    fn drop(&mut self) {
        if let Some(parent) = &self.parent {
            parent
                .inner
                .active_children
                .lock()
                .unwrap()
                .remove(&self.stable_path);
        }
    }
}

#[derive(Clone)]
pub struct Component<Prof: EngineProfile> {
    inner: Arc<ComponentInner<Prof>>,
}

struct ComponentBgChildReadinessState {
    remaining_count: usize,
    build_done: bool,
    is_readiness_set: bool,
    outcome: ComponentRunOutcome,
}

impl ComponentBgChildReadinessState {
    fn maybe_set_readiness(
        &mut self,
        result: Option<Result<ComponentRunOutcome, SharedError>>,
        readiness: &tokio::sync::SetOnce<SharedResult<ComponentRunOutcome>>,
    ) {
        if self.is_readiness_set {
            return;
        }
        if let Some(result) = result {
            if let Ok(outcome) = result {
                self.outcome.merge(outcome);
            } else {
                self.is_readiness_set = true;
                readiness.set(result).expect("readiness set more than once");
                return;
            }
        }
        if self.remaining_count == 0 && self.build_done {
            self.is_readiness_set = true;
            readiness
                .set(Ok(std::mem::take(&mut self.outcome)))
                .expect("readiness set more than once");
        }
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct ComponentRunOutcome {
    has_exception: bool,
    logic_deps: HashSet<Fingerprint>,
}

impl ComponentRunOutcome {
    fn exception() -> Self {
        Self {
            has_exception: true,
            ..Default::default()
        }
    }

    fn merge(&mut self, other: Self) {
        self.has_exception |= other.has_exception;
        self.logic_deps.extend(other.logic_deps);
    }
}

struct ComponentBgChildReadinessInner {
    state: Mutex<ComponentBgChildReadinessState>,
    readiness: tokio::sync::SetOnce<SharedResult<ComponentRunOutcome>>,
}

#[derive(Clone)]
pub struct ComponentBgChildReadiness {
    inner: Arc<ComponentBgChildReadinessInner>,
}

pub struct ComponentBgChildReadinessChildGuard {
    readiness: ComponentBgChildReadiness,
    resolved: bool,
}

impl Drop for ComponentBgChildReadinessChildGuard {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        let mut state = self.readiness.state().lock().unwrap();
        state.remaining_count -= 1;
        // state.maybe_set_readiness(None, self.readiness.readiness());
        state.maybe_set_readiness(
            Some(Err(SharedError::new(internal_error!(
                "Child component build cancelled"
            )))),
            self.readiness.readiness(),
        );
    }
}

impl ComponentBgChildReadinessChildGuard {
    pub(crate) fn resolve(mut self, outcome: ComponentRunOutcome) {
        {
            let mut state = self.readiness.state().lock().unwrap();
            state.remaining_count -= 1;
            state.maybe_set_readiness(Some(Ok(outcome)), self.readiness.readiness());
        }
        self.resolved = true;
    }
}

impl Default for ComponentBgChildReadiness {
    fn default() -> Self {
        Self {
            inner: Arc::new(ComponentBgChildReadinessInner {
                state: Mutex::new(ComponentBgChildReadinessState {
                    remaining_count: 0,
                    is_readiness_set: false,
                    build_done: false,
                    outcome: Default::default(),
                }),
                readiness: tokio::sync::SetOnce::new(),
            }),
        }
    }
}

impl ComponentBgChildReadiness {
    fn state(&self) -> &Mutex<ComponentBgChildReadinessState> {
        &self.inner.state
    }

    fn readiness(&self) -> &tokio::sync::SetOnce<SharedResult<ComponentRunOutcome>> {
        &self.inner.readiness
    }

    pub fn add_child(self) -> ComponentBgChildReadinessChildGuard {
        self.state().lock().unwrap().remaining_count += 1;
        ComponentBgChildReadinessChildGuard {
            readiness: self,
            resolved: false,
        }
    }

    fn set_build_done(&self) {
        let mut state = self.state().lock().unwrap();
        state.build_done = true;
        state.maybe_set_readiness(None, self.readiness());
    }
}

pub struct ComponentMountRunHandle<Prof: EngineProfile> {
    join_handle: tokio::task::JoinHandle<Result<ComponentBuildOutput<Prof>>>,
}

impl<Prof: EngineProfile> ComponentMountRunHandle<Prof> {
    pub async fn result(
        self,
        parent_context: Option<&ComponentProcessorContext<Prof>>,
    ) -> Result<Prof::FunctionData> {
        let output = self.join_handle.await??;
        if let Some(parent_context) = parent_context {
            parent_context.update_building_state(|building_state| {
                for target_state_path in
                    output.built_target_states_providers.curr_target_state_paths
                {
                    let Some(provider) = output
                        .built_target_states_providers
                        .providers
                        .get(&target_state_path)
                    else {
                        error!(
                            "target states provider not found for path {}",
                            target_state_path
                        );
                        continue;
                    };
                    if !provider.is_orphaned() {
                        building_state
                            .target_states
                            .provider_registry
                            .add(target_state_path, provider.clone())?;
                    }
                }
                Ok(())
            })?;
        }
        Ok(output.ret)
    }
}

pub struct ComponentExecutionHandle {
    fut: Pin<Box<dyn Future<Output = SharedResult<()>> + Send + Sync>>,
}

impl ComponentExecutionHandle {
    pub fn new(fut: impl Future<Output = SharedResult<()>> + Send + Sync + 'static) -> Self {
        Self { fut: Box::pin(fut) }
    }

    pub async fn ready(self) -> Result<()> {
        self.fut.await.into_result()
    }
}

struct ComponentBuildOutput<Prof: EngineProfile> {
    ret: Prof::FunctionData,
    built_target_states_providers: TargetStateProviderRegistry<Prof>,
}

impl<Prof: EngineProfile> Component<Prof> {
    pub(crate) fn new(
        app_ctx: AppContext<Prof>,
        stable_path: StablePath,
        parent: Option<Component<Prof>>,
    ) -> Self {
        Self {
            inner: Arc::new(ComponentInner {
                app_ctx,
                stable_path,
                parent,
                build_semaphore: tokio::sync::Semaphore::const_new(1),
                last_memo_fp: Mutex::new(None),
                latest_seq: AtomicU64::new(0),
                active_children: Mutex::new(HashMap::new()),
                live_state: Mutex::new(None),
            }),
        }
    }

    pub fn mount_child(&self, fn_ctx: &FnCallContext, stable_path: StablePath) -> Result<Self> {
        fn_ctx.update(|inner| inner.has_child_components = true);
        Ok(self.get_child(stable_path))
    }

    /// Mount and run a child in the foreground (use_mount path).
    /// Inherits live from the parent context.
    pub async fn use_mount(
        self,
        parent_ctx: &ComponentProcessorContext<Prof>,
        processor: Prof::ComponentProc,
    ) -> Result<ComponentMountRunHandle<Prof>> {
        let child_ctx = self.new_processor_context_for_build(
            Some(parent_ctx),
            parent_ctx.processing_stats().clone(),
            parent_ctx.full_reprocess(),
            parent_ctx.live(), // use_mount inherits live from parent
            parent_ctx.host_ctx().clone(),
        )?;
        self.run(processor, child_ctx).await
    }

    /// Mount and run a child in the background (mount path).
    /// Inherits live from the parent context.
    pub async fn mount(
        self,
        parent_ctx: &ComponentProcessorContext<Prof>,
        processor: Prof::ComponentProc,
        on_error: Option<
            Arc<
                dyn Fn(Error) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>
                    + Send
                    + Sync
                    + 'static,
            >,
        >,
        pre_execute_check: Option<Box<dyn FnOnce() -> bool + Send>>,
    ) -> Result<ComponentExecutionHandle> {
        let child_ctx = self.new_processor_context_for_build(
            Some(parent_ctx),
            parent_ctx.processing_stats().clone(),
            parent_ctx.full_reprocess(),
            parent_ctx.live(), // mount inherits live from parent
            parent_ctx.host_ctx().clone(),
        )?;
        self.run_in_background(processor, child_ctx, on_error, pre_execute_check)
            .await
    }

    pub fn get_child(&self, stable_path: StablePath) -> Self {
        let mut children = self.inner.active_children.lock().unwrap();
        if let Some(weak) = children.get(&stable_path) {
            if let Some(inner) = weak.upgrade() {
                return Self { inner };
            }
        }
        let child = Self::new(
            self.app_ctx().clone(),
            stable_path.clone(),
            Some(self.clone()),
        );
        children.insert(stable_path, Arc::downgrade(&child.inner));
        child
    }

    pub fn app_ctx(&self) -> &AppContext<Prof> {
        &self.inner.app_ctx
    }

    pub fn stable_path(&self) -> &StablePath {
        &self.inner.stable_path
    }

    pub fn set_live_state(&self, state: Arc<crate::engine::live_component::LiveComponentState>) {
        *self.inner.live_state.lock().unwrap() = Some(state);
    }

    pub fn live_state(&self) -> Option<Arc<crate::engine::live_component::LiveComponentState>> {
        self.inner.live_state.lock().unwrap().clone()
    }

    pub fn latest_seq(&self) -> &AtomicU64 {
        &self.inner.latest_seq
    }

    /// Returns true if this component has no active children (all Weak refs are dead).
    pub fn has_active_children(&self) -> bool {
        let children = self.inner.active_children.lock().unwrap();
        children.values().any(|w| w.strong_count() > 0)
    }

    /// Wait until all descendants are inactive (active_children is empty).
    /// Uses exponential backoff polling: 1ms → 2ms → 4ms → ... → 10s cap.
    pub async fn wait_until_inactive(&self) {
        let mut delay = std::time::Duration::from_millis(1);
        let max_delay = std::time::Duration::from_secs(10);
        while self.has_active_children() {
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(max_delay);
        }
    }

    pub fn parent(&self) -> Option<&Component<Prof>> {
        self.inner.parent.as_ref()
    }

    pub(crate) fn relative_path(&self) -> Result<StablePathRef<'_>> {
        if let Some(parent) = self.parent() {
            self.stable_path()
                .as_ref()
                .strip_parent(parent.stable_path().as_ref())
        } else {
            Ok(self.stable_path().as_ref())
        }
    }

    pub(crate) async fn run(
        self,
        processor: Prof::ComponentProc,
        context: ComponentProcessorContext<Prof>,
    ) -> Result<ComponentMountRunHandle<Prof>> {
        // Release parent's inflight permit (deadlock prevention).
        // On a component's first child mount, the parent gives up its slot
        // so children can make progress.
        if let Some(parent_ctx) = context.parent_context() {
            parent_ctx.release_inflight_permit();
        }

        // Acquire inflight permit (waits if quota exhausted).
        if let Some(sem) = self.app_ctx().inflight_semaphore() {
            let permit = sem
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| internal_error!("Inflight semaphore closed"))?;
            context.set_inflight_permit(permit);
        }

        let relative_path = self.relative_path()?;
        let child_readiness_guard = context
            .parent_context()
            .map(|c| c.components_readiness().clone().add_child());
        let span = info_span!("component.run", component_path = %relative_path);
        let join_handle = get_runtime().spawn(
            async move {
                let result = self.execute_once(&context, Some(&processor)).await;
                let (outcome, output) = match result {
                    Ok((outcome, output)) => (outcome, Ok(output)),
                    Err(err) => (ComponentRunOutcome::exception(), Err(err)),
                };
                context.release_inflight_permit();
                drop(processor);
                drop(context);
                drop(self);
                child_readiness_guard.map(|guard| guard.resolve(outcome));
                output?
                    .ok_or_else(|| internal_error!("component deletion can only run in background"))
            }
            .instrument(span),
        );
        Ok(ComponentMountRunHandle { join_handle })
    }

    pub(crate) async fn run_in_background(
        self,
        processor: Prof::ComponentProc,
        context: ComponentProcessorContext<Prof>,
        on_error: Option<
            Arc<
                dyn Fn(Error) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>
                    + Send
                    + Sync
                    + 'static,
            >,
        >,
        pre_execute_check: Option<Box<dyn FnOnce() -> bool + Send>>,
    ) -> Result<ComponentExecutionHandle> {
        // TODO: Skip building and reuse cached result if the component is already built and up to date.

        // Release parent's inflight permit (deadlock prevention).
        if let Some(parent_ctx) = context.parent_context() {
            parent_ctx.release_inflight_permit();
        }

        // Acquire inflight permit (waits if quota exhausted).
        if let Some(sem) = self.app_ctx().inflight_semaphore() {
            let permit = sem
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| internal_error!("Inflight semaphore closed"))?;
            context.set_inflight_permit(permit);
        }

        let child_readiness_guard = context
            .parent_context()
            .map(|c| c.components_readiness().clone().add_child());
        let join_handle = get_runtime().spawn(async move {
            // Check if this task has been superseded before executing.
            if let Some(check) = pre_execute_check {
                if !check() {
                    // Superseded — skip execution, resolve as success.
                    context.release_inflight_permit();
                    drop(processor);
                    drop(context);
                    drop(self);
                    if let Some(guard) = child_readiness_guard {
                        guard.resolve(ComponentRunOutcome::default());
                    }
                    return Ok(());
                }
            }
            let result = self.execute_once(&context, Some(&processor)).await;
            // For background child component, never propagate the error back to the parent.
            // If an error handler is registered, run it; otherwise log.
            // During cancellation (e.g. Ctrl+C), suppress error reporting since
            // failures are expected as coroutines are torn down.
            let outcome = match result {
                Ok((outcome, _)) => outcome,
                Err(err) => {
                    if crate::engine::runtime::is_cancelled() {
                        trace!("component build cancelled during shutdown");
                    } else if let Some(handler) = &on_error {
                        handler(err).await;
                    } else {
                        error!("component build failed:\n{err:?}");
                    }
                    ComponentRunOutcome::exception()
                }
            };
            context.release_inflight_permit();
            drop(processor);
            drop(context);
            drop(self);
            if let Some(guard) = child_readiness_guard {
                guard.resolve(outcome);
            }
            Ok(())
        });
        Ok(ComponentExecutionHandle::new(async move {
            join_handle
                .await
                .map_err(|e| SharedError::new(internal_error!("task panicked: {e}")))?
        }))
    }

    pub fn delete(
        self,
        context: ComponentProcessorContext<Prof>,
        pre_execute_check: Option<Box<dyn FnOnce() -> bool + Send>>,
    ) -> Result<ComponentExecutionHandle> {
        let child_readiness_guard = context
            .parent_context()
            .map(|c| c.components_readiness().clone().add_child());
        let join_handle: tokio::task::JoinHandle<SharedResult<()>> =
            get_runtime().spawn(async move {
                if let Some(check) = pre_execute_check {
                    if !check() {
                        drop(context);
                        drop(self);
                        if let Some(guard) = child_readiness_guard {
                            guard.resolve(ComponentRunOutcome::default());
                        }
                        return Ok(());
                    }
                }
                trace!("deleting component at {}", self.stable_path());
                let result = self.execute_once(&context, None).await;
                let outcome = match &result {
                    Ok((outcome, _)) => outcome.clone(),
                    Err(err) => {
                        error!("component delete failed:\n{err}");
                        ComponentRunOutcome::exception()
                    }
                };
                let task_result = result.map(|_| ()).map_err(SharedError::from);
                // Drop profile-specific objects BEFORE resolving child readiness.
                // See run_in_background for the rationale (PyGILState finalization fix).
                drop(context);
                drop(self);
                if let Some(guard) = child_readiness_guard {
                    guard.resolve(outcome);
                }
                task_result
            });
        Ok(ComponentExecutionHandle::new(async move {
            join_handle
                .await
                .map_err(|e| SharedError::new(internal_error!("task panicked: {e}")))?
        }))
    }

    async fn execute_once(
        &self,
        processor_context: &ComponentProcessorContext<Prof>,
        processor: Option<&Prof::ComponentProc>,
    ) -> Result<(ComponentRunOutcome, Option<ComponentBuildOutput<Prof>>)> {
        let mut reported_processor_name: Option<Cow<'_, str>> = None;
        let mut memo_fp_to_store: Option<Fingerprint> = None;
        // Memo states collected from state validation (on cache hit with invalid states)
        // or to be collected after execution (on cache miss).
        let mut memo_states_for_store: Option<MemoStatesPayload<Prof>> = None;
        let processing_stats = processor_context.processing_stats();

        if let Some(processor) = processor {
            let processor_name = processor.processor_info().name.as_str();
            memo_fp_to_store = processor.memo_key_fingerprint();

            // Fast-path: component memoization check does not require acquiring the build permit.
            // If it hits, we can immediately return without processing/submitting/waiting.

            match use_or_invalidate_component_memoization(processor_context, memo_fp_to_store).await
            {
                Ok(Some((ret, memo_states))) => {
                    // If processor has state handler and there are stored states, validate them.
                    if processor.has_memo_state_handler() && !memo_states.is_empty() {
                        let fut = processor.handle_memo_states(
                            processor_context.app_ctx().env().host_runtime_ctx(),
                            processor_context,
                            Some(memo_states),
                        )?;
                        let (new_states, can_reuse, states_changed) = fut.await?;
                        if can_reuse {
                            // Memo is reusable — update stored states if they changed
                            if states_changed {
                                update_component_memo_states(processor_context, &new_states)
                                    .await?;
                            }
                            processing_stats.update(processor_name.as_ref(), |stats| {
                                stats.num_execution_starts += 1;
                                stats.num_unchanged += 1;
                            });
                            return Ok((
                                ComponentRunOutcome::default(),
                                Some(ComponentBuildOutput {
                                    ret,
                                    built_target_states_providers: Default::default(),
                                }),
                            ));
                        }
                        // Not reusable — fall through to re-execution
                        memo_states_for_store = Some(new_states);
                    } else {
                        // No state handler or no states — use cached result directly
                        processing_stats.update(processor_name.as_ref(), |stats| {
                            stats.num_execution_starts += 1;
                            stats.num_unchanged += 1;
                        });
                        return Ok((
                            ComponentRunOutcome::default(),
                            Some(ComponentBuildOutput {
                                ret,
                                built_target_states_providers: Default::default(),
                            }),
                        ));
                    }
                }
                Err(err) => {
                    error!("component memoization restore failed: {err:?}");
                }
                Ok(None) => {}
            }

            processor_context
                .processing_stats()
                .update(processor_name.as_ref(), |stats| {
                    stats.num_execution_starts += 1;
                });
            reported_processor_name = Some(Cow::Borrowed(processor.processor_info().name.as_str()));
        }

        let result = {
            let reported_processor_name = &mut reported_processor_name;
            async move {
                // Acquire the semaphore to ensure `process()` and `commit_effects()` cannot happen in parallel.
                let ret_n_submit_output = {
                    let _permit = self.inner.build_semaphore.acquire().await?;

                    if memo_fp_to_store.is_some() {
                        *self.inner.last_memo_fp.lock().unwrap() = memo_fp_to_store;
                        // TODO: when matching, it means there're ongoing processing for the same memoization key pending on children.
                        // We can piggyback on the same processing to avoid duplicating the work.
                    }

                    let ret: Result<Option<Prof::FunctionData>> = match &processor {
                        Some(processor) => processor
                            .process(
                                processor_context.app_ctx().env().host_runtime_ctx(),
                                &processor_context,
                            )?
                            .await
                            .map(Some),
                        None => Ok(None),
                    };
                    match ret {
                        Ok(ret) => {
                            let submit_output = submit(processor_context, processor, |name| {
                                if reported_processor_name.is_none() {
                                    processing_stats.update(&name, |stats| {
                                        stats.num_execution_starts += 1;
                                    });
                                    *reported_processor_name = Some(Cow::Owned(name.to_string()));
                                }
                            })
                            .await?;
                            Ok((ret, submit_output))
                        }
                        Err(err) => Err(err),
                    }
                };

                // Wait until children components ready.
                let components_readiness = processor_context.components_readiness();
                components_readiness.set_build_done();
                let mut children_outcome = components_readiness
                    .readiness()
                    .wait()
                    .await
                    .clone()
                    .into_result()?;

                // Merge children's logic deps into this component's context.
                processor_context
                    .merge_logic_deps(std::mem::take(&mut children_outcome.logic_deps));

                let (ret, submit_output) = ret_n_submit_output?;
                let build_output = match ret {
                    Some(ret) => {
                        if !children_outcome.has_exception {
                            // Collect initial memo states on cache miss if processor has a state handler.
                            let memo_states: MemoStatesPayload<Prof> = if let Some(processor) =
                                processor
                                && processor.has_memo_state_handler()
                            {
                                if let Some(states) = memo_states_for_store.take() {
                                    // From invalid cache hit path
                                    states
                                } else {
                                    // Cache miss — collect initial states
                                    let fut = processor.handle_memo_states(
                                        processor_context.app_ctx().env().host_runtime_ctx(),
                                        processor_context,
                                        None,
                                    )?;
                                    let (initial_states, _, _) = fut.await?;
                                    initial_states
                                }
                            } else {
                                MemoStatesPayload::default()
                            };

                            let comp_memo = if let Some(fp) = memo_fp_to_store
                                && let last_memo_fp = processor_context
                                    .component()
                                    .inner
                                    .last_memo_fp
                                    .lock()
                                    .unwrap()
                                && *last_memo_fp == memo_fp_to_store
                            {
                                Some((fp, &ret, &memo_states))
                            } else {
                                None
                            };
                            post_submit_for_build(processor_context, comp_memo).await?;
                        }
                        Some(ComponentBuildOutput {
                            ret,
                            built_target_states_providers: submit_output
                                .built_target_states_providers
                                .ok_or_else(|| {
                                    internal_error!("expect built target states providers")
                                })?,
                        })
                    }
                    None => {
                        cleanup_tombstone(&processor_context).await?;
                        None
                    }
                };
                Ok::<_, Error>((
                    children_outcome,
                    build_output,
                    submit_output.touched_previous_states,
                ))
            }
            .await
        };

        let final_processor_name = reported_processor_name
            .as_ref()
            .map(|s| s.as_ref())
            .unwrap_or(db_schema::UNKNOWN_PROCESSOR_NAME);
        match result {
            Ok((children_outcome, build_output, touched_previous_states)) => {
                processing_stats.update(final_processor_name, |stats| {
                    if reported_processor_name.is_none() {
                        stats.num_execution_starts += 1;
                    }
                    match processor_context.mode() {
                        ComponentProcessingMode::Build => {
                            if touched_previous_states {
                                stats.num_reprocesses += 1;
                            } else {
                                stats.num_adds += 1;
                            }
                        }
                        ComponentProcessingMode::Delete => {
                            stats.num_deletes += 1;
                        }
                    }
                });
                Ok((children_outcome, build_output))
            }
            Err(err) => {
                processing_stats.update(final_processor_name, |stats| {
                    if reported_processor_name.is_none() {
                        stats.num_execution_starts += 1;
                    }
                    stats.num_errors += 1;
                });
                Err(err)
            }
        }
    }

    pub fn new_processor_context_for_build(
        &self,
        parent_ctx: Option<&ComponentProcessorContext<Prof>>,
        processing_stats: ProcessingStats,
        full_reprocess: bool,
        live: bool,
        host_ctx: Arc<Prof::HostCtx>,
    ) -> Result<ComponentProcessorContext<Prof>> {
        let providers = if let Some(parent_ctx) = parent_ctx {
            let sub_path = self
                .stable_path()
                .as_ref()
                .strip_parent(parent_ctx.stable_path().as_ref())?;
            parent_ctx.update_building_state(|building_state| {
                building_state
                    .child_path_set
                    .add_child(sub_path, StablePathSet::Component)?;
                Ok(building_state
                    .target_states
                    .provider_registry
                    .providers
                    .clone())
            })?
        } else {
            self.app_ctx()
                .env()
                .target_states_providers()
                .lock()
                .unwrap()
                .providers
                .clone()
        };
        Ok(ComponentProcessorContext::new(
            self.clone(),
            parent_ctx.cloned(),
            processing_stats,
            host_ctx,
            ComponentProcessingAction::new_build(providers, full_reprocess, live),
        ))
    }

    pub fn new_processor_context_for_delete(
        &self,
        providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
        parent_ctx: Option<&ComponentProcessorContext<Prof>>,
        processing_stats: ProcessingStats,
        host_ctx: Arc<Prof::HostCtx>,
    ) -> ComponentProcessorContext<Prof> {
        ComponentProcessorContext::new(
            self.clone(),
            parent_ctx.cloned(),
            processing_stats,
            host_ctx,
            ComponentProcessingAction::Delete(ComponentDeleteContext { providers }),
        )
    }
}
