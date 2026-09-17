use crate::engine::runtime::get_runtime;
use crate::prelude::*;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Weak;

use crate::engine::context::FnCallContext;
use crate::engine::context::{
    AppContext, ComponentDeleteContext, ComponentProcessingAction, ComponentProcessingMode,
    ComponentProcessorContext, MemoStatesPayload, PreviewActionCollector,
};
use crate::engine::deadline::DeadlineContext;
use crate::engine::execution::{
    cleanup_tombstone, eager_existence_upsert, post_submit_for_build, submit,
    update_component_memo_states, use_or_invalidate_component_memoization,
};
use crate::engine::profile::EngineProfile;
use crate::engine::stats::ProcessingStats;
use crate::engine::target_state::{TargetStateProvider, TargetStateProviderRegistry};
use crate::state::stable_path::{StablePath, StablePathRef};
use crate::state::stable_path_set::StablePathSet;
use crate::state::target_state_path::{TargetProviderDeps, TargetStatePath};
use cocoindex_utils::error::{SharedError, SharedResult, SharedResultExt};
use cocoindex_utils::fingerprint::Fingerprint;

/// Async on-error callback for background-style component execution.
///
/// Invoked by `run_in_background` / `delete` when the spawned task fails
/// (other than via cancellation, which is filtered). The callback can
/// either:
///
/// - Return `Ok(())` to swallow the failure (mount-style; the spawned
///   task returns Ok and `handle.ready()` resolves Ok). This is what
///   the Python-side exception handler chain does when at least one
///   handler returns normally.
/// - Return `Err(err)` to propagate the failure (the spawned task
///   returns Err and `handle.ready()` raises). This is what the chain
///   does when every handler re-raises, and what `app.drop()`'s
///   built-in raising handler does to surface root-delete failures.
///
/// Cancellation is never delivered to the handler — it's filtered
/// before this is invoked. The "no chain registered" case logs at
/// ERROR and swallows; only an explicitly-installed handler causes
/// propagation.
pub type OnError = Arc<
    dyn Fn(Error) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>
        + Send
        + Sync
        + 'static,
>;

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

    /// Serializes runs of this component. A run holds the permit from before
    /// its body starts until its memo is stored (see `execute_once`), so two
    /// runs never overlap, and the memo one run stores is in place before the
    /// next decides whether to execute its body.
    build_semaphore: tokio::sync::Semaphore,
    /// The memo most recently stored under `build_semaphore`, if any. A run
    /// that finds its own key here on acquiring the permit knows a same-key
    /// run just completed and stored its result, so it re-checks the memo
    /// instead of executing again — under `full_reprocess` only when that run
    /// belonged to the same operation (see `execute_once`).
    last_stored_memo: Mutex<Option<StoredMemo>>,

    /// Identity registry of child components, keyed by their full StablePath,
    /// so a re-mount of a path whose component is still referenced shares the
    /// same `ComponentInner` (and thus its `build_semaphore`). Weak: entries
    /// are removed by the child's Drop impl. This map says nothing about
    /// activity — see `active_ops`.
    ///
    /// `parking_lot::Mutex` (non-poisoning): the Drop impl below acquires this
    /// lock, and a poisoned `std::sync::Mutex` would cascade panics through
    /// every subsequent Drop on the same parent's children map.
    active_children: parking_lot::Mutex<HashMap<StablePath, Weak<ComponentInner<Prof>>>>,

    /// Shared state for a live component running at this path.
    /// `parking_lot::Mutex` (non-poisoning): symmetric with `active_children`,
    /// since cancel/drain paths can lock this from `Drop` as well.
    live_state:
        parking_lot::Mutex<Option<Arc<crate::engine::live_component::LiveComponentState<Prof>>>>,

    /// Number of processing tasks in flight in this component's subtree
    /// (itself and every descendant), maintained by [`ActivityGuard`]. This —
    /// not the reference count of `inner` — is what "active" means: anything
    /// may hold a `Component` or a processor context for as long as it likes
    /// (a host-language exception traceback, a stored handle) without keeping
    /// the component active.
    active_ops: std::sync::atomic::AtomicUsize,
    /// Signaled when `active_ops` drops to zero (see `wait_until_inactive`).
    inactive: tokio::sync::Notify,
}

/// Marks a processing task as in flight on a component for the guard's
/// lifetime. A task's entry point creates it before spawning the task and drops
/// it when the task ends — on every exit path, including cancellation and
/// panics — so activity is accounted for deterministically, independent of who
/// else holds the component.
///
/// Counts on the component and each of its ancestors, so a component is active
/// while anything in its subtree runs (`wait_until_inactive`, [`StatsGroup`]
/// member liveness).
pub(crate) struct ActivityGuard<Prof: EngineProfile> {
    component: Component<Prof>,
}

impl<Prof: EngineProfile> ActivityGuard<Prof> {
    pub(crate) fn new(component: Component<Prof>) -> Self {
        for inner in component.self_and_ancestors() {
            inner
                .active_ops
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        Self { component }
    }
}

/// What a processing task holds from its entry point until it ends
/// (see [`Component::start_task`]).
struct StartedTask<Prof: EngineProfile> {
    /// Keeps the component and its ancestors active; the task drops it right
    /// before resolving child readiness.
    activity: ActivityGuard<Prof>,
    /// Registration with the parent's readiness accumulator; `None` for the root.
    child_readiness: Option<ComponentBgChildReadinessChildGuard>,
}

impl<Prof: EngineProfile> Drop for ActivityGuard<Prof> {
    fn drop(&mut self) {
        for inner in self.component.self_and_ancestors() {
            if inner
                .active_ops
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
                == 1
            {
                inner.inactive.notify_waiters();
            }
        }
    }
}

impl<Prof: EngineProfile> Drop for ComponentInner<Prof> {
    fn drop(&mut self) {
        if let Some(parent) = &self.parent {
            // Identity check: only remove our own entry. A previous
            // `get_child(stable_path)` may have observed our `Weak` failing to
            // upgrade (strong_count hit zero) and inserted a *new* `Weak` at
            // the same key BEFORE this Drop ran. Removing by key alone would
            // erroneously delete the new entry. Compare the stored Weak's
            // pointer against `self` to remove only if the slot still
            // identifies us.
            let mut children = parent.inner.active_children.lock();
            if let Some(weak) = children.get(&self.stable_path)
                && std::ptr::eq(weak.as_ptr(), self as *const ComponentInner<Prof>)
            {
                children.remove(&self.stable_path);
            }
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
    target_provider_deps: TargetProviderDeps,
}

impl ComponentRunOutcome {
    fn exception() -> Self {
        Self {
            has_exception: true,
            ..Default::default()
        }
    }

    /// Outcome for a component that hit its memo cache: no exception, but it
    /// still reports the stored logic dependency set so a mounting parent's
    /// memo depends on this subtree — and likewise its target-provider deps, so
    /// a parent that later memo-hits (and therefore never mounts this child)
    /// still re-checks the provider generations this child declared against.
    fn reused(logic_deps: Vec<Fingerprint>, target_provider_deps: TargetProviderDeps) -> Self {
        Self {
            has_exception: false,
            logic_deps: logic_deps.into_iter().collect(),
            target_provider_deps,
        }
    }

    fn merge(&mut self, other: Self) {
        self.has_exception |= other.has_exception;
        self.logic_deps.extend(other.logic_deps);
        self.target_provider_deps.extend(other.target_provider_deps);
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
    pub(crate) fn resolve(self, outcome: ComponentRunOutcome) {
        self.resolve_result(Ok(outcome));
    }

    /// Like `resolve`, but propagates a full `SharedResult`: `Ok` merges the
    /// outcome (logic deps), `Err` fails the enclosing readiness. Used by a
    /// `StatsGroup` to forward its members' aggregate readiness to its parent.
    pub(crate) fn resolve_result(mut self, result: SharedResult<ComponentRunOutcome>) {
        {
            let mut state = self.readiness.state().lock().unwrap();
            state.remaining_count -= 1;
            state.maybe_set_readiness(Some(result), self.readiness.readiness());
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

/// A named, separate stats aggregation scope created by `coco.stats_group(...)`.
/// Components mounted within the scope report into `stats` (split out of the
/// enclosing aggregate), register their initial readiness into `readiness`, and
/// are tracked for liveness in `active_members`. Shared (`Arc`) between the
/// substituted context view (which pushes members) and the spawned
/// group-lifecycle task (which awaits readiness, then the members' inactivity).
pub(crate) struct StatsGroup<Prof: EngineProfile> {
    stats: ProcessingStats,
    readiness: ComponentBgChildReadiness,
    active_members: parking_lot::Mutex<Vec<Weak<ComponentInner<Prof>>>>,
}

impl<Prof: EngineProfile> StatsGroup<Prof> {
    pub(crate) fn new() -> Self {
        Self {
            stats: ProcessingStats::new(),
            readiness: ComponentBgChildReadiness::default(),
            active_members: parking_lot::Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn stats(&self) -> &ProcessingStats {
        &self.stats
    }

    pub(crate) fn readiness(&self) -> &ComponentBgChildReadiness {
        &self.readiness
    }

    /// Register a direct member for liveness tracking.
    pub(crate) fn push_member(&self, child: &Component<Prof>) {
        self.active_members.lock().push(child.downgrade_inner());
    }

    /// The most recently registered member that is still active, pruning
    /// finished members (inactive, or dropped) off the back as it scans. A
    /// member only becomes active again by being mounted again, which
    /// registers it again, so pruning is safe; each entry is removed at most
    /// once ⇒ amortized O(1) per registered member.
    fn last_active_member(&self) -> Option<Component<Prof>> {
        let mut members = self.active_members.lock();
        while let Some(last) = members.last() {
            if let Some(inner) = last.upgrade()
                && inner.active_ops.load(std::sync::atomic::Ordering::SeqCst) > 0
            {
                return Some(Component { inner });
            }
            members.pop();
        }
        None
    }

    /// Wait until no member (nor anything in a member's subtree) has a
    /// processing task in flight — the group's analogue of
    /// `Component::wait_until_inactive`.
    pub(crate) async fn wait_until_members_inactive(&self) {
        while let Some(member) = self.last_active_member() {
            member.wait_until_inactive().await;
        }
    }
}

impl<Prof: EngineProfile> ComponentProcessorContext<Prof> {
    /// Open a stats group rooted at this context. Returns a derived context view
    /// (whose mounts report into the group and register liveness with it) and
    /// the group's `ProcessingStats` (for the Python handle / watch).
    ///
    /// Spawns the group-lifecycle task, which mirrors the root's
    /// `notify_ready → wait_until_inactive → notify_terminated` flow but resolves
    /// the parent-readiness guard at READY (not at termination) so a live member
    /// can never deadlock the enclosing component's initial readiness.
    pub fn begin_stats_group(
        &self,
        title: String,
        report_to_stdout: bool,
        refresh_interval_secs: Option<f64>,
    ) -> (ComponentProcessorContext<Prof>, ProcessingStats) {
        let group = Arc::new(StatsGroup::new());
        let group_stats = group.stats().clone();

        // The group counts as one pending child of the enclosing readiness, so
        // the parent component (or outer group) waits for it as a unit.
        let parent_guard = self.components_readiness().clone().add_child();

        let cancel_token = self.app_ctx().cancellation_token();
        let live = self.live();
        let lifecycle_group = group.clone();
        get_runtime().spawn(async move {
            // Fires once registration is closed (`set_build_done` via
            // `end_stats_group`) AND every member reached initial readiness.
            let outcome = lifecycle_group.readiness().readiness().wait().await.clone();
            lifecycle_group.stats().notify_ready();
            // Propagate readiness/logic-deps upward AT READY — decoupled from
            // termination, matching the root (app.rs).
            parent_guard.resolve_result(outcome);

            if live && !cancel_token.is_cancelled() {
                // Cancel-aware wait for the group's members to finish (the
                // group's analogue of `wait_until_inactive`).
                tokio::select! {
                    () = lifecycle_group.wait_until_members_inactive() => {}
                    () = cancel_token.cancelled() => {}
                }
            }
            lifecycle_group.stats().notify_terminated();
        });

        if report_to_stdout {
            crate::engine::progress_display::spawn_group_plain_report(
                group_stats.clone(),
                title,
                live,
                refresh_interval_secs,
            );
        }

        (self.with_stats_group(&group), group_stats)
    }

    /// Close the group opened by `begin_stats_group` for member registration.
    /// Non-blocking — readiness then resolves once the members finish.
    pub fn end_stats_group(&self) {
        self.components_readiness().set_build_done();
    }
}

pub struct ComponentMountRunHandle<Prof: EngineProfile> {
    join_handle: tokio::task::JoinHandle<Result<ComponentBuildOutput<Prof>>>,
    /// The waiting caller's deadline, checked post-wait in `result()`
    /// (caller-attributed: the child's committed success is preserved).
    /// Root runs store NONE here — the root's post-update observation
    /// point is `AppOpHandle::result()`.
    caller_deadline: DeadlineContext,
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
                    building_state
                        .target_states
                        .provider_registry
                        .add(target_state_path, provider.clone())?;
                }
                Ok(())
            })?;
        }
        self.caller_deadline.check()?;
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

/// A memo stored by a run of a component under its `build_semaphore`: the
/// operation that stored it and the key it was stored under.
#[derive(Clone, Copy)]
struct StoredMemo {
    operation_generation: u64,
    memo_fp: Fingerprint,
}

/// Result of looking up a component's memo for the processor about to run.
enum MemoLookup<Prof: EngineProfile> {
    /// A valid memo stands in for a run: report the stored run's outcome and
    /// output.
    Reuse(ComponentRunOutcome, ComponentBuildOutput<Prof>),
    /// No usable memo. `revalidated_states` is `Some` when a memo matched the
    /// key but its memo states no longer validate: the states collected during
    /// validation are then stored with the new memo, instead of being
    /// collected again once the body has run.
    Miss {
        revalidated_states: Option<MemoStatesPayload<Prof>>,
    },
}

/// How the permit-guarded part of `execute_once` ended.
enum GuardedRun<Prof: EngineProfile> {
    /// The body did not run: a run with the same memo key completed under the
    /// permit while this one waited for it, and its memo was reused.
    Reused(ComponentRunOutcome, ComponentBuildOutput<Prof>),
    /// The body ran (build mode), or the component was deleted (delete mode).
    Executed {
        children_outcome: ComponentRunOutcome,
        build_output: Option<ComponentBuildOutput<Prof>>,
        touched_previous_states: bool,
    },
}

/// Look up the memo stored for `comp_ctx`'s component and, when `processor`
/// has a memo state handler, validate the stored memo states through it. A
/// stored memo whose key is not `memo_fp` — or any stored memo, when the
/// processor is not memoized (`memo_fp` is `None`) — is invalidated. A failure
/// to read or decode the memo is logged and counts as a miss; a failure in the
/// state handler propagates.
async fn lookup_component_memo<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    processor: &Prof::ComponentProc,
    memo_fp: Option<Fingerprint>,
) -> Result<MemoLookup<Prof>> {
    let memo = match use_or_invalidate_component_memoization(comp_ctx, memo_fp).await {
        Ok(memo) => memo,
        Err(err) => {
            error!("component memoization restore failed: {err:?}");
            None
        }
    };
    let Some((ret, memo_states, stored_logic_deps, stored_provider_deps)) = memo else {
        return Ok(MemoLookup::Miss {
            revalidated_states: None,
        });
    };
    if processor.has_memo_state_handler() && !memo_states.is_empty() {
        let fut = processor.handle_memo_states(
            comp_ctx.app_ctx().env().host_runtime_ctx(),
            comp_ctx,
            Some(memo_states),
        )?;
        let (new_states, can_reuse, states_changed) = fut.await?;
        if !can_reuse {
            return Ok(MemoLookup::Miss {
                revalidated_states: Some(new_states),
            });
        }
        // Reusable, but the states themselves moved (e.g. an mtime changed
        // while the content hash did not): refresh them in the stored memo.
        if states_changed {
            update_component_memo_states(comp_ctx, &new_states).await?;
        }
    }
    // Report the stored dependency sets upward even on a memo hit, so a
    // mounting parent's memo depends on this whole subtree (see
    // `merge_logic_deps` in `execute_once`).
    Ok(MemoLookup::Reuse(
        ComponentRunOutcome::reused(stored_logic_deps, stored_provider_deps),
        ComponentBuildOutput {
            ret,
            built_target_states_providers: Default::default(),
        },
    ))
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
                last_stored_memo: Mutex::new(None),
                active_children: parking_lot::Mutex::new(HashMap::new()),
                live_state: parking_lot::Mutex::new(None),
                active_ops: std::sync::atomic::AtomicUsize::new(0),
                inactive: tokio::sync::Notify::new(),
            }),
        }
    }

    fn self_and_ancestors(&self) -> impl Iterator<Item = &ComponentInner<Prof>> {
        std::iter::successors(Some(&*self.inner), |inner| {
            inner.parent.as_ref().map(|parent| &*parent.inner)
        })
    }

    /// Begin a processing task on this component: count it as in flight, then
    /// register it with the parent view — as a pending child for readiness and
    /// as a member of every enclosing stats group. Activity is counted first,
    /// so a member is never registered inactive (a concurrent group scan would
    /// prune it before its task had started); readiness and membership are
    /// registered together, so a group can never see a member become ready
    /// without also tracking its activity.
    fn start_task(&self, context: &ComponentProcessorContext<Prof>) -> StartedTask<Prof> {
        let activity = ActivityGuard::new(self.clone());
        let child_readiness = context.parent_context().map(|parent_ctx| {
            parent_ctx.push_active_member(self);
            parent_ctx.components_readiness().clone().add_child()
        });
        StartedTask {
            activity,
            child_readiness,
        }
    }

    pub fn mount_child(&self, fn_ctx: &FnCallContext, stable_path: StablePath) -> Result<Self> {
        fn_ctx.update(|inner| inner.has_child_components = true);
        Ok(self.get_child(stable_path))
    }

    /// Mount and run a child in the foreground (use_mount path).
    /// Inherits live from the parent context.
    ///
    /// `deadline` is the deadline for the CHILD, taken from the caller's
    /// current scope, and cannot be read from `parent_ctx.deadline`: that is
    /// the caller's own base, frozen at the caller's mount, while narrowing
    /// (`with coco.timeout(...)`) lives in the SDK's per-task carrier —
    /// concurrent tasks within one component can hold different narrowed
    /// scopes at the same moment, so the current value must travel with each
    /// call. It is threaded through the child's execution checkpoints and
    /// captured by the returned handle for the post-wait check; it is never
    /// stored on the ctx (the deadline only ever travels, never rests).
    pub async fn use_mount(
        self,
        parent_ctx: &ComponentProcessorContext<Prof>,
        processor: Prof::ComponentProc,
        deadline: DeadlineContext,
    ) -> Result<ComponentMountRunHandle<Prof>> {
        let child_ctx = self.new_processor_context_for_build(
            Some(parent_ctx),
            parent_ctx.processing_stats().clone(),
            parent_ctx.full_reprocess(),
            parent_ctx.live(), // use_mount inherits live from parent
            parent_ctx.preview_collector().cloned(),
            parent_ctx.host_ctx().clone(),
            // No build-mode on_error: use_mount is foreground; failures
            // propagate as `Err` to the awaiting parent via `.result()`.
            // Orphan-delete failures during this child's commit fall
            // through to the framework's default `error!` log.
            None,
        )?;
        self.run(processor, child_ctx, deadline, deadline).await
    }

    /// Mount and run a child in the background (mount path).
    /// Inherits live from the parent context.
    pub async fn mount(
        self,
        parent_ctx: &ComponentProcessorContext<Prof>,
        processor: Prof::ComponentProc,
        on_error: Option<OnError>,
        pre_execute_check: Option<Box<dyn FnOnce() -> bool + Send>>,
    ) -> Result<ComponentExecutionHandle> {
        // Store `on_error` on the child's build context too, so the
        // commit-phase GC sweep can cascade it to orphan deletes. The
        // same handler is also passed to `run_in_background` for the
        // child's own task failure — one handler, two surfaces.
        let child_ctx = self.new_processor_context_for_build(
            Some(parent_ctx),
            parent_ctx.processing_stats().clone(),
            parent_ctx.full_reprocess(),
            parent_ctx.live(), // mount inherits live from parent
            parent_ctx.preview_collector().cloned(),
            parent_ctx.host_ctx().clone(),
            on_error.clone(),
        )?;
        self.run_in_background(processor, child_ctx, on_error, pre_execute_check)
            .await
    }

    pub fn get_child(&self, stable_path: StablePath) -> Self {
        let mut children = self.inner.active_children.lock();
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

    /// A `Weak` to this component's inner, for membership tracking by a
    /// [`StatsGroup`]; the member counts as active while `active_ops > 0`.
    fn downgrade_inner(&self) -> Weak<ComponentInner<Prof>> {
        Arc::downgrade(&self.inner)
    }

    pub fn set_live_state(
        &self,
        state: Arc<crate::engine::live_component::LiveComponentState<Prof>>,
    ) {
        *self.inner.live_state.lock() = Some(state);
    }

    pub fn live_state(
        &self,
    ) -> Option<Arc<crate::engine::live_component::LiveComponentState<Prof>>> {
        self.inner.live_state.lock().clone()
    }

    /// True while a processing task is in flight on this component or any
    /// descendant (see [`ActivityGuard`]).
    pub fn is_active(&self) -> bool {
        self.inner
            .active_ops
            .load(std::sync::atomic::Ordering::SeqCst)
            > 0
    }

    /// Wait until no processing task is in flight in this component's
    /// subtree. Event-driven: returns as soon as the last [`ActivityGuard`]
    /// in the subtree drops.
    pub async fn wait_until_inactive(&self) {
        loop {
            // Create the future before checking: a `Notified` receives
            // `notify_waiters` from the moment it exists, so a guard dropped
            // between the check and the await still wakes us.
            let inactive = self.inner.inactive.notified();
            if !self.is_active() {
                return;
            }
            inactive.await;
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

    /// `deadline` governs this component's own execution checkpoints;
    /// `caller_deadline` is stored in the returned handle for the post-wait
    /// check. They coincide for use_mount; the root passes NONE as
    /// `caller_deadline` since AppOpHandle owns the root's post-result check.
    pub(crate) async fn run(
        self,
        processor: Prof::ComponentProc,
        context: ComponentProcessorContext<Prof>,
        deadline: DeadlineContext,
        caller_deadline: DeadlineContext,
    ) -> Result<ComponentMountRunHandle<Prof>> {
        let StartedTask {
            activity,
            child_readiness: child_readiness_guard,
        } = self.start_task(&context);

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
        let span = info_span!("component.run", component_path = %relative_path);
        let cancel_token = self.app_ctx().cancellation_token();
        let join_handle = get_runtime().spawn(
            async move {
                // Race the work against app-level cancellation. On cancel, the
                // work future is dropped, which cascades drop into from_py_future
                // → CancelOnDropPy and cancels the underlying Python task.
                let result = tokio::select! {
                    r = self.execute_once(&context, Some(&processor), deadline) => r,
                    _ = cancel_token.cancelled() => Err(internal_error!("operation cancelled")),
                };
                let (outcome, output) = match result {
                    Ok((outcome, output)) => (outcome, Ok(output)),
                    Err(err) => (ComponentRunOutcome::exception(), Err(err)),
                };
                context.release_inflight_permit();
                drop(processor);
                drop(context);
                drop(self);
                // Mark the task over before readiness resolves, so whoever
                // observes readiness also observes the component as inactive.
                drop(activity);
                child_readiness_guard.map(|guard| guard.resolve(outcome));
                output?
                    .ok_or_else(|| internal_error!("component deletion can only run in background"))
            }
            .instrument(span),
        );
        Ok(ComponentMountRunHandle {
            join_handle,
            caller_deadline,
        })
    }

    pub(crate) async fn run_in_background(
        self,
        processor: Prof::ComponentProc,
        context: ComponentProcessorContext<Prof>,
        on_error: Option<OnError>,
        pre_execute_check: Option<Box<dyn FnOnce() -> bool + Send>>,
    ) -> Result<ComponentExecutionHandle> {
        // TODO: Skip building and reuse cached result if the component is already built and up to date.
        let StartedTask {
            activity,
            child_readiness: child_readiness_guard,
        } = self.start_task(&context);

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

        let cancel_token = self.app_ctx().cancellation_token();
        let join_handle = get_runtime().spawn(async move {
            // Check if this task has been superseded before executing.
            if let Some(check) = pre_execute_check {
                if !check() {
                    // Superseded — skip execution, resolve as success.
                    context.release_inflight_permit();
                    drop(processor);
                    drop(context);
                    drop(self);
                    drop(activity);
                    if let Some(guard) = child_readiness_guard {
                        guard.resolve(ComponentRunOutcome::default());
                    }
                    return Ok(());
                }
            }
            // Race the work against app-level cancellation. On cancel, the
            // work future is dropped, which cascades drop into from_py_future
            // → CancelOnDropPy and cancels the underlying Python task.
            let result = tokio::select! {
                // Background components are deadline-isolated by design.
                r = self.execute_once(&context, Some(&processor), DeadlineContext::NONE) => r,
                _ = cancel_token.cancelled() => Err(internal_error!("operation cancelled")),
            };
            // Background-style error handling:
            // - Cancellation is always swallowed (no handler call, no
            //   propagation) — Ctrl+C / shutdown / re-mount shouldn't
            //   surface as a user-visible error.
            // - With a handler registered: invoke it. The handler's
            //   Result decides propagation — Ok = swallow (mount-style),
            //   Err = propagate via task_result. This lets the Python
            //   exception handler chain control propagation: handlers
            //   that return normally → swallow; chain exhausted via
            //   raises → propagate.
            // - No handler: log at ERROR, swallow. Matches the existing
            //   "no chain registered → not propagated" contract.
            let (outcome, task_result) = match result {
                Ok((outcome, _)) => (outcome, Ok(())),
                Err(err) => {
                    let task_result = if cancel_token.is_cancelled() || err.is_cancelled() {
                        trace!("component build cancelled");
                        Ok(())
                    } else if let Some(handler) = &on_error {
                        match handler(err).await {
                            Ok(()) => Ok(()),
                            Err(propagated) => Err(SharedError::from(propagated)),
                        }
                    } else {
                        error!("component build failed:\n{err:?}");
                        Ok(())
                    };
                    (ComponentRunOutcome::exception(), task_result)
                }
            };
            context.release_inflight_permit();
            drop(processor);
            drop(context);
            drop(self);
            // See `run` for why this precedes readiness resolution.
            drop(activity);
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

    pub fn delete(
        self,
        context: ComponentProcessorContext<Prof>,
        pre_execute_check: Option<Box<dyn FnOnce() -> bool + Send>>,
    ) -> Result<ComponentExecutionHandle> {
        let StartedTask {
            activity,
            child_readiness: child_readiness_guard,
        } = self.start_task(&context);
        // Pull on_error out of the delete context so the spawned task
        // can invoke it. The context still carries the same handler for
        // descendant GC sweeps to read and cascade.
        let on_error = context.delete_action_on_error();
        let join_handle: tokio::task::JoinHandle<SharedResult<()>> =
            get_runtime().spawn(async move {
                if let Some(check) = pre_execute_check {
                    if !check() {
                        drop(context);
                        drop(self);
                        drop(activity);
                        if let Some(guard) = child_readiness_guard {
                            guard.resolve(ComponentRunOutcome::default());
                        }
                        return Ok(());
                    }
                }
                trace!("deleting component at {}", self.stable_path());
                // Delete/GC runs must never be deadline-bounded.
                let result = self
                    .execute_once(&context, None, DeadlineContext::NONE)
                    .await;
                // Same error model as `run_in_background`: cancellation
                // filtered; with-handler delegates propagation to the
                // handler's Result (Ok = swallow, Err = propagate);
                // without-handler logs + swallow.
                let (outcome, task_result) = match result {
                    Ok((outcome, _)) => (outcome, Ok(())),
                    Err(err) => {
                        let task_result = if err.is_cancelled() {
                            trace!("component delete cancelled");
                            Ok(())
                        } else if let Some(handler) = &on_error {
                            match handler(err).await {
                                Ok(()) => Ok(()),
                                Err(propagated) => Err(SharedError::from(propagated)),
                            }
                        } else {
                            error!("component delete failed:\n{err:?}");
                            Ok(())
                        };
                        (ComponentRunOutcome::exception(), task_result)
                    }
                };
                // Drop profile-specific objects BEFORE resolving child readiness.
                // See run_in_background for the rationale (PyGILState finalization fix).
                drop(context);
                drop(self);
                // See `run` for why this precedes readiness resolution.
                drop(activity);
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
        deadline: DeadlineContext,
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
            deadline.check()?;

            // Fast-path: component memoization check does not require acquiring the build permit.
            // If it hits, we can immediately return without processing/submitting/waiting.
            // Under `full_reprocess` a stored memo may only be reused when this very
            // operation stored it, which only the permit-holding re-check below can
            // tell, so the fast-path is skipped.
            if !processor_context.full_reprocess() {
                match lookup_component_memo(processor_context, processor, memo_fp_to_store).await? {
                    MemoLookup::Reuse(outcome, output) => {
                        processing_stats.update(processor_name, |stats| {
                            stats.num_execution_starts += 1;
                            stats.num_unchanged += 1;
                        });
                        return Ok((outcome, Some(output)));
                    }
                    MemoLookup::Miss { revalidated_states } => {
                        memo_states_for_store = revalidated_states;
                    }
                }
            }

            processing_stats.update(processor_name, |stats| {
                stats.num_execution_starts += 1;
            });
            reported_processor_name = Some(Cow::Borrowed(processor_name));
        }

        let result = {
            let reported_processor_name = &mut reported_processor_name;
            async move {
                // The permit is held until the memo is stored (or, in delete
                // mode, the tombstone cleaned up), not only across `process()`
                // and `submit()`: a run queued behind this one must find the
                // stored memo once it gets the permit, so it can reuse it
                // instead of executing again.
                let _permit = self.inner.build_semaphore.acquire().await?;

                // A run with the same memo key completed under the permit while
                // this one waited for it — e.g. two `App::update` calls on one
                // app that both missed the fast-path above before either had
                // stored a memo. Re-check the memo now; a failed run stores
                // none (and records none), so a miss falls through to
                // executing. Under `full_reprocess` only a memo stored by this
                // same operation qualifies: that is the operation's own
                // execution of the component, not a cache from a previous run.
                let last_stored_memo = *self.inner.last_stored_memo.lock().unwrap();
                if let Some(processor) = processor
                    && let Some(memo_fp) = memo_fp_to_store
                    && let Some(stored) = last_stored_memo
                    && stored.memo_fp == memo_fp
                    && (!processor_context.full_reprocess()
                        || stored.operation_generation == processor_context.operation_generation())
                {
                    match lookup_component_memo(processor_context, processor, memo_fp_to_store)
                        .await?
                    {
                        MemoLookup::Reuse(outcome, output) => {
                            return Ok(GuardedRun::Reused(outcome, output));
                        }
                        MemoLookup::Miss { revalidated_states } => {
                            memo_states_for_store = revalidated_states;
                        }
                    }
                }

                // Build mode only: write the component's own existence bit
                // (and ancestor chain) into the parent in its own txn,
                // before the user processor runs. Maintains the invariant
                // that existence ⊇ tracked state and eliminates the
                // dual-writer conflict with the parent's commit-time
                // existence reconciliation. See `internal_states.md` §3.1.
                if processor_context.mode() == ComponentProcessingMode::Build
                    && !processor_context.preview()
                {
                    eager_existence_upsert(processor_context).await?;
                }

                // Eagerly load all function-memo and user-state entries for
                // this component into the per-build cache (one read txn), so
                // every subsequent fn-call probe and `use_state` serves from
                // memory. Skipped under `full_reprocess` and in delete mode
                // (no `ComponentBuildingState`); see the cache flush logic
                // for how those cases are handled at commit time.
                processor_context.prefetch_states().await?;

                // The earlier deadline check guards memo lookup. A component can still
                // spend time waiting for the build semaphore, existence upsert, or state
                // prefetch before the user body starts, so check again at the actual
                // processor-entry boundary.
                deadline.check()?;

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

                // Wait until children components ready before submitting this
                // component's target states and child-existence reconciliation.
                let components_readiness = processor_context.components_readiness();
                components_readiness.set_build_done();
                let mut children_outcome = components_readiness
                    .readiness()
                    .wait()
                    .await
                    .clone()
                    .into_result()?;

                // Merge children's logic deps into this component's context. The
                // full set (own fp ∪ all descendants) is taken once after
                // memo-state collection below and used for both this component's
                // own memo and the outcome reported to its parent.
                processor_context
                    .merge_logic_deps(std::mem::take(&mut children_outcome.logic_deps));
                processor_context.merge_target_provider_deps(std::mem::take(
                    &mut children_outcome.target_provider_deps,
                ));

                let ret = ret?;
                deadline.check()?;
                let submit_output = submit(processor_context, processor, |name| {
                    if reported_processor_name.is_none() {
                        processing_stats.update(&name, |stats| {
                            stats.num_execution_starts += 1;
                        });
                        *reported_processor_name = Some(Cow::Owned(name.to_string()));
                    }
                })
                .await?;

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

                            let comp_memo = memo_fp_to_store.map(|fp| (fp, &ret, &memo_states));
                            // Take the full dependency set once (O(1) move). It
                            // must run after the memo-state collection above, which
                            // reads the set via `collect_context_initial_states`.
                            // Serves both this component's own memo (sorted inside
                            // `post_submit_for_build`, only when memoizing) and the
                            // outcome reported to the parent across the mount
                            // boundary — so the parent's memo depends on this whole
                            // subtree's logic.
                            let logic_deps = processor_context.take_logic_deps();
                            let target_provider_deps =
                                processor_context.take_target_provider_deps();
                            post_submit_for_build(
                                processor_context,
                                comp_memo,
                                &logic_deps,
                                &target_provider_deps,
                            )
                            .await?;
                            // Record the store for a run queued on the permit
                            // (see the re-check above). Still under the permit,
                            // so this is the component's latest memo.
                            if let Some(memo_fp) = memo_fp_to_store {
                                *self.inner.last_stored_memo.lock().unwrap() = Some(StoredMemo {
                                    operation_generation: processor_context.operation_generation(),
                                    memo_fp,
                                });
                            }
                            children_outcome.logic_deps = logic_deps;
                            children_outcome.target_provider_deps = target_provider_deps;
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
                        // Delete path. When any descendant delete failed,
                        // skip `cleanup_tombstone` (symmetric with the
                        // build branch skipping `post_submit_for_build`)
                        // — that preserves the tombstone for the next
                        // reconcile to retry.
                        //
                        // We do NOT propagate via `Err` from here.
                        // Descendant-failure propagation to awaiting
                        // callers (notably `App.drop()`) happens via
                        // the cascading `on_error` plumbed through the
                        // GC sweep — see `execution.rs::launch_child_component_gc`.
                        // That's the single, unified error-handling
                        // channel; this branch just preserves metadata.
                        if !children_outcome.has_exception {
                            cleanup_tombstone(&processor_context).await?;
                        }
                        None
                    }
                };
                Ok::<_, Error>(GuardedRun::Executed {
                    children_outcome,
                    build_output,
                    touched_previous_states: submit_output.touched_previous_states,
                })
            }
            .await
        };

        let final_processor_name = reported_processor_name
            .as_ref()
            .map(|s| s.as_ref())
            .unwrap_or(db_schema::UNKNOWN_PROCESSOR_NAME);
        match result {
            Ok(GuardedRun::Reused(outcome, output)) => {
                // The execution start was counted before the permit; the run
                // ends like a fast-path memo hit.
                processing_stats.update(final_processor_name, |stats| {
                    stats.num_unchanged += 1;
                });
                Ok((outcome, Some(output)))
            }
            Ok(GuardedRun::Executed {
                children_outcome,
                build_output,
                touched_previous_states,
            }) => {
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
        preview_collector: Option<PreviewActionCollector<Prof>>,
        host_ctx: Arc<Prof::HostCtx>,
        on_error: Option<OnError>,
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
            ComponentProcessingAction::new_build(
                providers,
                full_reprocess,
                live,
                on_error,
                preview_collector,
            ),
        ))
    }

    pub fn new_processor_context_for_delete(
        &self,
        providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
        parent_ctx: Option<&ComponentProcessorContext<Prof>>,
        processing_stats: ProcessingStats,
        host_ctx: Arc<Prof::HostCtx>,
        on_error: Option<OnError>,
    ) -> ComponentProcessorContext<Prof> {
        ComponentProcessorContext::new(
            self.clone(),
            parent_ctx.cloned(),
            processing_stats,
            host_ctx,
            ComponentProcessingAction::Delete(ComponentDeleteContext {
                providers,
                on_error,
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{ActivityGuard, Component, ComponentProcessor, ComponentProcessorInfo, StatsGroup};
    use crate::engine::app::{App, AppUpdateOptions};
    use crate::engine::context::{
        ComponentProcessingAction, ComponentProcessorContext, FnCallContext, MemoStatesPayload,
    };
    use crate::engine::deadline::{
        DeadlineContext, testing_advance_deadline_clock, testing_deadline_clock_lock,
        testing_disable_deadline_clock, testing_reset_deadline_clock,
    };
    use crate::engine::environment::Environment;
    use crate::engine::profile::{EngineProfile, Persist};
    use crate::engine::target_state::{
        TargetActionSink, TargetActionWithChildSlot, TargetHandler, TargetReconcileOutput,
        TargetStateProviderRegistry,
    };
    use crate::state::stable_path::{StableKey, StablePath};
    use crate::state_store::StorageSettings;
    use async_trait::async_trait;
    use cocoindex_utils::fingerprint::Fingerprint;
    use std::hash::{Hash, Hasher};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn key_path(parent: &Component<TestProfile>, key: &str) -> StablePath {
        parent
            .stable_path()
            .concat_part(StableKey::Str(Arc::from(key)))
    }

    /// Activity is explicit: a task's `ActivityGuard` counts on the component
    /// and its ancestors, and dropping it wakes `wait_until_inactive`. Holding
    /// `Component`s has no effect on either.
    #[tokio::test]
    async fn activity_propagates_up_the_parent_chain_and_wakes_waiters() {
        let (app, _dir) = test_app("activity_guard").await;
        let root = Component::new(app.app_ctx().clone(), StablePath::root(), None);
        let child = root.get_child(key_path(&root, "child"));
        let grandchild = child.get_child(key_path(&child, "grandchild"));
        let chain = [&root, &child, &grandchild];
        assert!(!chain.iter().any(|c| c.is_active()));

        let activity = ActivityGuard::new(grandchild.clone());
        assert!(chain.iter().all(|c| c.is_active()));

        let waiter = tokio::spawn({
            let root = root.clone();
            async move { root.wait_until_inactive().await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !waiter.is_finished(),
            "must keep waiting while the grandchild's task is in flight"
        );
        drop(activity);
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("wait_until_inactive must wake when the last task ends")
            .unwrap();
        assert!(!chain.iter().any(|c| c.is_active()));
    }

    #[tokio::test]
    async fn stats_group_prunes_finished_members_even_while_still_referenced() {
        let (app, _dir) = test_app("stats_group_members").await;
        let root = Component::new(app.app_ctx().clone(), StablePath::root(), None);
        let member = root.get_child(key_path(&root, "member"));
        let group = StatsGroup::<TestProfile>::new();

        let activity = ActivityGuard::new(member.clone());
        group.push_member(&member);
        assert!(group.last_active_member().is_some());

        drop(activity);
        // `member` is still held (as a retained host-language context would
        // hold it), yet it is finished: pruned, nothing left to wait for.
        assert!(group.last_active_member().is_none());
        assert!(group.active_members.lock().is_empty());
        group.wait_until_members_inactive().await;
    }

    #[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
    struct TestProfile;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct TestData(Vec<u8>);

    impl Persist for TestData {
        fn to_bytes(&self) -> crate::prelude::Result<bytes::Bytes> {
            Ok(bytes::Bytes::from(self.0.clone()))
        }

        fn from_bytes(data: &[u8]) -> crate::prelude::Result<Self> {
            Ok(Self(data.to_vec()))
        }
    }

    struct NoopSink;

    #[async_trait]
    impl TargetActionSink<TestProfile> for NoopSink {
        async fn apply(
            &self,
            _host_runtime_ctx: &(),
            _host_ctx: Arc<()>,
            _actions: &[TargetActionWithChildSlot<TestProfile>],
        ) -> crate::prelude::Result<()> {
            Ok(())
        }
    }

    struct NoopHandler;

    impl TargetHandler<TestProfile> for NoopHandler {
        fn reconcile(
            &self,
            _key: StableKey,
            _desired_target_state: Option<&()>,
            _prev_possible_records: &[TestData],
            _prev_may_be_missing: bool,
        ) -> crate::prelude::Result<Option<TargetReconcileOutput<TestProfile>>> {
            Ok(None)
        }
    }

    impl Hash for TestProcessor {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.memo_fp.hash(state);
        }
    }

    impl PartialEq for TestProcessor {
        fn eq(&self, other: &Self) -> bool {
            self.memo_fp == other.memo_fp
        }
    }

    impl Eq for TestProcessor {}

    impl EngineProfile for TestProfile {
        type HostRuntimeCtx = ();
        type HostCtx = ();
        type ComponentProc = TestProcessor;
        type FunctionData = TestData;
        type TargetHdl = NoopHandler;
        type TargetStateTrackingRecord = TestData;
        type TargetAction = ();
        type TargetActionSink = NoopSink;
        type TargetStateValue = ();
    }

    /// Optional processor body, given the component's processor context.
    type ProcessHook = Arc<
        dyn Fn(
                ComponentProcessorContext<TestProfile>,
            )
                -> Pin<Box<dyn Future<Output = crate::prelude::Result<TestData>> + Send>>
            + Send
            + Sync,
    >;

    struct TestProcessor {
        info: ComponentProcessorInfo,
        memo_fp: Fingerprint,
        body_started: Arc<AtomicBool>,
        advance_clock_in_state_handler: bool,
        /// What the memo state handler reports as `can_reuse` on a memo hit.
        memo_states_reusable: bool,
        on_process: Option<ProcessHook>,
    }

    impl TestProcessor {
        fn new(
            name: &str,
            memo_fp: Fingerprint,
            body_started: Arc<AtomicBool>,
            advance_clock_in_state_handler: bool,
        ) -> Self {
            Self {
                info: ComponentProcessorInfo::new(name.to_string()),
                memo_fp,
                body_started,
                advance_clock_in_state_handler,
                memo_states_reusable: false,
                on_process: None,
            }
        }

        fn with_reusable_memo_states(mut self) -> Self {
            self.memo_states_reusable = true;
            self
        }

        fn with_on_process(mut self, hook: ProcessHook) -> Self {
            self.on_process = Some(hook);
            self
        }
    }

    impl ComponentProcessor<TestProfile> for TestProcessor {
        fn process(
            &self,
            _host_runtime_ctx: &(),
            comp_ctx: &ComponentProcessorContext<TestProfile>,
        ) -> crate::prelude::Result<
            impl Future<Output = crate::prelude::Result<TestData>> + Send + 'static,
        > {
            let body_started = self.body_started.clone();
            let hook = self
                .on_process
                .as_ref()
                .map(|hook| (hook.clone(), comp_ctx.clone()));
            Ok(async move {
                body_started.store(true, Ordering::SeqCst);
                match hook {
                    Some((hook, comp_ctx)) => hook(comp_ctx).await,
                    None => Ok(TestData(b"ret".to_vec())),
                }
            })
        }

        fn memo_key_fingerprint(&self) -> Option<Fingerprint> {
            Some(self.memo_fp)
        }

        fn processor_info(&self) -> &ComponentProcessorInfo {
            &self.info
        }

        fn has_memo_state_handler(&self) -> bool {
            true
        }

        fn handle_memo_states(
            &self,
            _host_runtime_ctx: &(),
            _comp_ctx: &ComponentProcessorContext<TestProfile>,
            _stored_states: Option<MemoStatesPayload<TestProfile>>,
        ) -> crate::prelude::Result<
            impl Future<Output = crate::prelude::Result<(MemoStatesPayload<TestProfile>, bool, bool)>>
            + Send
            + 'static,
        > {
            let advance_clock = self.advance_clock_in_state_handler;
            let reusable = self.memo_states_reusable;
            Ok(async move {
                if advance_clock {
                    testing_advance_deadline_clock(Duration::from_secs(2));
                }
                Ok((
                    MemoStatesPayload {
                        positional: vec![TestData(b"state".to_vec())],
                        by_context_fp: Vec::new(),
                    },
                    reusable,
                    false,
                ))
            })
        }
    }

    struct TestClockGuard {
        _guard: std::sync::MutexGuard<'static, ()>,
    }

    impl TestClockGuard {
        fn new() -> Self {
            let guard = testing_deadline_clock_lock();
            testing_reset_deadline_clock();
            Self { _guard: guard }
        }
    }

    impl Drop for TestClockGuard {
        fn drop(&mut self) {
            testing_disable_deadline_clock();
        }
    }

    async fn test_app(name: &str) -> (App<TestProfile>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let settings = StorageSettings {
            db_path: dir.path().join("lmdb"),
            lmdb_max_dbs: 64,
            lmdb_map_size: 1 << 24,
        };
        let providers = Arc::new(Mutex::new(TargetStateProviderRegistry::new(
            Default::default(),
        )));
        let env = Environment::<TestProfile>::new(settings, providers, ())
            .await
            .unwrap();
        let app = App::new(name, env, None).await.unwrap();
        (app, dir)
    }

    #[tokio::test]
    async fn deadline_rechecked_after_memo_state_handler_before_processor_body() {
        let _clock = TestClockGuard::new();
        let memo_fp = Fingerprint::from(&"processor-entry-deadline").unwrap();
        let (app, _dir) = test_app("deadline_pre_body").await;

        let first_body_started = Arc::new(AtomicBool::new(false));
        let first_processor = TestProcessor::new(
            "deadline_pre_body",
            memo_fp,
            first_body_started.clone(),
            false,
        );
        let (handle, _) = app
            .update(
                first_processor,
                AppUpdateOptions::default(),
                Arc::new(()),
                None,
            )
            .unwrap();
        handle.result().await.unwrap();
        assert!(first_body_started.load(Ordering::SeqCst));

        let second_body_started = Arc::new(AtomicBool::new(false));
        let second_processor = TestProcessor::new(
            "deadline_pre_body",
            memo_fp,
            second_body_started.clone(),
            true,
        );
        let deadline = DeadlineContext::NONE.with_timeout(Duration::from_secs(1));
        let (handle, _) = app
            .update(
                second_processor,
                AppUpdateOptions {
                    deadline,
                    ..AppUpdateOptions::default()
                },
                Arc::new(()),
                None,
            )
            .unwrap();
        let err = handle.result().await.unwrap_err();
        assert!(err.is_deadline_exceeded());
        assert!(
            !second_body_started.load(Ordering::SeqCst),
            "processor body must not start after memo-state validation expires the deadline"
        );
    }

    /// A processor context retained after its component's processing task
    /// ends — in the host language, an exception traceback keeping a frame
    /// that holds it, or a stored handle — keeps the component's identity
    /// alive but not active, so live-mode termination is unaffected.
    #[tokio::test]
    async fn retained_context_does_not_keep_component_active() {
        let (app, _dir) = test_app("retained_ctx").await;
        let child_path = StablePath::root().concat_part(StableKey::Str(Arc::from("child")));

        let retained_child_ctx: Arc<Mutex<Option<ComponentProcessorContext<TestProfile>>>> =
            Default::default();
        let root_component: Arc<Mutex<Option<Component<TestProfile>>>> = Default::default();

        let child_processor = {
            let retained = retained_child_ctx.clone();
            TestProcessor::new(
                "child",
                Fingerprint::from(&"retained_ctx/child").unwrap(),
                Arc::new(AtomicBool::new(false)),
                false,
            )
            .with_on_process(Arc::new(move |ctx| {
                *retained.lock().unwrap() = Some(ctx);
                Box::pin(async { Ok(TestData(b"child".to_vec())) })
            }))
        };
        let root_processor = {
            let root_component = root_component.clone();
            let child_path = child_path.clone();
            let child_processor = Mutex::new(Some(child_processor));
            TestProcessor::new(
                "root",
                Fingerprint::from(&"retained_ctx/root").unwrap(),
                Arc::new(AtomicBool::new(false)),
                false,
            )
            .with_on_process(Arc::new(move |ctx| {
                let root_component = root_component.clone();
                let child_path = child_path.clone();
                let child_processor = child_processor.lock().unwrap().take().unwrap();
                Box::pin(async move {
                    let component = ctx.component().clone();
                    *root_component.lock().unwrap() = Some(component.clone());
                    // Same steps as the host bindings' `use_mount`.
                    let handle = component
                        .mount_child(&FnCallContext::new(true), child_path)?
                        .use_mount(&ctx, child_processor, DeadlineContext::NONE)
                        .await?;
                    handle.result(Some(&ctx)).await?;
                    Ok(TestData(b"root".to_vec()))
                })
            }))
        };

        let (handle, _) = app
            .update(
                root_processor,
                AppUpdateOptions::default(),
                Arc::new(()),
                None,
            )
            .unwrap();
        handle.result().await.unwrap();

        let root = root_component
            .lock()
            .unwrap()
            .take()
            .expect("root processor ran");
        let child_ctx = retained_child_ctx
            .lock()
            .unwrap()
            .take()
            .expect("child processor ran");
        let child = child_ctx.component();
        assert_eq!(child.stable_path(), &child_path);
        // The retained context keeps the child's identity registered (a
        // re-mount would share it)...
        assert!(root.inner.active_children.lock().contains_key(&child_path));
        // ...but nothing is active, immediately and without polling.
        assert!(!child.is_active());
        assert!(!root.is_active());
    }

    /// A processor body that counts its runs and then holds the build for
    /// `hold`, long enough for a concurrent same-key run to miss the memo
    /// fast-path and queue on the permit.
    fn counting_body(body_runs: Arc<AtomicUsize>, hold: Duration) -> ProcessHook {
        Arc::new(move |_ctx| {
            let body_runs = body_runs.clone();
            Box::pin(async move {
                body_runs.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(hold).await;
                Ok(TestData(b"ret".to_vec()))
            })
        })
    }

    /// Start two updates of `app` back to back — the second before the first
    /// has stored its memo — with memoizable root processors sharing `body`.
    /// Returns both updates' results in start order.
    async fn update_twice_concurrently(
        app: &App<TestProfile>,
        name: &str,
        body: ProcessHook,
    ) -> [crate::prelude::Result<TestData>; 2] {
        let memo_fp = Fingerprint::from(&name).unwrap();
        let processor = || {
            TestProcessor::new(name, memo_fp, Arc::new(AtomicBool::new(false)), false)
                .with_reusable_memo_states()
                .with_on_process(body.clone())
        };
        let (first, _) = app
            .update(processor(), AppUpdateOptions::default(), Arc::new(()), None)
            .unwrap();
        let (second, _) = app
            .update(processor(), AppUpdateOptions::default(), Arc::new(()), None)
            .unwrap();
        [first.result().await, second.result().await]
    }

    /// Two runs of one component with the same memo key that both start before
    /// either has stored a memo execute the body once: the run that loses the
    /// race for the build permit finds the winner's memo under the permit and
    /// reuses it — same result, no second execution.
    #[tokio::test]
    async fn concurrent_same_key_runs_execute_body_once() {
        let (app, _dir) = test_app("memo_piggyback").await;
        let body_runs = Arc::new(AtomicUsize::new(0));
        let body = counting_body(body_runs.clone(), Duration::from_millis(100));
        let [first, second] = update_twice_concurrently(&app, "memo_piggyback", body).await;
        assert_eq!(first.unwrap(), TestData(b"ret".to_vec()));
        assert_eq!(second.unwrap(), TestData(b"ret".to_vec()));
        assert_eq!(body_runs.load(Ordering::SeqCst), 1);
    }

    /// The re-check under the permit consults the memo store, not only the
    /// key marker: a run that fails stores no memo, so the run queued behind
    /// it executes the body itself instead of reusing a stale result.
    #[tokio::test]
    async fn queued_run_executes_after_a_failed_same_key_run() {
        let (app, _dir) = test_app("memo_after_failure").await;
        let body_runs = Arc::new(AtomicUsize::new(0));
        let body: ProcessHook = {
            let body_runs = body_runs.clone();
            Arc::new(move |_ctx| {
                let body_runs = body_runs.clone();
                Box::pin(async move {
                    let run = body_runs.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    if run == 0 {
                        return Err(cocoindex_utils::internal_error!("first run fails"));
                    }
                    Ok(TestData(b"ret".to_vec()))
                })
            })
        };
        let results = update_twice_concurrently(&app, "memo_after_failure", body).await;
        assert_eq!(body_runs.load(Ordering::SeqCst), 2);
        let (failed, succeeded): (Vec<_>, Vec<_>) =
            results.into_iter().partition(|result| result.is_err());
        assert_eq!(failed.len(), 1, "exactly the first run to execute fails");
        assert_eq!(
            succeeded.into_iter().next().unwrap().unwrap(),
            TestData(b"ret".to_vec())
        );
    }

    /// Run one update whose root runs the same memoizable child component
    /// twice concurrently — the second run started before the first has
    /// stored its memo — with child processors sharing `body`. A path can be
    /// mounted only once per parent, so the second run gets a build context
    /// built directly. Returns both results in start order.
    async fn run_same_child_twice_in_one_update(
        app: &App<TestProfile>,
        name: &str,
        full_reprocess: bool,
        body: ProcessHook,
    ) -> [crate::prelude::Result<TestData>; 2] {
        let child_path = StablePath::root().concat_part(StableKey::Str(Arc::from("child")));
        let child_memo_fp = Fingerprint::from(&format!("{name}/child")).unwrap();
        let child_processor = || {
            TestProcessor::new(
                "child",
                child_memo_fp,
                Arc::new(AtomicBool::new(false)),
                false,
            )
            .with_reusable_memo_states()
            .with_on_process(body.clone())
        };
        let child_processors = Mutex::new(Some((child_processor(), child_processor())));
        let results: Arc<Mutex<Option<[crate::prelude::Result<TestData>; 2]>>> = Default::default();
        let root_processor = {
            let results = results.clone();
            TestProcessor::new(
                "root",
                Fingerprint::from(&format!("{name}/root")).unwrap(),
                Arc::new(AtomicBool::new(false)),
                false,
            )
            .with_on_process(Arc::new(move |ctx| {
                let (first, second) = child_processors.lock().unwrap().take().unwrap();
                let child_path = child_path.clone();
                let results = results.clone();
                Box::pin(async move {
                    let child = ctx
                        .component()
                        .mount_child(&FnCallContext::new(true), child_path)?;
                    let first = child
                        .clone()
                        .use_mount(&ctx, first, DeadlineContext::NONE)
                        .await?;
                    let second_ctx = ComponentProcessorContext::new(
                        child.clone(),
                        Some(ctx.clone()),
                        ctx.processing_stats().clone(),
                        ctx.host_ctx().clone(),
                        ComponentProcessingAction::new_build(
                            ctx.target_states_providers()?,
                            ctx.full_reprocess(),
                            ctx.live(),
                            None,
                            None,
                        ),
                    );
                    let second = child
                        .run(
                            second,
                            second_ctx,
                            DeadlineContext::NONE,
                            DeadlineContext::NONE,
                        )
                        .await?;
                    let first = first.result(Some(&ctx)).await;
                    let second = second.result(Some(&ctx)).await;
                    *results.lock().unwrap() = Some([first, second]);
                    Ok(TestData(b"root".to_vec()))
                })
            }))
        };
        let (handle, _) = app
            .update(
                root_processor,
                AppUpdateOptions {
                    full_reprocess,
                    ..AppUpdateOptions::default()
                },
                Arc::new(()),
                None,
            )
            .unwrap();
        handle.result().await.unwrap();
        results.lock().unwrap().take().expect("root processor ran")
    }

    /// `full_reprocess` ignores memos from previous runs, not this operation's
    /// own executions: two concurrent runs of one component in one
    /// `full_reprocess` update execute the body once, the second reusing the
    /// memo the first stored — the re-check under the permit sees that this
    /// operation stored it.
    #[tokio::test]
    async fn concurrent_same_key_runs_execute_body_once_under_full_reprocess() {
        let (app, _dir) = test_app("memo_piggyback_full_reprocess").await;
        let body_runs = Arc::new(AtomicUsize::new(0));
        let body = counting_body(body_runs.clone(), Duration::from_millis(100));
        let [first, second] =
            run_same_child_twice_in_one_update(&app, "memo_piggyback_full_reprocess", true, body)
                .await;
        assert_eq!(first.unwrap(), TestData(b"ret".to_vec()));
        assert_eq!(second.unwrap(), TestData(b"ret".to_vec()));
        assert_eq!(body_runs.load(Ordering::SeqCst), 1);
    }

    /// A memo stored by a previous operation is a cache: a later
    /// `full_reprocess` update executes the body again even though the
    /// component's last stored memo carries its key.
    #[tokio::test]
    async fn full_reprocess_reexecutes_a_memo_stored_by_a_previous_operation() {
        let (app, _dir) = test_app("memo_full_reprocess_generation").await;
        let body_runs = Arc::new(AtomicUsize::new(0));
        let body = counting_body(body_runs.clone(), Duration::ZERO);
        let memo_fp = Fingerprint::from(&"memo_full_reprocess_generation").unwrap();
        let processor = || {
            TestProcessor::new(
                "memo_full_reprocess_generation",
                memo_fp,
                Arc::new(AtomicBool::new(false)),
                false,
            )
            .with_reusable_memo_states()
            .with_on_process(body.clone())
        };
        for full_reprocess in [false, true] {
            let (handle, _) = app
                .update(
                    processor(),
                    AppUpdateOptions {
                        full_reprocess,
                        ..AppUpdateOptions::default()
                    },
                    Arc::new(()),
                    None,
                )
                .unwrap();
            assert_eq!(handle.result().await.unwrap(), TestData(b"ret".to_vec()));
        }
        assert_eq!(
            body_runs.load(Ordering::SeqCst),
            2,
            "the full_reprocess update must not reuse the previous update's memo"
        );
    }
}
