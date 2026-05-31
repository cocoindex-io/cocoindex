use std::collections::HashMap;
use std::future::Future;

use crate::engine::component::{
    Component, ComponentBgChildReadinessChildGuard, ComponentExecutionHandle, OnError,
};
use crate::engine::context::{ComponentProcessingAction, ComponentProcessorContext, FnCallContext};
use crate::engine::profile::EngineProfile;
use crate::engine::stats::ProcessingStats;
use crate::engine::target_state::TargetStateProvider;
use crate::prelude::*;
use crate::state::stable_path::StablePath;
use crate::state::target_state_path::TargetStatePath;
use cocoindex_utils::error::{SharedError, SharedResult};

use tokio::sync::oneshot;

/// Per-component drain timeout, used in two places:
///   - `mount_live_async`'s cancel-and-drain of a prior incarnation at
///     the same path (without a timeout, a wedged prior incarnation
///     could hold the parent's `update_full_lock` forever).
///   - `App::drop_app`'s registry walk (per-component, in parallel).
///
/// Single source of truth: both call sites should match so the design's
/// "drop_app" contract behaves the same as a re-mount's drain — see
/// specs/live_component/design.md.
pub(crate) const LIVE_COMPONENT_DRAIN_TIMEOUT_SECS: u64 = 30;

/// Result of mounting a live component.
pub struct MountLiveResult<Prof: EngineProfile> {
    pub controller: LiveComponentController<Prof>,
    pub readiness_handle: ComponentExecutionHandle,
}

/// Intermediate state after sync preparation, before async completion.
pub struct MountLivePending<Prof: EngineProfile> {
    child: Component<Prof>,
    parent_ctx: ComponentProcessorContext<Prof>,
    providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
    live: bool,
}

/// Mount a live component. Split into two phases:
/// - `prepare` (sync): registers the child component, borrows fn_ctx
/// - `complete` (async): cancels existing live state, creates controller
pub fn mount_live_prepare<Prof: EngineProfile>(
    parent_ctx: &ComponentProcessorContext<Prof>,
    fn_ctx: &FnCallContext,
    child_stable_path: StablePath,
    live: bool,
) -> Result<MountLivePending<Prof>> {
    // 1. Mount (or get existing) child component.
    let child = parent_ctx
        .component()
        .mount_child(fn_ctx, child_stable_path.clone())?;

    // Register the child in the parent's child_path_set and get providers
    // in a single lock acquisition.
    let sub_path = child_stable_path
        .as_ref()
        .strip_parent(parent_ctx.stable_path().as_ref())?;
    let providers = parent_ctx.update_building_state(|building_state| {
        building_state.child_path_set.add_child(
            sub_path,
            crate::state::stable_path_set::StablePathSet::Component,
        )?;
        Ok(building_state
            .target_states
            .provider_registry
            .providers
            .clone())
    })?;

    Ok(MountLivePending {
        child,
        parent_ctx: parent_ctx.clone(),
        providers,
        live,
    })
}

impl<Prof: EngineProfile> MountLivePending<Prof> {
    /// Complete the mount: cancel existing, create controller and readiness handle.
    pub async fn complete(self) -> Result<MountLiveResult<Prof>> {
        let Self {
            child,
            parent_ctx,
            providers,
            live,
        } = self;

        // 2. If existing live state, cancel and drain it (with 30s timeout).
        //
        // Without a timeout, a prior incarnation wedged in non-yielding
        // synchronous Python compute would hold the parent's
        // `update_full_lock` forever and block this re-mount path. After
        // the timeout we proceed anyway: the orphan `LiveComponentState`
        // remains alive (drain tasks captured `Arc<LiveComponentState>`
        // clones, so its registry `Weak` keeps upgrading) and the orphan
        // drain may continue writing target state for a while; the next
        // `update_full` reconciles. This trades quiescence for liveness —
        // same trade `App::drop_app` makes (see "drop_app" contract in
        // specs/live_component/design.md).
        if let Some(existing_state) = child.live_state() {
            let drain_fut = existing_state.cancel_and_drain();
            if tokio::time::timeout(
                std::time::Duration::from_secs(LIVE_COMPONENT_DRAIN_TIMEOUT_SECS),
                drain_fut,
            )
            .await
            .is_err()
            {
                tracing::warn!(
                    "mount_live_async: cancel_and_drain of prior incarnation \
                     timed out after {}s; proceeding with new mount. The orphan \
                     drain may continue briefly; next update_full will reconcile.",
                    LIVE_COMPONENT_DRAIN_TIMEOUT_SECS
                );
            }
        }

        // 3. Create readiness guard from parent's components_readiness.
        let readiness_guard = parent_ctx.components_readiness().clone().add_child();

        // 4. Create cancellation token as child of app's root token.
        let cancellation_token = parent_ctx.app_ctx().cancellation_token().child_token();

        // 5. Create LiveComponentState.
        let state = Arc::new(LiveComponentState::<Prof>::new(
            readiness_guard,
            cancellation_token,
        ));

        // 6. Install ordering (matters for drop_app race coverage):
        //    (i)   app_store the new state in `child.live_state` (strong-ref anchor)
        //    (ii)  register Weak in `app_ctx.live_components` (compaction-on-push)
        //
        // The DB `ChildExistence` row is written separately:
        //   - For `coco.mount(LiveCompClass)` from inside `process()`: by
        //     submit/commit, via `update_building_state`'s `child_path_set`.
        //   - For `operator.update(LiveCompClass)` from `process_live`: by
        //     `mount_inner_live` directly via `Storage::run_txn`, just below this
        //     `complete()` call. (See `LiveComponentController::mount_inner_live`.)
        // Both paths produce the same on-disk state.
        //
        // (i) → (ii) order is required: a Weak registered before its strong
        // owner exists would die on `upgrade()`. With the strong owner held
        // by `child.live_state`, the Weak in `live_components` upgrades for
        // the lifetime of the live component (or until cancel-and-drain
        // releases the strong ref).
        child.set_live_state(state.clone());
        parent_ctx
            .app_ctx()
            .register_live_component(Arc::downgrade(&state));

        // 7. Create the controller (providers were captured during prepare).
        let controller = LiveComponentController::new(
            child,
            state.clone(),
            parent_ctx.processing_stats().clone(),
            parent_ctx.host_ctx().clone(),
            parent_ctx.full_reprocess(),
            live,
            providers,
        );

        // 9. Create readiness handle that resolves when mark_ready is called.
        let readiness_handle = ComponentExecutionHandle::new(async move {
            state.ready_notified().await;
            Ok(())
        });

        Ok(MountLiveResult {
            controller,
            readiness_handle,
        })
    }
}

/// Outcome of an `update`/`delete` op as observed by its `ComponentMountHandle`.
///
/// Currently surfaces to Python through `make_handle`'s legacy
/// `ComponentExecutionHandle` adapter:
///   - `Executed(Ok(()))`   → `Ok(())`            — op ran cleanly
///   - `Executed(Err(e))`   → `Err(e)`            — user code error
///   - `Superseded`         → `Ok(())`            — coalesced; treated as no-op success
///   - `Cancelled`          → `Err(<cancelled>)`  — drain saw cancellation mid-op
///
/// The Python `MountOutcome` enum (4 variants: Executed/Superseded/Cancelled/Failed)
/// reconstructs this approximately at the boundary. Cancellation surfaces
/// cleanly via `Error::cancelled()` → `asyncio.CancelledError` (typed, no
/// string matching). The remaining boundary loss is `Superseded` being
/// collapsed into `Executed` — exposing `HandleOutcome` directly via PyO3
/// would close that gap.
pub(crate) enum HandleOutcome {
    Executed(SharedResult<()>),
    Superseded,
    Cancelled,
}

/// Readiness state. Under a single Mutex so mark_ready() can atomically
/// take the guard and set the ready flag.
pub(crate) struct ReadinessState {
    guard: Option<ComponentBgChildReadinessChildGuard>,
    ready: bool,
}

/// One queued op for a subpath, paired with the oneshot sender that
/// resolves the caller's handle.
struct QueuedOp<Prof: EngineProfile> {
    op: Op<Prof>,
    /// Single-shot. Resolved with `Executed(outcome)` if this op runs, or
    /// `Superseded` immediately if displaced before the drain task picks it
    /// up, or `Cancelled` if the drain task observes cancellation while
    /// running this op.
    handle_tx: oneshot::Sender<HandleOutcome>,
}

enum Op<Prof: EngineProfile> {
    /// `update(subpath, processor)` — calls `child.run_in_background`.
    /// Failures route through `on_error` (parent's exception handler
    /// chain); the handler's `Result` decides propagation (Ok = swallow
    /// mount-style; Err = propagate via `handle.ready()`).
    Update {
        processor: Prof::ComponentProc,
        on_error: Option<OnError>,
    },
    /// `delete(subpath)` — calls `child.delete`. Same error model as
    /// `Update`: handler controls whether failures propagate. The DB
    /// tombstone+existence-removal is performed at dispatch time
    /// (synchronously) regardless, so even a propagating handler
    /// preserves the retry-via-tombstone safety net.
    Delete { on_error: Option<OnError> },
}

/// Per-subpath state in the coalescing map.
struct PendingEntry<Prof: EngineProfile> {
    /// Newest queued op for this subpath. Replaced (with `Superseded`
    /// resolution for the displaced op) when a newer dispatch arrives
    /// while one is still queued.
    queued: Option<QueuedOp<Prof>>,
    /// True when the drain task is currently executing an op
    /// (i.e. between step 1's "take queued" and step 3's "clear flag").
    processing: bool,
}

/// Coalescing map plus update_full visibility flag.
struct PendingState<Prof: EngineProfile> {
    map: HashMap<StablePath, PendingEntry<Prof>>,
    /// Set by `update_full` while it's running. Drain tasks observing
    /// this flag block in step 1 (waiting for `pending_changed`) until
    /// `update_full` finishes.
    update_full_active: bool,
}

impl<Prof: EngineProfile> PendingState<Prof> {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            update_full_active: false,
        }
    }
}

/// Shared state stored in `ComponentInner::live_state`.
/// Does NOT reference `Component` — breaks the cyclic Arc.
pub struct LiveComponentState<Prof: EngineProfile> {
    /// Held by `update_full()` for its entire body. Concurrent calls
    /// (e.g. from `asyncio.gather`) serialize here.
    update_full_lock: tokio::sync::Mutex<()>,

    /// Per-subpath coalescing map + `update_full_active` flag.
    /// `parking_lot::Mutex` because we never hold it across `.await` and
    /// the panic-safety is simpler (no poisoning).
    pending: parking_lot::Mutex<PendingState<Prof>>,

    /// Signaled when any of:
    ///  - a drain task finishes processing an op (clears `processing` flag)
    ///  - update_full clears `update_full_active`
    /// Awaited by:
    ///  - update_full's recheck loop (no entry has `processing`)
    ///  - drain task step 1 (gating on `update_full_active`)
    pending_changed: tokio::sync::Notify,

    /// Readiness state — guard + flag under one lock.
    readiness: Mutex<ReadinessState>,

    /// Signaled once when `mark_ready` is called.
    ready_notify: tokio::sync::Notify,

    /// Cancellation token — triggered on re-mount, app shutdown, or
    /// catch-up `mark_ready`.
    cancellation_token: tokio_util::sync::CancellationToken,

    /// JoinHandle of the tokio task running `process_live`. Set by `start()`.
    live_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl<Prof: EngineProfile> LiveComponentState<Prof> {
    pub fn new(
        readiness_guard: ComponentBgChildReadinessChildGuard,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            update_full_lock: tokio::sync::Mutex::new(()),
            pending: parking_lot::Mutex::new(PendingState::new()),
            pending_changed: tokio::sync::Notify::new(),
            readiness: Mutex::new(ReadinessState {
                guard: Some(readiness_guard),
                ready: false,
            }),
            ready_notify: tokio::sync::Notify::new(),
            cancellation_token,
            live_task: Mutex::new(None),
        }
    }

    pub fn cancellation_token(&self) -> &tokio_util::sync::CancellationToken {
        &self.cancellation_token
    }

    /// Take the JoinHandle for the live task (if any). Used by re-mount and
    /// shutdown paths to await `process_live` termination.
    pub fn live_task_handle(&self) -> Option<tokio::task::JoinHandle<()>> {
        self.live_task.lock().unwrap().take()
    }

    /// Wait until `mark_ready()` is signaled (or already was).
    pub async fn ready_notified(&self) {
        // `Notify::notified()` does NOT register a waiter — registration
        // happens on first poll. Use `pin!` + `enable()` while holding the
        // readiness lock so any `ensure_mark_ready` between our flag-check
        // and our await is forced to wake us. Same lost-wakeup-avoidance
        // pattern used in `wait_until_quiescent` and `cancel_and_await_quiescence`.
        let notified = self.ready_notify.notified();
        tokio::pin!(notified);
        {
            let state = self.readiness.lock().unwrap();
            if state.ready {
                return;
            }
            notified.as_mut().enable();
        }
        notified.await;
    }

    fn is_ready(&self) -> bool {
        self.readiness.lock().unwrap().ready
    }

    /// Resolve readiness as success. No-op if already resolved.
    fn ensure_mark_ready(&self) {
        let mut state = self.readiness.lock().unwrap();
        if !state.ready {
            state.ready = true;
            if let Some(guard) = state.guard.take() {
                guard.resolve(Default::default());
            }
            self.ready_notify.notify_waiters();
        }
    }

    /// Resolve readiness with error. Drops the guard without calling
    /// resolve(); the guard's Drop reports failure to the parent.
    fn resolve_ready_with_error(&self, _err: Error) {
        let mut state = self.readiness.lock().unwrap();
        if !state.ready {
            state.ready = true;
            state.guard.take();
            self.ready_notify.notify_waiters();
        }
    }

    /// Wait until no entry has `processing == true`.
    /// Caller must NOT hold the `pending` lock.
    async fn wait_until_quiescent(&self) {
        loop {
            // Notified-while-holding-lock pattern (avoids lost wakeups).
            let notified = self.pending_changed.notified();
            tokio::pin!(notified);
            {
                let pending = self.pending.lock();
                if !pending.map.values().any(|e| e.processing) {
                    return;
                }
                notified.as_mut().enable();
            }
            notified.await;
        }
    }

    /// Cancel the controller and wait for full quiescence:
    ///  1. Cancel the token (causes `start()`'s biased select to drop
    ///     `process_live`; CancelOnDropPy aborts the Python task).
    ///  2. Await the live_task JoinHandle — `process_live` finishes
    ///     unwinding before this returns.
    ///  3. Wait for all subpath drain tasks to clear (`pending` empty
    ///     of in-flight ops).
    pub async fn cancel_and_drain(&self) {
        // Cancel BEFORE taking the handle: if a panic strikes between
        // here and the await below, the spawned task can still observe
        // cancellation via its biased-cancel arm and exit on its own.
        // Without this, a panic mid-call would detach the task with no
        // cancellation signal ever fired.
        // The follow-up `cancel_and_await_quiescence` re-cancels (idempotent
        // — no-op on the second call) and then waits for `pending` to drain.
        self.cancellation_token.cancel();
        let handle = self.live_task.lock().unwrap().take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        self.cancel_and_await_quiescence().await;
    }

    /// Cancel the token and wait until `pending` is fully empty.
    /// Factored out from `cancel_and_drain` for use by `App::drop_app`,
    /// which doesn't have direct access to the live_task JoinHandle and
    /// can rely on root-token cascade to terminate `process_live`.
    pub async fn cancel_and_await_quiescence(&self) {
        self.cancellation_token.cancel();
        loop {
            let notified = self.pending_changed.notified();
            tokio::pin!(notified);
            {
                let pending = self.pending.lock();
                // Empty map ⇒ no drain tasks alive ⇒ all subpaths quiesced.
                if pending.map.is_empty() {
                    return;
                }
                notified.as_mut().enable();
            }
            notified.await;
        }
    }
}

/// Returned to Python. Holds Component + shared state.
/// NOT stored in ComponentInner — only Python/PyO3 holds this.
#[derive(Clone)]
pub struct LiveComponentController<Prof: EngineProfile> {
    component: Component<Prof>,
    state: Arc<LiveComponentState<Prof>>,

    // --- Immutable config ---
    processing_stats: ProcessingStats,
    host_ctx: Arc<Prof::HostCtx>,
    full_reprocess: bool,
    live: bool,

    /// Providers inherited from the parent component context at creation time.
    /// Immutable — process() may not call use_mount(), so no new providers are created.
    providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
}

impl<Prof: EngineProfile> LiveComponentController<Prof> {
    pub fn new(
        component: Component<Prof>,
        state: Arc<LiveComponentState<Prof>>,
        processing_stats: ProcessingStats,
        host_ctx: Arc<Prof::HostCtx>,
        full_reprocess: bool,
        live: bool,
        providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
    ) -> Self {
        Self {
            component,
            state,
            processing_stats,
            host_ctx,
            full_reprocess,
            live,
            providers,
        }
    }

    pub fn state(&self) -> &Arc<LiveComponentState<Prof>> {
        &self.state
    }

    pub fn component(&self) -> &Component<Prof> {
        &self.component
    }

    pub fn is_live(&self) -> bool {
        self.live
    }

    /// Full processing cycle. Acquires `update_full_lock` (serializes with
    /// concurrent `update_full()` calls), waits for any in-flight subpath
    /// drains to quiesce, then runs `process()` via `run_in_background`.
    pub async fn update_full(
        &self,
        processor: Prof::ComponentProc,
        on_error: Option<OnError>,
    ) -> Result<()> {
        let _full_guard = self.state.update_full_lock.lock().await;

        // Step-1 cancellation gate (mirrors design.md Section "update_full
        // body, step 1"). Avoids paying drain cost on a cancelled controller.
        if self.state.cancellation_token.is_cancelled() {
            return Err(make_cancelled_error());
        }

        // Set update_full_active under the pending lock so any drain tasks
        // observing it know to gate themselves before promoting queued ops.
        // Use an RAII guard so we always clear it on every exit path.
        let _active_guard = UpdateFullActiveGuard::new(&self.state);

        // Wait until no subpath has `processing == true`. We allow queued
        // ops to remain — they'll be re-collapsed to `Superseded` once
        // process() completes, since `update_full` re-derives all subpaths
        // authoritatively.
        self.state.wait_until_quiescent().await;

        // Now run process() via run_in_background. Errors from process() are
        // routed via `on_error` (parent's exception handler chain), matching
        // the behavior of background `mount()` calls. Without on_error wired
        // here, periodic-refresh patterns (e.g. `coco.auto_refresh`) would
        // silently swallow cycle failures.
        let context = ComponentProcessorContext::new(
            self.component.clone(),
            None,
            self.processing_stats.clone(),
            self.host_ctx.clone(),
            // Mirror `Component::mount`: store the same on_error on the
            // build context so the cycle's commit-phase GC sweep cascades
            // it to orphan deletes too.
            ComponentProcessingAction::new_build(
                self.providers.clone(),
                self.full_reprocess,
                self.live,
                on_error.clone(),
                None,
            ),
        );

        let handle = self
            .component
            .clone()
            .run_in_background(processor, context, on_error, None)
            .await?;

        handle.ready().await?;

        Ok(())
    }

    /// Mount a child component incrementally. Same-subpath rapid calls are
    /// coalesced — only the most-recently-queued op runs; older ones
    /// resolve as `Superseded`. Returns immediately after dispatching;
    /// the actual processor execution happens on a per-subpath drain task.
    ///
    /// Errors from the spawned child are routed through `on_error` if
    /// provided (parent's exception handler chain), matching the behavior
    /// of background `coco.mount()` calls.
    pub async fn update(
        &self,
        subpath: StablePath,
        processor: Prof::ComponentProc,
        on_error: Option<OnError>,
    ) -> Result<ComponentExecutionHandle> {
        self.dispatch(
            subpath,
            Op::Update {
                processor,
                on_error,
            },
        )
        .await
    }

    /// Delete a child component. Same coalescing model as `update()`.
    /// The DB existence-removal + tombstone write happens synchronously
    /// here (before dispatch), so `update_full`'s tombstone scan sees a
    /// consistent state once dispatch returns.
    ///
    /// Failures route through `on_error` (parent's exception handler
    /// chain). The handler's `Result` decides propagation, symmetric
    /// with `update()` — `Ok` swallows; `Err` propagates via the
    /// returned handle. The tombstone always remains in the DB, so even
    /// a failed delete is retried by the next reconcile's GC sweep.
    pub async fn delete(
        &self,
        subpath: StablePath,
        on_error: Option<OnError>,
    ) -> Result<ComponentExecutionHandle> {
        // Synchronously remove the existence entry and write a tombstone,
        // matching the prior implementation's contract.
        if let Some((parent_ref, child_key)) = subpath.as_ref().split_parent() {
            let app_store = self.component.app_ctx().app_store().clone();
            let parent_path: StablePath = parent_ref.into();
            let child_key = child_key.clone();
            let component_path = self.component.stable_path().clone();
            let relative_child = subpath.as_ref().strip_parent(component_path.as_ref())?;
            let relative_child: StablePath = relative_child.into();
            self.component
                .app_ctx()
                .env()
                .run_txn(move |wtxn| {
                    let app_store = app_store.clone();
                    let parent_path = parent_path.clone();
                    let child_key = child_key.clone();
                    let component_path = component_path.clone();
                    let relative_child = relative_child.clone();
                    Box::pin(async move {
                        app_store
                            .remove_child_with_tombstone(
                                wtxn,
                                &parent_path,
                                &child_key,
                                &component_path,
                                &relative_child,
                            )
                            .await
                    })
                })
                .await?;
        }
        self.dispatch(subpath, Op::Delete { on_error }).await
    }

    /// Insert into `pending` (collapsing any queued op for the same subpath
    /// to `Superseded`), spawn a drain task if one isn't already alive for
    /// this subpath, return a handle that resolves with the dispatched op's
    /// outcome.
    ///
    /// Invariant: a map entry exists iff a drain task is alive (or is being
    /// spawned in this same lock acquisition). The dispatcher is the sole
    /// inserter; the drain task is the sole remover. Combined, that means:
    /// if `contains_key` returns true, a drain is alive; if false, we must
    /// insert and spawn.
    async fn dispatch(
        &self,
        subpath: StablePath,
        op: Op<Prof>,
    ) -> Result<ComponentExecutionHandle> {
        let (handle_tx, handle_rx) = oneshot::channel::<HandleOutcome>();

        // Pre-check cancellation: a controller that's already cancelled
        // (shutdown path) should not accept new work.
        if self.state.cancellation_token.is_cancelled() {
            let _ = handle_tx.send(HandleOutcome::Cancelled);
            return Ok(make_handle(handle_rx));
        }

        let new_op = QueuedOp { op, handle_tx };

        let need_spawn = {
            let mut pending = self.state.pending.lock();
            let need_spawn = !pending.map.contains_key(&subpath);
            let entry = pending.map.entry(subpath.clone()).or_insert(PendingEntry {
                queued: None,
                processing: false,
            });
            // Coalesce: a queued-but-not-yet-promoted op is displaced.
            if let Some(displaced) = entry.queued.take() {
                let _ = displaced.handle_tx.send(HandleOutcome::Superseded);
            }
            entry.queued = Some(new_op);
            need_spawn
        };

        if need_spawn {
            self.spawn_drain_task(subpath);
        }

        Ok(make_handle(handle_rx))
    }

    /// Spawn the per-subpath drain task. Each iteration:
    ///   1. Under pending lock: gate on `update_full_active`; take queued
    ///      op (or exit if none); set `processing = true`.
    ///   2. Without lock: run the op (with biased cancellation select);
    ///      send outcome on the queued op's handle_tx.
    ///   3. Under pending lock: clear `processing`; notify pending_changed;
    ///      loop to step 1.
    fn spawn_drain_task(&self, subpath: StablePath) {
        let state = self.state.clone();
        let component = self.component.clone();
        let processing_stats = self.processing_stats.clone();
        let host_ctx = self.host_ctx.clone();
        let providers = self.providers.clone();
        let full_reprocess = self.full_reprocess;
        let live = self.live;

        let handle = crate::engine::runtime::get_runtime().spawn(async move {
            drain_task_body(
                state,
                component,
                subpath,
                processing_stats,
                host_ctx,
                providers,
                full_reprocess,
                live,
            )
            .await;
        });
        // We spawn-and-forget; the drain task self-terminates when the
        // map entry is empty. The user's per-op handle is panic-safe via
        // `HandleSlotGuard` (sends `Cancelled` on drop), so a runtime-level
        // panic in the drain task body won't hang the user's await.
        //
        // Residual gap: a panic that aborts the drain task BEFORE it runs
        // step 3's "remove entry from pending.map" can leave the entry
        // present forever, blocking `cancel_and_await_quiescence`. That
        // path falls back to `App::drop_app`'s 30s timeout (entry leaked,
        // OS reaps on process exit) — same liveness-vs-quiescence trade
        // documented in the drop_app contract.
        drop(handle);
    }

    /// Signal readiness to parent component. Idempotent — subsequent calls are no-ops.
    /// - Live mode: resolves readiness and returns immediately.
    /// - Catch-up mode: resolves readiness, cancels the cancellation_token, then
    ///   suspends indefinitely (Poll::Pending). select! in start() drops the
    ///   process_live future, terminating the Python task via CancelOnDropPy.
    pub async fn mark_ready(&self) {
        {
            let mut state = self.state.readiness.lock().unwrap();
            if state.ready {
                return; // Idempotent
            }
            state.ready = true;
            if let Some(guard) = state.guard.take() {
                guard.resolve(Default::default());
            }
        }
        self.state.ready_notify.notify_waiters();

        if !self.live {
            // Catch-up mode: cancel the token so select! in start() drops process_live.
            self.state.cancellation_token.cancel();
            // Suspend forever — select! will drop us via CancelOnDropPy.
            std::future::pending::<()>().await;
        }
    }

    /// Start running process_live. Accepts the Python coroutine (as a Rust future
    /// via from_py_future), spawns a tokio task with cancellation support.
    /// Stores the JoinHandle for later cancellation.
    pub fn start<F>(&self, process_live_fut: F)
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let token = self.state.cancellation_token.clone();
        let state = self.state.clone();
        let handle = crate::engine::runtime::get_runtime().spawn(async move {
            let result = tokio::select! {
                biased;
                _ = token.cancelled() => Ok(()),
                result = process_live_fut => result,
            };
            match result {
                Ok(()) => state.ensure_mark_ready(),
                Err(e) => {
                    // Cancellation arm or cancellation-flavored error: treat
                    // as clean shutdown. Otherwise: real failure.
                    if token.is_cancelled() || e.is_cancelled() {
                        state.ensure_mark_ready();
                    } else if !state.is_ready() {
                        state.resolve_ready_with_error(e);
                    } else {
                        error!("process_live failed after mark_ready: {e:?}");
                    }
                }
            }
        });
        *self.state.live_task.lock().unwrap() = Some(handle);
    }

    /// Cancel this controller and wait for full quiescence. Delegates to
    /// `LiveComponentState::cancel_and_drain()`.
    pub async fn cancel_and_drain(&self) {
        self.state.cancel_and_drain().await;
    }

    /// Mount a nested live component at `child_path` from inside this
    /// controller's `process_live`. This is the Rust side of the
    /// `operator.update(p, LiveCompClass)` branch (Slice F).
    ///
    /// Acquires `update_full_lock` *cancellably* (biased select with
    /// `cancellation_token.cancelled()`), re-checks cancellation post-acquire,
    /// then constructs a fresh parent processor context anchored at this
    /// controller's component (mirroring `update_full`'s ctx construction)
    /// and calls `mount_live_prepare` + `MountLivePending::complete()` to
    /// install the inner live state at the child path.
    ///
    /// The `update_full_lock` is held for the duration of `complete()`,
    /// which does:
    ///   - cancel-and-drain of any prior incarnation at this child path
    ///   - install of the new `LiveComponentState` (writing `child.live_state`
    ///     and registering with `app_ctx.live_components`)
    ///
    /// The returned `MountLiveResult` carries the inner controller (caller
    /// will pass to Python and eventually call `.start()`) and the readiness
    /// handle (resolves when inner `mark_ready` fires or the inner is
    /// cancelled).
    ///
    /// Cancellation contract: returns `Err(<cancelled>)` if the controller's
    /// token fires before or during the lock acquire / mount. The Python
    /// wrapper maps this to `MountOutcome.cancelled` if needed.
    pub async fn mount_inner_live(&self, child_path: StablePath) -> Result<MountLiveResult<Prof>> {
        // Cancellable lock acquisition: shutdown breaks out within one poll.
        let _full_guard = tokio::select! {
            biased;
            _ = self.state.cancellation_token.cancelled() => return Err(make_cancelled_error()),
            guard = self.state.update_full_lock.lock() => guard,
        };

        // Post-acquire re-check: the token may have fired *while we were
        // waiting* but after the cancelled-arm was last polled.
        if self.state.cancellation_token.is_cancelled() {
            return Err(make_cancelled_error());
        }

        // Build a fresh parent processor context anchored at our component
        // (mirroring `update_full`'s ctx construction). The building state
        // is local to this context — child registration in `child_path_set`
        // is benign: nothing else reads this isolated state.
        let parent_ctx = ComponentProcessorContext::new(
            self.component.clone(),
            None,
            self.processing_stats.clone(),
            self.host_ctx.clone(),
            // No on_error on this synthetic parent context — it only
            // exists to register the child in `child_path_set`; no
            // commit/GC sweep runs through it.
            ComponentProcessingAction::new_build(
                self.providers.clone(),
                self.full_reprocess,
                self.live,
                None,
                None,
            ),
        );
        let fn_ctx = FnCallContext::default();

        let pending = mount_live_prepare(&parent_ctx, &fn_ctx, child_path.clone(), self.live)?;
        let result = pending.complete().await?;

        // Polish 4: write the `ChildExistence` DB row symmetric with the
        // through-`process()` install path (where it's written via
        // submit/commit). Without this row, the next `update_full`'s
        // tombstone scan can't see this child and won't reconcile its
        // target states on parent re-runs that DON'T re-issue this
        // operator.update call. With this row, the install/tombstone
        // state machine is symmetric across both installer paths.
        if let Some((parent_path_ref, child_key)) = child_path.as_ref().split_parent() {
            let app_store = self.component.app_ctx().app_store().clone();
            let parent_path: StablePath = parent_path_ref.into();
            let child_key = child_key.clone();
            self.component
                .app_ctx()
                .env()
                .run_txn(move |wtxn| {
                    let app_store = app_store.clone();
                    let parent_path = parent_path.clone();
                    let child_key = child_key.clone();
                    Box::pin(async move {
                        crate::engine::execution::ensure_path_node_type(
                            &app_store,
                            wtxn,
                            parent_path.as_ref(),
                            &child_key,
                            crate::state::db_schema::StablePathNodeType::Component,
                        )
                        .await
                    })
                })
                .await?;
        }

        Ok(result)
        // _full_guard drops here, releasing update_full_lock.
    }
}

/// Three-state slot for the drain task's in-flight `oneshot::Sender`.
///
/// Combined with `HandleSlotGuard` (RAII), this guarantees the user's
/// handle resolves with a sensible outcome even if the drain task panics
/// at a runtime level (e.g. allocator OOM, abort signal, or a panic in
/// `select!`/await machinery that escapes `run_op`'s caught surface).
///
/// State transitions during one drain iteration:
///   - `None` (initial / between iterations) — nothing to send
///   - `Pending(handle_tx)` (after step 1 took queued, before step 2's
///     outcome is computed) — guard sends `Cancelled` on drop
///   - `Resolved { handle_tx, outcome }` (after step 2 computed an
///     outcome, before step 3 sends it) — guard sends the truthful
///     outcome on drop, preserving "really-cancelled" vs "really-failed"
///     vs "really-succeeded" semantics for the brief send-pending window
///
/// Note on user-code panics: these surface through `JoinError::IsPanic`
/// inside the spawned `run_in_background` Future and become
/// `Executed(Err(...))` outcomes; they do NOT propagate as Rust panics
/// to this drain-task frame, so the panic guard's Pending → Cancelled
/// path is reserved for genuinely rare runtime-level panics.
enum HandleSlot {
    None,
    Pending(oneshot::Sender<HandleOutcome>),
    Resolved {
        handle_tx: oneshot::Sender<HandleOutcome>,
        outcome: HandleOutcome,
    },
}

struct HandleSlotGuard {
    slot: HandleSlot,
}

impl HandleSlotGuard {
    fn new() -> Self {
        Self {
            slot: HandleSlot::None,
        }
    }

    /// Transition `None → Pending(tx)`. Called after step 1 takes the
    /// queued op, before step 2 awaits `run_op`.
    fn set_pending(&mut self, tx: oneshot::Sender<HandleOutcome>) {
        debug_assert!(matches!(self.slot, HandleSlot::None));
        self.slot = HandleSlot::Pending(tx);
    }

    /// Transition `Pending(tx) → Resolved { tx, outcome }`. Called after
    /// step 2 computes the outcome, before step 3 sends it. Two
    /// statements with a one-instruction `Slot::None` gap (via
    /// `mem::replace`) — a panic in that instruction degrades to
    /// `Closed` (oneshot Sender drop → user sees RecvError).
    fn set_resolved(&mut self, outcome: HandleOutcome) {
        let prev = std::mem::replace(&mut self.slot, HandleSlot::None);
        let HandleSlot::Pending(handle_tx) = prev else {
            debug_assert!(false, "set_resolved called from non-Pending state");
            return;
        };
        self.slot = HandleSlot::Resolved { handle_tx, outcome };
    }

    /// Consume the guard, sending the resolved outcome (panicking if
    /// not in Resolved state — which would indicate a programming
    /// error). Used on the happy path; on Drop the guard handles the
    /// other states.
    fn send_resolved(mut self) {
        let slot = std::mem::replace(&mut self.slot, HandleSlot::None);
        match slot {
            HandleSlot::Resolved { handle_tx, outcome } => {
                let _ = handle_tx.send(outcome);
            }
            HandleSlot::Pending(_) | HandleSlot::None => {
                debug_assert!(false, "send_resolved called from non-Resolved state");
            }
        }
    }
}

impl Drop for HandleSlotGuard {
    fn drop(&mut self) {
        let slot = std::mem::replace(&mut self.slot, HandleSlot::None);
        match slot {
            HandleSlot::None => {}
            HandleSlot::Pending(tx) => {
                // We were mid-run-op and something panicked or
                // we exited an early-return path with the slot still
                // owned. Send `Cancelled` so the user's handle doesn't
                // hang forever waiting for an outcome.
                let _ = tx.send(HandleOutcome::Cancelled);
            }
            HandleSlot::Resolved { handle_tx, outcome } => {
                // Outcome was computed; deliver the truthful value
                // even on drop (preserves "really-Executed-Err" /
                // "really-Cancelled" distinction in the brief
                // send-pending window).
                let _ = handle_tx.send(outcome);
            }
        }
    }
}

/// Per-subpath drain task body. Owns one `Arc<LiveComponentState>` so the
/// registry `Weak` keeps upgrading for this task's lifetime. Loops over:
///   - Step 1: take the queued op (or gate on `update_full_active` / exit
///     when the entry is empty).
///   - Step 2: run the op under a panic-safe `HandleSlotGuard`, observing
///     cancellation via biased `select!`.
///   - Step 3: clear `processing`, notify, loop back to step 1.
async fn drain_task_body<Prof: EngineProfile>(
    state: Arc<LiveComponentState<Prof>>,
    component: Component<Prof>,
    subpath: StablePath,
    processing_stats: ProcessingStats,
    host_ctx: Arc<Prof::HostCtx>,
    providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
    full_reprocess: bool,
    live: bool,
) {
    loop {
        // ── Step 1: take queued op (or gate / exit) ──
        let queued = loop {
            // Notified-while-holding-lock pattern.
            let notified = state.pending_changed.notified();
            tokio::pin!(notified);
            {
                let mut pending = state.pending.lock();
                if state.cancellation_token.is_cancelled() {
                    // Cancellation: send Cancelled on any queued op, then exit.
                    if let Some(entry) = pending.map.remove(&subpath)
                        && let Some(q) = entry.queued
                    {
                        let _ = q.handle_tx.send(HandleOutcome::Cancelled);
                    }
                    state.pending_changed.notify_waiters();
                    return;
                }
                if pending.update_full_active {
                    notified.as_mut().enable();
                    // fall through to await below
                } else {
                    let Some(entry) = pending.map.get_mut(&subpath) else {
                        // Should not happen — dispatcher ensures entry exists
                        // when spawning a drain task.
                        return;
                    };
                    if let Some(queued) = entry.queued.take() {
                        entry.processing = true;
                        break queued;
                    } else {
                        // No work; remove entry so dispatcher knows to
                        // spawn a fresh drain task next time.
                        pending.map.remove(&subpath);
                        state.pending_changed.notify_waiters();
                        return;
                    }
                }
            }
            // Gated by update_full_active. Wait, then loop to recheck.
            // Also wake on cancellation so we don't strand here.
            tokio::select! {
                biased;
                _ = state.cancellation_token.cancelled() => {
                    // Loop iteration will observe is_cancelled() and exit.
                    continue;
                }
                _ = notified => continue,
            }
        };

        // ── Step 2: run the op under a panic-safe handle slot ──
        let mut slot_guard = HandleSlotGuard::new();
        slot_guard.set_pending(queued.handle_tx);

        let outcome = run_op(
            queued.op,
            &component,
            &subpath,
            &state,
            &processing_stats,
            &host_ctx,
            &providers,
            full_reprocess,
            live,
        )
        .await;

        let final_outcome = tokio::select! {
            biased;
            _ = state.cancellation_token.cancelled() => HandleOutcome::Cancelled,
            outcome = std::future::ready(outcome) => {
                // Reclassify cancellation-flavored errors when token already
                // fired (covers the "drained but cancellation_token won the
                // race elsewhere" case).
                match outcome {
                    Ok(()) => HandleOutcome::Executed(Ok(())),
                    Err(e) => {
                        if state.cancellation_token.is_cancelled()
                            && e.is_cancelled()
                        {
                            HandleOutcome::Cancelled
                        } else {
                            HandleOutcome::Executed(Err(SharedError::from(e)))
                        }
                    }
                }
            }
        };

        // Pending → Resolved → send. If anything between set_resolved
        // and send_resolved panics at the runtime level, the guard's
        // Drop sends the truthful resolved outcome rather than dropping
        // the Sender.
        slot_guard.set_resolved(final_outcome);
        slot_guard.send_resolved();

        // ── Step 3: clear processing, notify, loop ──
        {
            let mut pending = state.pending.lock();
            if let Some(entry) = pending.map.get_mut(&subpath) {
                entry.processing = false;
            }
            state.pending_changed.notify_waiters();
        }
    }
}

/// Execute a single op. Returns `Result<()>` from the underlying child
/// `run_in_background` / `delete` flow.
async fn run_op<Prof: EngineProfile>(
    op: Op<Prof>,
    component: &Component<Prof>,
    subpath: &StablePath,
    state: &LiveComponentState<Prof>,
    processing_stats: &ProcessingStats,
    host_ctx: &Arc<Prof::HostCtx>,
    providers: &rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
    full_reprocess: bool,
    live: bool,
) -> Result<()> {
    let _ = state; // kept for symmetry / future use
    let child = component.get_child(subpath.clone());
    match op {
        Op::Update {
            processor,
            on_error,
        } => {
            let context = ComponentProcessorContext::new(
                child.clone(),
                None,
                processing_stats.clone(),
                host_ctx.clone(),
                // Mirror `Component::mount`: same on_error stored on the
                // build context so this op's commit-phase GC sweep
                // cascades it to orphan deletes.
                ComponentProcessingAction::new_build(
                    providers.clone(),
                    full_reprocess,
                    live,
                    on_error.clone(),
                    None,
                ),
            );
            let inner_handle = child
                .run_in_background(processor, context, on_error, None)
                .await?;
            inner_handle.ready().await
        }
        Op::Delete { on_error } => {
            let context = child.new_processor_context_for_delete(
                providers.clone(),
                None,
                processing_stats.clone(),
                host_ctx.clone(),
                on_error,
            );
            let inner_handle = child.delete(context, None)?;
            inner_handle.ready().await
        }
    }
}

/// RAII guard for `update_full_active`. Sets the flag on construction;
/// clears + notifies on Drop, on every exit path (including panics).
struct UpdateFullActiveGuard<'a, Prof: EngineProfile> {
    state: &'a LiveComponentState<Prof>,
}

impl<'a, Prof: EngineProfile> UpdateFullActiveGuard<'a, Prof> {
    fn new(state: &'a LiveComponentState<Prof>) -> Self {
        {
            let mut pending = state.pending.lock();
            debug_assert!(
                !pending.update_full_active,
                "update_full_active already set — concurrent update_full?"
            );
            pending.update_full_active = true;
        }
        // No notify needed on set — drain tasks gate on this in their next
        // iteration; setting it now means the next gate-check fires.
        state.pending_changed.notify_waiters();
        Self { state }
    }
}

impl<'a, Prof: EngineProfile> Drop for UpdateFullActiveGuard<'a, Prof> {
    fn drop(&mut self) {
        {
            let mut pending = self.state.pending.lock();
            pending.update_full_active = false;
        }
        // Wake drain tasks waiting in step 1.
        self.state.pending_changed.notify_waiters();
    }
}

/// Wrap a `oneshot::Receiver<HandleOutcome>` as a `ComponentExecutionHandle`.
/// Maps the four outcome shapes to the legacy `Result<()>`:
///   - `Executed(Ok(()))`   → `Ok(())`
///   - `Executed(Err(e))`   → `Err(e)`
///   - `Superseded`         → `Ok(())`
///   - `Cancelled`          → `Err(<cancelled>)`
///   - oneshot closed (drain panicked) → `Err(<cancelled>)`
fn make_handle(rx: oneshot::Receiver<HandleOutcome>) -> ComponentExecutionHandle {
    ComponentExecutionHandle::new(async move {
        match rx.await {
            Ok(HandleOutcome::Executed(Ok(()))) => cocoindex_utils::error::shared_ok(()),
            Ok(HandleOutcome::Executed(Err(e))) => Err(e),
            Ok(HandleOutcome::Superseded) => cocoindex_utils::error::shared_ok(()),
            Ok(HandleOutcome::Cancelled) => Err(SharedError::from(make_cancelled_error())),
            Err(_closed) => Err(SharedError::from(make_cancelled_error())),
        }
    })
}

/// Construct a cancellation-flavored error.
///
/// Uses the typed `Error::cancelled()` constructor: at the PyO3 boundary,
/// this maps to Python's `asyncio.CancelledError`, so callers can `except`
/// it idiomatically. `Error::is_cancelled()` returns `true` from any layer,
/// so the drain task's reclassification logic can detect it via the typed
/// API rather than string-matching.
fn make_cancelled_error() -> Error {
    Error::cancelled()
}
