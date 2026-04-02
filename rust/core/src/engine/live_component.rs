use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::engine::component::{
    Component, ComponentBgChildReadinessChildGuard, ComponentExecutionHandle,
};
use crate::engine::context::{ComponentProcessingMode, ComponentProcessorContext, FnCallContext};
use crate::engine::profile::EngineProfile;
use crate::engine::stats::ProcessingStats;
use crate::engine::target_state::TargetStateProvider;
use crate::prelude::*;
use crate::state::stable_path::StablePath;
use crate::state::target_state_path::TargetStatePath;
use cocoindex_utils::error::SharedError;

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

        // 2. If existing live state, cancel and drain it.
        if let Some(existing_state) = child.live_state() {
            existing_state.cancellation_token().cancel();
            let handle = existing_state.live_task_handle();
            if let Some(handle) = handle {
                let _ = handle.await;
            }
            existing_state.drain_inflight().await;
        }

        // 3. Create readiness guard from parent's components_readiness.
        let readiness_guard = parent_ctx.components_readiness().clone().add_child();

        // 4. Create cancellation token as child of app's root token.
        let cancellation_token = parent_ctx.app_ctx().cancellation_token().child_token();

        // 5. Create LiveComponentState.
        let state = Arc::new(LiveComponentState::new(readiness_guard, cancellation_token));

        // 6. Store the state in the child component.
        child.set_live_state(state.clone());

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

/// Readiness state. Under a single Mutex so mark_ready() can atomically
/// take the guard and set the ready flag.
pub(crate) struct ReadinessState {
    guard: Option<ComponentBgChildReadinessChildGuard>,
    ready: bool,
}

/// Shared state stored in ComponentInner::live_state.
/// Does NOT reference Component — breaks the cyclic Arc.
/// Lock ordering: all locks (ops, readiness, live_task) are held independently.
pub struct LiveComponentState {
    /// RwLock for serialization only (guards no data).
    /// update_full takes write (exclusive), update/delete take read (concurrent).
    ops: tokio::sync::RwLock<()>,

    /// Readiness state — guard + flag under one lock.
    readiness: Mutex<ReadinessState>,

    /// Signaled once when mark_ready is called. Python side awaits for handle.ready().
    ready_notify: tokio::sync::Notify,

    /// Cancellation token — triggered on re-mount or app shutdown.
    cancellation_token: tokio_util::sync::CancellationToken,

    /// JoinHandle of the tokio task running process_live. Set by start().
    /// Dropping/aborting this cancels the Python task via CancelOnDropPy.
    live_task: Mutex<Option<tokio::task::JoinHandle<()>>>,

    /// In-flight child task tracking. Each spawned task holds an InflightGuard.
    /// update_full() waits for the counter to reach zero before proceeding.
    inflight_counter: AtomicUsize,
    inflight_drained: tokio::sync::Notify,

    /// Global sequence counter for "latest wins" ordering.
    /// Incremented on each update()/delete() invocation.
    /// The per-child latest_seq lives on ComponentInner.
    next_seq: AtomicU64,
}

impl LiveComponentState {
    pub fn new(
        readiness_guard: ComponentBgChildReadinessChildGuard,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            ops: tokio::sync::RwLock::new(()),
            readiness: Mutex::new(ReadinessState {
                guard: Some(readiness_guard),
                ready: false,
            }),
            ready_notify: tokio::sync::Notify::new(),
            cancellation_token,
            live_task: Mutex::new(None),
            inflight_counter: AtomicUsize::new(0),
            inflight_drained: tokio::sync::Notify::new(),
            next_seq: AtomicU64::new(0),
        }
    }

    pub fn cancellation_token(&self) -> &tokio_util::sync::CancellationToken {
        &self.cancellation_token
    }

    /// Take the JoinHandle for the live task (if any).
    pub fn live_task_handle(&self) -> Option<tokio::task::JoinHandle<()>> {
        self.live_task.lock().unwrap().take()
    }

    /// Wait until ready_notify is signaled.
    pub async fn ready_notified(&self) {
        // If already ready, return immediately.
        if self.readiness.lock().unwrap().ready {
            return;
        }
        self.ready_notify.notified().await;
    }

    /// Wait for all in-flight tasks to complete.
    pub async fn drain_inflight(&self) {
        loop {
            if self.inflight_counter.load(Ordering::Acquire) == 0 {
                return;
            }
            self.inflight_drained.notified().await;
        }
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

    /// Resolve readiness with error. Only effective if not already ready.
    /// Drops the guard without calling resolve(), which triggers the Drop impl
    /// that reports a cancellation error to the parent.
    fn resolve_ready_with_error(&self, _err: Error) {
        let mut state = self.readiness.lock().unwrap();
        if !state.ready {
            state.ready = true;
            // Drop the guard without calling resolve — triggers Drop impl
            // which sends a "cancelled" SharedError to the parent.
            state.guard.take();
            self.ready_notify.notify_waiters();
        }
    }
}

/// Returned to Python. Holds Component + shared state.
/// NOT stored in ComponentInner — only Python/PyO3 holds this.
#[derive(Clone)]
pub struct LiveComponentController<Prof: EngineProfile> {
    component: Component<Prof>,
    state: Arc<LiveComponentState>,

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
        state: Arc<LiveComponentState>,
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

    pub fn state(&self) -> &Arc<LiveComponentState> {
        &self.state
    }

    pub fn component(&self) -> &Component<Prof> {
        &self.component
    }

    pub fn is_live(&self) -> bool {
        self.live
    }

    /// Full processing cycle. Exclusive — waits for in-flight update/delete to drain,
    /// then runs process() through execute_once (memoization, submit, wait-children, post-submit).
    pub async fn update_full(&self, processor: Prof::ComponentProc) -> Result<()> {
        // Acquire exclusive lock — waits for all update/delete to release read locks.
        let _write_guard = self.state.ops.write().await;
        // Wait for in-flight child tasks to drain.
        self.state.drain_inflight().await;

        // Create fresh context with no parent.
        let context = ComponentProcessorContext::new(
            self.component.clone(),
            self.providers.clone(),
            None,
            self.processing_stats.clone(),
            ComponentProcessingMode::Build,
            self.full_reprocess,
            self.live,
            self.host_ctx.clone(),
        );

        // Run via run_in_background (no parent context → no readiness guard to grandparent).
        let handle = self
            .component
            .clone()
            .run_in_background(processor, context, None, None)
            .await?;

        // Await full readiness including all children.
        handle.ready().await?;

        Ok(())
    }

    /// Mount a child component incrementally. Concurrent with other update/delete calls.
    /// Waits for any in-flight update_full to complete first.
    pub async fn update(
        &self,
        subpath: StablePath,
        processor: Prof::ComponentProc,
    ) -> Result<ComponentExecutionHandle> {
        let _read_guard = self.state.ops.read().await;

        // Increment inflight counter. It will be decremented when the background
        // task completes (via the wrapper future on ComponentExecutionHandle).
        self.state.inflight_counter.fetch_add(1, Ordering::AcqRel);
        let state_for_drain = self.state.clone();

        let child = self.component.get_child(subpath);
        let seq = self.state.next_seq.fetch_add(1, Ordering::Relaxed);
        child.latest_seq().store(seq, Ordering::Release);

        let context = ComponentProcessorContext::new(
            child.clone(),
            self.providers.clone(),
            None,
            self.processing_stats.clone(),
            ComponentProcessingMode::Build,
            self.full_reprocess,
            self.live,
            self.host_ctx.clone(),
        );

        let child_for_check = child.clone();
        let pre_execute_check: Box<dyn FnOnce() -> bool + Send> =
            Box::new(move || child_for_check.latest_seq().load(Ordering::Acquire) == seq);

        let inner_handle = child
            .run_in_background(processor, context, None, Some(pre_execute_check))
            .await?;

        // Wrap: decrement inflight counter after the inner handle's future completes.
        Ok(ComponentExecutionHandle::new(async move {
            let result = inner_handle.ready().await;
            // Decrement inflight counter.
            if state_for_drain
                .inflight_counter
                .fetch_sub(1, Ordering::AcqRel)
                == 1
            {
                state_for_drain.inflight_drained.notify_waiters();
            }
            result.map_err(SharedError::from)
        }))
    }

    /// Delete a child component. Same concurrency as update().
    pub async fn delete(&self, subpath: StablePath) -> Result<ComponentExecutionHandle> {
        let _read_guard = self.state.ops.read().await;

        self.state.inflight_counter.fetch_add(1, Ordering::AcqRel);
        let state_for_drain = self.state.clone();

        let child = self.component.get_child(subpath.clone());
        let seq = self.state.next_seq.fetch_add(1, Ordering::Relaxed);
        child.latest_seq().store(seq, Ordering::Release);

        let context = ComponentProcessorContext::new(
            child.clone(),
            self.providers.clone(),
            None,
            self.processing_stats.clone(),
            ComponentProcessingMode::Delete,
            false,
            self.live,
            self.host_ctx.clone(),
        );

        let child_for_check = child.clone();
        let pre_execute_check: Box<dyn FnOnce() -> bool + Send> =
            Box::new(move || child_for_check.latest_seq().load(Ordering::Acquire) == seq);

        let inner_handle = child.delete(context, Some(pre_execute_check))?;

        // Wrap: decrement inflight + remove from active_children after completion.
        let component = self.component.clone();
        Ok(ComponentExecutionHandle::new(async move {
            let result = inner_handle.ready().await;
            component.remove_active_child(&subpath);
            // Decrement inflight counter.
            if state_for_drain
                .inflight_counter
                .fetch_sub(1, Ordering::AcqRel)
                == 1
            {
                state_for_drain.inflight_drained.notify_waiters();
            }
            result.map_err(SharedError::from)
        }))
    }

    /// Signal readiness to parent component. Idempotent — subsequent calls are no-ops.
    /// - Live mode: resolves readiness and returns immediately.
    /// - Non-live mode: resolves readiness, cancels the cancellation_token, then
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
            // Non-live mode: cancel the token so select! in start() drops process_live.
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
                result = process_live_fut => result,
                _ = token.cancelled() => Ok(()),
            };
            match result {
                Ok(()) => state.ensure_mark_ready(),
                Err(e) => {
                    if !state.is_ready() {
                        // Error before mark_ready — drop the guard to signal error.
                        state.resolve_ready_with_error(e);
                    } else {
                        error!("process_live failed after mark_ready: {e:?}");
                    }
                }
            }
        });
        *self.state.live_task.lock().unwrap() = Some(handle);
    }

    /// Cancel this controller and wait for full quiescence.
    /// Called by mount_live_async before creating a replacement controller.
    /// 1. Cancels the cancellation_token (causes tokio::select! to drop the
    ///    process_live future → CancelOnDropPy::drop() cancels the Python task).
    /// 2. Awaits the process_live JoinHandle to ensure the task has terminated.
    /// 3. Waits for inflight_counter to drain (in-flight child tasks complete).
    pub async fn cancel_and_drain(&self) {
        // 1. Cancel the token — causes select! to drop process_live future.
        self.state.cancellation_token.cancel();

        // 2. Await the JoinHandle to ensure the task has terminated.
        let handle = self.state.live_task.lock().unwrap().take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }

        // 3. Wait for in-flight child tasks to drain.
        self.state.drain_inflight().await;
    }
}
