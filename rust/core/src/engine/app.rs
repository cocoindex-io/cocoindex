use crate::engine::profile::EngineProfile;
use crate::engine::stats::{ProcessingStats, VersionedProcessingStats};
use crate::prelude::*;

use crate::engine::component::Component;
use crate::engine::context::{AppContext, PreviewActionCollector};
use crate::engine::live_component::{LIVE_COMPONENT_DRAIN_TIMEOUT_SECS, LiveComponentState};

use crate::engine::environment::{AppRegistration, Environment};
use crate::engine::runtime::get_runtime;
use crate::state::stable_path::StablePath;
use tokio::sync::watch;

/// Options for updating an app.
#[derive(Debug, Clone, Default)]
pub struct AppUpdateOptions {
    /// If true, reprocess everything and invalidate existing caches.
    pub full_reprocess: bool,
    /// If true, enable live component mode for this update.
    pub live: bool,
}

/// Handle returned by `App::update` or `App::drop_app` that provides access to
/// the running operation's stats and result.
pub struct AppOpHandle<T: Send + 'static> {
    task: tokio::task::JoinHandle<Result<T>>,
    stats: ProcessingStats,
    version_rx: watch::Receiver<u64>,
    /// Whether this is a live-mode operation (affects progress display).
    pub live: bool,
}

impl<T: Send + 'static> AppOpHandle<T> {
    /// Returns an atomic (version, stats) snapshot.
    pub fn stats_snapshot(&self) -> VersionedProcessingStats {
        self.stats.snapshot()
    }

    /// Returns the underlying `ProcessingStats` (Arc-based, safe to clone).
    pub fn stats(&self) -> &ProcessingStats {
        &self.stats
    }

    /// Waits for the version to change. Returns the new version.
    /// Returns `TERMINATED_VERSION` when the task completes.
    pub async fn changed(&mut self) -> Result<u64> {
        self.version_rx
            .changed()
            .await
            .map_err(|_| internal_error!("operation task dropped"))?;
        Ok(*self.version_rx.borrow())
    }

    /// Waits until the operation terminates, ignoring intermediate changes.
    /// Unlike `changed()`, this only resolves on termination, so callers that
    /// don't care about every update aren't woken on every version bump.
    pub async fn wait_terminated(&self) {
        self.stats.wait_terminated().await;
    }

    /// Awaits the task completion and returns the result.
    pub async fn result(self) -> Result<T> {
        self.task
            .await
            .map_err(|e| internal_error!("operation task panicked: {e}"))?
    }
}

pub struct App<Prof: EngineProfile> {
    root_component: Component<Prof>,
}

impl<Prof: EngineProfile> App<Prof> {
    pub async fn new(
        name: &str,
        env: Environment<Prof>,
        max_inflight_components: Option<usize>,
    ) -> Result<Self> {
        let app_reg = AppRegistration::new(name, &env)?;

        // TODO: This database initialization logic should happen lazily on first call to `update()`.
        let app_store = env.create_app_store(name).await?;

        let app_ctx = AppContext::new(env, app_store, app_reg, max_inflight_components);
        let root_component = Component::new(app_ctx, StablePath::root(), None);
        crate::telemetry::track("app_create");
        Ok(Self { root_component })
    }
}

impl<Prof: EngineProfile> App<Prof> {
    /// Starts an update and returns a handle for tracking progress and awaiting the result.
    ///
    /// The update runs as a spawned Tokio task. The handle provides:
    /// - `stats_snapshot()` for polling current stats
    /// - `changed()` for awaiting stats version changes
    /// - `result()` for awaiting the final result
    #[instrument(name = "app.update", skip_all, fields(app_name = %self.app_ctx().app_reg().name()))]
    pub fn update(
        &self,
        root_processor: Prof::ComponentProc,
        options: AppUpdateOptions,
        host_ctx: Arc<Prof::HostCtx>,
        preview_collector: Option<PreviewActionCollector<Prof>>,
    ) -> Result<(
        AppOpHandle<Prof::FunctionData>,
        Option<PreviewActionCollector<Prof>>,
    )> {
        crate::telemetry::track("app_update");
        // Refresh the app token if a prior operation (e.g. drop_app) cancelled
        // it, so this update starts with a non-cancelled token.
        self.app_ctx().reset_cancellation_token_if_cancelled();
        let processing_stats = ProcessingStats::new();
        let version_rx = processing_stats.subscribe();
        let context = self.root_component.new_processor_context_for_build(
            None,
            processing_stats.clone(),
            options.full_reprocess,
            options.live,
            preview_collector.clone(),
            host_ctx,
            // Root has no installed on_error in Build mode — orphan-delete
            // failures from the root's GC sweep log + swallow. (Cascading
            // a raising on_error from root would equate "any orphan delete
            // failed" with "the whole update failed", which is too strict;
            // tombstones survive for retry on the next reconcile.)
            None,
        )?;

        let root_component = self.root_component.clone();
        let stats_for_task = processing_stats.clone();
        let cancel_token = self.app_ctx().cancellation_token();
        let live = options.live;
        let span = Span::current();
        let task = get_runtime().spawn(
            async move {
                let run_fut = async {
                    root_component
                        .clone()
                        .run(root_processor, context)
                        .await?
                        .result(None)
                        .await
                };
                let result = tokio::select! {
                    result = run_fut => result,
                    _ = cancel_token.cancelled() => Err(internal_error!("Operation cancelled")),
                };
                stats_for_task.notify_ready();
                if live && result.is_ok() {
                    // In live mode, wait for all descendants to finish before signaling termination.
                    root_component.wait_until_inactive().await;
                }
                stats_for_task.notify_terminated();
                result
            }
            .instrument(span),
        );

        Ok((
            AppOpHandle {
                task,
                stats: processing_stats,
                version_rx,
                live,
            },
            preview_collector,
        ))
    }

    /// Drop the app, reverting all target states and clearing the database.
    ///
    /// Returns an `AppOpHandle<()>` for tracking progress and awaiting completion.
    /// Synchronous setup (cancellation, context construction) happens before the spawn.
    ///
    /// **Live-component drain**: atomically cancels the app token AND snapshots
    /// the live-components registry, then awaits each captured controller's
    /// `cancel_and_await_quiescence` + `live_task` JoinHandle (per-component
    /// 30s timeout, all drains in parallel via `join_all`) before tearing down
    /// shared resources. This prevents leaked drain tasks from writing to
    /// half-closed connection pools after teardown — see
    /// `specs/live_component/design.md` "drop_app" section for the full
    /// contract (it is documentation-only when timeouts fire).
    #[instrument(name = "app.drop", skip_all, fields(app_name = %self.app_ctx().app_reg().name()))]
    pub fn drop_app(&self, host_ctx: Arc<Prof::HostCtx>) -> Result<AppOpHandle<()>> {
        crate::telemetry::track("app_drop");
        // Refresh the app token if a prior operation cancelled it, so the
        // cancel below applies to a token shared with any concurrent update.
        self.app_ctx().reset_cancellation_token_if_cancelled();
        // Atomically cancel the app token AND snapshot the live-components
        // registry under the registry lock. This closes the race where a
        // concurrent mount_live_async could register *after* a separate
        // snapshot but *before* observing a separately-issued cancel.
        // Acquiring the lock first ensures any in-flight register-into-list
        // either happens before our snapshot (caught) or queues behind the
        // lock and sees the cancelled token immediately on first poll once
        // we release.
        let live_snapshot = self.app_ctx().cancel_and_snapshot_live_components();

        let processing_stats = ProcessingStats::default();
        let version_rx = processing_stats.subscribe();
        let providers = self
            .app_ctx()
            .env()
            .target_states_providers()
            .lock()
            .unwrap()
            .providers
            .clone();

        // Install a single on_error handler that always propagates: app.drop
        // is an explicit operation, so root-delete failures (and any
        // descendant failures, via the GC-sweep cascade) must surface to the
        // caller (Python `app.drop()` then raises). Without it, the framework
        // default of "log + swallow" would hide failures behind stale tracking
        // records while pretending app.drop succeeded. The handler is stored
        // in the delete context so the GC sweep can read and cascade it to
        // descendant deletes (see `specs/core/error_handling.md`).
        let raise_on_error: crate::engine::component::OnError =
            Arc::new(|err| Box::pin(async move { Err(err) }));
        let context = self.root_component.new_processor_context_for_delete(
            providers,
            None,
            processing_stats.clone(),
            host_ctx,
            Some(raise_on_error),
        );

        let root_component = self.root_component.clone();
        let stats_for_task = processing_stats.clone();
        let span = Span::current();
        let task = get_runtime().spawn(
            async move {
                // ── Live-component drain (must complete BEFORE deleting the
                // root component / clearing the DB, so leaked drain tasks
                // don't race teardown of shared resources) ──
                drain_live_components(live_snapshot).await;

                // Delete the root component (uses on_error from the context).
                let handle = root_component.clone().delete(context.clone(), None)?;

                // Wait for the drop operation to complete
                handle.ready().await?;

                // Drop the per-app state-store data. Clears the per-app
                // sub-database (heed 0.22 doesn't expose `mdb_drop`).
                // Subsumes the previous `clear_all` step — `drop_app`
                // wipes everything `clear_all` would have emptied.
                let app_name = root_component.app_ctx().app_reg().name().to_owned();
                root_component
                    .app_ctx()
                    .env()
                    .storage()
                    .drop_app(&app_name)
                    .await?;

                // Release the env-side `app_names` slot eagerly so a
                // follow-up `App::new(name, …)` (e.g. Python re-using
                // the same `App` instance for `update()` after `drop()`)
                // doesn't trip the "App name already registered" check
                // while pending tokio captures of `Arc<AppContextInner>`
                // are still releasing.
                root_component.app_ctx().app_reg().unregister();

                info!("App dropped successfully");
                stats_for_task.notify_terminated();
                Ok(())
            }
            .instrument(span),
        );

        Ok(AppOpHandle {
            task,
            stats: processing_stats,
            version_rx,
            live: false,
        })
    }

    pub fn app_ctx(&self) -> &AppContext<Prof> {
        self.root_component.app_ctx()
    }
}

/// Drain each live component in parallel with a per-component
/// `LIVE_COMPONENT_DRAIN_TIMEOUT_SECS` timeout (defined in `live_component.rs`,
/// shared with `mount_live_async`'s cancel-and-drain of a prior incarnation).
///
/// On timeout, the drain is leaked — see the "drop_app" contract in
/// `specs/live_component/design.md` (this is supported only when followed
/// by process exit or another `app.update()` of the same `App` instance;
/// new-`App`-after-drop is unsupported).
///
/// Awaits two things per component (inside the same timeout budget):
///  1. `cancel_and_await_quiescence()` — drains the per-subpath workers
///     until `pending` is empty.
///  2. `live_task` JoinHandle — awaits `process_live`'s tokio task fully
///     exiting. `process_live` may have user `finally:` cleanup that
///     touches shared resources directly outside the operator; without
///     this await, `drop_app` could proceed to tear down shared resources
///     while user cleanup is mid-flight.
///
/// Per-component drains run concurrently via `futures::future::join_all`,
/// so total wait is bounded by `max(per-component drain time)` rather than
/// `sum(...)`.
async fn drain_live_components<Prof: EngineProfile>(snapshot: Vec<Arc<LiveComponentState<Prof>>>) {
    if snapshot.is_empty() {
        return;
    }
    let drains = snapshot.into_iter().map(|state| async move {
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(LIVE_COMPONENT_DRAIN_TIMEOUT_SECS),
            async {
                state.cancel_and_await_quiescence().await;
                if let Some(handle) = state.live_task_handle() {
                    let _ = handle.await;
                }
            },
        )
        .await;
    });
    futures::future::join_all(drains).await;
}
