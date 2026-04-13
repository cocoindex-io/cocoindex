use crate::engine::profile::EngineProfile;
use crate::engine::stats::{ProcessingStats, VersionedProcessingStats};
use crate::prelude::*;

use crate::engine::component::Component;
use crate::engine::context::AppContext;

use crate::engine::environment::{AppRegistration, Environment};
use crate::engine::runtime::{get_runtime, global_cancellation_token};
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
    pub fn new(
        name: &str,
        env: Environment<Prof>,
        max_inflight_components: Option<usize>,
    ) -> Result<Self> {
        let app_reg = AppRegistration::new(name, &env)?;

        // TODO: This database initialization logic should happen lazily on first call to `update()`.
        let db = {
            let mut wtxn = env.db_env().write_txn()?;
            let db = env.db_env().create_database(&mut wtxn, Some(name))?;
            wtxn.commit()?;
            db
        };

        let app_ctx = AppContext::new(env, db, app_reg, max_inflight_components);
        let root_component = Component::new(app_ctx, StablePath::root(), None);
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
    ) -> Result<AppOpHandle<Prof::FunctionData>> {
        let processing_stats = ProcessingStats::new();
        let version_rx = processing_stats.subscribe();
        let context = self.root_component.new_processor_context_for_build(
            None,
            processing_stats.clone(),
            options.full_reprocess,
            options.live,
            host_ctx,
        )?;

        let root_component = self.root_component.clone();
        let stats_for_task = processing_stats.clone();
        let cancel_token = global_cancellation_token();
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

        Ok(AppOpHandle {
            task,
            stats: processing_stats,
            version_rx,
            live,
        })
    }

    /// Drop the app, reverting all target states and clearing the database.
    ///
    /// Returns an `AppOpHandle<()>` for tracking progress and awaiting completion.
    /// Synchronous setup (cancellation, context construction) happens before the spawn.
    #[instrument(name = "app.drop", skip_all, fields(app_name = %self.app_ctx().app_reg().name()))]
    pub fn drop_app(&self, host_ctx: Arc<Prof::HostCtx>) -> Result<AppOpHandle<()>> {
        self.app_ctx().cancellation_token().cancel();

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

        let context = self.root_component.new_processor_context_for_delete(
            providers,
            None,
            processing_stats.clone(),
            host_ctx,
        );

        let root_component = self.root_component.clone();
        let stats_for_task = processing_stats.clone();
        let span = Span::current();
        let task = get_runtime().spawn(
            async move {
                // Delete the root component
                let handle = root_component.clone().delete(context.clone(), None)?;

                // Wait for the drop operation to complete
                handle.ready().await?;

                // Clear the database
                let db = root_component.app_ctx().db().clone();
                root_component
                    .app_ctx()
                    .env()
                    .txn_batcher()
                    .run(move |wtxn| Ok(db.clear(wtxn)?))
                    .await?;

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
