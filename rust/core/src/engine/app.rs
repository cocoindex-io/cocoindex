use crate::engine::profile::EngineProfile;
use crate::engine::stats::{ProcessingStats, ProgressReporter, VersionedProcessingStats};
use crate::prelude::*;

use crate::engine::component::Component;
use crate::engine::context::AppContext;

use crate::engine::environment::{AppRegistration, Environment};
use crate::engine::runtime::get_runtime;
use crate::state::stable_path::StablePath;
use tokio::sync::watch;

/// Options for updating an app.
#[derive(Debug, Clone, Default)]
pub struct AppUpdateOptions {
    /// If true, periodically report processing stats to stdout.
    pub report_to_stdout: bool,
    /// If true, reprocess everything and invalidate existing caches.
    pub full_reprocess: bool,
}

/// Options for dropping an app.
#[derive(Debug, Clone, Default)]
pub struct AppDropOptions {
    pub report_to_stdout: bool,
}

/// Handle returned by `App::update` that provides access to the running update's
/// stats and result.
pub struct UpdateHandle<Prof: EngineProfile> {
    task: tokio::task::JoinHandle<Result<Prof::FunctionData>>,
    stats: ProcessingStats,
    version_rx: watch::Receiver<u64>,
}

impl<Prof: EngineProfile> UpdateHandle<Prof> {
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
            .map_err(|_| internal_error!("update task dropped"))?;
        Ok(*self.version_rx.borrow())
    }

    /// Awaits the task completion and returns the result.
    pub async fn result(self) -> Result<Prof::FunctionData> {
        self.task
            .await
            .map_err(|e| internal_error!("update task panicked: {e}"))?
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
        let root_component = Component::new(app_ctx, StablePath::root());
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
    ) -> Result<UpdateHandle<Prof>> {
        let processing_stats = ProcessingStats::new();
        let version_rx = processing_stats.subscribe();
        let context = self.root_component.new_processor_context_for_build(
            None,
            processing_stats.clone(),
            options.full_reprocess,
            host_ctx,
        )?;

        let root_component = self.root_component.clone();
        let stats_for_task = processing_stats.clone();
        let span = Span::current();
        let task = get_runtime().spawn(
            async move {
                let run_fut = async {
                    root_component
                        .run(root_processor, context)
                        .await?
                        .result(None)
                        .await
                };
                let result = if options.report_to_stdout {
                    let reporter = ProgressReporter::new(stats_for_task.clone());
                    reporter.run_with_progress(run_fut).await
                } else {
                    run_fut.await
                };
                stats_for_task.notify_terminated();
                result
            }
            .instrument(span),
        );

        Ok(UpdateHandle {
            task,
            stats: processing_stats,
            version_rx,
        })
    }

    /// Drop the app, reverting all target states and clearing the database.
    ///
    /// This method:
    /// 1. Deletes the root component (which cascades to delete all child components and their target states)
    /// 2. Waits for deletion to complete
    /// 3. Clears the app's database
    #[instrument(name = "app.drop", skip_all, fields(app_name = %self.app_ctx().app_reg().name()))]
    pub async fn drop_app(
        &self,
        options: AppDropOptions,
        host_ctx: Arc<Prof::HostCtx>,
    ) -> Result<()> {
        let processing_stats = ProcessingStats::default();
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

        let drop_fut = async {
            // Delete the root component
            let handle = self.root_component.clone().delete(context.clone())?;

            // Wait for the drop operation to complete
            handle.ready().await?;

            // Clear the database
            let db = self.app_ctx().db().clone();
            self.app_ctx()
                .env()
                .txn_batcher()
                .run(move |wtxn| Ok(db.clear(wtxn)?))
                .await?;

            info!("App dropped successfully");
            Ok(())
        };

        if options.report_to_stdout {
            let reporter = ProgressReporter::new(processing_stats);
            reporter.run_with_progress(drop_fut).await
        } else {
            drop_fut.await
        }
    }

    pub fn app_ctx(&self) -> &AppContext<Prof> {
        self.root_component.app_ctx()
    }
}
