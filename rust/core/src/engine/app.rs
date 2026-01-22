use crate::engine::profile::EngineProfile;
use crate::engine::stats::{ProcessingStats, ProgressReporter};
use crate::prelude::*;

use crate::engine::component::Component;
use crate::engine::context::AppContext;

use crate::engine::environment::{AppRegistration, Environment};
use crate::state::stable_path::StablePath;

/// Options for updating an app.
#[derive(Debug, Clone, Default)]
pub struct AppUpdateOptions {
    /// If true, periodically report processing stats to stdout.
    pub report_to_stdout: bool,
}

/// Options for dropping an app.
#[derive(Debug, Clone, Default)]
pub struct AppDropOptions {
    pub report_to_stdout: bool,
}

pub struct App<Prof: EngineProfile> {
    root_component: Component<Prof>,
}

impl<Prof: EngineProfile> App<Prof> {
    pub fn new(name: &str, env: Environment<Prof>) -> Result<Self> {
        let app_reg = AppRegistration::new(name, &env)?;

        let db = {
            let mut wtxn = env.db_env().write_txn()?;
            let db = env.db_env().create_database(&mut wtxn, Some(name))?;
            wtxn.commit()?;
            db
        };

        let app_ctx = AppContext::new(env, db, app_reg);
        let root_component = Component::new(app_ctx, StablePath::root());
        Ok(Self { root_component })
    }
}

impl<Prof: EngineProfile> App<Prof> {
    #[instrument(name = "app.update", skip_all, fields(app_name = %self.app_ctx().app_reg().name()))]
    pub async fn update(
        &self,
        root_processor: Prof::ComponentProc,
        options: AppUpdateOptions,
    ) -> Result<Prof::FunctionData> {
        let processing_stats = ProcessingStats::default();
        let context = self
            .root_component
            .new_processor_context_for_build(None, processing_stats.clone())?;

        let run_fut = async {
            self.root_component
                .clone()
                .run(root_processor, context)?
                .result(None)
                .await
        };

        if options.report_to_stdout {
            let reporter = ProgressReporter::new(processing_stats);
            reporter.run_with_progress(run_fut).await
        } else {
            run_fut.await
        }
    }

    /// Drop the app, reverting all target states and clearing the database.
    ///
    /// This method:
    /// 1. Deletes the root component (which cascades to delete all child components and their target states)
    /// 2. Waits for deletion to complete
    /// 3. Clears the app's database
    #[instrument(name = "app.drop", skip_all, fields(app_name = %self.app_ctx().app_reg().name()))]
    pub async fn drop_app(&self, options: AppDropOptions) -> Result<()> {
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
        );

        let drop_fut = async {
            // Delete the root component
            let handle = self.root_component.clone().delete(context.clone())?;

            // Wait for the drop operation to complete
            handle.ready().await?;

            // Clear the database
            let db_env = self.app_ctx().env().db_env();
            let mut wtxn = db_env.write_txn()?;
            self.app_ctx().db().clear(&mut wtxn)?;
            wtxn.commit()?;

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
