use crate::engine::profile::EngineProfile;
use crate::engine::stats::{ProcessingStats, ProgressReporter};
use crate::prelude::*;

use crate::engine::component::Component;
use crate::engine::context::AppContext;

use crate::engine::environment::{AppRegistration, Environment};
use crate::state::stable_path::StablePath;

/// Options for running an app.
#[derive(Debug, Clone, Default)]
pub struct AppRunOptions {
    /// If true, periodically report processing stats to stdout.
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
    #[instrument(name = "app.run", skip_all, fields(app_name = %self.app_ctx().app_reg().name()))]
    pub async fn run(
        &self,
        root_processor: Prof::ComponentProc,
        options: AppRunOptions,
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

    pub fn app_ctx(&self) -> &AppContext<Prof> {
        self.root_component.app_ctx()
    }
}
