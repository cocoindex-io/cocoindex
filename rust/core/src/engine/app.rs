use crate::engine::profile::EngineProfile;
use crate::prelude::*;

use crate::engine::component::Component;
use crate::engine::context::AppContext;

use crate::engine::environment::{AppRegistration, Environment};
use crate::state::stable_path::StablePath;

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
    pub async fn run(&self, root_processor: Prof::ComponentProc) -> Result<Prof::ComponentProcRet> {
        self.root_component
            .clone()
            .run(root_processor, None)?
            .result(None)
            .await
    }

    pub fn app_ctx(&self) -> &AppContext<Prof> {
        self.root_component.app_ctx()
    }
}
