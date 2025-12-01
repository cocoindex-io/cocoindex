use crate::engine::profile::EngineProfile;
use crate::prelude::*;

use crate::engine::component::Component;
use crate::engine::context::AppContext;

use crate::engine::environment::{AppRegistration, Environment};
use crate::state::state_path::StatePath;

pub struct App<Prof: EngineProfile> {
    root_component: Component<Prof>,
}

impl<Prof: EngineProfile> App<Prof> {
    pub fn new(
        name: &str,
        env: Environment<Prof>,
        root_component_builder: Prof::ComponentBld,
    ) -> Result<Self> {
        let app_reg = AppRegistration::new(name, &env)?;

        let existing_db = {
            let rtxn = env.db_env().read_txn()?;
            env.db_env().open_database(&rtxn, Some(name))?
        };
        let db = if let Some(db) = existing_db {
            db
        } else {
            let mut wtxn = env.db_env().write_txn()?;
            let db = env.db_env().create_database(&mut wtxn, Some(name))?;
            wtxn.commit()?;
            db
        };

        let app_ctx = Arc::new(AppContext { env, db, app_reg });
        let root_component =
            Component::new(app_ctx.clone(), StatePath::root(), root_component_builder);
        Ok(Self { root_component })
    }
}

impl<Prof: EngineProfile> App<Prof> {
    pub async fn update(&self) -> Result<Result<Prof::ComponentBuildRet, Prof::Error>> {
        self.root_component.build().await
    }
}
