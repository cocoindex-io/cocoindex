use crate::engine::profile::EngineProfile;
use crate::prelude::*;

use crate::engine::component::{Component, ComponentBuilder};
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
        let app_ctx = Arc::new(AppContext { env, app_reg });
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
