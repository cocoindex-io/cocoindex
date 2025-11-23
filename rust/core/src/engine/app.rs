use crate::prelude::*;

use crate::engine::component::{Component, ComponentBuilder};
use crate::engine::context::AppContext;

use crate::engine::environment::{AppRegistration, Environment};
use crate::state::state_path::StatePath;

pub struct App<CompBld: ComponentBuilder> {
    root_component: Component<CompBld>,
}

impl<CompBld: ComponentBuilder> App<CompBld> {
    pub fn new(name: &str, env: Environment, root_component_builder: CompBld) -> Result<Self> {
        let app_reg = AppRegistration::new(name, &env)?;
        let app_ctx = Arc::new(AppContext { env, app_reg });
        let root_component =
            Component::new(app_ctx.clone(), StatePath::root(), root_component_builder);
        Ok(Self { root_component })
    }
}

impl<CompBld: ComponentBuilder> App<CompBld> {
    pub async fn update(&self) -> Result<Result<CompBld::BuildRet, CompBld::BuildErr>> {
        self.root_component.build().await
    }
}
