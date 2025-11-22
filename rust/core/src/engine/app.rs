use crate::engine::component::{Component, ComponentBuilder};
use crate::engine::state_context::{AppContext, StateContext};
use crate::prelude::*;

use crate::engine::environment::{AppRegistration, Environment};
use crate::state::state_path::StatePath;

pub struct App<CompBld: ComponentBuilder> {
    app_ctx: Arc<AppContext>,
    root_component: Component<CompBld>,
}

impl<CompBld: ComponentBuilder> App<CompBld> {
    pub fn new(
        name: &str,
        env: Environment,
        root_component_builder: CompBld,
        host_state_context: CompBld::HostStateCtx,
    ) -> Result<Self> {
        let app_reg = AppRegistration::new(name, &env)?;
        let app_ctx = Arc::new(AppContext { env, app_reg });
        let state_context = StateContext {
            app_ctx: app_ctx.clone(),
            state_path: StatePath::root(),
            host_state_context,
        };
        let root_component = Component::new(Arc::new(state_context), root_component_builder);
        Ok(Self {
            app_ctx,
            root_component,
        })
    }
}

impl<CompBld: ComponentBuilder> App<CompBld> {
    pub async fn run(&self) -> Result<()> {
        self.root_component.build().await?;
        Ok(())
    }
}
