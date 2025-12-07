use crate::engine::effect_exec::commit_effects;
use crate::engine::profile::EngineProfile;
use crate::prelude::*;

use crate::engine::context::{AppContext, ComponentBuilderContext};
use crate::state::state_path::StatePath;

pub trait ComponentBuilder<Prof: EngineProfile>: Send + 'static {
    // TODO: Add method to expose function info and arguments, for tracing purpose & no-change detection.

    /// Run the logic to build the component.
    ///
    /// We expect the implementation of this method to spawn the logic to a separate thread or task when needed.
    #[allow(async_fn_in_trait)]
    async fn build(
        &self,
        context: &ComponentBuilderContext<Prof>,
    ) -> Result<Result<Prof::ComponentBuildRet, Prof::Error>>;
}

pub struct Component<Prof: EngineProfile> {
    app_ctx: Arc<AppContext<Prof>>,
    state_path: StatePath,
    builder: Prof::ComponentBld,
}

impl<Prof: EngineProfile> Component<Prof> {
    pub fn new(
        app_ctx: Arc<AppContext<Prof>>,
        state_path: StatePath,
        builder: Prof::ComponentBld,
    ) -> Self {
        Self {
            app_ctx,
            state_path,
            builder,
        }
    }

    pub fn app_ctx(&self) -> &Arc<AppContext<Prof>> {
        &self.app_ctx
    }

    pub fn state_path(&self) -> &StatePath {
        &self.state_path
    }

    pub async fn build(
        &self,
        parent_context: Option<Arc<ComponentBuilderContext<Prof>>>,
    ) -> Result<Result<Prof::ComponentBuildRet, Prof::Error>> {
        // TODO: Skip building and reuse cached result if the component is already built and up to date.

        let builder_context = ComponentBuilderContext::new(
            self.app_ctx.clone(),
            self.state_path.clone(),
            parent_context,
        );
        let ret = self.builder.build(&builder_context).await?;
        commit_effects(&builder_context).await?;
        Ok(ret)
    }
}
