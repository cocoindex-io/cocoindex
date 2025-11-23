use crate::prelude::*;

use crate::engine::context::{AppContext, ComponentBuilderContext};
use crate::state::state_path::StatePath;

pub trait ComponentBuilder: Send + 'static {
    type HostStateCtx: Send + Sync + Clone;
    type BuildRet: Send;
    type BuildErr: Send;

    // TODO: Add method to expose function info and arguments, for tracing purpose & no-change detection.

    /// Run the logic to build the component.
    ///
    /// We expect the implementation of this method to spawn the logic to a separate thread or task when needed.
    #[allow(async_fn_in_trait)]
    async fn build(
        &self,
        context: &Arc<ComponentBuilderContext>,
    ) -> Result<Result<Self::BuildRet, Self::BuildErr>>;
}

pub struct Component<Bld: ComponentBuilder> {
    app_ctx: Arc<AppContext>,
    state_path: StatePath,
    builder: Bld,
}

impl<Bld: ComponentBuilder> Component<Bld> {
    pub fn new(app_ctx: Arc<AppContext>, state_path: StatePath, builder: Bld) -> Self {
        Self {
            app_ctx,
            state_path,
            builder,
        }
    }

    pub fn app_ctx(&self) -> &Arc<AppContext> {
        &self.app_ctx
    }

    pub fn state_path(&self) -> &StatePath {
        &self.state_path
    }

    pub async fn build(&self) -> Result<Result<Bld::BuildRet, Bld::BuildErr>> {
        // TODO: Skip building and reuse cached result if the component is already built and up to date.

        let builder_context = Arc::new(ComponentBuilderContext {
            app_ctx: self.app_ctx.clone(),
            state_path: self.state_path.clone(),
        });
        let ret = self.builder.build(&builder_context).await?;
        Ok(ret)
    }
}
