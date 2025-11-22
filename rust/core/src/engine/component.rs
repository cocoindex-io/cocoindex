use crate::prelude::*;

use crate::engine::state_context::StateContext;

#[derive(Clone)]
pub struct ComponentBuilderContext<HostStateCtx> {
    pub state_context: Arc<StateContext<HostStateCtx>>,
    // TODO: Add fields to record effects, states, children components, etc.
}

#[async_trait]
pub trait ComponentBuilder: Clone + Send + 'static {
    type HostStateCtx: Send + Sync + Clone;
    type BuildRet: Send;

    // TODO: Add method to expose function info and arguments, for tracing purpose & no-change detection.

    async fn build(
        &self,
        context: &ComponentBuilderContext<Self::HostStateCtx>,
    ) -> Result<Self::BuildRet>;
}

pub struct Component<Bld: ComponentBuilder> {
    state_context: Arc<StateContext<Bld::HostStateCtx>>,
    builder: Bld,
}

impl<Bld: ComponentBuilder> Component<Bld> {
    pub fn new(state_context: Arc<StateContext<Bld::HostStateCtx>>, builder: Bld) -> Self {
        Self {
            state_context,
            builder,
        }
    }

    pub async fn build(&self) -> Result<Bld::BuildRet> {
        let builder_context = ComponentBuilderContext {
            state_context: self.state_context.clone(),
        };
        let builder = self.builder.clone();

        // TODO: Skip building and reuse cached result if the component is already built and up to date.
        let result = tokio::spawn(async move { builder.build(&builder_context).await }).await??;
        Ok(result)
    }
}
