use crate::engine::effect_exec::commit_effects;
use crate::engine::profile::EngineProfile;
use crate::prelude::*;

use crate::engine::context::{AppContext, ComponentProcessorContext};
use crate::state::state_path::StatePath;

pub trait ComponentProcessor<Prof: EngineProfile>: Send + 'static {
    // TODO: Add method to expose function info and arguments, for tracing purpose & no-change detection.

    /// Run the logic to build the component.
    ///
    /// We expect the implementation of this method to spawn the logic to a separate thread or task when needed.
    fn process(
        &self,
        context: &ComponentProcessorContext<Prof>,
    ) -> Result<impl Future<Output = Result<Prof::ComponentProcRet, Prof::Error>> + Send + 'static>;
}

pub struct Component<Prof: EngineProfile> {
    app_ctx: Arc<AppContext<Prof>>,
    state_path: StatePath,
    processor: Prof::ComponentProc,
}

impl<Prof: EngineProfile> Component<Prof> {
    pub fn new(
        app_ctx: Arc<AppContext<Prof>>,
        state_path: StatePath,
        builder: Prof::ComponentProc,
    ) -> Self {
        Self {
            app_ctx,
            state_path,
            processor: builder,
        }
    }

    pub fn app_ctx(&self) -> &Arc<AppContext<Prof>> {
        &self.app_ctx
    }

    pub fn state_path(&self) -> &StatePath {
        &self.state_path
    }

    pub async fn process(
        &self,
        parent_context: Option<Arc<ComponentProcessorContext<Prof>>>,
    ) -> Result<Result<Prof::ComponentProcRet, Prof::Error>> {
        // TODO: Skip building and reuse cached result if the component is already built and up to date.

        let processor_context = ComponentProcessorContext::new(
            self.app_ctx.clone(),
            self.state_path.clone(),
            parent_context,
        );
        let ret = self.processor.process(&processor_context)?.await;
        commit_effects(&processor_context).await?;
        Ok(ret)
    }
}
