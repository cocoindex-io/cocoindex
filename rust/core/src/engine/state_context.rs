use crate::prelude::*;

use crate::{
    engine::environment::{AppRegistration, Environment},
    state::state_path::StatePath,
};

pub struct AppContext {
    pub env: Environment,
    pub app_reg: AppRegistration,
}

pub struct StateContext<HostStateCtx> {
    pub app_ctx: Arc<AppContext>,
    pub state_path: StatePath,
    pub host_state_context: HostStateCtx,
}
