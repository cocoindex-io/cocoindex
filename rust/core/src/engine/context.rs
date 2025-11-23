use crate::prelude::*;

use crate::{
    engine::environment::{AppRegistration, Environment},
    state::state_path::StatePath,
};

pub struct AppContext {
    pub env: Environment,
    pub app_reg: AppRegistration,
}

pub struct ComponentBuilderContext {
    pub app_ctx: Arc<AppContext>,
    pub state_path: StatePath,
    // TODO: Add fields to record effects, states, children components, etc.
}
