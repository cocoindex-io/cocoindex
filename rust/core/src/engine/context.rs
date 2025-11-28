use crate::engine::profile::EngineProfile;
use crate::prelude::*;

use crate::{
    engine::environment::{AppRegistration, Environment},
    state::state_path::StatePath,
};

pub struct AppContext<Prof: EngineProfile> {
    pub env: Environment<Prof>,
    pub app_reg: AppRegistration<Prof>,
}

pub struct ComponentBuilderContext<Prof: EngineProfile> {
    pub app_ctx: Arc<AppContext<Prof>>,
    pub state_path: StatePath,
    // TODO: Add fields to record effects, states, children components, etc.
}
