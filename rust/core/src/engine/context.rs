use std::collections::BTreeMap;

use crate::engine::effect::EffectProvider;
use crate::engine::profile::EngineProfile;
use crate::prelude::*;

use crate::state::effect_path::EffectPath;
use crate::{
    engine::environment::{AppRegistration, Environment},
    state::state_path::StatePath,
};

pub struct AppContext<Prof: EngineProfile> {
    pub env: Environment<Prof>,
    pub db: heed::Database<db_schema::DbEntryKey, heed::types::Bytes>,
    pub app_reg: AppRegistration<Prof>,
}

pub struct DeclaredEffect<Prof: EngineProfile> {
    /// The state path for the component that mounts the effect to.
    pub mounted_state_path: StatePath,

    pub provider: EffectProvider<Prof>,
    pub key: Prof::EffectKey,
    pub decl: Prof::EffectDecl,
}

pub(crate) struct ComponentBuilderContextInner<Prof: EngineProfile> {
    pub app_ctx: Arc<AppContext<Prof>>,
    pub state_path: StatePath,

    pub declared_effects: Mutex<BTreeMap<EffectPath, DeclaredEffect<Prof>>>,
    // TODO: Add fields to record states, children components, etc.
}

#[derive(Clone)]
pub struct ComponentBuilderContext<Prof: EngineProfile> {
    pub(crate) inner: Arc<ComponentBuilderContextInner<Prof>>,
}

impl<Prof: EngineProfile> ComponentBuilderContext<Prof> {
    pub fn new(app_ctx: Arc<AppContext<Prof>>, state_path: StatePath) -> Self {
        Self {
            inner: Arc::new(ComponentBuilderContextInner {
                app_ctx,
                state_path,
                declared_effects: Default::default(),
            }),
        }
    }

    pub fn app_ctx(&self) -> &Arc<AppContext<Prof>> {
        &self.inner.app_ctx
    }

    pub fn state_path(&self) -> &StatePath {
        &self.inner.state_path
    }
}
