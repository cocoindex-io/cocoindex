use std::collections::BTreeMap;

use crate::engine::effect::{EffectProvider, EffectProviderRegistry};
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

pub(crate) struct DeclaredEffect<Prof: EngineProfile> {
    pub provider: EffectProvider<Prof>,
    pub key: Prof::EffectKey,
    pub decl: Prof::EffectDecl,
    pub child_provider: Option<EffectProvider<Prof>>,
}

pub(crate) struct ComponentEffectContext<Prof: EngineProfile> {
    pub declared_effects: BTreeMap<EffectPath, DeclaredEffect<Prof>>,
    pub providers: EffectProviderRegistry<Prof>,
}

pub(crate) struct ComponentBuilderContextInner<Prof: EngineProfile> {
    pub app_ctx: Arc<AppContext<Prof>>,
    pub state_path: StatePath,

    pub effect: Mutex<ComponentEffectContext<Prof>>,
    pub parent_context: Option<Arc<ComponentBuilderContextInner<Prof>>>,
    // TODO: Add fields to record states, children components, etc.
}

#[derive(Clone)]
pub struct ComponentBuilderContext<Prof: EngineProfile> {
    pub(crate) inner: Arc<ComponentBuilderContextInner<Prof>>,
}

impl<Prof: EngineProfile> ComponentBuilderContext<Prof> {
    pub fn new(
        app_ctx: Arc<AppContext<Prof>>,
        state_path: StatePath,
        parent_context: Option<Arc<ComponentBuilderContext<Prof>>>,
    ) -> Self {
        let providers = match &parent_context {
            Some(c) => EffectProviderRegistry::new(Some(&c.inner.effect.lock().unwrap().providers)),
            None => {
                EffectProviderRegistry::new(Some(&app_ctx.env.effect_providers().lock().unwrap()))
            }
        };
        Self {
            inner: Arc::new(ComponentBuilderContextInner {
                app_ctx,
                state_path,
                effect: Mutex::new(ComponentEffectContext {
                    declared_effects: Default::default(),
                    providers,
                }),
                parent_context: parent_context.map(|c| c.inner.clone()),
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
