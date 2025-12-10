use std::collections::BTreeMap;

use crate::engine::component::ComponentBgChildReadiness;
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
    pub provider_registry: EffectProviderRegistry<Prof>,
}

pub(crate) struct ComponentProcessorContextInner<Prof: EngineProfile> {
    pub app_ctx: Arc<AppContext<Prof>>,
    pub state_path: StatePath,
    pub parent_context: Option<Arc<ComponentProcessorContextInner<Prof>>>,

    pub effect: Mutex<ComponentEffectContext<Prof>>,
    pub components_readiness: Arc<ComponentBgChildReadiness>,
    // TODO: Add fields to record states, children components, etc.
}

#[derive(Clone)]
pub struct ComponentProcessorContext<Prof: EngineProfile> {
    pub(crate) inner: Arc<ComponentProcessorContextInner<Prof>>,
}

impl<Prof: EngineProfile> ComponentProcessorContext<Prof> {
    pub fn new(
        app_ctx: Arc<AppContext<Prof>>,
        state_path: StatePath,
        providers: rpds::HashTrieMapSync<EffectPath, EffectProvider<Prof>>,
        parent_context: Option<ComponentProcessorContext<Prof>>,
    ) -> Self {
        Self {
            inner: Arc::new(ComponentProcessorContextInner {
                app_ctx,
                state_path,
                effect: Mutex::new(ComponentEffectContext {
                    declared_effects: Default::default(),
                    provider_registry: EffectProviderRegistry::new(providers),
                }),
                parent_context: parent_context.map(|c| c.inner),
                components_readiness: Default::default(),
            }),
        }
    }

    pub fn app_ctx(&self) -> &Arc<AppContext<Prof>> {
        &self.inner.app_ctx
    }

    pub fn state_path(&self) -> &StatePath {
        &self.inner.state_path
    }

    pub(crate) fn effect(&self) -> &Mutex<ComponentEffectContext<Prof>> {
        &self.inner.effect
    }

    pub(crate) fn components_readiness(&self) -> &Arc<ComponentBgChildReadiness> {
        &self.inner.components_readiness
    }
}
