use std::collections::BTreeMap;

use crate::engine::component::{Component, ComponentBgChildReadiness};
use crate::engine::effect::{EffectProvider, EffectProviderRegistry};
use crate::engine::profile::EngineProfile;
use crate::prelude::*;

use crate::state::effect_path::EffectPath;
use crate::state::stable_path_set::ChildStablePathSet;
use crate::{
    engine::environment::{AppRegistration, Environment},
    state::stable_path::StablePath,
};

struct AppContextInner<Prof: EngineProfile> {
    env: Environment<Prof>,
    db: db_schema::Database,
    app_reg: AppRegistration<Prof>,
}

#[derive(Clone)]
pub struct AppContext<Prof: EngineProfile> {
    inner: Arc<AppContextInner<Prof>>,
}

impl<Prof: EngineProfile> AppContext<Prof> {
    pub fn new(
        env: Environment<Prof>,
        db: db_schema::Database,
        app_reg: AppRegistration<Prof>,
    ) -> Self {
        Self {
            inner: Arc::new(AppContextInner { env, db, app_reg }),
        }
    }

    pub fn env(&self) -> &Environment<Prof> {
        &self.inner.env
    }

    pub fn db(&self) -> &db_schema::Database {
        &self.inner.db
    }

    pub fn app_reg(&self) -> &AppRegistration<Prof> {
        &self.inner.app_reg
    }
}

pub(crate) struct DeclaredEffect<Prof: EngineProfile> {
    pub provider: EffectProvider<Prof>,
    pub key: Prof::EffectKey,
    pub value: Prof::EffectValue,
    pub child_provider: Option<EffectProvider<Prof>>,
}

pub(crate) struct ComponentEffectContext<Prof: EngineProfile> {
    pub declared_effects: BTreeMap<EffectPath, DeclaredEffect<Prof>>,
    pub provider_registry: EffectProviderRegistry<Prof>,
}

pub(crate) struct ComponentBuildingState<Prof: EngineProfile> {
    pub effect: ComponentEffectContext<Prof>,
    pub child_path_set: ChildStablePathSet,
}

pub(crate) struct ComponentDeleteContext<Prof: EngineProfile> {
    pub providers: rpds::HashTrieMapSync<EffectPath, EffectProvider<Prof>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ComponentProcessingMode {
    Build,
    Delete,
}

pub(crate) enum ComponentProcessingAction<Prof: EngineProfile> {
    Build(Mutex<Option<ComponentBuildingState<Prof>>>),
    Delete(ComponentDeleteContext<Prof>),
}

struct ComponentProcessorContextInner<Prof: EngineProfile> {
    component: Component<Prof>,
    parent_context: Option<ComponentProcessorContext<Prof>>,
    processing_action: ComponentProcessingAction<Prof>,
    components_readiness: ComponentBgChildReadiness,
    // TODO: Add fields to record states, children components, etc.
}

#[derive(Clone)]
pub struct ComponentProcessorContext<Prof: EngineProfile> {
    inner: Arc<ComponentProcessorContextInner<Prof>>,
}

impl<Prof: EngineProfile> ComponentProcessorContext<Prof> {
    pub(crate) fn new(
        component: Component<Prof>,
        providers: rpds::HashTrieMapSync<EffectPath, EffectProvider<Prof>>,
        parent_context: Option<ComponentProcessorContext<Prof>>,
        mode: ComponentProcessingMode,
    ) -> Self {
        let processing_state = if mode == ComponentProcessingMode::Build {
            ComponentProcessingAction::Build(Mutex::new(Some(ComponentBuildingState {
                effect: ComponentEffectContext {
                    declared_effects: Default::default(),
                    provider_registry: EffectProviderRegistry::new(providers),
                },
                child_path_set: Default::default(),
            })))
        } else {
            ComponentProcessingAction::Delete(ComponentDeleteContext { providers })
        };
        Self {
            inner: Arc::new(ComponentProcessorContextInner {
                component,
                parent_context,
                processing_action: processing_state,
                components_readiness: Default::default(),
            }),
        }
    }

    pub fn component(&self) -> &Component<Prof> {
        &self.inner.component
    }

    pub fn app_ctx(&self) -> &AppContext<Prof> {
        self.inner.component.app_ctx()
    }

    pub fn stable_path(&self) -> &StablePath {
        self.inner.component.stable_path()
    }

    pub(crate) fn parent_context(&self) -> Option<&ComponentProcessorContext<Prof>> {
        self.inner.parent_context.as_ref()
    }

    pub(crate) fn update_building_state<T>(
        &self,
        f: impl FnOnce(&mut ComponentBuildingState<Prof>) -> Result<T>,
    ) -> Result<T> {
        match &self.inner.processing_action {
            ComponentProcessingAction::Build(building_state) => {
                let mut building_state = building_state.lock().unwrap();
                let Some(building_state) = &mut *building_state else {
                    bail!(
                        "Processing for the component at {} is already finished",
                        self.stable_path()
                    );
                };
                f(building_state)
            }
            ComponentProcessingAction::Delete { .. } => {
                bail!(
                    "Processing for the component at {} is for deletion only",
                    self.stable_path()
                )
            }
        }
    }

    pub(crate) fn processing_state(&self) -> &ComponentProcessingAction<Prof> {
        &self.inner.processing_action
    }

    pub(crate) fn components_readiness(&self) -> &ComponentBgChildReadiness {
        &self.inner.components_readiness
    }

    pub(crate) fn mode(&self) -> ComponentProcessingMode {
        match &self.inner.processing_action {
            ComponentProcessingAction::Build(_) => ComponentProcessingMode::Build,
            ComponentProcessingAction::Delete { .. } => ComponentProcessingMode::Delete,
        }
    }
}
