use std::{fmt::Debug, hash::Hash};

use crate::engine::{
    component::ComponentBuilder,
    effect::{EffectReconciler, EffectSink},
};

pub trait EngineProfile: Debug + Clone + PartialEq + Eq + Hash {
    type HostStateCtx: Send + Sync + Clone;

    type ComponentBld: ComponentBuilder<Self>;
    type ComponentBuildRet: Send;
    type ComponentBuildErr: Send;

    type EffectRcl: EffectReconciler<Self>;
    type EffectKey: Clone + Send + Eq + Hash + 'static;
    type EffectState: Clone + Send + 'static;
    type EffectAction: Send + 'static;
    type EffectSink: EffectSink<Self>;
    type EffectDecl;
}
