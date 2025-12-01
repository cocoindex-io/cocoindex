use std::{fmt::Debug, hash::Hash};

use crate::engine::{
    component::ComponentBuilder,
    effect::{EffectReconciler, EffectSink},
};
use crate::prelude::*;

pub trait Persist: Sized {
    type Error;

    fn to_bytes(&self) -> Result<bytes::Bytes, Self::Error>;

    fn from_bytes(data: &[u8]) -> Result<Self, Self::Error>;
}

pub trait StableFingerprint {
    fn stable_fingerprint(&self) -> utils::fingerprint::Fingerprint;
}

pub trait EngineProfile: Debug + Clone + PartialEq + Eq + Hash {
    type Error: Send + Sync + std::error::Error + 'static;

    type HostStateCtx: Send + Sync + Clone;

    type ComponentBld: ComponentBuilder<Self>;
    type ComponentBuildRet: Send;

    type EffectRcl: EffectReconciler<Self>;
    type EffectKey: Clone
        + std::fmt::Debug
        + Send
        + Eq
        + Hash
        + Persist<Error = Self::Error>
        + StableFingerprint
        + 'static;
    type EffectState: Clone + Send + Persist<Error = Self::Error> + 'static;
    type EffectAction: Send + 'static;
    type EffectSink: EffectSink<Self>;
    type EffectDecl;
}
