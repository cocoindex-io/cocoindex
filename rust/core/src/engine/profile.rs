use std::{fmt::Debug, hash::Hash};

use crate::engine::{
    component::ComponentProcessor,
    effect::{EffectHandler, EffectSink},
};
use crate::prelude::*;

pub trait Persist: Sized {
    fn to_bytes(&self) -> Result<bytes::Bytes>;

    fn from_bytes(data: &[u8]) -> Result<Self>;
}

pub trait StableFingerprint {
    fn stable_fingerprint(&self) -> utils::fingerprint::Fingerprint;
}

pub trait EngineProfile: Debug + Clone + PartialEq + Eq + Hash + Default + 'static {
    type HostStateCtx: Send + Sync + Clone;

    type ComponentProc: ComponentProcessor<Self>;
    type ComponentProcRet: Send;

    type EffectHdl: EffectHandler<Self>;
    type EffectKey: Clone
        + std::fmt::Debug
        + Send
        + Eq
        + Hash
        + Persist
        + StableFingerprint
        + 'static;
    type EffectState: Clone + Send + Persist + 'static;
    type EffectAction: Send + 'static;
    type EffectSink: EffectSink<Self>;
    type EffectValue: Send + 'static;
}
