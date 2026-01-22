use std::{fmt::Debug, hash::Hash, sync::Arc};

use crate::engine::{
    component::ComponentProcessor,
    target_state::{TargetActionSink, TargetHandler},
};
use crate::prelude::*;

pub trait Persist: Sized {
    fn to_bytes(&self) -> Result<bytes::Bytes>;

    fn from_bytes(data: &[u8]) -> Result<Self>;
}

impl<T: Persist> Persist for Arc<T> {
    fn to_bytes(&self) -> Result<bytes::Bytes> {
        (**self).to_bytes()
    }

    fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(Arc::new(T::from_bytes(data)?))
    }
}

pub trait StableFingerprint {
    fn stable_fingerprint(&self) -> utils::fingerprint::Fingerprint;
}

impl<T: StableFingerprint> StableFingerprint for Arc<T> {
    fn stable_fingerprint(&self) -> utils::fingerprint::Fingerprint {
        (**self).stable_fingerprint()
    }
}

pub trait EngineProfile: Debug + Clone + PartialEq + Eq + Hash + Default + 'static {
    type HostRuntimeCtx: Clone + Send + Sync + 'static;

    type ComponentProc: ComponentProcessor<Self>;
    type FunctionData: Clone + Send + Sync + Persist + 'static;

    type TargetHdl: TargetHandler<Self>;
    type TargetStateKey: Clone
        + std::fmt::Debug
        + Send
        + Eq
        + Hash
        + Persist
        + StableFingerprint
        + 'static;
    type TargetStateTrackingRecord: Send + Persist + 'static;
    type TargetAction: Send + 'static;
    type TargetActionSink: TargetActionSink<Self>;
    type TargetStateValue: Send + 'static;
}
