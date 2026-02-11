//! Sealed `RustProfile` implementing `EngineProfile`.
//! All types here are `pub(crate)` — users never see this module.

#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use cocoindex_core::engine::component::{ComponentProcessor, ComponentProcessorInfo};
use cocoindex_core::engine::context::ComponentProcessorContext;
use cocoindex_core::engine::profile::{EngineProfile, Persist, StableFingerprint};
use cocoindex_core::engine::target_state::{
    ChildTargetDef, TargetActionSink, TargetHandler, TargetReconcileOutput,
};
use cocoindex_utils::fingerprint::{Fingerprint, Fingerprinter};
use serde::{Deserialize, Serialize};

use crate::error::Result;

// ---------------------------------------------------------------------------
// RustProfile — the sealed EngineProfile implementation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub(crate) struct RustProfile;

impl EngineProfile for RustProfile {
    type HostRuntimeCtx = ();
    type ComponentProc = BoxedProcessor;
    type FunctionData = Value;

    type TargetHdl = BoxedHandler;
    type TargetStateKey = Key;
    type TargetStateTrackingRecord = Value;
    type TargetAction = Action;
    type TargetActionSink = BoxedSink;
    type TargetStateValue = Value;
}

// ---------------------------------------------------------------------------
// Value — MessagePack-serialized bytes. Implements Persist.
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct Value(pub(crate) bytes::Bytes);

impl Value {
    pub(crate) fn from_serializable<T: Serialize>(data: &T) -> Result<Self> {
        let encoded = rmp_serde::to_vec_named(data)?;
        Ok(Self(bytes::Bytes::from(encoded)))
    }

    pub(crate) fn deserialize<T: for<'de> Deserialize<'de>>(&self) -> Result<T> {
        let val = rmp_serde::from_slice(&self.0)?;
        Ok(val)
    }

    /// Create a "unit" value (empty tuple).
    pub(crate) fn unit() -> Self {
        Self::from_serializable(&()).expect("unit serialization cannot fail")
    }
}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Value({} bytes)", self.0.len())
    }
}

impl Persist for Value {
    fn to_bytes(&self) -> cocoindex_utils::error::Result<bytes::Bytes> {
        Ok(self.0.clone())
    }

    fn from_bytes(data: &[u8]) -> cocoindex_utils::error::Result<Self> {
        Ok(Self(bytes::Bytes::copy_from_slice(data)))
    }
}

// Safety: Value wraps bytes::Bytes which is Send + Sync.
unsafe impl Send for Value {}
unsafe impl Sync for Value {}

// ---------------------------------------------------------------------------
// Key — Content-addressed key. Implements Persist + StableFingerprint.
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct Key {
    serialized: bytes::Bytes,
    fingerprint: Fingerprint,
}

impl Key {
    pub(crate) fn from_serializable<T: Serialize>(data: &T) -> Result<Self> {
        let serialized = rmp_serde::to_vec_named(data)?;
        let mut fp = Fingerprinter::default();
        fp.write_raw_bytes(&serialized);
        let fingerprint = fp.into_fingerprint();
        Ok(Self {
            serialized: bytes::Bytes::from(serialized),
            fingerprint,
        })
    }
}

impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Key({})", self.fingerprint)
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.serialized == other.serialized
    }
}

impl Eq for Key {}

impl std::hash::Hash for Key {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.fingerprint.hash(state);
    }
}

impl Persist for Key {
    fn to_bytes(&self) -> cocoindex_utils::error::Result<bytes::Bytes> {
        Ok(self.serialized.clone())
    }

    fn from_bytes(data: &[u8]) -> cocoindex_utils::error::Result<Self> {
        let mut fp = Fingerprinter::default();
        fp.write_raw_bytes(data);
        let fingerprint = fp.into_fingerprint();
        Ok(Self {
            serialized: bytes::Bytes::copy_from_slice(data),
            fingerprint,
        })
    }
}

impl StableFingerprint for Key {
    fn stable_fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }
}

// ---------------------------------------------------------------------------
// BoxedProcessor — Type-erased component processor wrapping user closures.
// ---------------------------------------------------------------------------

/// Closure that receives the engine-provided `ComponentProcessorContext` and
/// returns a future producing a `Value`.
type ProcessFn = Box<
    dyn FnOnce(
            ComponentProcessorContext<RustProfile>,
        ) -> Pin<Box<dyn Future<Output = Result<Value>> + Send + 'static>>
        + Send
        + 'static,
>;

pub(crate) struct BoxedProcessor {
    process_fn: std::sync::Mutex<Option<ProcessFn>>,
    memo_fp: Option<Fingerprint>,
    info: Arc<ComponentProcessorInfo>,
}

impl BoxedProcessor {
    pub(crate) fn new(
        process_fn: impl FnOnce(
                ComponentProcessorContext<RustProfile>,
            ) -> Pin<Box<dyn Future<Output = Result<Value>> + Send + 'static>>
            + Send
            + 'static,
        memo_fp: Option<Fingerprint>,
        name: String,
    ) -> Self {
        Self {
            process_fn: std::sync::Mutex::new(Some(Box::new(process_fn))),
            memo_fp,
            info: Arc::new(ComponentProcessorInfo::new(name)),
        }
    }
}

// Safety: process_fn is behind a Mutex and only taken once.
unsafe impl Sync for BoxedProcessor {}

impl ComponentProcessor<RustProfile> for BoxedProcessor {
    fn process(
        &self,
        _host_runtime_ctx: &(),
        comp_ctx: &ComponentProcessorContext<RustProfile>,
    ) -> cocoindex_utils::error::Result<impl Future<Output = cocoindex_utils::error::Result<Value>> + Send + 'static>
    {
        let process_fn = self
            .process_fn
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| cocoindex_utils::error::Error::internal_msg("processor already consumed"))?;
        let fut = process_fn(comp_ctx.clone());
        Ok(async move { fut.await.map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string())) })
    }

    fn memo_key_fingerprint(&self) -> Option<Fingerprint> {
        self.memo_fp
    }

    fn processor_info(&self) -> &ComponentProcessorInfo {
        &self.info
    }
}

// ---------------------------------------------------------------------------
// Action — Reconciliation action.
// ---------------------------------------------------------------------------

pub(crate) enum Action {
    Create(Value),
    Update(Value),
    Delete,
}

// Safety: Action wraps Value which is Send.
unsafe impl Send for Action {}

// ---------------------------------------------------------------------------
// BoxedHandler — Type-erased target handler for reconciliation.
// ---------------------------------------------------------------------------

pub(crate) struct BoxedHandler {
    reconcile_fn: Arc<
        dyn Fn(
                Key,
                Option<Value>,
                &[Value],
                bool,
            ) -> cocoindex_utils::error::Result<Option<TargetReconcileOutput<RustProfile>>>
            + Send
            + Sync,
    >,
}

impl BoxedHandler {
    pub(crate) fn new(
        f: impl Fn(
                Key,
                Option<Value>,
                &[Value],
                bool,
            ) -> cocoindex_utils::error::Result<Option<TargetReconcileOutput<RustProfile>>>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        Self {
            reconcile_fn: Arc::new(f),
        }
    }
}

impl TargetHandler<RustProfile> for BoxedHandler {
    fn reconcile(
        &self,
        key: Key,
        desired_target_state: Option<Value>,
        prev_possible_states: &[Value],
        prev_may_be_missing: bool,
    ) -> cocoindex_utils::error::Result<Option<TargetReconcileOutput<RustProfile>>> {
        (self.reconcile_fn)(key, desired_target_state, prev_possible_states, prev_may_be_missing)
    }
}

// ---------------------------------------------------------------------------
// BoxedSink — Type-erased action sink for batched target state application.
// ---------------------------------------------------------------------------

type SinkFn = Arc<
    dyn Fn(
            Vec<Action>,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = cocoindex_utils::error::Result<
                            Option<Vec<Option<ChildTargetDef<RustProfile>>>>,
                        >,
                    > + Send,
            >,
        > + Send
        + Sync,
>;

#[derive(Clone)]
pub(crate) struct BoxedSink {
    key: usize,
    apply_fn: SinkFn,
}

impl BoxedSink {
    pub(crate) fn new(
        f: impl Fn(
                Vec<Action>,
            ) -> Pin<
                Box<
                    dyn Future<
                            Output = cocoindex_utils::error::Result<
                                Option<Vec<Option<ChildTargetDef<RustProfile>>>>,
                            >,
                        > + Send,
                >,
            > + Send
            + Sync
            + 'static,
    ) -> Self {
        let arc: SinkFn = Arc::new(f);
        // Cast through a thin pointer to get a stable address for equality.
        let key = Arc::as_ptr(&arc) as *const () as usize;
        Self { key, apply_fn: arc }
    }
}

impl PartialEq for BoxedSink {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for BoxedSink {}

impl std::hash::Hash for BoxedSink {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

#[async_trait]
impl TargetActionSink<RustProfile> for BoxedSink {
    async fn apply(
        &self,
        _host_runtime_ctx: &(),
        actions: Vec<Action>,
    ) -> cocoindex_utils::error::Result<Option<Vec<Option<ChildTargetDef<RustProfile>>>>> {
        (self.apply_fn)(actions).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_roundtrip() {
        let original = vec![1u32, 2, 3];
        let v = Value::from_serializable(&original).unwrap();
        let restored: Vec<u32> = v.deserialize().unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn key_fingerprint_deterministic() {
        let k1 = Key::from_serializable(&"hello").unwrap();
        let k2 = Key::from_serializable(&"hello").unwrap();
        assert_eq!(k1.fingerprint, k2.fingerprint);
        assert_eq!(k1, k2);

        let k3 = Key::from_serializable(&"world").unwrap();
        assert_ne!(k1.fingerprint, k3.fingerprint);
    }

    #[test]
    fn value_unit() {
        let v = Value::unit();
        let _: () = v.deserialize().unwrap();
    }
}
