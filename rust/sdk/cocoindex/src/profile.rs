//! Sealed `RustProfile` implementing `EngineProfile`.
//! All types here are `pub(crate)` — users never see this module.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use cocoindex_core::engine::component::{ComponentProcessor, ComponentProcessorInfo};
use cocoindex_core::engine::context::{ComponentProcessorContext, MemoStatesPayload};
use cocoindex_core::engine::profile::{EngineProfile, Persist};
use cocoindex_core::engine::spill::Spillable;
use cocoindex_core::engine::target_state::{
    TargetActionSink, TargetActionWithChildSlot, TargetHandler, TargetReconcileOutput,
};
use cocoindex_core::state::stable_path::StableKey;
use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};

use crate::ctx::ContextStore;
use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// RustProfile — the sealed EngineProfile implementation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub(crate) struct RustProfile;

impl EngineProfile for RustProfile {
    type HostRuntimeCtx = ();
    /// The environment's context store, so target sinks can resolve provided
    /// resources (pools/clients) by their stable key at apply time (§10/§11).
    type HostCtx = ContextStore;
    type ComponentProc = BoxedProcessor;
    type FunctionData = Value;

    type TargetHdl = BoxedHandler;
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
        let encoded = rmp_serde::to_vec(data)?;
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

/// A value is its serialized bytes already, so it spills as is.
impl Spillable for Value {
    fn resident_size(&self) -> usize {
        self.0.len()
    }

    fn to_spill_bytes(&self) -> cocoindex_utils::error::Result<Option<std::borrow::Cow<'_, [u8]>>> {
        Ok(Some(std::borrow::Cow::Borrowed(&self.0)))
    }

    fn from_spill_bytes(bytes: &[u8]) -> cocoindex_utils::error::Result<Self> {
        Ok(Self(bytes::Bytes::copy_from_slice(bytes)))
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
    has_memo_state_handler: bool,
    info: Arc<ComponentProcessorInfo>,
}

impl BoxedProcessor {
    pub(crate) fn new(
        process_fn: impl FnOnce(
            ComponentProcessorContext<RustProfile>,
        )
            -> Pin<Box<dyn Future<Output = Result<Value>> + Send + 'static>>
        + Send
        + 'static,
        memo_fp: Option<Fingerprint>,
        name: String,
    ) -> Self {
        Self {
            process_fn: std::sync::Mutex::new(Some(Box::new(process_fn))),
            memo_fp,
            has_memo_state_handler: false,
            info: Arc::new(ComponentProcessorInfo::new(name)),
        }
    }

    pub(crate) fn with_memo_state_handler(mut self, enabled: bool) -> Self {
        self.has_memo_state_handler = enabled;
        self
    }
}

impl ComponentProcessor<RustProfile> for BoxedProcessor {
    fn process(
        &self,
        _host_runtime_ctx: &(),
        comp_ctx: &ComponentProcessorContext<RustProfile>,
    ) -> cocoindex_utils::error::Result<
        impl Future<Output = cocoindex_utils::error::Result<Value>> + Send + 'static,
    > {
        let process_fn = self
            .process_fn
            .lock()
            .map_err(|_| cocoindex_utils::error::Error::internal_msg("processor state poisoned"))?
            .take()
            .ok_or_else(|| {
                cocoindex_utils::error::Error::internal_msg("processor already consumed")
            })?;
        let fut = process_fn(comp_ctx.clone());
        Ok(async move { fut.await.map_err(crate::error::Error::into_core) })
    }

    fn memo_key_fingerprint(&self) -> Option<Fingerprint> {
        self.memo_fp
    }

    fn has_memo_state_handler(&self) -> bool {
        self.has_memo_state_handler
    }

    fn handle_memo_states(
        &self,
        _host_runtime_ctx: &(),
        comp_ctx: &ComponentProcessorContext<RustProfile>,
        stored_states: Option<MemoStatesPayload<RustProfile>>,
    ) -> cocoindex_utils::error::Result<
        impl Future<
            Output = cocoindex_utils::error::Result<(MemoStatesPayload<RustProfile>, bool, bool)>,
        > + Send
        + 'static,
    > {
        let context = Arc::clone(comp_ctx.host_ctx());
        let initial_context_states = stored_states
            .is_none()
            .then(|| comp_ctx.collect_context_initial_states());
        let stored_states = stored_states.unwrap_or_default();
        Ok(async move {
            let Some(initial_context_states) = initial_context_states else {
                let validation = context
                    .validate_memo_states(&stored_states.by_context_fp)
                    .await
                    .map_err(Error::into_core)?;
                return Ok((
                    MemoStatesPayload {
                        positional: stored_states.positional,
                        by_context_fp: validation.states,
                    },
                    validation.memo_valid,
                    validation.states_changed,
                ));
            };
            Ok((
                MemoStatesPayload {
                    positional: stored_states.positional,
                    by_context_fp: initial_context_states,
                },
                true,
                false,
            ))
        })
    }

    fn processor_info(&self) -> &ComponentProcessorInfo {
        &self.info
    }
}

// ---------------------------------------------------------------------------
// Action — Reconciliation action.
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) enum Action {
    Create(Value),
    Update(Value),
    Delete(Value),
}

/// An action spills as a kind byte followed by its value's bytes.
impl Spillable for Action {
    fn resident_size(&self) -> usize {
        self.value().0.len()
    }

    fn to_spill_bytes(&self) -> cocoindex_utils::error::Result<Option<std::borrow::Cow<'_, [u8]>>> {
        let (kind, value) = match self {
            Self::Create(value) => (0u8, value),
            Self::Update(value) => (1u8, value),
            Self::Delete(value) => (2u8, value),
        };
        let mut bytes = Vec::with_capacity(1 + value.0.len());
        bytes.push(kind);
        bytes.extend_from_slice(&value.0);
        Ok(Some(std::borrow::Cow::Owned(bytes)))
    }

    fn from_spill_bytes(bytes: &[u8]) -> cocoindex_utils::error::Result<Self> {
        let (kind, value) = bytes.split_first().ok_or_else(|| {
            cocoindex_utils::error::Error::internal_msg("empty spilled target action")
        })?;
        let value = Value(bytes::Bytes::copy_from_slice(value));
        Ok(match kind {
            0 => Self::Create(value),
            1 => Self::Update(value),
            2 => Self::Delete(value),
            kind => {
                return Err(cocoindex_utils::error::Error::internal_msg(format!(
                    "unknown spilled target action kind {kind}"
                )));
            }
        })
    }
}

impl Action {
    fn value(&self) -> &Value {
        match self {
            Self::Create(value) | Self::Update(value) | Self::Delete(value) => value,
        }
    }
}

// ---------------------------------------------------------------------------
// BoxedHandler — Type-erased target handler for reconciliation.
// ---------------------------------------------------------------------------

pub(crate) struct BoxedHandler {
    reconcile_fn: Arc<ReconcileFn>,
    attachments_fn: Arc<AttachmentsFn>,
}

type ReconcileFn = dyn Fn(
        StableKey,
        Option<&Value>,
        &[Value],
        bool,
    ) -> cocoindex_utils::error::Result<Option<TargetReconcileOutput<RustProfile>>>
    + Send
    + Sync;

type AttachmentsFn =
    dyn Fn() -> cocoindex_utils::error::Result<Vec<(Arc<str>, BoxedHandler)>> + Send + Sync;

impl BoxedHandler {
    pub(crate) fn new(
        f: impl Fn(
            StableKey,
            Option<&Value>,
            &[Value],
            bool,
        )
            -> cocoindex_utils::error::Result<Option<TargetReconcileOutput<RustProfile>>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            reconcile_fn: Arc::new(f),
            attachments_fn: Arc::new(|| Ok(vec![])),
        }
    }

    pub(crate) fn with_attachments(
        mut self,
        f: impl Fn() -> cocoindex_utils::error::Result<Vec<(Arc<str>, BoxedHandler)>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.attachments_fn = Arc::new(f);
        self
    }
}

impl TargetHandler<RustProfile> for BoxedHandler {
    fn reconcile(
        &self,
        key: StableKey,
        desired_target_state: Option<&Value>,
        prev_possible_states: &[Value],
        prev_may_be_missing: bool,
    ) -> cocoindex_utils::error::Result<Option<TargetReconcileOutput<RustProfile>>> {
        (self.reconcile_fn)(
            key,
            desired_target_state,
            prev_possible_states,
            prev_may_be_missing,
        )
    }

    fn attachments(&self) -> cocoindex_utils::error::Result<Vec<(Arc<str>, BoxedHandler)>> {
        (self.attachments_fn)()
    }
}

// ---------------------------------------------------------------------------
// BoxedSink — Type-erased action sink for batched target state application.
// ---------------------------------------------------------------------------

pub(crate) type SinkFuture =
    Pin<Box<dyn Future<Output = cocoindex_utils::error::Result<()>> + Send>>;

// The sink receives the host context (the environment's `ContextStore`) so it
// can resolve provided resources (pools/clients) by their stable key at apply
// time, and each action paired with the slot for its child target provider
// (`None` for leaf actions). The typed constructors on
// `target_state::TargetActionSink` decode both for connector code.
type SinkFn = Arc<
    dyn Fn(Arc<ContextStore>, Vec<TargetActionWithChildSlot<RustProfile>>) -> SinkFuture
        + Send
        + Sync,
>;

#[derive(Clone)]
pub(crate) struct BoxedSink {
    apply_fn: SinkFn,
}

impl BoxedSink {
    pub(crate) fn new(
        f: impl Fn(Arc<ContextStore>, Vec<TargetActionWithChildSlot<RustProfile>>) -> SinkFuture
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            apply_fn: Arc::new(f),
        }
    }
}

#[async_trait]
impl TargetActionSink<RustProfile> for BoxedSink {
    async fn apply(
        &self,
        _host_runtime_ctx: &(),
        host_ctx: Arc<ContextStore>,
        actions: &[TargetActionWithChildSlot<RustProfile>],
    ) -> cocoindex_utils::error::Result<()> {
        // The engine keeps the actions to retry subsets of a failed batch; an
        // `Action` wraps `Bytes` and a slot is an `Arc`, so this is a refcount
        // bump per action.
        (self.apply_fn)(host_ctx, actions.to_vec()).await
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
    fn value_unit() {
        let v = Value::unit();
        let _: () = v.deserialize().unwrap();
    }
}
