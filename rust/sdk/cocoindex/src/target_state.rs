//! Public target-state facade for connector authors.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use cocoindex_core::engine::target_state::ChildInvalidation;
pub use cocoindex_core::state::stable_path::StableKey;
use serde::{Serialize, de::DeserializeOwned};

use crate::ctx::Ctx;
use crate::error::Result;
use crate::profile::{Action, BoxedHandler, BoxedSink, RustProfile, Value};

pub trait IntoStableKey {
    fn into_stable_key(self) -> StableKey;
}

impl IntoStableKey for StableKey {
    fn into_stable_key(self) -> StableKey {
        self
    }
}

impl IntoStableKey for &str {
    fn into_stable_key(self) -> StableKey {
        StableKey::Str(Arc::from(self))
    }
}

impl IntoStableKey for String {
    fn into_stable_key(self) -> StableKey {
        StableKey::Str(Arc::from(self))
    }
}

impl IntoStableKey for i64 {
    fn into_stable_key(self) -> StableKey {
        StableKey::Int(self)
    }
}

impl IntoStableKey for u64 {
    fn into_stable_key(self) -> StableKey {
        StableKey::Int(self as i64)
    }
}

pub struct TargetStateProvider<V> {
    pub(crate) inner: cocoindex_core::engine::target_state::TargetStateProvider<RustProfile>,
    _value: PhantomData<fn() -> V>,
}

impl<V> Clone for TargetStateProvider<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _value: PhantomData,
        }
    }
}

impl<V> TargetStateProvider<V> {
    pub(crate) fn new(
        inner: cocoindex_core::engine::target_state::TargetStateProvider<RustProfile>,
    ) -> Self {
        Self {
            inner,
            _value: PhantomData,
        }
    }

    pub fn memo_key(&self) -> String {
        self.inner.target_state_path().to_string()
    }

    pub fn stable_key_chain(&self) -> Vec<StableKey> {
        self.inner.stable_key_chain()
    }

    pub fn target_state(&self, key: impl IntoStableKey, value: V) -> TargetState<V> {
        TargetState {
            provider: self.clone(),
            key: key.into_stable_key(),
            value,
        }
    }

    pub fn attachment<T>(&self, ctx: &Ctx, att_type: &str) -> Result<TargetStateProvider<T>> {
        let provider = ctx.register_attachment_target_provider(&self.inner, att_type)?;
        Ok(TargetStateProvider::new(provider))
    }
}

pub struct TargetState<V> {
    provider: TargetStateProvider<V>,
    key: StableKey,
    value: V,
}

impl<V> TargetState<V> {
    pub fn key(&self) -> &StableKey {
        &self.key
    }

    pub fn provider(&self) -> &TargetStateProvider<V> {
        &self.provider
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TargetAction<A> {
    Create(A),
    Update(A),
    Delete(A),
}

pub struct TargetReconcileOutput<A, R> {
    pub action: TargetAction<A>,
    pub sink: TargetActionSink<A>,
    pub tracking_record: Option<R>,
    pub child_invalidation: Option<TargetChildInvalidation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TargetChildInvalidation {
    Destructive,
    Lossy,
}

impl From<TargetChildInvalidation> for ChildInvalidation {
    fn from(value: TargetChildInvalidation) -> Self {
        match value {
            TargetChildInvalidation::Destructive => ChildInvalidation::Destructive,
            TargetChildInvalidation::Lossy => ChildInvalidation::Lossy,
        }
    }
}

#[derive(Clone)]
pub struct TargetActionSink<A> {
    inner: BoxedSink,
    _action: PhantomData<fn() -> A>,
}

impl<A> TargetActionSink<A>
where
    A: Serialize + DeserializeOwned + Send + 'static,
{
    pub fn from_async_fn<F, Fut>(f: F) -> Self
    where
        F: Fn(Vec<TargetAction<A>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let inner = BoxedSink::new(move |actions| {
            let decoded = actions
                .into_iter()
                .map(decode_action::<A>)
                .collect::<Result<Vec<_>>>();
            let fut = match decoded {
                Ok(actions) => {
                    Box::pin(f(actions)) as Pin<Box<dyn Future<Output = Result<()>> + Send>>
                }
                Err(err) => Box::pin(async move { Err(err) }),
            };
            Box::pin(async move {
                fut.await
                    .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
                Ok(None)
            })
        });
        Self {
            inner,
            _action: PhantomData,
        }
    }
}

fn decode_action<A: DeserializeOwned>(action: Action) -> Result<TargetAction<A>> {
    match action {
        Action::Create(value) => Ok(TargetAction::Create(value.deserialize()?)),
        Action::Update(value) => Ok(TargetAction::Update(value.deserialize()?)),
        Action::Delete(value) => Ok(TargetAction::Delete(value.deserialize()?)),
    }
}

pub trait TargetHandler<V>: Send + Sync + 'static
where
    V: Serialize + DeserializeOwned + Send + 'static,
{
    type TrackingRecord: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Action: Serialize + DeserializeOwned + Send + 'static;

    fn reconcile(
        &self,
        key: StableKey,
        desired_target_state: Option<V>,
        prev_possible_records: Vec<Self::TrackingRecord>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<Self::Action, Self::TrackingRecord>>>;
}

pub fn register_root_target_states_provider<V, H>(
    ctx: &Ctx,
    name: impl Into<String>,
    handler: H,
) -> Result<TargetStateProvider<V>>
where
    V: Serialize + DeserializeOwned + Send + 'static,
    H: TargetHandler<V>,
{
    let boxed = boxed_handler::<V, H>(handler);
    let provider = ctx.register_root_target_provider(name, boxed)?;
    Ok(TargetStateProvider::new(provider))
}

pub fn declare_target_state<V>(ctx: &Ctx, target_state: TargetState<V>) -> Result<()>
where
    V: Serialize + Send + 'static,
{
    ctx.declare_target_state(
        target_state.provider.inner,
        target_state.key,
        Value::from_serializable(&target_state.value)?,
    )
}

pub fn declare_target_state_with_child<V, ChildV>(
    ctx: &Ctx,
    target_state: TargetState<V>,
) -> Result<TargetStateProvider<ChildV>>
where
    V: Serialize + Send + 'static,
{
    let provider = ctx.declare_target_state_with_child(
        target_state.provider.inner,
        target_state.key,
        Value::from_serializable(&target_state.value)?,
    )?;
    Ok(TargetStateProvider::new(provider))
}

pub async fn mount_target<V, ChildV>(
    ctx: &Ctx,
    target_state: TargetState<V>,
) -> Result<TargetStateProvider<ChildV>>
where
    V: Serialize + Send + 'static,
    ChildV: Serialize + DeserializeOwned + Send + 'static,
{
    declare_target_state_with_child::<V, ChildV>(ctx, target_state)
}

fn boxed_handler<V, H>(handler: H) -> BoxedHandler
where
    V: Serialize + DeserializeOwned + Send + 'static,
    H: TargetHandler<V>,
{
    BoxedHandler::new(move |key, desired, prev, prev_may_be_missing| {
        let desired = desired
            .map(Value::deserialize::<V>)
            .transpose()
            .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
        let prev = prev
            .iter()
            .map(Value::deserialize::<H::TrackingRecord>)
            .collect::<Result<Vec<_>>>()
            .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
        let output = handler
            .reconcile(key, desired, prev, prev_may_be_missing)
            .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
        let Some(output) = output else {
            return Ok(None);
        };
        let action = match output.action {
            TargetAction::Create(action) => {
                Action::Create(Value::from_serializable(&action).map_err(internal)?)
            }
            TargetAction::Update(action) => {
                Action::Update(Value::from_serializable(&action).map_err(internal)?)
            }
            TargetAction::Delete(action) => {
                Action::Delete(Value::from_serializable(&action).map_err(internal)?)
            }
        };
        Ok(Some(
            cocoindex_core::engine::target_state::TargetReconcileOutput {
                action,
                sink: output.sink.inner,
                tracking_record: output
                    .tracking_record
                    .map(|record| Value::from_serializable(&record).map_err(internal))
                    .transpose()?,
                child_invalidation: output.child_invalidation.map(Into::into),
            },
        ))
    })
}

fn internal(err: impl std::fmt::Display) -> cocoindex_utils::error::Error {
    cocoindex_utils::error::Error::internal_msg(err.to_string())
}
