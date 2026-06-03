//! Public target-state API for connector authors.

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
        StableKey::Array(Arc::from([
            StableKey::Symbol(Arc::from("u64")),
            StableKey::Str(Arc::from(self.to_string())),
        ]))
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

    /// Memo key for this provider.
    ///
    /// The key is the provider path plus its generation once the parent has
    /// committed. Destructive invalidation changes the provider id; lossy
    /// invalidation changes the schema version. Use this key as a memo
    /// dependency when work should rerun after the target provider is recreated
    /// or its schema changes.
    pub fn memo_key(&self) -> String {
        let path = self.inner.target_state_path().to_string();
        match self.inner.provider_generation() {
            Some(g) => format!("{}[{},{}]", path, g.provider_id, g.provider_schema_version),
            None => path,
        }
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

    /// The declared value (the spec) carried by this target state.
    pub fn value(&self) -> &V {
        &self.value
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

/// A child (or attachment) target handler definition.
///
/// Returned by a *container* target's sink (see
/// [`TargetActionSink::from_async_fn_with_children`]) to fulfill the child
/// provider obtained from [`declare_target_state_with_child`]/[`mount_target`],
/// and by [`TargetHandler::attachments`] to define attachment handlers. It wraps
/// a typed [`TargetHandler`] for the child/attachment value type.
pub struct ChildTargetDef {
    handler: BoxedHandler,
}

impl ChildTargetDef {
    /// Wrap a typed child/attachment handler.
    pub fn new<ChildV, H>(handler: H) -> Self
    where
        ChildV: Serialize + DeserializeOwned + Send + 'static,
        H: TargetHandler<ChildV>,
    {
        Self {
            handler: boxed_handler::<ChildV, H>(handler),
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

    /// Build a sink for a *container* target whose actions each (optionally)
    /// produce a child target handler.
    ///
    /// The returned `Vec` must contain exactly one entry per input action, in
    /// the same order: `Some(child)` for an action whose target state declared a
    /// child provider, and `None` for an action without a child provider
    /// (typically an orphan delete).
    pub fn from_async_fn_with_children<F, Fut>(f: F) -> Self
    where
        F: Fn(Vec<TargetAction<A>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<Option<ChildTargetDef>>>> + Send + 'static,
    {
        let inner = BoxedSink::new(move |actions| {
            let decoded = actions
                .into_iter()
                .map(decode_action::<A>)
                .collect::<Result<Vec<_>>>();
            let fut = match decoded {
                Ok(actions) => Box::pin(f(actions))
                    as Pin<Box<dyn Future<Output = Result<Vec<Option<ChildTargetDef>>>> + Send>>,
                Err(err) => Box::pin(async move { Err(err) }),
            };
            Box::pin(async move {
                let defs = fut
                    .await
                    .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
                let mapped = defs
                    .into_iter()
                    .map(|d| {
                        d.map(|d| cocoindex_core::engine::target_state::ChildTargetDef {
                            handler: d.handler,
                        })
                    })
                    .collect();
                Ok(Some(mapped))
            })
        });
        Self {
            inner,
            _action: PhantomData,
        }
    }

    // Only the graph-target tests (`cypher_graph`) use this helper, and those are
    // gated behind the neo4j/falkordb features; matching the gate keeps it from
    // tripping dead-code warnings in builds without those features.
    #[cfg(all(test, any(feature = "neo4j", feature = "falkordb")))]
    pub(crate) async fn apply_for_test(
        &self,
        actions: Vec<TargetAction<A>>,
    ) -> Result<Option<Vec<Option<ChildTargetDef>>>> {
        use cocoindex_core::engine::target_state::TargetActionSink as _;

        let actions = actions
            .into_iter()
            .map(|action| match action {
                TargetAction::Create(value) => {
                    Ok(Action::Create(Value::from_serializable(&value)?))
                }
                TargetAction::Update(value) => {
                    Ok(Action::Update(Value::from_serializable(&value)?))
                }
                TargetAction::Delete(value) => {
                    Ok(Action::Delete(Value::from_serializable(&value)?))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let children = self
            .inner
            .apply(&(), Arc::new(()), actions)
            .await
            .map_err(|e| crate::error::Error::engine(e.to_string()))?;
        Ok(children.map(|children| {
            children
                .into_iter()
                .map(|child| {
                    child.map(|child| ChildTargetDef {
                        handler: child.handler,
                    })
                })
                .collect()
        }))
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

    /// Attachment handlers this handler supports, keyed by attachment type name.
    ///
    /// The engine eagerly registers these so orphaned attachments are cleaned up
    /// even when not declared in the current run. Obtain an attachment's provider
    /// to declare states on via [`TargetStateProvider::attachment`]. Defaults to
    /// no attachments.
    fn attachments(&self) -> Result<Vec<(String, ChildTargetDef)>> {
        Ok(Vec::new())
    }
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

/// Mount a parent target and return a ready child target provider.
///
/// The parent target state is declared and committed inside a foreground child
/// component, so the parent handler's sink runs and fulfills the child provider
/// (via [`TargetActionSink::from_async_fn_with_children`]) before this returns.
/// The returned provider is ready for immediate child declarations. Use
/// [`declare_target_state_with_child`] when the child provider can be fulfilled
/// when the enclosing component commits.
pub async fn mount_target<V, ChildV>(
    ctx: &Ctx,
    target_state: TargetState<V>,
) -> Result<TargetStateProvider<ChildV>>
where
    V: Serialize + Send + 'static,
    ChildV: Serialize + DeserializeOwned + Send + 'static,
{
    use std::sync::Mutex;

    let provider_inner = target_state.provider.inner.clone();
    // Use the stable provider *path* (not `memo_key`, which also encodes the
    // mutable provider generation) so the mounted sub-component's path stays
    // stable across destructive/lossy generation bumps and matches its prior run.
    let scope_key = format!(
        "cocoindex/mount_target/{}/{}",
        provider_inner.target_state_path(),
        target_state.key
    );
    let key = target_state.key;
    let value = Value::from_serializable(&target_state.value)?;

    type CoreProvider = cocoindex_core::engine::target_state::TargetStateProvider<RustProfile>;
    let slot: Arc<Mutex<Option<CoreProvider>>> = Arc::new(Mutex::new(None));
    let slot_inner = slot.clone();

    ctx.scope(&scope_key, move |child_ctx| async move {
        let child = child_ctx.declare_target_state_with_child(provider_inner, key, value)?;
        *slot_inner.lock().unwrap() = Some(child);
        Ok::<(), crate::error::Error>(())
    })
    .await?;

    let child = slot.lock().unwrap().take().ok_or_else(|| {
        crate::error::Error::engine("mount_target: child provider was not produced")
    })?;
    Ok(TargetStateProvider::new(child))
}

fn boxed_handler<V, H>(handler: H) -> BoxedHandler
where
    V: Serialize + DeserializeOwned + Send + 'static,
    H: TargetHandler<V>,
{
    let handler = Arc::new(handler);
    let attach_handler = handler.clone();
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
    .with_attachments(move || {
        let entries = attach_handler
            .attachments()
            .map_err(|e| cocoindex_utils::error::Error::internal_msg(e.to_string()))?;
        Ok(entries
            .into_iter()
            .map(|(name, def)| (Arc::from(name.as_str()), def.handler))
            .collect())
    })
}

fn internal(err: impl std::fmt::Display) -> cocoindex_utils::error::Error {
    cocoindex_utils::error::Error::internal_msg(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u64_stable_key_is_tagged_and_non_lossy() {
        let key = u64::MAX.into_stable_key();
        assert_eq!(
            key,
            StableKey::Array(Arc::from([
                StableKey::Symbol(Arc::from("u64")),
                StableKey::Str(Arc::from(u64::MAX.to_string())),
            ]))
        );
        assert_ne!(key, StableKey::Int(-1));
        assert_ne!(key, StableKey::Str(Arc::from(u64::MAX.to_string())));
    }
}
