//! Public target-state API for connector authors.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use cocoindex_core::engine::target_state::{
    ChildInvalidation, ChildTargetSlot, TargetActionSinkKeeper, TargetActionWithChildSlot,
};
pub use cocoindex_core::state::stable_path::StableKey;
use serde::{Serialize, de::DeserializeOwned};

use crate::ctx::{ContextStore, Ctx};
use crate::error::{Error, Result};
use crate::profile::{Action, BoxedHandler, BoxedSink, RustProfile, SinkFuture, Value};

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
/// Passed to [`ChildSlot::fulfill`] by a *container* target's sink (see
/// [`TargetActionSink::from_async_fn_with_children`]) to fulfill the child
/// provider obtained from [`declare_target_state_with_child`]/[`mount_target`],
/// and returned by [`TargetHandler::attachments`] to define attachment handlers.
/// It wraps a typed [`TargetHandler`] for the child/attachment value type.
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

/// Fulfillment handle for the child target provider of one container action.
///
/// A sink built with [`TargetActionSink::from_async_fn_with_children`] receives
/// `Some(slot)` alongside every action whose target state was declared with
/// [`declare_target_state_with_child`] / [`mount_target`], and must call
/// [`Self::fulfill`] on each before returning. `fulfill` consumes the slot, so
/// a double fulfillment is a compile error; a slot left unfulfilled fails the
/// commit.
pub struct ChildSlot {
    inner: ChildTargetSlot<RustProfile>,
}

impl ChildSlot {
    pub fn fulfill(self, child: ChildTargetDef) -> Result<()> {
        Ok(self.inner.fulfill(child.handler)?)
    }
}

#[derive(Clone)]
pub struct TargetActionSink<A> {
    inner: TargetActionSinkKeeper<RustProfile>,
    _action: PhantomData<fn() -> A>,
}

impl<A> TargetActionSink<A>
where
    A: Serialize + DeserializeOwned + Send + 'static,
{
    /// Build a sink for a *leaf* target, whose actions carry no child target
    /// states.
    pub fn from_async_fn<F, Fut>(f: F) -> Self
    where
        F: Fn(Vec<TargetAction<A>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Self::from_async_fn_with_ctx(move |_host_ctx, actions| f(actions))
    }

    /// Build a sink for a *container* target: each action comes with the
    /// [`ChildSlot`] for its child target provider (`None` for an action whose
    /// target state was not declared with a child — typically an orphan
    /// delete), and the closure must fulfill every slot it receives.
    pub fn from_async_fn_with_children<F, Fut>(f: F) -> Self
    where
        F: Fn(Vec<(TargetAction<A>, Option<ChildSlot>)>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Self::from_async_fn_with_children_ctx(move |_host_ctx, actions| f(actions))
    }

    /// Deprecated form of [`Self::from_async_fn_with_children`] taking the
    /// pre-slot closure shape: the closure returns the child handler
    /// definitions as a `Vec` index-aligned with the actions (`None` for an
    /// action without a child). The definitions are used to fulfill the
    /// actions' child slots; a missing definition for an action that has a
    /// slot, or a length mismatch, fails the batch.
    #[deprecated(note = "use `from_async_fn_with_children` and fulfill each action's `ChildSlot`")]
    pub fn from_async_fn_with_child_defs<F, Fut>(f: F) -> Self
    where
        F: Fn(Vec<TargetAction<A>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<Option<ChildTargetDef>>>> + Send + 'static,
    {
        let f = Arc::new(f);
        Self::from_async_fn_with_children(move |actions| {
            let f = f.clone();
            async move {
                let (actions, slots): (Vec<_>, Vec<_>) = actions.into_iter().unzip();
                let defs = f(actions).await?;
                if defs.len() != slots.len() {
                    return Err(Error::engine(format!(
                        "target action sink returned {} child handler definitions for {} actions",
                        defs.len(),
                        slots.len()
                    )));
                }
                for (slot, def) in slots.into_iter().zip(defs) {
                    if let Some(slot) = slot {
                        let def = def.ok_or_else(|| {
                            Error::engine(
                                "target action sink returned no child handler for an action \
                                 whose target state declared a child",
                            )
                        })?;
                        slot.fulfill(def)?;
                    }
                }
                Ok(())
            }
        })
    }

    /// Like [`Self::from_async_fn`], but the apply closure also receives the
    /// host context (the environment's [`ContextStore`]) so it can resolve a
    /// provided connection by its stable key at apply time (`design_connectors.md`
    /// §5.5). Used by connectors that resolve pools/clients in `apply`.
    // Used only by feature-gated connectors (postgres/sqlite today); harmless
    // dead code in builds without any such connector enabled.
    #[allow(dead_code)]
    pub(crate) fn from_async_fn_with_ctx<F, Fut>(f: F) -> Self
    where
        F: Fn(Arc<ContextStore>, Vec<TargetAction<A>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Self::new(move |host_ctx, actions| Ok(f(host_ctx, decode_leaf_actions::<A>(actions)?)))
    }

    /// Like [`Self::from_async_fn_with_children`], but the apply closure also
    /// receives the host context for apply-time connection resolution.
    #[allow(dead_code)]
    pub(crate) fn from_async_fn_with_children_ctx<F, Fut>(f: F) -> Self
    where
        F: Fn(Arc<ContextStore>, Vec<(TargetAction<A>, Option<ChildSlot>)>) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Self::new(move |host_ctx, actions| {
            Ok(f(host_ctx, decode_actions_with_children::<A>(actions)?))
        })
    }

    /// Wrap a closure that decodes the engine's batch and starts the apply
    /// future; a decode failure becomes a failed future.
    fn new<F, Fut>(f: F) -> Self
    where
        F: Fn(Arc<ContextStore>, Vec<TargetActionWithChildSlot<RustProfile>>) -> Result<Fut>
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let inner = TargetActionSinkKeeper::new(BoxedSink::new(move |host_ctx, actions| {
            let fut: SinkFuture = match f(host_ctx, actions) {
                Ok(fut) => Box::pin(async move { fut.await.map_err(Error::into_core) }),
                Err(err) => Box::pin(async move { Err(err.into_core()) }),
            };
            fut
        }));
        Self {
            inner,
            _action: PhantomData,
        }
    }

    // Only the graph-target tests (`cypher_graph`) use this helper, and those are
    // gated behind the neo4j/falkordb features; matching the gate keeps it from
    // tripping dead-code warnings in builds without those features.
    //
    // Every action gets a child slot; the returned entry is `None` where the
    // sink left it unfulfilled (e.g. a drop).
    #[cfg(all(test, any(feature = "neo4j", feature = "falkordb")))]
    pub(crate) async fn apply_for_test(
        &self,
        actions: Vec<TargetAction<A>>,
    ) -> Result<Vec<Option<ChildTargetDef>>> {
        let slots: Vec<ChildTargetSlot<RustProfile>> =
            actions.iter().map(|_| ChildTargetSlot::new()).collect();
        let actions = actions
            .into_iter()
            .zip(&slots)
            .map(|(action, slot)| {
                let action = match action {
                    TargetAction::Create(value) => {
                        Action::Create(Value::from_serializable(&value)?)
                    }
                    TargetAction::Update(value) => {
                        Action::Update(Value::from_serializable(&value)?)
                    }
                    TargetAction::Delete(value) => {
                        Action::Delete(Value::from_serializable(&value)?)
                    }
                };
                Ok((action, Some(slot.clone())))
            })
            .collect::<Result<Vec<_>>>()?;
        self.inner
            .apply(&(), Arc::new(crate::ctx::ContextStore::default()), actions)
            .await?;
        slots
            .iter()
            .map(|slot| Ok(slot.take()?.map(|handler| ChildTargetDef { handler })))
            .collect()
    }
}

/// Decode a leaf sink's batch. A child slot here means a target state was
/// declared with a child provider but routed to a sink that cannot fulfill one.
fn decode_leaf_actions<A: DeserializeOwned>(
    actions: Vec<TargetActionWithChildSlot<RustProfile>>,
) -> Result<Vec<TargetAction<A>>> {
    actions
        .into_iter()
        .map(|(action, child_slot)| {
            if child_slot.is_some() {
                return Err(Error::engine(
                    "target action sink built with `TargetActionSink::from_async_fn` received \
                     an action with child target states; build it with \
                     `from_async_fn_with_children` and fulfill their child slots",
                ));
            }
            decode_action::<A>(action)
        })
        .collect()
}

fn decode_actions_with_children<A: DeserializeOwned>(
    actions: Vec<TargetActionWithChildSlot<RustProfile>>,
) -> Result<Vec<(TargetAction<A>, Option<ChildSlot>)>> {
    actions
        .into_iter()
        .map(|(action, child_slot)| {
            Ok((
                decode_action::<A>(action)?,
                child_slot.map(|inner| ChildSlot { inner }),
            ))
        })
        .collect()
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
/// (via its [`ChildSlot`]) before this returns.
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
            .map_err(crate::error::Error::into_core)?;
        let prev = prev
            .iter()
            .map(Value::deserialize::<H::TrackingRecord>)
            .collect::<Result<Vec<_>>>()
            .map_err(crate::error::Error::into_core)?;
        let output = handler
            .reconcile(key, desired, prev, prev_may_be_missing)
            .map_err(crate::error::Error::into_core)?;
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
            .map_err(crate::error::Error::into_core)?;
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
