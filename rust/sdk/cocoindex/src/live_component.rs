//! SDK-level live components and exception handlers.
//!
//! A **live component** owns a set of child components / target states that
//! evolve over time. Unlike a plain `scope`, it has a long-lived `process_live`
//! body that reacts to a changing source: it can re-run a full pass
//! ([`LiveComponentOperator::update_full`]), incrementally add a child
//! ([`LiveComponentOperator::update`]), or remove one
//! ([`LiveComponentOperator::delete`]). [`crate::Ctx::auto_refresh`] is the
//! simplest live component (re-run on a timer); this module exposes the general
//! shape so connectors can drive change-feed sources (live localfs, Kafka,
//! object-store watching, …).
//!
//! Mirrors the Python `_internal/live_component.py` surface
//! (`LiveComponent` / `LiveComponentOperator` / `LiveMapFeed` / `LiveMapView` /
//! `LiveMapSubscriber`) and the `exception_handler` / `ExceptionContext`
//! error-routing model from `_internal/component_ctx.py`, adapted to Rust:
//!
//! - A `LiveComponent` is a trait with `process` (one full pass) and
//!   `process_live` (the long-lived reactive body), instead of a Python class
//!   with two coroutine methods.
//! - An [`ExceptionHandler`] is a synchronous closure that returns `Ok(())` to
//!   swallow a background failure or `Err(_)` to propagate it — the same
//!   "return = swallow, raise = propagate" contract the Python handler chain
//!   has, expressed through `Result`.

use std::fmt::Display;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use cocoindex_core::engine::component::OnError;
use cocoindex_core::engine::live_component::LiveComponentController;
use cocoindex_core::state::stable_path::{StableKey, StablePath};

use crate::app::AppInner;
use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::profile::{BoxedProcessor, RustProfile, Value};

type CoreError = cocoindex_utils::error::Error;

/// Map a core engine error into the SDK error type. Matches the conversion
/// already used by `Ctx::auto_refresh`.
pub(crate) fn engine_err(e: impl Display) -> Error {
    Error::engine(format!("{e}"))
}

/// Map an SDK error back into a core engine error, for propagation across the
/// `controller.start` / `on_error` boundary (both speak the core error type).
fn to_core_error(e: Error) -> CoreError {
    CoreError::internal_msg(format!("{e}"))
}

// ---------------------------------------------------------------------------
// Exception handlers
// ---------------------------------------------------------------------------

/// Which background operation raised the error routed to an [`ExceptionHandler`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MountKind {
    /// A full `process_live`/`process()` cycle
    /// ([`LiveComponentOperator::update_full`]).
    UpdateFull,
    /// An incremental child mount ([`LiveComponentOperator::update`]).
    Update,
    /// An incremental, background child delete
    /// ([`LiveComponentOperator::delete`]).
    Delete,
}

/// Metadata describing the failed background operation, passed to an
/// [`ExceptionHandler`]. Mirrors the Python `ExceptionContext` (carrying the
/// fields the Rust SDK can populate).
#[derive(Clone, Debug)]
pub struct ExceptionContext {
    /// Name of the app/environment the failing component runs under.
    pub env_name: String,
    /// Stable path of the live component the failure is attributed to.
    pub stable_path: String,
    /// Stable path of the parent component, if the failure is attributed to a
    /// child mount/delete.
    pub parent_stable_path: Option<String>,
    /// Name of the processor that failed, if known.
    pub processor_name: Option<String>,
    /// Which operation raised the error.
    pub mount_kind: MountKind,
    /// Whether the failure occurred in background work (a post-ready incremental
    /// update/delete in live mode) rather than the foreground catch-up pass.
    pub is_background: bool,
}

/// A handler for failures of background work owned by a live component.
///
/// Returning `Ok(())` **swallows** the failure (the operation's handle resolves
/// `Ok`); returning `Err(_)` **re-raises** it — propagating to the next outer
/// handler in the chain, or to the operation's handle if none remain. This is
/// the Rust expression of the Python handler chain's "return = swallow, raise =
/// propagate" rule.
pub type ExceptionHandler =
    Arc<dyn Fn(&Error, &ExceptionContext) -> Result<()> + Send + Sync + 'static>;

/// Wrap a chain of [`ExceptionHandler`]s (innermost component last) plus the
/// failure's static context as a core `OnError`. Handlers run nearest-first;
/// the first to return `Ok` swallows the error, otherwise the (possibly
/// rewritten) error propagates to the next outer handler and finally out.
pub(crate) fn build_chained_on_error(
    chain: &[ExceptionHandler],
    ctx: ExceptionContext,
) -> Option<OnError> {
    if chain.is_empty() {
        return None;
    }
    let chain: Vec<ExceptionHandler> = chain.to_vec();
    Some(Arc::new(move |err: CoreError| {
        let chain = chain.clone();
        let ctx = ctx.clone();
        Box::pin(async move {
            let mut current = engine_err(err);
            // Chain is stored outer→inner; run nearest (innermost) first.
            for handler in chain.iter().rev() {
                match handler(&current, &ctx) {
                    Ok(()) => return Ok(()),
                    Err(next) => current = next,
                }
            }
            Err(to_core_error(current))
        })
            as Pin<Box<dyn Future<Output = cocoindex_utils::error::Result<()>> + Send + 'static>>
    }))
}

// ---------------------------------------------------------------------------
// LiveComponent + operator
// ---------------------------------------------------------------------------

/// A component with a long-lived, reactive body.
///
/// Mount one with [`crate::Ctx::mount_live`]. The framework calls `process_live`
/// once on mount, on its own task; the default implementation runs a single
/// full pass and marks the component ready (observationally identical to a plain
/// `scope` in catch-up mode). Override it to react to a change feed.
#[async_trait]
pub trait LiveComponent: Send + Sync + 'static {
    /// Declare the component's complete desired state. Called by
    /// [`LiveComponentOperator::update_full`]; stale children/target states from
    /// the previous full pass are garbage-collected.
    async fn process(&self, ctx: Ctx) -> Result<()>;

    /// The long-lived reactive body. Use `operator` to run full passes and to
    /// incrementally add/remove children as the source changes. Call
    /// [`LiveComponentOperator::mark_ready`] once the initial catch-up is done
    /// — in catch-up (non-live) mode that also terminates the component.
    async fn process_live(&self, operator: LiveComponentOperator) -> Result<()> {
        operator.update_full().await?;
        operator.mark_ready().await;
        Ok(())
    }
}

/// Drives a mounted [`LiveComponent`]. Handed to `process_live`.
pub struct LiveComponentOperator {
    controller: LiveComponentController<RustProfile>,
    state: Arc<AppInner>,
    component_path: StablePath,
    instance: Arc<dyn LiveComponent>,
    /// Exception handlers in scope, ordered outermost→innermost; the innermost
    /// (this component's own handler, if any) runs first.
    handler_chain: Arc<Vec<ExceptionHandler>>,
    name: String,
}

impl LiveComponentOperator {
    fn child_path(&self, key: &dyn Display) -> StablePath {
        let key = StableKey::Str(Arc::from(key.to_string().as_str()));
        self.component_path.concat_part(key)
    }

    /// Context for a failure on this component's own full pass.
    fn exc_ctx(&self, mount_kind: MountKind) -> ExceptionContext {
        ExceptionContext {
            env_name: self.state.name.clone(),
            stable_path: self.component_path.to_string(),
            parent_stable_path: None,
            processor_name: Some(self.name.clone()),
            mount_kind,
            is_background: false,
        }
    }

    /// Context for a failure on a child mount/delete: the failing path is the
    /// child, and this component is its parent.
    fn child_exc_ctx(&self, mount_kind: MountKind, child_path: &StablePath) -> ExceptionContext {
        ExceptionContext {
            env_name: self.state.name.clone(),
            stable_path: child_path.to_string(),
            parent_stable_path: Some(self.component_path.to_string()),
            processor_name: Some(self.name.clone()),
            mount_kind,
            is_background: self.controller.is_live(),
        }
    }

    /// Run a full `process()` pass. Reconciles the complete desired state and
    /// garbage-collects children no longer declared. Failures route through the
    /// component's [`ExceptionHandler`] if one was registered (returning the
    /// handler's verdict), otherwise surface as `Err`.
    pub async fn update_full(&self) -> Result<()> {
        let instance = self.instance.clone();
        let state = self.state.clone();
        let chain = self.handler_chain.clone();
        let processor = BoxedProcessor::new(
            move |comp_ctx| {
                let ctx = Ctx::new_with_handlers(Some(comp_ctx), state.clone(), chain.clone());
                let instance = instance.clone();
                Box::pin(async move {
                    instance.process(ctx).await?;
                    Ok(Value::unit())
                })
            },
            None,
            format!("{}:process", self.name),
        );
        let on_error =
            build_chained_on_error(&self.handler_chain, self.exc_ctx(MountKind::UpdateFull));
        self.controller
            .update_full(processor, on_error)
            .await
            .map_err(engine_err)
    }

    /// Incrementally mount (or replace) a single child under `key`, running
    /// `f` to declare its state. Same-key rapid calls coalesce. Awaits until the
    /// child's state has synced.
    pub async fn update<K, F, Fut>(&self, key: K, f: F) -> Result<()>
    where
        K: Display,
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let child_path = self.child_path(&key);
        let state = self.state.clone();
        let chain = self.handler_chain.clone();
        let processor = BoxedProcessor::new(
            move |comp_ctx| {
                let ctx = Ctx::new_with_handlers(Some(comp_ctx), state.clone(), chain.clone());
                Box::pin(async move {
                    f(ctx).await?;
                    Ok(Value::unit())
                })
            },
            None,
            format!("{}:update", self.name),
        );
        let on_error = build_chained_on_error(
            &self.handler_chain,
            self.child_exc_ctx(MountKind::Update, &child_path),
        );
        let handle = self
            .controller
            .update(child_path, processor, on_error)
            .await
            .map_err(engine_err)?;
        handle.ready().await.map_err(engine_err)
    }

    /// Incrementally remove the child mounted under `key`, cleaning up its
    /// target states. Awaits until the deletion has synced.
    pub async fn delete<K: Display>(&self, key: K) -> Result<()> {
        let child_path = self.child_path(&key);
        let on_error = build_chained_on_error(
            &self.handler_chain,
            self.child_exc_ctx(MountKind::Delete, &child_path),
        );
        let handle = self
            .controller
            .delete(child_path, on_error)
            .await
            .map_err(engine_err)?;
        handle.ready().await.map_err(engine_err)
    }

    /// Signal that the initial catch-up is complete. Idempotent. In catch-up
    /// (non-live) mode this also terminates the component after the current
    /// `process_live` body unwinds.
    pub async fn mark_ready(&self) {
        self.controller.mark_ready().await;
    }

    /// Whether the component is running in live mode (`UpdateOptions.live`).
    pub fn is_live(&self) -> bool {
        self.controller.is_live()
    }
}

/// Internal constructor used by `Ctx::mount_live`. Kept here so the operator's
/// fields stay private to this module.
pub(crate) fn new_operator(
    controller: LiveComponentController<RustProfile>,
    state: Arc<AppInner>,
    component_path: StablePath,
    instance: Arc<dyn LiveComponent>,
    handler_chain: Arc<Vec<ExceptionHandler>>,
    name: String,
) -> LiveComponentOperator {
    LiveComponentOperator {
        controller,
        state,
        component_path,
        instance,
        handler_chain,
        name,
    }
}

/// Spawn a component's `process_live` body on the controller, adapting the SDK
/// error type to the core boundary `controller.start` expects.
pub(crate) fn start_process_live(
    controller: &LiveComponentController<RustProfile>,
    instance: Arc<dyn LiveComponent>,
    operator: LiveComponentOperator,
) {
    controller.start(async move { instance.process_live(operator).await.map_err(to_core_error) });
}

// ---------------------------------------------------------------------------
// Live maps: feed / view / subscriber
// ---------------------------------------------------------------------------

/// The per-item processor a [`LiveMapSubscriber`] invokes. Erased so the
/// subscriber and the internal mount-each component share one concrete type.
type ProcessItemFn<V> =
    Arc<dyn Fn(Ctx, V) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> + Send + Sync>;

/// Delivers change events from a [`LiveMapFeed`] into a live component: each
/// `(key, value)` becomes a child mount, each `key` removal a child delete.
/// Handed to [`LiveMapFeed::watch`] by [`crate::Ctx::mount_each_live`].
pub struct LiveMapSubscriber<K, V> {
    operator: LiveComponentOperator,
    process_fn: ProcessItemFn<V>,
    _key: PhantomData<fn() -> K>,
}

impl<K: Display, V: Send + 'static> LiveMapSubscriber<K, V> {
    /// Incrementally upsert one entry.
    pub async fn update(&self, key: K, value: V) -> Result<()> {
        let process_fn = self.process_fn.clone();
        self.operator
            .update(key, move |ctx| process_fn(ctx, value))
            .await
    }

    /// Incrementally remove one entry.
    pub async fn delete(&self, key: K) -> Result<()> {
        self.operator.delete(key).await
    }

    /// Re-scan all entries (a full pass). Use after a change that can't be
    /// expressed as point updates (e.g. a watched directory was removed).
    pub async fn update_all(&self) -> Result<()> {
        self.operator.update_full().await
    }

    /// Signal that the initial catch-up scan is complete.
    pub async fn mark_ready(&self) {
        self.operator.mark_ready().await;
    }

    /// Whether the component is running in live mode.
    pub fn is_live(&self) -> bool {
        self.operator.is_live()
    }
}

/// A watch-only change feed (e.g. a Kafka topic): it can stream changes to a
/// subscriber but has no scannable snapshot. Requires live mode.
#[async_trait]
pub trait LiveMapFeed<K, V>: Send + Sync + 'static {
    /// Stream changes to `subscriber` until cancelled. Call
    /// [`LiveMapSubscriber::mark_ready`] once caught up.
    async fn watch(&self, subscriber: LiveMapSubscriber<K, V>) -> Result<()>;
}

/// A change feed that also has a scannable current state (e.g. a local
/// directory): usable in both catch-up and live modes.
#[async_trait]
pub trait LiveMapView<K, V>: LiveMapFeed<K, V> {
    /// All current `(key, value)` pairs. Used for the catch-up full pass and
    /// whenever [`LiveMapSubscriber::update_all`] triggers a re-scan.
    async fn scan(&self) -> Result<Vec<(K, V)>>;
}

/// Internal `LiveComponent` that adapts a [`LiveMapView`] to the operator API,
/// mirroring Python's `_MountEachLiveComponent`.
pub(crate) struct MountEachLiveComponent<K, V, Feed> {
    feed: Arc<Feed>,
    process_fn: ProcessItemFn<V>,
    _key: PhantomData<fn() -> K>,
}

impl<K, V, Feed> MountEachLiveComponent<K, V, Feed> {
    pub(crate) fn new<F, Fut>(feed: Feed, process_fn: F) -> Self
    where
        F: Fn(Ctx, V) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Self {
            feed: Arc::new(feed),
            process_fn: Arc::new(move |ctx, v| Box::pin(process_fn(ctx, v))),
            _key: PhantomData,
        }
    }
}

#[async_trait]
impl<K, V, Feed> LiveComponent for MountEachLiveComponent<K, V, Feed>
where
    K: Display + Send + Sync + 'static,
    V: Send + Sync + 'static,
    Feed: LiveMapView<K, V>,
{
    async fn process(&self, ctx: Ctx) -> Result<()> {
        let items = self.feed.scan().await?;
        let process_fn = self.process_fn.clone();
        ctx.mount_each(
            items,
            |(k, _v): &(K, V)| k.to_string(),
            move |child, (_k, v)| {
                let process_fn = process_fn.clone();
                async move { process_fn(child, v).await }
            },
        )
        .await?;
        Ok(())
    }

    async fn process_live(&self, operator: LiveComponentOperator) -> Result<()> {
        // Initial catch-up: a full scan via `process()`. In catch-up (non-live)
        // mode `mark_ready` terminates the component here, so the `watch` loop
        // below only runs in live mode — matching Python, where a view's
        // `watch` drives `update_all` + `mark_ready` before its event loop.
        operator.update_full().await?;
        operator.mark_ready().await;
        let subscriber = LiveMapSubscriber {
            operator,
            process_fn: self.process_fn.clone(),
            _key: PhantomData,
        };
        self.feed.watch(subscriber).await
    }
}
