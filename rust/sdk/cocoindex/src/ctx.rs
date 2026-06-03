//! Pipeline context: scope, memo, write_file.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::future::Future;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use cocoindex_core::engine::context::{ComponentProcessorContext, FnCallContext};
use cocoindex_core::engine::environment::Environment;
use cocoindex_core::engine::execution;
use cocoindex_core::engine::live_component::mount_live_prepare;
use cocoindex_core::engine::target_state::TargetStateProvider;
use cocoindex_core::state::stable_path::StableKey;
use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};

use crate::app::{AppInner, StatsGroupHandle, StatsGroupOptions};
use crate::error::{Error, Result};
use crate::live_component::{
    ExceptionContext, ExceptionHandler, LiveComponent, LiveMapView, MountEachLiveComponent,
    MountKind, build_chained_on_error, new_operator, start_process_live,
};
use crate::profile::{BoxedHandler, BoxedProcessor, RustProfile, Value};

type ContextFingerprinter<T> = Arc<dyn Fn(&str, &T) -> Result<Fingerprint> + Send + Sync>;

/// A named context key for app-provided resources.
///
/// - [`ContextKey::new`] stores arbitrary `Send + Sync` resources (no change
///   tracking).
/// - [`ContextKey::new_detect_change`] tracks a serializable value: memoized
///   work is invalidated when the whole value's fingerprint changes.
/// - [`ContextKey::new_with_state`] tracks a derived state of an arbitrary
///   value. Only changes to the extracted state invalidate memoized work.
pub struct ContextKey<T> {
    name: Arc<str>,
    detect_change: bool,
    fingerprint_fn: Option<ContextFingerprinter<T>>,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Clone for ContextKey<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            detect_change: self.detect_change,
            fingerprint_fn: self.fingerprint_fn.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T> ContextKey<T> {
    /// Create a named context key without memo change tracking.
    ///
    /// # Panics
    /// Panics if the same key name has already been constructed in this
    /// process.
    pub fn new(name: impl Into<String>) -> Self {
        Self::with_parts(name.into(), false, None)
    }

    /// The stable key name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Whether values provided for this key participate in memo invalidation.
    pub fn detect_change(&self) -> bool {
        self.detect_change
    }

    fn with_parts(
        name: String,
        detect_change: bool,
        fingerprint_fn: Option<ContextFingerprinter<T>>,
    ) -> Self {
        let used = USED_CONTEXT_KEYS.get_or_init(|| Mutex::new(HashSet::new()));
        let duplicate = {
            let mut used = used.lock().expect("context key registry poisoned");
            !used.insert(name.clone())
        };
        assert!(!duplicate, "Context key {name} already used");
        Self {
            name: Arc::from(name),
            detect_change,
            fingerprint_fn,
            _marker: PhantomData,
        }
    }
}

impl<T: Serialize> ContextKey<T> {
    /// Create a named context key whose provided values invalidate memoized
    /// work when their serialized fingerprint changes.
    pub fn new_detect_change(name: impl Into<String>) -> Self {
        let fingerprint_fn: ContextFingerprinter<T> = Arc::new(|name: &str, value: &T| {
            Fingerprint::from(&("context_key", name, value))
                .map_err(|e| Error::engine(format!("context key fingerprint error: {e}")))
        });
        Self::with_parts(name.into(), true, Some(fingerprint_fn))
    }
}

impl<T> ContextKey<T> {
    /// Create a named context key whose memo invalidation is driven by a
    /// *derived state* rather than the whole value. `state_fn` extracts a
    /// serializable state from the provided value; memoized work that reads
    /// this key (via [`Ctx::get_key`]) is invalidated only when that state's
    /// fingerprint changes.
    ///
    /// Use this for resources that are not serializable, such as DB pools or
    /// clients, or when only a narrow identity like a connection string or
    /// schema version should affect memoization. The value type `T` need not be
    /// `Serialize`; only the extracted state must be.
    pub fn new_with_state<S, SF>(name: impl Into<String>, state_fn: SF) -> Self
    where
        S: Serialize,
        SF: Fn(&T) -> S + Send + Sync + 'static,
    {
        let fingerprint_fn: ContextFingerprinter<T> = Arc::new(move |name: &str, value: &T| {
            let state = state_fn(value);
            Fingerprint::from(&("context_key", name, &state))
                .map_err(|e| Error::engine(format!("context key state fingerprint error: {e}")))
        });
        Self::with_parts(name.into(), true, Some(fingerprint_fn))
    }
}

static USED_CONTEXT_KEYS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

#[derive(Default)]
pub(crate) struct ContextStore {
    values: HashMap<Arc<str>, Arc<dyn Any + Send + Sync>>,
    fingerprints: HashMap<Arc<str>, Fingerprint>,
}

impl ContextStore {
    pub(crate) fn provide<T: Send + Sync + 'static>(
        &mut self,
        key: &ContextKey<T>,
        value: T,
    ) -> Result<()> {
        if let Some(fingerprint_fn) = &key.fingerprint_fn {
            let fp = fingerprint_fn(&key.name, &value)?;
            self.fingerprints.insert(key.name.clone(), fp);
        }
        self.values.insert(key.name.clone(), Arc::new(value));
        Ok(())
    }

    pub(crate) fn register_logic(&self, env: &Environment<RustProfile>) {
        for fp in self.fingerprints.values() {
            env.register_logic(*fp);
        }
    }

    fn get<T: Send + Sync + 'static>(&self, key: &ContextKey<T>) -> Option<&T> {
        self.values
            .get(&key.name)
            .and_then(|value| value.downcast_ref::<T>())
    }

    fn fingerprint<T>(&self, key: &ContextKey<T>) -> Option<Fingerprint> {
        self.fingerprints.get(&key.name).copied()
    }
}

/// Pipeline context passed to closures inside `App::update()` / `App::run()`.
#[derive(Clone)]
pub struct Ctx {
    /// The core component processor context. Some when running inside a
    /// pipeline (enables LMDB memoization), None for standalone usage.
    pub(crate) comp_ctx: Option<ComponentProcessorContext<RustProfile>>,
    pub(crate) state: Arc<AppInner>,
    /// The function-call context this `Ctx` is scoped to. It is set when
    /// entering a memoized body (`memo`/`batch`) or a child `scope`, so that
    /// `get_key` records change-detection dependencies against the *correct*
    /// memo entry. This is a plain owned value (not a shared slot): each
    /// concurrent body receives its own scoped `Ctx`. `None` at the app root
    /// and in standalone use.
    pub(crate) fn_ctx: Option<Arc<FnCallContext>>,
    /// Exception handlers in scope for background work mounted from this `Ctx`,
    /// ordered outermost→innermost. Empty at the root; extended by
    /// `mount_live_with_handler` so nested live components inherit ancestors'
    /// handlers (the Python handler chain).
    pub(crate) handler_chain: Arc<Vec<crate::live_component::ExceptionHandler>>,
}

impl Ctx {
    pub(crate) fn new(
        comp_ctx: Option<ComponentProcessorContext<RustProfile>>,
        state: Arc<AppInner>,
    ) -> Self {
        Self {
            comp_ctx,
            state,
            fn_ctx: None,
            handler_chain: Arc::new(Vec::new()),
        }
    }

    pub(crate) fn new_with_handlers(
        comp_ctx: Option<ComponentProcessorContext<RustProfile>>,
        state: Arc<AppInner>,
        handler_chain: Arc<Vec<crate::live_component::ExceptionHandler>>,
    ) -> Self {
        Self {
            comp_ctx,
            state,
            fn_ctx: None,
            handler_chain,
        }
    }

    fn child(&self, comp_ctx: Option<ComponentProcessorContext<RustProfile>>) -> Self {
        Self {
            comp_ctx,
            state: self.state.clone(),
            fn_ctx: None,
            handler_chain: self.handler_chain.clone(),
        }
    }

    /// Return a clone of this `Ctx` scoped to `fn_ctx`, so `get_key` records
    /// change-detection dependencies against that function call's memo entry.
    pub(crate) fn with_fn_ctx(&self, fn_ctx: Arc<FnCallContext>) -> Self {
        Self {
            comp_ctx: self.comp_ctx.clone(),
            state: self.state.clone(),
            fn_ctx: Some(fn_ctx),
            handler_chain: self.handler_chain.clone(),
        }
    }

    /// Execute a non-memoized `#[cocoindex::function]` body while recording
    /// its logic and context dependencies into the surrounding function call.
    #[doc(hidden)]
    pub async fn __coco_tracked_fn<T, F, Fut>(
        &self,
        module_path: &'static str,
        fn_name: &'static str,
        code_hash: u64,
        f: F,
    ) -> Result<T>
    where
        F: FnOnce(Ctx) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let fp = Fingerprint::from(&("cocoindex_fn", module_path, fn_name, code_hash))
            .map_err(|e| Error::engine(format!("function logic fingerprint error: {e}")))?;
        let fn_ctx = Arc::new(FnCallContext::default());
        fn_ctx.add_fn_logic_dep(fp);
        let _guard = TrackedFnCallGuard {
            comp_ctx: self.comp_ctx.clone(),
            parent_fn_ctx: self.fn_ctx.clone(),
            fn_ctx: fn_ctx.clone(),
        };
        f(self.with_fn_ctx(fn_ctx)).await
    }
}

struct TrackedFnCallGuard {
    comp_ctx: Option<ComponentProcessorContext<RustProfile>>,
    parent_fn_ctx: Option<Arc<FnCallContext>>,
    fn_ctx: Arc<FnCallContext>,
}

impl Drop for TrackedFnCallGuard {
    fn drop(&mut self) {
        if let Some(parent) = &self.parent_fn_ctx {
            parent.join_child(&self.fn_ctx);
        } else if let Some(comp_ctx) = &self.comp_ctx {
            comp_ctx.join_fn_call(&self.fn_ctx);
        }
    }
}

pub(crate) struct FnCallGuard<'a> {
    comp_ctx: &'a ComponentProcessorContext<RustProfile>,
    fn_ctx: Arc<FnCallContext>,
}

impl<'a> Drop for FnCallGuard<'a> {
    fn drop(&mut self) {
        self.comp_ctx.join_fn_call(&self.fn_ctx);
    }
}

pub(crate) fn fn_call_guard<'a>(
    comp_ctx: &'a ComponentProcessorContext<RustProfile>,
    fn_ctx: Arc<FnCallContext>,
) -> FnCallGuard<'a> {
    FnCallGuard { comp_ctx, fn_ctx }
}

struct StatsGroupEndGuard {
    ctx: Option<ComponentProcessorContext<RustProfile>>,
}

impl StatsGroupEndGuard {
    fn new(ctx: ComponentProcessorContext<RustProfile>) -> Self {
        Self { ctx: Some(ctx) }
    }

    fn end(mut self) {
        if let Some(ctx) = self.ctx.take() {
            ctx.end_stats_group();
        }
    }
}

impl Drop for StatsGroupEndGuard {
    fn drop(&mut self) {
        if let Some(ctx) = self.ctx.take() {
            ctx.end_stats_group();
        }
    }
}

impl Ctx {
    /// Try to get a shared resource and return a typed error if missing.
    ///
    /// # Errors
    ///
    /// Returns [`Error::MissingContext`] if the requested type `T` was not
    /// provided to the app builder.
    pub fn get_or_err<T: Send + Sync + 'static>(&self) -> Result<&T> {
        self.state
            .state
            .get::<T>()
            .ok_or_else(|| Error::MissingContext(std::any::type_name::<T>().to_string()))
    }

    /// Try to get a shared resource. Returns None if not provided.
    pub fn try_get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.state.state.get::<T>()
    }

    /// Try to get a shared resource by named [`ContextKey`].
    ///
    /// If the key was created with [`ContextKey::new_detect_change`], the
    /// current memo/function call records a dependency on this value's
    /// fingerprint.
    pub fn get_key<T: Send + Sync + 'static>(&self, key: &ContextKey<T>) -> Result<&T> {
        let value = self
            .state
            .context
            .get(key)
            .ok_or_else(|| Error::MissingContext(key.name().to_string()))?;
        if key.detect_change()
            && let Some(fp) = self.state.context.fingerprint(key)
        {
            if let Some(fn_ctx) = &self.fn_ctx {
                fn_ctx.add_context_change_dep(fp);
            } else if let Some(comp_ctx) = &self.comp_ctx {
                let fn_ctx = FnCallContext::default();
                fn_ctx.add_context_change_dep(fp);
                comp_ctx.join_fn_call(&fn_ctx);
            }
        }
        Ok(value)
    }

    /// Returns true if this context has LMDB memoization available
    /// (i.e., running inside an `App::update()` pipeline).
    pub fn has_pipeline_context(&self) -> bool {
        self.comp_ctx.is_some()
    }

    pub(crate) async fn next_raw_id(&self) -> Result<u64> {
        let Some(comp_ctx) = &self.comp_ctx else {
            return Err(Error::engine(
                "IdGenerator requires an active pipeline context",
            ));
        };
        comp_ctx.app_ctx().next_id(None).await.map_err(Error::from)
    }

    pub(crate) fn register_root_target_provider(
        &self,
        name: impl Into<String>,
        handler: BoxedHandler,
    ) -> Result<TargetStateProvider<RustProfile>> {
        let Some(comp_ctx) = &self.comp_ctx else {
            return Err(Error::engine(
                "target providers require an active pipeline context",
            ));
        };
        execution::register_root_target_state_provider(comp_ctx, name.into(), handler)
            .map_err(Error::from)
    }

    pub(crate) fn register_attachment_target_provider(
        &self,
        parent: &TargetStateProvider<RustProfile>,
        att_type: &str,
    ) -> Result<TargetStateProvider<RustProfile>> {
        let Some(comp_ctx) = &self.comp_ctx else {
            return Err(Error::engine(
                "target providers require an active pipeline context",
            ));
        };
        parent
            .register_attachment_provider(comp_ctx, att_type)
            .map_err(Error::from)
    }

    pub(crate) fn declare_target_state(
        &self,
        provider: TargetStateProvider<RustProfile>,
        key: StableKey,
        value: Value,
    ) -> Result<()> {
        let Some(comp_ctx) = &self.comp_ctx else {
            return Err(Error::engine(
                "target states require an active pipeline context",
            ));
        };
        let fn_ctx = self
            .fn_ctx
            .clone()
            .unwrap_or_else(|| Arc::new(FnCallContext::default()));
        execution::declare_target_state(comp_ctx, &fn_ctx, provider, key, value)
            .map_err(Error::from)
    }

    pub(crate) fn declare_target_state_with_child(
        &self,
        provider: TargetStateProvider<RustProfile>,
        key: StableKey,
        value: Value,
    ) -> Result<TargetStateProvider<RustProfile>> {
        let Some(comp_ctx) = &self.comp_ctx else {
            return Err(Error::engine(
                "target states require an active pipeline context",
            ));
        };
        let fn_ctx = self
            .fn_ctx
            .clone()
            .unwrap_or_else(|| Arc::new(FnCallContext::default()));
        execution::declare_target_state_with_child(comp_ctx, &fn_ctx, provider, key, value)
            .map_err(Error::from)
    }

    /// Aggregate stats for components mounted inside `f` into a separate named
    /// group. Returns the closure result and a handle for polling/watching the
    /// group's stats.
    ///
    /// This does not print anything; use [`Ctx::stats_group_with_options`] to
    /// enable stdout progress reporting for the group.
    pub async fn stats_group<T, F, Fut>(
        &self,
        title: impl Into<String>,
        f: F,
    ) -> Result<(T, StatsGroupHandle)>
    where
        T: Send + 'static,
        F: FnOnce(Ctx, StatsGroupHandle) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        self.stats_group_with_options(title, StatsGroupOptions::default(), f)
            .await
    }

    /// Like [`Ctx::stats_group`], but with explicit [`StatsGroupOptions`]. Set
    /// `report_to_stdout` to print scoped progress, optionally with a custom
    /// `refresh_interval`.
    pub async fn stats_group_with_options<T, F, Fut>(
        &self,
        title: impl Into<String>,
        options: StatsGroupOptions,
        f: F,
    ) -> Result<(T, StatsGroupHandle)>
    where
        T: Send + 'static,
        F: FnOnce(Ctx, StatsGroupHandle) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let Some(comp_ctx) = &self.comp_ctx else {
            return Err(Error::engine(
                "stats_group requires an active pipeline context",
            ));
        };
        let (derived, stats) = comp_ctx.begin_stats_group(
            title.into(),
            options.report_to_stdout,
            options.refresh_interval.map(|d| d.as_secs_f64()),
        );
        let handle = StatsGroupHandle::new(stats);
        let scoped_ctx = Ctx {
            comp_ctx: Some(derived.clone()),
            state: self.state.clone(),
            fn_ctx: self.fn_ctx.clone(),
            handler_chain: self.handler_chain.clone(),
        };
        let group_guard = StatsGroupEndGuard::new(derived);
        let result = f(scoped_ctx, handle.clone()).await;
        group_guard.end();
        Ok((result?, handle))
    }

    /// Mount a periodic refresh component under `key`.
    ///
    /// In catch-up mode this runs `f` once and returns. In live mode it runs
    /// once, marks the component ready, then repeats after `interval` until the
    /// app/live component is cancelled.
    pub async fn auto_refresh<K, F, Fut>(&self, key: &K, interval: Duration, f: F) -> Result<()>
    where
        K: Display,
        F: Fn(Ctx) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let Some(comp_ctx) = &self.comp_ctx else {
            return f(self.clone()).await;
        };

        let key_str = key.to_string();
        let child_stable_key = StableKey::Str(Arc::from(key_str.as_str()));
        let child_path = comp_ctx.stable_path().concat_part(child_stable_key);
        let stable_path = child_path.to_string();
        let fn_ctx = Arc::new(FnCallContext::default());
        let pending = mount_live_prepare(comp_ctx, &fn_ctx, child_path, comp_ctx.live())
            .map_err(|e| Error::engine(format!("{e}")))?;
        let _guard = fn_call_guard(comp_ctx, fn_ctx);
        let result = pending
            .complete()
            .await
            .map_err(|e| Error::engine(format!("{e}")))?;
        let controller = result.controller;
        let readiness_handle = result.readiness_handle;
        let state = self.state.clone();
        let processor_name = format!("auto_refresh:{key_str}");
        let handler_chain = self.handler_chain.clone();
        let env_name = self.state.name.clone();
        controller.start({
            let controller = controller.clone();
            async move {
                let mut ready_marked = false;
                loop {
                    let processor =
                        auto_refresh_processor(state.clone(), f.clone(), processor_name.clone());
                    let on_error = build_chained_on_error(
                        &handler_chain,
                        ExceptionContext {
                            env_name: env_name.clone(),
                            stable_path: stable_path.clone(),
                            parent_stable_path: None,
                            processor_name: Some(processor_name.clone()),
                            mount_kind: MountKind::UpdateFull,
                            is_background: ready_marked && controller.is_live(),
                        },
                    );
                    match controller.update_full(processor, on_error).await {
                        Ok(()) => {
                            controller.mark_ready().await;
                            ready_marked = true;
                        }
                        Err(err) if ready_marked && !err.is_cancelled() => {
                            tracing::error!(
                                "auto_refresh cycle failed after readiness for `{key_str}`: {err:?}"
                            );
                        }
                        Err(err) => return Err(err),
                    }
                    tokio::time::sleep(interval).await;
                }
            }
        });
        readiness_handle
            .ready()
            .await
            .map_err(|e| Error::engine(format!("{e}")))
    }

    /// Mount a [`LiveComponent`] under `key`.
    ///
    /// The framework runs the component's `process_live` body once, on its own
    /// task, and returns when the component marks itself ready. In catch-up
    /// (non-live) mode the default body runs a single full pass; in live mode it
    /// keeps reacting to its source in the background until the app is dropped.
    pub async fn mount_live<K, C>(&self, key: &K, component: C) -> Result<()>
    where
        K: Display,
        C: LiveComponent,
    {
        self.mount_live_impl(key.to_string(), Arc::new(component), None)
            .await
    }

    /// Like [`Ctx::mount_live`], but routes background failures (full-pass and
    /// incremental update/delete) through `handler`. The handler returns
    /// `Ok(())` to swallow a failure or `Err(_)` to propagate it.
    pub async fn mount_live_with_handler<K, C, H>(
        &self,
        key: &K,
        component: C,
        handler: H,
    ) -> Result<()>
    where
        K: Display,
        C: LiveComponent,
        H: Fn(&Error, &ExceptionContext) -> Result<()> + Send + Sync + 'static,
    {
        let handler: ExceptionHandler = Arc::new(handler);
        self.mount_live_impl(key.to_string(), Arc::new(component), Some(handler))
            .await
    }

    /// Mount one child component per item from a live change feed, keyed by the
    /// feed's key. In catch-up mode the feed is scanned once; in live mode the
    /// feed streams incremental adds/removes that mount/delete children
    /// individually. The analogue of [`Ctx::mount_each`] for live sources.
    pub async fn mount_each_live<Key, K, V, Feed, F, Fut>(
        &self,
        key: &Key,
        feed: Feed,
        process_fn: F,
    ) -> Result<()>
    where
        Key: Display,
        K: Display + Send + Sync + 'static,
        V: Send + Sync + 'static,
        Feed: LiveMapView<K, V>,
        F: Fn(Ctx, V) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let component = MountEachLiveComponent::<K, V, Feed>::new(feed, process_fn);
        self.mount_live(key, component).await
    }

    async fn mount_live_impl(
        &self,
        key_str: String,
        instance: Arc<dyn LiveComponent>,
        handler: Option<ExceptionHandler>,
    ) -> Result<()> {
        let Some(comp_ctx) = &self.comp_ctx else {
            // No pipeline context — run a single full pass directly, matching
            // `auto_refresh`'s standalone behavior.
            return instance.process(self.clone()).await;
        };

        let child_stable_key = StableKey::Str(Arc::from(key_str.as_str()));
        let child_path = comp_ctx.stable_path().concat_part(child_stable_key);
        let fn_ctx = Arc::new(FnCallContext::default());
        let pending = mount_live_prepare(comp_ctx, &fn_ctx, child_path.clone(), comp_ctx.live())
            .map_err(|e| Error::engine(format!("{e}")))?;
        let _guard = fn_call_guard(comp_ctx, fn_ctx);
        let result = pending
            .complete()
            .await
            .map_err(|e| Error::engine(format!("{e}")))?;
        let controller = result.controller;
        let readiness_handle = result.readiness_handle;

        // Inherit ancestors' handlers and append this component's own, so an
        // unswallowed failure walks outward through the chain.
        let handler_chain = match handler {
            Some(handler) => {
                let mut chain = (*self.handler_chain).clone();
                chain.push(handler);
                Arc::new(chain)
            }
            None => self.handler_chain.clone(),
        };

        let operator = new_operator(
            controller.clone(),
            self.state.clone(),
            child_path,
            instance.clone(),
            handler_chain,
            format!("live:{key_str}"),
        );
        start_process_live(&controller, instance, operator);

        readiness_handle
            .ready()
            .await
            .map_err(|e| Error::engine(format!("{e}")))
    }

    /// Named sub-component. Creates a child scope in the pipeline tree.
    ///
    /// The key determines the child's stable path for memoization and
    /// target state tracking.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::ctx::Ctx;
    /// # async fn doc(ctx: &Ctx) -> cocoindex::error::Result<()> {
    /// let val = ctx.scope(&"child", |child_ctx| async move {
    ///     Ok(42)
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the closure returns an error, or if stable
    /// path/component tracking fails internally.
    pub async fn scope<K, T, F, Fut>(&self, key: &K, f: F) -> Result<T>
    where
        K: Display,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let Some(comp_ctx) = &self.comp_ctx else {
            // No pipeline context — just run the closure directly.
            let child_ctx = self.child(None);
            return f(child_ctx).await;
        };

        let key_str = key.to_string();
        let child_stable_key = StableKey::Str(Arc::from(key_str.as_str()));
        let child_path = comp_ctx.stable_path().concat_part(child_stable_key);

        let fn_ctx = Arc::new(FnCallContext::default());
        let child_component = comp_ctx
            .component()
            .mount_child(&fn_ctx, child_path)
            .map_err(|e| Error::engine(format!("{e}")))?;

        // Guard to ensure `join_fn_call` is executed even if `f` panics or the future
        // is dropped/cancelled early.
        let _guard = fn_call_guard(comp_ctx, fn_ctx.clone());

        let state = self.state.clone();
        let scope_fn_ctx = fn_ctx.clone();
        let scope_handler_chain = self.handler_chain.clone();
        let processor = BoxedProcessor::new(
            move |child_comp_ctx| {
                let ctx = Ctx {
                    comp_ctx: Some(child_comp_ctx),
                    state: state.clone(),
                    fn_ctx: Some(scope_fn_ctx.clone()),
                    handler_chain: scope_handler_chain.clone(),
                };
                Box::pin(async move {
                    let result = f(ctx).await?;
                    Value::from_serializable(&result)
                })
            },
            None,
            format!("scope:{key_str}"),
        );

        let handle = match child_component.use_mount(comp_ctx, processor).await {
            Ok(handle) => handle,
            Err(err) => {
                return Err(Error::engine(format!("{err}")));
            }
        };
        let value = handle.result(Some(comp_ctx)).await;
        let value = match value {
            Ok(value) => value,
            Err(err) => {
                return Err(Error::engine(format!("{err}")));
            }
        };
        let result: T = match value.deserialize() {
            Ok(result) => result,
            Err(err) => {
                return Err(Error::engine(format!("{err}")));
            }
        };
        Ok(result)
    }

    /// Cached computation. If `key` hasn't changed since the last run,
    /// returns the cached result from LMDB without executing `f`.
    ///
    /// The closure receives a `Ctx` scoped to this memo call. Use *that* `Ctx`
    /// (not a captured outer one) for `get_key` so change-detection
    /// dependencies are attributed to this memo entry — this is what keeps
    /// invalidation correct when memo bodies run concurrently.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::ctx::Ctx;
    /// # async fn doc(ctx: &Ctx, fingerprint: &str) -> cocoindex::error::Result<()> {
    /// let processed = ctx.memo(&fingerprint, |ctx| async move {
    ///     // ... expensive computation, using `ctx` for `get_key` ...
    ///     Ok("result".to_string())
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the closure returns an error, or if LMDB cache
    /// serialization/deserialization fails.
    pub async fn memo<K, T, F, Fut>(&self, key: &K, f: F) -> Result<T>
    where
        K: Serialize,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        crate::memo::cached(self, key, f).await
    }

    /// Batch-process items with per-item memoization.
    ///
    /// Probes the memo cache for each item. Cache hits return stored values.
    /// Cache misses are collected and passed to `f` as a single batch.
    /// Results are stored back and merged in original order.
    ///
    /// `f` must return exactly one result per miss item, in the same order.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::ctx::Ctx;
    /// # async fn doc(ctx: &Ctx, items: Vec<String>) -> cocoindex::error::Result<()> {
    /// let results = ctx.batch(
    ///     items,
    ///     |item| item.len(), // using length as cache key for demonstration
    ///     |_ctx, misses| async move {
    ///         Ok(misses.into_iter().map(|s| s.to_uppercase()).collect())
    ///     }
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The closure receives a `Ctx` scoped to this batch's memo call (shared by
    /// all miss items); use it for `get_key` so change-detection dependencies
    /// are attributed correctly.
    ///
    /// # Errors
    ///
    /// Returns an error if the closure returns an error, or if there's an
    /// LMDB or serialization error during memoization.
    pub async fn batch<I, K, T, F, Fut>(
        &self,
        items: I,
        key_fn: impl Fn(&I::Item) -> K,
        f: F,
    ) -> Result<Vec<T>>
    where
        I: IntoIterator,
        I::Item: Send + 'static,
        K: Serialize,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
        F: FnOnce(Ctx, Vec<I::Item>) -> Fut + Send,
        Fut: Future<Output = Result<Vec<T>>> + Send,
    {
        crate::memo::batch(self, items, move |item| Ok(key_fn(item)), f).await
    }

    /// Run a closure concurrently for each item, creating a child scope per item.
    ///
    /// Each item gets its own `Ctx` child scope keyed by `key_fn(item)`.
    /// All closures run concurrently via `try_join_all`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::ctx::Ctx;
    /// # async fn doc(ctx: &Ctx, tasks: Vec<String>) -> cocoindex::error::Result<()> {
    /// let results = ctx.mount_each(
    ///     tasks,
    ///     |task| task.clone(), // use the task string as scope key
    ///     |child_ctx, task| async move {
    ///         Ok(format!("processed {task}"))
    ///     }
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if any of the closures fail. The first encountered error
    /// is returned.
    pub async fn mount_each<I, K, F, Fut, T>(
        &self,
        items: I,
        key_fn: impl Fn(&I::Item) -> K,
        f: F,
    ) -> Result<Vec<T>>
    where
        I: IntoIterator,
        I::Item: Send + 'static,
        K: Display,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
        F: Fn(Ctx, I::Item) -> Fut + Send + Clone + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let mut keys = rustc_hash::FxHashSet::default();
        let mut keyed = Vec::new();

        for item in items {
            let key = key_fn(&item).to_string();
            if !keys.insert(key.clone()) {
                return Err(Error::engine(format!(
                    "duplicate key `{}` in mount_each batch",
                    key
                )));
            }
            keyed.push((key, item));
        }

        let futs: Vec<_> = keyed
            .into_iter()
            .map(|(key, item)| {
                let f = f.clone();
                async move { self.scope(&key, move |child| f(child, item)).await }
            })
            .collect();

        futures::future::try_join_all(futs).await
    }

    /// Run a closure concurrently for each item within the current scope (no child scopes).
    ///
    /// Like `futures::future::try_join_all` but with a mapping closure.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::ctx::Ctx;
    /// # async fn doc(ctx: &Ctx, tasks: Vec<String>) -> cocoindex::error::Result<()> {
    /// let results = ctx.map(tasks, |task| async move {
    ///     Ok(format!("processed {task}"))
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if any of the closures return an error.
    pub async fn map<I, F, Fut, T>(&self, items: I, f: F) -> Result<Vec<T>>
    where
        I: IntoIterator,
        T: Send + 'static,
        F: Fn(I::Item) -> Fut,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let futs: Vec<_> = items.into_iter().map(f).collect();
        futures::future::try_join_all(futs).await
    }

    /// Declare a file output. Writes content to the given path.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::ctx::Ctx;
    /// # async fn doc(ctx: &Ctx) -> cocoindex::error::Result<()> {
    /// ctx.write_file("output.txt", b"hello world")?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an [`Error::Io`] if the parent directory cannot be created or
    /// if the file cannot be written to disk.
    pub fn write_file(&self, path: impl AsRef<Path>, content: &[u8]) -> Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(Error::Io)?;
        }
        std::fs::write(path, content).map_err(Error::Io)
    }
}

fn auto_refresh_processor<F, Fut>(
    state: Arc<AppInner>,
    f: F,
    processor_name: String,
) -> BoxedProcessor
where
    F: Fn(Ctx) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    BoxedProcessor::new(
        move |comp_ctx| {
            let ctx = Ctx::new(Some(comp_ctx), state.clone());
            Box::pin(async move {
                f(ctx).await?;
                Ok(Value::unit())
            })
        },
        None,
        processor_name,
    )
}
