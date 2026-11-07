//! Pipeline context: scope, memo.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use cocoindex_core::engine::component::ComponentExecutionHandle;
use cocoindex_core::engine::context::{ComponentProcessorContext, FnCallContext, FnFrameId};
use cocoindex_core::engine::deadline::DeadlineContext;
use cocoindex_core::engine::environment::Environment;
use cocoindex_core::engine::execution;
use cocoindex_core::engine::live_component::mount_live_prepare;
use cocoindex_core::engine::target_state::TargetStateProvider;
use cocoindex_core::state::stable_path::{StableKey, StablePath, StablePathRef};
use cocoindex_utils::fingerprint::Fingerprint;
use futures::stream::{Stream, StreamExt};
use rustc_hash::FxHashSet;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::app::{AppInner, StatsGroupHandle, StatsGroupOptions};
use crate::deps::{DataDep, LogicDep, PropagatedCallback};
use crate::error::{Error, Result};
use crate::live_component::{
    ExceptionContext, ExceptionHandler, ExceptionSource, LiveComponent, LiveMapView,
    MountEachLiveComponent, MountKind, build_chained_on_error, new_operator, start_process_live,
};
use crate::logic::FnLogicEntry;
use crate::memo::{MemoInput, MemoStateDecision, MemoStateValue};
use crate::profile::{BoxedHandler, BoxedProcessor, RustProfile, Value};
use crate::target_state::IntoStableKey;
use crate::user_state::{IntoStateKey, StateHandle};

type ContextFingerprinter<T> = Arc<dyn Fn(&str, &T) -> Result<Fingerprint> + Send + Sync>;
type ContextStateFuture =
    Pin<Box<dyn Future<Output = Result<Option<MemoStateDecision>>> + Send + 'static>>;
type ContextStateFn<T> =
    Arc<dyn Fn(Arc<T>, Option<MemoStateValue>) -> ContextStateFuture + Send + Sync>;
type StoredContextStateFn = Arc<dyn Fn(Option<MemoStateValue>) -> ContextStateFuture + Send + Sync>;

/// Declare a process-wide [`ContextKey`] backed by a [`std::sync::LazyLock`].
///
/// By default, the Rust identifier is also the context key's stable name. Use
/// `key = "..."` when the persisted name must differ from the identifier or
/// remain stable across an identifier rename. Change detection is disabled by
/// default. With `detect_change`, the value's [`MemoInput`] implementation
/// defines its memo identity and optional external-state validation. For a
/// foreign type that cannot implement [`MemoInput`] because of Rust's orphan
/// rules, use a local newtype with a manual [`MemoInput`] implementation.
///
/// ```
/// # #[derive(cocoindex::MemoInput)]
/// # struct AppConfig;
/// # struct Database;
/// cocoindex::context_key!(static CONFIG: AppConfig);
/// cocoindex::context_key!(static TRACKED_CONFIG: AppConfig, detect_change);
/// cocoindex::context_key!(
///     static DB: Database,
///     key = "my_package/database"
/// );
/// ```
///
/// The key name is a stable persistent identity used in target-state and memo
/// keys. Renaming an identifier that uses the default name therefore changes
/// its identity; use `key = "..."` when that is not intended. `key =` changes
/// only this logical name. When both optional arguments are present, they must
/// appear in this order: `key = "..."`, then `detect_change`.
#[macro_export]
macro_rules! context_key {
    ($vis:vis static $key:ident: $value:ty, key = $name:literal, detect_change $(,)?) => {
        $vis static $key: ::std::sync::LazyLock<$crate::ContextKey<$value>> =
            ::std::sync::LazyLock::new(|| $crate::ContextKey::new_detect_change($name));
    };
    ($vis:vis static $key:ident: $value:ty, detect_change $(,)?) => {
        $vis static $key: ::std::sync::LazyLock<$crate::ContextKey<$value>> =
            ::std::sync::LazyLock::new(|| {
                $crate::ContextKey::new_detect_change(::std::stringify!($key))
            });
    };
    ($vis:vis static $key:ident: $value:ty, key = $name:literal $(,)?) => {
        $vis static $key: ::std::sync::LazyLock<$crate::ContextKey<$value>> =
            ::std::sync::LazyLock::new(|| $crate::ContextKey::new($name));
    };
    ($vis:vis static $key:ident: $value:ty $(,)?) => {
        $vis static $key: ::std::sync::LazyLock<$crate::ContextKey<$value>> =
            ::std::sync::LazyLock::new(|| $crate::ContextKey::new(::std::stringify!($key)));
    };
    ($($invalid:tt)*) => {
        ::core::compile_error!(
            "invalid context_key! syntax; expected `static NAME: Type` followed by optional \
             `key = \"...\"` and `detect_change` in that order"
        );
    };
}

/// A named context key for app-provided resources.
///
/// - [`ContextKey::new`] stores arbitrary `Send + Sync` resources (no change
///   tracking).
/// - [`ContextKey::new_detect_change`] uses the value type's [`MemoInput`]
///   behavior to invalidate memoized work.
pub struct ContextKey<T> {
    name: Arc<str>,
    detect_change: bool,
    fingerprint_fn: Option<ContextFingerprinter<T>>,
    state_fn: Option<ContextStateFn<T>>,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Clone for ContextKey<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            detect_change: self.detect_change,
            fingerprint_fn: self.fingerprint_fn.clone(),
            state_fn: self.state_fn.clone(),
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
        Self::with_parts(name.into(), false, None, None)
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
        state_fn: Option<ContextStateFn<T>>,
    ) -> Self {
        let used = USED_CONTEXT_KEYS.get_or_init(|| Mutex::new(HashSet::new()));
        let duplicate = {
            let mut used = used.lock().expect("context key registry poisoned");
            !used.insert(name.clone())
        };
        assert!(
            !duplicate,
            "Context key {name:?} is already used; choose a unique identifier or set \
             `key = \"stable_unique_name\"`"
        );
        Self {
            name: Arc::from(name),
            detect_change,
            fingerprint_fn,
            state_fn,
            _marker: PhantomData,
        }
    }
}

impl<T: MemoInput + 'static> ContextKey<T> {
    /// Create a named context key whose provided values invalidate memoized
    /// work when their type-defined memo identity or external state changes.
    pub fn new_detect_change(name: impl Into<String>) -> Self {
        let fingerprint_fn: ContextFingerprinter<T> = Arc::new(|name: &str, value: &T| {
            crate::memo::memo_input_fingerprint("context_key", name, value)
        });
        let state_fn: ContextStateFn<T> = Arc::new(|value, previous| {
            Box::pin(async move { value.memo_state(previous.as_ref()).await })
        });
        Self::with_parts(name.into(), true, Some(fingerprint_fn), Some(state_fn))
    }
}

static USED_CONTEXT_KEYS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

#[derive(Default, Clone)]
pub(crate) struct ContextStore {
    values: HashMap<Arc<str>, Arc<dyn Any + Send + Sync>>,
    fingerprints: HashMap<Arc<str>, Fingerprint>,
    state_fns: HashMap<Fingerprint, StoredContextStateFn>,
}

impl ContextStore {
    pub(crate) fn provide<T: Send + Sync + 'static>(
        &mut self,
        key: &ContextKey<T>,
        value: T,
    ) -> Result<()> {
        let value = Arc::new(value);
        if let Some(fingerprint_fn) = &key.fingerprint_fn {
            let fp = fingerprint_fn(&key.name, value.as_ref())?;
            if let Some(previous_fp) = self.fingerprints.insert(key.name.clone(), fp) {
                self.state_fns.remove(&previous_fp);
            }
            if let Some(state_fn) = &key.state_fn {
                let state_fn = Arc::clone(state_fn);
                let value = Arc::clone(&value);
                self.state_fns.insert(
                    fp,
                    Arc::new(move |previous| state_fn(Arc::clone(&value), previous)),
                );
            }
        }
        self.values.insert(key.name.clone(), value);
        Ok(())
    }

    pub(crate) fn register_logic(&self, env: &Environment<RustProfile>) {
        for fp in self.fingerprints.values() {
            env.register_logic(*fp);
        }
    }

    pub(crate) async fn register_initial_states(
        &mut self,
        env: &Environment<RustProfile>,
    ) -> Result<()> {
        let fingerprints = self.state_fns.keys().copied().collect::<Vec<_>>();
        for fingerprint in fingerprints {
            let state_fn = self
                .state_fns
                .get(&fingerprint)
                .expect("context state function disappeared during initialization");
            if let Some(decision) = state_fn(None).await? {
                let (state, _) = decision.into_parts();
                env.register_context_initial_states(fingerprint, vec![state]);
            } else {
                self.state_fns.remove(&fingerprint);
            }
        }
        Ok(())
    }

    pub(crate) fn has_memo_state_fns(&self) -> bool {
        !self.state_fns.is_empty()
    }

    pub(crate) async fn validate_memo_states(
        &self,
        stored: &[(Fingerprint, Vec<Value>)],
    ) -> Result<ContextMemoValidation> {
        let mut updated = Vec::with_capacity(stored.len());
        let mut memo_valid = true;

        for (fingerprint, states) in stored {
            let Some(state_fn) = self.state_fns.get(fingerprint) else {
                memo_valid = false;
                updated.push((*fingerprint, states.clone()));
                continue;
            };
            if states.len() != 1 {
                memo_valid = false;
                updated.push((*fingerprint, states.clone()));
                continue;
            }
            let previous = MemoStateValue::from_profile_value(states[0].clone());
            let Some(decision) = state_fn(Some(previous)).await? else {
                memo_valid = false;
                updated.push((*fingerprint, states.clone()));
                continue;
            };
            let (state, valid) = decision.into_parts();
            memo_valid &= valid;
            updated.push((*fingerprint, vec![state]));
        }

        let states_changed =
            updated
                .iter()
                .zip(stored)
                .any(|((new_fp, new_values), (old_fp, old_values))| {
                    new_fp != old_fp
                        || new_values.len() != old_values.len()
                        || new_values
                            .iter()
                            .zip(old_values)
                            .any(|(new, old)| new.0 != old.0)
                });
        Ok(ContextMemoValidation {
            states: updated,
            memo_valid,
            states_changed,
        })
    }

    fn get<T: Send + Sync + 'static>(&self, key: &ContextKey<T>) -> Option<&T> {
        self.values
            .get(&key.name)
            .and_then(|value| value.downcast_ref::<T>())
    }

    /// Resolve a provided resource by its `ContextKey` name (the stable
    /// `db_key`), returning a shared handle. Target sinks call this at apply
    /// time via `HostCtx` (the environment's [`ContextStore`]) so connection
    /// identity in target keys stays a stable key while the live pool/client is
    /// resolved only when the sink runs (see `design_connectors.md` §5.5).
    // Used only by feature-gated connectors (postgres/sqlite today).
    #[allow(dead_code)]
    pub(crate) fn resolve<T: Send + Sync + 'static>(&self, db_key: &str) -> Option<Arc<T>> {
        self.values
            .get(db_key)
            .and_then(|value| value.clone().downcast::<T>().ok())
    }

    fn fingerprint<T>(&self, key: &ContextKey<T>) -> Option<Fingerprint> {
        self.fingerprints.get(&key.name).copied()
    }
}

pub(crate) struct ContextMemoValidation {
    pub(crate) states: Vec<(Fingerprint, Vec<Value>)>,
    pub(crate) memo_valid: bool,
    pub(crate) states_changed: bool,
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
    /// Deadline carried by this SDK context. The `Ctx` is the SDK-side
    /// carrier: constructors receive the initial value explicitly (core
    /// component contexts do not store a deadline; core receives it as an
    /// argument at each call). `with_timeout` returns a narrowed clone.
    pub(crate) deadline: DeadlineContext,
    /// Exception handlers in scope for background work mounted from this `Ctx`,
    /// ordered outermost→innermost. The environment's handler at the root;
    /// extended by [`Ctx::with_exception_handler`] and
    /// `mount_live_with_handler` so nested components inherit ancestors'
    /// handlers (the Python handler chain).
    pub(crate) handler_chain: Arc<Vec<crate::live_component::ExceptionHandler>>,
    /// Path parts [`Ctx::with_subpath`] added below the component's own path.
    /// Children mounted from this `Ctx` go under `component path + subpath`,
    /// the way nested `coco.mount` calls go under a `with
    /// coco.component_subpath(...)` block in Python. Empty by default.
    pub(crate) subpath: Arc<[StableKey]>,
}

impl Ctx {
    /// The root context of a component run: its handler chain starts from the
    /// environment's handler, as a Python component context does.
    pub(crate) fn new(
        comp_ctx: Option<ComponentProcessorContext<RustProfile>>,
        state: Arc<AppInner>,
        deadline: DeadlineContext,
    ) -> Self {
        let handler_chain = Arc::new(state.exception_handler.iter().cloned().collect());
        Self::new_with_handlers(comp_ctx, state, handler_chain, deadline)
    }

    pub(crate) fn new_with_handlers(
        comp_ctx: Option<ComponentProcessorContext<RustProfile>>,
        state: Arc<AppInner>,
        handler_chain: Arc<Vec<crate::live_component::ExceptionHandler>>,
        deadline: DeadlineContext,
    ) -> Self {
        Self {
            comp_ctx,
            state,
            fn_ctx: None,
            deadline,
            handler_chain,
            subpath: Arc::from([]),
        }
    }

    fn child(&self, comp_ctx: Option<ComponentProcessorContext<RustProfile>>) -> Self {
        // The SDK Ctx is the deadline carrier: derived views inherit it.
        let deadline = self.deadline;
        Self {
            comp_ctx,
            state: self.state.clone(),
            fn_ctx: None,
            deadline,
            handler_chain: self.handler_chain.clone(),
            subpath: Arc::from([]),
        }
    }

    /// Return a clone of this `Ctx` scoped to `fn_ctx`, so `get_key` records
    /// change-detection dependencies against that function call's memo entry.
    pub(crate) fn with_fn_ctx(&self, fn_ctx: Arc<FnCallContext>) -> Self {
        Self {
            comp_ctx: self.comp_ctx.clone(),
            state: self.state.clone(),
            fn_ctx: Some(fn_ctx),
            deadline: self.deadline,
            handler_chain: self.handler_chain.clone(),
            subpath: self.subpath.clone(),
        }
    }

    /// The stable path children mounted from this `Ctx` go under: the
    /// component's path plus any [`Ctx::with_subpath`] parts.
    fn mount_base_path(&self, comp_ctx: &ComponentProcessorContext<RustProfile>) -> StablePath {
        let base = comp_ctx.stable_path();
        if self.subpath.is_empty() {
            base.clone()
        } else {
            base.concat(StablePathRef(&self.subpath))
        }
    }

    /// The stable path of the child mounted under `key` from this `Ctx`.
    fn child_path(
        &self,
        comp_ctx: &ComponentProcessorContext<RustProfile>,
        key: StableKey,
    ) -> StablePath {
        self.mount_base_path(comp_ctx).concat_part(key)
    }

    /// Return a clone of this `Ctx` whose child mounts go under `key`, below
    /// the current path: `ctx.with_subpath("process")` followed by
    /// `use_mount!("a.md", ...)` mounts at `process/a.md`. The Rust analogue of
    /// a `with coco.component_subpath("process"):` block in Python. Chain calls
    /// for a multi-part subpath.
    ///
    /// `key` is any [`IntoStableKey`]: a string, an integer, a UUID, bytes, a
    /// [`StableKey`] (for a symbol or an array), or a reference to one.
    ///
    /// The scoped context still belongs to the same component: target states
    /// declared through it are the component's own, and
    /// [`Ctx::use_state`] is an error inside a subpath, as in Python, because
    /// the state would be owned by the component but keyed as if by a child.
    #[must_use = "returns a new scoped Ctx; the original is unchanged"]
    pub fn with_subpath<K: IntoStableKey>(&self, key: K) -> Self {
        let mut scoped = self.clone();
        scoped.subpath = self
            .subpath
            .iter()
            .cloned()
            .chain(std::iter::once(key.into_stable_key()))
            .collect();
        scoped
    }

    /// Return a clone of this `Ctx` that routes the failures of components
    /// mounted from it in the background ([`Ctx::mount`], [`Ctx::mount_live`],
    /// `auto_refresh`, …) through `handler` first, then through the handlers
    /// already in scope, outermost last. The Rust analogue of Python's `async
    /// with coco.exception_handler(handler):` block.
    ///
    /// The handler returns `Ok(())` to swallow a failure or `Err(_)` to pass
    /// it (possibly rewritten) to the next outer handler; when every handler
    /// returns `Err`, the last error reaches the mount's
    /// [`ComponentMountHandle::ready`]. With no handler at all the failure is
    /// logged and dropped. See [`crate::ExceptionHandler`].
    #[must_use = "returns a new scoped Ctx; the original is unchanged"]
    pub fn with_exception_handler<H>(&self, handler: H) -> Self
    where
        H: Fn(&Error, &ExceptionContext) -> Result<()> + Send + Sync + 'static,
    {
        let mut chain = (*self.handler_chain).clone();
        chain.push(Arc::new(handler));
        let mut scoped = self.clone();
        scoped.handler_chain = Arc::new(chain);
        scoped
    }

    /// Execute a non-memoized `#[cocoindex::function]` body while recording
    /// its logic and context dependencies into the surrounding function call.
    #[doc(hidden)]
    pub async fn __coco_tracked_fn<T, F, Fut>(
        &self,
        logic: &'static FnLogicEntry,
        propagate_children_fn_logic: bool,
        f: F,
    ) -> Result<T>
    where
        F: FnOnce(Ctx) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let fp = logic.fingerprint()?;
        // `propagate_children_fn_logic` is the `logic_tracking` mode lowered by
        // the `#[function]` macro: `true` for `"full"` (default — a transitively
        // called function's logic change also invalidates this one), `false` for
        // `"self"` (only this function's own body is tracked).
        let fn_ctx = Arc::new(FnCallContext::new(propagate_children_fn_logic));
        fn_ctx.add_fn_logic_dep(fp);
        let _frame = self.frame_guard(fn_ctx.clone());
        f(self.with_fn_ctx(fn_ctx)).await
    }

    /// Guard that joins `fn_ctx` into this `Ctx`'s enclosing frame when it
    /// drops: the surrounding function call if there is one, else the
    /// component. Joining into the enclosing frame (not straight into the
    /// component) is what lets a memoized caller inherit everything its body
    /// observed — context deps, nested logic, target states, memo entries and
    /// child mounts — matching the Python SDK's `parent.join_child(fn_ctx)`.
    pub(crate) fn frame_guard(&self, fn_ctx: Arc<FnCallContext>) -> FnFrameGuard {
        FnFrameGuard {
            comp_ctx: self.comp_ctx.clone(),
            parent_fn_ctx: self.fn_ctx.clone(),
            fn_ctx,
            tunnel_owner: None,
        }
    }

    /// Like [`frame_guard`](Self::frame_guard) for a propagated callback's
    /// collector frame: what it gathered is addressed to `owner` when it
    /// joins the enclosing function frame (see
    /// `FnCallContext::join_tunneled_child`). At the component boundary it is
    /// flattened into the component's logic set like any other frame.
    pub(crate) fn tunnel_guard(
        &self,
        collector: Arc<FnCallContext>,
        owner: FnFrameId,
    ) -> FnFrameGuard {
        FnFrameGuard {
            comp_ctx: self.comp_ctx.clone(),
            parent_fn_ctx: self.fn_ctx.clone(),
            fn_ctx: collector,
            tunnel_owner: Some(owner),
        }
    }

    /// The frame the current call records into: the enclosing function frame
    /// if there is one, else a frame joined straight into the component.
    /// `None` outside a pipeline context.
    fn record_into_frame(&self, record: impl FnOnce(&FnCallContext)) -> Option<()> {
        if let Some(fn_ctx) = &self.fn_ctx {
            record(fn_ctx);
        } else {
            let frame = FnCallContext::default();
            record(&frame);
            self.comp_ctx.as_ref()?.join_fn_call(&frame);
        }
        Some(())
    }

    /// Record `dep` as part of the current call's logic.
    ///
    /// The dependency is added to the current function frame beside the
    /// function's own fingerprint and travels exactly as that fingerprint
    /// does: into the entry of a memoized caller, onward through `"full"`
    /// callers, hidden by a `"self"` caller above unless the call is carried by
    /// a [`PropagatedCallback`], and from a component into every ancestor
    /// component's entry. It is recorded whatever the current function's own
    /// `logic_tracking` is: it describes the call, not the function's code.
    ///
    /// Call it as many times as the call has dependencies; recording the same
    /// dependency twice records it once.
    ///
    /// Adopting `use_logic_dep` in a function that memoized callers already
    /// stored entries for does not re-store those entries: ship the adoption
    /// with a `version` bump on those callers.
    ///
    /// # Errors
    ///
    /// Returns an error outside a pipeline context.
    pub fn use_logic_dep(&self, dep: &LogicDep) -> Result<()> {
        let fp = dep.fingerprint();
        self.record_into_frame(|frame| frame.add_fn_logic_dep(fp))
            .ok_or_else(|| Error::engine("use_logic_dep requires an active pipeline context"))
    }

    /// Record one data dependency into the current call.
    ///
    /// **Experimental:** the API and its semantics may change in a later
    /// release.
    ///
    /// `key` identifies what was read; `state` is what was observed. Both are
    /// serialized now and stored with the memo entry; the next probe hands
    /// them to the kind's validator. Use plain values: the resolved path, a
    /// tuple of mtime and digest.
    ///
    /// Recording the same kind and key again with an equal state records once.
    /// Recording it with a different state makes the current run store no
    /// entry: output that used two states of one key has no single state to
    /// validate against, so the function or component re-runs next time. A
    /// body that writes what it then reads back should not record the
    /// read-back.
    ///
    /// The record rides up through every enclosing function frame regardless
    /// of `logic_tracking`, into every enclosing memoized function's entry and
    /// the component's entry, where it stops.
    ///
    /// # Errors
    ///
    /// Returns an error outside a pipeline context, when the current frame has
    /// already closed (a task recording after the function that started it
    /// returned), or when `key` or `state` cannot be serialized.
    pub fn use_data_dep<K: Serialize, S: Serialize>(
        &self,
        kind: &DataDep<K, S>,
        key: &K,
        state: &S,
    ) -> Result<()> {
        if self
            .fn_ctx
            .as_ref()
            .is_some_and(|fn_ctx| fn_ctx.is_closed())
        {
            return Err(Error::engine(format!(
                "use_data_dep({kind:?}) called after the function that owns the current frame returned: a task that outlives its frame cannot record into it. Await the reader inside the function that started it."
            )));
        }
        let record = crate::deps::data_dep_record(kind.fingerprint(), key, state)?;
        self.record_into_frame(|frame| frame.add_data_dep(record))
            .ok_or_else(|| Error::engine("use_data_dep requires an active pipeline context"))
    }

    /// Return `f` as a callback whose logic is propagated to its callers' memo
    /// entries, through `logic_tracking = "self"` / `"none"` layers.
    ///
    /// Call this in the function that hands the callback down (the
    /// **owner**): typically the library entry point that receives it. The
    /// returned wrapper runs `f` as usual; afterwards the logic `f` offered
    /// (its own fingerprint and its callees', per the `#[cocoindex::function]`
    /// it calls) is recorded in **every** memo entry between the call site and
    /// the owner's frame, regardless of their `logic_tracking`. The owner's own
    /// entry records it too; the owner's mode only decides whether it is
    /// offered to its callers (`"full"`) or stops there (`"self"` / `"none"`).
    ///
    /// The wrapper is valid only while the owner's call is running: calling it
    /// after the owner returned is an error. Adopting the wrapper later does
    /// not re-store existing memo entries.
    ///
    /// ```no_run
    /// # use std::future::Future;
    /// # use cocoindex::{Ctx, PropagatedCallback, Result};
    /// #[cocoindex::function]
    /// async fn visit_file(_ctx: &Ctx, file: String) -> Result<String> {
    ///     Ok(file.to_uppercase())
    /// }
    ///
    /// // A library layer that calls a callback it received.
    /// async fn accept<F, Fut>(ctx: &Ctx, file: String, visit: &PropagatedCallback<F>) -> Result<String>
    /// where
    ///     F: Fn(Ctx, String) -> Fut,
    ///     Fut: Future<Output = Result<String>>,
    /// {
    ///     visit.call(ctx, file).await
    /// }
    ///
    /// #[cocoindex::function]
    /// async fn walk(ctx: &Ctx, files: Vec<String>) -> Result<()> {
    ///     // `walk` owns the callback.
    ///     let visit = ctx.logic_propagated_to_callers(|ctx: Ctx, file: String| async move {
    ///         visit_file(&ctx, file).await
    ///     })?;
    ///     for file in files {
    ///         accept(ctx, file, &visit).await?;
    ///     }
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error outside a pipeline context.
    pub fn logic_propagated_to_callers<F>(&self, f: F) -> Result<PropagatedCallback<F>> {
        if self.comp_ctx.is_none() {
            return Err(Error::engine(
                "logic_propagated_to_callers requires an active pipeline context",
            ));
        }
        // With no enclosing function frame the owner is the component itself:
        // a frame no call site ever resolves, so what the callback offers
        // rides to the component boundary and is recorded there.
        let owner = self
            .fn_ctx
            .clone()
            .unwrap_or_else(|| Arc::new(FnCallContext::default()));
        Ok(PropagatedCallback::new(f, owner))
    }
}

/// The logic fingerprint of one `#[cocoindex::function]`, computed once per
/// function by [`FnLogicEntry::fingerprint`]. `deps_fp` is the fingerprint of
/// the function's `deps = ...` value, folded in so a change to it moves the
/// function's logic exactly as a body edit does.
pub(crate) fn fn_logic_fingerprint(
    module_path: &str,
    fn_name: &str,
    code_hash: u64,
    deps_fp: Option<Fingerprint>,
) -> Result<Fingerprint> {
    let fp = match deps_fp {
        None => Fingerprint::from(&("cocoindex_fn", module_path, fn_name, code_hash))?,
        Some(deps_fp) => {
            Fingerprint::from(&("cocoindex_fn", module_path, fn_name, code_hash, deps_fp))?
        }
    };
    Ok(fp)
}

/// Joins a function-call frame into its enclosing frame on drop, so the join
/// also happens on error and cancellation paths. See [`Ctx::frame_guard`].
pub(crate) struct FnFrameGuard {
    comp_ctx: Option<ComponentProcessorContext<RustProfile>>,
    parent_fn_ctx: Option<Arc<FnCallContext>>,
    fn_ctx: Arc<FnCallContext>,
    /// Set for a propagated callback's collector frame: the owner its logic
    /// is addressed to. See [`Ctx::tunnel_guard`].
    tunnel_owner: Option<FnFrameId>,
}

impl Drop for FnFrameGuard {
    fn drop(&mut self) {
        if let Some(parent) = &self.parent_fn_ctx {
            match self.tunnel_owner {
                Some(owner) => parent.join_tunneled_child(&self.fn_ctx, owner),
                None => parent.join_child(&self.fn_ctx),
            }
        } else if let Some(comp_ctx) = &self.comp_ctx {
            comp_ctx.join_fn_call(&self.fn_ctx);
        }
    }
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
    /// Return a cloned context with an additional timeout applied.
    ///
    /// If the current context already has an earlier deadline, the earlier one
    /// wins. The returned context should be passed to child scopes and helper
    /// calls whose work belongs to the narrower budget.
    #[must_use = "returns a new scoped Ctx; the original is unchanged"]
    pub fn with_timeout(&self, timeout: Duration) -> Self {
        let mut scoped = self.clone();
        scoped.deadline = scoped.deadline.with_timeout(timeout);
        scoped
    }

    /// Run a closure with a cloned context carrying an additional timeout.
    ///
    /// This is the least error-prone spelling for scoped work: name the closure
    /// argument `ctx` and it shadows the outer context, so calls inside the body
    /// naturally use the scoped deadline.
    pub async fn with_timeout_scope<F, Fut, T>(&self, timeout: Duration, f: F) -> T
    where
        F: FnOnce(Self) -> Fut,
        Fut: Future<Output = T>,
    {
        let scoped = self.with_timeout(timeout);
        f(scoped).await
    }

    /// Check the current deadline and return [`Error::DeadlineExceeded`] if it
    /// has expired.
    pub fn check_cancellation(&self) -> Result<()> {
        self.deadline.check().map_err(Error::from)
    }

    /// Return true when this context carries an active deadline.
    pub fn has_deadline(&self) -> bool {
        self.deadline.has_deadline()
    }

    /// Return the remaining budget, or `None` when no deadline is active.
    pub fn remaining_deadline(&self) -> Option<Duration> {
        self.deadline.remaining()
    }

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
            }
            if let Some(comp_ctx) = &self.comp_ctx {
                let component_dep = FnCallContext::default();
                component_dep.add_context_change_dep(fp);
                comp_ctx.join_fn_call(&component_dep);
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
        comp_ctx
            .app_ctx()
            .next_id(None, self.deadline)
            .await
            .map_err(Error::from)
    }

    /// Declare a persistent state for the current component.
    ///
    /// On the first run the returned handle holds `initial_value`; on later runs
    /// it holds the value persisted at the end of the previous run. Assign a new
    /// value via [`StateHandle::set`] to persist it for next time. The value is
    /// owned by the component's stable path, so it survives across runs and is
    /// garbage-collected if the component disappears.
    ///
    /// `key` must be declared at most once per component run (a duplicate is an
    /// error). It accepts a string or a [`StableKey`].
    pub fn use_state<K: IntoStateKey, T: Serialize + DeserializeOwned>(
        &self,
        key: K,
        initial_value: T,
    ) -> Result<StateHandle<T>> {
        let Some(comp_ctx) = &self.comp_ctx else {
            return Err(Error::engine(
                "use_state requires an active pipeline context",
            ));
        };
        if !self.subpath.is_empty() {
            return Err(Error::engine(
                "use_state cannot be called on a `Ctx::with_subpath` context: the state is \
                 owned by the component, not by the subpath",
            ));
        }
        let key = key.into_state_key();
        let stored = comp_ctx
            .use_state(key.clone(), Value::from_serializable(&initial_value)?)
            .map_err(Error::from)?;
        let value = stored.deserialize()?;
        Ok(StateHandle::new(key, value, comp_ctx.clone()))
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

    pub(crate) fn static_root_target_provider(
        &self,
        name: &str,
    ) -> Result<TargetStateProvider<RustProfile>> {
        self.state
            .static_root_providers
            .get(name)
            .cloned()
            .ok_or_else(|| {
                Error::engine(format!(
                    "static root target provider `{name}` is not registered"
                ))
            })
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
            deadline: self.deadline,
            handler_chain: self.handler_chain.clone(),
            subpath: self.subpath.clone(),
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
    pub async fn auto_refresh<K, F, Fut>(&self, key: K, interval: Duration, f: F) -> Result<()>
    where
        K: IntoStableKey,
        F: Fn(Ctx) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let Some(comp_ctx) = &self.comp_ctx else {
            return f(self.clone()).await;
        };

        let key = key.into_stable_key();
        let key_str = key_label(&key);
        let child_path = self.child_path(comp_ctx, key);
        let stable_path = child_path.to_string();
        let fn_ctx = Arc::new(FnCallContext::default());
        let Some(pending) = mount_live_prepare(comp_ctx, &fn_ctx, child_path, comp_ctx.live())
            .map_err(|e| Error::engine(format!("{e}")))?
        else {
            // The update's component selector leaves this component out.
            return Ok(());
        };
        let _frame = self.frame_guard(fn_ctx);
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
                    let processor = auto_refresh_processor(
                        state.clone(),
                        handler_chain.clone(),
                        f.clone(),
                        processor_name.clone(),
                    );
                    let on_error = build_chained_on_error(
                        &handler_chain,
                        ExceptionContext {
                            env_name: env_name.clone(),
                            stable_path: stable_path.clone(),
                            parent_stable_path: None,
                            processor_name: Some(processor_name.clone()),
                            mount_kind: MountKind::UpdateFull,
                            is_background: ready_marked && controller.is_live(),
                            source: ExceptionSource::Component,
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
    ///
    /// A failure in the component's processing is logged, not returned; use
    /// [`Ctx::mount_live_with_handler`] to observe or propagate it.
    pub async fn mount_live<K, C>(&self, key: K, component: C) -> Result<()>
    where
        K: IntoStableKey,
        C: LiveComponent,
    {
        self.mount_live_impl(key.into_stable_key(), Arc::new(component), None)
            .await
    }

    /// Like [`Ctx::mount_live`], but routes background failures (full-pass and
    /// incremental update/delete) through `handler`. The handler returns
    /// `Ok(())` to swallow a failure or `Err(_)` to propagate it.
    pub async fn mount_live_with_handler<K, C, H>(
        &self,
        key: K,
        component: C,
        handler: H,
    ) -> Result<()>
    where
        K: IntoStableKey,
        C: LiveComponent,
        H: Fn(&Error, &ExceptionContext) -> Result<()> + Send + Sync + 'static,
    {
        let handler: ExceptionHandler = Arc::new(handler);
        self.mount_live_impl(key.into_stable_key(), Arc::new(component), Some(handler))
            .await
    }

    /// Mount one child component per item from a live change feed, keyed by the
    /// feed's key. In catch-up mode the feed is scanned once; in live mode the
    /// feed streams incremental adds/removes that mount/delete children
    /// individually. The analogue of [`Ctx::mount_each`] for live sources.
    pub async fn mount_each_live<Key, K, V, Feed, F, Fut>(
        &self,
        key: Key,
        feed: Feed,
        process_fn: F,
    ) -> Result<()>
    where
        Key: IntoStableKey,
        K: std::fmt::Display + Send + Sync + 'static,
        V: Send + Sync + 'static,
        Feed: LiveMapView<K, V>,
        F: Fn(Ctx, V) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let component = MountEachLiveComponent::<K, V, Feed>::new(feed, process_fn);
        self.mount_live(key, component).await
    }

    /// Mount one [`LiveComponent`] *per item* — each item builds its own live
    /// component instance. This is the Rust analogue of passing a `LiveComponent`
    /// to Python's `mount_each`: where [`Ctx::mount_each`] runs a closure per item
    /// and [`Ctx::mount_each_live`] fans one feed out into closure-driven
    /// children, this gives each item a full `LiveComponent` (with its own
    /// `process_live`).
    ///
    /// `key_fn` derives each child's scope key from the item; `make_component`
    /// builds the component from the item. Components are mounted concurrently;
    /// duplicate keys are an error. In live mode each component keeps reacting to
    /// its source in the background until the app is dropped.
    pub async fn mount_each_live_component<I, K, F, C>(
        &self,
        items: I,
        key_fn: impl Fn(&I::Item) -> K,
        make_component: F,
    ) -> Result<()>
    where
        I: IntoIterator,
        K: IntoStableKey,
        F: Fn(I::Item) -> C,
        C: LiveComponent,
    {
        let mut keys = FxHashSet::default();
        let mut components = Vec::new();
        for item in items {
            let key = key_fn(&item).into_stable_key();
            if !keys.insert(key.clone()) {
                return Err(Error::engine(format!(
                    "duplicate key `{}` in mount_each_live_component batch",
                    key_label(&key)
                )));
            }
            components.push((key, make_component(item)));
        }
        let futs: Vec<_> = components
            .into_iter()
            .map(|(key, component)| async move { self.mount_live(key, component).await })
            .collect();
        futures::future::try_join_all(futs).await?;
        Ok(())
    }

    async fn mount_live_impl(
        &self,
        key: StableKey,
        instance: Arc<dyn LiveComponent>,
        handler: Option<ExceptionHandler>,
    ) -> Result<()> {
        let Some(comp_ctx) = &self.comp_ctx else {
            // No pipeline context — run a single full pass directly, matching
            // `auto_refresh`'s standalone behavior.
            return instance.process(self.clone()).await;
        };

        let key_str = key_label(&key);
        let child_path = self.child_path(comp_ctx, key);
        let fn_ctx = Arc::new(FnCallContext::default());
        let Some(pending) =
            mount_live_prepare(comp_ctx, &fn_ctx, child_path.clone(), comp_ctx.live())
                .map_err(|e| Error::engine(format!("{e}")))?
        else {
            // The update's component selector leaves this component out.
            return Ok(());
        };
        let _frame = self.frame_guard(fn_ctx);
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

    /// Mount a child component under `key` in the background and return a
    /// handle to wait for it. The Rust analogue of Python's `coco.mount`.
    ///
    /// Unlike [`Ctx::scope`] / `use_mount!`, the parent does not wait for the
    /// child and takes no value from it: the child runs on its own task, its
    /// target states sync when it finishes, and in live mode it can refresh
    /// independently. A failure in the child is off the parent's critical
    /// path: it is logged, or routed through the exception handlers in scope
    /// ([`Ctx::with_exception_handler`],
    /// [`EnvironmentBuilder::exception_handler`](crate::EnvironmentBuilder::exception_handler)),
    /// and reaches the caller only through [`ComponentMountHandle::ready`] when a
    /// handler returns `Err`. The parent's own completion still waits for every
    /// child it mounted.
    ///
    /// `key` is any [`IntoStableKey`]: a string, an integer, a UUID, bytes, a
    /// [`StableKey`], or a reference to one.
    ///
    /// Prefer the `mount!` macro for a `#[cocoindex::function]` call: it adds
    /// the function's logic and arguments as the component-memo fingerprint, so
    /// an unchanged child is skipped. This method always runs `f`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::ctx::Ctx;
    /// # async fn doc(ctx: &Ctx) -> cocoindex::error::Result<()> {
    /// let handle = ctx.mount("child", |child_ctx| async move {
    ///     // declare target states ...
    ///     Ok(())
    /// }).await?;
    /// handle.ready().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn mount<K, F, Fut>(&self, key: K, f: F) -> Result<ComponentMountHandle>
    where
        K: IntoStableKey,
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.__mount_fp(key.into_stable_key(), None, f).await
    }

    /// Background mount with an explicit component-memo fingerprint. The
    /// `mount!` macro calls this; [`Ctx::mount`] is the `memo = None`
    /// (always-run) case.
    #[doc(hidden)]
    pub async fn __mount_fp<F, Fut>(
        &self,
        key: StableKey,
        memo: Option<crate::mount::ComponentMemo>,
        f: F,
    ) -> Result<ComponentMountHandle>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.check_cancellation()?;
        let Some(comp_ctx) = &self.comp_ctx else {
            // No pipeline context — run the closure directly; the handle
            // reports its outcome.
            let outcome = f(self.child(None)).await;
            return Ok(ComponentMountHandle {
                inner: MountHandleInner::Immediate(outcome),
                deadline: self.deadline,
            });
        };
        let processor_name = format!("mount:{}", key_label(&key));
        let handle = self
            .mount_background(comp_ctx, key, processor_name, memo, f)
            .await?;
        Ok(ComponentMountHandle {
            inner: MountHandleInner::Core(vec![handle]),
            deadline: self.deadline,
        })
    }

    /// Mount `f` under `key` on its own task, reported as `processor_name`,
    /// and return the engine handle to wait for it. The one background mount
    /// path: [`Ctx::mount`] / `mount!` and every item of [`Ctx::mount_each`]
    /// / `mount_each!` go through it.
    ///
    /// Admission is bounded by the app's `max_inflight_components`: when the
    /// pool is full this waits before spawning, so a parent streaming items
    /// into `mount_each` runs ahead of its children only as far as the pool
    /// allows, as in Python.
    async fn mount_background<F, Fut>(
        &self,
        comp_ctx: &ComponentProcessorContext<RustProfile>,
        key: StableKey,
        processor_name: String,
        memo: Option<crate::mount::ComponentMemo>,
        f: F,
    ) -> Result<ComponentExecutionHandle>
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let child_path = self.child_path(comp_ctx, key);
        let fn_ctx = Arc::new(FnCallContext::default());
        let child_component = comp_ctx
            .component()
            .mount_child(&fn_ctx, child_path.clone())
            .map_err(Error::from)?;
        // See `use_mount_named`: join the mount frame into the enclosing frame
        // whatever happens below.
        let _frame = self.frame_guard(fn_ctx);

        let state = self.state.clone();
        let has_context_memo_state = state.context.has_memo_state_fns();
        let handler_chain = self.handler_chain.clone();
        let processor = BoxedProcessor::new(
            {
                let handler_chain = handler_chain.clone();
                move |child_comp_ctx| {
                    // A background child is deadline-isolated from its parent,
                    // as in Python: the parent's budget bounds the parent's own
                    // wait on `ready()`, not the child's work.
                    let ctx = Ctx::new_with_handlers(
                        Some(child_comp_ctx),
                        state.clone(),
                        handler_chain.clone(),
                        DeadlineContext::NONE,
                    );
                    // The body returns `()`: no value is serialized or stored.
                    Box::pin(async move {
                        f(ctx).await?;
                        Ok(Value::unit())
                    })
                }
            },
            memo.as_ref().map(|memo| memo.fp),
            processor_name.clone(),
        )
        .with_context_memo_state(has_context_memo_state)
        .with_state_inputs(memo.map(|memo| memo.state_inputs).unwrap_or_default());

        let on_error = build_chained_on_error(
            &handler_chain,
            ExceptionContext {
                env_name: self.state.name.clone(),
                stable_path: child_path.to_string(),
                parent_stable_path: Some(self.mount_base_path(comp_ctx).to_string()),
                processor_name: Some(processor_name),
                mount_kind: MountKind::Mount,
                is_background: true,
                source: ExceptionSource::Component,
            },
        );
        child_component
            .mount(comp_ctx, processor, on_error)
            .await
            .map_err(Error::from)
    }

    /// Named sub-component. Creates a child scope in the pipeline tree.
    ///
    /// The key determines the child's stable path for memoization and
    /// target state tracking; it is any [`IntoStableKey`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::ctx::Ctx;
    /// # async fn doc(ctx: &Ctx) -> cocoindex::error::Result<()> {
    /// let val = ctx.scope("child", |child_ctx| async move {
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
    pub async fn scope<K, T, F, Fut>(&self, key: K, f: F) -> Result<T>
    where
        K: IntoStableKey,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.__use_mount_fp(key.into_stable_key(), None, f).await
    }

    /// Foreground mount with an explicit component-memo fingerprint. The
    /// `use_mount!` macro calls this; `scope` is the `memo = None`
    /// (always-run) case kept for direct use and tests.
    ///
    /// When `memo` is `Some`, the engine checks its fingerprint and argument states before running the child
    /// and skips the whole component on an unchanged hit (see
    /// [`crate::mount::component_memo`]).
    #[doc(hidden)]
    pub async fn __use_mount_fp<T, F, Fut>(
        &self,
        key: StableKey,
        memo: Option<crate::mount::ComponentMemo>,
        f: F,
    ) -> Result<T>
    where
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let processor_name = format!("mount:{}", key_label(&key));
        self.use_mount_named(key, processor_name, memo, f).await
    }

    /// Foreground mount under `key`, reported (stats, `show -l`) as
    /// `processor_name`. Items of one `use_mount_each` share a name, the way
    /// Python names a processor after its function, so per-item mounts do not
    /// produce one stats line per item.
    async fn use_mount_named<T, F, Fut>(
        &self,
        key: StableKey,
        processor_name: String,
        memo: Option<crate::mount::ComponentMemo>,
        f: F,
    ) -> Result<T>
    where
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let Some(comp_ctx) = &self.comp_ctx else {
            // No pipeline context — just run the closure directly.
            let child_ctx = self.child(None);
            return f(child_ctx).await;
        };

        let child_path = self.child_path(comp_ctx, key);

        let fn_ctx = Arc::new(FnCallContext::default());
        let child_component = comp_ctx
            .component()
            .mount_child(&fn_ctx, child_path)
            .map_err(|e| Error::engine(format!("{e}")))?;

        // Join the mount frame into the enclosing frame even if `f` panics or the
        // future is dropped/cancelled early. Inside a memoized function this
        // carries `has_child_components` to the memo frame, so the engine rejects
        // the memo entry instead of letting a later hit delete the child's
        // target states.
        let _frame = self.frame_guard(fn_ctx.clone());

        let state = self.state.clone();
        let has_context_memo_state = state.context.has_memo_state_fns();
        let scope_handler_chain = self.handler_chain.clone();
        // Foreground child inherits the caller's current scoped deadline —
        // the same value passed to core use_mount below.
        let child_deadline = self.deadline;
        let processor = BoxedProcessor::new(
            move |child_comp_ctx| {
                let deadline = child_deadline;
                // The child component is its own root: frames opened in its
                // body join the child component (like the app root), never
                // the parent's mount frame. The child's dependencies reach the
                // parent through the engine's component run outcome.
                let ctx = Ctx::new_with_handlers(
                    Some(child_comp_ctx),
                    state.clone(),
                    scope_handler_chain.clone(),
                    deadline,
                );
                Box::pin(async move {
                    let result = f(ctx).await?;
                    Value::from_serializable(&result)
                })
            },
            memo.as_ref().map(|memo| memo.fp),
            processor_name,
        )
        .with_context_memo_state(has_context_memo_state)
        .with_state_inputs(memo.map(|memo| memo.state_inputs).unwrap_or_default());

        let handle = child_component
            .use_mount(comp_ctx, processor, self.deadline)
            .await
            .map_err(Error::from)?;
        let value = handle.result(Some(comp_ctx)).await.map_err(Error::from)?;
        value
            .deserialize()
            .map_err(|err| Error::engine(format!("{err}")))
    }

    /// Mount one background child component per item, keyed by `key_fn`, and
    /// return one handle for all of them. The Rust analogue of Python's
    /// `coco.mount_each` for a plain iterable: `items` is consumed as the
    /// children are mounted, each child runs on its own task under the app's
    /// `max_inflight_components` limit, and the parent neither waits for them
    /// nor takes a value from them (see [`Ctx::mount`] for how a child's
    /// failure is reported). For items whose values the caller needs, use
    /// [`Ctx::use_mount_each`]; for an async source, [`Ctx::mount_each_stream`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::ctx::Ctx;
    /// # async fn doc(ctx: &Ctx, tasks: Vec<String>) -> cocoindex::error::Result<()> {
    /// let handle = ctx.mount_each(
    ///     tasks,
    ///     |task| task.clone(), // use the task string as the child key
    ///     |child_ctx, task| async move {
    ///         // declare target states ...
    ///         Ok(())
    ///     }
    /// ).await?;
    /// handle.ready().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// All items are reported under one processor name, `mount_each`, in
    /// update stats and `cocoindex show -l`.
    ///
    /// # Errors
    ///
    /// Returns an error when `key_fn` yields the same key twice, or when a
    /// child cannot be mounted. A child's own failure is not an error here.
    pub async fn mount_each<I, K, F, Fut>(
        &self,
        items: I,
        key_fn: impl Fn(&I::Item) -> K,
        f: F,
    ) -> Result<ComponentMountHandle>
    where
        I: IntoIterator,
        I::Item: Send + 'static,
        K: IntoStableKey,
        F: Fn(Ctx, I::Item) -> Fut + Send + Clone + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let keyed = items
            .into_iter()
            .map(|item| (key_fn(&item).into_stable_key(), item));
        self.mount_each_keyed(
            futures::stream::iter(keyed),
            None,
            "mount_each".to_string(),
            |_| Ok(None),
            f,
        )
        .await
    }

    /// Like [`Ctx::mount_each`] for an async source of `(key, item)` pairs: a
    /// child is mounted as each pair arrives, so the source is read at the
    /// pace the children are admitted. The analogue of passing an async
    /// iterable to Python's `coco.mount_each`.
    pub async fn mount_each_stream<S, K, V, F, Fut>(
        &self,
        items: S,
        f: F,
    ) -> Result<ComponentMountHandle>
    where
        S: Stream<Item = (K, V)>,
        K: IntoStableKey,
        V: Send + 'static,
        F: Fn(Ctx, V) -> Fut + Send + Clone + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.mount_each_keyed(
            items.map(|(key, value)| (key.into_stable_key(), value)),
            None,
            "mount_each".to_string(),
            |_| Ok(None),
            f,
        )
        .await
    }

    /// Per-item background mounts with component-memo fingerprints, the
    /// runtime behind `mount_each!`. Each `(key, value)` pair becomes a child
    /// component at `prefix/key` (or `key` when `prefix` is `None`); `memo_of`
    /// computes the value's component-memo fingerprint and `body` runs the
    /// entry function under the child scope.
    #[doc(hidden)]
    pub async fn __mount_each_fp<K, V, M, B, Fut>(
        &self,
        items: impl IntoIterator<Item = (K, V)>,
        prefix: Option<&str>,
        memo_of: M,
        body: B,
    ) -> Result<ComponentMountHandle>
    where
        K: IntoStableKey,
        V: Send + 'static,
        M: Fn(&V) -> Result<Option<crate::mount::ComponentMemo>>,
        B: Fn(Ctx, V) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let processor_name = format!("mount:{}", prefix.unwrap_or("mount_each"));
        let keyed = items
            .into_iter()
            .map(|(key, value)| (key.into_stable_key(), value));
        self.mount_each_keyed(
            futures::stream::iter(keyed),
            prefix,
            processor_name,
            memo_of,
            body,
        )
        .await
    }

    /// The one `mount_each` path: stream `(key, value)` pairs, mount each in
    /// the background under `prefix` (a child path part, as Python's
    /// `mount_each` subpath) and gather the handles. Duplicate keys are an
    /// error, as in Python.
    async fn mount_each_keyed<S, V, M, B, Fut>(
        &self,
        items: S,
        prefix: Option<&str>,
        processor_name: String,
        memo_of: M,
        body: B,
    ) -> Result<ComponentMountHandle>
    where
        S: Stream<Item = (StableKey, V)>,
        V: Send + 'static,
        M: Fn(&V) -> Result<Option<crate::mount::ComponentMemo>>,
        B: Fn(Ctx, V) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.check_cancellation()?;
        let parent = match prefix {
            Some(prefix) => self.with_subpath(prefix),
            None => self.clone(),
        };
        let mut keys = FxHashSet::default();
        let mut items = std::pin::pin!(items);
        let Some(comp_ctx) = &parent.comp_ctx else {
            // No pipeline context — run the bodies one by one; the handle
            // reports the first failure.
            let mut outcome = Ok(());
            while let Some((key, value)) = items.next().await {
                check_unique_key(&mut keys, key)?;
                if let Err(err) = body(parent.child(None), value).await
                    && outcome.is_ok()
                {
                    outcome = Err(err);
                }
            }
            return Ok(ComponentMountHandle {
                inner: MountHandleInner::Immediate(outcome),
                deadline: self.deadline,
            });
        };
        let mut handles = Vec::new();
        while let Some((key, value)) = items.next().await {
            parent.check_cancellation()?;
            check_unique_key(&mut keys, key.clone())?;
            let memo = memo_of(&value)?;
            let body = body.clone();
            let handle = parent
                .mount_background(comp_ctx, key, processor_name.clone(), memo, move |child| {
                    body(child, value)
                })
                .await?;
            handles.push(handle);
        }
        Ok(ComponentMountHandle {
            inner: MountHandleInner::Core(handles),
            deadline: self.deadline,
        })
    }

    /// Run a closure for each item in its own child scope, in the foreground,
    /// and return the values in input order: the per-item form of
    /// [`Ctx::scope`] / `use_mount!`, where [`Ctx::mount_each`] is the
    /// per-item form of [`Ctx::mount`].
    ///
    /// Each item gets its own `Ctx` child scope keyed by `key_fn(item)`. All
    /// closures run concurrently; every started item runs to completion, then
    /// the first error in input order is returned.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cocoindex::ctx::Ctx;
    /// # async fn doc(ctx: &Ctx, tasks: Vec<String>) -> cocoindex::error::Result<()> {
    /// let results = ctx.use_mount_each(
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
    /// All items are reported under one processor name, `use_mount_each`, in
    /// update stats and `cocoindex show -l`.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the closures fail. The first encountered error
    /// is returned.
    pub async fn use_mount_each<I, K, F, Fut, T>(
        &self,
        items: I,
        key_fn: impl Fn(&I::Item) -> K,
        f: F,
    ) -> Result<Vec<T>>
    where
        I: IntoIterator,
        I::Item: Send + 'static,
        K: IntoStableKey,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
        F: Fn(Ctx, I::Item) -> Fut + Send + Clone + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let keyed = items
            .into_iter()
            .map(|item| (key_fn(&item).into_stable_key(), item));
        self.use_mount_each_keyed(keyed, None, "use_mount_each".to_string(), |_| Ok(None), f)
            .await
    }

    /// Per-item foreground mounts with component-memo fingerprints, the
    /// runtime behind `use_mount_each!`: [`Ctx::__mount_each_fp`] with the
    /// values returned in input order.
    #[doc(hidden)]
    pub async fn __use_mount_each_fp<K, V, M, B, Fut, T>(
        &self,
        items: impl IntoIterator<Item = (K, V)>,
        prefix: Option<&str>,
        memo_of: M,
        body: B,
    ) -> Result<Vec<T>>
    where
        K: IntoStableKey,
        V: Send + 'static,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
        M: Fn(&V) -> Result<Option<crate::mount::ComponentMemo>>,
        B: Fn(Ctx, V) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let processor_name = format!("mount:{}", prefix.unwrap_or("use_mount_each"));
        let keyed = items
            .into_iter()
            .map(|(key, value)| (key.into_stable_key(), value));
        self.use_mount_each_keyed(keyed, prefix, processor_name, memo_of, body)
            .await
    }

    async fn use_mount_each_keyed<V, M, B, Fut, T>(
        &self,
        items: impl IntoIterator<Item = (StableKey, V)>,
        prefix: Option<&str>,
        processor_name: String,
        memo_of: M,
        body: B,
    ) -> Result<Vec<T>>
    where
        V: Send + 'static,
        T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
        M: Fn(&V) -> Result<Option<crate::mount::ComponentMemo>>,
        B: Fn(Ctx, V) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let parent = match prefix {
            Some(prefix) => self.with_subpath(prefix),
            None => self.clone(),
        };
        let mut keys = FxHashSet::default();
        let mut futs = Vec::new();
        for (key, value) in items {
            check_unique_key(&mut keys, key.clone())?;
            let memo = memo_of(&value)?;
            let body = body.clone();
            let parent = &parent;
            let processor_name = processor_name.clone();
            futs.push(async move {
                parent
                    .use_mount_named(key, processor_name, memo, move |child| body(child, value))
                    .await
            });
        }
        let outcomes = futures::future::join_all(futs).await;
        outcomes.into_iter().collect()
    }

    /// Cached computation. If `key` hasn't changed since the last run,
    /// returns the cached result from LMDB without executing `f`.
    ///
    /// The closure receives a `Ctx` scoped to this memo call. Use *that* `Ctx`
    /// (not a captured outer one) for `get_key` so change-detection
    /// dependencies are attributed to this memo entry — this is what keeps
    /// invalidation correct when memo bodies run concurrently.
    ///
    /// A manual memo block is **not logic-tracked**: the closure's code is not
    /// part of `key`, so editing the closure does not invalidate the entry,
    /// and the block's logic is not offered to memoized callers. Prefer
    /// `#[cocoindex::function(memo)]`, which hashes the function body into
    /// the key, registers its logic and propagates it. When a manual block
    /// must invalidate after its enclosing `#[cocoindex::function]` changes,
    /// include that function's generated `__COCO_FN_HASH_<NAME>` constant in
    /// `key`.
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
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce(Ctx) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        crate::memo::cached(self, key, f).await
    }

    /// Run a closure concurrently for each item within the current scope (no
    /// child scopes). The analogue of Python's `coco.map`: the items run on
    /// the caller's task and start in input order, as tasks of one asyncio
    /// loop do. For CPU-bound work, spawn it from the closure
    /// (`tokio::task::spawn_blocking`).
    ///
    /// Every started item runs to completion, then the first error in input
    /// order is returned.
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
        // The futures are awaited in place, so they may borrow from the caller
        // (the source text, a shared `IdGenerator`, the target): `async |x| ...`.
        Fut: Future<Output = Result<T>> + Send,
    {
        self.check_cancellation()?;
        let deadline = self.deadline;
        let futs: Vec<_> = items
            .into_iter()
            .map(|item| async {
                // A task may start after the caller's deadline moved forward
                // while earlier tasks were being scheduled.
                deadline.check().map_err(Error::from)?;
                let result = f(item).await;
                // Do not let a value returned after the deadline look
                // successful to the caller.
                if result.is_ok() {
                    deadline.check().map_err(Error::from)?;
                }
                result
            })
            .collect();
        futures::future::join_all(futs).await.into_iter().collect()
    }
}

/// `key` as it reads in a processor name: the bare text of a string or
/// symbol key, the [`StableKey`] display form otherwise.
fn key_label(key: &StableKey) -> String {
    match key {
        StableKey::Str(text) | StableKey::Symbol(text) => text.to_string(),
        other => other.to_string(),
    }
}

/// Record `key` in `keys`, or fail: a `mount_each` batch mounts each key once.
fn check_unique_key(keys: &mut FxHashSet<StableKey>, key: StableKey) -> Result<()> {
    if keys.insert(key.clone()) {
        Ok(())
    } else {
        Err(Error::engine(format!(
            "duplicate key `{}` in mount_each batch",
            key_label(&key)
        )))
    }
}

/// Handle for a component started with [`Ctx::mount`] / `mount!`. Mirrors
/// Python's `ComponentMountHandle`.
pub struct ComponentMountHandle {
    inner: MountHandleInner,
    /// The mounting caller's deadline, checked around the wait: the child's
    /// work is not bounded by it, the caller's wait is.
    deadline: DeadlineContext,
}

enum MountHandleInner {
    /// One handle per child: a `mount` has one, a `mount_each` one per item.
    Core(Vec<ComponentExecutionHandle>),
    /// Standalone use (no pipeline): the bodies already ran.
    Immediate(Result<()>),
}

impl ComponentMountHandle {
    /// Wait until every component is ready: its body has run and its target
    /// states are synced. Returns `Err` only when the caller's deadline expires
    /// or an exception handler in scope propagated a child's failure (see
    /// [`Ctx::with_exception_handler`]); an unhandled failure is logged and
    /// resolves `Ok`, as in Python.
    pub async fn ready(self) -> Result<()> {
        self.deadline.check()?;
        match self.inner {
            MountHandleInner::Core(handles) => {
                for handle in handles {
                    handle.ready().await?;
                    // The child is ready; check the caller's own deadline
                    // before it continues or waits on the next child.
                    self.deadline.check()?;
                }
            }
            MountHandleInner::Immediate(outcome) => {
                outcome?;
                self.deadline.check()?;
            }
        }
        Ok(())
    }
}

fn auto_refresh_processor<F, Fut>(
    state: Arc<AppInner>,
    handler_chain: Arc<Vec<ExceptionHandler>>,
    f: F,
    processor_name: String,
) -> BoxedProcessor
where
    F: Fn(Ctx) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    BoxedProcessor::new(
        move |comp_ctx| {
            // Live refresh components are deadline-isolated by design. Nested
            // live components inherit the enclosing handler chain, as they do
            // from `LiveComponentOperator`.
            let ctx = Ctx::new_with_handlers(
                Some(comp_ctx),
                state.clone(),
                handler_chain.clone(),
                DeadlineContext::NONE,
            );
            Box::pin(async move {
                f(ctx).await?;
                Ok(Value::unit())
            })
        },
        None,
        processor_name,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{App, UpdateOptions};
    use cocoindex_core::engine::deadline::{
        testing_advance_deadline_clock, testing_disable_deadline_clock,
        testing_reset_deadline_clock,
    };

    static TEST_CLOCK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[derive(Serialize, crate::MemoInput)]
    struct TrackedConfig;

    crate::context_key!(static PLAIN_KEY: String);
    crate::context_key!(static NAMED_KEY: String, key = "ctx_macro_named");
    crate::context_key!(
        static DETECT_CHANGE_KEY: TrackedConfig,
        key = "ctx_macro_detect_change",
        detect_change
    );

    struct TestClockGuard {
        _guard: std::sync::MutexGuard<'static, ()>,
    }

    impl TestClockGuard {
        fn new() -> Self {
            let guard = TEST_CLOCK_LOCK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            testing_reset_deadline_clock();
            Self { _guard: guard }
        }

        fn reset(&self) {
            testing_reset_deadline_clock();
        }
    }

    impl Drop for TestClockGuard {
        fn drop(&mut self) {
            testing_disable_deadline_clock();
        }
    }

    #[test]
    fn context_key_macro_uses_identifier_by_default_and_supports_override() {
        assert_eq!(PLAIN_KEY.name(), "PLAIN_KEY");
        assert!(!PLAIN_KEY.detect_change());
        assert_eq!(NAMED_KEY.name(), "ctx_macro_named");
        assert!(!NAMED_KEY.detect_change());
        assert_eq!(DETECT_CHANGE_KEY.name(), "ctx_macro_detect_change");
        assert!(DETECT_CHANGE_KEY.detect_change());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn next_raw_id_checks_deadline_before_allocating() {
        let clock = TestClockGuard::new();
        let dir = tempfile::tempdir().unwrap();
        let app = App::builder("ctx_next_raw_id_deadline")
            .db_path(dir.path().join("lmdb"))
            .build()
            .await
            .unwrap();

        let err = app
            .update_with_options(
                UpdateOptions {
                    timeout: Some(Duration::from_secs(1)),
                    ..UpdateOptions::default()
                },
                |ctx| async move {
                    testing_advance_deadline_clock(Duration::from_secs(2));
                    let _ = ctx.next_raw_id().await?;
                    Ok(())
                },
            )
            .await
            .unwrap_err();
        assert!(err.is_deadline_exceeded());

        clock.reset();
        let first_id_after_timeout = app
            .update(|ctx| async move { ctx.next_raw_id().await })
            .await
            .unwrap();
        assert_eq!(
            first_id_after_timeout, 1,
            "expired next_id must not consume an ID allocation"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn with_timeout_scope_applies_deadline_to_scoped_ctx() {
        let _clock = TestClockGuard::new();
        let dir = tempfile::tempdir().unwrap();
        let app = App::builder("ctx_with_timeout_scope_deadline")
            .db_path(dir.path().join("lmdb"))
            .build()
            .await
            .unwrap();

        let err = app
            .update(|ctx| async move {
                ctx.with_timeout_scope(Duration::from_secs(1), |ctx| async move {
                    testing_advance_deadline_clock(Duration::from_secs(2));
                    ctx.check_cancellation()
                })
                .await
            })
            .await
            .unwrap_err();
        assert!(err.is_deadline_exceeded());
    }
}
