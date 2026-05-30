use std::collections::{BTreeMap, HashSet};

use cocoindex_utils::fingerprint::Fingerprint;

use crate::engine::component::{Component, ComponentBgChildReadiness, StatsGroup};
use crate::engine::id_sequencer::IdSequencerManager;
use crate::engine::profile::EngineProfile;
use crate::engine::stats::ProcessingStats;
use crate::engine::target_state::{TargetStateProvider, TargetStateProviderRegistry};
use crate::prelude::*;

use crate::state::stable_path::StableKey;

pub(crate) static TARGET_ID_KEY: LazyLock<StableKey> =
    LazyLock::new(|| StableKey::Symbol("cocoindex/_internal/target_id".into()));
use crate::state::stable_path_set::ChildStablePathSet;
use crate::state::target_state_path::TargetStatePath;
use crate::{
    engine::environment::{AppRegistration, Environment},
    state::stable_path::StablePath,
    state_store::AppStore,
};

use cocoindex_utils::deser::from_msgpack_slice;

use crate::engine::execution::{
    deserialize_context_memo_states, deserialize_memo_values, serialize_context_memo_states,
    serialize_memo_values,
};
use crate::engine::profile::Persist;

struct AppContextInner<Prof: EngineProfile> {
    env: Environment<Prof>,
    app_store: AppStore,
    app_reg: AppRegistration<Prof>,
    id_sequencer_manager: IdSequencerManager,
    inflight_semaphore: Option<Arc<tokio::sync::Semaphore>>,
    /// Cancellation token for in-flight app operations. Wrapped in a `Mutex` so
    /// it can be replaced with a fresh child of the global token after a
    /// previous cancellation (e.g. after `App::drop_app` finishes), allowing
    /// the same `App` instance to be reused for subsequent operations.
    cancellation_token: std::sync::Mutex<tokio_util::sync::CancellationToken>,

    /// Flat registry of all live components mounted under this app.
    /// `mount_live_async` registers (compacts then pushes) before returning;
    /// `App::drop_app` walks this at shutdown to drain each.
    /// Stores `Weak` so a freed `LiveComponentState` is naturally collected
    /// on the next compaction.
    /// Compaction-on-push: callers call `register_live_component` which removes
    /// dead entries before appending — bounding registry size by live count
    /// plus pending GC.
    live_components: parking_lot::Mutex<
        Vec<std::sync::Weak<crate::engine::live_component::LiveComponentState<Prof>>>,
    >,
}

#[derive(Clone)]
pub struct AppContext<Prof: EngineProfile> {
    inner: Arc<AppContextInner<Prof>>,
}

impl<Prof: EngineProfile> AppContext<Prof> {
    pub fn new(
        env: Environment<Prof>,
        app_store: AppStore,
        app_reg: AppRegistration<Prof>,
        max_inflight_components: Option<usize>,
    ) -> Self {
        let inflight_semaphore =
            max_inflight_components.map(|n| Arc::new(tokio::sync::Semaphore::new(n)));
        Self {
            inner: Arc::new(AppContextInner {
                env,
                app_store,
                app_reg,
                id_sequencer_manager: IdSequencerManager::new(),
                inflight_semaphore,
                cancellation_token: std::sync::Mutex::new(
                    crate::engine::runtime::global_cancellation_token().child_token(),
                ),
                live_components: parking_lot::Mutex::new(Vec::new()),
            }),
        }
    }

    /// Register a live component for shutdown drain coverage.
    ///
    /// Compacts dead `Weak`s out of the registry before appending the new one,
    /// so size is bounded by live + pending-GC count. The `Weak` is upgraded
    /// during `App::drop_app`'s walk to drive `cancel_and_await_quiescence`.
    pub fn register_live_component(
        &self,
        weak: std::sync::Weak<crate::engine::live_component::LiveComponentState<Prof>>,
    ) {
        let mut registry = self.inner.live_components.lock();
        registry.retain(|w| w.upgrade().is_some());
        registry.push(weak);
    }

    /// Atomically: cancel the app token AND snapshot the live-components
    /// registry into upgraded `Arc`s. Returns the snapshot.
    ///
    /// Acquiring the lock first closes a race: a concurrent
    /// `mount_live_async` mid-execution that captured an uncancelled
    /// parent_ctx token will either (a) finish registering before this lock
    /// acquisition (caught by the snapshot) or (b) queue behind the lock and
    /// see the cancelled token immediately on first poll once we release.
    pub fn cancel_and_snapshot_live_components(
        &self,
    ) -> Vec<Arc<crate::engine::live_component::LiveComponentState<Prof>>> {
        let registry = self.inner.live_components.lock();
        // Cancel inside the lock so a post-release registration sees the
        // cancelled token.
        self.inner.cancellation_token.lock().unwrap().cancel();
        registry
            .iter()
            .filter_map(std::sync::Weak::upgrade)
            .collect()
    }

    pub fn env(&self) -> &Environment<Prof> {
        &self.inner.env
    }

    pub fn app_store(&self) -> &AppStore {
        &self.inner.app_store
    }

    pub fn app_reg(&self) -> &AppRegistration<Prof> {
        &self.inner.app_reg
    }

    pub fn inflight_semaphore(&self) -> Option<&Arc<tokio::sync::Semaphore>> {
        self.inner.inflight_semaphore.as_ref()
    }

    /// Returns a clone of the current app-level cancellation token.
    ///
    /// The clone stays valid even if the slot is later refreshed via
    /// `reset_cancellation_token_if_cancelled`.
    pub fn cancellation_token(&self) -> tokio_util::sync::CancellationToken {
        self.inner.cancellation_token.lock().unwrap().clone()
    }

    /// Replace the app-level cancellation token with a fresh child of the global
    /// token if the current one has been cancelled. Call this before starting a
    /// new app operation so a prior cancellation (e.g. via `drop_app`) does not
    /// poison subsequent runs.
    pub fn reset_cancellation_token_if_cancelled(&self) {
        let mut slot = self.inner.cancellation_token.lock().unwrap();
        if slot.is_cancelled() {
            *slot = crate::engine::runtime::global_cancellation_token().child_token();
        }
    }

    /// Get the next ID for the given key.
    ///
    /// IDs are allocated in batches for efficiency. The key can be `None` for a default sequencer.
    pub async fn next_id(&self, key: Option<&StableKey>) -> Result<u64> {
        let default_key = StableKey::Null;
        let key = key.unwrap_or(&default_key);
        self.inner
            .id_sequencer_manager
            .next_id(self.inner.env.storage(), &self.inner.app_store, key)
            .await
    }
}

pub(crate) struct DeclaredTargetState<Prof: EngineProfile> {
    pub provider: TargetStateProvider<Prof>,
    pub item_key: StableKey,
    pub value: Prof::TargetStateValue,
    pub child_provider: Option<TargetStateProvider<Prof>>,
}

pub(crate) struct ComponentTargetStatesContext<Prof: EngineProfile> {
    pub declared_target_states: BTreeMap<TargetStatePath, DeclaredTargetState<Prof>>,
    pub provider_registry: TargetStateProviderRegistry<Prof>,
}

pub struct FnCallMemo<Prof: EngineProfile> {
    pub ret: Prof::FunctionData,
    pub(crate) target_state_paths: Vec<TargetStatePath>,
    pub(crate) dependency_memo_entries: HashSet<Fingerprint>,
    pub(crate) logic_deps: HashSet<Fingerprint>,
    pub memo_states: Vec<Prof::FunctionData>,
    /// Context-borne memo states, keyed by tracked-context value fingerprint.
    /// See `db_schema::FunctionMemoizationEntry::context_memo_states`.
    pub context_memo_states: Vec<(Fingerprint, Vec<Prof::FunctionData>)>,
    pub(crate) already_stored: bool,
}

/// Combined payload of positional and context-borne memo states.
///
/// Used to thread both halves through the `ComponentProcessor::handle_memo_states`
/// callback and through function-level memoization APIs. The core crate treats
/// the context fingerprints and values as opaque data — both come from Python in
/// the Python profile and are round-tripped through the Python state handler.
pub struct MemoStatesPayload<Prof: EngineProfile> {
    pub positional: Vec<Prof::FunctionData>,
    pub by_context_fp: Vec<(Fingerprint, Vec<Prof::FunctionData>)>,
}

impl<Prof: EngineProfile> Default for MemoStatesPayload<Prof> {
    fn default() -> Self {
        Self {
            positional: Vec::new(),
            by_context_fp: Vec::new(),
        }
    }
}

impl<Prof: EngineProfile> MemoStatesPayload<Prof> {
    pub fn is_empty(&self) -> bool {
        self.positional.is_empty() && self.by_context_fp.is_empty()
    }
}

pub enum FnCallMemoEntry<Prof: EngineProfile> {
    /// Prefetched from the database but not yet accessed during this build.
    /// Lazily decoded on first access (transitions to `Ready` or stays
    /// `Stored` for later GC). Treated as untouched at flush time.
    Stored(Vec<u8>),
    /// Memoization result is pending, i.e. the function call is not finished yet.
    Pending,
    /// Memoization result is ready. None means memoization is disabled, e.g. it mounts child components.
    Ready(Option<FnCallMemo<Prof>>),
}

impl<Prof: EngineProfile> Default for FnCallMemoEntry<Prof> {
    fn default() -> Self {
        Self::Pending
    }
}

/// In-memory cache of all function-memoization entries for a single
/// component build. Populated by [`Self::prefetch`] (one prefix scan over
/// the storage layer) at the start of build mode, then serves every
/// subsequent fn-memo lookup in memory. New entries from cache-miss
/// executions accumulate here; [`Self::flush_to_db`] applies the diff to
/// storage at commit time as per-entry writes and deletes.
pub(crate) struct FnMemoCache<Prof: EngineProfile> {
    /// All known fn-memo entries for this component. Prefetched entries
    /// start as `Stored(bytes)` and lazily decode to `Ready` on first
    /// access; cache-miss inserts go straight to `Pending` then `Ready`.
    entries: HashMap<Fingerprint, Arc<tokio::sync::RwLock<FnCallMemoEntry<Prof>>>>,
    /// True after a successful `prefetch`. Stays false under
    /// `full_reprocess` (prefetch skipped) or before `prefetch` runs.
    /// Determines the flush strategy: per-entry writes/deletes when true,
    /// prefix-delete + per-entry writes when false.
    is_fully_loaded: bool,
}

impl<Prof: EngineProfile> FnMemoCache<Prof> {
    pub(crate) fn new() -> Self {
        Self {
            entries: HashMap::new(),
            is_fully_loaded: false,
        }
    }

    /// Insert prefetched rows from the database into the cache and mark it
    /// fully loaded. The async I/O lives at the context layer
    /// ([`ComponentProcessorContext::prefetch_fn_memos`]); this is the sync
    /// half that runs under the building-state mutex.
    pub(crate) fn populate(&mut self, rows: Vec<(Fingerprint, Vec<u8>)>) {
        for (fp, bytes) in rows {
            self.entries.entry(fp).or_insert_with(|| {
                Arc::new(tokio::sync::RwLock::new(FnCallMemoEntry::Stored(bytes)))
            });
        }
        self.is_fully_loaded = true;
    }

    /// Get the entry for `fp`, inserting a `Pending` slot if absent. The
    /// returned `Arc<RwLock<_>>` is what `reserve_memoization` locks.
    pub(crate) fn entry_or_pending(
        &mut self,
        fp: Fingerprint,
    ) -> Arc<tokio::sync::RwLock<FnCallMemoEntry<Prof>>> {
        self.entries
            .entry(fp)
            .or_insert_with(|| Arc::new(tokio::sync::RwLock::new(FnCallMemoEntry::Pending)))
            .clone()
    }

    /// Read-only lookup. Returns `None` if no entry exists for `fp`. Used
    /// by the finalize-time dep walk.
    pub(crate) fn get(
        &self,
        fp: Fingerprint,
    ) -> Option<Arc<tokio::sync::RwLock<FnCallMemoEntry<Prof>>>> {
        self.entries.get(&fp).cloned()
    }

    pub(crate) fn is_fully_loaded(&self) -> bool {
        self.is_fully_loaded
    }

    /// Walk all entries. Used by finalize to enumerate touched/untouched
    /// state without consuming the cache.
    pub(crate) fn iter(
        &self,
    ) -> impl Iterator<
        Item = (
            &Fingerprint,
            &Arc<tokio::sync::RwLock<FnCallMemoEntry<Prof>>>,
        ),
    > {
        self.entries.iter()
    }

    /// Consume the cache, classify entries, and serialize the writes
    /// into a [`FnMemoFlushPlan`] that can be re-applied to storage
    /// across retries.
    ///
    /// Inclusion rule (same as the old `flush_to_db`):
    ///
    /// - `Ready(Some, already_stored=false)` → serialize bytes into
    ///   `writes` (new or re-executed entries that must be written).
    /// - `Ready(Some, already_stored=true)` → skip (DB row already correct).
    /// - `Stored(_)` and `Ready(None)` → record in `deletes`, only when
    ///   `is_fully_loaded=true` (otherwise these entries can't exist on
    ///   disk because prefetch didn't run).
    /// - `Pending` → no-op (the function errored before resolving; no
    ///   DB row exists or needs to exist).
    ///
    /// `clear_all_first` is set when `!is_fully_loaded` so the apply
    /// step prefix-deletes the whole range before writing `writes`.
    pub(crate) fn into_flush_plan(self) -> Result<FnMemoFlushPlan> {
        let mut plan = FnMemoFlushPlan {
            clear_all_first: !self.is_fully_loaded,
            writes: Vec::new(),
            deletes: Vec::new(),
        };
        for (fp, lock) in self.entries.into_iter() {
            // No other holders at flush time — extract by reference under
            // a try_write guard rather than unwrapping the Arc, since
            // upstream cancellation paths may have leaked clones.
            let mut guard = lock.try_write().map_err(|_| {
                internal_error!("fn memo entry for {fp:?} still locked at flush time")
            })?;
            let state = std::mem::replace(&mut *guard, FnCallMemoEntry::Pending);
            match state {
                FnCallMemoEntry::Ready(Some(memo)) => {
                    if memo.already_stored {
                        // Cache hit: DB row already correct.
                        continue;
                    }
                    let ret_bytes = memo.ret.to_bytes()?;
                    let memo_states_serialized = serialize_memo_values::<Prof>(&memo.memo_states)?;
                    let context_memo_states_serialized =
                        serialize_context_memo_states::<Prof>(&memo.context_memo_states)?;
                    let entry = db_schema::FunctionMemoizationEntry {
                        return_value: db_schema::MemoizedValue::Inlined(Cow::Borrowed(
                            ret_bytes.as_ref(),
                        )),
                        child_components: vec![],
                        target_state_paths: memo.target_state_paths,
                        dependency_memo_entries: memo.dependency_memo_entries.into_iter().collect(),
                        logic_deps: memo.logic_deps.into_iter().collect(),
                        memo_states: memo_states_serialized,
                        context_memo_states: context_memo_states_serialized,
                    };
                    let encoded = rmp_serde::to_vec_named(&entry)?;
                    plan.writes.push((fp, encoded));
                }
                FnCallMemoEntry::Stored(_) | FnCallMemoEntry::Ready(None) => {
                    if self.is_fully_loaded {
                        // Stored: untouched prefetched entry, stale.
                        // Ready(None): memoization disabled at runtime.
                        plan.deletes.push(fp);
                    }
                }
                FnCallMemoEntry::Pending => {
                    // The slot was inserted via `entry_or_pending` for a fp not
                    // present in the prefetched cache (prefetched fps decode
                    // to `Ready` in `reserve_memoization`, never end up
                    // Pending). The function call then errored before
                    // resolving — e.g. the caller wrapped it in try/except
                    // and continued. Such fps have no DB row, so nothing to
                    // write or delete.
                }
            }
        }
        Ok(plan)
    }
}

/// A serialized, re-applyable diff produced by
/// [`FnMemoCache::into_flush_plan`]. Owns the encoded bytes for every
/// write so `apply_to_db` can be called repeatedly across retries
/// without re-touching the cache.
pub(crate) struct FnMemoFlushPlan {
    pub(crate) clear_all_first: bool,
    pub(crate) writes: Vec<(Fingerprint, Vec<u8>)>,
    pub(crate) deletes: Vec<Fingerprint>,
}

impl<Prof: EngineProfile> Default for FnMemoCache<Prof> {
    fn default() -> Self {
        Self::new()
    }
}

/// Decode a `Stored(bytes)` entry into `Ready(Some(memo))` if the entry's
/// stored logic deps still resolve. If the deps no longer resolve (e.g.
/// logic registry no longer contains a fingerprint), or the entry is a
/// legacy form with `child_components`, the result is `Ready(None)` so
/// the entry is treated as a deletion at flush time.
///
/// This helper is shared between `reserve_memoization` (probe path) and
/// the finalize dep walk; both call it under the per-entry write lock.
///
/// `*entry` must be `Stored(_)` on entry. After this call it is `Ready`.
pub(crate) fn decode_stored_entry<Prof: EngineProfile>(
    entry: &mut FnCallMemoEntry<Prof>,
    env: &Environment<Prof>,
) -> Result<()> {
    let FnCallMemoEntry::Stored(bytes) = std::mem::replace(entry, FnCallMemoEntry::Pending) else {
        internal_bail!("decode_stored_entry called on non-Stored entry");
    };
    let decoded: db_schema::FunctionMemoizationEntry<'_> = from_msgpack_slice(&bytes)?;
    if !crate::engine::logic_registry::all_contained_with_env(&decoded.logic_deps, env) {
        *entry = FnCallMemoEntry::Ready(None);
        return Ok(());
    }
    if !decoded.child_components.is_empty() {
        // Legacy entry with stored child component paths. Invalidate so the
        // function re-runs, detects child components, and the entry is
        // cleaned up.
        *entry = FnCallMemoEntry::Ready(None);
        return Ok(());
    }
    let return_value_bytes = match decoded.return_value {
        db_schema::MemoizedValue::Inlined(b) => b,
    };
    let ret = Prof::FunctionData::from_bytes(return_value_bytes.as_ref())?;
    let memo_states = deserialize_memo_values::<Prof>(&decoded.memo_states)?;
    let context_memo_states =
        deserialize_context_memo_states::<Prof>(&decoded.context_memo_states)?;
    *entry = FnCallMemoEntry::Ready(Some(FnCallMemo {
        ret,
        target_state_paths: decoded.target_state_paths,
        dependency_memo_entries: decoded.dependency_memo_entries.into_iter().collect(),
        logic_deps: decoded.logic_deps.into_iter().collect(),
        memo_states,
        context_memo_states,
        already_stored: true,
    }));
    Ok(())
}

pub(crate) struct ComponentBuildingState<Prof: EngineProfile> {
    pub target_states: ComponentTargetStatesContext<Prof>,
    pub child_path_set: ChildStablePathSet,
    pub fn_memos: FnMemoCache<Prof>,
}

pub(crate) struct ComponentBuildContext<Prof: EngineProfile> {
    pub state: Mutex<Option<ComponentBuildingState<Prof>>>,
    pub full_reprocess: bool,
    pub live: bool,
    /// Error handler routed to orphan-delete failures from this build's
    /// commit-phase GC sweep. Same shape and meaning as
    /// `ComponentDeleteContext::on_error` — see that doc for the
    /// unified principle. `None` preserves the "log + swallow"
    /// default (e.g. root `App.update`, `use_mount`'s foreground path,
    /// `mount_inner_live`'s self-built parent context).
    pub on_error: Option<crate::engine::component::OnError>,
}

pub(crate) struct ComponentDeleteContext<Prof: EngineProfile> {
    pub providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
    /// Error handler that cascades through descendant deletes triggered
    /// by this delete's GC sweep. `App.drop()` installs a raising handler
    /// at the root; it propagates down so any descendant failure surfaces
    /// back through `handle.ready()` to the awaiting caller. `None`
    /// preserves the "log + swallow" default for callers that don't need
    /// propagation (e.g. `operator.delete` without a user-installed
    /// raising handler).
    ///
    /// See `specs/core/error_handling.md` for the unified principle.
    pub on_error: Option<crate::engine::component::OnError>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ComponentProcessingMode {
    Build,
    Delete,
}

pub(crate) enum ComponentProcessingAction<Prof: EngineProfile> {
    Build(ComponentBuildContext<Prof>),
    Delete(ComponentDeleteContext<Prof>),
}

impl<Prof: EngineProfile> ComponentProcessingAction<Prof> {
    pub fn new_build(
        providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
        full_reprocess: bool,
        live: bool,
        on_error: Option<crate::engine::component::OnError>,
    ) -> Self {
        Self::Build(ComponentBuildContext {
            state: Mutex::new(Some(ComponentBuildingState {
                target_states: ComponentTargetStatesContext {
                    declared_target_states: Default::default(),
                    provider_registry: TargetStateProviderRegistry::new(providers),
                },
                child_path_set: Default::default(),
                fn_memos: FnMemoCache::new(),
            })),
            full_reprocess,
            live,
            on_error,
        })
    }
}

struct ComponentProcessorContextInner<Prof: EngineProfile> {
    component: Component<Prof>,
    parent_context: Option<ComponentProcessorContext<Prof>>,
    processing_action: ComponentProcessingAction<Prof>,

    inflight_permit: Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,

    /// Logic fingerprints accumulated from function calls and child components.
    logic_deps: Mutex<HashSet<Fingerprint>>,

    /// Opaque per-operation context (e.g. ContextProvider on the Python side).
    host_ctx: Arc<Prof::HostCtx>,
}

/// A `ComponentProcessorContext` is a thin view over a shared `inner`
/// (component identity, building state, providers — never forked) plus three
/// **per-view** fields that a `stats_group` substitutes: the stats bucket, the
/// child-readiness accumulator, and the enclosing-group list for liveness.
/// `Clone` shares everything (all `Arc`-based handles), so an unscoped clone is
/// byte-for-byte equivalent; only `with_stats_group` produces a divergent view.
#[derive(Clone)]
pub struct ComponentProcessorContext<Prof: EngineProfile> {
    inner: Arc<ComponentProcessorContextInner<Prof>>,
    /// Where this view's components report stats (root's, or a group's).
    processing_stats: ProcessingStats,
    /// Where children mounted under this view register their readiness.
    components_readiness: ComponentBgChildReadiness,
    /// Enclosing stats groups, outermost-first, for live-member liveness
    /// fan-out. Empty for the root / unscoped views.
    stats_groups: Arc<Vec<Arc<StatsGroup<Prof>>>>,
}

impl<Prof: EngineProfile> ComponentProcessorContext<Prof> {
    pub(crate) fn new(
        component: Component<Prof>,
        parent_context: Option<ComponentProcessorContext<Prof>>,
        processing_stats: ProcessingStats,
        host_ctx: Arc<Prof::HostCtx>,
        processing_action: ComponentProcessingAction<Prof>,
    ) -> Self {
        Self {
            inner: Arc::new(ComponentProcessorContextInner {
                component,
                parent_context,
                processing_action,
                inflight_permit: Mutex::new(None),
                logic_deps: Mutex::new(HashSet::new()),
                host_ctx,
            }),
            processing_stats,
            components_readiness: Default::default(),
            stats_groups: Arc::new(Vec::new()),
        }
    }

    /// Derive a sibling view that reports into `group`'s stats, registers child
    /// readiness into `group`'s readiness, and appends `group` to the
    /// enclosing-group list (for live-member liveness). Shares `inner` — so
    /// component identity, building state, providers, and the inflight permit
    /// are unchanged.
    pub(crate) fn with_stats_group(&self, group: &Arc<StatsGroup<Prof>>) -> Self {
        let mut stats_groups = Vec::with_capacity(self.stats_groups.len() + 1);
        stats_groups.extend(self.stats_groups.iter().cloned());
        stats_groups.push(group.clone());
        Self {
            inner: self.inner.clone(),
            processing_stats: group.stats().clone(),
            components_readiness: group.readiness().clone(),
            stats_groups: Arc::new(stats_groups),
        }
    }

    /// Register a freshly-mounted child as an active member of every enclosing
    /// stats group, so each group's liveness tracking sees it (and, via the
    /// strong parent-chain, its whole subtree). No-op when not in a group.
    pub fn push_active_member(&self, child: &Component<Prof>) {
        for group in self.stats_groups.iter() {
            group.push_member(child);
        }
    }

    pub fn host_ctx(&self) -> &Arc<Prof::HostCtx> {
        &self.inner.host_ctx
    }

    pub fn component(&self) -> &Component<Prof> {
        &self.inner.component
    }

    pub fn app_ctx(&self) -> &AppContext<Prof> {
        self.inner.component.app_ctx()
    }

    pub fn stable_path(&self) -> &StablePath {
        self.inner.component.stable_path()
    }

    /// Eagerly load every function-memo entry for this component from
    /// storage into the per-build cache. Called at the start of build mode
    /// before any function calls run; skipped under `full_reprocess` so
    /// the cache stays empty and `FnMemoCache::flush_to_db` falls through
    /// to a prefix delete + write of newly-computed entries.
    pub(crate) async fn prefetch_fn_memos(&self) -> Result<()> {
        if self.full_reprocess() {
            return Ok(());
        }
        // Cheap check: skip if already loaded (re-entry, etc.).
        let already_loaded = match &self.inner.processing_action {
            ComponentProcessingAction::Build(build_ctx) => {
                let guard = build_ctx.state.lock().unwrap();
                let Some(state) = guard.as_ref() else {
                    return Ok(());
                };
                state.fn_memos.is_fully_loaded()
            }
            ComponentProcessingAction::Delete { .. } => return Ok(()),
        };
        if already_loaded {
            return Ok(());
        }
        let app_store = self.app_ctx().app_store();
        let path = self.stable_path();
        let rows = app_store.list_fn_memos(path).await?;
        self.update_building_state(|s| {
            s.fn_memos.populate(rows);
            Ok(())
        })
    }

    pub(crate) fn parent_context(&self) -> Option<&ComponentProcessorContext<Prof>> {
        self.inner.parent_context.as_ref()
    }

    /// Access the building state under a single lock acquisition.
    /// Callers should access all needed fields within `f` rather than wrapping
    /// this in convenience methods — the caller decides the lock granularity.
    pub(crate) fn update_building_state<T>(
        &self,
        f: impl FnOnce(&mut ComponentBuildingState<Prof>) -> Result<T>,
    ) -> Result<T> {
        match &self.inner.processing_action {
            ComponentProcessingAction::Build(build_ctx) => {
                let mut building_state = build_ctx.state.lock().unwrap();
                let Some(building_state) = &mut *building_state else {
                    internal_bail!(
                        "Processing for the component at {} is already finished",
                        self.stable_path()
                    );
                };
                f(building_state)
            }
            ComponentProcessingAction::Delete { .. } => {
                internal_bail!(
                    "Processing for the component at {} is for deletion only",
                    self.stable_path()
                )
            }
        }
    }

    pub(crate) fn processing_state(&self) -> &ComponentProcessingAction<Prof> {
        &self.inner.processing_action
    }

    pub fn components_readiness(&self) -> &ComponentBgChildReadiness {
        &self.components_readiness
    }

    pub(crate) fn mode(&self) -> ComponentProcessingMode {
        match &self.inner.processing_action {
            ComponentProcessingAction::Build(_) => ComponentProcessingMode::Build,
            ComponentProcessingAction::Delete { .. } => ComponentProcessingMode::Delete,
        }
    }

    /// Clone of the `on_error` handler installed at the context's creation,
    /// available for both Build- and Delete-mode contexts.
    ///
    /// Read by:
    /// - `Component::delete`'s spawned task (Delete only — invoke on this
    ///   component's own failure).
    /// - The commit-phase GC sweep (both modes — cascade to descendant
    ///   deletes triggered by this component's submit/commit).
    ///
    /// For Delete-mode (`App.drop`'s root), the raising handler installed
    /// at root cascades down through every recursive delete. For Build-mode
    /// (a `coco.mount` child whose `process()` no longer declares a
    /// previously-existing grandchild), the child's user-installed
    /// exception handler chain — wired through `Component::mount`'s
    /// `on_error` parameter — sees orphan-delete failures from the
    /// commit-phase GC sweep.
    ///
    /// Returns `None` when no handler was installed (e.g. root `App.update`,
    /// `use_mount`'s foreground path, `operator.delete` without a
    /// user-installed raising handler).
    pub(crate) fn processing_action_on_error(&self) -> Option<crate::engine::component::OnError> {
        match &self.inner.processing_action {
            ComponentProcessingAction::Build(b) => b.on_error.clone(),
            ComponentProcessingAction::Delete(d) => d.on_error.clone(),
        }
    }

    /// Delete-mode-only on_error accessor. Used by `Component::delete`'s
    /// spawned task to invoke the handler on this component's own
    /// execution failure (a *Build*-context's on_error is meant for
    /// orphan-delete cascades, not for invoking on the build's own
    /// failure — that flows through the `on_error` argument to
    /// `run_in_background` directly).
    pub(crate) fn delete_action_on_error(&self) -> Option<crate::engine::component::OnError> {
        match &self.inner.processing_action {
            ComponentProcessingAction::Delete(d) => d.on_error.clone(),
            ComponentProcessingAction::Build(_) => None,
        }
    }

    pub fn join_fn_call(&self, fn_ctx: &FnCallContext) {
        let (fn_logic_deps, context_change_deps) = fn_ctx.update(|inner| {
            (
                inner.fn_logic_deps.clone(),
                inner.context_change_deps.clone(),
            )
        });
        let mut deps = self.inner.logic_deps.lock().unwrap();
        deps.extend(fn_logic_deps);
        deps.extend(context_change_deps);
    }

    /// Merge additional logic deps (e.g. from child components) into this component's set.
    pub(crate) fn merge_logic_deps(&self, deps: impl IntoIterator<Item = Fingerprint>) {
        self.inner.logic_deps.lock().unwrap().extend(deps);
    }

    /// Take the accumulated logic deps as a sorted Vec for deterministic storage.
    pub(crate) fn take_logic_deps(&self) -> Vec<Fingerprint> {
        let deps = std::mem::take(&mut *self.inner.logic_deps.lock().unwrap());
        let mut v: Vec<_> = deps.into_iter().collect();
        v.sort();
        v
    }

    /// Collect initial memo states for the change-detection context fingerprints
    /// observed so far (stored in this component's `logic_deps`), by looking
    /// them up in the env's eager-initial-states registry.
    ///
    /// Used by the Python memoization layer on cache miss to populate a new
    /// entry's `context_memo_states` in a single Rust call, without
    /// snapshotting `logic_deps` to Python and doing per-entry lookups there.
    pub fn collect_context_initial_states(&self) -> Vec<(Fingerprint, Vec<Prof::FunctionData>)> {
        let deps = self.inner.logic_deps.lock().unwrap();
        self.app_ctx()
            .env()
            .collect_context_initial_states(deps.iter())
    }

    pub(crate) fn set_inflight_permit(&self, permit: tokio::sync::OwnedSemaphorePermit) {
        *self.inner.inflight_permit.lock().unwrap() = Some(permit);
    }

    /// Release the inflight permit if held. No-op after first call.
    pub(crate) fn release_inflight_permit(&self) {
        *self.inner.inflight_permit.lock().unwrap() = None;
    }

    pub fn processing_stats(&self) -> &ProcessingStats {
        &self.processing_stats
    }

    pub fn full_reprocess(&self) -> bool {
        match &self.inner.processing_action {
            ComponentProcessingAction::Build(build_ctx) => build_ctx.full_reprocess,
            ComponentProcessingAction::Delete { .. } => false,
        }
    }

    pub fn live(&self) -> bool {
        match &self.inner.processing_action {
            ComponentProcessingAction::Build(build_ctx) => build_ctx.live,
            ComponentProcessingAction::Delete { .. } => false,
        }
    }
}

#[derive(Default)]
pub struct FnCallContextInner {
    /// Target states that are declared by the function.
    pub target_state_paths: Vec<TargetStatePath>,
    /// Dependency entries that are declared by the function. Only needs to keep dependencies with side effects (target states / dependency entries with side effects).
    pub dependency_memo_entries: HashSet<Fingerprint>,

    /// Whether the function (directly or transitively) mounted any child components.
    /// If true, function-level memoization is disabled for this call.
    pub has_child_components: bool,

    /// Function logic fingerprints (mode-controlled propagation via `propagate_children_fn_logic`).
    pub fn_logic_deps: HashSet<Fingerprint>,
    /// Context key fingerprints (always propagate regardless of logic_tracking mode).
    pub context_change_deps: HashSet<Fingerprint>,
}

pub struct FnCallContext {
    pub(crate) inner: Mutex<FnCallContextInner>,
    /// Whether to merge children's `fn_logic_deps` into this context.
    /// `true` for "full" mode, `false` for "self" or `None` mode.
    propagate_children_fn_logic: bool,
}

impl Default for FnCallContext {
    fn default() -> Self {
        Self {
            inner: Mutex::new(FnCallContextInner::default()),
            propagate_children_fn_logic: true,
        }
    }
}

impl FnCallContext {
    pub fn new(propagate_children_fn_logic: bool) -> Self {
        Self {
            inner: Mutex::new(FnCallContextInner::default()),
            propagate_children_fn_logic,
        }
    }

    pub fn join_child(&self, child_fn_ctx: &FnCallContext) {
        // Take the child's inner first to keep lock scope small (and avoid deadlock).
        let child_inner = child_fn_ctx.update(std::mem::take);
        self.update(|inner| {
            inner
                .target_state_paths
                .extend(child_inner.target_state_paths);
            inner
                .dependency_memo_entries
                .extend(child_inner.dependency_memo_entries);
            inner.has_child_components |= child_inner.has_child_components;
            // Context change deps always propagate.
            inner
                .context_change_deps
                .extend(child_inner.context_change_deps);
            // Function logic deps conditionally propagate.
            if self.propagate_children_fn_logic {
                inner.fn_logic_deps.extend(child_inner.fn_logic_deps);
            }
        });
    }

    pub fn add_fn_logic_dep(&self, fp: Fingerprint) {
        self.update(|inner| {
            inner.fn_logic_deps.insert(fp);
        });
    }

    pub fn add_context_change_dep(&self, fp: Fingerprint) {
        self.update(|inner| {
            inner.context_change_deps.insert(fp);
        });
    }

    /// Collect initial memo states for the change-detection context fingerprints
    /// observed so far in this function-call context, by looking them up in
    /// the given env's eager-initial-states registry.
    ///
    /// Parallel to `ComponentProcessorContext::collect_context_initial_states`;
    /// used on cache miss in the function-level memoization path (where we
    /// only have an `FnCallContext`, not a `ComponentProcessorContext`).
    pub fn collect_context_initial_states<Prof: EngineProfile>(
        &self,
        env: &Environment<Prof>,
    ) -> Vec<(Fingerprint, Vec<Prof::FunctionData>)> {
        self.update(|inner| env.collect_context_initial_states(inner.context_change_deps.iter()))
    }

    pub fn update<T>(&self, f: impl FnOnce(&mut FnCallContextInner) -> T) -> T {
        let mut guard = self.inner.lock().unwrap();
        f(&mut guard)
    }
}
