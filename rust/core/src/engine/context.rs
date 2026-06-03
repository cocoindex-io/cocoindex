use std::collections::{BTreeMap, HashMap, HashSet};

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
    state_store::{AppStore, WriteTxn},
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

/// Lifecycle state of one user-state key within a single component build.
///
/// TODO: make `V` generic over the engine profile (like `FnCallMemoEntry<Prof>`) so
/// `Declared` can hold a `Prof::FunctionData` (= `PyStoredValue` in the Python profile)
/// instead of raw bytes. `PyStoredValue` holds both representations lazily behind an
/// `Arc<Mutex<...>>`, so cloning is a cheap refcount bump, deserialization from bytes
/// happens once on first access, and serialization to bytes happens once at flush.
/// Requires adding a `FunctionData`-equivalent associated type to `EngineProfile` (or
/// reusing `FunctionData` directly) and threading it through `UserStateCache`.
enum UserStateEntry {
    /// Prefetched from DB at build start; `use_state()` not yet called for
    /// this key. Deleted at flush time if it stays in this state (set-reduction path).
    Loaded(Vec<u8>),
    /// Claimed by `use_state()` and optionally overwritten by
    /// `update_declared()`. Written to DB at flush time.
    Declared(Vec<u8>),
}

/// Per-component user state cache for a single build.
///
/// Mirrors the `FnMemoCache` pattern: `populate` fills it from a
/// standalone read at build start, then `use_state` / `update_declared`
/// accumulate the declared states, and `flush_to_db` commits them.
/// At flush time only keys re-declared or newly declared by the user
/// during this build through use_state() survive: `Declared` entries are
/// written to DB and `Loaded` entries (present in the previous build but
/// not re-declared this build) are deleted.
pub(crate) struct UserStateCache {
    /// All entries seen this build. Prefetched entries start as `Loaded` and
    /// transition to `Declared` when claimed by `use_state()`; brand-new keys
    /// start directly as `Declared`.
    entries: HashMap<StableKey, UserStateEntry>,
    /// True after a successful `populate`;
    /// False under `full_reprocess` (prefetch skipped).
    ///
    /// When true: stale `Loaded` entries are individually deleted.
    /// When false: all existing entries are wiped upfront via
    /// delete_all_user_states before writing.
    is_loaded: bool,
}

impl UserStateCache {
    pub(crate) fn new() -> Self {
        Self {
            entries: HashMap::new(),
            is_loaded: false,
        }
    }

    pub(crate) fn is_loaded(&self) -> bool {
        self.is_loaded
    }

    pub(crate) fn populate(&mut self, rows: Vec<(StableKey, Vec<u8>)>) {
        self.entries = rows
            .into_iter()
            .map(|(k, v)| (k, UserStateEntry::Loaded(v)))
            .collect();
        self.is_loaded = true;
    }

    /// Register `key` as declared for this build. Returns the previously
    /// stored value (if any) or `initial_value`. Errors on duplicate keys.
    /// Called when the user calls coco.use_state("key", initial_value)
    pub(crate) fn use_state(&mut self, key: StableKey, initial_value: Vec<u8>) -> Result<Vec<u8>> {
        use std::collections::hash_map::Entry;
        match self.entries.entry(key) {
            Entry::Occupied(mut e) => match e.get() {
                UserStateEntry::Declared(_) => {
                    client_bail!(
                        "coco.use_state() key {:?} declared more than once in the same component run",
                        e.key()
                    );
                }
                UserStateEntry::Loaded(v) => {
                    let value = v.clone();
                    e.insert(UserStateEntry::Declared(value.clone()));
                    Ok(value)
                }
            },
            Entry::Vacant(e) => {
                e.insert(UserStateEntry::Declared(initial_value.clone()));
                Ok(initial_value)
            }
        }
    }

    /// Update the current value for an already-declared key. Called when
    /// the user sets `my_state.value = ...`.
    pub(crate) fn update_declared(&mut self, key: &StableKey, value: Vec<u8>) -> Result<()> {
        match self.entries.get_mut(key) {
            Some(UserStateEntry::Declared(v)) => {
                *v = value;
                Ok(())
            }
            _ => {
                client_bail!(
                    "coco.use_state() key {:?} has not been declared via use_state() in this component run",
                    key
                );
            }
        }
    }

    /// Drain the cache into a serialized plan for inclusion in [`CommitPlan`].
    /// Mirrors [`FnMemoCache::into_flush_plan`].
    pub(crate) fn into_flush_plan(self) -> UserStateFlushPlan {
        let mut plan = UserStateFlushPlan {
            clear_all_first: !self.is_loaded,
            writes: Vec::new(),
            deletes: Vec::new(),
        };
        for (key, entry) in self.entries {
            match entry {
                UserStateEntry::Loaded(_) if self.is_loaded => plan.deletes.push(key),
                UserStateEntry::Declared(value) => plan.writes.push((key, value)),
                _ => {}
            }
        }
        plan
    }

    /// Apply the set-reduction diff to LMDB directly (used by tests).
    pub(crate) async fn flush_to_db(
        self,
        wtxn: &mut WriteTxn<'_>,
        app_store: &AppStore,
        path: &StablePath,
    ) -> Result<()> {
        if !self.is_loaded {
            // full_reprocess path: wipe all existing entries, then write declared.
            app_store.delete_all_user_states(wtxn, path).await?;
        }
        for (key, entry) in &self.entries {
            match entry {
                UserStateEntry::Loaded(_) if self.is_loaded => {
                    // Loaded from DB but not re-declared this build: delete.
                    app_store.delete_user_state(wtxn, path, key).await?;
                }
                UserStateEntry::Declared(value) => {
                    app_store.write_user_state(wtxn, path, key, value).await?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// Serialized diff produced by [`UserStateCache::into_flush_plan`].
/// Included in [`CommitPlan`] so the AppStore can apply it atomically.
pub(crate) struct UserStateFlushPlan {
    pub(crate) clear_all_first: bool,
    pub(crate) writes: Vec<(StableKey, Vec<u8>)>,
    pub(crate) deletes: Vec<StableKey>,
}

pub(crate) struct ComponentBuildingState<Prof: EngineProfile> {
    pub target_states: ComponentTargetStatesContext<Prof>,
    pub child_path_set: ChildStablePathSet,
    pub fn_memos: FnMemoCache<Prof>,
    pub user_states: UserStateCache,
}

/// Shared collector for preview actions across all components in a single update.
pub(crate) type PreviewActionCollector<Prof> =
    Arc<std::sync::Mutex<Vec<<Prof as EngineProfile>::TargetAction>>>;

pub(crate) struct ComponentBuildContext<Prof: EngineProfile> {
    pub state: Mutex<Option<ComponentBuildingState<Prof>>>,
    pub full_reprocess: bool,
    pub live: bool,
    /// default (e.g. root `App.update`, `use_mount`'s foreground path,
    /// `mount_inner_live`'s self-built parent context).
    pub on_error: Option<crate::engine::component::OnError>,
    pub preview_collector: Option<PreviewActionCollector<Prof>>,
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
        preview_collector: Option<PreviewActionCollector<Prof>>,
    ) -> Self {
        Self::Build(ComponentBuildContext {
            state: Mutex::new(Some(ComponentBuildingState {
                target_states: ComponentTargetStatesContext {
                    declared_target_states: Default::default(),
                    provider_registry: TargetStateProviderRegistry::new(providers),
                },
                child_path_set: Default::default(),
                fn_memos: FnMemoCache::new(),
                user_states: UserStateCache::new(),
            })),
            full_reprocess,
            live,
            on_error,
            preview_collector,
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

    /// Eagerly load every user-state entry for this component from storage
    /// into the per-build cache. Mirrors `prefetch_fn_memos`; skipped under
    /// `full_reprocess` so `UserStateCache::flush_to_db` falls through to
    /// a delete-all + write of the newly-declared entries.
    pub(crate) async fn prefetch_user_states(&self) -> Result<()> {
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
                state.user_states.is_loaded()
            }
            ComponentProcessingAction::Delete { .. } => return Ok(()),
        };
        if already_loaded {
            return Ok(());
        }

        let rows = self
            .app_ctx()
            .app_store()
            .list_user_states(self.stable_path())
            .await?;
        self.update_building_state(|s| {
            s.user_states.populate(rows);
            Ok(())
        })
    }

    /// Register `key` as a user state for this build and return its current
    /// value. On first call for a key, the stored value (if any) is returned;
    /// otherwise `initial_value` is used. Duplicate keys within the same
    /// component run are an error.
    /// Called when the user calls coco.use_state("key", initial_value)
    pub fn use_state(&self, key: StableKey, initial_value: Vec<u8>) -> Result<Vec<u8>> {
        self.update_building_state(|s| s.user_states.use_state(key, initial_value))
    }

    /// Update the current value for an already-declared user state key.
    /// Called when the user sets `my_state.value = ...`.
    pub fn update_user_state(&self, key: &StableKey, value: Vec<u8>) -> Result<()> {
        self.update_building_state(|s| s.user_states.update_declared(key, value))
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

    pub fn preview(&self) -> bool {
        match &self.inner.processing_action {
            ComponentProcessingAction::Build(build_ctx) => build_ctx.preview_collector.is_some(),
            ComponentProcessingAction::Delete { .. } => false,
        }
    }

    pub(crate) fn preview_collector(&self) -> Option<&PreviewActionCollector<Prof>> {
        match &self.inner.processing_action {
            ComponentProcessingAction::Build(build_ctx) => build_ctx.preview_collector.as_ref(),
            ComponentProcessingAction::Delete { .. } => None,
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

#[cfg(test)]
mod tests {
    use super::{UserStateCache, UserStateEntry};
    use crate::state::stable_path::{StableKey, StablePath};
    use crate::state_store::{AppStore, WriteTxn};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn sym(s: &str) -> StableKey {
        StableKey::Symbol(Arc::from(s))
    }

    fn b(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    fn comp_path(name: &str) -> StablePath {
        StablePath(Arc::from(vec![StableKey::Str(Arc::from(name))]))
    }

    async fn make_test_store() -> (AppStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("mdb");
        std::fs::create_dir_all(&db_path).unwrap();
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .read_txn_without_tls()
                .max_dbs(4)
                .map_size(1 << 22)
                .open(&db_path)
        }
        .unwrap();
        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database(&mut wtxn, Some("test")).unwrap();
        wtxn.commit().unwrap();
        let storage = crate::state_store::Storage::from_env(env.clone());
        (AppStore::new(db, env, storage), dir)
    }

    fn to_map(pairs: Vec<(StableKey, Vec<u8>)>) -> HashMap<StableKey, Vec<u8>> {
        pairs.into_iter().collect()
    }

    // --- use_state -----------------------------------------------------------

    #[test]
    fn use_state_returns_initial_when_no_loaded_state() {
        let mut cache = UserStateCache::new();
        let val = cache.use_state(sym("k"), b("init")).unwrap();
        assert_eq!(val, b("init"));
    }

    #[test]
    fn use_state_returns_loaded_value_ignoring_initial() {
        let mut cache = UserStateCache::new();
        cache.populate(vec![(sym("k"), b("stored"))]);
        let val = cache.use_state(sym("k"), b("init")).unwrap();
        assert_eq!(val, b("stored")); // initial_value is ignored
    }

    #[test]
    fn use_state_duplicate_key_errors() {
        let mut cache = UserStateCache::new();
        cache.use_state(sym("k"), b("v")).unwrap();
        assert!(cache.use_state(sym("k"), b("v2")).is_err());
    }

    #[test]
    fn use_state_loaded_and_fresh_keys_independent() {
        let mut cache = UserStateCache::new();
        cache.populate(vec![(sym("a"), b("stored_a"))]);
        let va = cache.use_state(sym("a"), b("ignored")).unwrap();
        let vb = cache.use_state(sym("b"), b("init_b")).unwrap();
        assert_eq!(va, b("stored_a")); // from loaded state
        assert_eq!(vb, b("init_b")); // initial_value (not in loaded)
    }

    // --- update_declared -----------------------------------------------------

    #[test]
    fn update_declared_undeclared_key_errors() {
        let mut cache = UserStateCache::new();
        assert!(cache.update_declared(&sym("k"), b("v")).is_err());
    }

    #[test]
    fn update_declared_updates_value_in_declared() {
        let mut cache = UserStateCache::new();
        cache.use_state(sym("k"), b("init")).unwrap();
        cache.update_declared(&sym("k"), b("updated")).unwrap();
        // The updated value is what flush_to_db will write.
        assert!(matches!(
            &cache.entries[&sym("k")],
            UserStateEntry::Declared(v) if v == &b("updated")
        ));
    }

    // --- flush_to_db: set-reduction (is_loaded = true) -----------------------

    #[tokio::test]
    async fn flush_set_reduction_removes_dropped_keys() {
        // loaded = {a, b, c}; declared = {a, b}  →  c must be deleted.
        let (store, _dir) = make_test_store().await;
        let p = comp_path("comp");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, &sym("a"), b"a_val")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, &sym("b"), b"b_val")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, &sym("c"), b"c_val")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let mut cache = UserStateCache::new();
        cache.populate(vec![
            (sym("a"), b("a_val")),
            (sym("b"), b("b_val")),
            (sym("c"), b("c_val")),
        ]);
        cache.use_state(sym("a"), b("ignored")).unwrap();
        cache.use_state(sym("b"), b("ignored")).unwrap();
        // "c" not re-declared

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        cache.flush_to_db(&mut wtxn, &store, &p).await.unwrap();
        wtxn.into_inner().commit().unwrap();

        let entries = to_map(store.list_user_states(&p).await.unwrap());
        assert_eq!(entries.len(), 2);
        assert!(entries.contains_key(&sym("a")));
        assert!(entries.contains_key(&sym("b")));
        assert!(!entries.contains_key(&sym("c")));
    }

    #[tokio::test]
    async fn flush_set_reduction_adds_new_keys() {
        // loaded = {a}; declared = {a, b_new}  →  b is inserted.
        let (store, _dir) = make_test_store().await;
        let p = comp_path("comp");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, &sym("a"), b"a_val")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let mut cache = UserStateCache::new();
        cache.populate(vec![(sym("a"), b("a_val"))]);
        // keep initial value from previous flush.
        cache.use_state(sym("a"), b("should_be_ignored")).unwrap();
        cache.use_state(sym("b"), b("b_new")).unwrap(); // new key

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        cache.flush_to_db(&mut wtxn, &store, &p).await.unwrap();
        wtxn.into_inner().commit().unwrap();

        let entries = to_map(store.list_user_states(&p).await.unwrap());
        assert_eq!(entries.len(), 2);
        // keep initial value from previous flush.
        assert_eq!(entries[&sym("a")], b("a_val"));
        assert_eq!(entries[&sym("b")], b("b_new"));
    }

    #[tokio::test]
    async fn flush_set_reduction_persists_updated_value() {
        // loaded = {k: "old"}; declare k then update_declared to "new"
        // →  k must be written with "new".
        let (store, _dir) = make_test_store().await;
        let p = comp_path("comp");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, &sym("k"), b"old")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let mut cache = UserStateCache::new();
        cache.populate(vec![(sym("k"), b("old"))]);
        cache.use_state(sym("k"), b("ignored")).unwrap();
        cache.update_declared(&sym("k"), b("new")).unwrap();

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        cache.flush_to_db(&mut wtxn, &store, &p).await.unwrap();
        wtxn.into_inner().commit().unwrap();

        let entries = to_map(store.list_user_states(&p).await.unwrap());
        assert_eq!(entries[&sym("k")], b("new"));
    }

    #[tokio::test]
    async fn flush_set_reduction_empty_declared_removes_all() {
        // loaded = {a, b}; no use_state calls  →  all entries deleted.
        let (store, _dir) = make_test_store().await;
        let p = comp_path("comp");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, &sym("a"), b"a_val")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, &sym("b"), b"b_val")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let mut cache = UserStateCache::new();
        cache.populate(vec![(sym("a"), b("a_val")), (sym("b"), b("b_val"))]);
        // no use_state calls

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        cache.flush_to_db(&mut wtxn, &store, &p).await.unwrap();
        wtxn.into_inner().commit().unwrap();

        assert!(store.list_user_states(&p).await.unwrap().is_empty());
    }

    // --- flush_to_db: full-reprocess (is_loaded = false) ---------------------

    #[tokio::test]
    async fn flush_full_reprocess_writes_declared_only() {
        // DB has pre-existing entry; cache never populated; declared = {new_k}
        // delete_all wipes old entry, only new_k survives.
        let (store, _dir) = make_test_store().await;
        let p = comp_path("comp");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, &sym("old"), b"old_val")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let mut cache = UserStateCache::new(); // no populate
        cache.use_state(sym("new_k"), b("new_val")).unwrap();

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        cache.flush_to_db(&mut wtxn, &store, &p).await.unwrap();
        wtxn.into_inner().commit().unwrap();

        let entries = to_map(store.list_user_states(&p).await.unwrap());
        assert_eq!(entries.len(), 1);
        assert!(!entries.contains_key(&sym("old")));
        assert_eq!(entries[&sym("new_k")], b("new_val"));
    }

    #[tokio::test]
    async fn flush_full_reprocess_no_declared_clears_db() {
        // DB has pre-existing entry; cache never populated; no use_state calls
        // delete_all wipes everything, DB is empty.
        let (store, _dir) = make_test_store().await;
        let p = comp_path("comp");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, &sym("old"), b"old_val")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let cache = UserStateCache::new(); // no populate, no use_state

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        cache.flush_to_db(&mut wtxn, &store, &p).await.unwrap();
        wtxn.into_inner().commit().unwrap();

        assert!(store.list_user_states(&p).await.unwrap().is_empty());
    }

    // --- into_flush_plan: verifies plan output matches flush_to_db semantics --

    /// Helper: apply a UserStateFlushPlan through CommitPlan + AppStore::commit,
    /// which is the production path (as opposed to flush_to_db used above).
    async fn apply_plan_via_commit(store: &AppStore, path: &StablePath, cache: UserStateCache) {
        use crate::state_store::{CommitPlan, ExistenceReconciler};
        use futures::future::BoxFuture;

        let plan_data = cache.into_flush_plan();
        let plan = CommitPlan {
            new_tracking_info: None,
            target_owners_to_upsert: Vec::new(),
            target_owners_to_delete: Vec::new(),
            fn_memo_clear_all_first: false,
            fn_memo_writes: Vec::new(),
            fn_memo_deletes: Vec::new(),
            user_state_clear_all_first: plan_data.clear_all_first,
            user_state_writes: plan_data.writes,
            user_state_deletes: plan_data.deletes,
            child_path_set: None,
        };
        let reconciler: ExistenceReconciler =
            Box::new(|_wtxn| -> BoxFuture<'_, crate::prelude::Result<()>> {
                Box::pin(async { Ok(()) })
            });
        store.commit(path, plan, reconciler).await.unwrap();
    }

    #[tokio::test]
    async fn flush_plan_set_reduction_removes_dropped_keys() {
        // loaded = {a, b, c}; declared = {a, b} → c deleted via CommitPlan path.
        let (store, _dir) = make_test_store().await;
        let p = comp_path("comp");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, &sym("a"), b"a")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, &sym("b"), b"b")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, &sym("c"), b"c")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let mut cache = UserStateCache::new();
        cache.populate(vec![
            (sym("a"), b("a")),
            (sym("b"), b("b")),
            (sym("c"), b("c")),
        ]);
        cache.use_state(sym("a"), b("ignored")).unwrap();
        cache.use_state(sym("b"), b("ignored")).unwrap();
        // "c" not re-declared

        apply_plan_via_commit(&store, &p, cache).await;

        let entries = to_map(store.list_user_states(&p).await.unwrap());
        assert_eq!(entries.len(), 2);
        assert!(entries.contains_key(&sym("a")));
        assert!(entries.contains_key(&sym("b")));
        assert!(!entries.contains_key(&sym("c")));
    }

    #[tokio::test]
    async fn flush_plan_full_reprocess_wipes_and_writes_declared() {
        // DB has pre-existing entry; cache never populated (full_reprocess);
        // declared = {new_k} → old entry wiped, only new_k survives.
        let (store, _dir) = make_test_store().await;
        let p = comp_path("comp");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, &sym("old"), b"old")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let mut cache = UserStateCache::new(); // no populate
        cache.use_state(sym("new_k"), b("new_val")).unwrap();

        apply_plan_via_commit(&store, &p, cache).await;

        let entries = to_map(store.list_user_states(&p).await.unwrap());
        assert_eq!(entries.len(), 1);
        assert!(!entries.contains_key(&sym("old")));
        assert_eq!(entries[&sym("new_k")], b("new_val"));
    }
}
