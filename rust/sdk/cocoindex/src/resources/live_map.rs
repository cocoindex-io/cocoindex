//! In-memory live key-value map.
//!
//! [`LiveMap`] bridges live data-*producing* logic and live data-*consuming*
//! logic within a single CocoIndex session. The producing side declares
//! `(key, value)` entries as **target states** via [`LiveMap::declare_entry`];
//! the consuming side reads it as a [`LiveMapView`] through
//! [`Ctx::mount_each_live`](crate::Ctx::mount_each_live). All data is held in an
//! in-process map that the engine keeps in sync through normal target-state
//! ownership, and that same map is exposed as a live source for downstream
//! components.
//!
//! Designed for live mode (`UpdateOptions::live`). An entry exists as long as
//! some live component declares it; when its declaring component stops declaring
//! it (or disappears), the entry is removed.
//!
//! ```ignore
//! let lm: LiveMap<String, String> = LiveMap::create(&ctx).await?; // inside the component tree
//! lm.declare_entry(&ctx, key, value)?;                            // producer: inside any component
//! ctx.mount_each_live(&"entries", lm.clone(), process_entry).await?; // consumer: one component per entry
//! ```
//!
//! The key type `K` is any [`LiveMapKey`]: a stable key (`String`, `i64`,
//! `u64`, or a [`StableKey`] itself), as Python's `LiveMap[K, V]` takes any
//! `StableKey`.

use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::live_component::{LiveMapFeed, LiveMapSubscriber, LiveMapView};
use crate::target_state::{
    ChildTargetDef, IntoStableKey, SinkCache, StableKey, TargetAction, TargetActionSink,
    TargetChildInvalidation, TargetHandler, TargetReconcileOutput, TargetStateProvider,
    declare_target_state, mount_target, register_root_target_states_provider,
};

/// Process-wide counter giving each [`LiveMap`] a distinct container provider
/// name. Like Python's per-instance UUID, the name is session-scoped — LiveMap
/// is an in-memory, live-mode primitive, not cross-run persistent state.
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// The bound a [`LiveMap`] value must satisfy: it is a target-state value
/// (serializable), is cloned into the in-memory map and the consumer, and is
/// compared by `==` to suppress no-op change notifications. Auto-implemented for
/// every qualifying type — you never implement it by hand.
pub trait LiveMapValue:
    Serialize + DeserializeOwned + Clone + PartialEq + Send + Sync + 'static
{
}
impl<V: Serialize + DeserializeOwned + Clone + PartialEq + Send + Sync + 'static> LiveMapValue
    for V
{
}

/// The bound a [`LiveMap`] key must satisfy: it becomes both the entry's
/// target-state key and the consumer's per-entry component subpath, so it is a
/// stable key ([`IntoStableKey`]) that the map can read back from the engine
/// ([`from_stable_key`](LiveMapKey::from_stable_key)). Implemented for
/// `String`, `i64`, `u64` and [`StableKey`].
pub trait LiveMapKey: IntoStableKey + Clone + Display + Eq + Hash + Send + Sync + 'static {
    /// The key the engine handed back, decoded; `None` if it is not a key this
    /// type produces (a bug, reported as an error by the map).
    fn from_stable_key(key: &StableKey) -> Option<Self>;
}

impl LiveMapKey for String {
    fn from_stable_key(key: &StableKey) -> Option<Self> {
        match key {
            StableKey::Str(s) | StableKey::Symbol(s) => Some(s.to_string()),
            _ => None,
        }
    }
}

impl LiveMapKey for i64 {
    fn from_stable_key(key: &StableKey) -> Option<Self> {
        match key {
            StableKey::Int(i) => Some(*i),
            _ => None,
        }
    }
}

impl LiveMapKey for u64 {
    fn from_stable_key(key: &StableKey) -> Option<Self> {
        // The inverse of `IntoStableKey for u64`: `[Symbol("u64"), Str(n)]`.
        match key {
            StableKey::Array(parts) => match parts.as_ref() {
                [StableKey::Symbol(tag), StableKey::Str(n)] if tag.as_ref() == "u64" => {
                    n.parse().ok()
                }
                _ => None,
            },
            _ => None,
        }
    }
}

impl LiveMapKey for StableKey {
    fn from_stable_key(key: &StableKey) -> Option<Self> {
        Some(key.clone())
    }
}

enum Change<K, V> {
    Upsert(K, V),
    Delete(K),
}

/// A change, numbered so a watcher can drop what its scan already covered.
struct SeqChange<K, V> {
    seq: u64,
    change: Change<K, V>,
}

struct LiveMapState<K, V> {
    entries: HashMap<K, V>,
    /// Number of changes applied so far.
    seq: u64,
    /// `seq` as of the latest scan.
    scan_seq: u64,
    /// The active `watch()`'s queue. Changes are queued only while a watcher
    /// is armed, so a map nobody watches does not accumulate them.
    watcher: Option<mpsc::UnboundedSender<SeqChange<K, V>>>,
}

struct LiveMapInner<K, V> {
    state: Mutex<LiveMapState<K, V>>,
}

impl<K: LiveMapKey, V: LiveMapValue> LiveMapInner<K, V> {
    fn lock(&self) -> std::sync::MutexGuard<'_, LiveMapState<K, V>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Apply one entry action to the in-memory map, emitting a change to the
    /// watcher only when the value actually changed (the `==` gate).
    fn apply(&self, key: K, value: Option<V>) {
        let mut state = self.lock();
        let change = match value {
            Some(v) => {
                if state.entries.get(&key) == Some(&v) {
                    return;
                }
                state.entries.insert(key.clone(), v.clone());
                Change::Upsert(key, v)
            }
            None => {
                if state.entries.remove(&key).is_none() {
                    return;
                }
                Change::Delete(key)
            }
        };
        state.seq += 1;
        let seq = state.seq;
        if let Some(watcher) = &state.watcher {
            let _ = watcher.send(SeqChange { seq, change });
        }
    }
}

/// Disarms the watcher when `watch()` ends (returns, fails or is cancelled),
/// so the map can be watched again, like Python's `finally` reset.
struct WatcherGuard<'a, K: LiveMapKey, V: LiveMapValue>(&'a LiveMapInner<K, V>);

impl<K: LiveMapKey, V: LiveMapValue> Drop for WatcherGuard<'_, K, V> {
    fn drop(&mut self) {
        self.0.lock().watcher = None;
    }
}

/// An in-memory, keyed collection that is both a target (declare entries into it)
/// and a [`LiveMapView`] (consume it with [`Ctx::mount_each_live`]). See the
/// [module docs](self). Cheap to clone — the producer and consumer share state.
pub struct LiveMap<K, V> {
    inner: Arc<LiveMapInner<K, V>>,
    entry_provider: TargetStateProvider<V>,
}

impl<K, V> Clone for LiveMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            entry_provider: self.entry_provider.clone(),
        }
    }
}

impl<K: LiveMapKey, V: LiveMapValue> LiveMap<K, V> {
    /// Create a LiveMap and mount its backing target. Call inside a component
    /// context (an `App::update`/`run` closure or a mounted component).
    pub async fn create(ctx: &Ctx) -> Result<Self> {
        let inner = Arc::new(LiveMapInner {
            state: Mutex::new(LiveMapState {
                entries: HashMap::new(),
                seq: 0,
                scan_seq: 0,
                watcher: None,
            }),
        });
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let container_provider = register_root_target_states_provider(
            ctx,
            format!("cocoindex/livemap/{id}"),
            ContainerHandler {
                inner: inner.clone(),
                sink: SinkCache::default(),
            },
        )?;
        let container = container_provider.target_state("container", ContainerSpec);
        let entry_provider = mount_target::<ContainerSpec, V>(ctx, container).await?;
        Ok(Self {
            inner,
            entry_provider,
        })
    }

    /// Declare an entry, owned by the calling component. Call inside a component
    /// context. The entry lives until its declaring component stops declaring it.
    pub fn declare_entry(&self, ctx: &Ctx, key: K, value: V) -> Result<()> {
        declare_target_state(ctx, self.entry_provider.target_state(key, value))
    }
}

#[async_trait]
impl<K: LiveMapKey, V: LiveMapValue> LiveMapView<K, V> for LiveMap<K, V> {
    async fn scan(&self) -> Result<Vec<(K, V)>> {
        let mut state = self.inner.lock();
        state.scan_seq = state.seq;
        Ok(state
            .entries
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
}

#[async_trait]
impl<K: LiveMapKey, V: LiveMapValue> LiveMapFeed<K, V> for LiveMap<K, V> {
    async fn watch(&self, subscriber: LiveMapSubscriber<K, V>) -> Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let changed_since_scan = {
            let mut state = self.inner.lock();
            if state.watcher.is_some() {
                return Err(Error::engine(
                    "LiveMap supports a single active watch() at a time.",
                ));
            }
            state.watcher = Some(tx);
            state.seq != state.scan_seq
        };
        let _guard = WatcherGuard(self.inner.as_ref());
        // The framework scanned before calling `watch`. Changes applied between
        // that scan and arming the queue were not queued, so rescan to cover
        // them; changes queued since arming that the scan already reflects are
        // skipped below by sequence number.
        if changed_since_scan {
            subscriber.update_all().await?;
        }
        let snapshot_seq = self.inner.lock().scan_seq;
        while let Some(SeqChange { seq, change }) = rx.recv().await {
            if seq <= snapshot_seq {
                continue;
            }
            match change {
                Change::Upsert(key, value) => subscriber.update(key, value).await?,
                Change::Delete(key) => subscriber.delete(key).await?,
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Producer-side target machinery: a per-map container whose children are entries.
// ---------------------------------------------------------------------------

/// Marker value for the per-map container target state.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ContainerSpec;

/// Existence marker for the container tracking record.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ContainerRecord;

/// The container itself has nothing to apply: its entries live in the map.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ContainerAction;

/// Existence marker for an entry tracking record (never read — its only job is to
/// be present so the engine can drive a delete when the producer stops declaring
/// the entry; change detection is done by `==` in the sink).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct EntryRecord;

/// An action travels through the engine serialized, so it carries the engine's
/// [`StableKey`]; the sink decodes it to `K` with [`LiveMapKey::from_stable_key`].
#[derive(Clone, Serialize, Deserialize)]
struct EntryAction<V> {
    key: StableKey,
    /// `Some` to upsert, `None` to remove.
    value: Option<V>,
}

struct ContainerHandler<K, V> {
    inner: Arc<LiveMapInner<K, V>>,
    sink: SinkCache<ContainerAction>,
}

impl<K: LiveMapKey, V: LiveMapValue> TargetHandler<ContainerSpec> for ContainerHandler<K, V> {
    type TrackingRecord = ContainerRecord;
    type Action = ContainerAction;

    fn reconcile(
        &self,
        _key: StableKey,
        desired: Option<ContainerSpec>,
        _prev: Vec<ContainerRecord>,
        _prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<ContainerAction, ContainerRecord>>> {
        match desired {
            Some(_) => Ok(Some(TargetReconcileOutput {
                action: TargetAction::Update(ContainerAction),
                sink: self.sink(),
                tracking_record: Some(ContainerRecord),
                child_invalidation: None,
            })),
            None => Ok(Some(TargetReconcileOutput {
                action: TargetAction::Delete(ContainerAction),
                sink: self.sink(),
                tracking_record: None,
                child_invalidation: Some(TargetChildInvalidation::Destructive),
            })),
        }
    }
}

impl<K: LiveMapKey, V: LiveMapValue> ContainerHandler<K, V> {
    fn sink(&self) -> TargetActionSink<ContainerAction> {
        self.sink.get(|| {
            let inner = self.inner.clone();
            TargetActionSink::from_async_fn_with_child_handler(
                |_actions| async { Ok(()) },
                move |_: &ContainerAction| {
                    Ok(ChildTargetDef::new::<V, _>(EntryHandler {
                        inner: inner.clone(),
                    }))
                },
            )
        })
    }
}

struct EntryHandler<K, V> {
    inner: Arc<LiveMapInner<K, V>>,
}

impl<K: LiveMapKey, V: LiveMapValue> TargetHandler<V> for EntryHandler<K, V> {
    type TrackingRecord = EntryRecord;
    type Action = EntryAction<V>;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<V>,
        _prev: Vec<EntryRecord>,
        _prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<EntryAction<V>, EntryRecord>>> {
        // Fail here, before anything is applied, if the key is not a `K`.
        decode_key::<K>(&key)?;
        // Never skip: applying to the in-memory map is cheap and the `==` gate in
        // the sink decides whether to notify the consumer.
        match desired {
            Some(value) => Ok(Some(TargetReconcileOutput {
                action: TargetAction::Update(EntryAction {
                    key,
                    value: Some(value),
                }),
                sink: self.sink(),
                tracking_record: Some(EntryRecord),
                child_invalidation: None,
            })),
            None => Ok(Some(TargetReconcileOutput {
                action: TargetAction::Delete(EntryAction { key, value: None }),
                sink: self.sink(),
                tracking_record: None,
                child_invalidation: None,
            })),
        }
    }
}

impl<K: LiveMapKey, V: LiveMapValue> EntryHandler<K, V> {
    fn sink(&self) -> TargetActionSink<EntryAction<V>> {
        let inner = self.inner.clone();
        TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<EntryAction<V>>>| {
            let inner = inner.clone();
            async move {
                for action in actions {
                    let a = match action {
                        TargetAction::Create(a)
                        | TargetAction::Update(a)
                        | TargetAction::Delete(a) => a,
                    };
                    inner.apply(decode_key::<K>(&a.key)?, a.value);
                }
                Ok(())
            }
        })
    }
}

fn decode_key<K: LiveMapKey>(key: &StableKey) -> Result<K> {
    K::from_stable_key(key).ok_or_else(|| {
        Error::engine(format!(
            "LiveMap entry key {key:?} is not a {}",
            std::any::type_name::<K>()
        ))
    })
}
