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
//! let lm: LiveMap<String> = LiveMap::create(&ctx).await?; // inside the component tree
//! lm.declare_entry(&ctx, key, value)?;                    // producer: inside any component
//! ctx.mount_each_live(lm.clone(), process_entry).await?;  // consumer: one component per entry
//! ```

use std::collections::HashMap;
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
    ChildSlot, ChildTargetDef, StableKey, TargetAction, TargetActionSink, TargetChildInvalidation,
    TargetHandler, TargetReconcileOutput, TargetStateProvider, declare_target_state, mount_target,
    register_root_target_states_provider,
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

enum Change<V> {
    Upsert(String, V),
    Delete(String),
}

/// A change, numbered so a watcher can drop what its scan already covered.
struct SeqChange<V> {
    seq: u64,
    change: Change<V>,
}

struct LiveMapState<V> {
    entries: HashMap<String, V>,
    /// Number of changes applied so far.
    seq: u64,
    /// `seq` as of the latest scan.
    scan_seq: u64,
    /// The active `watch()`'s queue. Changes are queued only while a watcher
    /// is armed, so a map nobody watches does not accumulate them.
    watcher: Option<mpsc::UnboundedSender<SeqChange<V>>>,
}

struct LiveMapInner<V> {
    state: Mutex<LiveMapState<V>>,
}

impl<V: LiveMapValue> LiveMapInner<V> {
    fn lock(&self) -> std::sync::MutexGuard<'_, LiveMapState<V>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Apply one entry action to the in-memory map, emitting a change to the
    /// watcher only when the value actually changed (the `==` gate).
    fn apply(&self, key: String, value: Option<V>) {
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
struct WatcherGuard<'a, V: LiveMapValue>(&'a LiveMapInner<V>);

impl<V: LiveMapValue> Drop for WatcherGuard<'_, V> {
    fn drop(&mut self) {
        self.0.lock().watcher = None;
    }
}

/// An in-memory, keyed collection that is both a target (declare entries into it)
/// and a [`LiveMapView`] (consume it with [`Ctx::mount_each_live`]). See the
/// [module docs](self). Cheap to clone — the producer and consumer share state.
pub struct LiveMap<V> {
    inner: Arc<LiveMapInner<V>>,
    entry_provider: TargetStateProvider<V>,
}

impl<V> Clone for LiveMap<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            entry_provider: self.entry_provider.clone(),
        }
    }
}

impl<V: LiveMapValue> LiveMap<V> {
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
    pub fn declare_entry(&self, ctx: &Ctx, key: impl Into<String>, value: V) -> Result<()> {
        let key = key.into();
        declare_target_state(
            ctx,
            self.entry_provider
                .target_state(StableKey::Str(Arc::from(key.as_str())), value),
        )
    }
}

#[async_trait]
impl<V: LiveMapValue> LiveMapView<String, V> for LiveMap<V> {
    async fn scan(&self) -> Result<Vec<(String, V)>> {
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
impl<V: LiveMapValue> LiveMapFeed<String, V> for LiveMap<V> {
    async fn watch(&self, subscriber: LiveMapSubscriber<String, V>) -> Result<()> {
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ContainerAction {
    deleted: bool,
}

/// Existence marker for an entry tracking record (never read — its only job is to
/// be present so the engine can drive a delete when the producer stops declaring
/// the entry; change detection is done by `==` in the sink).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct EntryRecord;

#[derive(Clone, Serialize, Deserialize)]
struct EntryAction<V> {
    key: String,
    /// `Some` to upsert, `None` to remove.
    value: Option<V>,
}

struct ContainerHandler<V> {
    inner: Arc<LiveMapInner<V>>,
}

impl<V: LiveMapValue> TargetHandler<ContainerSpec> for ContainerHandler<V> {
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
                action: TargetAction::Update(ContainerAction { deleted: false }),
                sink: self.sink(),
                tracking_record: Some(ContainerRecord),
                child_invalidation: None,
            })),
            None => Ok(Some(TargetReconcileOutput {
                action: TargetAction::Delete(ContainerAction { deleted: true }),
                sink: self.sink(),
                tracking_record: None,
                child_invalidation: Some(TargetChildInvalidation::Destructive),
            })),
        }
    }
}

impl<V: LiveMapValue> ContainerHandler<V> {
    fn sink(&self) -> TargetActionSink<ContainerAction> {
        let inner = self.inner.clone();
        TargetActionSink::from_async_fn_with_children(
            move |actions: Vec<(TargetAction<ContainerAction>, Option<ChildSlot>)>| {
                let inner = inner.clone();
                async move {
                    for (action, child_slot) in actions {
                        let a = match action {
                            TargetAction::Create(a)
                            | TargetAction::Update(a)
                            | TargetAction::Delete(a) => a,
                        };
                        if a.deleted {
                            continue;
                        }
                        if let Some(slot) = child_slot {
                            slot.fulfill(ChildTargetDef::new::<V, _>(EntryHandler {
                                inner: inner.clone(),
                            }))?;
                        }
                    }
                    Ok(())
                }
            },
        )
    }
}

struct EntryHandler<V> {
    inner: Arc<LiveMapInner<V>>,
}

impl<V: LiveMapValue> TargetHandler<V> for EntryHandler<V> {
    type TrackingRecord = EntryRecord;
    type Action = EntryAction<V>;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<V>,
        _prev: Vec<EntryRecord>,
        _prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<EntryAction<V>, EntryRecord>>> {
        let key = match &key {
            StableKey::Str(s) | StableKey::Symbol(s) => s.to_string(),
            other => return Err(Error::engine(format!("unsupported LiveMap key: {other:?}"))),
        };
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

impl<V: LiveMapValue> EntryHandler<V> {
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
                    inner.apply(a.key, a.value);
                }
                Ok(())
            }
        })
    }
}
