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
    ChildTargetDef, StableKey, TargetAction, TargetActionSink, TargetChildInvalidation,
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

struct LiveMapInner<V> {
    entries: Mutex<HashMap<String, V>>,
    /// Always live, so producer changes queue even before a consumer attaches.
    sender: mpsc::UnboundedSender<Change<V>>,
    /// Taken by the single active `watch()`.
    receiver: Mutex<Option<mpsc::UnboundedReceiver<Change<V>>>>,
}

impl<V: LiveMapValue> LiveMapInner<V> {
    /// Apply one entry action to the in-memory map, emitting a change to the
    /// watcher only when the value actually changed (the `==` gate).
    fn apply(&self, key: String, value: Option<V>) {
        let mut entries = self.entries.lock().unwrap();
        match value {
            Some(v) => {
                if entries.get(&key) != Some(&v) {
                    entries.insert(key.clone(), v.clone());
                    let _ = self.sender.send(Change::Upsert(key, v));
                }
            }
            None => {
                if entries.remove(&key).is_some() {
                    let _ = self.sender.send(Change::Delete(key));
                }
            }
        }
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
        let (sender, receiver) = mpsc::unbounded_channel();
        let inner = Arc::new(LiveMapInner {
            entries: Mutex::new(HashMap::new()),
            sender,
            receiver: Mutex::new(Some(receiver)),
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
        let entries = self.inner.entries.lock().unwrap();
        Ok(entries.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }
}

#[async_trait]
impl<V: LiveMapValue> LiveMapFeed<String, V> for LiveMap<V> {
    async fn watch(&self, subscriber: LiveMapSubscriber<String, V>) -> Result<()> {
        let mut rx = self
            .inner
            .receiver
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| Error::engine("LiveMap supports a single active watch() at a time."))?;
        // The framework already ran the catch-up scan + mark_ready before calling
        // `watch`. Drain incremental changes from here on. Changes that occurred
        // before the scan are replayed too; the consumer's update/delete is
        // idempotent, so the redundancy is harmless.
        while let Some(change) = rx.recv().await {
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
            move |actions: Vec<TargetAction<ContainerAction>>| {
                let inner = inner.clone();
                async move {
                    let mut out: Vec<Option<ChildTargetDef>> = Vec::with_capacity(actions.len());
                    for action in actions {
                        let a = match action {
                            TargetAction::Create(a)
                            | TargetAction::Update(a)
                            | TargetAction::Delete(a) => a,
                        };
                        if a.deleted {
                            out.push(None);
                        } else {
                            out.push(Some(ChildTargetDef::new::<V, _>(EntryHandler {
                                inner: inner.clone(),
                            })));
                        }
                    }
                    Ok(out)
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
