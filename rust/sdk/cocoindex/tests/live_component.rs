//! SDK live-component + exception-handler tests.
//!
//! Mirrors the Python `test_live_component.py` / `test_exception_handlers.py`
//! families against the Rust SDK surface (`Ctx::mount_live`,
//! `Ctx::mount_live_with_handler`, `Ctx::mount_each_live`,
//! `LiveComponentOperator`, `LiveMapView` / `LiveMapSubscriber`).
//!
//!   cargo test -p cocoindex --test live_component

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cocoindex::{
    App, Ctx, Error, LiveComponent, LiveComponentOperator, LiveMapFeed, LiveMapSubscriber,
    LiveMapView, MountKind, Result, StableKey, TargetAction, TargetActionSink, TargetHandler,
    TargetReconcileOutput, UpdateOptions, async_trait, declare_target_state,
    register_root_target_states_provider,
};
use tokio::sync::Notify;

// ---------------------------------------------------------------------------
// Shared helpers (mirrors tests/target_state.rs)
// ---------------------------------------------------------------------------

type Log = Arc<Mutex<Vec<String>>>;

fn new_log() -> Log {
    Arc::new(Mutex::new(Vec::new()))
}

fn drain_sorted(log: &Log) -> Vec<String> {
    let mut v = std::mem::take(&mut *log.lock().unwrap());
    v.sort();
    v
}

fn key_str(key: &StableKey) -> String {
    match key {
        StableKey::Str(s) | StableKey::Symbol(s) => s.to_string(),
        StableKey::Int(i) => i.to_string(),
        other => format!("{other:?}"),
    }
}

async fn temp_app(name: &str) -> (App, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder(name)
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap();
    (app, dir)
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct RowAction {
    key: String,
    value: Option<String>,
}

fn recording_sink(log: Log) -> TargetActionSink<RowAction> {
    TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<RowAction>>| {
        let log = log.clone();
        async move {
            let mut log = log.lock().unwrap();
            for action in actions {
                let (verb, row) = match action {
                    TargetAction::Create(r) => ("create", r),
                    TargetAction::Update(r) => ("update", r),
                    TargetAction::Delete(r) => ("delete", r),
                };
                log.push(match row.value {
                    Some(v) => format!("{verb} {}={}", row.key, v),
                    None => format!("{verb} {}", row.key),
                });
            }
            Ok(())
        }
    })
}

#[derive(Clone)]
struct RowHandler {
    sink: TargetActionSink<RowAction>,
}

impl TargetHandler<String> for RowHandler {
    type TrackingRecord = String;
    type Action = RowAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<String>,
        prev: Vec<String>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<RowAction, String>>> {
        let k = key_str(&key);
        match desired {
            Some(value) => {
                let unchanged =
                    !prev_may_be_missing && !prev.is_empty() && prev.iter().all(|p| *p == value);
                if unchanged {
                    return Ok(None);
                }
                let row = RowAction {
                    key: k,
                    value: Some(value.clone()),
                };
                let action = if prev.is_empty() {
                    TargetAction::Create(row)
                } else {
                    TargetAction::Update(row)
                };
                Ok(Some(TargetReconcileOutput {
                    action,
                    sink: self.sink.clone(),
                    tracking_record: Some(value),
                    child_invalidation: None,
                }))
            }
            None => {
                if prev.is_empty() && !prev_may_be_missing {
                    return Ok(None);
                }
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Delete(RowAction {
                        key: k,
                        value: None,
                    }),
                    sink: self.sink.clone(),
                    tracking_record: None,
                    child_invalidation: None,
                }))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Basic full-pass behavior
// ---------------------------------------------------------------------------

/// A component that counts its `process()` invocations.
struct Counter {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl LiveComponent for Counter {
    async fn process(&self, _ctx: Ctx) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn mount_live_catch_up_runs_process_once() {
    let (app, _dir) = temp_app("live_catchup").await;
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_update = calls.clone();

    app.update(move |ctx| {
        let calls = calls_for_update.clone();
        async move { ctx.mount_live(&"comp", Counter { calls }).await }
    })
    .await
    .unwrap();

    // Default process_live → one full pass, then mark_ready (terminates in
    // catch-up mode).
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// A component whose `process()` declares a fixed set of rows.
struct Declarer {
    sink: TargetActionSink<RowAction>,
    rows: Vec<(&'static str, &'static str)>,
}

#[async_trait]
impl LiveComponent for Declarer {
    async fn process(&self, ctx: Ctx) -> Result<()> {
        let provider = register_root_target_states_provider(
            &ctx,
            "live/rows",
            RowHandler {
                sink: self.sink.clone(),
            },
        )?;
        for (k, v) in &self.rows {
            declare_target_state(&ctx, provider.target_state(*k, v.to_string()))?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn mount_live_full_pass_declares_target_states() {
    let (app, _dir) = temp_app("live_declare").await;
    let log = new_log();
    let sink = recording_sink(log.clone());

    app.update(move |ctx| {
        let sink = sink.clone();
        async move {
            ctx.mount_live(
                &"comp",
                Declarer {
                    sink,
                    rows: vec![("a", "v1"), ("b", "v1")],
                },
            )
            .await
        }
    })
    .await
    .unwrap();

    assert_eq!(drain_sorted(&log), vec!["create a=v1", "create b=v1"]);
}

// ---------------------------------------------------------------------------
// Incremental update / delete from process_live
// ---------------------------------------------------------------------------

/// Live component whose `process()` mounts one child per source row (each
/// declaring a row into a shared, ancestor-registered provider). After the
/// initial full pass, `process_live` directly deletes one child via the
/// operator — mirroring Python's `_IncrementalDeleteDirectLiveComponent`, where
/// the target is registered globally so the delete path can reconcile it.
struct DirectDelete {
    provider: cocoindex::TargetStateProvider<String>,
    source: Vec<(&'static str, &'static str)>,
    done: Arc<Notify>,
}

#[async_trait]
impl LiveComponent for DirectDelete {
    async fn process(&self, ctx: Ctx) -> Result<()> {
        let provider = self.provider.clone();
        ctx.mount_each(
            self.source.clone(),
            |(k, _v): &(&'static str, &'static str)| (*k).to_string(),
            move |child, (k, v)| {
                let provider = provider.clone();
                async move {
                    declare_target_state(&child, provider.target_state(k, v.to_string()))?;
                    Ok::<(), Error>(())
                }
            },
        )
        .await?;
        Ok(())
    }

    async fn process_live(&self, operator: LiveComponentOperator) -> Result<()> {
        operator.update_full().await?;
        operator.mark_ready().await;
        // Directly delete the child mounted for key "a"; its target state is GC'd.
        operator.delete("a").await?;
        self.done.notify_waiters();
        Ok(())
    }
}

#[tokio::test]
async fn mount_live_incremental_delete_removes_child_state() {
    let (app, _dir) = temp_app("live_incremental").await;
    let log = new_log();
    let sink = recording_sink(log.clone());
    let done = Arc::new(Notify::new());
    let done_for_update = done.clone();

    let handle = app
        .start_update_with_options(
            UpdateOptions {
                full_reprocess: false,
                live: true,
            },
            move |ctx| {
                let sink = sink.clone();
                let done = done_for_update.clone();
                async move {
                    // Register the provider at the root so it's captured into the
                    // live component's provider set and available at delete time.
                    let provider = register_root_target_states_provider(
                        &ctx,
                        "live/items",
                        RowHandler { sink },
                    )?;
                    ctx.mount_live(
                        &"comp",
                        DirectDelete {
                            provider,
                            source: vec![("a", "va"), ("b", "vb")],
                            done,
                        },
                    )
                    .await
                }
            },
        )
        .unwrap();

    tokio::time::timeout(Duration::from_secs(10), done.notified())
        .await
        .expect("incremental delete did not complete");

    assert_eq!(
        drain_sorted(&log),
        vec!["create a=va", "create b=vb", "delete a"]
    );

    let _ = app.drop_state().await;
    let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
}

// ---------------------------------------------------------------------------
// Exception handlers
// ---------------------------------------------------------------------------

/// A component whose `process()` always fails.
struct Failing;

#[async_trait]
impl LiveComponent for Failing {
    async fn process(&self, _ctx: Ctx) -> Result<()> {
        Err(Error::engine("process boom"))
    }
}

#[tokio::test]
async fn mount_live_exception_handler_swallows_failure() {
    let (app, _dir) = temp_app("live_handler_swallow").await;
    let seen: Arc<Mutex<Vec<MountKind>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_for_update = seen.clone();

    // Handler returns Ok → swallows → mount succeeds.
    app.update(move |ctx| {
        let seen = seen_for_update.clone();
        async move {
            ctx.mount_live_with_handler(&"comp", Failing, move |_err, exc| {
                seen.lock().unwrap().push(exc.mount_kind);
                Ok(())
            })
            .await
        }
    })
    .await
    .expect("swallowing handler should make the mount succeed");

    let seen = seen.lock().unwrap();
    assert_eq!(*seen, vec![MountKind::UpdateFull]);
}

#[tokio::test]
async fn mount_live_exception_handler_propagates_failure() {
    let (app, _dir) = temp_app("live_handler_propagate").await;

    // Handler returns Err → propagates → mount fails.
    let result = app
        .update(move |ctx| async move {
            ctx.mount_live_with_handler(&"comp", Failing, move |err, _exc| {
                // Re-raise: propagate the failure.
                Err(Error::engine(format!("handled: {err}")))
            })
            .await
        })
        .await;

    assert!(
        result.is_err(),
        "propagating handler should surface the failure"
    );
}

// ---------------------------------------------------------------------------
// mount_each_live over a LiveMapView / LiveMapFeed
// ---------------------------------------------------------------------------

/// In-memory view: `scan()` returns a fixed snapshot; `watch()` is unused.
struct MemView {
    items: Vec<(String, i64)>,
}

#[async_trait]
impl LiveMapFeed<String, i64> for MemView {
    async fn watch(&self, _subscriber: LiveMapSubscriber<String, i64>) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl LiveMapView<String, i64> for MemView {
    async fn scan(&self) -> Result<Vec<(String, i64)>> {
        Ok(self.items.clone())
    }
}

#[tokio::test]
async fn mount_each_live_catch_up_scans_all_items() {
    let (app, _dir) = temp_app("each_live_catchup").await;
    let processed: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
    let processed_for_update = processed.clone();

    app.update(move |ctx| {
        let processed = processed_for_update.clone();
        async move {
            let view = MemView {
                items: vec![
                    ("a".to_string(), 1),
                    ("b".to_string(), 2),
                    ("c".to_string(), 3),
                ],
            };
            ctx.mount_each_live(&"each", view, move |_ctx, value: i64| {
                let processed = processed.clone();
                async move {
                    processed.lock().unwrap().push(value);
                    Ok(())
                }
            })
            .await
        }
    })
    .await
    .unwrap();

    let mut got = processed.lock().unwrap().clone();
    got.sort();
    assert_eq!(got, vec![1, 2, 3]);
}

/// Watch-only stream: emits a fixed set of incremental updates after readiness.
struct StreamFeed {
    items: Vec<(String, i64)>,
    done: Arc<Notify>,
}

#[async_trait]
impl LiveMapFeed<String, i64> for StreamFeed {
    async fn watch(&self, subscriber: LiveMapSubscriber<String, i64>) -> Result<()> {
        subscriber.mark_ready().await;
        for (k, v) in &self.items {
            subscriber.update(k.clone(), *v).await?;
        }
        self.done.notify_waiters();
        Ok(())
    }
}

#[async_trait]
impl LiveMapView<String, i64> for StreamFeed {
    async fn scan(&self) -> Result<Vec<(String, i64)>> {
        // Empty initial snapshot; everything arrives via watch().
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn mount_each_live_streams_incremental_updates() {
    let (app, _dir) = temp_app("each_live_stream").await;
    let processed: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
    let processed_for_update = processed.clone();
    let done = Arc::new(Notify::new());
    let done_for_update = done.clone();

    let handle = app
        .start_update_with_options(
            UpdateOptions {
                full_reprocess: false,
                live: true,
            },
            move |ctx| {
                let processed = processed_for_update.clone();
                let done = done_for_update.clone();
                async move {
                    let feed = StreamFeed {
                        items: vec![("x".to_string(), 10), ("y".to_string(), 20)],
                        done,
                    };
                    ctx.mount_each_live(&"each", feed, move |_ctx, value: i64| {
                        let processed = processed.clone();
                        async move {
                            processed.lock().unwrap().push(value);
                            Ok(())
                        }
                    })
                    .await
                }
            },
        )
        .unwrap();

    tokio::time::timeout(Duration::from_secs(10), done.notified())
        .await
        .expect("stream feed did not finish emitting");

    let mut got = processed.lock().unwrap().clone();
    got.sort();
    assert_eq!(got, vec![10, 20]);

    let _ = app.drop_state().await;
    let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
}
