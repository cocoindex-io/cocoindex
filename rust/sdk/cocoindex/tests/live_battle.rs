//! Adversarial live-mode tests driven straight through a
//! [`LiveComponentOperator`]: update storms on one key and on many, rapid
//! create-delete-create, incremental work racing a full pass, a child that
//! fails and recovers, readiness and `mark_ready` semantics, `auto_refresh`
//! pacing, and shutdown while work is in flight.
//!
//! The test body drives the operator itself: the component's `process_live`
//! hands it out through a channel and then waits, so each test can issue
//! exactly the sequence of operations it wants to probe.
//!
//!   cargo test -p cocoindex --test live_battle

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cocoindex::{
    App, Ctx, Error, LiveComponent, LiveComponentOperator, MountKind, Result, StableKey,
    TargetAction, TargetActionSink, TargetHandler, TargetReconcileOutput, TargetStateProvider,
    UpdateOptions, async_trait, declare_target_state, register_root_target_states_provider,
};
use tokio::sync::{Notify, mpsc};

// ---------------------------------------------------------------------------
// A target that keeps the rows it was told to write
// ---------------------------------------------------------------------------

/// The rows the sink holds, and how many actions of each kind it applied.
#[derive(Default, Debug)]
struct Table {
    rows: BTreeMap<String, String>,
    creates: usize,
    updates: usize,
    deletes: usize,
}

type SharedTable = Arc<Mutex<Table>>;

/// What the exception handler saw: kind, background, failing path, message.
type Reports = Arc<Mutex<Vec<(MountKind, bool, String, String)>>>;

fn key_str(key: &StableKey) -> String {
    match key {
        StableKey::Str(s) | StableKey::Symbol(s) => s.to_string(),
        other => format!("{other:?}"),
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Row {
    key: String,
    value: Option<String>,
}

#[derive(Clone)]
struct RowHandler {
    sink: TargetActionSink<Row>,
}

fn row_handler(table: SharedTable) -> RowHandler {
    RowHandler {
        sink: TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<Row>>| {
            let table = table.clone();
            async move {
                let mut table = table.lock().unwrap();
                for action in actions {
                    match action {
                        TargetAction::Create(row) => {
                            table.creates += 1;
                            table.rows.insert(row.key, row.value.unwrap_or_default());
                        }
                        TargetAction::Update(row) => {
                            table.updates += 1;
                            table.rows.insert(row.key, row.value.unwrap_or_default());
                        }
                        TargetAction::Delete(row) => {
                            table.deletes += 1;
                            table.rows.remove(&row.key);
                        }
                    }
                }
                Ok(())
            }
        }),
    }
}

impl TargetHandler<String> for RowHandler {
    type TrackingRecord = String;
    type Action = Row;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<String>,
        prev: Vec<String>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<Row, String>>> {
        let key = key_str(&key);
        match desired {
            Some(value) => {
                if !prev_may_be_missing && !prev.is_empty() && prev.iter().all(|p| *p == value) {
                    return Ok(None);
                }
                let row = Row {
                    key,
                    value: Some(value.clone()),
                };
                Ok(Some(TargetReconcileOutput {
                    action: if prev.is_empty() {
                        TargetAction::Create(row)
                    } else {
                        TargetAction::Update(row)
                    },
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
                    action: TargetAction::Delete(Row { key, value: None }),
                    sink: self.sink.clone(),
                    tracking_record: None,
                    child_invalidation: None,
                }))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// A live component the test drives from outside
// ---------------------------------------------------------------------------

/// The source a full pass declares: one row per entry.
type Source = Arc<Mutex<BTreeMap<String, String>>>;

/// A hook the full pass awaits before declaring, so a test can hold a pass
/// open while it races incremental work against it.
#[derive(Default)]
struct Gate {
    entered: Notify,
    release: Notify,
    armed: AtomicBool,
    /// When set, the next full pass fails (after declaring nothing).
    fail: AtomicBool,
}

/// A live component whose full pass declares `source` and whose
/// `process_live` hands its operator to the test, then waits to be cancelled.
/// It marks itself ready only when `ready_on_start` is set; otherwise the test
/// decides when.
struct Driven {
    provider: TargetStateProvider<String>,
    source: Source,
    gate: Arc<Gate>,
    full_passes: Arc<AtomicUsize>,
    operator_tx: mpsc::UnboundedSender<Arc<LiveComponentOperator>>,
    ready_on_start: bool,
}

#[async_trait]
impl LiveComponent for Driven {
    async fn process(&self, ctx: Ctx) -> Result<()> {
        self.full_passes.fetch_add(1, Ordering::SeqCst);
        if self.gate.armed.load(Ordering::SeqCst) {
            self.gate.entered.notify_one();
            self.gate.release.notified().await;
        }
        if self.gate.fail.swap(false, Ordering::SeqCst) {
            return Err(Error::engine("full pass failed"));
        }
        let entries: Vec<(String, String)> = self
            .source
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let provider = self.provider.clone();
        ctx.mount_each(
            entries,
            |(k, _): &(String, String)| k.clone(),
            move |child, (k, v)| {
                let provider = provider.clone();
                async move { declare_target_state(&child, provider.target_state(k, v)) }
            },
        )
        .await?;
        Ok(())
    }

    async fn process_live(&self, operator: LiveComponentOperator) -> Result<()> {
        operator.update_full().await?;
        if self.ready_on_start {
            operator.mark_ready().await;
        }
        let operator = Arc::new(operator);
        let _ = self.operator_tx.send(operator.clone());
        std::future::pending::<()>().await;
        Ok(())
    }
}

/// What a running driven app exposes to its test.
struct Harness {
    app: App,
    _dir: tempfile::TempDir,
    table: SharedTable,
    source: Source,
    gate: Arc<Gate>,
    full_passes: Arc<AtomicUsize>,
    operator: Arc<LiveComponentOperator>,
    provider: TargetStateProvider<String>,
    handle: Option<cocoindex::UpdateHandle<()>>,
    reports: Reports,
}

impl Harness {
    /// Start a live update with one driven component under `comp` and wait for
    /// its operator. Background failures go to a handler that records and
    /// swallows them.
    async fn start(name: &str, initial: &[(&str, &str)]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let app = App::builder(name)
            .db_path(dir.path().join(".cocoindex_db"))
            .build()
            .await
            .unwrap();
        let table: SharedTable = Arc::default();
        let source: Source = Arc::new(Mutex::new(
            initial
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        ));
        let gate = Arc::new(Gate::default());
        let full_passes = Arc::new(AtomicUsize::new(0));
        let reports: Reports = Arc::default();
        let (operator_tx, mut operator_rx) = mpsc::unbounded_channel();
        let (provider_tx, mut provider_rx) = mpsc::unbounded_channel();

        let handle = {
            let table = table.clone();
            let source = source.clone();
            let gate = gate.clone();
            let full_passes = full_passes.clone();
            let reports = reports.clone();
            app.start_update_with_options(
                UpdateOptions {
                    live: true,
                    ..UpdateOptions::default()
                },
                move |ctx| async move {
                    let provider = register_root_target_states_provider(
                        &ctx,
                        "battle/rows",
                        row_handler(table),
                    )?;
                    let _ = provider_tx.send(provider.clone());
                    ctx.mount_live_with_handler(
                        &"comp",
                        Driven {
                            provider,
                            source,
                            gate,
                            full_passes,
                            operator_tx,
                            ready_on_start: true,
                        },
                        move |err, exc| {
                            reports.lock().unwrap().push((
                                exc.mount_kind,
                                exc.is_background,
                                exc.stable_path.clone(),
                                err.to_string(),
                            ));
                            Ok(())
                        },
                    )
                    .await
                },
            )
            .unwrap()
        };
        let operator = tokio::time::timeout(Duration::from_secs(10), operator_rx.recv())
            .await
            .expect("live component did not start")
            .unwrap();
        let provider = provider_rx.recv().await.unwrap();
        Self {
            app,
            _dir: dir,
            table,
            source,
            gate,
            full_passes,
            operator,
            provider,
            handle: Some(handle),
            reports,
        }
    }

    fn rows(&self) -> BTreeMap<String, String> {
        self.table.lock().unwrap().rows.clone()
    }

    /// `update` the child under `key` to declare `key = value`.
    async fn start_put(&self, key: &str, value: &str) -> cocoindex::live_component::ReadyHandle {
        let provider = self.provider.clone();
        let (k, v) = (key.to_string(), value.to_string());
        self.operator
            .start_update(key, move |ctx| async move {
                declare_target_state(&ctx, provider.target_state(k, v))
            })
            .await
            .unwrap()
    }

    async fn put(&self, key: &str, value: &str) {
        self.start_put(key, value).await.ready().await.unwrap();
    }

    async fn stop(mut self) {
        let _ = tokio::time::timeout(Duration::from_secs(20), self.app.drop_state()).await;
        if let Some(handle) = self.handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Storms
// ---------------------------------------------------------------------------

/// A thousand updates of one key issued without waiting: every handle
/// resolves, the target ends at the last value, and queued updates coalesce
/// (the child runs far fewer times than it was asked to).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_key_storm_coalesces_and_last_value_wins() {
    let h = Harness::start("battle_same_key_storm", &[]).await;
    let runs = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for i in 0..1000 {
        let provider = h.provider.clone();
        let runs = runs.clone();
        let value = format!("v{i}");
        handles.push(
            h.operator
                .start_update("hot", move |ctx| async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    declare_target_state(&ctx, provider.target_state("hot".to_string(), value))
                })
                .await
                .unwrap(),
        );
    }
    for handle in handles {
        handle.ready().await.unwrap();
    }
    assert_eq!(h.rows().get("hot").map(String::as_str), Some("v999"));
    let ran = runs.load(Ordering::SeqCst);
    assert!(
        ran < 1000,
        "1000 queued updates of one key should coalesce; the child ran {ran} times"
    );
    h.stop().await;
}

/// Two thousand distinct keys issued without waiting all land, each with one
/// create, and a later full pass that declares the same rows changes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_key_storm_lands_every_key_once() {
    let h = Harness::start("battle_distinct_storm", &[]).await;
    let n = 2000;
    let mut handles = Vec::with_capacity(n);
    for i in 0..n {
        handles.push(h.start_put(&format!("k{i:05}"), &format!("v{i}")).await);
    }
    for handle in handles {
        handle.ready().await.unwrap();
    }
    {
        let table = h.table.lock().unwrap();
        assert_eq!(table.rows.len(), n);
        assert_eq!(table.creates, n);
        assert_eq!(table.updates, 0);
        assert_eq!(table.deletes, 0);
    }
    h.stop().await;
}

/// Create, delete and create again the same key, fifty times over and
/// without waiting: the key ends present with its last value; a sequence that
/// ends in a delete leaves it absent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rapid_create_delete_create_ends_in_the_last_operation() {
    let h = Harness::start("battle_create_delete", &[]).await;
    let mut handles = Vec::new();
    for i in 0..50 {
        handles.push(h.start_put("flip", &format!("v{i}")).await);
        handles.push(h.operator.start_delete("flip").await.unwrap());
    }
    handles.push(h.start_put("flip", "final").await);
    for handle in handles {
        handle.ready().await.unwrap();
    }
    assert_eq!(h.rows().get("flip").map(String::as_str), Some("final"));

    let mut handles = Vec::new();
    for i in 0..50 {
        handles.push(h.operator.start_delete("flip").await.unwrap());
        handles.push(h.start_put("flip", &format!("w{i}")).await);
    }
    handles.push(h.operator.start_delete("flip").await.unwrap());
    for handle in handles {
        handle.ready().await.unwrap();
    }
    assert_eq!(h.rows().get("flip"), None);

    // A full pass after the churn neither resurrects the deleted key nor
    // drops a declared one.
    h.source.lock().unwrap().insert("steady".into(), "s".into());
    h.operator.update_full().await.unwrap();
    assert_eq!(
        h.rows(),
        BTreeMap::from([("steady".to_string(), "s".to_string())])
    );
    h.stop().await;
}

// ---------------------------------------------------------------------------
// Incremental work racing a full pass
// ---------------------------------------------------------------------------

/// An incremental update issued while a full pass runs waits for the pass,
/// then applies on top of it: the end state has both the pass's rows and the
/// update's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_issued_during_a_full_pass_applies_after_it() {
    let h = Harness::start("battle_update_during_full", &[("a", "1")]).await;
    h.gate.armed.store(true, Ordering::SeqCst);
    h.source.lock().unwrap().insert("b".into(), "2".into());

    let operator = h.operator.clone();
    let full = tokio::spawn(async move { operator.update_full().await });
    tokio::time::timeout(Duration::from_secs(5), h.gate.entered.notified())
        .await
        .expect("full pass did not start");

    let handle = h.start_put("c", "3").await;
    // The pass is still held open: the update must not have landed yet.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(h.rows().get("c"), None, "update ran during the full pass");

    h.gate.armed.store(false, Ordering::SeqCst);
    h.gate.release.notify_one();
    full.await.unwrap().unwrap();
    handle.ready().await.unwrap();

    // `c` was mounted by `update` only. The pass that ran before it does not
    // know it; it stays until the next full pass, which garbage-collects it,
    // as in the Python SDK.
    assert_eq!(
        h.rows(),
        BTreeMap::from([
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
            ("c".to_string(), "3".to_string()),
        ])
    );
    h.operator.update_full().await.unwrap();
    assert_eq!(
        h.rows(),
        BTreeMap::from([
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
        ])
    );
    h.stop().await;
}

/// Concurrent full passes serialize: none overlaps another.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_full_passes_serialize() {
    let h = Harness::start("battle_full_serialize", &[("a", "1")]).await;
    let before = h.full_passes.load(Ordering::SeqCst);
    let passes: Vec<_> = (0..8)
        .map(|_| {
            let operator = h.operator.clone();
            tokio::spawn(async move { operator.update_full().await })
        })
        .collect();
    for pass in passes {
        pass.await.unwrap().unwrap();
    }
    assert_eq!(h.full_passes.load(Ordering::SeqCst) - before, 8);
    assert_eq!(
        h.rows(),
        BTreeMap::from([("a".to_string(), "1".to_string())])
    );
    h.stop().await;
}

// ---------------------------------------------------------------------------
// Failure and recovery
// ---------------------------------------------------------------------------

/// A child that fails in live mode keeps the target state its last good run
/// wrote, its failure reaches the handler as background `Update` work, and the
/// next good run replaces the state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failing_child_keeps_its_last_good_state_and_recovers() {
    let h = Harness::start("battle_fail_recover", &[]).await;
    h.put("k", "good-1").await;
    assert_eq!(h.rows().get("k").map(String::as_str), Some("good-1"));

    // The failing run declares a new value before failing: nothing of it may
    // land.
    let provider = h.provider.clone();
    h.operator
        .update("k", move |ctx| async move {
            declare_target_state(&ctx, provider.target_state("k".to_string(), "bad".into()))?;
            Err(Error::engine("source unreadable"))
        })
        .await
        .expect("the handler swallows the failure");
    assert_eq!(h.rows().get("k").map(String::as_str), Some("good-1"));
    {
        let reports = h.reports.lock().unwrap();
        assert!(
            reports.iter().any(|(kind, background, path, err)| {
                *kind == MountKind::Update
                    && *background
                    && path.contains('k')
                    && err.contains("source unreadable")
            }),
            "{reports:?}"
        );
    }
    // A failing first run of a new key leaves nothing behind either.
    h.operator
        .update("fresh", |_ctx| async move { Err(Error::engine("boom")) })
        .await
        .unwrap();
    assert_eq!(h.rows().get("fresh"), None);

    h.put("k", "good-2").await;
    h.put("fresh", "ok").await;
    assert_eq!(h.rows().get("k").map(String::as_str), Some("good-2"));
    assert_eq!(h.rows().get("fresh").map(String::as_str), Some("ok"));

    // A full pass after the failures keeps declared rows and drops the
    // update-only ones, as usual.
    h.source.lock().unwrap().insert("k".into(), "good-2".into());
    h.operator.update_full().await.unwrap();
    assert_eq!(
        h.rows(),
        BTreeMap::from([("k".to_string(), "good-2".to_string())])
    );
    h.stop().await;
}

/// A full pass that fails keeps every row the previous pass declared, its
/// failure reaches the handler as `UpdateFull`, and the next pass that
/// succeeds reconciles normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failing_full_pass_keeps_previous_rows() {
    let h = Harness::start("battle_full_fail", &[("a", "1"), ("b", "2")]).await;
    assert_eq!(h.rows().len(), 2);
    h.source.lock().unwrap().remove("b");
    h.gate.fail.store(true, Ordering::SeqCst);
    h.operator
        .update_full()
        .await
        .expect("the handler swallows it");
    assert_eq!(
        h.rows().get("b").map(String::as_str),
        Some("2"),
        "a failed full pass must not delete the previous pass's rows"
    );
    assert!(
        h.reports
            .lock()
            .unwrap()
            .iter()
            .any(|(kind, _, _, err)| *kind == MountKind::UpdateFull
                && err.contains("full pass failed"))
    );
    h.operator.update_full().await.unwrap();
    assert_eq!(
        h.rows(),
        BTreeMap::from([("a".to_string(), "1".to_string())])
    );
    h.stop().await;
}

// ---------------------------------------------------------------------------
// Readiness
// ---------------------------------------------------------------------------

/// A component that marks ready only when told to, and records when its mount
/// returned relative to that.
struct LateReady {
    release: Arc<Notify>,
    marked: Arc<AtomicBool>,
    calls_to_mark: usize,
}

#[async_trait]
impl LiveComponent for LateReady {
    async fn process(&self, _ctx: Ctx) -> Result<()> {
        Ok(())
    }

    async fn process_live(&self, operator: LiveComponentOperator) -> Result<()> {
        operator.update_full().await?;
        self.release.notified().await;
        self.marked.store(true, Ordering::SeqCst);
        for _ in 0..self.calls_to_mark {
            operator.mark_ready().await;
        }
        std::future::pending::<()>().await;
        Ok(())
    }
}

/// `mount_live` returns only once the component marks itself ready, the
/// update's readiness waits for it too, and a repeated `mark_ready` is a
/// no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mount_live_returns_only_after_mark_ready() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder("battle_late_ready")
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap();
    let release = Arc::new(Notify::new());
    let marked = Arc::new(AtomicBool::new(false));
    let mounted = Arc::new(AtomicBool::new(false));
    let mut handle = {
        let release = release.clone();
        let marked = marked.clone();
        let mounted = mounted.clone();
        app.start_update_with_options(
            UpdateOptions {
                live: true,
                ..UpdateOptions::default()
            },
            move |ctx| async move {
                ctx.mount_live(
                    &"late",
                    LateReady {
                        release,
                        marked: marked.clone(),
                        calls_to_mark: 3,
                    },
                )
                .await?;
                assert!(
                    marked.load(Ordering::SeqCst),
                    "mount_live returned before mark_ready"
                );
                mounted.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .unwrap()
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!mounted.load(Ordering::SeqCst));
    assert!(
        handle.detailed_stats_snapshot().status != cocoindex::UpdateStatus::Ready,
        "the update reported ready before its live component"
    );
    release.notify_one();
    let deadline = Instant::now() + Duration::from_secs(10);
    while handle.detailed_stats_snapshot().status != cocoindex::UpdateStatus::Ready {
        assert!(Instant::now() < deadline, "update never became ready");
        let _ = tokio::time::timeout(Duration::from_millis(100), handle.changed()).await;
    }
    assert!(mounted.load(Ordering::SeqCst));
    let _ = app.drop_state().await;
    let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
}

/// A `process_live` that returns without calling `mark_ready` is ready when
/// it returns (as in Python, where returning marks the component ready).
struct ReturnsWithoutReady;

#[async_trait]
impl LiveComponent for ReturnsWithoutReady {
    async fn process(&self, _ctx: Ctx) -> Result<()> {
        Ok(())
    }

    async fn process_live(&self, operator: LiveComponentOperator) -> Result<()> {
        operator.update_full().await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_live_returning_without_mark_ready_is_ready() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder("battle_return_ready")
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap();
    for live in [false, true] {
        tokio::time::timeout(
            Duration::from_secs(10),
            app.update_with_options(
                UpdateOptions {
                    live,
                    ..UpdateOptions::default()
                },
                |ctx| async move { ctx.mount_live(&"r", ReturnsWithoutReady).await },
            ),
        )
        .await
        .expect("mount_live hung without mark_ready")
        .unwrap();
    }
}

/// A `process_live` that fails before it is ready: the failure reaches the
/// caller of `mount_live` (the parent cannot have its live child half-up), in
/// catch-up and in live mode.
struct FailsBeforeReady;

#[async_trait]
impl LiveComponent for FailsBeforeReady {
    async fn process(&self, _ctx: Ctx) -> Result<()> {
        Ok(())
    }

    async fn process_live(&self, _operator: LiveComponentOperator) -> Result<()> {
        Err(Error::engine("cannot connect to the source"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_live_failing_before_ready_fails_the_mount() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder("battle_fail_before_ready")
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap();
    for live in [false, true] {
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            app.update_with_options(
                UpdateOptions {
                    live,
                    ..UpdateOptions::default()
                },
                |ctx| async move { ctx.mount_live(&"f", FailsBeforeReady).await },
            ),
        )
        .await
        .expect("update hung");
        let err = result.expect_err("a process_live that failed before ready must fail the update");
        assert!(
            format!("{err:?}").contains("cannot connect to the source"),
            "live={live}: {err:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// auto_refresh
// ---------------------------------------------------------------------------

/// Cycles slower than the interval never overlap, and the loop keeps going.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_refresh_cycles_never_overlap() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder("battle_auto_refresh_overlap")
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap();
    let running = Arc::new(AtomicUsize::new(0));
    let max_running = Arc::new(AtomicUsize::new(0));
    let cycles = Arc::new(AtomicUsize::new(0));
    let handle = {
        let (running, max_running, cycles) = (running.clone(), max_running.clone(), cycles.clone());
        app.start_update_with_options(
            UpdateOptions {
                live: true,
                ..UpdateOptions::default()
            },
            move |ctx| async move {
                ctx.auto_refresh("poll", Duration::from_millis(10), move |_ctx| {
                    let (running, max_running, cycles) =
                        (running.clone(), max_running.clone(), cycles.clone());
                    async move {
                        let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                        max_running.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        running.fetch_sub(1, Ordering::SeqCst);
                        cycles.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                })
                .await
            },
        )
        .unwrap()
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while cycles.load(Ordering::SeqCst) < 6 {
        assert!(Instant::now() < deadline, "auto_refresh stalled");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(max_running.load(Ordering::SeqCst), 1, "cycles overlapped");
    let _ = app.drop_state().await;
    let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
    // Once the app is dropped, no further cycle starts.
    let after_drop = cycles.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(cycles.load(Ordering::SeqCst) <= after_drop + 1);
}

/// The interval is the pause between the end of one cycle and the start of
/// the next: fast cycles at a 200ms interval run about five times a second,
/// not hundreds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_refresh_respects_its_interval() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder("battle_auto_refresh_interval")
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap();
    let cycles = Arc::new(AtomicUsize::new(0));
    let handle = {
        let cycles = cycles.clone();
        app.start_update_with_options(
            UpdateOptions {
                live: true,
                ..UpdateOptions::default()
            },
            move |ctx| async move {
                ctx.auto_refresh("tick", Duration::from_millis(200), move |_ctx| {
                    let cycles = cycles.clone();
                    async move {
                        cycles.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                })
                .await
            },
        )
        .unwrap()
    };
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let n = cycles.load(Ordering::SeqCst);
    assert!(
        (3..=8).contains(&n),
        "expected ~6 cycles in 1.1s at a 200ms interval, got {n}"
    );
    let _ = app.drop_state().await;
    let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
}

// ---------------------------------------------------------------------------
// Shutdown while work is in flight
// ---------------------------------------------------------------------------

/// Dropping the app while slow updates are queued and running finishes
/// promptly, resolves every handle, and leaves no row behind: no update lands
/// after the drop has reverted the target.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_while_updates_are_in_flight_resolves_every_handle() {
    let h = Harness::start("battle_drop_in_flight", &[("seed", "s")]).await;
    let started = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for i in 0..200 {
        let provider = h.provider.clone();
        let started = started.clone();
        let key = format!("slow{i:03}");
        handles.push(
            h.operator
                .start_update(key.clone(), move |ctx| async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    declare_target_state(&ctx, provider.target_state(key, "v".into()))
                })
                .await
                .unwrap(),
        );
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while started.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let table = h.table.clone();
    let begun = Instant::now();
    let app = h.app.clone();
    tokio::time::timeout(Duration::from_secs(20), app.drop_state())
        .await
        .expect("drop_state hung with updates in flight")
        .unwrap();
    assert!(
        begun.elapsed() < Duration::from_secs(10),
        "drop took {:?}",
        begun.elapsed()
    );
    for handle in handles {
        // Each handle resolves: done, or cancelled by the drop.
        tokio::time::timeout(Duration::from_secs(5), handle.ready())
            .await
            .expect("a handle never resolved after the drop")
            .ok();
    }
    // Nothing reaches the target once the drop has returned. (A provider
    // registered with `register_root_target_states_provider` is not reverted
    // by `drop_state`, so the rows written before the drop stay.)
    let writes_at_drop = {
        let t = table.lock().unwrap();
        t.creates + t.updates
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    let writes_after = {
        let t = table.lock().unwrap();
        t.creates + t.updates
    };
    assert_eq!(
        writes_after, writes_at_drop,
        "a cancelled update wrote after drop_state returned"
    );
    h.stop().await;
}

/// A new key created and deleted at once, without waiting for the create:
/// the delete wins and no row is left behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_key_created_then_deleted_at_once_leaves_nothing() {
    let h = Harness::start("battle_new_key_delete", &[]).await;
    let mut handles = Vec::new();
    for i in 0..200 {
        let key = format!("tmp{i:03}");
        handles.push(h.start_put(&key, "x").await);
        handles.push(h.operator.start_delete(key).await.unwrap());
    }
    for handle in handles {
        handle.ready().await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let rows = h.rows();
    assert!(
        rows.is_empty(),
        "{} rows survived their delete: {:?}",
        rows.len(),
        rows.keys().take(5).collect::<Vec<_>>()
    );
    h.stop().await;
}
