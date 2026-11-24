//! Criterion benchmarks for the Rust SDK's engine-facing hot paths, end to
//! end through the public SDK API against a temp LMDB state store.
//!
//! Run with:
//!
//! ```bash
//! cargo bench -p cocoindex --bench sdk_engine
//! cargo bench -p cocoindex --bench sdk_engine -- memo_call      # one group
//! ```
//!
//! Groups (see `benches/README.md` for the baseline numbers):
//!
//! 1. `memo_call` — one `#[cocoindex::function(memo)]` call, hit and miss,
//!    with a small (`i64`) and a large (~100 KB string) argument. Reported
//!    per call: the loop runs inside one `App::update`.
//! 2. `component_overhead` — a warm run over N trivial components, through
//!    `Ctx::mount_each` (no component memo: every child runs and commits),
//!    through `mount_each!` of a logic-tracked function keyed by file-name
//!    strings (every child is a component-memo hit), and through sequential
//!    `Ctx::scope` (`use_mount!`). Reported per component.
//! 3. `fingerprint` — `Fingerprint::from(&Vec<u8>)` (serde path) against
//!    `Fingerprint::from_bytes` at 1 KB, 100 KB and 10 MB.
//! 4. `target_reconcile` — 10,000 rows declared into a counting no-op
//!    `TargetHandler`: `cold` (fresh store: reconcile + create + tracking
//!    record write) and `no_change` (every row's stored tracking record is
//!    decoded and reconciles to `None`). Reported per row.
//! 5. `batched_fn` — N concurrent calls of a `#[function(batching)]` with a
//!    trivial body. Reported per call.
//! 6. `startup` — open an environment on a fresh directory, create an app and
//!    run an empty update: the async API and the `*_blocking` API.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use cocoindex::{
    App, Ctx, Environment, Result, StableKey, TargetAction, TargetActionSink, TargetHandler,
    TargetReconcileOutput, TargetStateProvider, declare_target_state,
    register_root_target_states_provider,
};
use cocoindex_utils::fingerprint::Fingerprint;
use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use tempfile::TempDir;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

struct TempApp {
    app: App,
    _dir: TempDir,
}

async fn temp_app(name: &str) -> TempApp {
    let dir = TempDir::new().unwrap();
    let app = App::builder(name)
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap();
    TempApp { app, _dir: dir }
}

// ---------------------------------------------------------------------------
// 1. memo_call
// ---------------------------------------------------------------------------

#[cocoindex::function(memo)]
async fn memo_small(_ctx: &Ctx, value: i64) -> Result<i64> {
    Ok(value + 1)
}

#[cocoindex::function(memo)]
async fn memo_large(_ctx: &Ctx, text: &String) -> Result<usize> {
    Ok(text.len())
}

const LARGE_ARG_LEN: usize = 100 * 1024;

/// A ~100 KB string whose first 20 bytes hold a counter, so a miss loop can
/// change the argument in place without copying the buffer.
fn large_arg() -> String {
    let mut text = String::with_capacity(LARGE_ARG_LEN);
    while text.len() < LARGE_ARG_LEN {
        text.push_str("0123456789abcdef");
    }
    text.truncate(LARGE_ARG_LEN);
    text
}

fn stamp(text: &mut String, counter: u64) {
    // The digits are ASCII, so the string stays valid UTF-8.
    let digits = format!("{counter:020}");
    text.replace_range(..20, &digits);
}

fn bench_memo_call(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("memo_call");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Hit: the same argument every call. The first call (the miss) is paid
    // in a warm-up update before measuring.
    let fixture = rt.block_on(temp_app("memo_hit_small"));
    rt.block_on(async {
        fixture
            .app
            .update(|ctx| async move { memo_small(&ctx, 7).await })
            .await
            .unwrap()
    });
    group.bench_function("hit/small", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                fixture
                    .app
                    .update(move |ctx| async move {
                        let start = Instant::now();
                        for _ in 0..iters {
                            black_box(memo_small(&ctx, black_box(7)).await?);
                        }
                        Ok(start.elapsed())
                    })
                    .await
                    .unwrap()
            })
        });
    });

    let fixture = rt.block_on(temp_app("memo_hit_large"));
    let text = Arc::new(large_arg());
    rt.block_on(async {
        let text = text.clone();
        fixture
            .app
            .update(move |ctx| async move { memo_large(&ctx, &text).await })
            .await
            .unwrap()
    });
    group.bench_function("hit/large_100k", |b| {
        b.iter_custom(|iters| {
            let text = text.clone();
            rt.block_on(async {
                fixture
                    .app
                    .update(move |ctx| async move {
                        let start = Instant::now();
                        for _ in 0..iters {
                            black_box(memo_large(&ctx, black_box(&text)).await?);
                        }
                        Ok(start.elapsed())
                    })
                    .await
                    .unwrap()
            })
        });
    });

    // Miss: a fresh argument every call, so every call computes and stores.
    let fixture = rt.block_on(temp_app("memo_miss_small"));
    let next = Arc::new(AtomicUsize::new(0));
    group.bench_function("miss/small", |b| {
        b.iter_custom(|iters| {
            let next = next.clone();
            rt.block_on(async {
                fixture
                    .app
                    .update(move |ctx| async move {
                        let start = Instant::now();
                        for _ in 0..iters {
                            let value = next.fetch_add(1, Ordering::Relaxed) as i64;
                            black_box(memo_small(&ctx, black_box(value)).await?);
                        }
                        Ok(start.elapsed())
                    })
                    .await
                    .unwrap()
            })
        });
    });

    let fixture = rt.block_on(temp_app("memo_miss_large"));
    let next = Arc::new(AtomicUsize::new(0));
    group.bench_function("miss/large_100k", |b| {
        b.iter_custom(|iters| {
            let next = next.clone();
            rt.block_on(async {
                fixture
                    .app
                    .update(move |ctx| async move {
                        let mut text = large_arg();
                        let start = Instant::now();
                        for _ in 0..iters {
                            stamp(&mut text, next.fetch_add(1, Ordering::Relaxed) as u64);
                            black_box(memo_large(&ctx, black_box(&text)).await?);
                        }
                        Ok(start.elapsed())
                    })
                    .await
                    .unwrap()
            })
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// 2. component_overhead
// ---------------------------------------------------------------------------

async fn mount_each_run(app: &App, n: usize) -> Duration {
    app.update(move |ctx| async move {
        let start = Instant::now();
        ctx.mount_each(0..n, |i| *i as i64, |_child, _i| async { Ok(()) })
            .await?
            .ready()
            .await?;
        Ok(start.elapsed())
    })
    .await
    .unwrap()
}

/// A logic-tracked child component: a warm run reuses its component memo.
#[cocoindex::function]
async fn memo_hit_child(_ctx: &Ctx, idx: i64) -> Result<()> {
    black_box(idx);
    Ok(())
}

async fn mount_each_memo_run(app: &App, n: usize) -> Duration {
    app.update(move |ctx| async move {
        let start = Instant::now();
        let items = (0..n).map(|i| (format!("file_{i:05}.md"), i as i64));
        cocoindex::mount_each!("files", items, |idx| memo_hit_child(ctx, idx))
            .await?
            .ready()
            .await?;
        Ok(start.elapsed())
    })
    .await
    .unwrap()
}

async fn scope_run(app: &App, n: usize) -> Duration {
    app.update(move |ctx| async move {
        let start = Instant::now();
        for i in 0..n {
            ctx.scope(i as i64, |_child| async { Ok(()) }).await?;
        }
        Ok(start.elapsed())
    })
    .await
    .unwrap()
}

fn bench_component_overhead(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("component_overhead");
    group.sampling_mode(SamplingMode::Flat);
    for n in [1_000usize, 10_000] {
        group.throughput(Throughput::Elements(n as u64));

        let fixture = rt.block_on(temp_app("mount_each_warm"));
        // The cold run creates the component paths; every measured run is warm.
        rt.block_on(mount_each_run(&fixture.app, n));
        group.bench_function(BenchmarkId::new("mount_each/warm", n), |b| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += mount_each_run(&fixture.app, n).await;
                    }
                    total
                })
            });
        });

        let fixture = rt.block_on(temp_app("mount_each_memo_warm"));
        rt.block_on(mount_each_memo_run(&fixture.app, n));
        group.bench_function(BenchmarkId::new("mount_each_memo/warm", n), |b| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += mount_each_memo_run(&fixture.app, n).await;
                    }
                    total
                })
            });
        });

        let fixture = rt.block_on(temp_app("scope_warm"));
        rt.block_on(scope_run(&fixture.app, n));
        group.bench_function(BenchmarkId::new("use_mount/warm", n), |b| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += scope_run(&fixture.app, n).await;
                    }
                    total
                })
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 3. fingerprint
// ---------------------------------------------------------------------------

fn bench_fingerprint(c: &mut Criterion) {
    let mut group = c.benchmark_group("fingerprint");
    for (label, len) in [
        ("1k", 1024usize),
        ("100k", 100 * 1024),
        ("10m", 10 * 1024 * 1024),
    ] {
        let bytes: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        group.throughput(Throughput::Bytes(len as u64));
        group.bench_function(BenchmarkId::new("from_vec", label), |b| {
            b.iter(|| black_box(Fingerprint::from(black_box(&bytes)).unwrap()));
        });
        group.bench_function(BenchmarkId::new("from_bytes", label), |b| {
            b.iter(|| black_box(Fingerprint::from_bytes(black_box(bytes.as_slice()))));
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 4. target_reconcile
// ---------------------------------------------------------------------------

/// What the counting handler saw: reconcile calls and sink actions.
#[derive(Default)]
struct TargetCounters {
    reconciles: AtomicUsize,
    actions: AtomicUsize,
}

#[derive(Clone)]
struct CountingHandler {
    counters: Arc<TargetCounters>,
    sink: TargetActionSink<i64>,
}

fn counting_handler(counters: Arc<TargetCounters>) -> CountingHandler {
    let sink_counters = counters.clone();
    let sink = TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<i64>>| {
        let counters = sink_counters.clone();
        async move {
            counters.actions.fetch_add(actions.len(), Ordering::Relaxed);
            Ok(())
        }
    });
    CountingHandler { counters, sink }
}

impl TargetHandler<i64> for CountingHandler {
    type TrackingRecord = i64;
    type Action = i64;

    fn reconcile(
        &self,
        _key: StableKey,
        desired: Option<i64>,
        prev: Vec<i64>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<i64, i64>>> {
        self.counters.reconciles.fetch_add(1, Ordering::Relaxed);
        match desired {
            Some(value) => {
                let unchanged =
                    !prev_may_be_missing && !prev.is_empty() && prev.iter().all(|p| *p == value);
                if unchanged {
                    return Ok(None);
                }
                let action = if prev.is_empty() {
                    TargetAction::Create(value)
                } else {
                    TargetAction::Update(value)
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
                    action: TargetAction::Delete(prev.first().copied().unwrap_or_default()),
                    sink: self.sink.clone(),
                    tracking_record: None,
                    child_invalidation: None,
                }))
            }
        }
    }
}

const RECONCILE_ROWS: usize = 10_000;

/// Declare `RECONCILE_ROWS` rows with constant values in one component.
async fn declare_rows_run(app: &App, counters: Arc<TargetCounters>) -> Duration {
    app.update(move |ctx| async move {
        let provider: TargetStateProvider<i64> = register_root_target_states_provider(
            &ctx,
            "bench/counting_rows",
            counting_handler(counters),
        )?;
        let start = Instant::now();
        ctx.scope(&"rows", move |child| async move {
            for i in 0..RECONCILE_ROWS {
                declare_target_state(&child, provider.target_state(format!("r_{i}"), i as i64))?;
            }
            Ok(())
        })
        .await?;
        Ok(start.elapsed())
    })
    .await
    .unwrap()
}

fn bench_target_reconcile(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("target_reconcile");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(RECONCILE_ROWS as u64));

    group.bench_function(BenchmarkId::new("cold", RECONCILE_ROWS), |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let fixture = temp_app("reconcile_cold").await;
                    let counters = Arc::new(TargetCounters::default());
                    total += declare_rows_run(&fixture.app, counters.clone()).await;
                    assert_eq!(counters.reconciles.load(Ordering::Relaxed), RECONCILE_ROWS);
                    assert_eq!(counters.actions.load(Ordering::Relaxed), RECONCILE_ROWS);
                }
                total
            })
        });
    });

    let fixture = rt.block_on(temp_app("reconcile_no_change"));
    rt.block_on(declare_rows_run(
        &fixture.app,
        Arc::new(TargetCounters::default()),
    ));
    group.bench_function(BenchmarkId::new("no_change", RECONCILE_ROWS), |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let counters = Arc::new(TargetCounters::default());
                    total += declare_rows_run(&fixture.app, counters.clone()).await;
                    assert_eq!(counters.reconciles.load(Ordering::Relaxed), RECONCILE_ROWS);
                    assert_eq!(counters.actions.load(Ordering::Relaxed), 0);
                }
                total
            })
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// 5. batched_fn
// ---------------------------------------------------------------------------

#[cocoindex::function(batching)]
async fn double_batched(_ctx: &Ctx, items: Vec<i64>) -> Result<Vec<i64>> {
    Ok(items.into_iter().map(|item| item * 2).collect())
}

async fn batched_run(app: &App, n: usize) -> Duration {
    app.update(move |ctx| async move {
        let start = Instant::now();
        let outputs = ctx
            .map(0..n as i64, |item| {
                let ctx = ctx.clone();
                async move { double_batched(&ctx, item).await }
            })
            .await?;
        assert_eq!(outputs.len(), n);
        Ok(start.elapsed())
    })
    .await
    .unwrap()
}

fn bench_batched_fn(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("batched_fn");
    group.sampling_mode(SamplingMode::Flat);
    let fixture = rt.block_on(temp_app("batched"));
    for n in [1_000usize, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("concurrent_calls", n), |b| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += batched_run(&fixture.app, n).await;
                    }
                    total
                })
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 6. startup
// ---------------------------------------------------------------------------

fn bench_startup(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("startup");
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("async/open_and_empty_update", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let dir = TempDir::new().unwrap();
                    let start = Instant::now();
                    let env = Environment::builder()
                        .db_path(dir.path().join(".cocoindex_db"))
                        .build()
                        .await
                        .unwrap();
                    let app = env.app("startup").await.unwrap();
                    app.update(|_ctx| async { Ok(()) }).await.unwrap();
                    total += start.elapsed();
                    env.shutdown().await;
                }
                total
            })
        });
    });

    group.bench_function("blocking/open_and_empty_update", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let dir = TempDir::new().unwrap();
                let start = Instant::now();
                let env = Environment::builder()
                    .db_path(dir.path().join(".cocoindex_db"))
                    .build_blocking()
                    .unwrap();
                let app = env.app_blocking("startup").unwrap();
                app.update_blocking(|_ctx| async { Ok(()) }).unwrap();
                total += start.elapsed();
                rt.block_on(env.shutdown());
            }
            total
        });
    });
    group.finish();
}

fn benchmark_config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .sample_size(10)
}

criterion_group! {
    name = benches;
    config = benchmark_config();
    targets = bench_memo_call, bench_component_overhead, bench_fingerprint,
        bench_target_reconcile, bench_batched_fn, bench_startup
}
criterion_main!(benches);
