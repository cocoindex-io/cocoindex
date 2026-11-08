//! Long live-mode runs, behind `#[ignore]` (minutes, and they measure the
//! process's memory, so run them one at a time in release mode):
//!
//!   cargo test -p cocoindex --features fs_live --release --test live_soak \
//!     -- --ignored --test-threads=1 --nocapture
//!
//! * `soak_100k_events_keep_memory_and_queue_bounded`: 100k change events
//!   through a live feed — rewrites of a hot key set, and distinct keys
//!   created then deleted (the churn of component paths a log directory or a
//!   topic of unique keys produces) — with resident memory and the number of
//!   unresolved handles sampled as it runs.
//! * `soak_fs_100k_writes`: 100k file writes over 1000 files through the live
//!   LocalFS watcher (more than the inotify queue holds, so overflow rescans
//!   happen too), then the output tree checked against the source tree.
//! * `fs_event_to_target_latency`: the time from a file write to its output
//!   file, over sequential single-file edits.
#![cfg(feature = "fs_live")]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cocoindex::live_component::{LiveMapFeed, LiveMapSubscriber, LiveMapView};
use cocoindex::resources::fs::{self, FileEntry};
use cocoindex::{
    App, Result, StableKey, TargetAction, TargetActionSink, TargetHandler, TargetReconcileOutput,
    UpdateOptions, async_trait, declare_target_state, register_root_target_states_provider,
};
use futures::StreamExt;
use tokio::sync::mpsc;

/// Anonymous resident memory of this process (the heap: what a leak grows),
/// in MiB. File-backed pages — the LMDB map, which grows with the state db —
/// are left out.
fn rss_mib() -> f64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let kib: f64 = status
        .lines()
        .find_map(|l| l.strip_prefix("RssAnon:"))
        .and_then(|v| v.split_whitespace().next()?.parse().ok())
        .unwrap_or(0.0);
    kib / 1024.0
}

// ---------------------------------------------------------------------------
// 100k events through a channel-driven feed
// ---------------------------------------------------------------------------

enum Event {
    Put(String, String),
    Delete(String),
}

/// A watch-only feed fed from a channel. It queues each event without waiting
/// and keeps the unresolved handles, recording the most it ever held.
struct ChannelFeed {
    rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<Event>>,
    in_flight_max: Arc<AtomicUsize>,
    in_flight_now: Arc<AtomicUsize>,
}

#[async_trait]
impl LiveMapFeed<String, String> for ChannelFeed {
    async fn watch(&self, subscriber: LiveMapSubscriber<String, String>) -> Result<()> {
        let mut rx = self.rx.lock().await;
        let mut in_flight = futures::stream::FuturesUnordered::new();
        loop {
            tokio::select! {
                event = rx.recv() => {
                    let Some(event) = event else { break };
                    let handle = match event {
                        Event::Put(k, v) => subscriber.start_update(k, v).await?,
                        Event::Delete(k) => subscriber.start_delete(k).await?,
                    };
                    in_flight.push(handle.ready());
                    self.in_flight_max.fetch_max(in_flight.len(), Ordering::Relaxed);
                }
                Some(result) = in_flight.next(), if !in_flight.is_empty() => result?,
            }
            self.in_flight_now.store(in_flight.len(), Ordering::Relaxed);
        }
        while let Some(result) = in_flight.next().await {
            result?;
        }
        Ok(())
    }
}

#[async_trait]
impl LiveMapView<String, String> for ChannelFeed {
    async fn scan(&self) -> Result<Vec<(String, String)>> {
        Ok(Vec::new())
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Row(String, Option<String>);

/// Keeps the rows in a map and counts applied actions.
#[derive(Clone)]
struct MapHandler {
    rows: Arc<Mutex<BTreeMap<String, String>>>,
    applied: Arc<AtomicUsize>,
    sink: TargetActionSink<Row>,
}

impl MapHandler {
    fn new() -> Self {
        let rows: Arc<Mutex<BTreeMap<String, String>>> = Arc::default();
        let applied: Arc<AtomicUsize> = Arc::default();
        let sink = {
            let (rows, applied) = (rows.clone(), applied.clone());
            TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<Row>>| {
                let (rows, applied) = (rows.clone(), applied.clone());
                async move {
                    let mut rows = rows.lock().unwrap();
                    for action in actions {
                        applied.fetch_add(1, Ordering::Relaxed);
                        match action {
                            TargetAction::Create(Row(k, v)) | TargetAction::Update(Row(k, v)) => {
                                rows.insert(k, v.unwrap_or_default());
                            }
                            TargetAction::Delete(Row(k, _)) => {
                                rows.remove(&k);
                            }
                        }
                    }
                    Ok(())
                }
            })
        };
        Self {
            rows,
            applied,
            sink,
        }
    }
}

impl TargetHandler<String> for MapHandler {
    type TrackingRecord = String;
    type Action = Row;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<String>,
        prev: Vec<String>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<Row, String>>> {
        let key = match key {
            StableKey::Str(s) => s.to_string(),
            other => format!("{other:?}"),
        };
        if desired.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }
        let sink = self.sink.clone();
        Ok(Some(TargetReconcileOutput {
            action: match &desired {
                Some(v) if prev.is_empty() => TargetAction::Create(Row(key, Some(v.clone()))),
                Some(v) => TargetAction::Update(Row(key, Some(v.clone()))),
                None => TargetAction::Delete(Row(key, None)),
            },
            sink,
            tracking_record: desired,
            child_invalidation: None,
        }))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "soak test: run with --release --ignored --test-threads=1"]
async fn soak_100k_events_keep_memory_and_queue_bounded() {
    let events: usize = std::env::var("SOAK_EVENTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);
    const HOT_KEYS: usize = 1000;
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder("LiveSoakEvents")
        .db_path(dir.path().join("db"))
        .build()
        .await
        .unwrap();
    let (tx, rx) = mpsc::unbounded_channel();
    let in_flight_max = Arc::new(AtomicUsize::new(0));
    let in_flight_now = Arc::new(AtomicUsize::new(0));
    let handler = MapHandler::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let handle = {
        let feed = ChannelFeed {
            rx: tokio::sync::Mutex::new(rx),
            in_flight_max: in_flight_max.clone(),
            in_flight_now: in_flight_now.clone(),
        };
        let (handler, runs) = (handler.clone(), runs.clone());
        app.start_update_with_options(
            UpdateOptions {
                live: true,
                ..UpdateOptions::default()
            },
            move |ctx| async move {
                let provider = register_root_target_states_provider(&ctx, "soak/rows", handler)?;
                ctx.mount_each_live(&"events", feed, move |ctx, value: String| {
                    let provider = provider.clone();
                    let runs = runs.clone();
                    async move {
                        runs.fetch_add(1, Ordering::Relaxed);
                        let key = value.split_once('=').unwrap().0.to_string();
                        declare_target_state(&ctx, provider.target_state(key, value))
                    }
                })
                .await
            },
        )
        .unwrap()
    };
    tokio::time::sleep(Duration::from_millis(500)).await;

    let started = Instant::now();
    let mut samples = Vec::new();
    let mut expected: BTreeMap<String, String> = BTreeMap::new();
    let mut churn = 0usize;
    // `SOAK_MODE=hot` sends only hot-key rewrites, `churn` only create/delete
    // pairs; the default mixes both.
    let mode = std::env::var("SOAK_MODE").unwrap_or_default();
    for i in 0..events {
        let hot = match mode.as_str() {
            "hot" => true,
            "churn" => false,
            _ => i % 2 == 0,
        };
        let create = if mode == "churn" {
            i % 2 == 0
        } else {
            i % 4 == 1
        };
        if hot {
            let key = format!("hot{:04}", (i / 2) % HOT_KEYS);
            let value = format!("{key}={i}");
            expected.insert(key.clone(), value.clone());
            tx.send(Event::Put(key, value)).unwrap();
        } else if create {
            let key = format!("churn{churn:06}");
            tx.send(Event::Put(key.clone(), format!("{key}=x")))
                .unwrap();
        } else {
            let key = format!("churn{churn:06}");
            tx.send(Event::Delete(key)).unwrap();
            churn += 1;
        }
        if i % (events / 10).max(1) == (events / 10).max(1) - 1 {
            // Pace the producer to the consumer so the samples measure the
            // engine rather than the channel backlog.
            while in_flight_now.load(Ordering::Relaxed) > 5_000 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            samples.push((i + 1, rss_mib(), in_flight_now.load(Ordering::Relaxed)));
        }
    }
    let deadline = Instant::now() + Duration::from_secs(600);
    let mut last_report = Instant::now();
    loop {
        let rows = handler.rows.lock().unwrap().clone();
        if rows == expected {
            break;
        }
        if last_report.elapsed() > Duration::from_secs(10) || Instant::now() > deadline {
            last_report = Instant::now();
            let wrong: Vec<_> = expected
                .iter()
                .filter(|(k, v)| rows.get(*k) != Some(*v))
                .take(5)
                .map(|(k, v)| (k.clone(), v.clone(), rows.get(k).cloned()))
                .collect();
            let extra = rows.keys().filter(|k| !expected.contains_key(*k)).count();
            eprintln!(
                "waiting: {} rows ({} expected, {extra} extra), unresolved handles {}, runs {}; first wrong: {wrong:?}",
                rows.len(),
                expected.len(),
                in_flight_now.load(Ordering::Relaxed),
                runs.load(Ordering::Relaxed),
            );
        }
        assert!(Instant::now() < deadline, "the soak never converged");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let took = started.elapsed();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let settled_rss = rss_mib();
    for (events, rss, pending) in &samples {
        eprintln!("after {events:>6} events: rss {rss:7.1} MiB, unresolved handles {pending}");
    }
    eprintln!(
        "{events} events converged in {took:?} ({:.0} events/s); {} component runs, {} target actions; \
         most unresolved handles {}; rss settled at {settled_rss:.1} MiB",
        events as f64 / took.as_secs_f64(),
        runs.load(Ordering::Relaxed),
        handler.applied.load(Ordering::Relaxed),
        in_flight_max.load(Ordering::Relaxed),
    );
    // Memory: the second half of the run must not grow like the first. The
    // churn creates and deletes 25k distinct component paths; a per-path leak
    // would show here.
    let at_half = samples[samples.len() / 2 - 1].1;
    let at_end = samples.last().unwrap().1;
    assert!(
        at_end - at_half < 16.0,
        "rss grew {:.1} MiB over the second half of the events",
        at_end - at_half
    );
    assert_eq!(
        in_flight_now.load(Ordering::Relaxed),
        0,
        "handles left unresolved"
    );

    let _ = tokio::time::timeout(Duration::from_secs(60), app.drop_state()).await;
    let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
}

// ---------------------------------------------------------------------------
// LocalFS
// ---------------------------------------------------------------------------

fn tree(dir: &Path) -> BTreeMap<String, String> {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|e| {
                    let e = e.ok()?;
                    Some((
                        e.file_name().to_string_lossy().into_owned(),
                        std::fs::read_to_string(e.path()).ok()?,
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

async fn start_fs(
    app: &App,
    src: &Path,
    out: &Path,
    runs: Arc<AtomicUsize>,
) -> cocoindex::UpdateHandle<()> {
    let (src, out) = (src.to_path_buf(), out.to_path_buf());
    app.start_update_with_options(
        UpdateOptions {
            live: true,
            ..UpdateOptions::default()
        },
        move |ctx| async move {
            let target = fs::mount_dir_target(&ctx, out)?;
            let feed = fs::walk_dir(src).live().rescan_interval(None);
            ctx.mount_each_live(&"files", feed, move |ctx, file: FileEntry| {
                let (target, runs) = (target.clone(), runs.clone());
                async move {
                    runs.fetch_add(1, Ordering::Relaxed);
                    let content = file.content()?;
                    target.declare_file(&ctx, &file.key(), &content)
                }
            })
            .await
        },
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "soak test: run with --release --ignored --test-threads=1"]
async fn soak_fs_100k_writes() {
    const FILES: usize = 1000;
    let rounds: usize = std::env::var("SOAK_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let (src, out) = (root.join("src"), root.join("out"));
    std::fs::create_dir_all(&src).unwrap();
    let app = App::builder("LiveSoakFs")
        .db_path(root.join("db"))
        .build()
        .await
        .unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let handle = start_fs(&app, &src, &out, runs.clone()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let started = Instant::now();
    let mut samples = Vec::new();
    for round in 0..rounds {
        for i in 0..FILES {
            std::fs::write(src.join(format!("f{i:04}.txt")), format!("{i}-{round}")).unwrap();
        }
        if round % (rounds / 10).max(1) == (rounds / 10).max(1) - 1 {
            samples.push(((round + 1) * FILES, rss_mib(), runs.load(Ordering::Relaxed)));
            // Let the watcher keep up a little, as a real writer would.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let written = started.elapsed();
    let expected = tree(&src);
    let deadline = Instant::now() + Duration::from_secs(600);
    while tree(&out) != expected {
        assert!(Instant::now() < deadline, "the fs soak never converged");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let took = started.elapsed();
    for (writes, rss, runs) in &samples {
        eprintln!("after {writes:>6} writes: rss {rss:7.1} MiB, component runs so far {runs}");
    }
    eprintln!(
        "{} writes over {FILES} files written in {written:?}, converged in {took:?}; {} component runs; rss {:.1} MiB",
        FILES * rounds,
        runs.load(Ordering::Relaxed),
        rss_mib()
    );
    let at_half = samples[samples.len() / 2 - 1].1;
    let at_end = samples.last().unwrap().1;
    assert!(
        at_end - at_half < 64.0,
        "rss grew {:.1} MiB over the second half",
        at_end - at_half
    );
    let _ = tokio::time::timeout(Duration::from_secs(60), app.drop_state()).await;
    let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement: run with --release --ignored --nocapture"]
async fn fs_event_to_target_latency() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let (src, out) = (root.join("src"), root.join("out"));
    std::fs::create_dir_all(&src).unwrap();
    for i in 0..100 {
        std::fs::write(src.join(format!("bg{i:03}.txt")), b"bg").unwrap();
    }
    let app = App::builder("LiveLatency")
        .db_path(root.join("db"))
        .build()
        .await
        .unwrap();
    let handle = start_fs(&app, &src, &out, Arc::default()).await;
    let deadline = Instant::now() + Duration::from_secs(60);
    while tree(&out).len() < 100 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut latencies = Vec::new();
    for i in 0..50 {
        let name = if i % 2 == 0 {
            "edit.txt".to_string()
        } else {
            format!("new{i:02}.txt")
        };
        let content = format!("v{i}");
        let written = Instant::now();
        std::fs::write(src.join(&name), &content).unwrap();
        loop {
            if std::fs::read_to_string(out.join(&name)).ok().as_deref() == Some(content.as_str()) {
                break;
            }
            assert!(
                written.elapsed() < Duration::from_secs(10),
                "edit {i} never landed"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        latencies.push(written.elapsed());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    latencies.sort();
    let pick = |q: f64| latencies[((latencies.len() - 1) as f64 * q).round() as usize];
    eprintln!(
        "event-to-target latency over {} edits: min {:?}, p50 {:?}, p95 {:?}, max {:?}",
        latencies.len(),
        latencies[0],
        pick(0.5),
        pick(0.95),
        latencies.last().unwrap()
    );
    let _ = tokio::time::timeout(Duration::from_secs(20), app.drop_state()).await;
    let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
}
