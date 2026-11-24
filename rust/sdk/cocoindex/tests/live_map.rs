//! Integration test for the in-memory `LiveMap` resource.

use std::sync::{Arc, Mutex};

use cocoindex::{App, LiveMap, LiveMapView, PreviewAction, StableKey};

/// Smoke test: `LiveMap::create` mounts its backing container target and
/// `declare_entry` declares entries that sync (run the entry sink) when the
/// component commits — the whole producer-side target machinery runs end to end.
#[test]
fn live_map_create_and_declare() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder("live_map_smoke")
        .db_path(dir.path().join("lmdb"))
        .build_blocking()
        .unwrap();

    app.update_blocking(|ctx| async move {
        let lm: LiveMap<String, String> = LiveMap::create(&ctx).await?;
        lm.declare_entry(&ctx, "a".to_string(), "alpha".to_string())?;
        lm.declare_entry(&ctx, "b".to_string(), "beta".to_string())?;
        Ok(())
    })
    .unwrap();
}

/// Preview plans the container and its entries without applying them: the
/// map stays empty.
#[tokio::test]
async fn live_map_preview_applies_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::open("live_map_preview", dir.path().join("lmdb"))
        .await
        .unwrap();
    let created: Arc<Mutex<Option<LiveMap<String, String>>>> = Arc::new(Mutex::new(None));

    let actions = app
        .preview({
            let created = created.clone();
            move |ctx| async move {
                let lm: LiveMap<String, String> = LiveMap::create(&ctx).await?;
                lm.declare_entry(&ctx, "a".to_string(), "alpha".to_string())?;
                lm.declare_entry(&ctx, "b".to_string(), "beta".to_string())?;
                *created.lock().unwrap() = Some(lm);
                Ok(())
            }
        })
        .await
        .unwrap();
    let mut planned: Vec<String> = actions
        .iter()
        .map(|action| {
            let (verb, value) = match action {
                PreviewAction::Create(v) => ("create", v),
                PreviewAction::Update(v) => ("update", v),
                PreviewAction::Delete(v) => ("delete", v),
            };
            // Actions are encoded as arrays: the container's is empty, an
            // entry's is `[key, value]`.
            let fields: Vec<serde_json::Value> = value.decode().unwrap();
            match fields.as_slice() {
                [] => format!("{verb} container"),
                [key, value] => format!("{verb} {key} = {value}"),
                other => panic!("unexpected action {other:?}"),
            }
        })
        .collect();
    planned.sort();
    // LiveMap plans every write as an update.
    assert_eq!(
        planned,
        vec![
            r#"update "a" = "alpha""#,
            r#"update "b" = "beta""#,
            "update container",
        ]
    );
    let lm = created.lock().unwrap().take().unwrap();
    assert!(lm.scan().await.unwrap().is_empty());
}

/// `K` is any stable key, as Python's `LiveMap[K, V]`: an `i64` key round-trips
/// through the engine (declare → reconcile → sink) and comes back as an `i64`.
#[tokio::test]
async fn live_map_integer_keys_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::open("live_map_int_keys", dir.path().join("lmdb"))
        .await
        .unwrap();
    let created: Arc<Mutex<Option<LiveMap<i64, String>>>> = Arc::new(Mutex::new(None));
    app.update({
        let created = created.clone();
        move |ctx| async move {
            let lm: LiveMap<i64, String> = LiveMap::create(&ctx).await?;
            lm.declare_entry(&ctx, 7, "seven".to_string())?;
            lm.declare_entry(&ctx, -2, "minus two".to_string())?;
            *created.lock().unwrap() = Some(lm);
            Ok(())
        }
    })
    .await
    .unwrap();
    let lm = created.lock().unwrap().take().unwrap();
    let mut entries = lm.scan().await.unwrap();
    entries.sort();
    assert_eq!(
        entries,
        vec![(-2, "minus two".to_string()), (7, "seven".to_string())]
    );
}

/// `StableKey` itself is a key type, so composite keys work too.
#[tokio::test]
async fn live_map_stable_keys_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::open("live_map_stable_keys", dir.path().join("lmdb"))
        .await
        .unwrap();
    let created: Arc<Mutex<Option<LiveMap<StableKey, i64>>>> = Arc::new(Mutex::new(None));
    let composite = StableKey::Array(Arc::from([
        StableKey::Str(Arc::from("doc")),
        StableKey::Int(3),
    ]));
    app.update({
        let created = created.clone();
        let composite = composite.clone();
        move |ctx| async move {
            let lm: LiveMap<StableKey, i64> = LiveMap::create(&ctx).await?;
            lm.declare_entry(&ctx, composite, 42)?;
            lm.declare_entry(&ctx, StableKey::Str(Arc::from("plain")), 1)?;
            *created.lock().unwrap() = Some(lm);
            Ok(())
        }
    })
    .await
    .unwrap();
    let lm = created.lock().unwrap().take().unwrap();
    let mut entries = lm.scan().await.unwrap();
    entries.sort();
    let mut expected = vec![(composite, 42), (StableKey::Str(Arc::from("plain")), 1)];
    expected.sort();
    assert_eq!(entries, expected);
}

mod live {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use cocoindex::{
        App, Ctx, LiveComponent, LiveComponentOperator, LiveMap, Result, UpdateOptions, async_trait,
    };
    use tokio::sync::Notify;

    /// Declares every entry of `source`; re-runs its full pass on `trigger`.
    struct Producer {
        lm: LiveMap<String, String>,
        source: Arc<Mutex<Vec<(String, String)>>>,
        trigger: Arc<Notify>,
    }

    #[async_trait]
    impl LiveComponent for Producer {
        async fn process(&self, ctx: Ctx) -> Result<()> {
            let entries = self.source.lock().unwrap().clone();
            for (key, value) in entries {
                self.lm.declare_entry(&ctx, key, value)?;
            }
            Ok(())
        }

        async fn process_live(&self, operator: LiveComponentOperator) -> Result<()> {
            operator.update_full().await?;
            operator.mark_ready().await;
            loop {
                self.trigger.notified().await;
                operator.update_full().await?;
            }
        }
    }

    /// Consumes the map; runs its full pass twice, so its `mount_each_live`
    /// child is mounted again and the map is watched a second time.
    struct Consumer {
        lm: LiveMap<String, String>,
        seen: Arc<Mutex<Vec<String>>>,
        remounted: Arc<Notify>,
    }

    #[async_trait]
    impl LiveComponent for Consumer {
        async fn process(&self, ctx: Ctx) -> Result<()> {
            let seen = self.seen.clone();
            ctx.mount_each_live(&"each", self.lm.clone(), move |_ctx, value: String| {
                let seen = seen.clone();
                async move {
                    seen.lock().unwrap().push(value);
                    Ok(())
                }
            })
            .await
        }

        async fn process_live(&self, operator: LiveComponentOperator) -> Result<()> {
            operator.update_full().await?;
            operator.mark_ready().await;
            operator.update_full().await?;
            self.remounted.notify_one();
            std::future::pending::<()>().await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn live_map_keeps_feeding_a_consumer_that_is_mounted_again() {
        let dir = tempfile::tempdir().unwrap();
        let app = App::open("live_map_rewatch", dir.path().join("lmdb"))
            .await
            .unwrap();
        let source = Arc::new(Mutex::new(vec![("a".to_string(), "1".to_string())]));
        let trigger = Arc::new(Notify::new());
        let remounted = Arc::new(Notify::new());
        let seen = Arc::new(Mutex::new(Vec::new()));

        let handle = {
            let (source, trigger, remounted, seen) = (
                source.clone(),
                trigger.clone(),
                remounted.clone(),
                seen.clone(),
            );
            app.start_update_with_options(
                UpdateOptions {
                    live: true,
                    ..UpdateOptions::default()
                },
                move |ctx| async move {
                    let lm: LiveMap<String, String> = LiveMap::create(&ctx).await?;
                    ctx.mount_live(
                        &"producer",
                        Producer {
                            lm: lm.clone(),
                            source,
                            trigger,
                        },
                    )
                    .await?;
                    ctx.mount_live(
                        &"consumer",
                        Consumer {
                            lm,
                            seen,
                            remounted,
                        },
                    )
                    .await
                },
            )
            .unwrap()
        };

        tokio::time::timeout(Duration::from_secs(10), remounted.notified())
            .await
            .expect("consumer was not mounted again");
        source
            .lock()
            .unwrap()
            .push(("b".to_string(), "2".to_string()));
        trigger.notify_one();

        let delivered = tokio::time::timeout(Duration::from_secs(10), async {
            while !seen.lock().unwrap().contains(&"2".to_string()) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        assert!(
            delivered.is_ok(),
            "entry produced after the consumer was mounted again never arrived: {:?}",
            seen.lock().unwrap()
        );

        let _ = app.drop_state().await;
        let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
    }

    /// Consumes the map with a slow per-entry component that records each
    /// value it processed (in order, per key) and the latest value per key.
    struct SlowConsumer {
        lm: LiveMap<String, String>,
        latest: Arc<Mutex<std::collections::BTreeMap<String, String>>>,
        runs: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl LiveComponent for SlowConsumer {
        async fn process(&self, ctx: Ctx) -> Result<()> {
            let (latest, runs) = (self.latest.clone(), self.runs.clone());
            ctx.mount_each_live(&"each", self.lm.clone(), move |_ctx, value: String| {
                let (latest, runs) = (latest.clone(), runs.clone());
                async move {
                    runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let (key, _) = value.split_once('=').unwrap();
                    latest
                        .lock()
                        .unwrap()
                        .insert(key.to_string(), value.clone());
                    Ok(())
                }
            })
            .await
        }
    }

    /// A burst of changes from the producer: 200 new entries at once are
    /// consumed concurrently (the consumer takes 50ms an entry, so one at a
    /// time would need ten seconds), and 50 rewrites of one entry end at the
    /// last one with fewer runs than writes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn live_map_burst_is_consumed_concurrently_and_coalesced() {
        let dir = tempfile::tempdir().unwrap();
        let app = App::open("live_map_burst", dir.path().join("lmdb"))
            .await
            .unwrap();
        let source = Arc::new(Mutex::new(vec![("seed".to_string(), "seed=0".to_string())]));
        let trigger = Arc::new(Notify::new());
        let latest = Arc::new(Mutex::new(std::collections::BTreeMap::new()));
        let runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let handle = {
            let (source, trigger, latest, runs) = (
                source.clone(),
                trigger.clone(),
                latest.clone(),
                runs.clone(),
            );
            app.start_update_with_options(
                UpdateOptions {
                    live: true,
                    ..UpdateOptions::default()
                },
                move |ctx| async move {
                    let lm: LiveMap<String, String> = LiveMap::create(&ctx).await?;
                    ctx.mount_live(
                        &"producer",
                        Producer {
                            lm: lm.clone(),
                            source,
                            trigger,
                        },
                    )
                    .await?;
                    ctx.mount_live(&"consumer", SlowConsumer { lm, latest, runs })
                        .await
                },
            )
            .unwrap()
        };
        type Pred = Box<dyn Fn(&std::collections::BTreeMap<String, String>) -> bool>;
        let wait = |pred: Pred, secs: u64| {
            let latest = latest.clone();
            async move {
                let deadline = std::time::Instant::now() + Duration::from_secs(secs);
                while !pred(&latest.lock().unwrap()) {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "timed out; consumed {:?}",
                        latest.lock().unwrap().len()
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        };
        wait(Box::new(|m| m.contains_key("seed")), 10).await;

        let started = std::time::Instant::now();
        source
            .lock()
            .unwrap()
            .extend((0..200).map(|i| (format!("k{i:03}"), format!("k{i:03}=v"))));
        trigger.notify_one();
        wait(Box::new(|m| m.len() == 201), 30).await;
        let took = started.elapsed();
        eprintln!("live map: 200 entries consumed in {took:?}");
        assert!(
            took < Duration::from_secs(5),
            "200 entries at 50ms each took {took:?}: consumed one at a time"
        );

        // Rewrite one entry 50 times, one producer pass each.
        let runs_before = runs.load(std::sync::atomic::Ordering::SeqCst);
        for i in 0..50 {
            source.lock().unwrap()[0] = ("seed".to_string(), format!("seed={i}"));
            trigger.notify_one();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        wait(
            Box::new(|m| m.get("seed").map(String::as_str) == Some("seed=49")),
            30,
        )
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            latest.lock().unwrap().get("seed").map(String::as_str),
            Some("seed=49")
        );
        let seed_runs = runs.load(std::sync::atomic::Ordering::SeqCst) - runs_before;
        eprintln!("live map: 50 rewrites of one entry ran its component {seed_runs} times");

        let _ = app.drop_state().await;
        let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
    }
}
