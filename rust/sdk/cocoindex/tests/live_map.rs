//! Integration test for the in-memory `LiveMap` resource.

use cocoindex::{App, LiveMap};

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
        let lm: LiveMap<String> = LiveMap::create(&ctx).await?;
        lm.declare_entry(&ctx, "a", "alpha".to_string())?;
        lm.declare_entry(&ctx, "b", "beta".to_string())?;
        Ok(())
    })
    .unwrap();
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
        lm: LiveMap<String>,
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
        lm: LiveMap<String>,
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
                    let lm: LiveMap<String> = LiveMap::create(&ctx).await?;
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
}
