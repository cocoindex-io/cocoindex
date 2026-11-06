//! Live-Iggy integration test for the `iggy` source (`topic_as_map`).
//!
//! Skips gracefully when `IGGY_CONNECTION_STRING` is unset. Run with an Apache
//! Iggy server on localhost:
//!   IGGY_CONNECTION_STRING=iggy://iggy:iggy@localhost:8090 \
//!     cargo test -p cocoindex --features iggy --test iggy_source
//!
//! The topic is populated through the (already-tested) Iggy *target* — Iggy
//! messages have no key field, so payloads are self-identifying (`"<key>=<v>"`)
//! and the source's `key_fn` recovers the key. Then the *source* reads them via
//! `Ctx::mount_each_live`: catch-up `scan` compacts the log to the latest payload
//! per key, and live `watch` tails new messages.
#![cfg(feature = "iggy")]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cocoindex::connectors::iggy::{self, IggyConsumer, IggyProducer, IggyReceivedMessage};
use cocoindex::{App, Environment, Result, UpdateOptions};
use iggy::prelude::{
    CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize, StreamClient, TopicClient,
};

const PARTITION: u32 = 0;
const GROUP: &str = "cocoindex-test";

cocoindex::context_key!(static PRODUCER: IggyProducer);

/// An app over `db_path` with the producer provided under `PRODUCER`.
async fn app_with_producer(name: &str, db_path: &std::path::Path, producer: &IggyProducer) -> App {
    Environment::builder()
        .db_path(db_path)
        .provide_key(&PRODUCER, producer.clone())
        .build()
        .await
        .unwrap()
        .app(name)
        .await
        .unwrap()
}

/// Live runs share the process-wide cancellation the resume test uses to stop
/// its runs, so they take turns.
static LIVE_RUNS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn ensure_stream_topic(producer: &IggyProducer, stream: &str, topic: &str) {
    let client = producer.client();
    let _ = client.create_stream(stream).await;
    let stream_id = Identifier::from_str_value(stream).unwrap();
    let _ = client
        .create_topic(
            &stream_id,
            topic,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await;
}

/// Produce `messages` (key -> "key=value" payload) through the Iggy target. A
/// shared `db_path` across calls lets reconciliation skip unchanged messages.
async fn declare(
    producer: &IggyProducer,
    db_path: &std::path::Path,
    stream: &str,
    topic: &str,
    messages: Vec<(&str, &str)>,
) {
    let producer = producer.clone();
    let stream = stream.to_string();
    let topic = topic.to_string();
    let messages: Vec<(String, String)> = messages
        .into_iter()
        .map(|(k, v)| (k.to_string(), format!("{k}={v}")))
        .collect();
    let app = app_with_producer("IggySourcePopulate", db_path, &producer).await;
    app.run(move |ctx| {
        let stream = stream.clone();
        let topic = topic.clone();
        let messages = messages.clone();
        async move {
            let target = iggy::mount_iggy_topic_target(
                &ctx,
                &PRODUCER,
                stream,
                topic,
                iggy::IggyTopicOptions {
                    partition: PARTITION,
                    ..Default::default()
                },
            )
            .await?;
            for (k, payload) in &messages {
                target.declare_message(&ctx, k, payload)?;
            }
            Ok(())
        }
    })
    .await
    .unwrap();
}

/// `key_fn` recovering the key from a `"<key>=<value>"` payload.
fn key_fn() -> iggy::IggyKeyFn {
    Arc::new(|message: &IggyReceivedMessage| {
        message
            .payload_str()
            .split_once('=')
            .map(|(k, _)| k.to_string())
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iggy_source_catch_up_scan_compacts_log() -> Result<()> {
    let Ok(conn) = std::env::var("IGGY_CONNECTION_STRING") else {
        eprintln!("skipping live Iggy source test; IGGY_CONNECTION_STRING is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let stream = format!("coco_src_scan_{nonce}");
    let topic = "rows";

    let producer = IggyProducer::connect(&conn).await?;
    ensure_stream_topic(&producer, &stream, topic).await;

    let tmp = tempfile::tempdir().unwrap();
    let pop_db = tmp.path().join("populate_db");
    // Produce k1=v1, k2=v2; then update k1 -> v1b (k2 unchanged, skipped).
    declare(
        &producer,
        &pop_db,
        &stream,
        topic,
        vec![("k1", "v1"), ("k2", "v2")],
    )
    .await;
    declare(
        &producer,
        &pop_db,
        &stream,
        topic,
        vec![("k1", "v1b"), ("k2", "v2")],
    )
    .await;

    let consumer = IggyConsumer::connect(&conn).await?;
    let processed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let app = App::builder("IggySourceScan")
        .db_path(tmp.path().join("source_db"))
        .build()
        .await?;
    app.run({
        let processed = processed.clone();
        let consumer = consumer.clone();
        let stream = stream.clone();
        move |ctx| {
            let processed = processed.clone();
            let consumer = consumer.clone();
            let stream = stream.clone();
            async move {
                let feed = iggy::topic_as_map(&consumer, GROUP, stream, topic, key_fn());
                ctx.mount_each_live(
                    &"messages",
                    feed,
                    move |_ctx, message: IggyReceivedMessage| {
                        let processed = processed.clone();
                        async move {
                            processed.lock().unwrap().push(message.payload_str());
                            Ok(())
                        }
                    },
                )
                .await
            }
        }
    })
    .await?;

    let mut got = processed.lock().unwrap().clone();
    got.sort();
    assert_eq!(
        got,
        vec!["k1=v1b".to_string(), "k2=v2".to_string()],
        "catch-up scan should compact to the latest payload per key"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iggy_source_live_watch_tails_new_messages() -> Result<()> {
    let Ok(conn) = std::env::var("IGGY_CONNECTION_STRING") else {
        eprintln!("skipping live Iggy source watch test; IGGY_CONNECTION_STRING is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let stream = format!("coco_src_watch_{nonce}");
    let topic = "rows";

    let producer = IggyProducer::connect(&conn).await?;
    ensure_stream_topic(&producer, &stream, topic).await;

    let tmp = tempfile::tempdir().unwrap();
    let pop_db = tmp.path().join("populate_db");
    declare(&producer, &pop_db, &stream, topic, vec![("k1", "v1")]).await;

    let _live = LIVE_RUNS.lock().await;
    let consumer = IggyConsumer::connect(&conn).await?;
    let processed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let app = App::builder("IggySourceWatch")
        .db_path(tmp.path().join("source_db"))
        .build()
        .await?;
    let handle = app
        .start_update_with_options(
            UpdateOptions {
                full_reprocess: false,
                live: true,
                ..UpdateOptions::default()
            },
            {
                let processed = processed.clone();
                let consumer = consumer.clone();
                let stream = stream.clone();
                move |ctx| {
                    let processed = processed.clone();
                    let consumer = consumer.clone();
                    let stream = stream.clone();
                    async move {
                        let feed = iggy::topic_as_map(&consumer, GROUP, stream, topic, key_fn());
                        ctx.mount_each_live(
                            &"messages",
                            feed,
                            move |_ctx, message: IggyReceivedMessage| {
                                let processed = processed.clone();
                                async move {
                                    processed.lock().unwrap().push(message.payload_str());
                                    Ok(())
                                }
                            },
                        )
                        .await
                    }
                }
            },
        )
        .unwrap();

    // Produce a NEW message after the source is live; `watch` must tail it.
    declare(
        &producer,
        &pop_db,
        &stream,
        topic,
        vec![("k1", "v1"), ("k2", "v2")],
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        {
            let got = processed.lock().unwrap();
            if got.iter().any(|v| v == "k1=v1") && got.iter().any(|v| v == "k2=v2") {
                break;
            }
        }
        if Instant::now() > deadline {
            let got = processed.lock().unwrap().clone();
            panic!("live watch did not tail the new message in time; processed={got:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let _ = app.drop_state().await;
    let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iggy_source_stream_reads_all_payloads_keyless() -> Result<()> {
    let Ok(conn) = std::env::var("IGGY_CONNECTION_STRING") else {
        eprintln!("skipping live Iggy stream test; IGGY_CONNECTION_STRING is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let stream = format!("coco_src_stream_{nonce}");
    let topic = "rows";

    let producer = IggyProducer::connect(&conn).await?;
    ensure_stream_topic(&producer, &stream, topic).await;

    let tmp = tempfile::tempdir().unwrap();
    let pop_db = tmp.path().join("populate_db");
    // Three distinct payloads; the keyless stream reads all (no key_fn needed).
    declare(
        &producer,
        &pop_db,
        &stream,
        topic,
        vec![("k1", "v1"), ("k2", "v2"), ("k3", "v3")],
    )
    .await;

    let consumer = IggyConsumer::connect(&conn).await?;
    let processed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let app = App::builder("IggySourceStream")
        .db_path(tmp.path().join("source_db"))
        .build()
        .await?;
    app.run({
        let processed = processed.clone();
        let consumer = consumer.clone();
        let stream = stream.clone();
        move |ctx| {
            let processed = processed.clone();
            let consumer = consumer.clone();
            let stream = stream.clone();
            async move {
                let feed = iggy::topic_as_stream(&consumer, GROUP, stream, topic);
                ctx.mount_each_live(
                    &"stream",
                    feed,
                    move |_ctx, message: IggyReceivedMessage| {
                        let processed = processed.clone();
                        async move {
                            processed.lock().unwrap().push(message.payload_str());
                            Ok(())
                        }
                    },
                )
                .await
            }
        }
    })
    .await?;

    let mut got = processed.lock().unwrap().clone();
    got.sort();
    assert_eq!(
        got,
        vec![
            "k1=v1".to_string(),
            "k2=v2".to_string(),
            "k3=v3".to_string()
        ],
        "the keyless stream should read every payload in offset order"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iggy_source_stream_reads_all_partitions() -> Result<()> {
    let Ok(conn) = std::env::var("IGGY_CONNECTION_STRING") else {
        eprintln!("skipping live Iggy multi-partition test; IGGY_CONNECTION_STRING is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let stream = format!("coco_src_mpart_{nonce}");
    let topic = "rows";

    // Create a 3-partition topic and discover its actual partition IDs.
    let producer = IggyProducer::connect(&conn).await?;
    let client = producer.client();
    let _ = client.create_stream(&stream).await;
    let stream_id = Identifier::from_str_value(&stream).unwrap();
    let _ = client
        .create_topic(
            &stream_id,
            topic,
            3,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await;
    let topic_id = Identifier::from_str_value(topic).unwrap();
    let details = client
        .get_topic(&stream_id, &topic_id)
        .await
        .unwrap()
        .expect("topic exists");
    let pids: Vec<u32> = if details.partitions.is_empty() {
        (1..=details.partitions_count).collect()
    } else {
        details.partitions.iter().map(|p| p.id).collect()
    };
    assert!(
        pids.len() >= 2,
        "expected a multi-partition topic, got {pids:?}"
    );

    // Produce one message to each discovered partition via the target.
    let tmp = tempfile::tempdir().unwrap();
    let mut expected = Vec::new();
    for (i, pid) in pids.iter().enumerate() {
        let value = format!("p{pid}msg{i}");
        expected.push(value.clone());
        let producer = producer.clone();
        let stream = stream.clone();
        let pid = *pid;
        let app = app_with_producer(
            "IggyMpartPopulate",
            &tmp.path().join(format!("pop_{i}")),
            &producer,
        )
        .await;
        app.run(move |ctx| {
            let stream = stream.clone();
            let value = value.clone();
            async move {
                let target = iggy::mount_iggy_topic_target(
                    &ctx,
                    &PRODUCER,
                    stream,
                    topic,
                    iggy::IggyTopicOptions {
                        partition: pid,
                        ..Default::default()
                    },
                )
                .await?;
                target.declare_message(&ctx, &format!("k{i}"), &value)?;
                Ok(())
            }
        })
        .await?;
    }

    // The keyless stream must read messages from every partition.
    let consumer = IggyConsumer::connect(&conn).await?;
    let processed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let app = App::builder("IggyMpartStream")
        .db_path(tmp.path().join("source_db"))
        .build()
        .await?;
    app.run({
        let processed = processed.clone();
        let consumer = consumer.clone();
        let stream = stream.clone();
        move |ctx| {
            let processed = processed.clone();
            let consumer = consumer.clone();
            let stream = stream.clone();
            async move {
                let feed = iggy::topic_as_stream(&consumer, GROUP, stream, topic);
                ctx.mount_each_live(
                    &"stream",
                    feed,
                    move |_ctx, message: IggyReceivedMessage| {
                        let processed = processed.clone();
                        async move {
                            processed.lock().unwrap().push(message.payload_str());
                            Ok(())
                        }
                    },
                )
                .await
            }
        }
    })
    .await?;

    let mut got = processed.lock().unwrap().clone();
    got.sort();
    expected.sort();
    assert_eq!(
        got,
        expected,
        "the keyless stream should read one message from each of the {} partitions",
        pids.len()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iggy_source_consumer_group_resumes_from_stored_offset() -> Result<()> {
    let Ok(conn) = std::env::var("IGGY_CONNECTION_STRING") else {
        eprintln!("skipping live Iggy consumer-group test; IGGY_CONNECTION_STRING is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let stream = format!("coco_src_group_{nonce}");
    let topic = "rows";

    let producer = IggyProducer::connect(&conn).await?;
    ensure_stream_topic(&producer, &stream, topic).await;
    let tmp = tempfile::tempdir().unwrap();
    let pop_db = tmp.path().join("populate_db");
    declare(&producer, &pop_db, &stream, topic, vec![("k1", "v1")]).await;

    let consumer = IggyConsumer::connect(&conn).await?;
    let source_db = tmp.path().join("source_db");
    let _live = LIVE_RUNS.lock().await;

    // A live run: processes everything (the catch-up scan), stores the group's
    // offset on the server, then stops. Stopping goes through the engine's
    // process-wide cancellation, the path Ctrl-C takes, which keeps the app's
    // state (`drop_state` would wipe it).
    let run_live = |processed: Arc<Mutex<Vec<String>>>, until: Vec<&'static str>| {
        let consumer = consumer.clone();
        let stream = stream.clone();
        let source_db = source_db.clone();
        async move {
            let app = App::builder("IggySourceGroup")
                .db_path(&source_db)
                .build()
                .await?;
            let handle = app.start_update_with_options(
                UpdateOptions {
                    full_reprocess: false,
                    live: true,
                    ..UpdateOptions::default()
                },
                {
                    let processed = processed.clone();
                    move |ctx| {
                        let processed = processed.clone();
                        let consumer = consumer.clone();
                        let stream = stream.clone();
                        async move {
                            let feed = iggy::topic_as_map_with_options(
                                &consumer,
                                GROUP,
                                stream,
                                topic,
                                key_fn(),
                                iggy::IggySourceOptions {
                                    partition: PARTITION,
                                    poll: iggy::IggyPollOptions {
                                        batch_length: 1,
                                        poll_interval: Duration::from_millis(50),
                                        ..Default::default()
                                    },
                                    is_deletion: None,
                                },
                            );
                            ctx.mount_each_live(
                                &"messages",
                                feed,
                                move |_ctx, message: IggyReceivedMessage| {
                                    let processed = processed.clone();
                                    async move {
                                        processed.lock().unwrap().push(message.payload_str());
                                        Ok(())
                                    }
                                },
                            )
                            .await
                        }
                    }
                },
            )?;
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let got = processed.lock().unwrap().clone();
                if until.iter().all(|v| got.iter().any(|g| g == v)) {
                    break;
                }
                if Instant::now() > deadline {
                    panic!("live run did not process {until:?} in time; processed={got:?}");
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            // A little grace so the offset store after the batch lands.
            tokio::time::sleep(Duration::from_millis(500)).await;
            cocoindex_core::engine::runtime::cancel_all();
            let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
            cocoindex_core::engine::runtime::reset_global_cancellation();
            Ok::<_, cocoindex::Error>(())
        }
    };

    let first: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    run_live(first.clone(), vec!["k1=v1"]).await?;
    assert_eq!(first.lock().unwrap().clone(), vec!["k1=v1".to_string()]);

    // A message arrives while no run is live; the next live run resumes from
    // the group's stored offset: it replays only the new message.
    declare(
        &producer,
        &pop_db,
        &stream,
        topic,
        vec![("k1", "v1"), ("k2", "v2")],
    )
    .await;
    let second: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    run_live(second.clone(), vec!["k2=v2"]).await?;
    assert_eq!(
        second.lock().unwrap().clone(),
        vec!["k2=v2".to_string()],
        "a live run with a consumer group resumes from the stored offset"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iggy_source_init_retries_wait_for_a_late_topic() -> Result<()> {
    let Ok(conn) = std::env::var("IGGY_CONNECTION_STRING") else {
        eprintln!("skipping live Iggy init-retry test; IGGY_CONNECTION_STRING is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let stream = format!("coco_src_late_{nonce}");
    let topic = "rows";
    let producer = IggyProducer::connect(&conn).await?;
    let consumer = IggyConsumer::connect(&conn).await?;
    let tmp = tempfile::tempdir().unwrap();

    // Without retries a missing topic is an error at once.
    let app = App::builder("IggySourceLateNoRetry")
        .db_path(tmp.path().join("db0"))
        .build()
        .await?;
    let err = app
        .run({
            let consumer = consumer.clone();
            let stream = stream.clone();
            move |ctx| {
                let consumer = consumer.clone();
                let stream = stream.clone();
                async move {
                    let feed = iggy::topic_as_stream(&consumer, GROUP, stream, topic);
                    ctx.mount_each_live(
                        &"stream",
                        feed,
                        |_ctx, _message: IggyReceivedMessage| async { Ok(()) },
                    )
                    .await
                }
            }
        })
        .await
        .expect_err("a missing topic without init retries is an error");
    assert!(err.to_string().contains("not found"), "{err}");

    // With retries the source waits for the topic the producer creates.
    let create = {
        let producer = producer.clone();
        let stream = stream.clone();
        let pop_db = tmp.path().join("pop");
        async move {
            tokio::time::sleep(Duration::from_millis(600)).await;
            ensure_stream_topic(&producer, &stream, topic).await;
            declare(&producer, &pop_db, &stream, topic, vec![("k1", "v1")]).await;
        }
    };
    let processed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let app = App::builder("IggySourceLateRetry")
        .db_path(tmp.path().join("db1"))
        .build()
        .await?;
    let read = app.run({
        let processed = processed.clone();
        let consumer = consumer.clone();
        let stream = stream.clone();
        move |ctx| {
            let processed = processed.clone();
            let consumer = consumer.clone();
            let stream = stream.clone();
            async move {
                let feed = iggy::topic_as_stream_with_options(
                    &consumer,
                    GROUP,
                    stream,
                    topic,
                    iggy::IggyPollOptions {
                        init_retries: 20,
                        init_retry_interval: Duration::from_millis(200),
                        ..Default::default()
                    },
                );
                ctx.mount_each_live(
                    &"stream",
                    feed,
                    move |_ctx, message: IggyReceivedMessage| {
                        let processed = processed.clone();
                        async move {
                            processed.lock().unwrap().push(message.payload_str());
                            Ok(())
                        }
                    },
                )
                .await
            }
        }
    });
    let (_, result) = tokio::join!(create, read);
    result?;
    assert_eq!(processed.lock().unwrap().clone(), vec!["k1=v1".to_string()]);
    Ok(())
}

/// Records what a `LiveStream<Vec<u8>>` delivered and when it became ready.
#[derive(Default)]
struct PayloadRecorder {
    payloads: Mutex<Vec<Vec<u8>>>,
    ready: std::sync::atomic::AtomicBool,
}

#[cocoindex::async_trait]
impl cocoindex::LiveStreamSubscriber<Vec<u8>> for PayloadRecorder {
    async fn send(&self, message: Vec<u8>) -> Result<cocoindex::ReadyHandle> {
        self.payloads.lock().unwrap().push(message);
        Ok(cocoindex::ReadyHandle::immediate())
    }

    async fn mark_ready(&self) {
        self.ready.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// `topic_as_stream(..).payloads()` watched directly as a `LiveStream`: every
/// payload is delivered and readiness is signalled once the messages that
/// existed when the watch started are processed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iggy_stream_payloads_watch_delivers_values_and_marks_ready() -> Result<()> {
    let Ok(conn) = std::env::var("IGGY_CONNECTION_STRING") else {
        eprintln!("skipping live Iggy payload stream test; IGGY_CONNECTION_STRING is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let stream = format!("coco_src_payloads_{nonce}");
    let topic = "rows";
    let producer = IggyProducer::connect(&conn).await?;
    ensure_stream_topic(&producer, &stream, topic).await;
    let tmp = tempfile::tempdir().unwrap();
    declare(
        &producer,
        &tmp.path().join("populate_db"),
        &stream,
        topic,
        vec![("k1", "v1"), ("k2", "v2")],
    )
    .await;

    let consumer = IggyConsumer::connect(&conn).await?;
    let payloads = iggy::topic_as_stream(&consumer, GROUP, stream, topic).payloads();
    let recorder = PayloadRecorder::default();
    let outcome = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::select! {
            r = cocoindex::LiveStream::watch(&payloads, &recorder) => r,
            _ = async {
                while !recorder.ready.load(std::sync::atomic::Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            } => Ok(()),
        }
    })
    .await;
    assert!(outcome.is_ok(), "the stream did not become ready in time");
    outcome.unwrap()?;
    let mut got: Vec<String> = recorder
        .payloads
        .lock()
        .unwrap()
        .iter()
        .map(|p| String::from_utf8_lossy(p).into_owned())
        .collect();
    got.sort();
    assert_eq!(got, vec!["k1=v1".to_string(), "k2=v2".to_string()]);
    Ok(())
}
