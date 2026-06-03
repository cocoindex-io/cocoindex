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

use cocoindex::iggy::{self, IggyConsumer, IggyProducer};
use cocoindex::{App, Result, UpdateOptions};
use iggy::prelude::{
    CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize, StreamClient, TopicClient,
};

const PARTITION: u32 = 0;

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
    let app = App::builder("IggySourcePopulate")
        .db_path(db_path)
        .build()
        .await
        .unwrap();
    app.run(move |ctx| {
        let producer = producer.clone();
        let stream = stream.clone();
        let topic = topic.clone();
        let messages = messages.clone();
        async move {
            let target = iggy::mount_iggy_topic_target(
                &ctx,
                &producer,
                stream,
                topic,
                iggy::IggyTopicOptions {
                    partition: PARTITION,
                    ..Default::default()
                },
            )?;
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
    Arc::new(|payload: &[u8]| {
        let s = String::from_utf8_lossy(payload);
        s.split_once('=').map(|(k, _)| k.to_string())
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
                let feed = iggy::topic_as_map(&consumer, stream, topic, key_fn());
                ctx.mount_each_live(&"messages", feed, move |_ctx, value: Vec<u8>| {
                    let processed = processed.clone();
                    async move {
                        processed
                            .lock()
                            .unwrap()
                            .push(String::from_utf8_lossy(&value).into_owned());
                        Ok(())
                    }
                })
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
                        let feed = iggy::topic_as_map(&consumer, stream, topic, key_fn());
                        ctx.mount_each_live(&"messages", feed, move |_ctx, value: Vec<u8>| {
                            let processed = processed.clone();
                            async move {
                                processed
                                    .lock()
                                    .unwrap()
                                    .push(String::from_utf8_lossy(&value).into_owned());
                                Ok(())
                            }
                        })
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
                let feed = iggy::topic_as_stream(&consumer, stream, topic);
                ctx.mount_each_live(&"stream", feed, move |_ctx, value: Vec<u8>| {
                    let processed = processed.clone();
                    async move {
                        processed
                            .lock()
                            .unwrap()
                            .push(String::from_utf8_lossy(&value).into_owned());
                        Ok(())
                    }
                })
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
        let app = App::builder("IggyMpartPopulate")
            .db_path(tmp.path().join(format!("pop_{i}")))
            .build()
            .await?;
        app.run(move |ctx| {
            let producer = producer.clone();
            let stream = stream.clone();
            let value = value.clone();
            async move {
                let target = iggy::mount_iggy_topic_target(
                    &ctx,
                    &producer,
                    stream,
                    topic,
                    iggy::IggyTopicOptions {
                        partition: pid,
                        ..Default::default()
                    },
                )?;
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
                let feed = iggy::topic_as_stream(&consumer, stream, topic);
                ctx.mount_each_live(&"stream", feed, move |_ctx, value: Vec<u8>| {
                    let processed = processed.clone();
                    async move {
                        processed
                            .lock()
                            .unwrap()
                            .push(String::from_utf8_lossy(&value).into_owned());
                        Ok(())
                    }
                })
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
