//! Live-Kafka integration test for the `kafka` source (`topic_as_map`).
//!
//! Skips gracefully when `KAFKA_BOOTSTRAP_SERVERS` is unset. Run with a broker
//! (Apache Kafka or Redpanda) on localhost:
//!   KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
//!     cargo test -p cocoindex --features kafka --test kafka_source
//!
//! The topic is populated through the (already-tested) Kafka *target* — declaring
//! messages produces records, dropping a message produces a tombstone — then the
//! *source* reads them back via `Ctx::mount_each_live`:
//!   * catch-up (`scan`) compacts the log to the latest value per key, dropping
//!     tombstoned keys;
//!   * live (`watch`) tails new records produced after the source started.
#![cfg(feature = "kafka")]

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cocoindex::{App, Result, UpdateOptions, kafka};

fn brokers() -> Option<Vec<String>> {
    let raw = std::env::var("KAFKA_BOOTSTRAP_SERVERS").ok()?;
    Some(raw.split(',').map(|s| s.to_string()).collect())
}

fn unique_topic(label: &str) -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("cocoindex_kafka_src_{label}_{nonce}")
}

/// Populate a topic by declaring `messages` through the Kafka target. A shared
/// `db_path` across calls lets reconciliation drop messages (→ tombstones).
async fn declare(
    producer: &kafka::KafkaProducer,
    db_path: &std::path::Path,
    topic: &str,
    messages: Vec<(&str, &str)>,
) {
    let producer = producer.clone();
    let topic = topic.to_string();
    let messages: Vec<(String, String)> = messages
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let app = App::builder("KafkaSourcePopulate")
        .db_path(db_path)
        .build()
        .await
        .unwrap();
    app.run(move |ctx| {
        let producer = producer.clone();
        let topic = topic.clone();
        let messages = messages.clone();
        async move {
            let target = kafka::mount_kafka_topic_target(
                &ctx,
                &producer,
                topic,
                kafka::KafkaTopicOptions::default(),
            )?;
            for (k, v) in &messages {
                target.declare_message(&ctx, k, v)?;
            }
            Ok(())
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn kafka_source_catch_up_scan_compacts_log() -> Result<()> {
    let Some(brokers) = brokers() else {
        eprintln!("skipping live Kafka source test; KAFKA_BOOTSTRAP_SERVERS is not set");
        return Ok(());
    };
    let broker_refs: Vec<&str> = brokers.iter().map(String::as_str).collect();
    let topic = unique_topic("scan");

    let producer = kafka::KafkaProducer::connect(&broker_refs).await?;
    producer.ensure_topic(&topic, 1).await?;

    let tmp = tempfile::tempdir().unwrap();
    let pop_db = tmp.path().join("populate_db");
    // Produce: k1=v1, k2=v2; then update k1->v1b and drop k2 (tombstone).
    declare(&producer, &pop_db, &topic, vec![("k1", "v1"), ("k2", "v2")]).await;
    declare(&producer, &pop_db, &topic, vec![("k1", "v1b")]).await;

    // Catch-up source run: scan() compacts to the latest value per key, and the
    // tombstoned k2 is gone — so only "v1b" is processed.
    let consumer = kafka::KafkaConsumer::connect(&broker_refs).await?;
    let processed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let app = App::builder("KafkaSourceScan")
        .db_path(tmp.path().join("source_db"))
        .build()
        .await?;
    app.run({
        let processed = processed.clone();
        let consumer = consumer.clone();
        let topic = topic.clone();
        move |ctx| {
            let processed = processed.clone();
            let consumer = consumer.clone();
            let topic = topic.clone();
            async move {
                let feed = kafka::topic_as_map(&consumer, topic);
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

    let got = processed.lock().unwrap().clone();
    assert_eq!(
        got,
        vec!["v1b".to_string()],
        "catch-up scan should compact to the latest value per key and drop tombstones"
    );
    Ok(())
}

#[tokio::test]
async fn kafka_source_live_watch_tails_new_records() -> Result<()> {
    let Some(brokers) = brokers() else {
        eprintln!("skipping live Kafka source watch test; KAFKA_BOOTSTRAP_SERVERS is not set");
        return Ok(());
    };
    let broker_refs: Vec<&str> = brokers.iter().map(String::as_str).collect();
    let topic = unique_topic("watch");

    let producer = kafka::KafkaProducer::connect(&broker_refs).await?;
    producer.ensure_topic(&topic, 1).await?;

    let tmp = tempfile::tempdir().unwrap();
    let pop_db = tmp.path().join("populate_db");
    // One record exists before the source starts (picked up by the catch-up scan).
    declare(&producer, &pop_db, &topic, vec![("k1", "v1")]).await;

    let consumer = kafka::KafkaConsumer::connect(&broker_refs).await?;
    let processed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let app = App::builder("KafkaSourceWatch")
        .db_path(tmp.path().join("source_db"))
        .build()
        .await?;
    let handle = app
        .start_update_with_options(
            UpdateOptions {
                full_reprocess: false,
                live: true,
            },
            {
                let processed = processed.clone();
                let consumer = consumer.clone();
                let topic = topic.clone();
                move |ctx| {
                    let processed = processed.clone();
                    let consumer = consumer.clone();
                    let topic = topic.clone();
                    async move {
                        let feed = kafka::topic_as_map(&consumer, topic);
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

    // Produce a NEW record after the source is live; `watch` must tail it.
    declare(&producer, &pop_db, &topic, vec![("k1", "v1"), ("k2", "v2")]).await;

    // Poll until both the catch-up value and the live-tailed value arrive.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        {
            let got = processed.lock().unwrap();
            if got.iter().any(|v| v == "v1") && got.iter().any(|v| v == "v2") {
                break;
            }
        }
        if std::time::Instant::now() > deadline {
            let got = processed.lock().unwrap().clone();
            panic!("live watch did not tail the new record in time; processed={got:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let _ = app.drop_state().await;
    let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
    Ok(())
}
