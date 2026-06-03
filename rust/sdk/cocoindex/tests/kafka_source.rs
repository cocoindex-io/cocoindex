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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cocoindex::{App, Result, UpdateOptions, kafka};
use rskafka::client::ClientBuilder;
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::record::Record;

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
                ..UpdateOptions::default()
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

#[tokio::test]
async fn kafka_source_reads_all_partitions() -> Result<()> {
    let Some(brokers) = brokers() else {
        eprintln!("skipping live Kafka multi-partition test; KAFKA_BOOTSTRAP_SERVERS is not set");
        return Ok(());
    };
    let broker_refs: Vec<&str> = brokers.iter().map(String::as_str).collect();
    let topic = unique_topic("multipart");

    // A 3-partition topic, with one keyed record produced directly to each
    // partition (so the source must read all partitions, not just partition 0).
    let producer = kafka::KafkaProducer::connect(&broker_refs).await?;
    producer.ensure_topic(&topic, 3).await?;

    let raw = ClientBuilder::new(brokers.clone())
        .build()
        .await
        .map_err(|e| cocoindex::Error::engine(format!("client: {e}")))?;
    for (pid, key, val) in [(0, "k0", "v0"), (1, "k1", "v1"), (2, "k2", "v2")] {
        let pc = raw
            .partition_client(topic.clone(), pid, UnknownTopicHandling::Retry)
            .await
            .map_err(|e| cocoindex::Error::engine(format!("partition_client: {e}")))?;
        let record = Record {
            key: Some(key.as_bytes().to_vec()),
            value: Some(val.as_bytes().to_vec()),
            headers: BTreeMap::new(),
            timestamp: rskafka::chrono::DateTime::from_timestamp(0, 0).unwrap(),
        };
        pc.produce(vec![record], Compression::NoCompression)
            .await
            .map_err(|e| cocoindex::Error::engine(format!("produce p{pid}: {e}")))?;
    }

    let consumer = kafka::KafkaConsumer::connect(&broker_refs).await?;
    let processed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let tmp = tempfile::tempdir().unwrap();
    let app = App::builder("KafkaSourceMultiPart")
        .db_path(tmp.path().join("db"))
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

    let mut got = processed.lock().unwrap().clone();
    got.sort();
    assert_eq!(
        got,
        vec!["v0".to_string(), "v1".to_string(), "v2".to_string()],
        "the catch-up scan should read records from all three partitions"
    );
    Ok(())
}

#[tokio::test]
async fn kafka_source_stream_reads_all_payloads_keyless() -> Result<()> {
    let Some(brokers) = brokers() else {
        eprintln!("skipping live Kafka stream test; KAFKA_BOOTSTRAP_SERVERS is not set");
        return Ok(());
    };
    let broker_refs: Vec<&str> = brokers.iter().map(String::as_str).collect();
    let topic = unique_topic("stream");

    // Produce NULL-KEY records across two partitions. `topic_as_map` would skip
    // these (no key); the keyless `topic_as_stream` must read every one.
    let producer = kafka::KafkaProducer::connect(&broker_refs).await?;
    producer.ensure_topic(&topic, 2).await?;
    let raw = ClientBuilder::new(brokers.clone())
        .build()
        .await
        .map_err(|e| cocoindex::Error::engine(format!("client: {e}")))?;
    for (pid, val) in [(0, "a"), (0, "b"), (1, "c")] {
        let pc = raw
            .partition_client(topic.clone(), pid, UnknownTopicHandling::Retry)
            .await
            .map_err(|e| cocoindex::Error::engine(format!("partition_client: {e}")))?;
        let record = Record {
            key: None,
            value: Some(val.as_bytes().to_vec()),
            headers: BTreeMap::new(),
            timestamp: rskafka::chrono::DateTime::from_timestamp(0, 0).unwrap(),
        };
        pc.produce(vec![record], Compression::NoCompression)
            .await
            .map_err(|e| cocoindex::Error::engine(format!("produce p{pid}: {e}")))?;
    }

    let consumer = kafka::KafkaConsumer::connect(&broker_refs).await?;
    let processed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let tmp = tempfile::tempdir().unwrap();
    let app = App::builder("KafkaSourceStream")
        .db_path(tmp.path().join("db"))
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
                let feed = kafka::topic_as_stream(&consumer, topic);
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
        vec!["a".to_string(), "b".to_string(), "c".to_string()],
        "the keyless stream should read every payload across partitions, including null-key records"
    );
    Ok(())
}
