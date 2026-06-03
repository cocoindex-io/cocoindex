//! Live-Kafka integration test for the `kafka::KafkaTopicTarget`.
//!
//! Skips gracefully when `KAFKA_BOOTSTRAP_SERVERS` is unset. Run with a broker
//! (Apache Kafka or Redpanda) on localhost:
//!   KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
//!     cargo test -p cocoindex --features kafka --test kafka_target
//!
//! Strategy: because a Kafka topic is an append-only log, "did we re-produce?"
//! is observable via the high-watermark / record count. We run a real
//! source->topic pipeline repeatedly, mutating the declared messages between
//! runs, and consume the topic back to assert incremental behavior:
//!   T1 declare 2 msgs        -> 2 records produced
//!   T2 re-run unchanged      -> still 2 (nothing re-produced)
//!   T3 change 1 value        -> 3 total; latest record for that key = new value
//!   T4 stop declaring 1 msg  -> 4 total; latest record for that key = tombstone
#![cfg(feature = "kafka")]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex::{App, Result, kafka};
use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::{Client, ClientBuilder};

/// Consume every record on partition 0 from the beginning. Returns the decoded
/// `(key, value)` pairs in log order plus the high watermark.
async fn consume_all(client: &Client, topic: &str) -> (Vec<(String, Option<String>)>, i64) {
    let pc = client
        .partition_client(topic.to_string(), 0, UnknownTopicHandling::Retry)
        .await
        .unwrap();
    let mut out = Vec::new();
    let mut offset = 0i64;
    loop {
        let (records, high_watermark) = pc.fetch_records(offset, 1..1_000_000, 500).await.unwrap();
        if records.is_empty() {
            return (out, high_watermark);
        }
        for r in &records {
            let key = r
                .record
                .key
                .as_ref()
                .map(|k| String::from_utf8_lossy(k).to_string())
                .unwrap_or_default();
            let value = r
                .record
                .value
                .as_ref()
                .map(|v| String::from_utf8_lossy(v).to_string());
            out.push((key, value));
            offset = r.offset + 1;
        }
        if offset >= high_watermark {
            return (out, high_watermark);
        }
    }
}

/// Latest produced value for `key` in log order (`None` = tombstone / never seen).
fn latest(records: &[(String, Option<String>)], key: &str) -> Option<Option<String>> {
    records
        .iter()
        .filter(|(k, _)| k == key)
        .next_back()
        .map(|(_, v)| v.clone())
}

#[tokio::test]
async fn kafka_target_produces_skips_updates_and_tombstones_when_available() -> Result<()> {
    let Ok(brokers) = std::env::var("KAFKA_BOOTSTRAP_SERVERS") else {
        eprintln!("skipping live Kafka target test; KAFKA_BOOTSTRAP_SERVERS is not set");
        return Ok(());
    };
    let broker_refs: Vec<&str> = brokers.split(',').collect();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let topic = format!("cocoindex_kafka_test_{nonce}");

    let producer = kafka::KafkaProducer::connect(&broker_refs).await?;
    producer.ensure_topic(&topic, 1).await?;

    // A raw consumer client to read the topic back.
    let consumer = ClientBuilder::new(broker_refs.iter().map(|s| s.to_string()).collect())
        .build()
        .await
        .map_err(|e| cocoindex::Error::engine(format!("consumer connect: {e}")))?;

    let tempdir = tempfile::tempdir().unwrap();
    let db_path = tempdir.path().join(".cocoindex_db");

    // Build + run a pipeline that declares the given messages on the topic.
    // db_path persists across runs so reconciliation sees prior tracking records.
    let run = |messages: Vec<(String, String)>| {
        let producer = producer.clone();
        let topic = topic.clone();
        let db_path = db_path.clone();
        async move {
            let app = App::builder("KafkaTargetTest")
                .db_path(&db_path)
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
    };

    let msg = |k: &str, v: &str| (k.to_string(), v.to_string());

    // --- T1: first run produces both messages ---
    run(vec![msg("k1", "v1"), msg("k2", "v2")]).await;
    let (records, hw) = consume_all(&consumer, &topic).await;
    assert_eq!(hw, 2, "two records produced on first run");
    assert_eq!(latest(&records, "k1"), Some(Some("v1".to_string())));
    assert_eq!(latest(&records, "k2"), Some(Some("v2".to_string())));

    // --- T2: unchanged re-run produces nothing ---
    run(vec![msg("k1", "v1"), msg("k2", "v2")]).await;
    let (_records, hw) = consume_all(&consumer, &topic).await;
    assert_eq!(hw, 2, "unchanged messages are not re-produced");

    // --- T3: change one value -> only that message is re-produced ---
    run(vec![msg("k1", "v1-updated"), msg("k2", "v2")]).await;
    let (records, hw) = consume_all(&consumer, &topic).await;
    assert_eq!(hw, 3, "only the changed message is produced");
    assert_eq!(latest(&records, "k1"), Some(Some("v1-updated".to_string())));

    // --- T4: stop declaring k1 -> a tombstone is produced for it ---
    run(vec![msg("k2", "v2")]).await;
    let (records, hw) = consume_all(&consumer, &topic).await;
    assert_eq!(hw, 4, "a tombstone is produced for the removed message");
    assert_eq!(
        latest(&records, "k1"),
        Some(None),
        "removed message's latest record is a tombstone (null value)"
    );

    Ok(())
}

#[tokio::test]
async fn kafka_target_custom_delete_value_when_available() -> Result<()> {
    let Ok(brokers) = std::env::var("KAFKA_BOOTSTRAP_SERVERS") else {
        eprintln!("skipping live Kafka custom-delete test; KAFKA_BOOTSTRAP_SERVERS is not set");
        return Ok(());
    };
    let broker_refs: Vec<&str> = brokers.split(',').collect();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let topic = format!("cocoindex_kafka_delval_test_{nonce}");

    let producer = kafka::KafkaProducer::connect(&broker_refs).await?;
    producer.ensure_topic(&topic, 1).await?;
    let consumer = ClientBuilder::new(broker_refs.iter().map(|s| s.to_string()).collect())
        .build()
        .await
        .map_err(|e| cocoindex::Error::engine(format!("consumer connect: {e}")))?;

    let tempdir = tempfile::tempdir().unwrap();
    let db_path = tempdir.path().join(".cocoindex_db");

    // A delete emits `deleted:<key>` as the value instead of a null tombstone.
    let run = |messages: Vec<(String, String)>| {
        let producer = producer.clone();
        let topic = topic.clone();
        let db_path = db_path.clone();
        async move {
            let app = App::builder("KafkaCustomDeleteTest")
                .db_path(&db_path)
                .build()
                .await
                .unwrap();
            app.run(move |ctx| {
                let producer = producer.clone();
                let topic = topic.clone();
                let messages = messages.clone();
                async move {
                    let options = kafka::KafkaTopicOptions {
                        deletion_value_fn: Some(Arc::new(|key: &str| {
                            format!("deleted:{key}").into_bytes()
                        })),
                        ..Default::default()
                    };
                    let target = kafka::mount_kafka_topic_target(&ctx, &producer, topic, options)?;
                    for (k, v) in &messages {
                        target.declare_message(&ctx, k, v)?;
                    }
                    Ok(())
                }
            })
            .await
            .unwrap();
        }
    };

    let msg = |k: &str, v: &str| (k.to_string(), v.to_string());

    run(vec![msg("k1", "v1")]).await;
    // Stop declaring k1 -> a delete is produced using the custom value, not null.
    run(vec![]).await;
    let (records, _hw) = consume_all(&consumer, &topic).await;
    assert_eq!(
        latest(&records, "k1"),
        Some(Some("deleted:k1".to_string())),
        "removed message's latest record uses the custom deletion value"
    );

    Ok(())
}
