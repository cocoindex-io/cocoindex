//! Live-Iggy integration test for the `iggy::IggyTopicTarget`.
//!
//! Skips gracefully when `IGGY_CONNECTION_STRING` is unset. Run with an Apache
//! Iggy server on localhost:
//!   IGGY_CONNECTION_STRING=iggy://iggy:iggy@localhost:8090 \
//!     cargo test -p cocoindex --features iggy --test iggy_target
//!
//! Strategy: an Iggy topic is an append-only log, so "did we re-send?" is
//! observable by polling the topic back and counting messages. We run a real
//! declare->send pipeline repeatedly, mutating the declared messages between
//! runs, and poll the topic to assert incremental behavior. Messages carry a
//! self-identifying `"<key>=<value>"` payload (Iggy messages have no key field),
//! so the latest value per key is observable from the payloads alone.
#![cfg(feature = "iggy")]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex::iggy::{self, IggyProducer};
use cocoindex::{App, Result};
use iggy::prelude::{
    CompressionAlgorithm, Consumer, Identifier, IggyExpiry, MaxTopicSize, MessageClient,
    PollingStrategy, StreamClient, TopicClient,
};

const PARTITION: u32 = 0; // Iggy partitions are 0-indexed (matches the connector default).

/// Poll every message on the topic from the beginning, returning payloads in
/// log order.
async fn poll_all(producer: &IggyProducer, stream: &str, topic: &str) -> Vec<String> {
    let stream_id = Identifier::from_str_value(stream).unwrap();
    let topic_id = Identifier::from_str_value(topic).unwrap();
    let consumer = Consumer::new(Identifier::numeric(1).unwrap());
    let polled = producer
        .client()
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(PARTITION),
            &consumer,
            &PollingStrategy::offset(0),
            10_000,
            false,
        )
        .await
        .expect("poll_messages");
    polled
        .messages
        .iter()
        .map(|m| String::from_utf8_lossy(&m.payload).to_string())
        .collect()
}

/// Latest payload for a `"<key>=..."` message in log order.
fn latest(payloads: &[String], key: &str) -> Option<String> {
    let prefix = format!("{key}=");
    payloads
        .iter()
        .filter(|p| p.starts_with(&prefix))
        .next_back()
        .cloned()
}

/// Create the (user-managed) stream + single-partition topic the target writes to.
async fn ensure_stream_topic(producer: &IggyProducer, stream: &str, topic: &str) {
    let client = producer.client();
    // Streams/topics are user-managed; create them as test setup (ignore
    // already-exists errors).
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

#[tokio::test]
async fn iggy_target_sends_skips_and_updates_when_available() -> Result<()> {
    let Ok(conn) = std::env::var("IGGY_CONNECTION_STRING") else {
        eprintln!("skipping live Iggy target test; IGGY_CONNECTION_STRING is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let stream = format!("coco_test_{nonce}");
    let topic = "rows".to_string();

    let producer = IggyProducer::connect(&conn).await?;
    ensure_stream_topic(&producer, &stream, &topic).await;

    let tempdir = tempfile::tempdir().unwrap();
    let db_path = tempdir.path().join(".cocoindex_db");

    // db_path persists across runs so reconciliation sees prior tracking records.
    let run = |messages: Vec<(String, String)>| {
        let producer = producer.clone();
        let stream = stream.clone();
        let topic = topic.clone();
        let db_path = db_path.clone();
        async move {
            let app = App::builder("IggyTargetTest")
                .db_path(&db_path)
                .build()
                .await
                .unwrap();
            app.run(move |ctx| {
                let producer = producer.clone();
                let stream = stream.clone();
                let topic = topic.clone();
                let messages = messages.clone();
                async move {
                    let options = iggy::IggyTopicOptions {
                        partition: PARTITION,
                        ..Default::default()
                    };
                    let target =
                        iggy::mount_iggy_topic_target(&ctx, &producer, stream, topic, options)?;
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

    // Self-identifying payloads, since Iggy messages have no key field.
    let msg = |k: &str, v: &str| (k.to_string(), format!("{k}={v}"));

    // --- T1: first run sends both messages ---
    run(vec![msg("k1", "v1"), msg("k2", "v2")]).await;
    let payloads = poll_all(&producer, &stream, &topic).await;
    assert_eq!(payloads.len(), 2, "two messages sent on first run");
    assert_eq!(latest(&payloads, "k1").as_deref(), Some("k1=v1"));
    assert_eq!(latest(&payloads, "k2").as_deref(), Some("k2=v2"));

    // --- T2: unchanged re-run sends nothing ---
    run(vec![msg("k1", "v1"), msg("k2", "v2")]).await;
    let payloads = poll_all(&producer, &stream, &topic).await;
    assert_eq!(payloads.len(), 2, "unchanged messages are not re-sent");

    // --- T3: change one value -> only that message is re-sent ---
    run(vec![msg("k1", "v1b"), msg("k2", "v2")]).await;
    let payloads = poll_all(&producer, &stream, &topic).await;
    assert_eq!(payloads.len(), 3, "only the changed message is sent");
    assert_eq!(latest(&payloads, "k1").as_deref(), Some("k1=v1b"));

    Ok(())
}

#[tokio::test]
async fn iggy_target_custom_delete_value_when_available() -> Result<()> {
    let Ok(conn) = std::env::var("IGGY_CONNECTION_STRING") else {
        eprintln!("skipping live Iggy custom-delete test; IGGY_CONNECTION_STRING is not set");
        return Ok(());
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let stream = format!("coco_test_del_{nonce}");
    let topic = "rows".to_string();

    let producer = IggyProducer::connect(&conn).await?;
    ensure_stream_topic(&producer, &stream, &topic).await;

    let tempdir = tempfile::tempdir().unwrap();
    let db_path = tempdir.path().join(".cocoindex_db");

    // A delete sends `<key>=<deleted>` as the value (Iggy has no tombstone).
    let run = |messages: Vec<(String, String)>| {
        let producer = producer.clone();
        let stream = stream.clone();
        let topic = topic.clone();
        let db_path = db_path.clone();
        async move {
            let app = App::builder("IggyCustomDeleteTest")
                .db_path(&db_path)
                .build()
                .await
                .unwrap();
            app.run(move |ctx| {
                let producer = producer.clone();
                let stream = stream.clone();
                let topic = topic.clone();
                let messages = messages.clone();
                async move {
                    let options = iggy::IggyTopicOptions {
                        partition: PARTITION,
                        deletion_value_fn: Some(Arc::new(|key: &str| {
                            format!("{key}=<deleted>").into_bytes()
                        })),
                    };
                    let target =
                        iggy::mount_iggy_topic_target(&ctx, &producer, stream, topic, options)?;
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

    let msg = |k: &str, v: &str| (k.to_string(), format!("{k}={v}"));

    run(vec![msg("k1", "v1")]).await;
    // Stop declaring k1 -> a delete is sent using the custom value.
    run(vec![]).await;
    let payloads = poll_all(&producer, &stream, &topic).await;
    assert_eq!(
        latest(&payloads, "k1").as_deref(),
        Some("k1=<deleted>"),
        "removed message's latest payload uses the custom deletion value"
    );

    Ok(())
}
