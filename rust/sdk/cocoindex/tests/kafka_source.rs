//! Live-Kafka integration test for the `kafka` sources (`topic_as_map`,
//! `topic_as_stream`).
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
//!   * live (`watch`) tails new records produced after the source started;
//!   * a consumer group resumes a live run from its committed offsets.
#![cfg(feature = "kafka")]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cocoindex::connectors::kafka::{self, KafkaMessage};
use cocoindex::{App, Environment, Result, UpdateOptions};
use rskafka::client::ClientBuilder;
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::record::Record;

cocoindex::context_key!(static PRODUCER: kafka::KafkaProducer);

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
    let topic = topic.to_string();
    let messages: Vec<(String, String)> = messages
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let app = Environment::builder()
        .db_path(db_path)
        .provide_key(&PRODUCER, producer.clone())
        .build()
        .await
        .unwrap()
        .app("KafkaSourcePopulate")
        .await
        .unwrap();
    app.run(move |ctx| {
        let topic = topic.clone();
        let messages = messages.clone();
        async move {
            let target = kafka::mount_kafka_topic_target(
                &ctx,
                &PRODUCER,
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

/// Produce one raw record (any key, any partition, with headers).
async fn produce_raw(
    brokers: &[String],
    topic: &str,
    partition: i32,
    key: Option<&str>,
    value: Option<&str>,
    headers: BTreeMap<String, Vec<u8>>,
) -> Result<()> {
    let raw = ClientBuilder::new(brokers.to_vec())
        .build()
        .await
        .map_err(|e| cocoindex::Error::engine(format!("client: {e}")))?;
    let pc = raw
        .partition_client(topic.to_string(), partition, UnknownTopicHandling::Retry)
        .await
        .map_err(|e| cocoindex::Error::engine(format!("partition_client: {e}")))?;
    let record = Record {
        key: key.map(|k| k.as_bytes().to_vec()),
        value: value.map(|v| v.as_bytes().to_vec()),
        headers,
        timestamp: rskafka::chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
    };
    pc.produce(vec![record], Compression::NoCompression)
        .await
        .map_err(|e| cocoindex::Error::engine(format!("produce p{partition}: {e}")))?;
    Ok(())
}

type Processed = Arc<Mutex<Vec<KafkaMessage>>>;

fn values(processed: &Processed) -> Vec<String> {
    processed
        .lock()
        .unwrap()
        .iter()
        .map(|m| m.value_str().unwrap_or_default())
        .collect()
}

/// Live runs share the process-wide cancellation the resume test uses to stop
/// its runs, so they take turns.
static LIVE_RUNS: Mutex<()> = Mutex::new(());

/// Mount the feed `make_feed` builds with `mount_each_live` in a catch-up run,
/// recording every processed message.
async fn collect<Feed>(
    app: &App,
    make_feed: impl Fn() -> Feed + Send + Sync + 'static,
    processed: Processed,
) -> Result<()>
where
    Feed: cocoindex::live_component::LiveMapView<String, KafkaMessage>,
{
    app.run(move |ctx| {
        let processed = processed.clone();
        let feed = make_feed();
        async move {
            ctx.mount_each_live(&"messages", feed, move |_ctx, message: KafkaMessage| {
                let processed = processed.clone();
                async move {
                    processed.lock().unwrap().push(message);
                    Ok(())
                }
            })
            .await
        }
    })
    .await?;
    Ok(())
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
    let processed: Processed = Arc::new(Mutex::new(Vec::new()));
    let app = App::builder("KafkaSourceScan")
        .db_path(tmp.path().join("source_db"))
        .build()
        .await?;
    collect(
        &app,
        {
            let topic = topic.clone();
            move || kafka::topic_as_map(&consumer, topic.clone())
        },
        processed.clone(),
    )
    .await?;

    assert_eq!(
        values(&processed),
        vec!["v1b".to_string()],
        "catch-up scan should compact to the latest value per key and drop tombstones"
    );
    let message = processed.lock().unwrap()[0].clone();
    assert_eq!(message.topic, topic);
    assert_eq!(message.partition, 0);
    assert_eq!(message.key_str().as_deref(), Some("k1"));
    assert_eq!(message.offset, 2, "k1=v1b is the third record");
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

    let _live = LIVE_RUNS.lock().unwrap();
    let consumer = kafka::KafkaConsumer::connect(&broker_refs).await?;
    let processed: Processed = Arc::new(Mutex::new(Vec::new()));

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
                        ctx.mount_each_live(
                            &"messages",
                            feed,
                            move |_ctx, message: KafkaMessage| {
                                let processed = processed.clone();
                                async move {
                                    processed.lock().unwrap().push(message);
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

    // Produce a NEW record after the source is live; `watch` must tail it.
    declare(&producer, &pop_db, &topic, vec![("k1", "v1"), ("k2", "v2")]).await;

    // Poll until both the catch-up value and the live-tailed value arrive.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let got = values(&processed);
        if got.iter().any(|v| v == "v1") && got.iter().any(|v| v == "v2") {
            break;
        }
        if std::time::Instant::now() > deadline {
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
    for (pid, key, val) in [(0, "k0", "v0"), (1, "k1", "v1"), (2, "k2", "v2")] {
        produce_raw(&brokers, &topic, pid, Some(key), Some(val), BTreeMap::new()).await?;
    }

    let consumer = kafka::KafkaConsumer::connect(&broker_refs).await?;
    let processed: Processed = Arc::new(Mutex::new(Vec::new()));
    let tmp = tempfile::tempdir().unwrap();
    let app = App::builder("KafkaSourceMultiPart")
        .db_path(tmp.path().join("db"))
        .build()
        .await?;
    collect(
        &app,
        {
            let topic = topic.clone();
            move || kafka::topic_as_map(&consumer, topic.clone())
        },
        processed.clone(),
    )
    .await?;

    let mut got: Vec<(i32, String)> = processed
        .lock()
        .unwrap()
        .iter()
        .map(|m| (m.partition, m.value_str().unwrap()))
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![(0, "v0".into()), (1, "v1".into()), (2, "v2".into())],
        "the catch-up scan should read records from all three partitions"
    );
    Ok(())
}

#[tokio::test]
async fn kafka_source_stream_reads_every_record_of_several_topics() -> Result<()> {
    let Some(brokers) = brokers() else {
        eprintln!("skipping live Kafka stream test; KAFKA_BOOTSTRAP_SERVERS is not set");
        return Ok(());
    };
    let broker_refs: Vec<&str> = brokers.iter().map(String::as_str).collect();
    let topic_a = unique_topic("stream_a");
    let topic_b = unique_topic("stream_b");

    // Null-key records across two partitions of topic A (`topic_as_map` would
    // skip these), a tombstone, and a record with headers on topic B. The
    // keyless stream must read every one of them, from both topics.
    let producer = kafka::KafkaProducer::connect(&broker_refs).await?;
    producer.ensure_topic(&topic_a, 2).await?;
    producer.ensure_topic(&topic_b, 1).await?;
    for (pid, val) in [(0, "a"), (0, "b"), (1, "c")] {
        produce_raw(&brokers, &topic_a, pid, None, Some(val), BTreeMap::new()).await?;
    }
    produce_raw(&brokers, &topic_a, 1, Some("gone"), None, BTreeMap::new()).await?;
    let headers = BTreeMap::from([("trace".to_string(), b"t-1".to_vec())]);
    produce_raw(
        &brokers,
        &topic_b,
        0,
        Some("kb"),
        Some("d"),
        headers.clone(),
    )
    .await?;

    let consumer = kafka::KafkaConsumer::connect(&broker_refs).await?;
    let processed: Processed = Arc::new(Mutex::new(Vec::new()));
    let tmp = tempfile::tempdir().unwrap();
    let app = App::builder("KafkaSourceStream")
        .db_path(tmp.path().join("db"))
        .build()
        .await?;
    collect(
        &app,
        {
            let topics = [topic_a.clone(), topic_b.clone()];
            move || kafka::topic_as_stream(&consumer, topics.clone())
        },
        processed.clone(),
    )
    .await?;

    let messages = processed.lock().unwrap().clone();
    let mut got: Vec<(String, i32, i64, Option<String>)> = messages
        .iter()
        .map(|m| (m.topic.clone(), m.partition, m.offset, m.value_str()))
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![
            (topic_a.clone(), 0, 0, Some("a".into())),
            (topic_a.clone(), 0, 1, Some("b".into())),
            (topic_a.clone(), 1, 0, Some("c".into())),
            (topic_a.clone(), 1, 1, None),
            (topic_b.clone(), 0, 0, Some("d".into())),
        ],
        "the stream should read every record of both topics, tombstones included"
    );
    let with_headers = messages.iter().find(|m| m.topic == topic_b).unwrap();
    assert_eq!(with_headers.headers, headers);
    assert_eq!(with_headers.key_str().as_deref(), Some("kb"));
    assert_eq!(with_headers.timestamp.timestamp(), 1_700_000_000);
    Ok(())
}

#[tokio::test]
async fn kafka_source_consumer_group_resumes_from_committed_offsets() -> Result<()> {
    let Some(brokers) = brokers() else {
        eprintln!("skipping live Kafka consumer-group test; KAFKA_BOOTSTRAP_SERVERS is not set");
        return Ok(());
    };
    let broker_refs: Vec<&str> = brokers.iter().map(String::as_str).collect();
    let topic = unique_topic("group");

    let producer = kafka::KafkaProducer::connect(&broker_refs).await?;
    producer.ensure_topic(&topic, 1).await?;
    let tmp = tempfile::tempdir().unwrap();
    let pop_db = tmp.path().join("populate_db");
    declare(&producer, &pop_db, &topic, vec![("k1", "v1")]).await;

    let consumer = kafka::KafkaConsumer::connect_with_options(
        &broker_refs,
        kafka::KafkaConsumerOptions {
            consumer_group: Some("cocoindex-test".to_string()),
            ..Default::default()
        },
    )
    .await?;
    let source_db = tmp.path().join("source_db");
    let _live = LIVE_RUNS.lock().unwrap();

    // A live run: processes everything (the catch-up scan), commits offsets,
    // then stops. Stopping goes through the engine's process-wide
    // cancellation, the path Ctrl-C takes, which keeps the committed state
    // (`drop_state` would wipe it).
    let run_live = |processed: Processed, until: Vec<&'static str>| {
        let consumer = consumer.clone();
        let topic = topic.clone();
        let source_db = source_db.clone();
        async move {
            let app = App::builder("KafkaSourceGroup")
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
                        let topic = topic.clone();
                        async move {
                            let feed = kafka::topic_as_map(&consumer, topic);
                            ctx.mount_each_live(
                                &"messages",
                                feed,
                                move |_ctx, message: KafkaMessage| {
                                    let processed = processed.clone();
                                    async move {
                                        processed.lock().unwrap().push(message);
                                        Ok(())
                                    }
                                },
                            )
                            .await
                        }
                    }
                },
            )?;
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            loop {
                let got = values(&processed);
                if until.iter().all(|v| got.iter().any(|g| g == v)) {
                    break;
                }
                if std::time::Instant::now() > deadline {
                    panic!("live run did not process {until:?} in time; processed={got:?}");
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            // A little grace so the offset commit after the batch lands.
            tokio::time::sleep(Duration::from_millis(500)).await;
            cocoindex_core::engine::runtime::cancel_all();
            let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
            cocoindex_core::engine::runtime::reset_global_cancellation();
            Ok::<_, cocoindex::Error>(())
        }
    };

    let first: Processed = Arc::new(Mutex::new(Vec::new()));
    run_live(first.clone(), vec!["v1"]).await?;
    assert_eq!(values(&first), vec!["v1".to_string()]);

    // A record arrives while no run is live; the next live run resumes from the
    // committed offsets: it replays only the new record, not "v1" again.
    declare(&producer, &pop_db, &topic, vec![("k1", "v1"), ("k2", "v2")]).await;
    let second: Processed = Arc::new(Mutex::new(Vec::new()));
    run_live(second.clone(), vec!["v2"]).await?;
    assert_eq!(
        values(&second),
        vec!["v2".to_string()],
        "a live run with a consumer group resumes from the committed offsets"
    );
    Ok(())
}
