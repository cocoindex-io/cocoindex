//! CSV to Kafka — Rust port of the Python `csv_to_kafka` example.
//!
//! Reads local CSV files, converts each row to a JSON object (header row as
//! keys), and publishes one Kafka message per row via CocoIndex's declarative
//! `KafkaTopicTarget`. Each message is keyed by the first CSV column, matching
//! the Python example.
//!
//!   cargo run -- index                 # read ./data/*.csv -> produce changed rows
//!   cargo run -- consume               # print all messages currently on the topic
//!
//! Incrementality (two layers):
//!   - `process_csv` is memoized, so unchanged CSV files skip parsing entirely;
//!   - the managed `KafkaTopicTarget` only *produces* a message when its value
//!     changed since the last run, and produces a tombstone (null value) for a
//!     row that disappeared from the source.
//!
//! Parallels the Python example:
//!   - target            : `kafka::mount_kafka_topic_target` (cf. `mount_kafka_topic_target`)
//!   - per-file compute  : `#[cocoindex::function(memo)]`    (cf. `@coco.fn(memo=True)`)
//!   - declare a message : `target.declare_message(key, value)`
//!
//! Note: unlike the Python example (`live=True` continuous watch), this runs a
//! single pass per `index` invocation — the Rust SDK's `fs::walk` is one-shot.

use cocoindex::fs::FileEntry;
use cocoindex::kafka::{self, KafkaProducer};
use cocoindex::prelude::*;
use rskafka::client::ClientBuilder;
use rskafka::client::partition::UnknownTopicHandling;

fn bootstrap_servers() -> String {
    std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

fn topic_name() -> String {
    std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "cocoindex-csv-rows".to_string())
}

/// Parse one CSV file into `(message_key, json_value)` pairs. Memoized: a file
/// whose content is unchanged since the last run is not re-parsed.
#[cocoindex::function(memo)]
async fn process_csv(_ctx: &Ctx, file: &FileEntry) -> Result<Vec<(String, String)>> {
    let text = file.content_str()?;
    let mut reader = csv::Reader::from_reader(text.as_bytes());
    let headers = reader
        .headers()
        .map_err(|e| Error::engine(format!("csv headers: {e}")))?
        .clone();
    if headers.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for record in reader.records() {
        let record = record.map_err(|e| Error::engine(format!("csv record: {e}")))?;
        let Some(first) = record.get(0).filter(|s| !s.is_empty()) else {
            continue;
        };
        let mut row = serde_json::Map::new();
        for (header, field) in headers.iter().zip(record.iter()) {
            row.insert(
                header.to_string(),
                serde_json::Value::String(field.to_string()),
            );
        }
        let key = first.to_string();
        let value = serde_json::to_string(&row).map_err(|e| Error::engine(format!("json: {e}")))?;
        out.push((key, value));
    }
    Ok(out)
}

async fn index(producer: KafkaProducer, topic: String) -> Result<()> {
    // The topic is user-managed; create it up front if absent (idempotent).
    producer.ensure_topic(&topic, 1).await?;

    let app = App::builder("CsvToKafkaRust")
        .db_path(std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
        .build()
        .await?;

    let stats = app
        .run(move |ctx| {
            let producer = producer.clone();
            let topic = topic.clone();
            async move {
                let target = kafka::mount_kafka_topic_target(
                    &ctx,
                    &producer,
                    topic,
                    kafka::KafkaTopicOptions::default(),
                )?;

                let files = cocoindex::fs::walk("./data", &["**/*.csv"])?;
                println!("found {} CSV file(s)", files.len());

                let per_file = ctx
                    .mount_each(
                        files,
                        |f| f.key(),
                        |child, file| async move { process_csv(&child, &file).await },
                    )
                    .await?;

                let mut declared = 0;
                for messages in &per_file {
                    for (key, value) in messages {
                        target.declare_message(&ctx, key, value)?;
                        declared += 1;
                    }
                }
                println!("declared {declared} row message(s)");
                Ok(())
            }
        })
        .await?;
    println!("{stats}");
    Ok(())
}

async fn consume(brokers: &str, topic: &str) -> Result<()> {
    let client = ClientBuilder::new(brokers.split(',').map(str::to_string).collect())
        .build()
        .await
        .map_err(|e| Error::engine(format!("kafka connect: {e}")))?;
    let pc = client
        .partition_client(topic.to_string(), 0, UnknownTopicHandling::Retry)
        .await
        .map_err(|e| Error::engine(format!("partition_client: {e}")))?;

    println!("Messages on {topic:?} (partition 0):");
    println!("{}", "-".repeat(60));
    let mut offset = 0i64;
    let mut total = 0;
    loop {
        let (records, high_watermark) = pc
            .fetch_records(offset, 1..1_000_000, 1_000)
            .await
            .map_err(|e| Error::engine(format!("fetch: {e}")))?;
        if records.is_empty() {
            break;
        }
        for r in &records {
            let key = r
                .record
                .key
                .as_ref()
                .map(|k| String::from_utf8_lossy(k).to_string())
                .unwrap_or_default();
            match &r.record.value {
                Some(v) => println!("  {key} = {}", String::from_utf8_lossy(v)),
                None => println!("  {key} = <tombstone>"),
            }
            offset = r.offset + 1;
            total += 1;
        }
        if offset >= high_watermark {
            break;
        }
    }
    println!("{}", "-".repeat(60));
    println!("{total} record(s)");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cmd = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "index".to_string());
    let brokers = bootstrap_servers();
    let topic = topic_name();

    match cmd.as_str() {
        "consume" => consume(&brokers, &topic).await?,
        "index" => {
            let broker_refs: Vec<&str> = brokers.split(',').collect();
            let producer = KafkaProducer::connect(&broker_refs).await?;
            index(producer, topic).await?;
        }
        other => {
            eprintln!("unknown command {other:?}; use `index` or `consume`");
            std::process::exit(2);
        }
    }
    Ok(())
}
