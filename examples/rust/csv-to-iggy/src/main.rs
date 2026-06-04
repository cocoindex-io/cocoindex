//! CSV to Iggy — Rust analogue of the Python `csv_to_kafka` example, targeting
//! Apache Iggy instead of Kafka (Python ships an `iggy` connector with the same
//! shape as its `kafka` one).
//!
//! Reads local CSV files, converts each row to a JSON object (header row as
//! keys), and publishes one Iggy message per row via CocoIndex's declarative
//! `IggyTopicTarget`. Each row is keyed by the first CSV column.
//!
//!   cargo run -- index      # read ./data/*.csv -> send changed rows
//!   cargo run -- consume    # poll and print all messages on the topic
//!
//! Incrementality (two layers):
//!   - `process_csv` is memoized, so unchanged CSV files skip parsing entirely;
//!   - the managed `IggyTopicTarget` only *sends* a message when its value
//!     changed since the last run. Iggy has no tombstone, so a removed row is
//!     represented by a custom deletion value (`{"_deleted":"<key>"}`).
//!
//! Stream/topic are user-managed (CocoIndex never creates/drops them); `index`
//! creates them up front as a convenience.

use cocoindex::fs::FileEntry;
use cocoindex::iggy::{self, IggyProducer};
use cocoindex::prelude::*;
use iggy::prelude::{
    CompressionAlgorithm, Consumer, Identifier, IggyExpiry, MaxTopicSize, MessageClient,
    PollingStrategy, StreamClient, SystemClient, TopicClient,
};
use std::sync::Arc;

const PARTITION: u32 = 0; // Iggy partitions are 0-indexed (matches the connector default).

fn connection_string() -> String {
    std::env::var("IGGY_CONNECTION_STRING")
        .unwrap_or_else(|_| "iggy://iggy:iggy@localhost:8090".to_string())
}

fn stream_name() -> String {
    std::env::var("IGGY_STREAM").unwrap_or_else(|_| "cocoindex".to_string())
}

fn topic_name() -> String {
    std::env::var("IGGY_TOPIC").unwrap_or_else(|_| "csv-rows".to_string())
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

/// Create the (user-managed) stream + single-partition topic if they don't exist.
async fn ensure_stream_topic(producer: &IggyProducer, stream: &str, topic: &str) -> Result<()> {
    let client = producer.client();
    // create_* error if the stream/topic already exists; that's fine.
    let _ = client.create_stream(stream).await;
    let stream_id = Identifier::from_str_value(stream)
        .map_err(|e| Error::engine(format!("iggy stream id: {e}")))?;
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
    Ok(())
}

async fn index(producer: IggyProducer, stream: String, topic: String) -> Result<()> {
    ensure_stream_topic(&producer, &stream, &topic).await?;

    let app = App::builder("CsvToIggyRust")
        .db_path(std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
        .build()
        .await?;

    let stats = app
        .run(move |ctx| {
            let producer = producer.clone();
            let stream = stream.clone();
            let topic = topic.clone();
            async move {
                let options = iggy::IggyTopicOptions {
                    partition: PARTITION,
                    // Iggy has no tombstone; emit a delete marker for removed rows.
                    deletion_value_fn: Some(Arc::new(|key: &str| {
                        format!("{{\"_deleted\":\"{key}\"}}").into_bytes()
                    })),
                };
                let target = iggy::mount_iggy_topic_target(&ctx, &producer, stream, topic, options)?;

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

async fn consume(producer: &IggyProducer, stream: &str, topic: &str) -> Result<()> {
    let stream_id = Identifier::from_str_value(stream)
        .map_err(|e| Error::engine(format!("iggy stream id: {e}")))?;
    let topic_id =
        Identifier::from_str_value(topic).map_err(|e| Error::engine(format!("iggy topic id: {e}")))?;
    let consumer = Consumer::new(
        Identifier::numeric(1).map_err(|e| Error::engine(format!("iggy consumer id: {e}")))?,
    );
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
        .map_err(|e| Error::engine(format!("iggy poll: {e}")))?;

    println!("Messages on {stream}/{topic} (partition {PARTITION}):");
    println!("{}", "-".repeat(60));
    for m in &polled.messages {
        println!("  {}", String::from_utf8_lossy(&m.payload));
    }
    println!("{}", "-".repeat(60));
    println!("{} message(s)", polled.messages.len());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cmd = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "index".to_string());
    let conn = connection_string();
    let stream = stream_name();
    let topic = topic_name();

    let producer = IggyProducer::connect(&conn).await?;
    // Ensure we're logged in / reachable before doing work.
    producer
        .client()
        .ping()
        .await
        .map_err(|e| Error::engine(format!("iggy ping: {e}")))?;

    match cmd.as_str() {
        "consume" => consume(&producer, &stream, &topic).await?,
        "index" => index(producer, stream, topic).await?,
        other => {
            eprintln!("unknown command {other:?}; use `index` or `consume`");
            std::process::exit(2);
        }
    }
    Ok(())
}
