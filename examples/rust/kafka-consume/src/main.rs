//! Kafka consume — a self-contained example of the Rust `kafka` **source**
//! (`topic_as_map` + `Ctx::mount_each_live`), the analogue of Python's
//! `topic_as_map`. Pairs with the `csv-to-kafka` target example.
//!
//!   KAFKA_BOOTSTRAP_SERVERS=localhost:9092 cargo run -- <topic>           # snapshot
//!   KAFKA_BOOTSTRAP_SERVERS=localhost:9092 cargo run -- <topic> --live    # tail forever
//!
//! `topic_as_map` is a `LiveMapView`: in catch-up mode it scans the log up to the
//! high-watermark, compacted to the latest value per key (tombstones drop keys);
//! in `--live` mode it then tails new records. Each value becomes one child
//! component via `mount_each_live` — here we just print it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use cocoindex::{App, UpdateOptions, kafka};

#[tokio::main]
async fn main() -> cocoindex::Result<()> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let topic = args
        .first()
        .cloned()
        .unwrap_or_else(|| "cocoindex_demo".to_string());
    let live = args.iter().any(|a| a == "--live");

    let brokers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "localhost:9092".to_string());
    let broker_refs: Vec<&str> = brokers.split(',').collect();
    let consumer = kafka::KafkaConsumer::connect(&broker_refs).await?;

    let count = Arc::new(AtomicUsize::new(0));
    let app = App::open("kafka_consume", ".cocoindex_db").await?;

    let body = {
        let consumer = consumer.clone();
        let topic = topic.clone();
        let count = count.clone();
        move |ctx: cocoindex::Ctx| {
            let consumer = consumer.clone();
            let topic = topic.clone();
            let count = count.clone();
            async move {
                let feed = kafka::topic_as_map(&consumer, topic);
                ctx.mount_each_live(&"messages", feed, move |_ctx, value: Vec<u8>| {
                    let count = count.clone();
                    async move {
                        count.fetch_add(1, Ordering::Relaxed);
                        println!("message: {}", String::from_utf8_lossy(&value));
                        Ok(())
                    }
                })
                .await
            }
        }
    };

    if live {
        println!("tailing topic {topic:?} (Ctrl-C to stop)…");
        let handle = app.start_update_with_options(
            UpdateOptions {
                full_reprocess: false,
                live: true,
                ..UpdateOptions::default()
            },
            body,
        )?;
        handle.result().await?;
    } else {
        app.run(body).await?;
        println!("snapshot: {} message(s) in {topic:?}", count.load(Ordering::Relaxed));
    }
    Ok(())
}
