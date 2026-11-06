//! Kafka → LanceDB — Rust port of the Python `kafka_to_lancedb` example.
//!
//! Consume Kafka messages produced by `csv_to_kafka` and dispatch each one to a
//! LanceDB table by shape. Each message is a JSON row from either
//! `products.csv` or `employees.csv`; a message with a `sku` field becomes a
//! `products` row, one with an `emp_id` field an `employees` row.
//!
//! Run `csv_to_kafka` first to populate the topic, then:
//!
//!   cargo run                    # catch-up: drain what's there, write the rows, exit
//!   cargo run -- --live          # catch up, then keep consuming new messages
//!
//! Parallels the Python example:
//!   - source   : `kafka::topic_as_map` + `ctx.mount_each_live` (cf. `kafka.topic_as_map` + `coco.mount_each`)
//!   - target   : `lancedb::LanceTableTarget::declare_row` (cf. `lancedb.TableTarget.declare_row`)
//!   - schema   : `#[derive(SchemaFields)]` + `TableSchema::from_row` (cf. `TableSchema.from_class`)
//!
//! `declare_row` is deliberately not an upsert call: a new or changed key is
//! upserted, a tombstone (null value) removes that key's row, and the same row
//! declared again writes nothing. The consumer group's offsets are committed by
//! CocoIndex after each row is durably written.

use std::path::PathBuf;

use cocoindex::connectors::kafka::{
    self, KafkaConnectOptions, KafkaConsumer, KafkaConsumerOptions, KafkaMessage, KafkaSasl,
};
use cocoindex::connectors::lancedb::{self, LanceDatabase, LanceTableTarget, TableSchema};
use cocoindex::prelude::*;

const APP_NAME: &str = "KafkaToLanceDB";

cocoindex::context_key!(static LANCE_DB: LanceDatabase);

// --- Row schemas matching the CSV data ---

#[derive(Clone, Debug, Serialize, Deserialize, SchemaFields)]
struct Product {
    sku: String,
    name: String,
    category: String,
    price: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize, SchemaFields)]
struct Employee {
    emp_id: String,
    first_name: String,
    last_name: String,
    department: String,
    email: String,
}

/// The JSON shape `csv_to_kafka` produces: every CSV cell is a string, so
/// `price` arrives as `"29.99"`.
#[derive(Deserialize)]
struct ProductMessage {
    sku: String,
    name: String,
    category: String,
    price: String,
}

#[cocoindex::function]
async fn process_message(
    ctx: &Ctx,
    msg: KafkaMessage,
    products_table: LanceTableTarget,
    employees_table: LanceTableTarget,
) -> Result<()> {
    let Some(text) = msg.value_str() else {
        return Ok(());
    };
    let row: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| Error::engine(format!("message {} is not JSON: {e}", msg.offset)))?;

    if row.get("sku").is_some() {
        let p: ProductMessage = serde_json::from_value(row)
            .map_err(|e| Error::engine(format!("product message {}: {e}", msg.offset)))?;
        let price: f64 =
            p.price.trim().parse().map_err(|e| {
                Error::engine(format!("product {} price {:?}: {e}", p.sku, p.price))
            })?;
        products_table.declare_row(
            ctx,
            &Product {
                sku: p.sku,
                name: p.name,
                category: p.category,
                price,
            },
        )?;
    } else if row.get("emp_id").is_some() {
        let e: Employee = serde_json::from_value(row)
            .map_err(|e| Error::engine(format!("employee message {}: {e}", msg.offset)))?;
        employees_table.declare_row(ctx, &e)?;
    }
    // A message matching neither shape is quietly skipped.
    Ok(())
}

async fn app_main(ctx: Ctx, consumer: KafkaConsumer, topic: String) -> Result<()> {
    let products_table = lancedb::mount_table_target(
        &ctx,
        &LANCE_DB,
        "products",
        TableSchema::from_row::<Product>(["sku"])?,
    )
    .await?;
    let employees_table = lancedb::mount_table_target(
        &ctx,
        &LANCE_DB,
        "employees",
        TableSchema::from_row::<Employee>(["emp_id"])?,
    )
    .await?;

    // One child component per message key; in live mode new records keep
    // arriving until the app is stopped.
    let items = kafka::topic_as_map(&consumer, topic);
    ctx.mount_each_live(&"messages", items, move |ctx, msg: KafkaMessage| {
        let (products_table, employees_table) = (products_table.clone(), employees_table.clone());
        async move { process_message(&ctx, msg, products_table, employees_table).await }
    })
    .await
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

async fn connect_consumer() -> Result<KafkaConsumer> {
    let brokers = env_or("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    let broker_refs: Vec<&str> = brokers.split(',').collect();
    let mut connect = KafkaConnectOptions::default();
    // SASL/PLAIN over TLS for a managed broker, as the Python example does
    // when `KAFKA_SASL_USERNAME` is set.
    let username = env_or("KAFKA_SASL_USERNAME", "");
    if !username.is_empty() {
        connect.sasl = Some(KafkaSasl::Plain {
            username,
            password: env_or("KAFKA_SASL_PASSWORD", ""),
        });
        connect.tls = Some(Default::default());
    }
    KafkaConsumer::connect_with_options(
        &broker_refs,
        KafkaConsumerOptions {
            consumer_group: Some(env_or("KAFKA_GROUP_ID", "kafka-to-lancedb")),
            connect,
        },
    )
    .await
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let live = std::env::args().any(|a| a == "--live" || a == "-L");
    let topic = env_or("KAFKA_TOPIC", "cocoindex-csv-rows");
    let lancedb_uri = std::env::var("LANCEDB_URI").unwrap_or_else(|_| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("lancedb_data")
            .to_string_lossy()
            .into_owned()
    });

    let consumer = connect_consumer().await?;
    let db = LanceDatabase::connect(&lancedb_uri).await?;
    let app = Environment::builder()
        .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
        .provide_key(&LANCE_DB, db)
        .build()
        .await?
        .app(APP_NAME)
        .await?;

    let body = move |ctx: Ctx| app_main(ctx, consumer, topic);
    if live {
        println!("consuming (Ctrl-C to stop)…");
        let handle = app.start_update_with_options(
            UpdateOptions {
                live: true,
                ..UpdateOptions::default()
            },
            body,
        )?;
        handle.result().await?;
    } else {
        let stats = app.run(body).await?;
        println!("{stats}");
    }
    Ok(())
}
