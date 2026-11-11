# Kafka → LanceDB (Rust)

Rust port of the Python [`kafka_to_lancedb`](../../kafka_to_lancedb) example.

Consume a Kafka topic of JSON messages and dispatch each one by shape into the
right LanceDB table: a message with a `sku` field becomes a `products` row, one
with an `emp_id` field an `employees` row. This is the consumer side of
[`csv_to_kafka`](../csv_to_kafka): the same declarative flow that *produced*
the topic now *consumes* it.

## How it works

Kafka is a source you treat as a keyed map (`kafka::topic_as_map`); each
LanceDB table is a target you declare rows on. `process_message` runs once per
message key: decode the value, parse the JSON, and dispatch on shape.

```rust
#[cocoindex::function]
async fn process_message(ctx: &Ctx, msg: KafkaMessage,
                         products_table: LanceTableTarget, employees_table: LanceTableTarget) -> Result<()> {
    let Some(text) = msg.value_str() else { return Ok(()) };
    let row: serde_json::Value = serde_json::from_str(&text)?;
    if row.get("sku").is_some() {
        products_table.declare_row(ctx, &Product { .. })?;
    } else if row.get("emp_id").is_some() {
        employees_table.declare_row(ctx, &serde_json::from_value::<Employee>(row)?)?;
    }
    Ok(())
}
```

`declare_row` is deliberately *not* an upsert call. A new or changed primary
key is upserted; a tombstone (null value) removes that key's row; the same row
declared again writes nothing. The consumer group's offsets are committed by
CocoIndex *after* each row is durably written, so a crash mid-flight replays
from the last message it actually persisted.

## Parallel to the Python example

| Concern        | Python                                              | Rust (this example)                                          |
| -------------- | --------------------------------------------------- | ------------------------------------------------------------ |
| Consumer       | `confluent_kafka.aio.AIOConsumer(config)`           | `kafka::KafkaConsumer::connect_with_options` (consumer group, SASL/TLS) |
| Source         | `kafka.topic_as_map(consumer, topic)` + `coco.mount_each` | `kafka::topic_as_map(&consumer, topic)` + `ctx.mount_each_live` |
| Target         | `lancedb.mount_table_target` / `declare_row`        | `lancedb::mount_table_target` / `LanceTableTarget::declare_row` |
| Row schema     | `TableSchema.from_class(Product, primary_key=["sku"])` | `TableSchema::from_row::<Product>(["sku"])`               |
| Live mode      | `cocoindex update -L`                               | `cargo run -- --live`                                        |

## Run

> Needs a running Kafka broker with a topic to consume. The easy way to
> populate it: run [`csv_to_kafka`](../csv_to_kafka) first — both examples
> default to the same `KAFKA_TOPIC` (`cocoindex-csv-rows`).

**1. Configure:**

```sh
# optional: KAFKA_BOOTSTRAP_SERVERS (default localhost:9092), KAFKA_TOPIC (cocoindex-csv-rows),
# KAFKA_GROUP_ID (kafka-to-lancedb), LANCEDB_URI (./lancedb_data),
# KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD (SASL/PLAIN over TLS, for a managed broker)
```

**2. Run the pipeline** — catch-up (drain what's there, then exit) or live
(catch up, then keep consuming):

```sh
cargo run                    # catch-up
cargo run -- --live          # live (Ctrl-C to stop)
```

**3. Look at the tables** — they're just files under `./lancedb_data`; open
them with any LanceDB client, e.g. in Python:

```python
import lancedb

db = lancedb.connect("./lancedb_data")
for row in db.open_table("products").to_arrow().to_pylist():
    print(row)
for row in db.open_table("employees").to_arrow().to_pylist():
    print(row)
```

Every `sku` message is a row in `products`, every `emp_id` message a row in
`employees` — keyed exactly as it was on the topic, so re-consuming the same key
updates the row in place rather than duplicating.
