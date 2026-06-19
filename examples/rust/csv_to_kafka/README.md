# CSV to Kafka (Rust)

Rust port of the Python [`csv_to_kafka`](../../csv_to_kafka) example.

It reads local CSV files, converts each row to a JSON object (using the header
row as keys), and publishes one Kafka message per row through CocoIndex's
declarative `KafkaTopicTarget`. Each message is keyed by the first CSV column,
matching the Python example.

## Parallel to the Python example

| Concern            | Python                                        | Rust (this example)                              |
| ------------------ | --------------------------------------------- | ------------------------------------------------ |
| Target             | `kafka.mount_kafka_topic_target(...)`         | `kafka::mount_kafka_topic_target(...)`           |
| Per-file compute   | `@coco.fn(memo=True) process_csv`             | `#[cocoindex::function(memo)] process_csv`       |
| Declare a message  | `topic_target.declare_target_state(key, val)` | `target.declare_message(ctx, key, val)`          |
| Kafka client       | `confluent_kafka` (librdkafka)                | [`rskafka`] (pure Rust, no C deps)               |

### Incrementality (two layers)

- `process_csv` is **memoized** — a CSV file whose content is unchanged is not
  re-parsed.
- The managed `KafkaTopicTarget` only **produces** a message when its value
  changed since the last run, and produces a **tombstone** (a record with a null
  value) for a row that disappeared from the source.

Both the topic and the partitioning are user-managed: like the Python connector,
CocoIndex never creates or drops topics during reconciliation (this example
calls `ensure_topic` explicitly), and messages are produced to partition 0.

## Run

Start a Kafka or Redpanda broker on `localhost:9092`, then:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092   # default
export KAFKA_TOPIC=cocoindex-csv-rows           # default

# Read ./data/*.csv and produce a message per row (incremental on re-run)
cargo run -- index

# Print all messages currently on the topic
cargo run -- consume
```

Edit a row in `data/products.csv` and re-run `cargo run -- index`: only the
changed row is re-produced. Delete a row and re-run: a tombstone is produced for
it.

> Note: unlike the Python example (`live=True` continuous watch), this runs a
> single pass per `index` invocation — the Rust SDK's `fs::walk` is one-shot.

[`rskafka`]: https://crates.io/crates/rskafka
