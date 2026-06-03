# Kafka consume (Rust)

A self-contained example of the Rust SDK's native Kafka **source**
(`cocoindex::kafka::topic_as_map` + `Ctx::mount_each_live`) — the analogue of
Python's `topic_as_map`. Pairs with the [`csv-to-kafka`](../csv-to-kafka) target
example: produce with that, consume with this.

`topic_as_map` is a `LiveMapView` over a topic's partition 0:

- **catch-up** (default): `scan()` reads the log up to the high-watermark,
  compacted to the latest value per key (tombstones remove keys), then exits.
- **live** (`--live`): after the catch-up scan it `watch()`es, tailing new
  records as they are produced.

Each message value becomes one child component via `mount_each_live`; this demo
just prints it.

## Run

```bash
# Snapshot the current state of a topic and exit
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 cargo run -- my_topic

# Tail new records live (Ctrl-C to stop)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 cargo run -- my_topic --live
```

Requires a running Kafka/Redpanda broker. The native source is also covered by
`rust/sdk/cocoindex/tests/kafka_source.rs` (catch-up compaction + live tailing).
