# CSV to Iggy (Rust)

Rust analogue of the Python [`csv_to_kafka`](../../csv_to_kafka) example, targeting
**Apache Iggy** instead of Kafka. (Python ships an `iggy` connector with the same
shape as its `kafka` one; this mirrors the Rust [`csv-to-kafka`](../csv-to-kafka)
example onto `cocoindex::iggy`.)

Reads local CSV files, converts each row to a JSON object (header row as keys),
and publishes one Iggy message per row via CocoIndex's declarative
`IggyTopicTarget`.

## Parallel to the Python example

| Concern          | Python (`csv_to_kafka` / `iggy`)         | Rust (this example)                                 |
| ---------------- | ---------------------------------------- | --------------------------------------------------- |
| Per-file compute | `@coco.fn(memo=True)`                    | `#[cocoindex::function(memo)] process_csv`          |
| Target           | `mount_kafka_topic_target` / `mount_iggy_topic_target` | `cocoindex::iggy::mount_iggy_topic_target` |
| Declare a message| `target.declare_target_state(key, value)`| `target.declare_message(key, value)`                |

Incrementality (two layers): unchanged CSV files are memo-skipped, and the
managed `IggyTopicTarget` only *sends* a message when its value changed since the
last run.

**Iggy vs Kafka:** Iggy has no tombstone (null-value) concept, so a removed row
is represented by a custom **deletion value** (`{"_deleted":"<key>"}`) via
`IggyTopicOptions::deletion_value_fn`. Streams and topics are **user-managed**
(CocoIndex never creates/drops them); `index` creates them up front as a
convenience. Iggy messages have no key field, so the row payload is the JSON
object itself.

## Run

Start an Iggy server (the Apache image needs `io_uring` syscalls and binding to
`0.0.0.0`; it generates a random root password on first boot — see its logs):

```bash
docker run -d --name iggy --security-opt seccomp=unconfined \
  -e IGGY_TCP_ADDRESS=0.0.0.0:8090 -p 8090:8090 apache/iggy:latest
docker logs iggy 2>&1 | grep 'root user password'   # grab the generated password

export IGGY_CONNECTION_STRING="iggy://iggy:<password>@localhost:8090"
cargo run -- index      # ./data/*.csv -> send changed rows
cargo run -- consume    # poll and print all messages on the topic
```

`IGGY_STREAM` / `IGGY_TOPIC` override the defaults (`cocoindex` / `csv-rows`).
