# Revisioned Kafka messages with FastStream

This example implements a custom CocoIndex target using FastStream's Kafka
publisher and Prometheus middleware. A JSON snapshot declares the desired rows.
CocoIndex compares them with its saved target state and publishes logical upserts
and deletes; a small SQLite consumer demonstrates durable duplicate suppression
and revision ordering.

It is an example-level integration for Kafka. FastStream remains an optional
example dependency. The broker must already have the destination topic; the target
never creates or deletes topics.

## Run locally

Run these commands from the repository root. The example requires Python 3.11 or
newer, the repository's development prerequisites, and Docker for the local broker.
The supplied broker is for local development and binds only to loopback.

1. Copy `examples/faststream_sink/.env.example` to
   `examples/faststream_sink/.env`. Generate a cluster ID with
   `docker run --rm apache/kafka:4.1.2 /opt/kafka/bin/kafka-storage.sh random-uuid`
   and put that value in `KAFKA_CLUSTER_ID`. Keep the ID with the broker's volume.
   The application verifies the actual broker identity at startup.
2. Start the broker and create the example topic:

   ```sh
   docker compose --env-file examples/faststream_sink/.env \
     -f examples/faststream_sink/compose.yaml up -d
   docker compose --env-file examples/faststream_sink/.env \
     -f examples/faststream_sink/compose.yaml exec kafka \
     /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 \
     --create --if-not-exists --topic cocoindex-faststream \
     --partitions 1 --replication-factor 1 \
     --config cleanup.policy=delete --config retention.ms=-1 --config retention.bytes=-1
   ```

   Wait for the broker to finish starting if the topic command initially cannot
   connect. The unlimited retention here is a deliberate local-demo setting: it
   keeps all events available for rebuilding the projection.
3. Start the consumer in one terminal:

   ```sh
   uv run --project examples/faststream_sink --with-editable "$PWD" \
     python -m examples.faststream_sink.consumer
   ```

4. Start the pipeline in another terminal:

   ```sh
   uv run --project examples/faststream_sink --with-editable "$PWD" \
     cocoindex update -L examples.faststream_sink.main
   ```

   Omit `-L` for a catch-up update that exits after confirmation. Live mode watches
   `examples/faststream_sink/snapshot.json` and keeps the metrics endpoint running.
   Scrape `http://127.0.0.1:8001/metrics` while the pipeline runs.
5. Change a row's value **and increment its revision**. Remove the row to publish
   a delete. If you recreate it, use a revision higher than its last revision
   before removal. Query the projection with:

   ```sh
   sqlite3 faststream-projection.sqlite3 \
     'SELECT record_key, revision, CAST(value_json AS TEXT) FROM records WHERE phase = 0;'
   ```

To publish deletes for all remaining tracked rows, stop live mode and run
`cocoindex drop examples.faststream_sink.main` through the same `uv run` prefix.
The publisher is provided by the environment lifespan, so cleanup also works in
that new process without rerunning the declarations. Stop the consumer and broker
when finished. `docker compose down` retains the broker volume; deleting that
volume also deletes its message history.

## Revision and delivery contract

The source owns a **durable, monotonically increasing revision for each logical
record**. The example does not allocate revisions and has no transactional outbox.
A local counter that resets on restart, a timestamp with possible collisions, or a
content hash is not a substitute. Keep the CocoIndex state store and coordinate
writers; this example runs one active producer for a logical destination.

An event contains the logical destination, verified cluster ID, topic, record key,
revision, operation, canonical JSON value, value fingerprint, and stable event ID.
The event ID derives from destination/key/revision/operation. The content hash is
separate, so contradictory content at the same revision is detected rather than
silently treated as a different message.

The consumer orders events by `(revision, phase)`, with upsert phase `0` and delete
phase `1`. Thus `A@1 → B@2 → A@3 → delete@3 → A@4` is valid. Reverting the value to
`A` still requires revision `3`; recreating it after deletion requires revision
`4`. A replay of `upsert@3` cannot resurrect `delete@3`. CocoIndex no longer retains
a row's revision after successful deletion, so the source must preserve that
history when assigning a recreation's revision.

The handler checks every possible previous tracking record, rejects decreasing or
conflicting revisions, and republishes when the previous delivery is uncertain.
It refuses a deletion if there is no previous revision with which to order it.
Unchanged, certain rows produce no event. Persisted routing data lets a later drop
publish to the original topic.

Each publisher call uses `no_confirm=False` and `acks="all"`. Kafka producer
idempotence is disabled so retriable batches expire; the consumer provides the
application's revision and replay handling. At most eight calls per provided destination are admitted concurrently;
each admitted call has a 15-second timeout. A successful CocoIndex update means
**broker confirmation**, not successful consumption or visibility in SQLite.
A timeout or crash can happen after the broker accepted a message but before
CocoIndex records success. Retrying therefore has **at-least-once** delivery:
stable IDs and consumer revision fences handle replays. A multi-message action
batch is not a Kafka transaction; partial failure propagates so it can be retried.

Shutdown drains the one owned broker-stop task before propagating cancellation.
The producer uses a 10-second request timeout, which also expires its retryable
batches. Connection and metadata operations can add time; neither the publish
timeout nor batch expiry is a universal process-exit deadline. Cleanup is not
cancelled midway, because that can leave an enqueued send and its connections alive.

The SQLite consumer commits the projection value and its revision fence in one
transaction, then commits the Kafka offset. A crash between those commits causes
a duplicate delivery. It retains deletion records, rejects conflicting equal
revisions, ignores stale events, and checks stored content before accepting a
replay as a no-op. Malformed or conflicting messages stop this small consumer
before offset commit; inspect and repair the cause before restarting it.

## Recovery and configuration

- Keep the consumer database, including its delete records and metadata. A new
  database receives a new consumer group and starts at the earliest retained
  offset. An older backup must not be resumed with broker offsets ahead of that
  backup: rebuild a fresh database from complete retained history, or perform an
  explicit coordinated offset reset.
- Rebuilding requires complete retained history or a source-controlled bootstrap
  with new revisions. Expired history cannot be recovered by retrying unchanged
  CocoIndex rows. Do not use Kafka compaction alone for this protocol: a late,
  stale replay can be the last offset even though its revision is lower.
- The `KAFKA_DESTINATION` ContextKey identifies one logical destination. Credential
  changes for that same broker do not change event identity. Before moving to a
  new cluster or changing the ContextKey, clean up using the old configuration;
  retain the old provider until cleanup succeeds. Repointing the existing key to
  another cluster is rejected. A missing provider fails cleanup rather than
  reporting a successful delete.
- `connection.py` uses plaintext settings for the loopback demo. For a managed
  cluster, configure equivalent TLS/authentication on the FastStream broker,
  identity-check admin client, and consumer. Do not put credentials in event
  payloads or the logical destination key.
- Events are limited to 256 KiB and identity fields to 1,024 UTF-8 bytes. The
  example publishes JSON values, not arbitrary Python objects.

## Verify changes

From the repository root, run the example's separate test gate:

```sh
uv run --project examples/faststream_sink --group test --with-editable "$PWD" \
  python -m pytest examples/faststream_sink/tests
```

The repository-wide hooks do not collect example tests. The focused tests cover
revision conflicts and reordering, persistence and corruption checks, the actual
CocoIndex confirmation boundary, replay after a lost confirmation, unchanged
updates, and cleanup after reopening the state store.

FastStream publisher metrics are exported through its
[Prometheus middleware](https://faststream.ag2.ai/latest/getting-started/observability/prometheus/).
The `no_confirm` behavior is documented in its
[Kafka publisher API](https://faststream.ag2.ai/latest/api/faststream/kafka/broker/KafkaBroker/).
For the broker image, see the
[Apache Kafka 4.1 Docker guide](https://kafka.apache.org/41/getting-started/docker/).
