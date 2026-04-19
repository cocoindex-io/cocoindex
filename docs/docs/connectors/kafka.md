---
title: Kafka
toc_max_heading_level: 4
description: CocoIndex connector for consuming and producing messages with Apache Kafka topics.
---

# Kafka

The `kafka` connector supports Kafka as both a **source** (consuming messages as a live keyed map) and a **target** (producing messages for declared target states).

```python
from cocoindex.connectors import kafka
```

:::note Dependencies
This connector requires additional dependencies. Install with:

```bash
pip install cocoindex[kafka]
```

:::

## As source

The `kafka` connector can treat a Kafka topic as a live keyed map — each message is an upsert or delete for a key. It returns a [`LiveMapFeed`](../advanced_topics/live_component.md#live-map) for use with `mount_each()`.

### Setting up a consumer

Create an `AIOConsumer` directly — no `ContextKey` needed. The consumer must be **unsubscribed** (the connector handles subscription internally to manage partition rebalance callbacks).

```python
from confluent_kafka.aio import AIOConsumer

consumer = AIOConsumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "my-group",
    "enable.auto.commit": "false",
})
```

### `topic_as_map()`

```python
def topic_as_map(
    consumer: AIOConsumer,
    topics: list[str],
    *,
    is_deletion: IsDeleteFn | None = None,
) -> LiveMapFeed[bytes | str, Message]:
```

**Parameters:**

- `consumer` — An unsubscribed `AIOConsumer`. Auto-commit should be disabled.
- `topics` — Topics to subscribe to.
- `is_deletion` — Optional predicate `(message: Message) -> bool` for custom deletion detection on non-tombstone messages (see [Deletion handling](#source-deletion-handling)).

**Returns:** A `LiveMapFeed[bytes | str, Message]` where each item is keyed by the message key and the value is the full `confluent_kafka.Message` object.

### Deletion handling {#source-deletion-handling}

Messages with `None` value (Kafka tombstones) are **always** treated as deletions. The optional `is_deletion` predicate provides additional deletion logic for non-tombstone messages:

```python
# Default: only tombstones are deletions
items = kafka.topic_as_map(consumer, ["my-topic"])

# Custom: also treat messages with a specific header as deletions
items = kafka.topic_as_map(
    consumer, ["my-topic"],
    is_deletion=lambda msg: msg.value() == b"DELETED",
)
```

### Offset management

Offsets are committed automatically with at-least-once semantics. Messages are processed in parallel, but an offset is only committed after all earlier messages in the same partition have been fully processed. Messages with `None` keys are logged as errors and skipped.

### Readiness

The feed signals readiness after catching up to the high watermark offsets that existed when consumption started. After that, it continues consuming indefinitely until the app is stopped.

### Example

```python
from collections.abc import AsyncIterator

from confluent_kafka import Message
from confluent_kafka.aio import AIOConsumer
from cocoindex.connectors import kafka, localfs
import cocoindex as coco


@coco.fn(memo=True)
async def process_message(msg: Message, target: localfs.DirTarget) -> None:
    key = msg.key()
    value = msg.value()
    if isinstance(key, bytes):
        key = key.decode()
    target.declare_file(filename=f"{key}.bin", content=value)


@coco.fn
async def app_main(outdir: pathlib.Path) -> None:
    target = await localfs.mount_dir_target(outdir)

    consumer = AIOConsumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "my-group",
        "enable.auto.commit": "false",
    })
    items = kafka.topic_as_map(consumer, ["my-topic"])
    await coco.mount_each(process_message, items, target)


app = coco.App(
    coco.AppConfig(name="KafkaToFiles"),
    app_main,
    outdir=pathlib.Path("./out"),
)
app.update_blocking(live=True)
```

## As target

The `kafka` connector provides target state APIs for producing messages to Kafka topics. Topics are user-managed (CocoIndex does not create or drop topics) — CocoIndex only produces messages to them.

### Setting up a producer

Create a `ContextKey[AIOProducer]` to identify your producer, then provide it in your lifespan:

:::note
The key name is load-bearing across runs — it's the stable identity CocoIndex uses to track target state for topics produced through this key. See [ContextKey as stable identity](../programming_guide/context.md#contextkey-as-stable-identity) before renaming.
:::

```python
from confluent_kafka import AIOProducer
import cocoindex as coco

KAFKA_PRODUCER = coco.ContextKey[AIOProducer]("my_kafka_producer")

@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    producer = AIOProducer({"bootstrap.servers": "localhost:9092"})
    builder.provide(KAFKA_PRODUCER, producer)
    yield
```

### Declaring target states

#### Topics (parent state)

Declares a topic as a target state. Returns a `KafkaTopicTarget` for declaring messages.

```python
def declare_kafka_topic_target(
    producer: ContextKey[AIOProducer],
    topic: str,
    *,
    deletion_value_fn: DeletionValueFn | None = None,
) -> KafkaTopicTarget[coco.PendingS]
```

**Parameters:**

- `producer` — A `ContextKey[AIOProducer]` identifying the producer to use.
- `topic` — The Kafka topic name.
- `deletion_value_fn` — Optional callback that produces a deletion value for a given key (see [Deletion handling](#deletion-handling)).

**Returns:** A pending `KafkaTopicTarget`. Use the async convenience wrapper to resolve:

```python
topic_target = await kafka.mount_kafka_topic_target(
    KAFKA_PRODUCER, "my-topic"
)
```

#### Messages (child states)

Once a `KafkaTopicTarget` is resolved, declare target states to produce messages:

```python
def KafkaTopicTarget.declare_target_state(
    self,
    *,
    key: bytes | str,
    value: bytes | str,
) -> None
```

**Parameters:**

- `key` — The message key, used as the stable identity for change detection.
- `value` — The message value.

CocoIndex fingerprints the value and only produces a message when it has changed since the last run.

### Deletion handling

When a previously declared target state is no longer declared, CocoIndex produces a deletion message. The behavior depends on `deletion_value_fn`:

- **Without callback** (default): Produces a message with the key and no value (Kafka tombstone).
- **With callback**: Calls `deletion_value_fn(key)` to produce the deletion value.

```python
# Tombstone on deletion (default)
topic_target = await kafka.mount_kafka_topic_target(
    KAFKA_PRODUCER, "my-topic"
)

# Custom deletion value
topic_target = await kafka.mount_kafka_topic_target(
    KAFKA_PRODUCER, "my-topic",
    deletion_value_fn=lambda key: b'{"deleted": true}',
)
```

### Example

```python
from collections.abc import AsyncIterator

from confluent_kafka import AIOProducer
from cocoindex.connectors import kafka, localfs
import cocoindex as coco

KAFKA_PRODUCER = coco.ContextKey[AIOProducer]("my_kafka_producer")


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    producer = AIOProducer({"bootstrap.servers": "localhost:9092"})
    builder.provide(KAFKA_PRODUCER, producer)
    yield


@coco.fn(memo=True)
async def process_file(
    file: localfs.File, topic_target: kafka.KafkaTopicTarget
) -> None:
    content = await file.read_bytes()
    topic_target.declare_target_state(
        key=file.file_path.path.as_posix().encode(),
        value=content,
    )


@coco.fn
async def app_main() -> None:
    topic_target = await kafka.mount_kafka_topic_target(
        KAFKA_PRODUCER, "file-contents"
    )

    files = localfs.walk_dir(localfs.FilePath(path="./data"))
    await coco.mount_each(process_file, files.items(), topic_target)


app = coco.App(
    coco.AppConfig(name="FilesToKafka"),
    app_main,
)
app.update_blocking(report_to_stdout=True)
```
