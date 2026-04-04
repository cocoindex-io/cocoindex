---
title: Kafka
toc_max_heading_level: 4
description: CocoIndex connector for producing messages to Apache Kafka topics.
---

# Kafka

The `kafka` connector lets you declare target states backed by Kafka messages. CocoIndex tracks what messages should exist and automatically produces upserts and deletions to the topic.

```python
from cocoindex.connectors import kafka
```

:::note Dependencies
This connector requires additional dependencies. Install with:

```bash
pip install cocoindex[kafka]
```

:::

## As target

The `kafka` connector provides target state APIs for producing messages to Kafka topics. Topics are user-managed (CocoIndex does not create or drop topics) — CocoIndex only produces messages to them.

### Setting up a producer

Create a `ContextKey[AIOProducer]` (with `tracked=False`) to identify your producer, then provide it in your lifespan:

```python
from confluent_kafka import AIOProducer
import cocoindex as coco

KAFKA_PRODUCER = coco.ContextKey[AIOProducer]("my_kafka_producer", tracked=False)

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

KAFKA_PRODUCER = coco.ContextKey[AIOProducer]("my_kafka_producer", tracked=False)


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    producer = AIOProducer({"bootstrap.servers": "localhost:9092"})
    builder.provide(KAFKA_PRODUCER, producer)
    yield


@coco.fn(memo=True)
async def process_file(
    file: localfs.FileLike, topic_target: kafka.KafkaTopicTarget
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
