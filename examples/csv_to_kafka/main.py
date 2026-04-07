"""Watch local CSV files and publish each row as a JSON message to a Kafka topic."""

import csv
import io
import json
import os
from collections.abc import AsyncIterator

from confluent_kafka.aio import AIOProducer

import cocoindex as coco
from cocoindex.connectors import kafka, localfs
from cocoindex.resources.file import FileLike, PatternFilePathMatcher

KAFKA_PRODUCER = coco.ContextKey[AIOProducer]("kafka_producer", tracked=False)

KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "cocoindex-csv-rows")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_SASL_USERNAME = os.environ.get("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.environ.get("KAFKA_SASL_PASSWORD", "")


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    config: dict[str, str] = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
    if KAFKA_SASL_USERNAME:
        config.update(
            {
                "sasl.mechanism": "PLAIN",
                "security.protocol": "SASL_SSL",
                "sasl.username": KAFKA_SASL_USERNAME,
                "sasl.password": KAFKA_SASL_PASSWORD,
            }
        )
    producer = AIOProducer(config)
    builder.provide(KAFKA_PRODUCER, producer)
    yield


@coco.fn(memo=True)
async def process_csv(file: FileLike, topic_target: kafka.KafkaTopicTarget) -> None:
    text = await file.read_text()
    reader = csv.DictReader(io.StringIO(text))

    headers = reader.fieldnames
    if not headers:
        return
    first_col = headers[0]

    for row in reader:
        key_value = row.get(first_col, None)
        if key_value is not None:
            value = json.dumps(row)
            topic_target.declare_target_state(key=key_value, value=value)


@coco.fn
async def app_main() -> None:
    topic_target = await kafka.mount_kafka_topic_target(KAFKA_PRODUCER, KAFKA_TOPIC)

    files = localfs.walk_dir(
        localfs.FilePath(path="./data"),
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.csv"]),
        live=True,
    )
    await coco.mount_each(process_csv, files.items(), topic_target)


app = coco.App(
    coco.AppConfig(name="CsvToKafka"),
    app_main,
)
