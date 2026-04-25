"""
Consume Kafka messages produced by csv_to_kafka and dispatch to LanceDB tables.

Each message is a JSON row from either products.csv or employees.csv.
The pipeline detects the schema and writes to the appropriate LanceDB table.

Run csv_to_kafka first to populate the Kafka topic, then run this example.
"""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass

from confluent_kafka import Message
from confluent_kafka.aio import AIOConsumer

import cocoindex as coco
from cocoindex.connectors import kafka, lancedb

KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "csv-rows")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "kafka-to-lancedb")
KAFKA_SASL_USERNAME = os.environ.get("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.environ.get("KAFKA_SASL_PASSWORD", "")

LANCEDB_URI = os.environ.get("LANCEDB_URI", "./lancedb_data")

LANCE_DB = coco.ContextKey[lancedb.LanceAsyncConnection]("kafka_to_lancedb_db")

# --- Row schemas matching the CSV data ---


@dataclass
class Product:
    sku: str
    name: str
    category: str
    price: float


@dataclass
class Employee:
    emp_id: str
    first_name: str
    last_name: str
    department: str
    email: str


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    conn = await lancedb.connect_async(LANCEDB_URI)
    builder.provide(LANCE_DB, conn)
    yield


@coco.fn
async def process_message(
    msg: Message,
    products_table: lancedb.TableTarget[Product],
    employees_table: lancedb.TableTarget[Employee],
) -> None:
    value = msg.value()
    if value is None:
        return
    text = value.decode() if isinstance(value, bytes) else value
    row = json.loads(text)

    if "sku" in row:
        products_table.declare_row(
            row=Product(**{**row, "price": float(row["price"])}),
        )
    elif "emp_id" in row:
        employees_table.declare_row(row=Employee(**row))


@coco.fn
async def app_main() -> None:
    products_table = await lancedb.mount_table_target(
        LANCE_DB,
        table_name="products",
        table_schema=await lancedb.TableSchema.from_class(Product, primary_key=["sku"]),
    )

    employees_table = await lancedb.mount_table_target(
        LANCE_DB,
        table_name="employees",
        table_schema=await lancedb.TableSchema.from_class(
            Employee, primary_key=["emp_id"]
        ),
    )

    config: dict[str, str] = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "enable.auto.commit": "false",
        "auto.offset.reset": "earliest",
    }
    if KAFKA_SASL_USERNAME:
        config.update(
            {
                "sasl.mechanism": "PLAIN",
                "security.protocol": "SASL_SSL",
                "sasl.username": KAFKA_SASL_USERNAME,
                "sasl.password": KAFKA_SASL_PASSWORD,
            }
        )

    consumer = AIOConsumer(config)
    items = kafka.topic_as_map(consumer, [KAFKA_TOPIC])
    await coco.mount_each(process_message, items, products_table, employees_table)


app = coco.App(
    coco.AppConfig(name="KafkaToLanceDB"),
    app_main,
)
