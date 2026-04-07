# Kafka to LanceDB

This example consumes JSON messages from a Kafka topic (produced by the [csv_to_kafka](../csv_to_kafka) example) and dispatches them to two LanceDB tables — `products` and `employees` — based on the message content.

## Prerequisites

- A running Kafka broker (default: `localhost:9092`)
- The `csv_to_kafka` example running (or having run) to populate the topic

## Configuration

Copy `.env.example` to `.env` and edit as needed:

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=cocoindex-example-csv-rows
KAFKA_GROUP_ID=kafka-to-lancedb
LANCEDB_URI=./lancedb_data
```

## Run

Install deps:

```sh
pip install -e .
```

Run the pipeline in live mode to continuously consume new messages:

```sh
cocoindex update -L main.py
```

Messages with a `sku` field are written to the `products` table; messages with an `emp_id` field go to the `employees` table.

## Inspect LanceDB data

After the pipeline has processed some messages, you can inspect the tables with Python:

```python
import lancedb

db = lancedb.connect("./lancedb_data")

print("=== Products ===")
for row in db.open_table("products").to_arrow().to_pylist():
    print(row)

print("\n=== Employees ===")
for row in db.open_table("employees").to_arrow().to_pylist():
    print(row)
```
