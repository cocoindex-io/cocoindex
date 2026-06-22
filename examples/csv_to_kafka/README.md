# CSV to Kafka

This example watches local CSV files, converts each row to a JSON object (using the header row as keys), and publishes messages to a Kafka topic. When a CSV file is modified, only the changed rows are re-published.

## Prerequisites

- A running Kafka broker (default: `localhost:9092`)

## Configuration

Edit `.env` to set your Kafka connection:

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=cocoindex-csv-rows
```

## Run

Install deps:

```sh
pip install -e .
```

Run the pipeline in live mode so changes to CSV files are picked up automatically:

```sh
cocoindex update -L main.py
```

Each row is published as a JSON message keyed by its first column value, with the full row as the JSON value. For example, a row in `products.csv` with SKU `SKU001` is published with key `SKU001` and value `{"sku": "SKU001", "name": "Wireless Mouse", ...}`.
