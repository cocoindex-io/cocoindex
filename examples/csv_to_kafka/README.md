# CSV to Kafka

This example watches local CSV files, converts each row to a JSON object (using the header row as keys), and publishes messages to a Kafka topic. When a CSV file is modified, only the changed rows are re-published.

## Prerequisites

- A running Kafka broker (default: `localhost:9092`)

## Configuration

Edit `.env` to set your Kafka connection:

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=csv-rows
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

Each row is published as a JSON message with key `{filename}/{first_column_value}`. For example, a row in `products.csv` with SKU `SKU001` gets key `products.csv/SKU001`.
