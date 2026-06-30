# Snowflake Target Example

This example writes a small set of order rows into a Snowflake table with the
CocoIndex Snowflake target connector. CocoIndex owns the table state: it creates
the database, schema, and table when needed, writes rows with Snowflake `MERGE`,
and deletes target rows that are no longer declared by the pipeline.

## Prerequisites

1. Install dependencies:

```sh
pip install -e .
```

2. Copy the environment template and fill in your Snowflake account details:

```sh
cp .env.example .env
```

3. Create a small warehouse for the demo. You can run this in Snowflake:

```sql
CREATE WAREHOUSE IF NOT EXISTS COCOINDEX_DEMO_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
```

The connector creates `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, and
`SNOWFLAKE_TABLE` when the app runs.

## Run

Build/update the target table:

```sh
cocoindex update main
```

Print the rows written to Snowflake:

```sh
python main.py
```

## Try an incremental update

Edit `SAMPLE_ORDERS` in `main.py`, then run:

```sh
cocoindex update main
python main.py
```

Only the changed order rows are upserted. If you remove one of the sample
orders, CocoIndex deletes the corresponding row from Snowflake on the next run.
