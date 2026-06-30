# Snowflake Target Example

This example shows how to use Snowflake as a CocoIndex target.

Snowflake is the table store in this example. CocoIndex is responsible for the
target state: it creates the database, schema, and table when needed, writes
rows with Snowflake `MERGE`, and deletes rows that are no longer declared by the
flow.

The example keeps the source data in Python so the Snowflake target behavior is
easy to see. It declares three order records, computes `order_total`, writes the
rows to Snowflake, then reads the table back with the Snowflake Python
connector.

## What this creates

By default the flow writes to:

- database: `COCOINDEX_DEMO_DB`
- schema: `PUBLIC`
- table: `COCOINDEX_ORDERS`
- warehouse: `COCOINDEX_DEMO_WH`

The warehouse must exist before the flow runs. The Snowflake connector creates
the database, schema, and table.

## How the flow works

1. `coco_lifespan` reads `SNOWFLAKE_*` environment variables and provides a
   Snowflake connection config to CocoIndex.
2. `mount_table_target` declares the Snowflake table target and its primary key.
3. `process_order` converts each source order into the row shape stored in
   Snowflake.
4. `cocoindex update main` reconciles the declared rows with the table.

## Prerequisites

Run commands from this directory:

```sh
cd examples/snowflake_target
```

1. Install dependencies from a source checkout:

```sh
pip install -e "../..[snowflake]" -e .
```

2. Copy the environment template:

```sh
cp .env.example .env
```

Edit `.env` with your Snowflake account, user, password, and optional role.
`SNOWFLAKE_ACCOUNT` is the Snowflake account identifier, for example
`ORGNAME-ACCOUNTNAME`.

3. Create a small warehouse for the demo in Snowflake:

```sql
CREATE WAREHOUSE IF NOT EXISTS COCOINDEX_DEMO_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
```

If you use a different warehouse name, update `SNOWFLAKE_WAREHOUSE` in `.env`.

## Run

Build/update the target table:

```sh
cocoindex update main
```

Print the rows written to Snowflake:

```sh
python main.py
```

Expected output:

```text
('ORD-1001', 'Summit Labs', 'mechanical keyboard', 2, 259.0, 'paid', 'web')
('ORD-1002', 'Beacon Retail', 'standing desk', 1, 399.0, 'paid', 'sales')
('ORD-1003', 'Ridgeview Health', 'noise cancelling headphones', 3, 599.97, 'pending', 'partner')
```

## Try an incremental update

Edit `SAMPLE_ORDERS` in `main.py`, then run:

```sh
cocoindex update main
python main.py
```

Only the changed order rows are upserted. If you remove one of the sample
orders, CocoIndex deletes the corresponding row from Snowflake on the next run.
