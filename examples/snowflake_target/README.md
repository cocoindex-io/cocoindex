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

| Object | Default value | Created by |
|--------|---------------|------------|
| Warehouse | `COCOINDEX_DEMO_WH` | You, before running the example |
| Database | `COCOINDEX_DEMO_DB` | CocoIndex |
| Schema | `PUBLIC` | CocoIndex, if needed |
| Table | `COCOINDEX_ORDERS` | CocoIndex |

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

3. Fill in `.env`.

| Variable | Required | How to choose it |
|----------|----------|------------------|
| `COCOINDEX_DB` | Yes | Local CocoIndex state database. The default `./cocoindex.db` is fine for the example. |
| `SNOWFLAKE_ACCOUNT` | Yes | Snowflake account identifier, for example `ORGNAME-ACCOUNTNAME`. Do not include `.snowflakecomputing.com`. |
| `SNOWFLAKE_USER` | Yes | Snowflake login name. |
| `SNOWFLAKE_PASSWORD` | Yes | Password for `SNOWFLAKE_USER`. |
| `SNOWFLAKE_ROLE` | No | Role for the session. Leave blank to use the user's default role. |
| `SNOWFLAKE_WAREHOUSE` | Yes | Existing warehouse used to run DDL and DML. The README uses `COCOINDEX_DEMO_WH`. |
| `SNOWFLAKE_DATABASE` | Yes | Target database name. CocoIndex creates it when the role has permission. |
| `SNOWFLAKE_SCHEMA` | Yes | Target schema name. The default is `PUBLIC`. |
| `SNOWFLAKE_TABLE` | Yes | Target table name. The default is `COCOINDEX_ORDERS`. |

You can also check the current Snowflake session values in a worksheet:

```sql
SELECT
  CURRENT_ORGANIZATION_NAME() AS organization_name,
  CURRENT_ACCOUNT_NAME() AS account_name,
  CURRENT_USER() AS user_name,
  CURRENT_ROLE() AS role_name,
  CURRENT_WAREHOUSE() AS warehouse_name;
```

For `SNOWFLAKE_ACCOUNT`, use the account identifier shown in Snowflake Account
Details. In newer accounts this usually matches
`ORGANIZATION_NAME-ACCOUNT_NAME`.

4. Create a small warehouse for the demo in Snowflake:

```sql
CREATE WAREHOUSE IF NOT EXISTS COCOINDEX_DEMO_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
```

If you use a different warehouse name, update `SNOWFLAKE_WAREHOUSE` in `.env`.

5. Use a role with the needed permissions.

For a trial account, `ACCOUNTADMIN` is enough. For a narrower role, it needs:

- `USAGE` on the warehouse
- permission to create or use the target database
- permission to create or use the target schema
- permission to create the target table and run `MERGE` and `DELETE`

## Authentication used here

This example uses username and password authentication:

```python
snowflake.ConnectionConfig(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
    role=os.environ.get("SNOWFLAKE_ROLE") or None,
)
```

Key-pair authentication is not covered by this example. The current
`ConnectionConfig` in this PR accepts `account`, `user`, `password`,
`warehouse`, and `role`, and the live validation used the password path.

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
