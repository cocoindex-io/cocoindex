# BigQuery Target Example

This example shows how to use BigQuery as a CocoIndex target.

BigQuery is the table store in this example. CocoIndex is responsible for the
target state: it creates the dataset and table when needed, writes rows with
BigQuery `MERGE`, and deletes rows that are no longer declared by the flow.

The example keeps the source data in Python so the BigQuery target behavior is
easy to see. It declares three order records, computes `order_total`, writes the
rows to BigQuery, then reads the table back with the BigQuery Python client.

## What this creates

By default the flow writes to:

| Object | Default value | Created by |
|--------|---------------|------------|
| Project | value of `BIGQUERY_PROJECT` | You, before running the example |
| Dataset | `cocoindex_demo` | CocoIndex, if the service account has permission |
| Table | `cocoindex_orders` | CocoIndex |

The Google Cloud project must exist before the flow runs. The BigQuery
connector creates the dataset and table.

## How the flow works

1. `coco_lifespan` reads `BIGQUERY_*` environment variables and provides a
   BigQuery connection config to CocoIndex.
2. `mount_table_target` declares the BigQuery table target and its primary key.
3. `process_order` converts each source order into the row shape stored in
   BigQuery.
4. `cocoindex update main` reconciles the declared rows with the table.

## Prerequisites

Run commands from this directory:

```sh
cd examples/bigquery_target
```

1. Install dependencies from a source checkout:

```sh
pip install -e "../..[bigquery]" -e .
```

2. Copy the environment template:

```sh
cp .env.example .env
```

3. Fill in `.env`.

| Variable | Required | How to choose it |
|----------|----------|------------------|
| `COCOINDEX_DB` | Yes | Local CocoIndex state database. The default `./cocoindex.db` is fine for the example. |
| `BIGQUERY_PROJECT` | Yes | Google Cloud project that owns the target dataset and table. |
| `BIGQUERY_DATASET` | Yes | Target dataset name. CocoIndex creates it when the credential has permission. |
| `BIGQUERY_TABLE` | Yes | Target table name. The default is `cocoindex_orders`. |
| `BIGQUERY_LOCATION` | No | BigQuery job location, for example `US` or `EU`. Use the same location as the dataset. |
| `GOOGLE_APPLICATION_CREDENTIALS` | No | Path to a service account JSON key. Leave blank to use Application Default Credentials. |

4. Use a credential with the needed permissions.

For a small demo, the credential needs:

- permission to create or use the target dataset
- permission to create the target table
- permission to run BigQuery query jobs
- permission to run `MERGE` and `DELETE` on the target table

## Authentication used here

This example supports two standard Google authentication paths:

```python
bigquery.ConnectionConfig(
    project=os.environ.get("BIGQUERY_PROJECT"),
    credentials_path=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or None,
    location=os.environ.get("BIGQUERY_LOCATION") or None,
)
```

If `GOOGLE_APPLICATION_CREDENTIALS` is blank, the Google client uses
Application Default Credentials.

## Run

Build/update the target table:

```sh
cocoindex update main
```

Print the rows written to BigQuery:

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
orders, CocoIndex deletes the corresponding row from BigQuery on the next run.
