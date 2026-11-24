# BigQuery Target (Rust)

Rust port of the Python [`bigquery_target`](../../bigquery_target) example,
using the Rust SDK's **BigQuery table target** (`cocoindex::connectors::bigquery`).

The example keeps the source data in the program so the target behavior is
easy to see. It declares three order records, computes `order_total`, writes
the rows to BigQuery, then reads the table back.

BigQuery has no official Rust client, so the connector speaks the BigQuery v2
REST API (`jobs.query`) through `reqwest`. CocoIndex creates the dataset and
table when needed, writes rows with `MERGE`, and deletes rows that are no
longer declared.

| Concept    | API                                                                     |
| ---------- | ----------------------------------------------------------------------- |
| Connection | `BigQueryConnection::connect(BigQueryConfig { project, credentials_path, location })` |
| Schema     | `bigquery::TableSchema::from_row::<BigQueryOrder>(["order_id"])`         |
| Target     | `bigquery::mount_table_target` + `TableTarget::declare_row`              |
| Read back  | `BigQueryConnection::query`                                              |

## Authentication

`credentials_path` (or `GOOGLE_APPLICATION_CREDENTIALS`) names a service-account
JSON key. Without one, the connector uses Application Default Credentials: the
`gcloud auth application-default login` file, then the GCE metadata server.

## Run

```bash
cp .env.example .env     # set BIGQUERY_PROJECT (and optionally the rest)

cargo run -- index       # reconcile the three orders into BigQuery
cargo run -- print       # read the table back
```

Expected `print` output:

```text
"ORD-1001" | "Summit Labs" | "mechanical keyboard" | 2 | 259.0 | "paid" | "web"
"ORD-1002" | "Beacon Retail" | "standing desk" | 1 | 399.0 | "paid" | "sales"
"ORD-1003" | "Ridgeview Health" | "noise cancelling headphones" | 3 | 599.97 | "pending" | "partner"
```

Edit `sample_orders()` in `src/main.rs` and run `index` again: only the changed
orders are upserted, and a removed order's row is deleted from BigQuery.
