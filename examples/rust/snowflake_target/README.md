# Snowflake Target (Rust)

Rust port of the Python [`snowflake_target`](../../snowflake_target) example,
using the Rust SDK's **Snowflake table target** (`cocoindex::connectors::snowflake`).

The example keeps the source data in the program so the target behavior is
easy to see. It declares three order records, computes `order_total`, writes
the rows to Snowflake, then reads the table back.

Snowflake has no official Rust client, so the connector speaks the HTTP
protocol the official connectors use (password login, statements with
server-side bindings) through `reqwest`. CocoIndex creates the database, schema
and table when needed, writes rows with `MERGE INTO`, and deletes rows that are
no longer declared. The warehouse must exist before the flow runs.

| Concept    | API                                                                     |
| ---------- | ----------------------------------------------------------------------- |
| Connection | `SnowflakeConnection::new(SnowflakeConfig::new(account, user, password).warehouse(..))` |
| Schema     | `snowflake::TableSchema::from_row::<SnowflakeOrder>(["order_id"])`       |
| Target     | `snowflake::mount_table_target_with_options` + `TableTarget::declare_row` |
| Read back  | `SnowflakeConnection::query`                                             |

## Run

```bash
cp .env.example .env     # set SNOWFLAKE_ACCOUNT / USER / PASSWORD / WAREHOUSE

cargo run -- index       # reconcile the three orders into Snowflake
cargo run -- print       # read the table back
```

Expected `print` output:

```text
"ORD-1001" | "Summit Labs" | "mechanical keyboard" | 2 | 259.0 | "paid" | "web"
"ORD-1002" | "Beacon Retail" | "standing desk" | 1 | 399.0 | "paid" | "sales"
"ORD-1003" | "Ridgeview Health" | "noise cancelling headphones" | 3 | 599.97 | "pending" | "partner"
```

Edit `sample_orders()` in `src/main.rs` and run `index` again: only the changed
orders are upserted, and a removed order's row is deleted from Snowflake.
