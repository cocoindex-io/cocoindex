//! Snowflake target — Rust port of the Python `snowflake_target` example.
//!
//! Declares three order rows, computes `order_total`, and writes them to a
//! Snowflake table through the declarative `snowflake::TableTarget`: the
//! database, schema and table are created when needed, rows are `MERGE`d, and
//! rows no longer declared are deleted on the next run.
//!
//!   cargo run -- index   # reconcile the orders into Snowflake
//!   cargo run -- print   # read the table back
//!
//! Config (env, e.g. via `.env`):
//!   SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD   required
//!   SNOWFLAKE_WAREHOUSE   existing warehouse (default `COCOINDEX_DEMO_WH`)
//!   SNOWFLAKE_ROLE        optional
//!   SNOWFLAKE_DATABASE    default `COCOINDEX_DEMO_DB`
//!   SNOWFLAKE_SCHEMA      default `PUBLIC`
//!   SNOWFLAKE_TABLE       default `COCOINDEX_ORDERS`

use cocoindex::connectors::snowflake::{
    self, SnowflakeConfig, SnowflakeConnection, SnowflakeTableOptions, TableSchema,
};
use cocoindex::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

cocoindex::context_key!(static SNOWFLAKE: SnowflakeConnection, key = "snowflake_demo");

#[derive(Clone, Serialize, Deserialize, MemoInput)]
struct SourceOrder {
    order_id: String,
    customer: String,
    product: String,
    quantity: i64,
    unit_price: f64,
    status: String,
    attributes: JsonValue,
}

#[derive(Clone, Serialize, Deserialize, SchemaFields)]
struct SnowflakeOrder {
    order_id: String,
    customer: String,
    product: String,
    quantity: i64,
    unit_price: f64,
    order_total: f64,
    status: String,
    #[coco(json)]
    attributes: JsonValue,
}

fn sample_orders() -> Vec<SourceOrder> {
    let order = |id: &str,
                 customer: &str,
                 product: &str,
                 quantity,
                 unit_price,
                 status: &str,
                 channel: &str,
                 priority: &str| SourceOrder {
        order_id: id.to_string(),
        customer: customer.to_string(),
        product: product.to_string(),
        quantity,
        unit_price,
        status: status.to_string(),
        attributes: serde_json::json!({"channel": channel, "priority": priority}),
    };
    vec![
        order(
            "ORD-1001",
            "Summit Labs",
            "mechanical keyboard",
            2,
            129.50,
            "paid",
            "web",
            "standard",
        ),
        order(
            "ORD-1002",
            "Beacon Retail",
            "standing desk",
            1,
            399.00,
            "paid",
            "sales",
            "rush",
        ),
        order(
            "ORD-1003",
            "Ridgeview Health",
            "noise cancelling headphones",
            3,
            199.99,
            "pending",
            "partner",
            "expedite",
        ),
    ]
}

/// Compute one order's row. Memoized by the order's content, so an unchanged
/// order costs nothing on a re-run.
#[cocoindex::function(memo)]
async fn process_order(_ctx: &Ctx, order: SourceOrder) -> Result<SnowflakeOrder> {
    let order_total = (order.quantity as f64 * order.unit_price * 100.0).round() / 100.0;
    Ok(SnowflakeOrder {
        order_id: order.order_id,
        customer: order.customer,
        product: order.product,
        quantity: order.quantity,
        unit_price: order.unit_price,
        order_total,
        status: order.status,
        attributes: order.attributes,
    })
}

#[derive(Clone)]
struct Settings {
    database: String,
    schema: String,
    table: String,
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn connect() -> Result<(SnowflakeConnection, Settings)> {
    let _ = dotenvy::dotenv();
    let required = |name: &str| {
        std::env::var(name)
            .ok()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| Error::engine(format!("{name} is not set")))
    };
    let mut config = SnowflakeConfig::new(
        required("SNOWFLAKE_ACCOUNT")?,
        required("SNOWFLAKE_USER")?,
        required("SNOWFLAKE_PASSWORD")?,
    )
    .warehouse(env_or("SNOWFLAKE_WAREHOUSE", "COCOINDEX_DEMO_WH"));
    if let Some(role) = std::env::var("SNOWFLAKE_ROLE")
        .ok()
        .filter(|v| !v.is_empty())
    {
        config = config.role(role);
    }
    Ok((
        SnowflakeConnection::new(config),
        Settings {
            database: env_or("SNOWFLAKE_DATABASE", "COCOINDEX_DEMO_DB"),
            schema: env_or("SNOWFLAKE_SCHEMA", "PUBLIC"),
            table: env_or("SNOWFLAKE_TABLE", "COCOINDEX_ORDERS"),
        },
    ))
}

async fn index() -> Result<()> {
    let (conn, settings) = connect()?;
    let app = cocoindex::Environment::builder()
        .db_path(".cocoindex_db")
        .provide_key(&SNOWFLAKE, conn)
        .build()
        .await?
        .app("SnowflakeTarget")
        .await?;
    let stats = app
        .run(move |ctx| {
            let settings = settings.clone();
            async move {
                let schema = TableSchema::from_row::<SnowflakeOrder>(["order_id"])?;
                let table = snowflake::mount_table_target_with_options(
                    &ctx,
                    &SNOWFLAKE,
                    settings.table,
                    schema,
                    SnowflakeTableOptions {
                        database: Some(settings.database),
                        schema: Some(settings.schema),
                        ..Default::default()
                    },
                )
                .await?;
                let orders = sample_orders()
                    .into_iter()
                    .map(|order| (order.order_id.clone(), order));
                let rows = mount_each!(orders, |order| process_order(ctx, order)).await?;
                for row in &rows {
                    table.declare_row(&ctx, row)?;
                }
                Ok(())
            }
        })
        .await?;
    println!("{stats}");
    Ok(())
}

async fn print() -> Result<()> {
    let (conn, settings) = connect()?;
    let rows = conn
        .query(&format!(
            "SELECT \"order_id\", \"customer\", \"product\", \"quantity\", \"order_total\", \
             \"status\", \"attributes\":channel::string AS channel \
             FROM \"{}\".\"{}\".\"{}\" ORDER BY \"order_id\"",
            settings.database, settings.schema, settings.table
        ))
        .await?;
    for row in rows {
        println!(
            "{} | {} | {} | {} | {} | {} | {}",
            row["order_id"],
            row["customer"],
            row["product"],
            row["quantity"],
            row["order_total"],
            row["status"],
            row["CHANNEL"]
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("print") => print().await,
        _ => index().await,
    }
}
