//! BigQuery target — Rust port of the Python `bigquery_target` example.
//!
//! Declares three order rows, computes `order_total`, and writes them to a
//! BigQuery table through the declarative `bigquery::TableTarget`: the dataset
//! and table are created when needed, rows are `MERGE`d, and rows no longer
//! declared are deleted on the next run.
//!
//!   cargo run -- index   # reconcile the orders into BigQuery
//!   cargo run -- print   # read the table back
//!
//! Config (env, e.g. via `.env`):
//!   BIGQUERY_PROJECT                 required
//!   BIGQUERY_DATASET                 default `cocoindex_demo`
//!   BIGQUERY_TABLE                   default `cocoindex_orders`
//!   BIGQUERY_LOCATION                optional, e.g. `US`
//!   GOOGLE_APPLICATION_CREDENTIALS   optional service-account key file; else
//!                                    Application Default Credentials

use cocoindex::connectors::bigquery::{self, BigQueryConfig, BigQueryConnection, TableSchema};
use cocoindex::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

cocoindex::context_key!(static BIGQUERY: BigQueryConnection, key = "bigquery_demo");

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
struct BigQueryOrder {
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
async fn process_order(_ctx: &Ctx, order: SourceOrder) -> Result<BigQueryOrder> {
    let order_total = (order.quantity as f64 * order.unit_price * 100.0).round() / 100.0;
    Ok(BigQueryOrder {
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

struct Settings {
    dataset: String,
    table: String,
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn connect() -> Result<(BigQueryConnection, Settings)> {
    let _ = dotenvy::dotenv();
    let optional = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let conn = BigQueryConnection::connect(BigQueryConfig {
        project: optional("BIGQUERY_PROJECT"),
        credentials_path: optional("GOOGLE_APPLICATION_CREDENTIALS").map(Into::into),
        location: optional("BIGQUERY_LOCATION"),
    })?;
    Ok((
        conn,
        Settings {
            dataset: env_or("BIGQUERY_DATASET", "cocoindex_demo"),
            table: env_or("BIGQUERY_TABLE", "cocoindex_orders"),
        },
    ))
}

async fn index() -> Result<()> {
    let (conn, settings) = connect()?;
    let app = cocoindex::Environment::builder()
        .db_path(".cocoindex_db")
        .provide_key(&BIGQUERY, conn)
        .build()
        .await?
        .app("BigQueryTarget")
        .await?;
    let stats = app
        .run(move |ctx| {
            let settings = Settings {
                dataset: settings.dataset.clone(),
                table: settings.table.clone(),
            };
            async move {
                let schema = TableSchema::from_row::<BigQueryOrder>(["order_id"])?;
                let table = bigquery::mount_table_target(
                    &ctx,
                    &BIGQUERY,
                    settings.dataset,
                    settings.table,
                    schema,
                )
                .await?;
                let orders = sample_orders()
                    .into_iter()
                    .map(|order| (order.order_id.clone(), order));
                let rows = use_mount_each!(orders, |order| process_order(ctx, order)).await?;
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
            "SELECT `order_id`, `customer`, `product`, `quantity`, `order_total`, `status`, \
             JSON_VALUE(`attributes`, '$.channel') AS channel \
             FROM `{}.{}.{}` ORDER BY `order_id`",
            conn.project(),
            settings.dataset,
            settings.table
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
            row["channel"]
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
