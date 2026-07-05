"""Write a small set of order rows to BigQuery with a CocoIndex table target."""

from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from dotenv import load_dotenv

import cocoindex as coco
from cocoindex.connectors import bigquery

load_dotenv()

BIGQUERY = coco.ContextKey[bigquery.ConnectionConfig]("bigquery_demo")

PROJECT = os.environ.get("BIGQUERY_PROJECT") or None
DATASET = os.environ.get("BIGQUERY_DATASET", "cocoindex_demo")
TABLE_NAME = os.environ.get("BIGQUERY_TABLE", "cocoindex_orders")
LOCATION = os.environ.get("BIGQUERY_LOCATION") or None
CREDENTIALS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or None


@dataclass(frozen=True)
class SourceOrder:
    order_id: str
    customer: str
    product: str
    quantity: int
    unit_price: float
    status: str
    attributes: dict[str, object]


@dataclass(frozen=True)
class BigQueryOrder:
    order_id: str
    customer: str
    product: str
    quantity: int
    unit_price: float
    order_total: float
    status: str
    attributes: dict[str, object]


SAMPLE_ORDERS = (
    SourceOrder(
        order_id="ORD-1001",
        customer="Summit Labs",
        product="mechanical keyboard",
        quantity=2,
        unit_price=129.50,
        status="paid",
        attributes={"channel": "web", "priority": "standard"},
    ),
    SourceOrder(
        order_id="ORD-1002",
        customer="Beacon Retail",
        product="standing desk",
        quantity=1,
        unit_price=399.00,
        status="paid",
        attributes={"channel": "sales", "priority": "rush"},
    ),
    SourceOrder(
        order_id="ORD-1003",
        customer="Ridgeview Health",
        product="noise cancelling headphones",
        quantity=3,
        unit_price=199.99,
        status="pending",
        attributes={"channel": "partner", "priority": "expedite"},
    ),
)


@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.provide(
        BIGQUERY,
        bigquery.ConnectionConfig(
            project=PROJECT,
            credentials_path=CREDENTIALS_PATH,
            location=LOCATION,
        ),
    )
    yield


@coco.fn(memo=True)
async def process_order(
    order: SourceOrder,
    table: bigquery.TableTarget[BigQueryOrder],
) -> None:
    table.declare_row(
        row=BigQueryOrder(
            order_id=order.order_id,
            customer=order.customer,
            product=order.product,
            quantity=order.quantity,
            unit_price=order.unit_price,
            order_total=round(order.quantity * order.unit_price, 2),
            status=order.status,
            attributes=order.attributes,
        )
    )


@coco.fn
async def app_main() -> None:
    table = await bigquery.mount_table_target(
        BIGQUERY,
        table_name=TABLE_NAME,
        table_schema=await bigquery.TableSchema.from_class(
            BigQueryOrder,
            primary_key=["order_id"],
        ),
        project=PROJECT,
        dataset=DATASET,
    )

    await coco.mount_each(
        process_order,
        ((order.order_id, order) for order in SAMPLE_ORDERS),
        table,
    )


app = coco.App(
    coco.AppConfig(name="BigQueryTarget"),
    app_main,
)


def _qualified_table_name() -> str:
    project_prefix = f"{PROJECT}." if PROJECT else ""
    return f"`{project_prefix}{DATASET}.{TABLE_NAME}`"


def _bigquery_client() -> Any:
    from google.cloud import bigquery as google_bigquery

    credentials = None
    if CREDENTIALS_PATH:
        from google.oauth2 import service_account

        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_PATH
        )
    return google_bigquery.Client(
        project=PROJECT,
        credentials=credentials,
        location=LOCATION,
    )


def print_rows() -> None:
    client = _bigquery_client()
    query_job = client.query(
        f"""
        SELECT
            `order_id`,
            `customer`,
            `product`,
            `quantity`,
            `order_total`,
            `status`,
            JSON_VALUE(`attributes`, '$.channel') AS channel
        FROM {_qualified_table_name()}
        ORDER BY `order_id`
        """
    )
    for row in query_job.result():
        print(
            (
                row["order_id"],
                row["customer"],
                row["product"],
                row["quantity"],
                row["order_total"],
                row["status"],
                row["channel"],
            )
        )
    close = getattr(client, "close", None)
    if close is not None:
        close()


if __name__ == "__main__":
    print_rows()
