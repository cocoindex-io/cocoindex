"""Write a small set of order rows to Snowflake with a CocoIndex table target."""

from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass

from dotenv import load_dotenv

import cocoindex as coco
from cocoindex.connectors import snowflake

load_dotenv()

SNOWFLAKE = coco.ContextKey[snowflake.ConnectionConfig]("snowflake_demo")

DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "COCOINDEX_DEMO_DB")
SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
TABLE_NAME = os.environ.get("SNOWFLAKE_TABLE", "COCOINDEX_ORDERS")


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
class SnowflakeOrder:
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
        customer="Ada Lovelace",
        product="mechanical keyboard",
        quantity=2,
        unit_price=129.50,
        status="paid",
        attributes={"channel": "web", "priority": "standard"},
    ),
    SourceOrder(
        order_id="ORD-1002",
        customer="Grace Hopper",
        product="standing desk",
        quantity=1,
        unit_price=399.00,
        status="paid",
        attributes={"channel": "sales", "priority": "white_glove"},
    ),
    SourceOrder(
        order_id="ORD-1003",
        customer="Katherine Johnson",
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
        SNOWFLAKE,
        snowflake.ConnectionConfig(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
            role=os.environ.get("SNOWFLAKE_ROLE") or None,
        ),
    )
    yield


@coco.fn(memo=True)
async def process_order(
    order: SourceOrder,
    table: snowflake.TableTarget[SnowflakeOrder],
) -> None:
    table.declare_row(
        row=SnowflakeOrder(
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
    table = await snowflake.mount_table_target(
        SNOWFLAKE,
        table_name=TABLE_NAME,
        table_schema=await snowflake.TableSchema.from_class(
            SnowflakeOrder,
            primary_key=["order_id"],
        ),
        database=DATABASE,
        schema=SCHEMA,
    )

    await coco.mount_each(
        process_order,
        ((order.order_id, order) for order in SAMPLE_ORDERS),
        table,
    )


app = coco.App(
    coco.AppConfig(name="SnowflakeTarget"),
    app_main,
)


def _qualified_table_name() -> str:
    return f'"{DATABASE}"."{SCHEMA}"."{TABLE_NAME}"'


def print_rows() -> None:
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        role=os.environ.get("SNOWFLAKE_ROLE") or None,
    )
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT
                    "order_id",
                    "customer",
                    "product",
                    "quantity",
                    "order_total",
                    "status",
                    "attributes":channel::string AS channel
                FROM {_qualified_table_name()}
                ORDER BY "order_id"
                """
            )
            for row in cursor.fetchall():
                print(row)
        finally:
            cursor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    print_rows()
