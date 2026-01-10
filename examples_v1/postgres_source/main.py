"""
PostgreSQL Source (v1) - CocoIndex pipeline example.

- Read product rows from a source PostgreSQL table
- Compute derived fields and embeddings
- Store results into a target PostgreSQL table with pgvector
- Query demo using pgvector cosine distance (<=>)
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import sys
from dataclasses import dataclass
from typing import Annotated, AsyncIterator, cast

import asyncpg
from numpy.typing import NDArray

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import postgres
from cocoindex.extras.sentence_transformers import SentenceTransformerEmbedder


DATABASE_URL = os.getenv(
    "COCOINDEX_DATABASE_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex"
)
SOURCE_DATABASE_URL = os.getenv("SOURCE_DATABASE_URL", DATABASE_URL)
TABLE_NAME = "output"
PG_SCHEMA_NAME = "coco_examples_v1"
TOP_K = 5


PG_DB = coco.ContextKey[postgres.PgDatabase]("pg_db")
SOURCE_POOL = coco.ContextKey[asyncpg.Pool]("source_pool")

_embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")


@dataclass
class SourceProduct:
    product_category: str
    product_name: str
    description: str
    price: float
    amount: int


@dataclass
class OutputProduct:
    product_category: str
    product_name: str
    description: str
    price: float
    amount: int
    total_value: float
    embedding: Annotated[NDArray, _embedder]


@coco.function
def setup_table(
    scope: coco.Scope,
) -> postgres.TableTarget[OutputProduct, coco.PendingS]:
    return scope.use(PG_DB).declare_table_target(
        scope,
        table_name=TABLE_NAME,
        table_schema=postgres.TableSchema(
            OutputProduct,
            primary_key=["product_category", "product_name"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )


@coco_aio.lifespan
async def coco_lifespan(
    builder: coco_aio.EnvironmentBuilder,
) -> AsyncIterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")

    async with (
        await postgres.create_pool(DATABASE_URL) as target_pool,
        await postgres.create_pool(SOURCE_DATABASE_URL) as source_pool,
    ):
        builder.provide(PG_DB, postgres.register_db("postgres_source_db", target_pool))
        builder.provide(SOURCE_POOL, source_pool)
        yield


@coco.function(memo=True)
async def process_product(
    scope: coco.Scope,
    product: SourceProduct,
    table: postgres.TableTarget[OutputProduct],
) -> None:
    full_description = f"Category: {product.product_category}\nName: {product.product_name}\n\n{product.description}"
    total_value = product.price * product.amount
    embedding = await _embedder.embed_async(full_description)
    table.declare_row(
        scope,
        row=OutputProduct(
            product_category=product.product_category,
            product_name=product.product_name,
            description=product.description,
            price=product.price,
            amount=product.amount,
            total_value=total_value,
            embedding=embedding,
        ),
    )


@coco.function
async def app_main(scope: coco.Scope) -> None:
    table = await coco_aio.mount_run(setup_table, scope / "setup").result()

    source = postgres.PgTableSource(
        scope.use(SOURCE_POOL),
        table_name="source_products",
        columns=["product_category", "product_name", "description", "price", "amount"],
        row_factory=lambda row: SourceProduct(**row),
    )

    rows_any: list[SourceProduct] | list[dict[str, object]] = await source.rows_async()
    if not all(isinstance(row, SourceProduct) for row in rows_any):
        raise TypeError("Expected SourceProduct rows from PgTableSource.")
    rows = cast(list[SourceProduct], rows_any)
    for product in rows:
        coco_aio.mount(
            process_product,
            scope / "row" / product.product_category / product.product_name,
            product,
            table,
        )


app = coco_aio.App(
    app_main,
    coco_aio.AppConfig(name="PostgresSourceV1"),
)


async def query_once(pool: asyncpg.Pool, query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await _embedder.embed_async(query)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                product_category,
                product_name,
                description,
                amount,
                total_value,
                embedding <=> $1 AS distance
            FROM "{PG_SCHEMA_NAME}"."{TABLE_NAME}"
            ORDER BY distance ASC
            LIMIT $2
            """,
            query_vec,
            top_k,
        )

    for r in rows:
        score = 1.0 - float(r["distance"])
        print(
            f"[{score:.3f}] {r['product_category']} | {r['product_name']} | {r['amount']} | {r['total_value']}"
        )
        print(f"    {r['description']}")
        print("---")


async def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "query":
        async with await postgres.create_pool(DATABASE_URL) as pool:
            if len(sys.argv) > 2:
                q = " ".join(sys.argv[2:])
                await query_once(pool, q)
                return
            print('Usage: python main.py query "your search query"')
        return

    await app.run()


if __name__ == "__main__":
    asyncio.run(main())
