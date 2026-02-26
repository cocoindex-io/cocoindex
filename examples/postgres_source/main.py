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
import sys
from dataclasses import dataclass
from typing import Annotated, AsyncIterator

import asyncpg
from numpy.typing import NDArray

import cocoindex as coco
from cocoindex.connectors import postgres
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder


DATABASE_URL = os.getenv(
    "POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex"
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


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    # Provide resources needed across the CocoIndex environment
    async with (
        await postgres.create_pool(DATABASE_URL) as target_pool,
        await postgres.create_pool(SOURCE_DATABASE_URL) as source_pool,
    ):
        builder.provide(PG_DB, postgres.register_db("postgres_source_db", target_pool))
        builder.provide(SOURCE_POOL, source_pool)
        yield


@coco.fn(memo=True)
async def process_product(
    product: SourceProduct,
    table: postgres.TableTarget[OutputProduct],
) -> None:
    full_description = f"Category: {product.product_category}\nName: {product.product_name}\n\n{product.description}"
    total_value = product.price * product.amount
    embedding = await _embedder.embed(full_description)
    table.declare_row(
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


@coco.fn
async def app_main() -> None:
    target_db = coco.use_context(PG_DB)
    target_table = await target_db.mount_table_target(
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            OutputProduct,
            primary_key=["product_category", "product_name"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )

    source = postgres.PgTableSource(
        coco.use_context(SOURCE_POOL),
        table_name="source_products",
        row_type=SourceProduct,
    )

    await coco.mount_each(
        process_product,
        source.fetch_rows().items(lambda p: (p.product_category, p.product_name)),
        target_table,
    )


app = coco.App(
    coco.AppConfig(name="PostgresSourceV1"),
    app_main,
)


async def query_once(pool: asyncpg.Pool, query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await _embedder.embed(query)
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


async def query() -> None:
    async with await postgres.create_pool(DATABASE_URL) as pool:
        if len(sys.argv) > 2:
            q = " ".join(sys.argv[2:])
            await query_once(pool, q)
            return
        print('Usage: python main.py query "your search query"')


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "query":
        asyncio.run(query())
