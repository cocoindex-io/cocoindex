from typing import Any
import os
import datetime

from dotenv import load_dotenv
from psycopg_pool import ConnectionPool
from pgvector.psycopg import register_vector  # type: ignore[import-untyped]
from psycopg.rows import dict_row
from numpy.typing import NDArray

import numpy as np
import cocoindex


@cocoindex.op.function()
def calculate_total_value(
    price: float,
    amount: int,
) -> float:
    return price * amount


@cocoindex.op.function()
def make_full_description(
    category: str,
    name: str,
    description: str,
) -> str:
    return f"Category: {category}\nName: {name}\n\n{description}"


@cocoindex.transform_flow()
def text_to_embedding(
    text: cocoindex.DataSlice[str],
) -> cocoindex.DataSlice[NDArray[np.float32]]:
    """
    Embed the text using a SentenceTransformer model.
    This is a shared logic between indexing and querying, so extract it as a function.
    """
    return text.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2"
        )
    )


@cocoindex.flow_def(name="PostgresProductIndexing")
def postgres_product_indexing_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define a flow that reads product data from a PostgreSQL table, generates embeddings,
    and stores them in another PostgreSQL table with pgvector.
    """
    data_scope["products"] = flow_builder.add_source(
        cocoindex.sources.Postgres(
            table_name="source_products",
            # Optional. Use the default CocoIndex database if not specified.
            database=cocoindex.add_transient_auth_entry(
                cocoindex.DatabaseConnectionSpec(
                    url=os.environ["SOURCE_DATABASE_URL"],
                )
            ),
            # Optional.
            ordinal_column="modified_time",
            notification=cocoindex.sources.PostgresNotification(),
        ),
    )

    indexed_product = data_scope.add_collector()
    with data_scope["products"].row() as product:
        product["full_description"] = flow_builder.transform(
            make_full_description,
            product["product_category"],
            product["product_name"],
            product["description"],
        )
        product["total_value"] = flow_builder.transform(
            calculate_total_value,
            product["price"],
            product["amount"],
        )
        product["embedding"] = product["full_description"].transform(
            cocoindex.functions.SentenceTransformerEmbed(
                model="sentence-transformers/all-MiniLM-L6-v2"
            )
        )
        indexed_product.collect(
            product_category=product["product_category"],
            product_name=product["product_name"],
            description=product["description"],
            price=product["price"],
            amount=product["amount"],
            total_value=product["total_value"],
            embedding=product["embedding"],
        )

    indexed_product.export(
        "output",
        cocoindex.targets.Postgres(),
        primary_key_fields=["product_category", "product_name"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                field_name="embedding",
                metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
            )
        ],
    )


def search(pool: ConnectionPool, query: str, top_k: int = 5) -> list[dict[str, Any]]:
    # Get the table name, for the export target in the text_embedding_flow above.
    table_name = cocoindex.utils.get_target_default_name(
        postgres_product_indexing_flow, "output"
    )
    # Evaluate the transform flow defined above with the input query, to get the embedding.
    query_vector = text_to_embedding.eval(query)
    # Run the query and get the results.
    with pool.connection() as conn:
        register_vector(conn)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT
                    product_category,
                    product_name,
                    description,
                    amount,
                    total_value,
                    (embedding <=> %s) AS distance
                FROM {table_name}
                ORDER BY distance ASC
                LIMIT %s
            """,
                (query_vector, top_k),
            )
            return cur.fetchall()


def _main() -> None:
    # Initialize the database connection pool.
    pool = ConnectionPool(os.environ["COCOINDEX_DATABASE_URL"])
    # Run queries in a loop to demonstrate the query capabilities.
    while True:
        query = input("Enter search query (or Enter to quit): ")
        if query == "":
            break
        # Run the query function with the database connection pool and the query.
        results = search(pool, query)
        print("\nSearch results:")
        for result in results:
            score = 1.0 - result["distance"]
            print(
                f"[{score:.3f}] {result['product_category']} | {result['product_name']} | {result['amount']} | {result['total_value']}"
            )
            print(f"    {result['description']}")
            print("---")
        print()


if __name__ == "__main__":
    load_dotenv()
    cocoindex.init()
    _main()
