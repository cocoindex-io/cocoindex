import cocoindex
import os


@cocoindex.flow_def(name="PostgresMessageIndexing")
def postgres_message_indexing_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define a flow that reads data from a PostgreSQL table, generates embeddings,
    and stores them in another PostgreSQL table with pgvector.
    """

    data_scope["messages"] = flow_builder.add_source(
        cocoindex.sources.Postgres(
            table_name="source_messages",
            # Optional. Use the default CocoIndex database if not specified.
            database=cocoindex.add_transient_auth_entry(
                cocoindex.sources.DatabaseConnectionSpec(
                    url=os.getenv("SOURCE_DATABASE_URL"),
                )
            ),
            # Optional.
            ordinal_column="created_at",
        )
    )

    indexed_messages = data_scope.add_collector()
    with data_scope["messages"].row() as message_row:
        # Use the indexing column for embedding generation
        message_row["embedding"] = message_row["message"].transform(
            cocoindex.functions.SentenceTransformerEmbed(
                model="sentence-transformers/all-MiniLM-L6-v2"
            )
        )
        # Collect the data - include key columns and content
        indexed_messages.collect(
            id=message_row["id"],
            author=message_row["author"],
            message=message_row["message"],
            embedding=message_row["embedding"],
        )

    indexed_messages.export(
        "output",
        cocoindex.targets.Postgres(),
        primary_key_fields=["id"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                field_name="embedding",
                metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
            )
        ],
    )


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


@cocoindex.flow_def(name="PostgresProductIndexing")
def postgres_product_indexing_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define a flow that reads data from a PostgreSQL table, generates embeddings,
    and stores them in another PostgreSQL table with pgvector.
    """
    data_scope["products"] = flow_builder.add_source(
        cocoindex.sources.Postgres(
            table_name="source_products",
            # Optional. Use the default CocoIndex database if not specified.
            database=cocoindex.add_transient_auth_entry(
                cocoindex.sources.DatabaseConnectionSpec(
                    url=os.getenv("SOURCE_DATABASE_URL"),
                )
            ),
            # Optional.
            ordinal_column="modified_time",
        )
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
