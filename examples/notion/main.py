from dotenv import load_dotenv
from psycopg_pool import ConnectionPool
import cocoindex
import os
from typing import Any


@cocoindex.transform_flow()
def text_to_embedding(
    text: cocoindex.DataSlice[str],
) -> cocoindex.DataSlice[list[float]]:
    """
    Embed the text using a SentenceTransformer model.
    This is a shared logic between indexing and querying, so extract it as a function.
    """
    return text.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2"
        )
    )


@cocoindex.flow_def(name="NotionTextEmbedding")
def notion_text_embedding_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define an example flow that embeds text from Notion databases and pages into a vector database.
    """
    notion_token = os.environ["NOTION_TOKEN"]

    # Add Notion source
    database_ids = (
        os.environ.get("NOTION_DATABASE_IDS", "").split(",")
        if os.environ.get("NOTION_DATABASE_IDS")
        else []
    )
    page_ids = (
        os.environ.get("NOTION_PAGE_IDS", "").split(",")
        if os.environ.get("NOTION_PAGE_IDS")
        else []
    )

    # For now, let's use only one type at a time to avoid conflicts
    if database_ids:
        data_scope["notion_content"] = flow_builder.add_source(
            cocoindex.sources.Notion(
                token=notion_token,
                source_type="database",
                database_ids=database_ids,
            )
        )
    elif page_ids:
        data_scope["notion_content"] = flow_builder.add_source(
            cocoindex.sources.Notion(
                token=notion_token,
                source_type="page",
                page_ids=page_ids,
            )
        )
    else:
        # If no IDs provided, create a dummy source that won't produce any data
        data_scope["notion_content"] = flow_builder.add_source(
            cocoindex.sources.Notion(
                token=notion_token,
                source_type="page",
                page_ids=[],
            )
        )

    doc_embeddings = data_scope.add_collector()

    # Process Notion content
    with data_scope["notion_content"].row() as notion_entry:
        print(f"""DEBUG: Processing notion entry content {notion_entry["content"]}""")

        notion_entry["chunks"] = notion_entry["content"].transform(
            cocoindex.functions.SplitRecursively(),
            language="markdown",
            chunk_size=200,
            chunk_overlap=0,
        )

        with notion_entry["chunks"].row() as chunk:
            print("row")
            chunk["embedding"] = text_to_embedding(chunk["text"])
            doc_embeddings.collect(
                notion_id=notion_entry["id"],
                title=notion_entry["title"],
                location=chunk["location"],
                text=chunk["text"],
                embedding=chunk["embedding"],
            )

    doc_embeddings.export(
        "doc_embeddings",
        cocoindex.targets.Postgres(),
        primary_key_fields=["notion_id", "location"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                field_name="embedding",
                metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
            )
        ],
    )


def search(pool: ConnectionPool, query: str, top_k: int = 5) -> list[dict[str, Any]]:
    # Get the table name, for the export target in the notion_text_embedding_flow above.
    table_name = cocoindex.utils.get_target_default_name(
        notion_text_embedding_flow, "doc_embeddings"
    )
    # Evaluate the transform flow defined above with the input query, to get the embedding.
    query_vector = text_to_embedding.eval(query)
    # Run the query and get the results.
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT title, text, embedding <=> %s::vector AS distance
                FROM {table_name} ORDER BY distance LIMIT %s
            """,
                (query_vector, top_k),
            )
            return [
                {
                    "title": row[0],
                    "text": row[1],
                    "score": 1.0 - row[2],
                }
                for row in cur.fetchall()
            ]


def _main() -> None:
    # Initialize the database connection pool.
    pool = ConnectionPool(os.getenv("COCOINDEX_DATABASE_URL"))

    notion_text_embedding_flow.setup()
    # with cocoindex.FlowLiveUpdater(notion_text_embedding_flow):
    if 1:
        # Run queries in a loop to demonstrate the query capabilities.
        while True:
            query = input("Enter search query (or Enter to quit): ")
            if query == "":
                break
            # Run the query function with the database connection pool and the query.
            results = search(pool, query)
            print("\nSearch results:")
            for result in results:
                print(f"[{result['score']:.3f}] {result['title']}")
                print(f"    {result['text']}")
                print("---")
            print()


if __name__ == "__main__":
    load_dotenv()
    cocoindex.init()
    _main()
