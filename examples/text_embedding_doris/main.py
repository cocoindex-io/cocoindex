import os
import datetime
import math
from typing import Any
from dotenv import load_dotenv
import cocoindex
import cocoindex.targets.doris as coco_doris

# Define Doris table name
DORIS_TABLE = "TextEmbedding"


def get_doris_config() -> dict[str, Any]:
    """Get Doris configuration from environment variables."""
    return {
        "fe_host": os.environ.get("DORIS_FE_HOST", "localhost"),
        "fe_http_port": int(os.environ.get("DORIS_HTTP_PORT", "8030")),
        "query_port": int(os.environ.get("DORIS_QUERY_PORT", "9030")),
        "username": os.environ.get("DORIS_USERNAME", "root"),
        "password": os.environ.get("DORIS_PASSWORD", ""),
        "database": os.environ.get("DORIS_DATABASE", "cocoindex_demo"),
    }


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


@cocoindex.flow_def(name="TextEmbeddingWithDoris")
def text_embedding_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define an example flow that embeds text into Apache Doris vector database.
    """
    config = get_doris_config()

    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="markdown_files"),
        refresh_interval=datetime.timedelta(seconds=5),
    )

    doc_embeddings = data_scope.add_collector()

    with data_scope["documents"].row() as doc:
        doc["chunks"] = doc["content"].transform(
            cocoindex.functions.SplitRecursively(),
            language="markdown",
            chunk_size=500,
            chunk_overlap=100,
        )

        with doc["chunks"].row() as chunk:
            chunk["embedding"] = text_to_embedding(chunk["text"])
            doc_embeddings.collect(
                id=cocoindex.GeneratedField.UUID,
                filename=doc["filename"],
                location=chunk["location"],
                text=chunk["text"],
                text_embedding=chunk["embedding"],
            )

    doc_embeddings.export(
        "doc_embeddings",
        coco_doris.DorisTarget(
            fe_host=config["fe_host"],
            fe_http_port=config["fe_http_port"],
            query_port=config["query_port"],
            username=config["username"],
            password=config["password"],
            database=config["database"],
            table=DORIS_TABLE,
        ),
        primary_key_fields=["id"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                "text_embedding", cocoindex.VectorSimilarityMetric.L2_DISTANCE
            )
        ],
        fts_indexes=[
            cocoindex.FtsIndexDef(field_name="text", parameters={"parser": "unicode"})
        ],
    )


@text_embedding_flow.query_handler(
    result_fields=cocoindex.QueryHandlerResultFields(
        embedding=["embedding"],
        score="score",
    ),
)
async def search(query: str) -> cocoindex.QueryOutput:
    print("Searching...", query)
    config = get_doris_config()

    # Get the embedding for the query
    query_embedding = await text_to_embedding.eval_async(query)

    # Build search query using Doris helper
    sql = coco_doris.build_vector_search_query(
        table=f"{config['database']}.{DORIS_TABLE}",
        vector_field="text_embedding",
        query_vector=query_embedding,
        metric="l2_distance",
        limit=5,
        select_columns=["id", "filename", "text"],
    )

    # Execute query
    conn = await coco_doris.connect_async(
        fe_host=config["fe_host"],
        query_port=config["query_port"],
        username=config["username"],
        password=config["password"],
        database=config["database"],
    )

    try:
        async with conn.cursor() as cursor:
            await cursor.execute(sql)
            search_results = await cursor.fetchall()
    finally:
        conn.close()
        await conn.ensure_closed()

    return cocoindex.QueryOutput(
        results=[
            {
                "filename": result[1],
                "text": result[2],
                "embedding": query_embedding,  # Use query embedding for display
                "score": math.sqrt(result[3]) if len(result) > 3 else 0.0,
            }
            for result in search_results
        ],
        query_info=cocoindex.QueryInfo(
            embedding=query_embedding,
            similarity_metric=cocoindex.VectorSimilarityMetric.L2_DISTANCE,
        ),
    )


async def _main() -> None:
    # Run queries in a loop to demonstrate the query capabilities.
    while True:
        query = input("Enter search query (or Enter to quit): ")
        if query == "":
            break

        # Run the async query function
        query_output = await search(query)
        print("\nSearch results:")
        for result in query_output.results:
            print(f"[{result['score']:.3f}] {result['filename']}")
            print(f"    {result['text'][:200]}...")
            print("---")
        print()


if __name__ == "__main__":
    import asyncio

    load_dotenv()
    cocoindex.init()
    asyncio.run(_main())
