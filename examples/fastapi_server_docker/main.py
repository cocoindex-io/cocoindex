import cocoindex
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from psycopg_pool import ConnectionPool
import os


@cocoindex.transform_flow()
def text_to_embedding(
    text: cocoindex.DataSlice[str],
) -> cocoindex.DataSlice[list[float]]:
    """
    Embed the text using a SentenceTransformer model.
    This is a shared logic between indexing and querying.
    """
    return text.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2"
        )
    )


@cocoindex.flow_def(name="MarkdownEmbeddingFastApiExample")
def markdown_embedding_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
):
    """
    Define an example flow that embeds markdown files into a vector database.
    """
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="files")
    )
    doc_embeddings = data_scope.add_collector()

    with data_scope["documents"].row() as doc:
        doc["chunks"] = doc["content"].transform(
            cocoindex.functions.SplitRecursively(),
            language="markdown",
            chunk_size=2000,
            chunk_overlap=500,
        )

        with doc["chunks"].row() as chunk:
            chunk["embedding"] = text_to_embedding(chunk["text"])
            doc_embeddings.collect(
                filename=doc["filename"],
                location=chunk["location"],
                text=chunk["text"],
                embedding=chunk["embedding"],
            )

    doc_embeddings.export(
        "doc_embeddings",
        cocoindex.storages.Postgres(),
        primary_key_fields=["filename", "location"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                field_name="embedding",
                metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
            )
        ],
    )


def search(pool: ConnectionPool, query: str, top_k: int = 5):
    # Get the table name, for the export target in the text_embedding_flow above.
    table_name = cocoindex.utils.get_target_storage_default_name(
        markdown_embedding_flow, "doc_embeddings"
    )
    # Evaluate the transform flow defined above with the input query, to get the embedding.
    query_vector = text_to_embedding.eval(query)
    # Run the query and get the results.
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT filename, text, embedding <=> %s::vector AS distance
                FROM {table_name} ORDER BY distance LIMIT %s
            """,
                (query_vector, top_k),
            )
            return [
                {"filename": row[0], "text": row[1], "score": 1.0 - row[2]}
                for row in cur.fetchall()
            ]


fastapi_app = FastAPI()


@fastapi_app.on_event("startup")
def startup_event():
    load_dotenv()
    cocoindex.init()
    # Initialize database connection pool
    fastapi_app.state.pool = ConnectionPool(os.getenv("COCOINDEX_DATABASE_URL"))


@fastapi_app.get("/search")
def search_endpoint(
    q: str = Query(..., description="Search query"),
    limit: int = Query(5, description="Number of results"),
):
    results = search(fastapi_app.state.pool, q, limit)
    return {"results": results}


if __name__ == "__main__":
    load_dotenv()
    cocoindex.init()
    uvicorn.run(fastapi_app, host="0.0.0.0", port=8080)
