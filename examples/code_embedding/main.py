from dotenv import load_dotenv
from psycopg_pool import ConnectionPool
from pgvector.psycopg import register_vector
from typing import Any
import cocoindex
import os
from numpy.typing import NDArray
import numpy as np


@cocoindex.op.function()
def extract_extension(filename: str) -> str:
    """Extract the extension of a filename."""
    return os.path.splitext(filename)[1]


@cocoindex.transform_flow()
def code_to_embedding(
    text: cocoindex.DataSlice[str],
) -> cocoindex.DataSlice[NDArray[np.float32]]:
    """
    Embed the text using a SentenceTransformer model.
    """
    # You can also switch to Voyage embedding model:
    #    return text.transform(
    #        cocoindex.functions.EmbedText(
    #            api_type=cocoindex.llm.LlmApiType.VOYAGE,
    #            model="voyage-code-3",
    #        )
    #    )
    return text.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2"
        )
    )


@cocoindex.flow_def(name="CodeEmbedding")
def code_embedding_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define an example flow that embeds files into a vector database.
    """
    data_scope["files"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(
            path="../..",
            included_patterns=["*.py", "*.rs", "*.toml", "*.md", "*.mdx"],
            excluded_patterns=["**/.*", "target", "**/node_modules"],
        )
    )
    code_embeddings = data_scope.add_collector()

    with data_scope["files"].row() as file:
        file["extension"] = file["filename"].transform(extract_extension)
        file["chunks"] = file["content"].transform(
            cocoindex.functions.SplitRecursively(),
            language=file["extension"],
            chunk_size=1000,
            min_chunk_size=300,
            chunk_overlap=300,
        )
        with file["chunks"].row() as chunk:
            chunk["embedding"] = chunk["text"].call(code_to_embedding)
            code_embeddings.collect(
                filename=file["filename"],
                location=chunk["location"],
                code=chunk["text"],
                embedding=chunk["embedding"],
            )

    code_embeddings.export(
        "code_embeddings",
        cocoindex.targets.Postgres(),
        primary_key_fields=["filename", "location"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                field_name="embedding",
                metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
            )
        ],
    )


def search(pool: ConnectionPool, query: str, top_k: int = 5) -> list[dict[str, Any]]:
    # Get the table name, for the export target in the code_embedding_flow above.
    table_name = cocoindex.utils.get_target_default_name(
        code_embedding_flow, "code_embeddings"
    )
    # Evaluate the transform flow defined above with the input query, to get the embedding.
    query_vector = code_to_embedding.eval(query)
    # Run the query and get the results.
    with pool.connection() as conn:
        register_vector(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT filename, code, embedding <=> %s AS distance
                FROM {table_name} ORDER BY distance LIMIT %s
            """,
                (query_vector, top_k),
            )
            return [
                {"filename": row[0], "code": row[1], "score": 1.0 - row[2]}
                for row in cur.fetchall()
            ]


def _main() -> None:
    # Make sure the flow is built and up-to-date.
    stats = code_embedding_flow.update()
    print("Updated index: ", stats)

    # Initialize the database connection pool.
    pool = ConnectionPool(os.getenv("COCOINDEX_DATABASE_URL"))
    # Run queries in a loop to demonstrate the query capabilities.
    while True:
        query = input("Enter search query (or Enter to quit): ")
        if query == "":
            break
        # Run the query function with the database connection pool and the query.
        results = search(pool, query)
        print("\nSearch results:")
        for result in results:
            print(f"[{result['score']:.3f}] {result['filename']}")
            print(f"    {result['code']}")
            print("---")
        print()


if __name__ == "__main__":
    load_dotenv()
    cocoindex.init()
    _main()
