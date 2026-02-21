import os
from datetime import timedelta

import cocoindex
import numpy as np
from dotenv import load_dotenv
from numpy.typing import NDArray
from pydantic import BaseModel
from surrealdb import BlockingWsSurrealConnection

TOP_K = 5

SURREALDB_URL = os.environ.get("COCOINDEX_SURREALDB_URL", "ws://localhost:8000")
SURREALDB_USER = os.environ.get("COCOINDEX_SURREALDB_USER", "root")
SURREALDB_PASSWORD = os.environ.get("COCOINDEX_SURREALDB_PASSWORD", "secret")
SURREALDB_NS = os.environ.get("COCOINDEX_SURREALDB_NS", "cocoindex")
SURREALDB_DB = os.environ.get("COCOINDEX_SURREALDB_DB", "text_embedding_surrealdb")

surrealdb_conn_spec = cocoindex.add_auth_entry(
    "SurrealDBConnection",
    cocoindex.targets.SurrealDBConnection(
        url=SURREALDB_URL,
        namespace=SURREALDB_NS,
        database=SURREALDB_DB,
        user=SURREALDB_USER,
        password=SURREALDB_PASSWORD,
    ),
)


# SurrealDB connection generator
class SurrealDBClient:
    def __enter__(self):
        self.conn = BlockingWsSurrealConnection(SURREALDB_URL)
        self.conn.signin({"username": SURREALDB_USER, "password": SURREALDB_PASSWORD})
        self.conn.use(SURREALDB_NS, SURREALDB_DB)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()


# -- Asynchronous connection generator
# class SurrealDBClientAsync:
#     async def __aenter__(self):
#         self.async_conn = AsyncWsSurrealConnection(SURREALDB_URL)
#         await self.async_conn.signin(
#             {"username": SURREALDB_USER, "password": SURREALDB_PASSWORD}
#         )
#         await self.async_conn.use(SURREALDB_NS, SURREALDB_DB)
#         return self.async_conn

#     async def __aexit__(self, exc_type, exc_val, exc_tb):
#         await self.async_conn.close()


@cocoindex.transform_flow()
def text_to_embedding(
    text: cocoindex.DataSlice[str],
) -> cocoindex.DataSlice[NDArray[np.float32]]:
    """
    Embed the text using a SentenceTransformer model.
    This is a shared logic between indexing and querying, so extract it as a function."""
    # Remote embedding model:
    return text.transform(
        cocoindex.functions.EmbedText(
            api_type=cocoindex.LlmApiType.OPENAI,
            model="text-embedding-3-small",
        )
    )
    # Local embedding model:
    # return text.transform(
    #     cocoindex.functions.SentenceTransformerEmbed(
    #         model="sentence-transformers/all-MiniLM-L6-v2"
    #     )
    # )


@cocoindex.flow_def(name="TextEmbedding")
def text_embedding_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define an example flow that embeds text into a vector database.
    """
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="markdown_files"),
        refresh_interval=timedelta(seconds=5),
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
        cocoindex.targets.SurrealDB(
            surrealdb_conn_spec,
            # TODO: remove table name and expect it to be TextEmbedding__doc_embeddings
            "Chunk",
            # cocoindex.targets.Nodes(label="Document"),
        ),
        primary_key_fields=["filename", "location"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                field_name="embedding",
                metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
            )
        ],
    )


# Declaring it as a query handler, so that you can easily run queries in CocoInsight.
@text_embedding_flow.query_handler(
    result_fields=cocoindex.QueryHandlerResultFields(
        embedding=["embedding"],
        score="score",
    ),
)
def search(query: str) -> cocoindex.QueryOutput:
    class QueryResult(BaseModel):
        # filename: str
        text: str
        score: float

    # Get the table name, for the export target in the text_embedding_flow above.
    # TODO: get table name from text_embedding_flow
    # table_name = cocoindex.utils.get_target_default_name(
    #     text_embedding_flow, "doc_embeddings"
    # )
    table_name = "Chunk"

    # Evaluate the transform flow defined above with the input query, to get the embedding.
    query_vector = text_to_embedding.eval(query)

    # Run the query and get the results.
    threshold = 0.1
    query = f"""SELECT *, score
        OMIT embedding
        FROM (
            SELECT *, (1 - vector::distance::knn()) AS score
            FROM {table_name}
            WHERE embedding <|{TOP_K},40|> $embedding
        )
        WHERE score >= {threshold}
        ORDER BY score DESC;
    """
    with SurrealDBClient() as conn:
        res = conn.query(
            query,
            {"embedding": query_vector.tolist()},
        )
        if isinstance(res, list):
            results = [QueryResult.model_validate(row) for row in res]
        else:
            raise ValueError(f"Unexpected result type: {type(res)}")
        return cocoindex.QueryOutput(
            results=results,
            query_info=cocoindex.QueryInfo(
                embedding=query_vector,
                similarity_metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
            ),
        )


def _main() -> None:
    # Run queries in a loop to demonstrate the query capabilities.
    while True:
        query = input("Enter search query (or Enter to quit): ")
        if query == "":
            break
        # Run the query function with the database connection pool and the query.
        query_output = search(query)
        print("\nSearch results:")
        for result in query_output.results:
            # TODO: fix filename is not included
            # print(f"[{result.score:.3f}] {result.filename}")
            print(f"[{result.score:.3f}]")
            print(f"    {result.text}")
            print("---")
        print()


if __name__ == "__main__":
    load_dotenv()
    cocoindex.init()
    _main()
