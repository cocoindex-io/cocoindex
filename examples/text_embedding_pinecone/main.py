import os
import math
import datetime
import cocoindex
from cocoindex.auth_registry import add_auth_entry
from cocoindex.targets._engine_builtin_specs import Pinecone, PineconeConnection
import cocoindex.targets.pinecone as coco_pinecone

# Pinecone connection constants
PINECONE_INDEX = os.environ.get("PINECONE_INDEX", "TextEmbedding")


@cocoindex.transform_flow()
def text_to_embedding(
    text: cocoindex.DataSlice[str],
) -> cocoindex.DataSlice[list[float]]:
    return text.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2"
        )
    )


@cocoindex.flow_def(name="TextEmbeddingWithPinecone")
def text_embedding_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Example flow that embeds text and exports to Pinecone.
    """

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

    # Create Pinecone auth entry from env
    pinecone_api_key = os.environ.get("PINECONE_API_KEY")
    pinecone_env = os.environ.get("PINECONE_ENVIRONMENT")
    if not pinecone_api_key:
        raise RuntimeError(
            "PINECONE_API_KEY is required in environment to run this example"
        )

    pinecone_conn = add_auth_entry(
        "pinecone_connection",
        PineconeConnection(api_key=pinecone_api_key, environment=pinecone_env),
    )

    doc_embeddings.export(
        "doc_embeddings",
        Pinecone(index_name=PINECONE_INDEX, connection=pinecone_conn),
        primary_key_fields=["id"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                "text_embedding", cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY
            )
        ],
    )


@text_embedding_flow.query_handler(
    result_fields=cocoindex.QueryHandlerResultFields(
        embedding=["embedding"],
        score="score",
    ),
)
async def search(query: str) -> cocoindex.QueryOutput:
    # Get query embedding
    query_embedding = await text_to_embedding.eval_async(query)

    # Use helper to get Pinecone index
    index = coco_pinecone.get_index(
        api_key=os.environ["PINECONE_API_KEY"], index_name=PINECONE_INDEX
    )

    # Query Pinecone
    response = index.query(vector=query_embedding, top_k=5)

    results = []
    for match in response.matches:
        metadata = match.get("metadata", {})
        results.append(
            {
                "filename": metadata.get("filename"),
                "text": metadata.get("text"),
                "embedding": match.get("values"),
                "score": match.get("score"),
            }
        )

    return cocoindex.QueryOutput(
        results=results,
        query_info=cocoindex.QueryInfo(
            embedding=query_embedding,
            similarity_metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
        ),
    )
