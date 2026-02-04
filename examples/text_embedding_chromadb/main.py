import os
import datetime
import cocoindex
import chromadb
import cocoindex.targets.chromadb as coco_chromadb

# ChromaDB connection defaults (override via env)
CHROMADB_PATH = os.environ.get("CHROMADB_PATH", "./chromadb_data")
CHROMADB_COLLECTION = os.environ.get("CHROMADB_COLLECTION", "text_embedding")


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


@cocoindex.flow_def(name="TextEmbeddingWithChromaDB")
def text_embedding_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define an example flow that embeds text into a vector database.
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

    doc_embeddings.export(
        "doc_embeddings",
        coco_chromadb.ChromaDB(
            collection_name=CHROMADB_COLLECTION,
            path=CHROMADB_PATH,
            document_field="text",
        ),
        primary_key_fields=["id"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                "text_embedding",
                cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
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
    print("Searching...", query)
    client = chromadb.PersistentClient(path=CHROMADB_PATH)
    collection = client.get_collection(name=CHROMADB_COLLECTION)

    # Get the embedding for the query
    query_embedding = await text_to_embedding.eval_async(query)

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=5,
        include=["documents", "metadatas", "embeddings", "distances"],
    )

    return cocoindex.QueryOutput(
        results=[
            {
                "filename": results["metadatas"][0][i].get("filename", ""),
                "text": (results["documents"][0][i] if results["documents"] else ""),
                "embedding": (
                    results["embeddings"][0][i] if results["embeddings"] else []
                ),
                "score": 1.0 - results["distances"][0][i],
            }
            for i in range(len(results["ids"][0]))
        ],
        query_info=cocoindex.QueryInfo(
            embedding=query_embedding,
            similarity_metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
        ),
    )
