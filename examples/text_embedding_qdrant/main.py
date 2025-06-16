from dotenv import load_dotenv
from qdrant_client import QdrantClient
import cocoindex

# Define Qdrant connection constants
QDRANT_URL = "http://localhost:6334"
QDRANT_COLLECTION = "TextEmbedding"


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


@cocoindex.flow_def(name="TextEmbeddingWithQdrant")
def text_embedding_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define an example flow that embeds text into a vector database.
    """
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="markdown_files")
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
                id=cocoindex.GeneratedField.UUID,
                filename=doc["filename"],
                location=chunk["location"],
                text=chunk["text"],
                # 'text_embedding' is the name of the vector we've created the Qdrant collection with.
                text_embedding=chunk["embedding"],
            )

    doc_embeddings.export(
        "doc_embeddings",
        cocoindex.targets.Qdrant(collection_name=QDRANT_COLLECTION),
        primary_key_fields=["id"],
    )


def _main() -> None:
    # Initialize Qdrant client
    client = QdrantClient(url=QDRANT_URL, prefer_grpc=True)

    # Run queries in a loop to demonstrate the query capabilities.
    while True:
        query = input("Enter search query (or Enter to quit): ")
        if query == "":
            break

        # Get the embedding for the query
        query_embedding = text_to_embedding.eval(query)

        search_results = client.search(
            collection_name=QDRANT_COLLECTION,
            query_vector=("text_embedding", query_embedding),
            limit=10,
        )
        print("\nSearch results:")
        for result in search_results:
            score = result.score
            payload = result.payload
            if payload is None:
                continue
            print(f"[{score:.3f}] {payload['filename']}")
            print(f"    {payload['text']}")
            print("---")
        print()


if __name__ == "__main__":
    load_dotenv()
    cocoindex.init()
    _main()
