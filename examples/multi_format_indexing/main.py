import cocoindex
import os
import mimetypes

from dotenv import load_dotenv
from dataclasses import dataclass
from pdf2image import convert_from_bytes
from io import BytesIO

from qdrant_client import QdrantClient

QDRANT_GRPC_URL = "http://localhost:6334"
QDRANT_COLLECTION = "MultiFormatIndexings"
COLPALI_MODEL_NAME = os.getenv("COLPALI_MODEL", "vidore/colpali-v1.2")


@dataclass
class Page:
    page_number: int | None
    image: bytes


@cocoindex.op.function()
def file_to_pages(filename: str, content: bytes) -> list[Page]:
    """
    Classify file content based on MIME type detection.
    Returns ClassifiedFileContent with appropriate field populated based on file type.
    """
    # Guess the MIME type based on the filename
    mime_type, _ = mimetypes.guess_type(filename)

    if mime_type == "application/pdf":
        images = convert_from_bytes(content, dpi=300)
        pages = []
        for i, image in enumerate(images):
            with BytesIO() as buffer:
                image.save(buffer, format="PNG")
                pages.append(Page(page_number=i + 1, image=buffer.getvalue()))
        return pages
    elif mime_type and mime_type.startswith("image/"):
        return [Page(page_number=None, image=content)]
    else:
        return []


qdrant_connection = cocoindex.add_auth_entry(
    "qdrant_connection",
    cocoindex.targets.QdrantConnection(grpc_url=QDRANT_GRPC_URL),
)


@cocoindex.flow_def(name="MultiFormatIndexing")
def multi_format_indexing_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define an example flow that embeds files into a vector database.
    """
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="source_files", binary=True)
    )

    output_embeddings = data_scope.add_collector()

    with data_scope["documents"].row() as doc:
        doc["pages"] = flow_builder.transform(
            file_to_pages, filename=doc["filename"], content=doc["content"]
        )
        with doc["pages"].row() as page:
            page["embedding"] = page["image"].transform(
                cocoindex.functions.ColPaliEmbedImage(model=COLPALI_MODEL_NAME)
            )
            output_embeddings.collect(
                id=cocoindex.GeneratedField.UUID,
                filename=doc["filename"],
                page=page["page_number"],
                embedding=page["embedding"],
            )

    output_embeddings.export(
        "multi_format_indexings",
        cocoindex.targets.Qdrant(
            connection=qdrant_connection,
            collection_name=QDRANT_COLLECTION,
        ),
        primary_key_fields=["id"],
    )


@cocoindex.transform_flow()
def query_to_colpali_embedding(
    text: cocoindex.DataSlice[str],
) -> cocoindex.DataSlice[list[list[float]]]:
    return text.transform(
        cocoindex.functions.ColPaliEmbedQuery(model=COLPALI_MODEL_NAME)
    )


def _main() -> None:
    # Initialize Qdrant client
    client = QdrantClient(url=QDRANT_GRPC_URL, prefer_grpc=True)

    # Run queries in a loop to demonstrate the query capabilities.
    while True:
        query = input("Enter search query (or Enter to quit): ")
        if query == "":
            break

        # Get the embedding for the query
        query_embedding = query_to_colpali_embedding.eval(query)

        search_results = client.query_points(
            collection_name=QDRANT_COLLECTION,
            query=query_embedding,  # Multi-vector format: list[list[float]]
            using="embedding",  # Specify the vector field name
            limit=5,
            with_payload=True,
        )
        print("\nSearch results:")
        for result in search_results.points:
            score = result.score
            payload = result.payload
            if payload is None:
                continue
            page_number = payload["page"]
            page_number_str = f"Page:{page_number}" if page_number is not None else ""
            print(f"[{score:.3f}] {payload['filename']} {page_number_str}")
            print("---")
        print()


if __name__ == "__main__":
    load_dotenv()
    cocoindex.init()
    _main()
