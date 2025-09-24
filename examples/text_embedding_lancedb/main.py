from dotenv import load_dotenv
import datetime
import cocoindex
import math
import cocoindex.targets.lancedb as coco_lancedb

# Define LanceDB connection constants
LANCEDB_URI = "./lancedb_data"  # Local directory for LanceDB
LANCEDB_TABLE = "TextEmbedding"


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


@cocoindex.flow_def(name="TextEmbeddingWithLanceDB")
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
                # 'text_embedding' is the name of the vector we've created the LanceDB table with.
                text_embedding=chunk["embedding"],
            )

    doc_embeddings.export(
        "doc_embeddings",
        coco_lancedb.LanceDB(db_uri=LANCEDB_URI, table_name=LANCEDB_TABLE),
        primary_key_fields=["id"],
        # We cannot enable it when the table has no data yet, as LanceDB requires data to train the index.
        # See: https://github.com/lancedb/lance/issues/4034
        #
        #   vector_indexes=[
        #       cocoindex.VectorIndexDef(
        #           "text_embedding", cocoindex.VectorSimilarityMetric.L2_DISTANCE
        #       ),
        #   ],
    )


@text_embedding_flow.query_handler(
    result_fields=cocoindex.QueryHandlerResultFields(
        embedding=["embedding"],
        score="score",
    ),
)
async def search(query: str) -> cocoindex.QueryOutput:
    print("Searching...", query)
    db = await coco_lancedb.connect_async(LANCEDB_URI)
    table = await db.open_table(LANCEDB_TABLE)

    # Get the embedding for the query
    query_embedding = await text_to_embedding.eval_async(query)

    search = await table.search(query_embedding, vector_column_name="text_embedding")
    search_results = await search.limit(5).to_list()

    print(search_results)

    return cocoindex.QueryOutput(
        results=[
            {
                "filename": result["filename"],
                "text": result["text"],
                "embedding": result["text_embedding"],
                # Qdrant's L2 "distance" is squared, so we take the square root to align with normal L2 distance
                "score": math.sqrt(result["_distance"]),
            }
            for result in search_results
        ],
        query_info=cocoindex.QueryInfo(
            embedding=query_embedding,
            similarity_metric=cocoindex.VectorSimilarityMetric.L2_DISTANCE,
        ),
    )
