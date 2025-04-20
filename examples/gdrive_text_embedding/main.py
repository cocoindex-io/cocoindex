from dotenv import load_dotenv

import asyncio
import cocoindex
import datetime
import os

@cocoindex.flow_def(name="GoogleDriveTextEmbedding")
def gdrive_text_embedding_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    """
    Define an example flow that embeds text into a vector database.
    """
    credential_path = os.environ["GOOGLE_SERVICE_ACCOUNT_CREDENTIAL"]
    root_folder_ids = os.environ["GOOGLE_DRIVE_ROOT_FOLDER_IDS"].split(",")

    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.GoogleDrive(
            service_account_credential_path=credential_path,
            root_folder_ids=root_folder_ids,
            recent_changes_poll_interval=datetime.timedelta(seconds=10)),
        refresh_interval=datetime.timedelta(minutes=1))

    doc_embeddings = data_scope.add_collector()

    with data_scope["documents"].row() as doc:
        doc["chunks"] = doc["content"].transform(
            cocoindex.functions.SplitRecursively(),
            language="markdown", chunk_size=2000, chunk_overlap=500)

        with doc["chunks"].row() as chunk:
            chunk["embedding"] = chunk["text"].transform(
                cocoindex.functions.SentenceTransformerEmbed(
                 model="sentence-transformers/all-MiniLM-L6-v2")) 
            doc_embeddings.collect(filename=doc["filename"], location=chunk["location"],
                                   text=chunk["text"], embedding=chunk["embedding"])

    doc_embeddings.export(
        "doc_embeddings",
        cocoindex.storages.Postgres(),
        primary_key_fields=["filename", "location"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                field_name="embedding",
                metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY)])

query_handler = cocoindex.query.SimpleSemanticsQueryHandler(
    name="SemanticsSearch",
    flow=gdrive_text_embedding_flow,
    target_name="doc_embeddings",
    query_transform_flow=lambda text: text.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2")),
    default_similarity_metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY)

@cocoindex.main_fn()
async def _run():
    # Use a `FlowLiveUpdater` to keep the flow data updated.
    async with cocoindex.FlowLiveUpdater(gdrive_text_embedding_flow):
        # Run queries in a loop to demonstrate the query capabilities.
        while True:
            try:
                query = input("Enter search query (or Enter to quit): ")
                if query == '':
                    break
                results, _ = query_handler.search(query, 10)
                print("\nSearch results:")
                for result in results:
                    print(f"[{result.score:.3f}] {result.data['filename']}")
                    print(f"    {result.data['text']}")
                    print("---")
                print()
            except KeyboardInterrupt:
                break

if __name__ == "__main__":
    load_dotenv(override=True)
    asyncio.run(_run())
