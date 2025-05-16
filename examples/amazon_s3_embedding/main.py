from dotenv import load_dotenv

import cocoindex
import os

@cocoindex.flow_def(name="AmazonS3TextEmbedding")
def amazon_s3_text_embedding_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    """
    Define an example flow that embeds text from Amazon S3 into a vector database.
    """
    bucket_name = os.environ["AMAZON_S3_BUCKET_NAME"]
    prefix = os.environ.get("AMAZON_S3_PREFIX", None)
    sqs_queue_url = os.environ.get("AMAZON_S3_SQS_QUEUE_URL", None)

    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.AmazonS3(
            bucket_name=bucket_name,
            prefix=prefix,
            included_patterns=["*.md", "*.txt", "*.docx"],
            binary=False,
            sqs_queue_url=sqs_queue_url))

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
    flow=amazon_s3_text_embedding_flow,
    target_name="doc_embeddings",
    query_transform_flow=lambda text: text.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2")),
    default_similarity_metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY)

@cocoindex.main_fn()
def _run():
    # Use a `FlowLiveUpdater` to keep the flow data updated.
    with cocoindex.FlowLiveUpdater(amazon_s3_text_embedding_flow):
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
    _run()
