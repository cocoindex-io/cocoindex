from dotenv import load_dotenv

import cocoindex

def text_to_embedding(text: cocoindex.DataSlice) -> cocoindex.DataSlice:
    """
    Embed the text using a SentenceTransformer model.
    This is a shared logic between indexing and querying, so extract it as a function.
    """
    return text.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2"))

@cocoindex.flow_def(name="TextEmbedding")
def text_embedding_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    """
    Define an example flow that embeds text into a vector database.
    """
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="markdown_files"))

    doc_embeddings = data_scope.add_collector()

    with data_scope["documents"].row() as doc:
        doc["chunks"] = doc["content"].transform(
            cocoindex.functions.SplitRecursively(),
            language="markdown", chunk_size=300, chunk_overlap=100)

        with doc["chunks"].row() as chunk:
            chunk["embedding"] = text_to_embedding(chunk["text"])
            doc_embeddings.collect(filename=doc["filename"], location=chunk["location"],
                                   text=chunk["text"], embedding=chunk["embedding"])

    doc_embeddings.export(
        "doc_embeddings",
        cocoindex.storages.Postgres(),
        primary_key_fields=["filename", "location"],
        vector_index=[("embedding", cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY)])

query_handler = cocoindex.query.SimpleSemanticsQueryHandler(
    name="SemanticsSearch",
    flow=text_embedding_flow,
    target_name="doc_embeddings",
    query_transform_flow=text_to_embedding,
    default_similarity_metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY)

@cocoindex.main_fn()
def _run():
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
