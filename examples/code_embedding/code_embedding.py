from dotenv import load_dotenv

import cocoindex

def code_to_embedding(text: cocoindex.DataSlice) -> cocoindex.DataSlice:
    """
    Embed the text using a SentenceTransformer model.
    """
    return text.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2"))

@cocoindex.flow_def(name="CodeEmbedding")
def code_embedding_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    """
    Define an example flow that embeds files into a vector database.
    """
    data_scope["files"] = flow_builder.add_source(cocoindex.sources.LocalFile(path="."))

    code_embeddings = data_scope.add_collector()

    with data_scope["files"].row() as file:
        file["chunks"] = file["content"].transform(
                    cocoindex.functions.SplitRecursively(
                        language="javascript", chunk_size=300, chunk_overlap=100))
        with file["chunks"].row() as chunk:
            chunk["embedding"] = chunk["text"].call(code_to_embedding)
            code_embeddings.collect(filename=file["filename"], location=chunk["location"],
                                    code=chunk["text"], embedding=chunk["embedding"])

    code_embeddings.export(
        "code_embeddings",
        cocoindex.storages.Postgres(),
        primary_key_fields=["filename", "location"],
        vector_index=[("embedding", cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY)])


query_handler = cocoindex.query.SimpleSemanticsQueryHandler(
    name="SemanticsSearch",
    flow=code_embedding_flow,
    target_name="code_embeddings",
    query_transform_flow=code_to_embedding,
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
                print(f"    {result.data['code']}")
                print("---")
            print()
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    load_dotenv(override=True)
    _run()
