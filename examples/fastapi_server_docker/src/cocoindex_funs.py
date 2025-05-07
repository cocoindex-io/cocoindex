import cocoindex
import os

@cocoindex.op.function()
def extract_extension(filename: str) -> str:
    """Extract the extension of a filename."""
    return os.path.splitext(filename)[1]

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
    data_scope["files"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="sample_code",
                                    included_patterns=["*.py", "*.rs", "*.toml", "*.md", "*.mdx", "*.ts", "*.tsx"],
                                    excluded_patterns=[".*", "target", "**/node_modules"]))
    code_embeddings = data_scope.add_collector()

    with data_scope["files"].row() as file:
        file["extension"] = file["filename"].transform(extract_extension)
        file["chunks"] = file["content"].transform(
            cocoindex.functions.SplitRecursively(),
            language=file["extension"], chunk_size=1000, chunk_overlap=300)
        with file["chunks"].row() as chunk:
            chunk["embedding"] = chunk["text"].call(code_to_embedding)
            code_embeddings.collect(filename=file["filename"], location=chunk["location"],
                                    code=chunk["text"], embedding=chunk["embedding"])

    code_embeddings.export(
        "code_embeddings",
        cocoindex.storages.Postgres(),
        primary_key_fields=["filename", "location"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                field_name="embedding",
                metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY)])
