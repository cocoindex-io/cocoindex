import cocoindex
import os
import tempfile
from typing import List, Dict, Any

from dotenv import load_dotenv
from marker.config.parser import ConfigParser
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import text_from_rendered
from psycopg_pool import ConnectionPool
from jinja2 import Template


class PdfToMarkdown(cocoindex.op.FunctionSpec):
    """Convert a PDF to markdown."""


@cocoindex.op.executor_class(gpu=True, cache=True, behavior_version=1)
class PdfToMarkdownExecutor:
    """Executor for PdfToMarkdown."""

    spec: PdfToMarkdown
    _converter: PdfConverter

    def prepare(self):
        config_parser = ConfigParser({})
        self._converter = PdfConverter(
            create_model_dict(), config=config_parser.generate_config_dict()
        )

    def __call__(self, content: bytes) -> str:
        with tempfile.NamedTemporaryFile(delete=True, suffix=".pdf") as temp_file:
            temp_file.write(content)
            temp_file.flush()
            text, _, _ = text_from_rendered(self._converter(temp_file.name))
            return text


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


@cocoindex.flow_def(name="PdfEmbedding")
def pdf_embedding_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
):
    """
    Define an example flow that embeds files into a vector database.
    """
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="pdf_files", binary=True)
    )

    pdf_embeddings = data_scope.add_collector()

    with data_scope["documents"].row() as doc:
        doc["markdown"] = doc["content"].transform(PdfToMarkdown())
        doc["chunks"] = doc["markdown"].transform(
            cocoindex.functions.SplitRecursively(),
            language="markdown",
            chunk_size=2000,
            chunk_overlap=500,
        )

        with doc["chunks"].row() as chunk:
            chunk["embedding"] = text_to_embedding(chunk["text"])
            pdf_embeddings.collect(
                id=cocoindex.GeneratedField.UUID,
                filename=doc["filename"],
                location=chunk["location"],
                text=chunk["text"],
                embedding=chunk["embedding"],
            )

    pdf_embeddings.export(
        "pdf_embeddings",
        cocoindex.targets.Postgres(),
        primary_key_fields=["id"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                field_name="embedding",
                metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
            )
        ],
    )


def search(pool: ConnectionPool, query: str, top_k: int = 5):
    # Get the table name, for the export target in the pdf_embedding_flow above.
    table_name = cocoindex.utils.get_target_default_name(
        pdf_embedding_flow, "pdf_embeddings"
    )
    # Evaluate the transform flow defined above with the input query, to get the embedding.
    query_vector = text_to_embedding.eval(query)
    # Run the query and get the results.
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT filename, text, embedding <=> %s::vector AS distance
                FROM {table_name} ORDER BY distance LIMIT %s
            """,
                (query_vector, top_k),
            )
            return [
                {"filename": row[0], "text": row[1], "score": 1.0 - row[2]}
                for row in cur.fetchall()
            ]


# Define the search results template using Jinja2
SEARCH_RESULTS_TEMPLATE = Template("""
Search results:
{% for result in results %}
[{{ "%.3f"|format(result.score) }}] {{ result.filename }}
    {{ result.text }}
---
{% endfor %}
""")


def _main():
    # Initialize the database connection pool.
    pool = ConnectionPool(os.getenv("COCOINDEX_DATABASE_URL"))
    # Run queries in a loop to demonstrate the query capabilities.
    while True:
        query = input("Enter search query (or Enter to quit): ")
        if query == "":
            break
        # Run the query function with the database connection pool and the query.
        results = search(pool, query)
        print(SEARCH_RESULTS_TEMPLATE.render(results=results))


if __name__ == "__main__":
    load_dotenv()
    cocoindex.init()
    _main()
