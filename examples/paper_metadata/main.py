import cocoindex
import io
import tempfile
import dataclasses
import datetime

from marker.config.parser import ConfigParser
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import text_from_rendered
from functools import cache
from pypdf import PdfReader, PdfWriter


@cache
def get_marker_converter() -> PdfConverter:
    config_parser = ConfigParser({})
    return PdfConverter(
        create_model_dict(), config=config_parser.generate_config_dict()
    )


@dataclasses.dataclass
class PaperBasicInfo:
    num_pages: int
    first_page: bytes


@cocoindex.op.function()
def extract_basic_info(content: bytes) -> PaperBasicInfo:
    """Extract the first pages of a PDF."""
    reader = PdfReader(io.BytesIO(content))

    output = io.BytesIO()
    writer = PdfWriter()
    writer.add_page(reader.pages[0])
    writer.write(output)

    return PaperBasicInfo(num_pages=len(reader.pages), first_page=output.getvalue())


@dataclasses.dataclass
class Author:
    """One author of the paper."""

    name: str
    email: str | None
    affiliation: str | None


@dataclasses.dataclass
class PaperMetadata:
    """
    Metadata for a paper.
    """

    title: str
    authors: list[Author]
    abstract: str


@cocoindex.op.function(gpu=True, cache=True, behavior_version=2)
def pdf_to_markdown(content: bytes) -> str:
    """Convert to Markdown."""

    with tempfile.NamedTemporaryFile(delete=True, suffix=".pdf") as temp_file:
        temp_file.write(content)
        temp_file.flush()
        text, _, _ = text_from_rendered(get_marker_converter()(temp_file.name))
        return text


@cocoindex.flow_def(name="PaperMetadata")
def paper_metadata_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define an example flow that embeds files into a vector database.
    """
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="papers", binary=True),
        refresh_interval=datetime.timedelta(seconds=10),
    )

    paper_metadata = data_scope.add_collector()
    author_papers = data_scope.add_collector()
    metadata_embeddings = data_scope.add_collector()

    with data_scope["documents"].row() as doc:
        # Extract metadata
        doc["basic_info"] = doc["content"].transform(extract_basic_info)
        doc["first_page_md"] = doc["basic_info"]["first_page"].transform(
            pdf_to_markdown
        )
        doc["metadata"] = doc["first_page_md"].transform(
            cocoindex.functions.ExtractByLlm(
                llm_spec=cocoindex.LlmSpec(
                    api_type=cocoindex.LlmApiType.OPENAI, model="gpt-4o"
                ),
                output_type=PaperMetadata,
                instruction="Please extract the metadata from the first page of the paper.",
            )
        )

        # Collect metadata
        paper_metadata.collect(
            filename=doc["filename"],
            title=doc["metadata"]["title"],
            authors=doc["metadata"]["authors"],
            abstract=doc["metadata"]["abstract"],
            num_pages=doc["basic_info"]["num_pages"],
        )

        # Collect author to filename mapping
        with doc["metadata"]["authors"].row() as author:
            author_papers.collect(
                author_name=author["name"],
                filename=doc["filename"],
            )

        # Embed title and abstract, and collect embeddings
        doc["title_embedding"] = doc["metadata"]["title"].transform(
            cocoindex.functions.SentenceTransformerEmbed(
                model="sentence-transformers/all-MiniLM-L6-v2"
            )
        )
        doc["abstract_chunks"] = doc["metadata"]["abstract"].transform(
            cocoindex.functions.SplitRecursively(
                custom_languages=[
                    cocoindex.functions.CustomLanguageSpec(
                        language_name="abstract",
                        separators_regex=[r"[.?!]+\s+", r"[:;]\s+", r",\s+", r"\s+"],
                    )
                ]
            ),
            language="abstract",
            chunk_size=500,
            min_chunk_size=200,
            chunk_overlap=150,
        )
        metadata_embeddings.collect(
            id=cocoindex.GeneratedField.UUID,
            filename=doc["filename"],
            location="title",
            text=doc["metadata"]["title"],
            embedding=doc["title_embedding"],
        )
        with doc["abstract_chunks"].row() as chunk:
            chunk["embedding"] = chunk["text"].transform(
                cocoindex.functions.SentenceTransformerEmbed(
                    model="sentence-transformers/all-MiniLM-L6-v2"
                )
            )
            metadata_embeddings.collect(
                id=cocoindex.GeneratedField.UUID,
                filename=doc["filename"],
                location="abstract",
                text=chunk["text"],
                embedding=chunk["embedding"],
            )

    paper_metadata.export(
        "paper_metadata",
        cocoindex.targets.Postgres(),
        primary_key_fields=["filename"],
    )
    author_papers.export(
        "author_papers",
        cocoindex.targets.Postgres(),
        primary_key_fields=["author_name", "filename"],
    )
    metadata_embeddings.export(
        "metadata_embeddings",
        cocoindex.targets.Postgres(),
        primary_key_fields=["id"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                field_name="embedding",
                metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
            )
        ],
    )
