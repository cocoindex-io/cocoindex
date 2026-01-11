"""
Paper Metadata (v1) - CocoIndex pipeline example.

- Walk local PDF files
- Extract the first page text
- Use an LLM to extract title/authors/abstract
- Embed title and abstract chunks (SentenceTransformers)
- Store metadata and embeddings in Postgres (pgvector)
- Query demo using pgvector cosine distance (<=>)
"""

from __future__ import annotations

import asyncio
import functools
import io
import os
import pathlib
import sys
import uuid
from dataclasses import dataclass
from typing import AsyncIterator, Annotated

import asyncpg
from numpy.typing import NDArray
from openai import OpenAI
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from pypdf import PdfReader, PdfWriter

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import localfs, postgres
from cocoindex.extras.text import CustomLanguageConfig, RecursiveSplitter
from cocoindex.extras.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.file import FileLike, PatternFilePathMatcher


TABLE_METADATA = "paper_metadata"
TABLE_AUTHOR_PAPERS = "author_papers"
TABLE_EMBEDDINGS = "metadata_embeddings"
PG_SCHEMA_NAME = "coco_examples_v1"
LLM_MODEL = "gpt-4o"
TOP_K = 5


PG_DB = coco.ContextKey[postgres.PgDatabase]("pg_db")

_embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")

_abstract_splitter = RecursiveSplitter(
    custom_languages=[
        CustomLanguageConfig(
            language_name="abstract",
            separators_regex=[r"[.?!]+\s+", r"[:;]\s+", r",\s+", r"\s+"],
        )
    ]
)


@functools.cache
def openai_client() -> OpenAI:
    return OpenAI()


# =========================================================================
# Data models
# =========================================================================


@dataclass
class PaperBasicInfo:
    num_pages: int
    first_page: bytes


class AuthorModel(BaseModel):
    name: str
    email: str | None = None
    affiliation: str | None = None


class PaperMetadataModel(BaseModel):
    title: str
    authors: list[AuthorModel] = Field(default_factory=list)
    abstract: str


@dataclass
class PaperMetadataRow:
    filename: str
    title: str
    authors: list[dict[str, str | None]]
    abstract: str
    num_pages: int


@dataclass
class AuthorPaperRow:
    author_name: str
    filename: str


@dataclass
class MetadataEmbeddingRow:
    id: uuid.UUID
    filename: str
    location: str
    text: str
    embedding: Annotated[NDArray, _embedder]


# =========================================================================
# PDF + LLM extraction
# =========================================================================


@coco.function
def extract_basic_info(scope: coco.Scope, content: bytes) -> PaperBasicInfo:
    """Extract first page bytes and page count from a PDF."""
    reader = PdfReader(io.BytesIO(content))

    output = io.BytesIO()
    writer = PdfWriter()
    writer.add_page(reader.pages[0])
    writer.write(output)

    return PaperBasicInfo(num_pages=len(reader.pages), first_page=output.getvalue())


@coco.function
def pdf_to_markdown(scope: coco.Scope, content: bytes) -> str:
    """Convert PDF bytes to text using pypdf."""
    reader = PdfReader(io.BytesIO(content))
    page_text = reader.pages[0].extract_text() if reader.pages else ""
    return page_text or ""


@coco.function
def extract_metadata(scope: coco.Scope, markdown: str) -> PaperMetadataModel:
    """Extract paper metadata from first-page text using an LLM."""
    client = openai_client()
    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "You extract metadata from academic paper first pages. "
                    "Return only JSON with keys: title, authors, abstract. "
                    "authors is a list of {name, email, affiliation}. "
                    "Use null for missing fields."
                ),
            },
            {
                "role": "user",
                "content": markdown[:4000],
            },
        ],
        response_format={"type": "json_object"},
        temperature=0,
    )

    content = response.choices[0].message.content
    if not content:
        raise RuntimeError("LLM returned empty content.")
    return PaperMetadataModel.model_validate_json(content)


@coco_aio.lifespan
async def coco_lifespan(
    builder: coco_aio.EnvironmentBuilder,
) -> AsyncIterator[None]:
    # For CocoIndex internal states
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    # Provide resources needed across the CocoIndex environment
    database_url = os.getenv("COCOINDEX_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("COCOINDEX_DATABASE_URL or DATABASE_URL is not set")

    async with await postgres.create_pool(database_url) as pool:
        builder.provide(PG_DB, postgres.register_db("paper_metadata_db", pool))
        yield


@coco.function(memo=True)
async def process_file(
    scope: coco.Scope,
    file: FileLike,
    metadata_table: postgres.TableTarget[PaperMetadataRow],
    author_table: postgres.TableTarget[AuthorPaperRow],
    embedding_table: postgres.TableTarget[MetadataEmbeddingRow],
) -> None:
    content = file.read()

    basic_info = extract_basic_info(scope, content)
    first_page_md = pdf_to_markdown(scope, basic_info.first_page)
    metadata = extract_metadata(scope, first_page_md)

    authors_payload = [a.model_dump() for a in metadata.authors]

    metadata_table.declare_row(
        scope,
        row=PaperMetadataRow(
            filename=str(file.relative_path),
            title=metadata.title,
            authors=authors_payload,
            abstract=metadata.abstract,
            num_pages=basic_info.num_pages,
        ),
    )

    for author in metadata.authors:
        if author.name:
            author_table.declare_row(
                scope,
                row=AuthorPaperRow(
                    author_name=author.name,
                    filename=str(file.relative_path),
                ),
            )

    title_embedding = await _embedder.embed_async(metadata.title)
    embedding_table.declare_row(
        scope,
        row=MetadataEmbeddingRow(
            id=uuid.uuid4(),
            filename=str(file.relative_path),
            location="title",
            text=metadata.title,
            embedding=title_embedding,
        ),
    )

    abstract_chunks = _abstract_splitter.split(
        metadata.abstract,
        chunk_size=500,
        min_chunk_size=200,
        chunk_overlap=150,
        language="abstract",
    )
    for chunk in abstract_chunks:
        embedding_table.declare_row(
            scope,
            row=MetadataEmbeddingRow(
                id=uuid.uuid4(),
                filename=str(file.relative_path),
                location="abstract",
                text=chunk.text,
                embedding=await _embedder.embed_async(chunk.text),
            ),
        )


@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path) -> None:
    target_db = scope.use(PG_DB)
    metadata_table = coco.mount_run(
        target_db.declare_table_target,
        scope / "setup" / "paper_metadata",
        table_name=TABLE_METADATA,
        table_schema=postgres.TableSchema(
            PaperMetadataRow,
            primary_key=["filename"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    ).result()
    author_table = coco.mount_run(
        target_db.declare_table_target,
        scope / "setup" / "author_papers",
        table_name=TABLE_AUTHOR_PAPERS,
        table_schema=postgres.TableSchema(
            AuthorPaperRow,
            primary_key=["author_name", "filename"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    ).result()
    embedding_table = coco.mount_run(
        target_db.declare_table_target,
        scope / "setup" / "metadata_embeddings",
        table_name=TABLE_EMBEDDINGS,
        table_schema=postgres.TableSchema(
            MetadataEmbeddingRow,
            primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    ).result()

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["*.pdf"]),
    )
    for f in files:
        coco.mount(
            process_file,
            scope / "file" / str(f.relative_path),
            f,
            metadata_table,
            author_table,
            embedding_table,
        )


app = coco_aio.App(
    app_main,
    coco_aio.AppConfig(name="PaperMetadataV1"),
    sourcedir=pathlib.Path("./papers"),
)


# =========================================================================
# Query demo (no vector index)
# =========================================================================


async def query_once(pool: asyncpg.Pool, query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await _embedder.embed_async(query)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                filename,
                location,
                text,
                embedding <=> $1 AS distance
            FROM "{PG_SCHEMA_NAME}"."{TABLE_EMBEDDINGS}"
            ORDER BY distance ASC
            LIMIT $2
            """,
            query_vec,
            top_k,
        )

    for r in rows:
        score = 1.0 - float(r["distance"])
        print(f"[{score:.3f}] {r['filename']} ({r['location']})")
        print(f"    {r['text']}")
        print("---")


async def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "query":
        database_url = os.getenv("COCOINDEX_DATABASE_URL") or os.getenv("DATABASE_URL")
        if not database_url:
            raise ValueError("COCOINDEX_DATABASE_URL or DATABASE_URL is not set")

        async with await postgres.create_pool(database_url) as pool:
            if len(sys.argv) > 2:
                q = " ".join(sys.argv[2:])
                await query_once(pool, q)
                return

            while True:
                q = input("Enter search query (or Enter to quit): ").strip()
                if not q:
                    break
                await query_once(pool, q)
        return

    await app.run()


if __name__ == "__main__":
    load_dotenv()
    asyncio.run(main())
