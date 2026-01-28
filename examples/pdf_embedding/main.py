"""
PDF Embedding (v1) - CocoIndex pipeline example.

- Walk local PDF files
- Convert PDFs to markdown
- Chunk text (RecursiveSplitter)
- Embed chunks (SentenceTransformers)
- Store into Postgres with pgvector column (no vector index)
- Query demo using pgvector cosine distance (<=>)
"""

from __future__ import annotations

import asyncio
import functools
import os
import pathlib
import sys
import tempfile
from dataclasses import dataclass
from typing import AsyncIterator, Annotated

from dotenv import load_dotenv
from marker.config.parser import ConfigParser
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import text_from_rendered
from numpy.typing import NDArray
import asyncpg

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import localfs, postgres
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import FileLike, PatternFilePathMatcher


TABLE_NAME = "pdf_embeddings"
PG_SCHEMA_NAME = "coco_examples"
TOP_K = 5


PG_DB = coco.ContextKey[postgres.PgDatabase]("pg_db")

_embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
_splitter = RecursiveSplitter()


@functools.cache
def pdf_converter() -> PdfConverter:
    config_parser = ConfigParser({})
    return PdfConverter(
        create_model_dict(), config=config_parser.generate_config_dict()
    )


def pdf_to_markdown(content: bytes) -> str:
    converter = pdf_converter()
    with tempfile.NamedTemporaryFile(delete=True, suffix=".pdf") as temp_file:
        temp_file.write(content)
        temp_file.flush()
        text_any, _, _ = text_from_rendered(converter(temp_file.name))
        return text_any


@dataclass
class PdfEmbedding:
    filename: str
    chunk_start: int
    chunk_end: int
    text: str
    embedding: Annotated[NDArray, _embedder]


@coco_aio.lifespan
async def coco_lifespan(
    builder: coco_aio.EnvironmentBuilder,
) -> AsyncIterator[None]:
    # Provide resources needed across the CocoIndex environment
    database_url = os.getenv("POSTGRES_URL")
    if not database_url:
        raise ValueError("POSTGRES_URL is not set")

    async with await postgres.create_pool(database_url) as pool:
        builder.provide(PG_DB, postgres.register_db("pdf_embedding_db", pool))
        yield


@coco.function(memo=True)
async def process_chunk(
    filename: pathlib.PurePath,
    chunk: Chunk,
    table: postgres.TableTarget[PdfEmbedding],
) -> None:
    table.declare_row(
        row=PdfEmbedding(
            filename=str(filename),
            chunk_start=chunk.start.char_offset,
            chunk_end=chunk.end.char_offset,
            text=chunk.text,
            embedding=await _embedder.embed_async(chunk.text),
        ),
    )


@coco.function(memo=True)
async def process_file(
    file: FileLike,
    table: postgres.TableTarget[PdfEmbedding],
) -> None:
    content = file.read()
    markdown = pdf_to_markdown(content)
    chunks = _splitter.split(
        markdown, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    await asyncio.gather(
        *(process_chunk(file.file_path.path, chunk, table) for chunk in chunks)
    )


@coco.function
def app_main(sourcedir: pathlib.Path) -> None:
    target_db = coco.use_context(PG_DB)
    target_table = coco.mount_run(
        coco.component_subpath("setup", "table"),
        target_db.declare_table_target,
        table_name=TABLE_NAME,
        table_schema=postgres.TableSchema(
            PdfEmbedding,
            primary_key=["filename", "chunk_start"],
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
            coco.component_subpath("file", str(f.file_path.path)),
            process_file,
            f,
            target_table,
        )


app = coco_aio.App(
    coco_aio.AppConfig(name="PdfEmbeddingV1"),
    app_main,
    sourcedir=pathlib.Path("./pdf_files"),
)


# ============================================================================
# Query demo (no vector index)
# ============================================================================


async def query_once(pool: asyncpg.Pool, query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await _embedder.embed_async(query)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                filename,
                text,
                embedding <=> $1 AS distance
            FROM "{PG_SCHEMA_NAME}"."{TABLE_NAME}"
            ORDER BY distance ASC
            LIMIT $2
            """,
            query_vec,
            top_k,
        )

    for r in rows:
        score = 1.0 - float(r["distance"])
        print(f"[{score:.3f}] {r['filename']}")
        print(f"    {r['text']}")
        print("---")


async def query() -> None:
    database_url = os.getenv("POSTGRES_URL")
    if not database_url:
        raise ValueError("POSTGRES_URL is not set")

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


load_dotenv()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "query":
        asyncio.run(query())
