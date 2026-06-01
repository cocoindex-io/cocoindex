"""
PDF Embedding (v1) - CocoIndex pipeline example.

Index (use `-L` for live mode, omit for one-shot catch-up):
    cocoindex update main
    cocoindex update -L main

Query the index:
    python main.py "your query"

Pipeline: walk local PDFs -> convert to markdown -> chunk -> embed -> store in Postgres pgvector.
"""

from __future__ import annotations

import asyncio
import functools
import io
import os
import pathlib
import sys
from dataclasses import dataclass
from typing import AsyncIterator, Annotated

from dotenv import load_dotenv
from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
from docling.datamodel.base_models import DocumentStream, InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from numpy.typing import NDArray
import asyncpg
from pgvector.asyncpg import register_vector

import cocoindex as coco
from cocoindex.connectors import localfs, postgres
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.id import IdGenerator


TABLE_NAME = "pdf_embeddings"
PG_SCHEMA_NAME = "coco_examples"
TOP_K = 5


EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
PG_DB = coco.ContextKey[asyncpg.Pool]("pdf_embedding_db")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)

_splitter = RecursiveSplitter()


@functools.cache
def pdf_converter() -> DocumentConverter:
    pipeline_options = PdfPipelineOptions(
        accelerator_options=AcceleratorOptions(device=AcceleratorDevice.CPU)
    )
    return DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
    )


@coco.fn.as_async(runner=coco.GPU)
def pdf_to_markdown(content: bytes) -> str:
    source = DocumentStream(name="input.pdf", stream=io.BytesIO(content))
    return pdf_converter().convert(source).document.export_to_markdown()


@dataclass
class PdfEmbedding:
    id: int
    filename: str
    chunk_start: int
    chunk_end: int
    text: str
    embedding: Annotated[NDArray, EMBEDDER]


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    database_url = os.getenv("POSTGRES_URL")
    if not database_url:
        raise ValueError("POSTGRES_URL is not set")

    async with asyncpg.create_pool(database_url) as pool:
        builder.provide(PG_DB, pool)
        builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
        yield


@coco.fn
async def process_chunk(
    chunk: Chunk,
    filename: pathlib.PurePath,
    id_gen: IdGenerator,
    table: postgres.TableTarget[PdfEmbedding],
) -> None:
    table.declare_row(
        row=PdfEmbedding(
            id=await id_gen.next_id(chunk.text),
            filename=str(filename),
            chunk_start=chunk.start.char_offset,
            chunk_end=chunk.end.char_offset,
            text=chunk.text,
            embedding=await coco.use_context(EMBEDDER).embed(chunk.text),
        ),
    )


@coco.fn(memo=True)
async def process_file(
    file: FileLike,
    table: postgres.TableTarget[PdfEmbedding],
) -> None:
    markdown = await pdf_to_markdown(await file.read())
    chunks = _splitter.split(
        markdown, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    id_gen = IdGenerator()
    await coco.map(process_chunk, chunks, file.file_path.path, id_gen, table)


@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    target_table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            PdfEmbedding,
            primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.pdf"]),
        live=True,  # source supports live watch; pass -L to `cocoindex update` to actually run live
    )
    await coco.mount_each(process_file, files.items(), target_table)


app = coco.App(
    coco.AppConfig(name="PdfEmbeddingV1"),
    app_main,
    sourcedir=pathlib.Path("./pdf_files"),
)


# ============================================================================
# Query demo (no vector index)
# ============================================================================


async def query_once(
    pool: asyncpg.Pool,
    embedder: SentenceTransformerEmbedder,
    query: str,
    *,
    top_k: int = TOP_K,
) -> None:
    query_vec = await embedder.embed(query)
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


async def query(initial_query: str | None = None) -> None:
    database_url = os.getenv("POSTGRES_URL")
    if not database_url:
        raise ValueError("POSTGRES_URL is not set")

    embedder = SentenceTransformerEmbedder(EMBED_MODEL)
    async with asyncpg.create_pool(database_url, init=register_vector) as pool:
        if initial_query is not None:
            await query_once(pool, embedder, initial_query)
            return

        while True:
            q = input("Enter search query (or Enter to quit): ").strip()
            if not q:
                break
            await query_once(pool, embedder, q)


if __name__ == "__main__":
    load_dotenv()
    initial = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else None
    asyncio.run(query(initial))
