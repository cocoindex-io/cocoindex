"""
Code Embedding (v1) - CocoIndex pipeline example.

- Walk local code files (Python, Rust, TOML, Markdown)
- Detect programming language
- Chunk code (RecursiveSplitter with syntax awareness)
- Embed chunks (SentenceTransformers)
- Store into Postgres with pgvector column
- Query demo using pgvector cosine distance (<=>)
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import sys
from dataclasses import dataclass
from typing import AsyncIterator, Annotated

import asyncpg
from numpy.typing import NDArray

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import localfs, postgres
from cocoindex.ops.text import RecursiveSplitter, detect_code_language
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import AsyncFileLike, FileLike, PatternFilePathMatcher
from cocoindex.resources.id import IdGenerator


DATABASE_URL = os.getenv(
    "POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex"
)
TABLE_NAME = "code_embeddings"
PG_SCHEMA_NAME = "coco_examples"
TOP_K = 5


PG_DB = coco.ContextKey[postgres.PgDatabase]("pg_db")

_embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
_splitter = RecursiveSplitter()


@dataclass
class CodeEmbedding:
    id: int
    filename: str
    code: str
    embedding: Annotated[NDArray, _embedder]
    start_line: int
    end_line: int


@coco_aio.lifespan
async def coco_lifespan(
    builder: coco_aio.EnvironmentBuilder,
) -> AsyncIterator[None]:
    # Provide resources needed across the CocoIndex environment
    async with await postgres.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, postgres.register_db("code_embedding_db", pool))
        yield


@coco.function(memo=True)
async def process_file(
    file: AsyncFileLike,
    table: postgres.TableTarget[CodeEmbedding],
) -> None:
    text = await file.read_text()
    # Detect programming language from filename
    language = detect_code_language(filename=str(file.file_path.path.name))

    # Split with syntax awareness if language is detected
    chunks = _splitter.split(
        text,
        chunk_size=1000,
        min_chunk_size=300,
        chunk_overlap=300,
        language=language,
    )
    id_gen = IdGenerator()

    async def process_chunk(chunk: Chunk) -> None:
        embedding = await _embedder.embed(chunk.text)
        table.declare_row(
            row=CodeEmbedding(
                id=await id_gen.next_id(chunk.text),
                filename=str(file.file_path.path),
                code=chunk.text,
                embedding=embedding,
                start_line=chunk.start.line,
                end_line=chunk.end.line,
            ),
        )

    await coco_aio.map(process_chunk, chunks)


@coco.function
async def app_main(sourcedir: pathlib.Path) -> None:
    target_db = coco.use_context(PG_DB)

    with coco.component_subpath("setup"):
        target_table = await coco_aio.mount_target(
            target_db.table_target(
                table_name=TABLE_NAME,
                table_schema=await postgres.TableSchema.from_class(
                    CodeEmbedding,
                    primary_key=["id"],
                ),
                pg_schema_name=PG_SCHEMA_NAME,
            )
        )

    # Process multiple file types across the repository
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(
            included_patterns=[
                "**/*.py",
                "**/*.rs",
                "**/*.toml",
                "**/*.md",
                "**/*.mdx",
            ],
            excluded_patterns=["**/.*", "**/target", "**/node_modules"],
        ),
    )
    with coco.component_subpath("file"):
        # The first argument `files` is iterated.
        # The element is passed to `process_file` as the first argument.
        # And its "key" is also provided as the component subpath.
        coco_aio.mount_each(process_file, files, target_table)


app = coco_aio.App(
    coco_aio.AppConfig(name="CodeEmbeddingV1"),
    app_main,
    sourcedir=pathlib.Path(__file__).parent / ".." / "..",  # Index from repository root
)


# ============================================================================
# Query demo
# ============================================================================


async def query_once(pool: asyncpg.Pool, query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await _embedder.embed(query)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                filename,
                code,
                embedding <=> $1 AS distance,
                start_line,
                end_line
            FROM "{PG_SCHEMA_NAME}"."{TABLE_NAME}"
            ORDER BY distance ASC
            LIMIT $2
            """,
            query_vec,
            top_k,
        )

    for r in rows:
        score = 1.0 - float(r["distance"])
        print(f"[{score:.3f}] {r['filename']} (L{r['start_line']}-L{r['end_line']})")
        print(f"    {r['code']}")
        print("---")


async def query() -> None:
    async with await postgres.create_pool(DATABASE_URL) as pool:
        if len(sys.argv) > 2:
            q = " ".join(sys.argv[2:])
            await query_once(pool, q)
            return

        while True:
            q = input("Enter search query (or Enter to quit): ").strip()
            if not q:
                break
            await query_once(pool, q)


async def update_index() -> None:
    async with coco_aio.runtime():
        await app.update(report_to_stdout=True)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "query":
        asyncio.run(query())
    asyncio.run(update_index())
