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
import pathlib
import sys
from dataclasses import dataclass
from typing import AsyncIterator, Annotated

import asyncpg
from numpy.typing import NDArray

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import localfs, postgres
from cocoindex.extras.text import RecursiveSplitter, detect_code_language
from cocoindex.extras.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.chunk import Chunk


DATABASE_URL = "postgres://cocoindex:cocoindex@localhost/cocoindex"
TABLE_NAME = "code_embeddings"
PG_SCHEMA_NAME = "coco_examples"
TOP_K = 5


@dataclass
class _GlobalState:
    pool: asyncpg.Pool | None = None
    db: postgres.PgDatabase | None = None


_state = _GlobalState()
_embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
_splitter = RecursiveSplitter()


# ============================================================================
# Table schema
# ============================================================================
@dataclass
class CodeEmbedding:
    filename: str
    location: str
    code: str
    embedding: Annotated[NDArray, _embedder]
    start_line: int
    end_line: int


@coco.function
def setup_table(
    scope: coco.Scope,
) -> postgres.TableTarget[CodeEmbedding, coco.PendingS]:
    assert _state.db is not None
    return _state.db.declare_table_target(
        scope,
        table_name=TABLE_NAME,
        table_schema=postgres.TableSchema(
            CodeEmbedding,
            primary_key=["filename", "location"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )


# ============================================================================
# CocoIndex environment + app
# ============================================================================


@coco_aio.lifespan
async def coco_lifespan(
    builder: coco_aio.EnvironmentBuilder,
) -> AsyncIterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")

    # register_vector is needed for query
    async with await postgres.create_pool(DATABASE_URL) as pool:
        _state.pool = pool
        _state.db = postgres.register_db("code_embedding_db", pool)
        yield


@coco.function(memo=True)
async def process_chunk(
    scope: coco.Scope,
    filename: pathlib.PurePath,
    chunk: Chunk,
    table: postgres.TableTarget[CodeEmbedding],
) -> None:
    # Create location string from chunk position (used as part of primary key)
    location = f"{chunk.start.char_offset}-{chunk.end.char_offset}"
    table.declare_row(
        scope,
        row=CodeEmbedding(
            filename=str(filename),
            location=location,
            code=chunk.text,
            embedding=await _embedder.embed_async(chunk.text),
            start_line=chunk.start.line,
            end_line=chunk.end.line,
        ),
    )


@coco.function(memo=True)
async def process_file(
    scope: coco.Scope,
    file: FileLike,
    table: postgres.TableTarget[CodeEmbedding],
) -> None:
    text = file.read_text()
    # Detect programming language from filename
    language = detect_code_language(filename=str(file.relative_path.name))

    # Split with syntax awareness if language is detected
    chunks = _splitter.split(
        text,
        chunk_size=1000,
        min_chunk_size=300,
        chunk_overlap=300,
        language=language,
    )
    await asyncio.gather(
        *(process_chunk(scope, file.relative_path, chunk, table) for chunk in chunks)
    )


@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path) -> None:
    table = coco.mount_run(setup_table, scope / "setup").result()

    # Process multiple file types across the repository
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(
            included_patterns=["*.py", "*.rs", "*.toml", "*.md", "*.mdx"],
            excluded_patterns=[".*/**", "target/**", "node_modules/**"],
        ),
    )
    for file in files:
        print(f"Processing in background: {str(file.relative_path)}")
        coco.mount(process_file, scope / "file" / str(file.relative_path), file, table)


app = coco_aio.App(
    app_main,
    coco_aio.AppConfig(name="CodeEmbeddingV1"),
    sourcedir=pathlib.Path(__file__).parent / ".." / "..",  # Index from repository root
)


# ============================================================================
# Query demo
# ============================================================================


async def query_once(query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await _embedder.embed_async(query)
    pool = _state.pool
    assert pool is not None

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


async def main() -> None:
    async with coco_aio.runtime():
        if len(sys.argv) > 1 and sys.argv[1] == "query":
            if len(sys.argv) > 2:
                q = " ".join(sys.argv[2:])
                await query_once(q)
                return

            while True:
                q = input("Enter search query (or Enter to quit): ").strip()
                if not q:
                    break
                await query_once(q)
            return

        await app.run()


if __name__ == "__main__":
    asyncio.run(main())
