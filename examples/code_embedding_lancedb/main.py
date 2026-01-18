"""
Code Embedding with LanceDB (v1) - CocoIndex pipeline example.

- Walk local code files (Python, Rust, TOML, Markdown)
- Detect programming language
- Chunk code (RecursiveSplitter with syntax awareness)
- Embed chunks (SentenceTransformers)
- Store into LanceDB with vector column and FTS
- Query demo using LanceDB native vector search
"""

from __future__ import annotations

import asyncio
import pathlib
import sys
from dataclasses import dataclass
from typing import AsyncIterator, Annotated

from numpy.typing import NDArray

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import localfs, lancedb
from cocoindex.extras.text import RecursiveSplitter, detect_code_language
from cocoindex.extras.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.schema import FtsSpec


LANCEDB_URI = "./lancedb_data"
TABLE_NAME = "code_embeddings"
TOP_K = 5


LANCE_DB = coco.ContextKey[lancedb.LanceDatabase]("lance_db")

_embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
_splitter = RecursiveSplitter()


@dataclass
class CodeEmbedding:
    filename: str
    location: str
    code: str
    embedding: Annotated[NDArray, _embedder]
    start_line: int
    end_line: int


@coco_aio.lifespan
async def coco_lifespan(
    builder: coco_aio.EnvironmentBuilder,
) -> AsyncIterator[None]:
    # For CocoIndex internal states
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    # Provide resources needed across the CocoIndex environment
    await lancedb._register_db_async("code_embedding_db", LANCEDB_URI)
    builder.provide(LANCE_DB, lancedb.LanceDatabase("code_embedding_db"))
    yield


@coco.function(memo=True)
async def process_chunk(
    scope: coco.Scope,
    filename: pathlib.PurePath,
    chunk: Chunk,
    table: lancedb.TableTarget[CodeEmbedding],
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
    table: lancedb.TableTarget[CodeEmbedding],
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
    target_db = scope.use(LANCE_DB)
    target_table = coco.mount_run(
        target_db.declare_table_target,
        scope / "setup" / "table",
        table_name=TABLE_NAME,
        table_schema=lancedb.TableSchema(
            CodeEmbedding,
            primary_key=["filename", "location"],
            column_specs={
                "code": FtsSpec(tokenizer="simple"),
            },
        ),
    ).result()

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
        coco.mount(
            process_file, scope / "file" / str(file.relative_path), file, target_table
        )


app = coco_aio.App(
    app_main,
    coco_aio.AppConfig(name="CodeEmbeddingLanceDBV1"),
    sourcedir=pathlib.Path(__file__).parent / ".." / "..",  # Index from repository root
)


# ============================================================================
# Query demo
# ============================================================================


async def query_once(
    db: lancedb.LanceDatabase, query: str, *, top_k: int = TOP_K
) -> None:
    query_vec = await _embedder.embed_async(query)

    # Get connection from registry
    conn = lancedb._get_connection(db.key)
    table = await conn.open_table(TABLE_NAME)

    # LanceDB vector search
    search = await table.search(query_vec, vector_column_name="embedding")
    results = await search.limit(top_k).to_list()

    for r in results:
        # LanceDB returns "_distance" field
        # Convert distance to similarity score (1.0 = perfect match, 0.0 = far)
        score = 1.0 - r["_distance"]
        print(f"[{score:.3f}] {r['filename']} (L{r['start_line']}-L{r['end_line']})")
        print(f"    {r['code']}")
        print("---")


async def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "query":
        await lancedb._register_db_async("code_embedding_db", LANCEDB_URI)
        db = lancedb.LanceDatabase("code_embedding_db")

        if len(sys.argv) > 2:
            q = " ".join(sys.argv[2:])
            await query_once(db, q)
            return

        while True:
            q = input("Enter search query (or Enter to quit): ").strip()
            if not q:
                break
            await query_once(db, q)
        return

    await app.update(report_to_stdout=True)


if __name__ == "__main__":
    asyncio.run(main())
