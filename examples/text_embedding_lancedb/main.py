"""
Text Embedding with LanceDB (v1) - CocoIndex pipeline example.

- Walk local markdown files
- Chunk text (RecursiveSplitter)
- Embed chunks (SentenceTransformers)
- Store into LanceDB with vector column
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
from cocoindex.connectors import lancedb, localfs
from cocoindex.extras.text import RecursiveSplitter
from cocoindex.extras.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import FileLike, PatternFilePathMatcher


LANCEDB_URI = "./lancedb_data"
TABLE_NAME = "doc_embeddings"
TOP_K = 5


LANCE_DB = coco.ContextKey[lancedb.LanceDatabase]("lance_db")

_embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
_splitter = RecursiveSplitter()


@dataclass
class DocEmbedding:
    filename: str
    chunk_start: int
    chunk_end: int
    text: str
    embedding: Annotated[NDArray, _embedder]


@coco_aio.lifespan
async def coco_lifespan(
    builder: coco_aio.EnvironmentBuilder,
) -> AsyncIterator[None]:
    # For CocoIndex internal states
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    # Provide resources needed across the CocoIndex environment
    conn = await lancedb.connect_async(LANCEDB_URI)
    builder.provide(LANCE_DB, lancedb.register_db("text_embedding_db", conn))
    yield


@coco.function(memo=True)
async def process_chunk(
    scope: coco.Scope,
    filename: pathlib.PurePath,
    chunk: Chunk,
    table: lancedb.TableTarget[DocEmbedding],
) -> None:
    table.declare_row(
        scope,
        row=DocEmbedding(
            filename=str(filename),
            chunk_start=chunk.start.char_offset,
            chunk_end=chunk.end.char_offset,
            text=chunk.text,
            embedding=await _embedder.embed_async(chunk.text),
        ),
    )


@coco.function(memo=True)
async def process_file(
    scope: coco.Scope,
    file: FileLike,
    table: lancedb.TableTarget[DocEmbedding],
) -> None:
    text = file.read_text()
    chunks = _splitter.split(
        text, chunk_size=2000, chunk_overlap=500, language="markdown"
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
            DocEmbedding, primary_key=["filename", "chunk_start"]
        ),
    ).result()

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["*.md"]),
    )
    for f in files:
        coco.mount(process_file, scope / "file" / str(f.relative_path), f, target_table)


app = coco_aio.App(
    app_main,
    coco_aio.AppConfig(name="TextEmbeddingLanceDBV1"),
    sourcedir=pathlib.Path("./markdown_files"),
)


# ============================================================================
# Query demo
# ============================================================================


async def query_once(
    conn: lancedb.LanceAsyncConnection, query_text: str, *, top_k: int = TOP_K
) -> None:
    query_vec = await _embedder.embed_async(query_text)

    table = await conn.open_table(TABLE_NAME)

    search = await table.search(query_vec, vector_column_name="embedding")
    results = await search.limit(top_k).to_list()

    for r in results:
        score = 1.0 - r["_distance"]
        print(f"[{score:.3f}] {r['filename']}")
        print(f"    {r['text']}")
        print("---")


async def query() -> None:
    conn = await lancedb.connect_async(LANCEDB_URI)

    if len(sys.argv) > 2:
        q = " ".join(sys.argv[2:])
        await query_once(conn, q)
        return

    while True:
        q = input("Enter search query (or Enter to quit): ").strip()
        if not q:
            break
        await query_once(conn, q)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "query":
        asyncio.run(query())
