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
from cocoindex.connectors import lancedb, localfs
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import AsyncFileLike, PatternFilePathMatcher
from cocoindex.resources.id import IdGenerator


LANCEDB_URI = "./lancedb_data"
TABLE_NAME = "doc_embeddings"
TOP_K = 5


LANCE_DB = coco.ContextKey[lancedb.LanceDatabase]("lance_db")

_embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
_splitter = RecursiveSplitter()


@dataclass
class DocEmbedding:
    id: int
    filename: str
    chunk_start: int
    chunk_end: int
    text: str
    embedding: Annotated[NDArray, _embedder]


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    # Provide resources needed across the CocoIndex environment
    conn = await lancedb.connect_async(LANCEDB_URI)
    builder.provide(LANCE_DB, lancedb.register_db("text_embedding_db", conn))
    yield


@coco.function
async def process_chunk(
    chunk: Chunk,
    filename: pathlib.PurePath,
    id_gen: IdGenerator,
    table: lancedb.TableTarget[DocEmbedding],
) -> None:
    table.declare_row(
        row=DocEmbedding(
            id=await id_gen.next_id(chunk.text),
            filename=str(filename),
            chunk_start=chunk.start.char_offset,
            chunk_end=chunk.end.char_offset,
            text=chunk.text,
            embedding=await _embedder.embed(chunk.text),
        ),
    )


@coco.function(memo=True)
async def process_file(
    file: AsyncFileLike,
    table: lancedb.TableTarget[DocEmbedding],
) -> None:
    text = await file.read_text()
    chunks = _splitter.split(
        text, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    id_gen = IdGenerator()
    await coco.map(process_chunk, chunks, file.file_path.path, id_gen, table)


@coco.function
async def app_main(sourcedir: pathlib.Path) -> None:
    target_db = coco.use_context(LANCE_DB)
    target_table = await target_db.mount_table_target(
        table_name=TABLE_NAME,
        table_schema=await lancedb.TableSchema.from_class(
            DocEmbedding, primary_key=["id"]
        ),
    )

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
    )
    await coco.mount_each(process_file, files.items(), target_table)


app = coco.App(
    coco.AppConfig(name="TextEmbeddingLanceDBV1"),
    app_main,
    sourcedir=pathlib.Path("./markdown_files"),
)


# ============================================================================
# Query demo
# ============================================================================


async def query_once(
    conn: lancedb.LanceAsyncConnection, query_text: str, *, top_k: int = TOP_K
) -> None:
    query_vec = await _embedder.embed(query_text)

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

    if len(sys.argv) > 1:
        q = " ".join(sys.argv[1:])
        await query_once(conn, q)
        return

    while True:
        q = input("Enter search query (or Enter to quit): ").strip()
        if not q:
            break
        await query_once(conn, q)


if __name__ == "__main__":
    asyncio.run(query())
