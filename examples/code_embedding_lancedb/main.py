"""
Code Embedding with LanceDB (v1) - CocoIndex pipeline example.

- Walk local code files (Python, Rust, TOML, Markdown)
- Detect programming language
- Chunk code (RecursiveSplitter with syntax awareness)
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
from cocoindex.connectors import localfs, lancedb
from cocoindex.ops.text import RecursiveSplitter, detect_code_language
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.id import IdGenerator


LANCEDB_URI = "./lancedb_data"
TABLE_NAME = "code_embeddings"
TOP_K = 5


LANCE_DB = coco.ContextKey[lancedb.LanceDatabase]("lance_db")

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


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    # Provide resources needed across the CocoIndex environment
    conn = await lancedb.connect_async(LANCEDB_URI)
    builder.provide(LANCE_DB, lancedb.register_db("code_embedding_db", conn))
    yield


@coco.fn
async def process_chunk(
    chunk: Chunk,
    filename: pathlib.PurePath,
    id_gen: IdGenerator,
    table: lancedb.TableTarget[CodeEmbedding],
) -> None:
    table.declare_row(
        row=CodeEmbedding(
            id=await id_gen.next_id(chunk.text),
            filename=str(filename),
            code=chunk.text,
            embedding=await _embedder.embed(chunk.text),
            start_line=chunk.start.line,
            end_line=chunk.end.line,
        ),
    )


@coco.fn(memo=True)
async def process_file(
    file: FileLike,
    table: lancedb.TableTarget[CodeEmbedding],
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
    await coco.map(process_chunk, chunks, file.file_path.path, id_gen, table)


@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    target_db = coco.use_context(LANCE_DB)
    target_table = await target_db.mount_table_target(
        table_name=TABLE_NAME,
        table_schema=await lancedb.TableSchema.from_class(
            CodeEmbedding, primary_key=["id"]
        ),
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
    await coco.mount_each(process_file, files.items(), target_table)


app = coco.App(
    coco.AppConfig(name="CodeEmbeddingLanceDBV1"),
    app_main,
    sourcedir=pathlib.Path(__file__).parent / ".." / "..",  # Index from repository root
)


# ============================================================================
# Query demo
# ============================================================================


async def query_once(
    conn: lancedb.LanceAsyncConnection, query_text: str, *, top_k: int = TOP_K
) -> None:
    query_vec = await _embedder.embed(query_text)

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
