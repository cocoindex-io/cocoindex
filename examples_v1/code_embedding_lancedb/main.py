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
import functools
import pathlib
import sys
import threading
from dataclasses import dataclass
from typing import AsyncIterator

import numpy as np
from numpy.typing import NDArray
from sentence_transformers import SentenceTransformer

import cocoindex as coco
import cocoindex.aio as coco_aio
from cocoindex.connectors import localfs, lancedb
from cocoindex.extras.text import RecursiveSplitter, detect_code_language
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.schema import VectorSpec, FtsSpec


LANCEDB_URI = "./lancedb_data"
TABLE_NAME = "code_embeddings"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
TOP_K = 5


@dataclass
class _GlobalState:
    db: lancedb.LanceDatabase | None = None


_state = _GlobalState()
_splitter = RecursiveSplitter()


@functools.cache
def embedder() -> SentenceTransformer:
    # The model is cached in-process so repeated runs are fast.
    return SentenceTransformer(EMBED_MODEL)


_gpu_lock = threading.Lock()


def embed_text(text: str) -> NDArray[np.float32]:
    # TODO: convert to a cocoindex function with GPU and batching support
    with _gpu_lock:
        return embedder().encode(
            [text], convert_to_numpy=True, normalize_embeddings=True
        )[0]


# ============================================================================
# Table schema
# ============================================================================


@dataclass
class CodeEmbedding:
    filename: str
    location: str
    code: str
    embedding: NDArray[np.float32]
    start_line: int
    end_line: int


@coco.function
def setup_table(
    scope: coco.Scope,
) -> lancedb.TableTarget[CodeEmbedding, coco.PendingS]:
    assert _state.db is not None

    dim = embedder().get_sentence_embedding_dimension()
    if dim is None:
        raise RuntimeError(f"Embedding dimension is unknown for model {EMBED_MODEL}.")
    return _state.db.declare_table_target(
        scope,
        table_name=TABLE_NAME,
        table_schema=lancedb.TableSchema(
            CodeEmbedding,
            primary_key=["filename", "location"],
            column_specs={
                "embedding": VectorSpec(dim),
                "code": FtsSpec(tokenizer="simple"),
            },
        ),
    )


# ============================================================================
# CocoIndex environment + app
# ============================================================================


@coco_aio.lifespan
async def coco_lifespan(
    builder: coco_aio.EnvironmentBuilder,
) -> AsyncIterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")

    # Register LanceDB connection
    await lancedb._register_db_async("code_embedding_db", LANCEDB_URI)
    _state.db = lancedb.LanceDatabase("code_embedding_db")
    yield


@coco.function
def process_chunk(
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
            embedding=embed_text(chunk.text),
            start_line=chunk.start.line,
            end_line=chunk.end.line,
        ),
    )


@coco.function(memo=True)
def process_file(
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
    # TODO: Process chunks in parallel
    for chunk in chunks:
        process_chunk(scope, file.relative_path, chunk, table)


@coco.function
async def app_main(scope: coco.Scope, sourcedir: pathlib.Path) -> None:
    table = await coco_aio.mount_run(setup_table, scope / "setup").result()

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
        coco_aio.mount(
            process_file, scope / "file" / str(file.relative_path), file, table
        )
    print(f"Waiting all background processing to finish")


app = coco_aio.App(
    app_main,
    coco_aio.AppConfig(name="CodeEmbeddingLanceDBV1"),
    sourcedir=pathlib.Path(__file__).parent / ".." / "..",  # Index from repository root
)


# ============================================================================
# Query demo
# ============================================================================


async def query_once(query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await asyncio.to_thread(embed_text, query)

    assert _state.db is not None
    # Get connection from registry
    conn = lancedb._get_connection(_state.db.key)
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
