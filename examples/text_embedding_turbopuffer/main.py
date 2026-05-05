"""
Text Embedding with Turbopuffer (v1) - CocoIndex pipeline example.

- Walk local markdown files
- Chunk text (RecursiveSplitter)
- Embed chunks (SentenceTransformers)
- Store into a Turbopuffer namespace
- Query demo using Turbopuffer ANN search
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import sys
from typing import AsyncIterator

from dotenv import load_dotenv

import cocoindex as coco
from cocoindex.connectors import localfs, turbopuffer
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.id import IdGenerator

TPUF_REGION = os.environ.get("TURBOPUFFER_REGION", "gcp-us-central1")
TPUF_NAMESPACE = "TextEmbedding"
TOP_K = 5

EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
TPUF_DB = coco.ContextKey[turbopuffer.AsyncTurbopuffer]("text_embedding_turbopuffer")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)

_splitter = RecursiveSplitter()


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    api_key = os.environ.get("TURBOPUFFER_API_KEY")
    if not api_key:
        raise RuntimeError("TURBOPUFFER_API_KEY is not set")
    client = turbopuffer.AsyncTurbopuffer(region=TPUF_REGION, api_key=api_key)
    builder.provide(TPUF_DB, client)
    builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
    yield


@coco.fn
async def process_chunk(
    chunk: Chunk,
    filename: pathlib.PurePath,
    id_gen: IdGenerator,
    target: turbopuffer.NamespaceTarget,
) -> None:
    embedding_vec = await coco.use_context(EMBEDDER).embed(chunk.text)

    target.declare_row(
        turbopuffer.Row(
            id=str(await id_gen.next_id(chunk.text)),
            vector=embedding_vec,
            attributes={
                "filename": str(filename),
                "chunk_start": chunk.start.char_offset,
                "chunk_end": chunk.end.char_offset,
                "text": chunk.text,
            },
        )
    )


@coco.fn(memo=True)
async def process_file(
    file: FileLike,
    target: turbopuffer.NamespaceTarget,
) -> None:
    text = await file.read_text()
    chunks = _splitter.split(
        text, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    id_gen = IdGenerator()
    await coco.map(process_chunk, chunks, file.file_path.path, id_gen, target)


@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    target_namespace = await turbopuffer.mount_namespace_target(
        TPUF_DB,
        namespace_name=TPUF_NAMESPACE,
        schema=await turbopuffer.NamespaceSchema.create(
            vectors=turbopuffer.VectorDef(schema=EMBEDDER),
        ),
    )
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
    )
    await coco.mount_each(process_file, files.items(), target_namespace)


app = coco.App(
    coco.AppConfig(name="TextEmbeddingTurbopufferV1"),
    app_main,
    sourcedir=pathlib.Path("./markdown_files"),
)


# ============================================================================
# Query demo
# ============================================================================


async def query_once(
    client: turbopuffer.AsyncTurbopuffer,
    embedder: SentenceTransformerEmbedder,
    query: str,
    *,
    top_k: int = TOP_K,
) -> None:
    query_vec = await embedder.embed(query)
    ns = client.namespace(TPUF_NAMESPACE)
    result = await ns.query(
        rank_by=("vector", "ANN", query_vec.tolist()),
        top_k=top_k,
        include_attributes=True,
    )

    for row in getattr(result, "rows", []):
        distance = getattr(row, "$dist", None)
        distance_str = f"{distance:.3f}" if isinstance(distance, (int, float)) else "?"
        print(f"[{distance_str}] {row.filename}")
        print(f"    {row.text}")
        print("---")


async def query() -> None:
    api_key = os.environ.get("TURBOPUFFER_API_KEY")
    if not api_key:
        print("TURBOPUFFER_API_KEY is not set", file=sys.stderr)
        sys.exit(1)
    embedder = SentenceTransformerEmbedder(EMBED_MODEL)
    async with turbopuffer.AsyncTurbopuffer(
        region=TPUF_REGION, api_key=api_key
    ) as client:
        if len(sys.argv) > 1:
            q = " ".join(sys.argv[1:])
            await query_once(client, embedder, q)
            return

        while True:
            q = input("Enter search query (or Enter to quit): ").strip()
            if not q:
                break
            await query_once(client, embedder, q)


if __name__ == "__main__":
    load_dotenv()
    asyncio.run(query())
