"""
Entire Session Search - CocoIndex pipeline example.

Indexes AI coding session data captured by Entire (https://entire.io)
into Postgres with pgvector for semantic search.

Handles four file types from Entire's checkpoint format:
- full.jsonl:  conversation transcript (chunked + embedded)
- prompt.txt:  user's initial prompt (embedded directly)
- context.md:  AI-generated session summary (chunked + embedded)
- metadata.json: token counts, files touched, timestamps (stored as metadata)
"""

from __future__ import annotations

import asyncio
import json
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
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.id import IdGenerator

from models import ChunkInput, SessionInfo, TranscriptChunk


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATABASE_URL = os.getenv(
    "POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex"
)
TABLE_EMBEDDINGS = os.getenv("TABLE_EMBEDDINGS", "session_embeddings")
TABLE_METADATA = os.getenv("TABLE_METADATA", "session_metadata")
PG_SCHEMA_NAME = os.getenv("PG_SCHEMA_NAME", "entire")
TOP_K = 5

PG_DB = coco.ContextKey[postgres.PgDatabase]("pg_db")

_embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
_splitter = RecursiveSplitter()


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@coco_aio.lifespan
async def coco_lifespan(
    builder: coco_aio.EnvironmentBuilder,
) -> AsyncIterator[None]:
    async with await postgres.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, postgres.register_db("entire_session_db", pool))
        yield


# ---------------------------------------------------------------------------
# Row types
# ---------------------------------------------------------------------------


@dataclass
class SessionEmbeddingRow:
    id: int
    checkpoint_id: str
    session_index: str
    content_type: str  # "transcript", "prompt", or "context"
    role: str  # "user", "assistant", or "" for non-transcript
    text: str
    embedding: Annotated[NDArray, _embedder]


@dataclass
class SessionMetadataRow:
    checkpoint_id: str
    session_index: str
    prompt_summary: str
    total_tokens: int
    files_touched: str  # JSON array
    agent_percentage: float | None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def extract_session_info(file: FileLike) -> SessionInfo:
    """Extract checkpoint_id and session_index from file path.

    Entire layout: <shard>/<checkpoint_id>/<session_idx>/<filename>
    """
    parts = file.file_path.path.parts
    # parts[-1] is the filename, [-2] is session_index, [-3] is checkpoint_id
    return SessionInfo(
        checkpoint_id=parts[-3],
        session_index=parts[-2],
    )


def parse_transcript(content: str) -> list[TranscriptChunk]:
    """Parse full.jsonl into transcript chunks, skipping trivial entries."""
    chunks: list[TranscriptChunk] = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue

        role = entry.get("role", "unknown")
        text = ""

        # Handle different content formats
        if isinstance(entry.get("content"), str):
            text = entry["content"]
        elif isinstance(entry.get("content"), list):
            # content can be a list of parts (text, tool_use, etc.)
            text_parts = []
            for part in entry["content"]:
                if isinstance(part, str):
                    text_parts.append(part)
                elif isinstance(part, dict) and part.get("type") == "text":
                    text_parts.append(part.get("text", ""))
            text = "\n".join(text_parts)
        elif "text" in entry:
            text = entry["text"]

        text = text.strip()
        if len(text) < 20:
            continue
        chunks.append(TranscriptChunk(role=role, text=text))
    return chunks


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------


@coco.function
async def process_chunk(
    chunk: ChunkInput,
    info: SessionInfo,
    id_gen: IdGenerator,
    emb_table: postgres.TableTarget[SessionEmbeddingRow],
) -> None:
    emb_table.declare_row(
        row=SessionEmbeddingRow(
            id=await id_gen.next_id(chunk.text),
            checkpoint_id=info.checkpoint_id,
            session_index=info.session_index,
            content_type=chunk.content_type,
            role=chunk.role,
            text=chunk.text,
            embedding=await _embedder.embed(chunk.text),
        ),
    )


@coco.function(memo=True)
async def process_file(
    file: FileLike,
    emb_table: postgres.TableTarget[SessionEmbeddingRow],
    meta_table: postgres.TableTarget[SessionMetadataRow],
) -> None:
    info = extract_session_info(file)
    filename = file.file_path.path.name
    id_gen = IdGenerator()

    if filename == "full.jsonl":
        content = file.read_text()
        chunks = parse_transcript(content)
        await coco_aio.map(
            process_chunk,
            [
                ChunkInput(text=c.text, content_type="transcript", role=c.role)
                for c in chunks
            ],
            info,
            id_gen,
            emb_table,
        )

    elif filename == "prompt.txt":
        text = file.read_text().strip()
        if text:
            emb_table.declare_row(
                row=SessionEmbeddingRow(
                    id=await id_gen.next_id(text),
                    checkpoint_id=info.checkpoint_id,
                    session_index=info.session_index,
                    content_type="prompt",
                    role="user",
                    text=text,
                    embedding=await _embedder.embed(text),
                ),
            )

    elif filename == "context.md":
        text = file.read_text().strip()
        if text:
            chunks = _splitter.split(
                text, chunk_size=2000, chunk_overlap=500, language="markdown"
            )
            await coco_aio.map(
                process_chunk,
                [
                    ChunkInput(text=c.text, content_type="context", role="")
                    for c in chunks
                ],
                info,
                id_gen,
                emb_table,
            )

    elif filename == "metadata.json":
        content = file.read_text()
        try:
            meta = json.loads(content)
        except json.JSONDecodeError:
            return

        # Extract a prompt summary from the metadata if available
        prompt_summary = meta.get("prompt_summary", meta.get("title", ""))

        # Token counts
        total_tokens = meta.get("total_tokens", 0)
        if not total_tokens:
            input_t = meta.get("input_tokens", 0) or 0
            output_t = meta.get("output_tokens", 0) or 0
            total_tokens = input_t + output_t

        # Files touched
        files_touched = meta.get("files_touched", [])
        if isinstance(files_touched, list):
            files_touched_str = json.dumps(files_touched)
        else:
            files_touched_str = str(files_touched)

        # Agent percentage
        agent_pct = meta.get("agent_percentage")
        if agent_pct is not None:
            agent_pct = float(agent_pct)

        meta_table.declare_row(
            row=SessionMetadataRow(
                checkpoint_id=info.checkpoint_id,
                session_index=info.session_index,
                prompt_summary=prompt_summary,
                total_tokens=total_tokens,
                files_touched=files_touched_str,
                agent_percentage=agent_pct,
            ),
        )


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------


@coco.function
async def app_main(checkpoints_dir: pathlib.Path) -> None:
    target_db = coco.use_context(PG_DB)

    emb_table = await target_db.mount_table_target(
        table_name=TABLE_EMBEDDINGS,
        table_schema=await postgres.TableSchema.from_class(
            SessionEmbeddingRow,
            primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )

    meta_table = await target_db.mount_table_target(
        table_name=TABLE_METADATA,
        table_schema=await postgres.TableSchema.from_class(
            SessionMetadataRow,
            primary_key=["checkpoint_id", "session_index"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )

    files = localfs.walk_dir(
        checkpoints_dir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(
            included_patterns=[
                "**/full.jsonl",
                "**/prompt.txt",
                "**/context.md",
                "**/metadata.json",
            ],
            excluded_patterns=["**/content_hash.txt"],
        ),
    )
    await coco_aio.mount_each(process_file, files.items(), emb_table, meta_table)


app = coco_aio.App(
    coco_aio.AppConfig(name="EntireSessionSearch"),
    app_main,
    checkpoints_dir=pathlib.Path("./entire_checkpoints"),
)


# ---------------------------------------------------------------------------
# Query demo
# ---------------------------------------------------------------------------


async def query_once(pool: asyncpg.Pool, query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await _embedder.embed(query)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                checkpoint_id,
                session_index,
                content_type,
                role,
                text,
                embedding <=> $1 AS distance
            FROM "{PG_SCHEMA_NAME}"."{TABLE_EMBEDDINGS}"
            ORDER BY distance ASC
            LIMIT $2
            """,
            query_vec,
            top_k,
        )

    for r in rows:
        score = 1.0 - float(r["distance"])
        tag = r["content_type"]
        if r["role"]:
            tag += f"/{r['role']}"
        print(f"[{score:.3f}] {r['checkpoint_id']}/{r['session_index']} ({tag})")
        print(f"    {r['text'][:200]}")
        print("---")


async def query() -> None:
    async with await postgres.create_pool(DATABASE_URL) as pool:
        if len(sys.argv) > 1:
            q = " ".join(sys.argv[1:])
            await query_once(pool, q)
            return

        while True:
            q = input("Enter search query (or Enter to quit): ").strip()
            if not q:
                break
            await query_once(pool, q)


if __name__ == "__main__":
    asyncio.run(query())
