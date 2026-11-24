"""
Discord Text Embedding (v1) - CocoIndex pipeline example.

Index (one-shot catch-up; the discord source has no live mode):
    cocoindex update main

Query the index:
    python main.py "your query"

Pipeline: read forum posts and messages from Discord -> chunk -> embed -> store in pgvector.
"""

from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from typing import Annotated, AsyncIterator

import asyncpg
from dotenv import load_dotenv
from numpy.typing import NDArray
from pgvector.asyncpg import register_vector

import cocoindex as coco
from cocoindex.connectors import discord, postgres
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.id import IdGenerator


load_dotenv()

DATABASE_URL = os.getenv(
    "POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex"
)
TABLE_NAME = "discord_embeddings"
PG_SCHEMA_NAME = "coco_examples_v1"
TOP_K = 5

EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
PG_DB = coco.ContextKey[asyncpg.Pool]("discord_text_embedding_db")
DISCORD = coco.ContextKey[discord.DiscordApp]("discord")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)

# Only the fields the text and the links read, so a reaction, a pin or someone
# joining a thread re-embeds nothing. A thread keeps the fields that change when
# it gets a reply, a deletion, an archive or a lock, since those are the only
# signs that the messages inside it changed.
MESSAGE_FIELDS = [
    "author",
    "channel_id",
    "content",
    "embeds",
    "guild_id",
    "mentions",
    "type",
]
THREAD_FIELDS = [
    "guild_id",
    "name",
    "tags",
    "message_count",
    "total_message_sent",
    "last_message_id",
    "archived",
    "archive_timestamp",
    "locked",
]

# Default, reply, slash command and context-menu command; the rest are system
# notices such as joins and pins.
CONVERSATION_TYPES = (0, 19, 20, 23)

_splitter = RecursiveSplitter()


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    async with (
        asyncpg.create_pool(DATABASE_URL) as pool,
        discord.DiscordApp(token=os.environ["DISCORD_BOT_TOKEN"]) as app,
    ):
        builder.provide(PG_DB, pool)
        builder.provide(DISCORD, app)
        builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
        yield


@dataclass
class ChunkEmbedding:
    id: int
    url: str
    title: str
    text: str
    embedding: Annotated[NDArray, EMBEDDER]


@coco.fn(memo=True)
async def process_item(
    item: discord.DiscordMessage | discord.DiscordThread,
    table: postgres.TableTarget[ChunkEmbedding],
) -> None:
    if isinstance(item, discord.DiscordThread):
        url = f"https://discord.com/channels/{item.guild_id}/{item.id}"
        title = item.name or ""
    elif item.type in CONVERSATION_TYPES:
        url = (
            f"https://discord.com/channels/{item.guild_id}/{item.channel_id}/{item.id}"
        )
        title = ""
    else:
        return
    text = await item.read_text()
    chunks = _splitter.split(
        text, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    id_gen = IdGenerator()
    await coco.map(_emit_chunk, chunks, url, title, id_gen, table)


@coco.fn
async def _emit_chunk(
    chunk: Chunk,
    url: str,
    title: str,
    id_gen: IdGenerator,
    table: postgres.TableTarget[ChunkEmbedding],
) -> None:
    table.declare_row(
        row=ChunkEmbedding(
            id=await id_gen.next_id(chunk.text),
            url=url,
            title=title,
            text=chunk.text,
            embedding=await coco.use_context(EMBEDDER).embed(chunk.text),
        ),
    )


@coco.fn
async def sync_channel(
    channel: discord.DiscordChannel,
    table: postgres.TableTarget[ChunkEmbedding],
) -> None:
    await coco.mount_each(process_item, channel.items(), table)


@coco.fn
async def app_main() -> None:
    table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            ChunkEmbedding,
            primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )

    app = coco.use_context(DISCORD)
    channel_ids = [
        cid.strip()
        for cid in os.environ["DISCORD_CHANNEL_IDS"].split(",")
        if cid.strip()
    ]
    channels = [
        (
            cid,
            discord.DiscordChannel(
                app=app,
                channel_id=cid,
                message_fields=MESSAGE_FIELDS,
                thread_fields=THREAD_FIELDS,
            ),
        )
        for cid in channel_ids
    ]
    await coco.mount_each(sync_channel, channels, table)


app = coco.App(
    coco.AppConfig(name="DiscordTextEmbeddingV1"),
    app_main,
)


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
                url,
                title,
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
        print(f"[{score:.3f}] {r['title'] or r['url']}")
        if r["title"]:
            print(f"    {r['url']}")
        print(f"    {r['text']}")
        print("---")


async def query(initial_query: str | None = None) -> None:
    embedder = SentenceTransformerEmbedder(EMBED_MODEL)
    async with asyncpg.create_pool(DATABASE_URL, init=register_vector) as pool:
        if initial_query is not None:
            await query_once(pool, embedder, initial_query)
            return

        while True:
            q = input("Enter search query (or Enter to quit): ").strip()
            if not q:
                break
            await query_once(pool, embedder, q)


if __name__ == "__main__":
    initial = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else None
    asyncio.run(query(initial))
