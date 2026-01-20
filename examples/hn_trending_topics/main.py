"""
HackerNews Trending Topics - CocoIndex Pipeline Example

This example demonstrates a CocoIndex pipeline that:
1. Scrapes HackerNews threads and comments via API
2. Extracts topics using LLM (Gemini 2.5 Flash via LiteLLM)
3. Stores everything in PostgreSQL using the cocoindex postgres target
"""

import asyncio
import os
import sys
import pathlib
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator

import aiohttp
import asyncpg
from litellm import acompletion
from pydantic import BaseModel, Field

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import postgres

# Configuration
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex"
)
MAX_THREADS = 10
LLM_MODEL = "gemini/gemini-2.5-flash"

# Scoring weights for trending topics query
THREAD_LEVEL_MENTION_SCORE = 5
COMMENT_LEVEL_MENTION_SCORE = 1

PG_DB = coco.ContextKey[postgres.PgDatabase]("pg_db")


# ============================================================================
# Data models for HackerNews content
# ============================================================================


@dataclass
class Comment:
    id: str
    author: str | None
    text: str | None
    created_at: datetime | None


@dataclass
class Thread:
    id: str
    author: str | None
    text: str
    url: str | None
    created_at: datetime | None
    comments: list[Comment]


# ============================================================================
# Table schemas as dataclasses (for PostgreSQL target)
# ============================================================================


@dataclass
class HnMessage:
    """Schema for hn_messages table."""

    id: str
    thread_id: str
    content_type: str
    author: str | None
    text: str | None
    url: str | None
    created_at: datetime | None


@dataclass
class HnTopic:
    """Schema for hn_topics table."""

    topic: str
    message_id: str
    thread_id: str
    content_type: str
    created_at: datetime | None


# ============================================================================
# LLM topic extraction
# ============================================================================


class TopicsResponse(BaseModel):
    """Response containing a list of extracted topics."""

    topics: list[str] = Field(
        description="""List of extracted topics.

Each topic can be a product name, technology, model, people, company name, business domain, etc.
Capitalize for proper nouns and acronyms only.
Use the form that is clear alone.
Avoid acronyms unless very popular and unambiguous for common people even without context.

Examples:
- "Anthropic" (not "ANTHR")
- "Claude" (specific product name)
- "React" (well-known library)
- "PostgreSQL" (canonical database name)

For topics that are a phrase combining multiple things, normalize into multiple topics if needed. Examples:
- "books for autistic kids" -> "book", "autistic", "autistic kids"
- "local Large Language Model" -> "local Large Language Model", "Large Language Model"

For people, use preferred name and last name. Examples:
- "Bill Clinton" instead of "William Jefferson Clinton"

When there're multiple common ways to refer to the same thing, use multiple topics. Examples:
- "John Kennedy", "JFK"
"""
    )


@coco.function
async def extract_topics(scope: coco.Scope, text: str | None) -> list[str]:
    """Extract topics from text using LLM."""
    if not text or not text.strip():
        return []

    response = await acompletion(
        model=LLM_MODEL,
        messages=[
            {
                "role": "user",
                "content": f"Extract topics from the following text:\n\n{text[:4000]}",
            }
        ],
        response_format=TopicsResponse,
    )

    content = response.choices[0].message.content
    return TopicsResponse.model_validate_json(content).topics


# ============================================================================
# HackerNews API functions
# ============================================================================


async def fetch_thread_list(
    session: aiohttp.ClientSession, max_results: int = MAX_THREADS
) -> list[str]:
    """Fetch list of recent thread IDs from HackerNews."""
    search_url = "https://hn.algolia.com/api/v1/search_by_date"
    params: dict[str, str | int] = {"tags": "story", "hitsPerPage": max_results}

    async with session.get(search_url, params=params) as response:
        response.raise_for_status()
        data = await response.json()
        thread_ids = [
            hit["objectID"] for hit in data.get("hits", []) if hit.get("objectID")
        ]
        return thread_ids


async def fetch_thread(session: aiohttp.ClientSession, thread_id: str) -> Thread:
    """Fetch a single thread with all its comments."""
    item_url = f"https://hn.algolia.com/api/v1/items/{thread_id}"

    async with session.get(item_url) as response:
        response.raise_for_status()
        data = await response.json()

        comments: list[Comment] = []

        # Parse comments recursively
        def parse_comments(parent: dict[str, Any]) -> None:
            for child in parent.get("children", []):
                if comment_id := child.get("id"):
                    ctime = child.get("created_at")
                    comments.append(
                        Comment(
                            id=str(comment_id),
                            author=child.get("author"),
                            text=child.get("text"),
                            created_at=datetime.fromisoformat(ctime) if ctime else None,
                        )
                    )
                parse_comments(child)

        parse_comments(data)

        ctime = data.get("created_at")
        text = data.get("title", "")
        if more_text := data.get("text"):
            text += "\n\n" + more_text

        return Thread(
            id=thread_id,
            author=data.get("author"),
            text=text,
            url=data.get("url"),
            created_at=datetime.fromisoformat(ctime) if ctime else None,
            comments=comments,
        )


# ============================================================================
# CocoIndex setup
# ============================================================================


@coco_aio.lifespan
async def coco_lifespan(builder: coco_aio.EnvironmentBuilder) -> AsyncIterator[None]:
    """Set up CocoIndex environment with PostgreSQL database."""
    # For CocoIndex internal states
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    # Provide resources needed across the CocoIndex environment
    async with await asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, postgres.register_db("hn_db", pool))
        yield


@dataclass
class TableTargets:
    """Container for table targets."""

    messages: postgres.TableTarget[HnMessage]
    topics: postgres.TableTarget[HnTopic]


@coco.function
async def process_thread(
    scope: coco.Scope,
    thread_id: str,
    targets: TableTargets,
) -> None:
    """Fetch and process a single thread and its comments."""
    async with aiohttp.ClientSession() as session:
        thread = await fetch_thread(session, thread_id)
    thread_topics = await extract_topics(scope, thread.text)

    # Declare thread message row
    targets.messages.declare_row(
        scope,
        row=HnMessage(
            id=thread.id,
            thread_id=thread.id,
            content_type="thread",
            author=thread.author,
            text=thread.text,
            url=thread.url,
            created_at=thread.created_at,
        ),
    )
    # Declare thread topic rows
    for topic in thread_topics:
        targets.topics.declare_row(
            scope,
            row=HnTopic(
                topic=topic,
                message_id=thread.id,
                thread_id=thread.id,
                content_type="thread",
                created_at=thread.created_at,
            ),
        )
    # Process comments
    for comment in thread.comments:
        comment_topics = await extract_topics(scope, comment.text)

        # Declare comment message row
        targets.messages.declare_row(
            scope,
            row=HnMessage(
                id=comment.id,
                thread_id=thread.id,
                content_type="comment",
                author=comment.author,
                text=comment.text,
                url="",
                created_at=comment.created_at,
            ),
        )
        # Declare comment topic rows
        for topic in comment_topics:
            targets.topics.declare_row(
                scope,
                row=HnTopic(
                    topic=topic,
                    message_id=comment.id,
                    thread_id=thread.id,
                    content_type="comment",
                    created_at=comment.created_at,
                ),
            )


@coco.function
async def app_main(scope: coco.Scope) -> None:
    """Main pipeline function."""
    print("Starting HackerNews Trending Topics Pipeline")
    print(f"Using LLM: {LLM_MODEL}")
    print(f"Database: {DATABASE_URL}")
    print()

    # Set up table targets
    target_db = scope.use(PG_DB)
    messages_table = await coco_aio.mount_run(
        target_db.declare_table_target,
        scope / "setup" / "messages",
        table_name="hn_messages",
        table_schema=postgres.TableSchema(HnMessage, primary_key=["id"]),
        pg_schema_name="coco_examples",
    ).result()
    topics_table = await coco_aio.mount_run(
        target_db.declare_table_target,
        scope / "setup" / "topics",
        table_name="hn_topics",
        table_schema=postgres.TableSchema(HnTopic, primary_key=["topic", "message_id"]),
        pg_schema_name="coco_examples",
    ).result()
    targets = TableTargets(messages=messages_table, topics=topics_table)

    # Fetch thread IDs from HackerNews
    async with aiohttp.ClientSession() as session:
        thread_ids = await fetch_thread_list(session)

    # Process threads (each component fetches its own thread data)
    for thread_id in thread_ids:
        coco_aio.mount(process_thread, scope / "thread" / thread_id, thread_id, targets)


# ============================================================================
# App definition
# ============================================================================

app = coco_aio.App(
    app_main,
    coco_aio.AppConfig(name="HNTrendingTopics"),
)


# ============================================================================
# Query utilities (for demo purposes)
# ============================================================================


async def get_trending_topics(
    pool: asyncpg.Pool, limit: int = 20
) -> list[dict[str, Any]]:
    """Get trending topics ranked by score."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                topic,
                SUM(CASE WHEN content_type = 'thread' THEN {THREAD_LEVEL_MENTION_SCORE} ELSE {COMMENT_LEVEL_MENTION_SCORE} END) AS score,
                MAX(created_at) AS latest_mention,
                COUNT(DISTINCT thread_id) AS thread_count
            FROM hn_topics
            GROUP BY topic
            ORDER BY score DESC, latest_mention DESC
            LIMIT $1
        """,
            limit,
        )

        return [
            {
                "topic": row["topic"],
                "score": row["score"],
                "latest_mention": row["latest_mention"].isoformat()
                if row["latest_mention"]
                else None,
                "thread_count": row["thread_count"],
            }
            for row in rows
        ]


async def search_by_topic(pool: asyncpg.Pool, topic: str) -> list[dict[str, Any]]:
    """Search messages by topic."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT m.id, m.thread_id, m.author, m.content_type, m.text, m.created_at, t.topic
            FROM hn_topics t
            JOIN hn_messages m ON t.message_id = m.id
            WHERE LOWER(t.topic) LIKE LOWER($1)
            ORDER BY m.created_at DESC
        """,
            f"%{topic}%",
        )

        return [
            {
                "id": row["id"],
                "url": f"https://news.ycombinator.com/item?id={row['thread_id']}",
                "author": row["author"],
                "type": row["content_type"],
                "text": row["text"][:500] if row["text"] else None,
                "created_at": row["created_at"].isoformat()
                if row["created_at"]
                else None,
                "topic": row["topic"],
            }
            for row in rows
        ]


async def query_demo() -> None:
    """Demo querying the database."""
    pool = await asyncpg.create_pool(DATABASE_URL)
    if not pool:
        raise RuntimeError("Failed to create database pool")

    try:
        print("Top 20 Trending Topics:")
        print("-" * 60)
        topics = await get_trending_topics(pool, limit=20)
        for i, topic in enumerate(topics, 1):
            print(
                f"{i:2}. {topic['topic']:<30} (score: {topic['score']}, threads: {topic['thread_count']})"
            )

        print()
        print("Searching for 'AI' related content:")
        print("-" * 60)
        results = await search_by_topic(pool, "AI")
        for result in results[:5]:
            print(
                f"[{result['type']}] by {result['author']}: {result['text'][:100]}..."
            )

    finally:
        await pool.close()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "query":
        asyncio.run(query_demo())
