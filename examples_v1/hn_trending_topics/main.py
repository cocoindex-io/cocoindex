"""
HackerNews Trending Topics - Batch Pipeline Example

This example demonstrates a batch pipeline that:
1. Scrapes HackerNews threads and comments via API
2. Extracts topics using LLM (Gemini 2.5 Flash via LiteLLM)
3. Stores everything in PostgreSQL using asyncpg
"""

import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import aiohttp
import asyncpg
from litellm import acompletion
from pydantic import BaseModel, Field


# Configuration
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost:5432/hn_topics")
MAX_THREADS = 200
LLM_MODEL = "gemini/gemini-2.5-flash-preview-05-20"

# Scoring weights
THREAD_LEVEL_MENTION_SCORE = 5
COMMENT_LEVEL_MENTION_SCORE = 1


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


async def create_database(pool: asyncpg.Pool) -> None:
    """Create the database tables."""
    async with pool.acquire() as conn:
        await conn.execute("""
            DROP TABLE IF EXISTS hn_topics CASCADE;
            DROP TABLE IF EXISTS hn_messages CASCADE;
        """)

        await conn.execute("""
            CREATE TABLE hn_messages (
                id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                content_type TEXT NOT NULL,
                author TEXT,
                text TEXT,
                url TEXT,
                created_at TIMESTAMPTZ
            );

            CREATE INDEX idx_messages_thread_id ON hn_messages(thread_id);
            CREATE INDEX idx_messages_created_at ON hn_messages(created_at);
        """)

        await conn.execute("""
            CREATE TABLE hn_topics (
                topic TEXT NOT NULL,
                message_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                content_type TEXT NOT NULL,
                created_at TIMESTAMPTZ,
                PRIMARY KEY (topic, message_id)
            );

            CREATE INDEX idx_topics_topic ON hn_topics(topic);
            CREATE INDEX idx_topics_thread_id ON hn_topics(thread_id);
        """)

    print("Database tables created successfully")


async def fetch_thread_list(
    session: aiohttp.ClientSession, max_results: int = MAX_THREADS
) -> list[str]:
    """Fetch list of recent thread IDs from HackerNews."""
    search_url = "https://hn.algolia.com/api/v1/search_by_date"
    params = {"tags": "story", "hitsPerPage": max_results}

    async with session.get(search_url, params=params) as response:
        response.raise_for_status()
        data = await response.json()
        thread_ids = [
            hit["objectID"] for hit in data.get("hits", []) if hit.get("objectID")
        ]
        print(f"Found {len(thread_ids)} threads")
        return thread_ids


async def fetch_thread(session: aiohttp.ClientSession, thread_id: str) -> Thread | None:
    """Fetch a single thread with all its comments."""
    item_url = f"https://hn.algolia.com/api/v1/items/{thread_id}"

    try:
        async with session.get(item_url) as response:
            response.raise_for_status()
            data = await response.json()

            if not data:
                return None

            # Parse comments recursively
            comments: list[Comment] = []

            def parse_comments(parent: dict[str, Any]) -> None:
                for child in parent.get("children", []):
                    if comment_id := child.get("id"):
                        ctime = child.get("created_at")
                        comments.append(
                            Comment(
                                id=str(comment_id),
                                author=child.get("author"),
                                text=child.get("text"),
                                created_at=datetime.fromisoformat(ctime)
                                if ctime
                                else None,
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
    except Exception as e:
        print(f"Error fetching thread {thread_id}: {e}")
        return None


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

For topics that are a phrase combining multiple things, normalize into multiple topics if needed.
Examples:
- "books for autistic kids" -> "book", "autistic", "autistic kids"
- "local Large Language Model" -> "local Large Language Model", "Large Language Model"

For people, use preferred name and last name.
Examples:
- "Bill Clinton" instead of "William Jefferson Clinton"

When there're multiple common ways to refer to the same thing, use multiple topics.
Examples:
- "John Kennedy", "JFK"
"""
    )


def build_response_schema(model: type[BaseModel]) -> dict[str, Any]:
    """Build a JSON schema response format from a Pydantic model."""
    schema = model.model_json_schema()
    # Remove $defs if present and inline them (for strict mode compatibility)
    if "$defs" in schema:
        del schema["$defs"]
    return {
        "type": "json_schema",
        "json_schema": {
            "name": model.__name__,
            "strict": True,
            "schema": schema,
        },
    }


TOPIC_EXTRACTION_PROMPT = """Extract topics from the following text.

Text to analyze:
{text}"""


async def extract_topics(text: str | None) -> list[str]:
    """Extract topics from text using LLM."""
    if not text or not text.strip():
        return []

    try:
        response = await acompletion(
            model=LLM_MODEL,
            messages=[
                {
                    "role": "user",
                    "content": TOPIC_EXTRACTION_PROMPT.format(text=text[:4000]),
                }
            ],
            response_format=build_response_schema(TopicsResponse),
        )

        content = response.choices[0].message.content
        if content:
            result = TopicsResponse.model_validate_json(content)
            return result.topics
        return []
    except Exception as e:
        print(f"Error extracting topics: {e}")
        return []


async def process_thread(
    thread: Thread,
    pool: asyncpg.Pool,
    semaphore: asyncio.Semaphore,
) -> None:
    """Process a single thread and its comments."""
    async with semaphore:
        # Extract topics from thread
        thread_topics = await extract_topics(thread.text)

        async with pool.acquire() as conn:
            # Insert thread message
            await conn.execute(
                """
                INSERT INTO hn_messages (id, thread_id, content_type, author, text, url, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (id) DO UPDATE SET
                    text = EXCLUDED.text,
                    author = EXCLUDED.author,
                    url = EXCLUDED.url,
                    created_at = EXCLUDED.created_at
            """,
                thread.id,
                thread.id,
                "thread",
                thread.author,
                thread.text,
                thread.url,
                thread.created_at,
            )

            # Insert thread topics
            for topic in thread_topics:
                await conn.execute(
                    """
                    INSERT INTO hn_topics (topic, message_id, thread_id, content_type, created_at)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (topic, message_id) DO NOTHING
                """,
                    topic,
                    thread.id,
                    thread.id,
                    "thread",
                    thread.created_at,
                )

        # Process comments
        for comment in thread.comments:
            comment_topics = await extract_topics(comment.text)

            async with pool.acquire() as conn:
                # Insert comment message
                await conn.execute(
                    """
                    INSERT INTO hn_messages (id, thread_id, content_type, author, text, url, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (id) DO UPDATE SET
                        text = EXCLUDED.text,
                        author = EXCLUDED.author,
                        created_at = EXCLUDED.created_at
                """,
                    comment.id,
                    thread.id,
                    "comment",
                    comment.author,
                    comment.text,
                    "",
                    comment.created_at,
                )

                # Insert comment topics
                for topic in comment_topics:
                    await conn.execute(
                        """
                        INSERT INTO hn_topics (topic, message_id, thread_id, content_type, created_at)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (topic, message_id) DO NOTHING
                    """,
                        topic,
                        comment.id,
                        thread.id,
                        "comment",
                        comment.created_at,
                    )

        print(
            f"Processed thread {thread.id}: {len(thread_topics)} topics, {len(thread.comments)} comments"
        )


async def run_pipeline() -> None:
    """Run the full batch pipeline."""
    print("Starting HackerNews Trending Topics Pipeline")
    print(f"Using LLM: {LLM_MODEL}")
    print(f"Database: {DATABASE_URL}")
    print()

    # Create database connection pool
    pool = await asyncpg.create_pool(DATABASE_URL)
    if not pool:
        raise RuntimeError("Failed to create database pool")

    try:
        # Create tables
        await create_database(pool)

        # Fetch threads
        async with aiohttp.ClientSession() as session:
            thread_ids = await fetch_thread_list(session)

            # Fetch full thread data
            print(f"Fetching {len(thread_ids)} threads...")
            threads: list[Thread] = []
            for thread_id in thread_ids:
                thread = await fetch_thread(session, thread_id)
                if thread:
                    threads.append(thread)

            print(f"Successfully fetched {len(threads)} threads")

        # Process threads with rate limiting
        print(f"Processing threads with LLM topic extraction...")
        semaphore = asyncio.Semaphore(5)  # Limit concurrent LLM calls

        tasks = [process_thread(thread, pool, semaphore) for thread in threads]
        await asyncio.gather(*tasks)

        # Print summary
        async with pool.acquire() as conn:
            message_count = await conn.fetchval("SELECT COUNT(*) FROM hn_messages")
            topic_count = await conn.fetchval("SELECT COUNT(*) FROM hn_topics")
            unique_topics = await conn.fetchval(
                "SELECT COUNT(DISTINCT topic) FROM hn_topics"
            )

        print()
        print("Pipeline completed!")
        print(f"  Messages: {message_count}")
        print(f"  Topic mentions: {topic_count}")
        print(f"  Unique topics: {unique_topics}")

    finally:
        await pool.close()


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
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "query":
        asyncio.run(query_demo())
    else:
        asyncio.run(run_pipeline())
