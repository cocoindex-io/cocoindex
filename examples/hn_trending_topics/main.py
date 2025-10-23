"""
HackerNews Trending Topics Example

This example demonstrates how to use a custom source with CocoIndex to index
HackerNews threads and extract trending topics using LLM.
"""

import cocoindex
import os
import functools
from psycopg_pool import ConnectionPool
from datetime import timedelta, datetime
from typing import Any, AsyncIterator, NamedTuple
import aiohttp
import dataclasses

from cocoindex.op import (
    NON_EXISTENCE,
    SourceSpec,
    NO_ORDINAL,
    source_connector,
    PartialSourceRow,
    PartialSourceRowData,
)

THREAD_LEVEL_MENTION_SCORE = 5
COMMENT_LEVEL_MENTION_SCORE = 1


class _HackerNewsThreadKey(NamedTuple):
    """Row key type for HackerNews source."""

    thread_id: str


@dataclasses.dataclass
class _HackerNewsComment:
    id: str
    author: str | None
    text: str | None
    created_at: datetime | None


@dataclasses.dataclass
class _HackerNewsThread:
    """Value type for HackerNews source."""

    author: str | None
    text: str
    url: str | None
    created_at: datetime | None
    comments: list[_HackerNewsComment]


@dataclasses.dataclass
class Topic:
    """
    A single topic extracted from text.

    The topic can be a product name, technology, model, people, company name, business domain, etc.
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

    topic: str


# Define the source spec that users will instantiate
class HackerNewsSource(SourceSpec):
    """Source spec for HackerNews API."""

    tag: str | None = None
    max_results: int = 100


@source_connector(
    spec_cls=HackerNewsSource,
    key_type=_HackerNewsThreadKey,
    value_type=_HackerNewsThread,
)
class HackerNewsConnector:
    """Custom source connector for HackerNews API."""

    _spec: HackerNewsSource
    _session: aiohttp.ClientSession

    def __init__(self, spec: HackerNewsSource, session: aiohttp.ClientSession):
        self._spec = spec
        self._session = session

    @staticmethod
    async def create(spec: HackerNewsSource) -> "HackerNewsConnector":
        """Create a HackerNews connector from the spec."""
        return HackerNewsConnector(spec, aiohttp.ClientSession())

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Ensure we have an active HTTP session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def list(
        self,
    ) -> AsyncIterator[PartialSourceRow[_HackerNewsThreadKey, _HackerNewsThread]]:
        """List HackerNews threads using the search API."""
        session = await self._ensure_session()

        # Use HackerNews search API
        search_url = "https://hn.algolia.com/api/v1/search_by_date"
        params: dict[str, Any] = {"hitsPerPage": self._spec.max_results}
        if self._spec.tag:
            params["tags"] = self._spec.tag
        async with session.get(search_url, params=params) as response:
            response.raise_for_status()
            data = await response.json()
            for hit in data.get("hits", []):
                if thread_id := hit.get("objectID", None):
                    utime = hit.get("updated_at")
                    ordinal = (
                        int(datetime.fromisoformat(utime).timestamp())
                        if utime
                        else NO_ORDINAL
                    )
                    yield PartialSourceRow(
                        key=_HackerNewsThreadKey(thread_id=thread_id),
                        data=PartialSourceRowData(ordinal=ordinal),
                    )

    async def get_value(
        self, key: _HackerNewsThreadKey
    ) -> PartialSourceRowData[_HackerNewsThread]:
        """Get a specific HackerNews thread by ID using the items API."""
        session = await self._ensure_session()

        # Use HackerNews items API to get full thread with comments
        item_url = f"https://hn.algolia.com/api/v1/items/{key.thread_id}"

        async with session.get(item_url) as response:
            response.raise_for_status()
            data = await response.json()

            if not data:
                return PartialSourceRowData(
                    value=NON_EXISTENCE,
                    ordinal=NO_ORDINAL,
                    content_version_fp=None,
                )
            return PartialSourceRowData(
                value=HackerNewsConnector._parse_hackernews_thread(data)
            )

    def provides_ordinal(self) -> bool:
        """Indicate that this source provides ordinal information."""
        return True

    @staticmethod
    def _parse_hackernews_thread(data: dict[str, Any]) -> _HackerNewsThread:
        comments: list[_HackerNewsComment] = []

        def _add_comments(parent: dict[str, Any]) -> None:
            children = parent.get("children", None)
            if not children:
                return
            for child in children:
                ctime = child.get("created_at")
                if comment_id := child.get("id", None):
                    comments.append(
                        _HackerNewsComment(
                            id=str(comment_id),
                            author=child.get("author", ""),
                            text=child.get("text", ""),
                            created_at=datetime.fromisoformat(ctime) if ctime else None,
                        )
                    )
                _add_comments(child)

        _add_comments(data)

        ctime = data.get("created_at")
        text = data.get("title", "")
        if more_text := data.get("text", None):
            text += "\n\n" + more_text
        return _HackerNewsThread(
            author=data.get("author"),
            text=text,
            url=data.get("url"),
            created_at=datetime.fromisoformat(ctime) if ctime else None,
            comments=comments,
        )


@cocoindex.flow_def(name="HackerNewsTrendingTopics")
def hackernews_trending_topics_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define a flow that indexes HackerNews threads, comments, and extracts trending topics.
    """

    # Add the custom source to the flow
    data_scope["threads"] = flow_builder.add_source(
        HackerNewsSource(tag="story", max_results=200),
        refresh_interval=timedelta(seconds=30),
    )

    # Create collectors for different types of searchable content
    message_index = data_scope.add_collector()
    topic_index = data_scope.add_collector()

    # Process each thread
    with data_scope["threads"].row() as thread:
        # Extract topics from thread text using LLM
        thread["topics"] = thread["text"].transform(
            cocoindex.functions.ExtractByLlm(
                llm_spec=cocoindex.LlmSpec(
                    api_type=cocoindex.LlmApiType.OPENAI, model="gpt-5-mini"
                ),
                output_type=list[Topic],
            )
        )

        # Index the main thread content
        message_index.collect(
            id=thread["thread_id"],
            thread_id=thread["thread_id"],
            content_type="thread",
            author=thread["author"],
            text=thread["text"],
            url=thread["url"],
            created_at=thread["created_at"],
        )

        # Collect topics from thread
        with thread["topics"].row() as topic:
            topic_index.collect(
                message_id=thread["thread_id"],
                thread_id=thread["thread_id"],
                topic=topic["topic"],
                content_type="thread",
                created_at=thread["created_at"],
            )

        # Index individual comments
        with thread["comments"].row() as comment:
            # Extract topics from comment text using LLM
            comment["topics"] = comment["text"].transform(
                cocoindex.functions.ExtractByLlm(
                    llm_spec=cocoindex.LlmSpec(
                        api_type=cocoindex.LlmApiType.OPENAI, model="gpt-5-mini"
                    ),
                    output_type=list[Topic],
                )
            )

            message_index.collect(
                id=comment["id"],
                thread_id=thread["thread_id"],
                content_type="comment",
                author=comment["author"],
                text=comment["text"],
                url="",
                created_at=comment["created_at"],
            )

            # Collect topics from comment
            with comment["topics"].row() as topic:
                topic_index.collect(
                    message_id=comment["id"],
                    thread_id=thread["thread_id"],
                    topic=topic["topic"],
                    content_type="comment",
                    created_at=comment["created_at"],
                )

    # Export to database tables
    message_index.export(
        "hn_messages",
        cocoindex.targets.Postgres(),
        primary_key_fields=["id"],
    )

    # Export topics to separate table
    topic_index.export(
        "hn_topics",
        cocoindex.targets.Postgres(),
        primary_key_fields=["topic", "message_id"],
    )


@functools.cache
def connection_pool() -> ConnectionPool:
    """Get a connection pool to the database."""
    return ConnectionPool(os.environ["COCOINDEX_DATABASE_URL"])


@hackernews_trending_topics_flow.query_handler()
def search_by_topic(topic: str) -> cocoindex.QueryOutput:
    """Search HackerNews content by topic."""
    topic_table = cocoindex.utils.get_target_default_name(
        hackernews_trending_topics_flow, "hn_topics"
    )
    message_table = cocoindex.utils.get_target_default_name(
        hackernews_trending_topics_flow, "hn_messages"
    )

    with connection_pool().connection() as conn:
        with conn.cursor() as cur:
            # Search for matching topics and join with messages
            cur.execute(
                f"""
                SELECT m.id, m.thread_id, m.author, m.content_type, m.text, m.created_at, t.topic
                FROM {topic_table} t
                JOIN {message_table} m ON t.message_id = m.id
                WHERE LOWER(t.topic) LIKE LOWER(%s)
                ORDER BY m.created_at DESC
                """,
                (f"%{topic}%",),
            )

            results = []
            for row in cur.fetchall():
                results.append(
                    {
                        "id": row[0],
                        "url": f"https://news.ycombinator.com/item?id={row[1]}",
                        "author": row[2],
                        "type": row[3],
                        "text": row[4],
                        "created_at": row[5].isoformat(),
                        "topic": row[6],
                    }
                )

            return cocoindex.QueryOutput(results=results)


def get_threads_for_topic(topic: str) -> list[dict[str, Any]]:
    """Get the threads for a given topic."""
    topic_table = cocoindex.utils.get_target_default_name(
        hackernews_trending_topics_flow, "hn_topics"
    )
    with connection_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    thread_id,
                    SUM(CASE WHEN content_type = 'thread' THEN {THREAD_LEVEL_MENTION_SCORE} ELSE {COMMENT_LEVEL_MENTION_SCORE} END) AS score,
                    MAX(created_at) AS latest_mention
                FROM {topic_table} WHERE topic = %s
                GROUP BY thread_id ORDER BY score DESC, latest_mention DESC""",
                (topic,),
            )
            return [
                {
                    "url": f"https://news.ycombinator.com/item?id={row[0]}",
                    "score": row[1],
                    "latest_time": row[2].isoformat(),
                }
                for row in cur.fetchall()
            ]


@hackernews_trending_topics_flow.query_handler()
def get_trending_topics(_query: str = "", limit: int = 20) -> cocoindex.QueryOutput:
    """Get the most trending topics across all HackerNews content."""
    topic_table = cocoindex.utils.get_target_default_name(
        hackernews_trending_topics_flow, "hn_topics"
    )

    with connection_pool().connection() as conn:
        with conn.cursor() as cur:
            # Aggregate topics by frequency
            cur.execute(
                f"""
                SELECT
                    topic,
                    SUM(CASE WHEN content_type = 'thread' THEN {THREAD_LEVEL_MENTION_SCORE} ELSE {COMMENT_LEVEL_MENTION_SCORE} END) AS score,
                    MAX(created_at) AS latest_mention
                FROM {topic_table}
                GROUP BY topic
                ORDER BY score DESC, latest_mention DESC
                LIMIT %s
                """,
                (limit,),
            )

            results = []
            for row in cur.fetchall():
                results.append(
                    {
                        "topic": row[0],
                        "score": row[1],
                        "latest_time": row[2].isoformat(),
                        "threads": get_threads_for_topic(row[0]),
                    }
                )

            return cocoindex.QueryOutput(results=results)
