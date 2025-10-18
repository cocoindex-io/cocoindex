"""
HackerNews Custom Source Example

This example demonstrates how to use a custom source with CocoIndex to index
and search HackerNews threads. It shows:

1. How to define a custom source connector
2. How to register and use the custom source in a flow
3. How to process complex nested data (threads with comments)
4. How to build searchable indexes from external API data

All implementation is contained in this single file for simplicity.
"""

from cocoindex.typing import KEY_FIELD_NAME
import cocoindex
import os
import functools
from psycopg_pool import ConnectionPool
from datetime import timedelta
from typing import Any, Self, AsyncIterator
import aiohttp
import dataclasses

from cocoindex.op import (
    NON_EXISTENCE,
    SourceSpec,
    SourceReadOptions,
    NO_ORDINAL,
    source_connector,
    PartialSourceRow,
    PartialSourceRowData,
)


@dataclasses.dataclass
class HackerNewsComment:
    """HackerNews comment data structure."""

    id: str
    author: str
    text: str
    created_at: int


@dataclasses.dataclass
class HackerNewsThread:
    """HackerNews thread data structure."""

    title: str
    author: str
    url: str
    type: str
    text: str
    created_at: int
    comments: list[HackerNewsComment]


def _create_thread_from_api_data(data: dict[str, Any]) -> HackerNewsThread:
    """Create a HackerNewsThread from API response data."""
    title = data.get("title", "")
    author = data.get("author", "")
    url = data.get("url", "")
    thread_type = data.get("_tags", [""])[0] if data.get("_tags") else ""
    text = data.get("text", "") or data.get("story_text", "")
    created_at = data.get("created_at_i", 0)

    # Flatten comments from the API response
    comments = []
    if "children" in data:

        def _add_comments(children_list: list[dict[str, Any]]) -> None:
            for child in children_list:
                if child.get("text"):
                    comment = HackerNewsComment(
                        id=str(child.get("id", "")),
                        author=child.get("author", ""),
                        text=child.get("text", ""),
                        created_at=child.get("created_at_i", 0),
                    )
                    comments.append(comment)

                # Recursively process nested comments
                if "children" in child:
                    _add_comments(child["children"])

        _add_comments(data["children"])

    return HackerNewsThread(
        title=title,
        author=author,
        url=url,
        type=thread_type,
        text=text,
        created_at=created_at,
        comments=comments,
    )


# Define the source spec that users will instantiate
class HackerNewsSourceSpec(SourceSpec):
    """Source spec for HackerNews API."""

    query: str = "python"  # Search query for HackerNews stories
    max_results: int = 100  # Maximum number of results to fetch


@source_connector(
    spec_cls=HackerNewsSourceSpec, key_type=str, value_type=HackerNewsThread
)
class HackerNewsConnector:
    """Custom source connector for HackerNews API."""

    def __init__(self, query: str, max_results: int, session: aiohttp.ClientSession):
        """
        Initialize HackerNews connector.

        Args:
            query: Search query for HackerNews stories
            max_results: Maximum number of results to fetch
        """
        self._query = query
        self._max_results = max_results
        self._session = session

    @staticmethod
    async def create(spec: HackerNewsSourceSpec) -> Self:
        """Create a HackerNews connector from the spec."""
        session = aiohttp.ClientSession()
        return HackerNewsConnector(spec.query, spec.max_results, session)

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Ensure we have an active HTTP session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def list(
        self, options: SourceReadOptions
    ) -> AsyncIterator[PartialSourceRow[str, HackerNewsThread]]:
        """
        List HackerNews threads using the search API.

        Returns:
            AsyncIterator yielding PartialSourceRow objects with key and data
        """
        session = await self._ensure_session()

        # Use HackerNews search API
        search_url = "https://hn.algolia.com/api/v1/search"
        params = {
            "query": self._query,
            "tags": "story",
            "hitsPerPage": min(self._max_results, 1000),  # API limit
            "attributesToRetrieve": "objectID,title,author,url,created_at_i,updated_at_i,story_text,_tags",
        }

        async with session.get(search_url, params=params) as response:
            response.raise_for_status()
            data = await response.json()

            for hit in data.get("hits", []):
                thread_id = hit.get("objectID")
                if not thread_id:
                    continue

                # Create thread object from API data
                thread_data = _create_thread_from_api_data(hit)

                # Convert to the expected format
                key = str(thread_id)  # Key as string

                # Get updated_at for ordinal
                updated_at = hit.get("updated_at_i", 0) or hit.get("created_at_i", 0)

                # Create PartialSourceRow
                yield PartialSourceRow(
                    key=key,
                    data=PartialSourceRowData(
                        ordinal=updated_at if updated_at > 0 else NO_ORDINAL,
                    ),
                )

    async def get_value(
        self, key: str, options: SourceReadOptions
    ) -> PartialSourceRowData[HackerNewsThread]:
        """
        Get a specific HackerNews thread by ID using the items API.

        Args:
            key: thread ID as string
            options: Read options (not used in this implementation)

        Returns:
            PartialSourceRowData[HackerNewsThread] with value, ordinal, and content_version_fp
        """
        thread_id = key
        session = await self._ensure_session()

        # Use HackerNews items API to get full thread with comments
        item_url = f"https://hn.algolia.com/api/v1/items/{thread_id}"

        async with session.get(item_url) as response:
            response.raise_for_status()
            data = await response.json()

            if not data:
                return PartialSourceRowData(
                    value=NON_EXISTENCE,
                    ordinal=NO_ORDINAL,
                    content_version_fp=None,
                )

            # Create thread object with full data including comments
            thread_data = _create_thread_from_api_data(data)

            # Get updated_at for ordinal
            updated_at = data.get("updated_at_i", 0) or data.get("created_at_i", 0)

            return PartialSourceRowData(
                value=thread_data,
                ordinal=updated_at if updated_at > 0 else NO_ORDINAL,
                content_version_fp=None,
            )

    def provides_ordinal(self) -> bool:
        """Indicate that this source provides ordinal information."""
        return True


@cocoindex.flow_def(name="HackerNewsIndex")
def hackernews_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define a flow that indexes HackerNews threads and their comments.
    """

    # Add the custom source to the flow
    data_scope["threads"] = flow_builder.add_source(
        HackerNewsSourceSpec(
            query="python AI machine learning",  # Search for Python and AI related stories
            max_results=50,
        ),
        refresh_interval=timedelta(minutes=30),  # Refresh every 30 minutes
    )

    # Create collectors for different types of searchable content
    message_index = data_scope.add_collector()

    # Process each thread
    with data_scope["threads"].row() as thread:
        # Index the main thread content
        message_index.collect(
            id=thread[KEY_FIELD_NAME],
            content_type="thread",
            author=thread["author"],
            text=thread["title"],
            url=thread["url"],
            created_at=thread["created_at"],
        )

        # Index individual comments
        with thread["comments"].row() as comment:
            message_index.collect(
                id=comment["id"],
                content_type="comment",
                author=comment["author"],
                text=comment["text"],
                url="",
                created_at=comment["created_at"],
            )

    # Export to database tables
    message_index.export(
        "hn_messages",
        cocoindex.targets.Postgres(),
        primary_key_fields=["id"],
    )


@functools.cache
def connection_pool() -> ConnectionPool:
    """Get a connection pool to the database."""
    return ConnectionPool(os.environ["COCOINDEX_DATABASE_URL"])


@hackernews_flow.query_handler()
def search_threads(query: str, limit: int = 10) -> cocoindex.QueryOutput:
    """Search HackerNews threads by title and content."""
    table_name = cocoindex.utils.get_target_default_name(hackernews_flow, "hn_threads")

    with connection_pool().connection() as conn:
        with conn.cursor() as cur:
            # Simple text search using PostgreSQL's text search capabilities
            cur.execute(
                f"""
                SELECT thread_id, title, author, url, text, created_at,
                       ts_rank(to_tsvector('english', search_text), plainto_tsquery('english', %s)) as rank
                FROM {table_name}
                WHERE to_tsvector('english', search_text) @@ plainto_tsquery('english', %s)
                ORDER BY rank DESC, created_at DESC
                LIMIT %s
                """,
                (query, query, limit),
            )

            results = []
            for row in cur.fetchall():
                results.append(
                    {
                        "thread_id": row[0],
                        "title": row[1],
                        "author": row[2],
                        "url": row[3],
                        "text": row[4][:200] + "..."
                        if len(row[4]) > 200
                        else row[4],  # Truncate long text
                        "created_at": row[5],
                        "rank": float(row[6]),
                    }
                )

            return cocoindex.QueryOutput(results=results)


@hackernews_flow.query_handler()
def search_comments(query: str, limit: int = 10) -> cocoindex.QueryOutput:
    """Search HackerNews comments by content."""
    table_name = cocoindex.utils.get_target_default_name(hackernews_flow, "hn_comments")

    with connection_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT thread_id, comment_id, author, text, thread_title, created_at,
                       ts_rank(to_tsvector('english', search_text), plainto_tsquery('english', %s)) as rank
                FROM {table_name}
                WHERE to_tsvector('english', search_text) @@ plainto_tsquery('english', %s)
                ORDER BY rank DESC, created_at DESC
                LIMIT %s
                """,
                (query, query, limit),
            )

            results = []
            for row in cur.fetchall():
                results.append(
                    {
                        "thread_id": row[0],
                        "comment_id": row[1],
                        "author": row[2],
                        "text": row[3][:200] + "..."
                        if len(row[3]) > 200
                        else row[3],  # Truncate long text
                        "thread_title": row[4],
                        "created_at": row[5],
                        "rank": float(row[6]),
                    }
                )

            return cocoindex.QueryOutput(results=results)
