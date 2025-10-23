"""
HackerNews Custom Source Example

This example demonstrates how to use a custom source with CocoIndex to index
and search HackerNews threads.
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


@cocoindex.flow_def(name="HackerNewsIndex")
def hackernews_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define a flow that indexes HackerNews threads and their comments.
    """

    # Add the custom source to the flow
    data_scope["threads"] = flow_builder.add_source(
        HackerNewsSource(tag="story", max_results=500),
        refresh_interval=timedelta(minutes=1),
    )

    # Create collectors for different types of searchable content
    message_index = data_scope.add_collector()

    # Process each thread
    with data_scope["threads"].row() as thread:
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

        # Index individual comments
        with thread["comments"].row() as comment:
            message_index.collect(
                id=comment["id"],
                thread_id=thread["thread_id"],
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
def search_text(query: str) -> cocoindex.QueryOutput:
    """Search HackerNews threads by title and content."""
    table_name = cocoindex.utils.get_target_default_name(hackernews_flow, "hn_messages")

    with connection_pool().connection() as conn:
        with conn.cursor() as cur:
            # Simple text search using PostgreSQL's text search capabilities
            cur.execute(
                f"""
                SELECT id, thread_id, author, content_type, text, created_at,
                       ts_rank(to_tsvector('english', text), plainto_tsquery('english', %s)) as rank
                FROM {table_name}
                WHERE to_tsvector('english', text) @@ plainto_tsquery('english', %s)
                ORDER BY rank DESC, created_at DESC
                """,
                (query, query),
            )

            results = []
            for row in cur.fetchall():
                results.append(
                    {
                        "id": row[0],
                        "thread_id": row[1],
                        "author": row[2],
                        "content_type": row[3],
                        "text": row[4],
                        "created_at": row[5].isoformat(),
                    }
                )

            return cocoindex.QueryOutput(results=results)
