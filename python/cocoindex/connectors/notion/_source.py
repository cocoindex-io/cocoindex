"""Read Notion data-source pages using the versioned Markdown API."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
import math
from typing import Any
from uuid import UUID

from cocoindex._internal import deadline
from cocoindex._internal.extras import install_hint

try:
    import httpx
except ImportError as e:
    raise ImportError("httpx is required for Notion. " + install_hint("notion")) from e


@dataclass(frozen=True)
class NotionPage:
    """A page's stable identity, title, URL and current Notion-flavored Markdown.

    Pass the page to a memoized transform: all four fields participate in its
    memo key, so content changes are detected without relying on timestamps.
    """

    page_id: str
    title: str
    url: str
    markdown: str


class NotionSource:
    """Read active pages from a Notion data source (not a database ID).

    ``client`` is caller-owned and must supply an Authorization header. The
    source neither closes nor serializes it. Requests always go to api.notion.com,
    pin Notion-Version to 2026-03-11 and do not follow redirects.
    """

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def _request_json(
        self, method: str, path: str, body: dict[str, object] | None = None
    ) -> dict[str, Any]:
        retry_after: float | None = None

        async def request() -> dict[str, Any]:
            nonlocal retry_after
            retry_after = None
            response = await self._client.request(
                method,
                f"https://api.notion.com/v1/{path}",
                headers={"Notion-Version": "2026-03-11"},
                params={"markdown_version": "v2"} if method == "GET" else None,
                json=body,
                follow_redirects=False,
                timeout=30.0,
            )
            if response.status_code == 429:
                try:
                    value = float(response.headers.get("Retry-After", ""))
                except ValueError:
                    pass
                else:
                    if math.isfinite(value) and value >= 0:
                        retry_after = value
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise ValueError("Expected a Notion JSON object")
            return data

        def is_transient(error: Exception) -> bool:
            return isinstance(error, httpx.TransportError) or (
                isinstance(error, httpx.HTTPStatusError)
                and error.response.status_code in (429, 500, 502, 503, 504, 529)
            )

        return await deadline.retry_transient(
            request,
            retry_on=is_transient,
            max_attempts=5,
            timeout=timedelta(seconds=120),
            bound_attempt=True,
            backoff=lambda attempt: (
                retry_after if retry_after is not None else min(2.0**attempt, 30.0)
            ),
        )

    async def items(self, data_source_id: str) -> AsyncIterator[tuple[str, NotionPage]]:
        """Scan pages as ``(page_id, page)`` pairs for ``coco.mount_each``.

        Every scan re-reads Markdown. Truncation, unknown blocks, incomplete
        query results, invalid pagination and HTTP errors fail the scan instead
        of presenting partial content as a successful snapshot. Complete the
        scan before mounting children if an incomplete scan must retain all
        previously mounted pages. This is polling, not a live subscription.
        """
        source_id = str(UUID(data_source_id))
        cursor: str | None = None
        cursors: set[str] = set()
        page_ids: set[str] = set()
        while True:
            body: dict[str, object] = {"page_size": 100}
            if cursor is not None:
                body["start_cursor"] = cursor
            data = await self._request_json(
                "POST", f"data_sources/{source_id}/query", body
            )
            if data.get("request_status", {}).get("type") == "incomplete":
                raise ValueError("Notion data-source query is incomplete")
            # Check pagination before yielding this response's pages.
            has_more = data["has_more"]
            next_cursor = data["next_cursor"]
            if not isinstance(has_more, bool):
                raise ValueError("Invalid Notion has_more")
            if has_more:
                if (
                    not isinstance(next_cursor, str)
                    or not next_cursor
                    or next_cursor in cursors
                ):
                    raise ValueError("Notion pagination did not advance")
                cursors.add(next_cursor)
            results = data["results"]
            if not isinstance(results, list):
                raise ValueError("Invalid Notion query results")
            for result in results:
                if result["object"] != "page":
                    raise ValueError("Notion query returned a non-page result")
                if (
                    result.get("archived")
                    or result.get("is_archived")
                    or result.get("in_trash")
                ):
                    continue
                page_id = str(UUID(result["id"]))
                if page_id in page_ids:
                    raise ValueError(f"Duplicate Notion page in scan: {page_id}")
                page_ids.add(page_id)
                content = await self._request_json("GET", f"pages/{page_id}/markdown")
                if content["truncated"] or content["unknown_block_ids"]:
                    raise ValueError(f"Incomplete Notion Markdown for page {page_id}")
                markdown = content["markdown"]
                url = result["url"]
                if not isinstance(markdown, str) or not isinstance(url, str):
                    raise ValueError("Invalid Notion page content or URL")
                title = "".join(
                    part["plain_text"]
                    for prop in result["properties"].values()
                    if prop.get("type") == "title"
                    for part in prop["title"]
                )
                yield page_id, NotionPage(page_id, title, url, markdown)
            if not has_more:
                return
            cursor = next_cursor


__all__ = ["NotionPage", "NotionSource"]
