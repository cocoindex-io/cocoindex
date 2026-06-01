"""Notion HTTP client used by the target connector.

Thin wrapper around the Notion REST API (version 2025-09-03) with the
concurrency + retry behavior required by CocoIndex's high-fanout sinks:

- shared per-client asyncio.Semaphore caps concurrent calls
- honors Retry-After on 429s
- bounded exponential backoff via tenacity
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

import aiohttp

NOTION_API_VERSION = "2025-09-03"
NOTION_BASE_URL = "https://api.notion.com/v1"

# Manual retry: 10 attempts with exponential backoff capped at 60s. Implemented
# inline (rather than via tenacity) so mypy can fully type the decorated method
# without an untyped-decorator escape hatch.
_MAX_ATTEMPTS = 10
_MIN_WAIT = 2.0
_MAX_WAIT = 60.0


@dataclass
class NotionClient:
    """A token-scoped Notion API client with built-in rate limiting.

    Use as an async context manager so the underlying aiohttp session is
    closed deterministically::

        async with NotionClient(token=...) as client:
            ...

    Or pass an explicit ``session`` if you want to reuse one across clients.

    ``max_concurrency`` defaults to 3, matching Notion's documented sustained
    rate limit; tune lower if the integration is shared with other workloads.
    """

    token: str
    max_concurrency: int = 3
    session: aiohttp.ClientSession | None = None
    _owns_session: bool = field(default=False, init=False)
    _sem: asyncio.Semaphore = field(init=False)

    def __post_init__(self) -> None:
        self._sem = asyncio.Semaphore(self.max_concurrency)

    async def __aenter__(self) -> "NotionClient":
        if self.session is None:
            self.session = aiohttp.ClientSession()
            self._owns_session = True
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_session and self.session is not None:
            await self.session.close()
            self.session = None
            self._owns_session = False

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Notion-Version": NOTION_API_VERSION,
            "Content-Type": "application/json",
        }

    def _require_session(self) -> aiohttp.ClientSession:
        if self.session is None:
            raise RuntimeError(
                "NotionClient session not started; use 'async with NotionClient(...)' "
                "or assign a session explicitly."
            )
        return self.session

    async def _request(
        self, method: str, path: str, json_body: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        session = self._require_session()
        url = f"{NOTION_BASE_URL}{path}"
        last_error: Exception | None = None
        for attempt in range(_MAX_ATTEMPTS):
            async with self._sem:
                async with session.request(
                    method, url, json=json_body, headers=self._headers()
                ) as r:
                    if r.status == 429:
                        # Honor server-supplied backoff, then retry.
                        retry_after = float(r.headers.get("Retry-After", "1"))
                        await asyncio.sleep(retry_after)
                        last_error = RuntimeError("notion rate_limited")
                        continue
                    r.raise_for_status()
                    result: dict[str, Any] = await r.json()
                    return result
            # Exponential backoff between attempts, capped at _MAX_WAIT.
            await asyncio.sleep(min(_MIN_WAIT * (2**attempt), _MAX_WAIT))
        raise RuntimeError(
            f"Notion request {method} {path} failed after {_MAX_ATTEMPTS} attempts"
        ) from last_error

    async def get_data_source(self, data_source_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/data_sources/{data_source_id}")

    async def get_database(self, database_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/databases/{database_id}")

    async def create_database(
        self,
        *,
        parent_page_id: str,
        title: str,
        properties: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """POST /v1/databases — create a new database under a parent page,
        with one initial data source.

        Returns the full database object, including ``data_sources`` — the
        new data source's id is ``result["data_sources"][0]["id"]``.
        """
        body: dict[str, Any] = {
            "parent": {"type": "page_id", "page_id": parent_page_id},
            "title": [{"type": "text", "text": {"content": title}}],
            "initial_data_source": {"properties": properties},
        }
        return await self._request("POST", "/databases", body)

    async def create_data_source(
        self,
        *,
        parent_database_id: str,
        title: str,
        properties: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """POST /v1/data_sources — add a new data source to an existing
        database.
        """
        body: dict[str, Any] = {
            "parent": {"type": "database_id", "database_id": parent_database_id},
            "title": [{"type": "text", "text": {"content": title}}],
            "properties": properties,
        }
        return await self._request("POST", "/data_sources", body)

    async def update_data_source_properties(
        self,
        data_source_id: str,
        properties: dict[str, dict[str, Any] | None],
    ) -> dict[str, Any]:
        """PATCH /v1/data_sources/{id} — add / rename / change-type / remove
        properties. To remove, pass ``None`` as the value for that name.
        """
        return await self._request(
            "PATCH", f"/data_sources/{data_source_id}", {"properties": properties}
        )

    async def query_data_source(
        self,
        data_source_id: str,
        *,
        filter: dict[str, Any] | None = None,
        page_size: int = 100,
        start_cursor: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"page_size": page_size}
        if filter is not None:
            body["filter"] = filter
        if start_cursor is not None:
            body["start_cursor"] = start_cursor
        return await self._request(
            "POST", f"/data_sources/{data_source_id}/query", body
        )

    async def query_all(
        self, data_source_id: str, *, filter: dict[str, Any] | None = None
    ) -> AsyncIterator[dict[str, Any]]:
        cursor: str | None = None
        while True:
            res = await self.query_data_source(
                data_source_id, filter=filter, start_cursor=cursor
            )
            for page in res.get("results", []):
                yield page
            if not res.get("has_more"):
                return
            cursor = res.get("next_cursor")

    async def create_page(
        self, data_source_id: str, properties: dict[str, Any]
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "parent": {"data_source_id": data_source_id},
            "properties": properties,
        }
        return await self._request("POST", "/pages", body)

    async def update_page_properties(
        self, page_id: str, properties: dict[str, Any]
    ) -> dict[str, Any]:
        return await self._request(
            "PATCH", f"/pages/{page_id}", {"properties": properties}
        )

    async def archive_page(self, page_id: str) -> dict[str, Any]:
        return await self._request("PATCH", f"/pages/{page_id}", {"archived": True})

    async def delete_page(self, page_id: str) -> dict[str, Any]:
        # Notion treats DELETE on a block (a page IS a block) as a trash operation.
        return await self._request("DELETE", f"/blocks/{page_id}")
