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
from tenacity import retry, stop_after_attempt, wait_exponential

NOTION_API_VERSION = "2025-09-03"
NOTION_BASE_URL = "https://api.notion.com/v1"


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

    @retry(stop=stop_after_attempt(10), wait=wait_exponential(min=2, max=60))
    async def _request(
        self, method: str, path: str, json_body: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        session = self._require_session()
        url = f"{NOTION_BASE_URL}{path}"
        async with self._sem:
            async with session.request(
                method, url, json=json_body, headers=self._headers()
            ) as r:
                if r.status == 429:
                    # Honor server-supplied backoff before tenacity decides.
                    retry_after = float(r.headers.get("Retry-After", "1"))
                    await asyncio.sleep(retry_after)
                    raise RuntimeError("notion rate_limited")
                r.raise_for_status()
                result: dict[str, Any] = await r.json()
                return result

    async def get_data_source(self, data_source_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/data_sources/{data_source_id}")

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
