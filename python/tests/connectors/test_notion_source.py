"""Notion's HTTP contract and incremental processing, without credentials."""

import asyncio
import json
from datetime import timedelta
from typing import Any

import pytest

try:
    import httpx
except ImportError:
    pytest.skip("httpx is not installed", allow_module_level=True)

from cocoindex.connectors.notion import NotionPage, NotionSource
import cocoindex as coco
from tests import common
from tests.common.target_states import GlobalDictTarget


SOURCE_ID = "00000000-0000-0000-0000-000000000001"
PAGE_ID = "00000000-0000-0000-0000-000000000002"
OTHER_ID = "00000000-0000-0000-0000-000000000003"


def page(page_id: str = PAGE_ID) -> dict[str, Any]:
    return {
        "object": "page",
        "id": page_id,
        "url": f"https://www.notion.so/{page_id}",
        "properties": {
            "Custom title": {
                "type": "title",
                "title": [{"plain_text": "Hello "}, {"plain_text": "世界"}],
            },
        },
    }


def query(*pages: dict[str, Any], cursor: str | None = None) -> dict[str, Any]:
    return {
        "results": list(pages),
        "has_more": cursor is not None,
        "next_cursor": cursor,
    }


def markdown(text: str = "# Hello 世界\n") -> dict[str, Any]:
    return {"markdown": text, "truncated": False, "unknown_block_ids": []}


async def collect(source: NotionSource) -> list[tuple[str, NotionPage]]:
    return [item async for item in source.items(SOURCE_ID)]


@pytest.mark.asyncio
async def test_pagination_content_and_client_ownership() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.host == "api.notion.com"
        assert request.headers["Authorization"] == "Bearer test-only"
        assert request.headers["Notion-Version"] == "2026-03-11"
        if request.method == "POST":
            body = json.loads(request.content)
            assert body["page_size"] == 100
            data = (
                query(page(OTHER_ID))
                if body.get("start_cursor") == "opaque cursor"
                else query(page(), cursor="opaque cursor")
            )
        else:
            assert request.url.params["markdown_version"] == "v2"
            data = markdown("") if OTHER_ID in request.url.path else markdown()
        return httpx.Response(200, json=data)

    async with httpx.AsyncClient(
        headers={"Authorization": "Bearer test-only", "Notion-Version": "old"},
        base_url="https://example.invalid",
        transport=httpx.MockTransport(respond),
    ) as client:
        source = NotionSource(client)
        items = await collect(source)
        assert [key for key, _ in items] == [PAGE_ID, OTHER_ID]
        assert items[0][1] == NotionPage(
            PAGE_ID, "Hello 世界", f"https://www.notion.so/{PAGE_ID}", "# Hello 世界\n"
        )
        assert items[1][1].markdown == ""
        assert not client.is_closed
        await collect(source)
        assert len(requests) == 8  # Fresh query and content on each scan.
        assert client.headers["Notion-Version"] == "old"


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["archived", "is_archived", "in_trash"])
async def test_archived_pages_are_omitted(field: str) -> None:
    archived = page()
    archived[field] = True

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        return httpx.Response(200, json=query(archived))

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        assert await collect(NotionSource(client)) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_query",
    [
        {**query(page()), "request_status": {"type": "incomplete"}},
        {**query(page()), "has_more": True},
        {**query(page()), "has_more": "false"},
        {**query(page()), "results": {}},
        query({"object": "data_source"}),
    ],
)
async def test_invalid_or_capped_query_fails_before_content(
    bad_query: dict[str, Any],
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        return httpx.Response(200, json=bad_query)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ValueError):
            await collect(NotionSource(client))


@pytest.mark.asyncio
@pytest.mark.parametrize("duplicate_page", [False, True])
async def test_repeated_cursor_or_page_is_rejected(duplicate_page: bool) -> None:
    queries = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal queries
        if request.method == "GET":
            return httpx.Response(200, json=markdown())
        queries += 1
        return httpx.Response(
            200,
            json=query(
                page(), cursor="first" if queries == 1 or not duplicate_page else "next"
            ),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ValueError, match="Duplicate|did not advance"):
            await collect(NotionSource(client))
        assert queries == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_content",
    [
        {**markdown(), "truncated": True},
        {**markdown(), "unknown_block_ids": [OTHER_ID]},
        {**markdown(), "markdown": None},
    ],
)
async def test_incomplete_or_invalid_markdown_fails(
    bad_content: dict[str, Any],
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json=query(page()) if request.method == "POST" else bad_content
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ValueError):
            await collect(NotionSource(client))


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 401, 403, 404, 406, 302])
async def test_permanent_errors_and_redirects_are_not_retried(status: int) -> None:
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(status, headers={"Location": "https://example.invalid"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), follow_redirects=True
    ) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await collect(NotionSource(client))
    assert calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 500, 502, 503, 504, 529])
async def test_transient_retry_and_exhaustion(
    status: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = 0
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(asyncio, "sleep", sleep)

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(status, headers={"Retry-After": "3"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await collect(NotionSource(client))
    assert calls == 5
    assert sleeps == ([3.0] * 4 if status == 429 else [1.0, 2.0, 4.0, 8.0])


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_after", ["invalid", "NaN", "inf", "-1"])
async def test_invalid_retry_after_falls_back(
    retry_after: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = 0
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(asyncio, "sleep", sleep)

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return (
            httpx.Response(429, headers={"Retry-After": retry_after})
            if calls == 1
            else httpx.Response(200, json=query())
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        assert await collect(NotionSource(client)) == []
    assert sleeps == [1.0]


@pytest.mark.asyncio
async def test_transport_failure_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    async def sleep(delay: float) -> None:
        pass

    monkeypatch.setattr(asyncio, "sleep", sleep)

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ReadTimeout("test", request=request)
        return httpx.Response(200, json=query())

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        assert await collect(NotionSource(client)) == []
    assert calls == 2


@pytest.mark.asyncio
async def test_cancellation_propagates() -> None:
    calls = 0

    async def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise asyncio.CancelledError()

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(asyncio.CancelledError):
            await collect(NotionSource(client))
    assert calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("rate_limited", [False, True])
async def test_ambient_deadline_bounds_request_and_retry_sleep(
    rate_limited: bool,
) -> None:
    calls = 0

    async def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if rate_limited:
            return httpx.Response(429, headers={"Retry-After": "600"})
        await asyncio.sleep(10)
        return httpx.Response(200, json=query())

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with coco.timeout(timedelta(milliseconds=20)):
            with pytest.raises(coco.DeadlineExceededError):
                await collect(NotionSource(client))
    assert calls == 1


@pytest.mark.asyncio
async def test_invalid_id_fails_without_http() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        pytest.fail("Invalid ID must not make an HTTP request")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ValueError):
            async for _ in NotionSource(client).items("../other?query=bad"):
                pass


@pytest.mark.asyncio
async def test_app_reuses_memo_updates_content_and_deletes_removed_page() -> None:
    contents = {PAGE_ID: "original", OTHER_ID: "other"}
    transformed: list[str] = []
    fail_scan = False
    GlobalDictTarget.store.clear()
    env = common.create_test_env(__file__)

    def respond(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            response = query(*(page(key) for key in contents))
            if fail_scan:
                response["request_status"] = {"type": "incomplete"}
            return httpx.Response(200, json=response)
        key = request.url.path.split("/")[-2]
        return httpx.Response(200, json=markdown(contents[key]))

    @coco.fn(memo=True)
    def transform(entry: NotionPage) -> str:
        transformed.append(entry.page_id)
        return entry.markdown.upper()

    @coco.fn
    def process(entry: NotionPage) -> None:
        coco.declare_target_state(
            GlobalDictTarget.target_state(entry.page_id, transform(entry))
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        source = NotionSource(client)

        @coco.fn
        async def main() -> None:
            # A failed scan must not reconcile a partial list of children.
            items = await collect(source)
            await coco.mount_each(process, items)

        app = coco.App(coco.AppConfig(name="notion_source", environment=env), main)
        try:
            await app.update()
            assert set(transformed) == {PAGE_ID, OTHER_ID}
            transformed.clear()
            await app.update()
            assert transformed == []
            assert set(GlobalDictTarget.store.data) == {PAGE_ID, OTHER_ID}

            # No timestamp is consulted: a changed body invalidates the memo.
            contents[PAGE_ID] = "changed"
            await app.update()
            assert transformed == [PAGE_ID]
            assert GlobalDictTarget.store.data[PAGE_ID].data == "CHANGED"

            del contents[OTHER_ID]
            fail_scan = True
            with pytest.raises(ValueError, match="incomplete"):
                await app.update()
            assert set(GlobalDictTarget.store.data) == {PAGE_ID, OTHER_ID}

            fail_scan = False
            await app.update()
            assert set(GlobalDictTarget.store.data) == {PAGE_ID}
            await app.drop()
            assert GlobalDictTarget.store.data == {}
        finally:
            env.close()
