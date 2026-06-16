"""Tests for the Notion target connector.

Requires a real Notion workspace. Gated by env vars:

  NOTION_TEST_TOKEN        — internal-integration secret
  NOTION_TEST_PARENT_PAGE  — page UUID, shared with the integration; tests
                             create temporary databases under it and archive
                             them on teardown.

The whole suite is skipped if either is missing — match the optional-dep
pattern used elsewhere in this repo.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, AsyncIterator, Callable, Coroutine, cast

import pytest
import pytest_asyncio
from typing_extensions import Annotated

import cocoindex as coco
from cocoindex._internal.context_keys import ContextProvider
from cocoindex.connectors import notion
from cocoindex.connectors.notion._target import (  # pyright: ignore[reportPrivateUsage]
    _DatabaseSpec,
    _evolve_schema_if_needed,
    _find_or_create_data_source,
    _property_filter,
)

from tests import common

NOTION_TEST_TOKEN = os.environ.get("NOTION_TEST_TOKEN")
NOTION_TEST_PARENT_PAGE = os.environ.get("NOTION_TEST_PARENT_PAGE")

requires_notion_env = pytest.mark.skipif(
    not (NOTION_TEST_TOKEN and NOTION_TEST_PARENT_PAGE),
    reason="NOTION_TEST_TOKEN and NOTION_TEST_PARENT_PAGE not set",
)

NOTION_CK = coco.ContextKey[notion.NotionClient]("notion_test_client")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unique_title(test_name: str) -> str:
    """Distinct title per test invocation so reruns don't collide."""
    return f"cocoindex-test-{test_name}-{int(time.time())}-{uuid.uuid4().hex[:6]}"


async def _create_test_db(
    client: notion.NotionClient,
    parent_page_id: str,
    title: str,
    properties: dict[str, dict[str, Any]],
) -> str:
    """Create a test database and return the data_source_id."""
    res = await client.create_database(
        parent_page_id=parent_page_id, title=title, properties=properties
    )
    data_source_id: str = (res.get("data_sources") or [{}])[0]["id"]
    return data_source_id


async def _archive_db(client: notion.NotionClient, data_source_id: str) -> None:
    """Best-effort archive — never raise from teardown."""
    try:
        ds = await client.get_data_source(data_source_id)
        db_id = (ds.get("parent") or {}).get("database_id")
        if db_id:
            await client._request("PATCH", f"/databases/{db_id}", {"in_trash": True})
    except Exception:
        pass


async def _active_pages(
    client: notion.NotionClient, data_source_id: str
) -> list[dict[str, Any]]:
    pages = []
    async for page in client.query_all(data_source_id):
        pages.append(page)
    return pages


def _title_of(page: dict[str, Any]) -> str:
    parts = page["properties"].get("Name", {}).get("title") or []
    return "".join(p.get("plain_text", "") for p in parts)


def _make_env(client: notion.NotionClient, suffix: str) -> coco.Environment:
    ctx = ContextProvider()
    ctx.provide(NOTION_CK, client)
    settings = coco.Settings.from_env(
        db_path=common.get_env_db_path(f"connectors__test_notion_target__{suffix}")
    )
    return coco.Environment(settings, context_provider=ctx)


# ---------------------------------------------------------------------------
# Row types
# ---------------------------------------------------------------------------


@dataclass
class Person:
    name: Annotated[str, notion.TitleProp("Name")]
    email: Annotated[str, notion.EmailProp("Email")]
    role: Annotated[str, notion.SelectProp("Role")]
    active: Annotated[bool, notion.CheckboxProp("Active")]


@dataclass
class PersonWithNotes(Person):
    notes: Annotated[str, notion.RichTextProp("Notes")]


PERSON_SCHEMA_PROPS: dict[str, dict[str, Any]] = {
    "Name": {"title": {}},
    "Email": {"email": {}},
    "Role": {
        "select": {
            "options": [
                {"name": "Engineer"},
                {"name": "Designer"},
            ]
        }
    },
    "Active": {"checkbox": {}},
}


class _FakeSystemClient(notion.NotionClient):
    def __init__(self) -> None:
        super().__init__(token="fake", session=cast(Any, object()))
        self.child_pages: list[dict[str, Any]] = []
        self.databases: dict[str, dict[str, Any]] = {}
        self.data_source: dict[str, Any] = {"properties": {}}
        self.created_databases: list[dict[str, Any]] = []
        self.schema_updates: list[dict[str, dict[str, Any] | None]] = []
        self.request_paths: list[str] = []

    async def _request(
        self, method: str, path: str, json_body: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        self.request_paths.append(path)
        assert method == "GET"
        assert path.startswith("/blocks/parent-page/children?")
        if not self.child_pages:
            return {"results": [], "has_more": False, "next_cursor": None}
        if "start_cursor=cursor-2" in path:
            return {
                "results": [self.child_pages[1]],
                "has_more": False,
                "next_cursor": None,
            }
        return {
            "results": [self.child_pages[0]],
            "has_more": len(self.child_pages) > 1,
            "next_cursor": "cursor-2" if len(self.child_pages) > 1 else None,
        }

    async def get_database(self, database_id: str) -> dict[str, Any]:
        return self.databases[database_id]

    async def create_database(
        self,
        *,
        parent_page_id: str,
        title: str,
        properties: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        self.created_databases.append(
            {
                "parent_page_id": parent_page_id,
                "title": title,
                "properties": properties,
            }
        )
        return {"data_sources": [{"id": "created-ds"}]}

    async def get_data_source(self, data_source_id: str) -> dict[str, Any]:
        assert data_source_id == "ds-1"
        return self.data_source

    async def update_data_source_properties(
        self,
        data_source_id: str,
        properties: dict[str, dict[str, Any] | None],
    ) -> dict[str, Any]:
        assert data_source_id == "ds-1"
        self.schema_updates.append(properties)
        return {"id": data_source_id, "properties": properties}


class _FakeResponse:
    def __init__(self, status: int, body: dict[str, Any]) -> None:
        self.status = status
        self.headers: dict[str, str] = {}
        self._body = body

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    def raise_for_status(self) -> None:
        return None

    async def json(self) -> dict[str, Any]:
        return self._body


class _FakeSession:
    def __init__(self, second_request_started: asyncio.Event) -> None:
        self.second_request_started = second_request_started
        self.calls = 0

    def request(
        self,
        method: str,
        url: str,
        *,
        json: dict[str, Any] | None,
        headers: dict[str, str],
    ) -> _FakeResponse:
        self.calls += 1
        if self.calls == 1:
            response = _FakeResponse(429, {})
            response.headers["Retry-After"] = "1"
            return response
        self.second_request_started.set()
        return _FakeResponse(200, {"ok": True})


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def client() -> AsyncIterator[notion.NotionClient]:
    """Per-test NotionClient with the test token."""
    assert NOTION_TEST_TOKEN is not None
    async with notion.NotionClient(token=NOTION_TEST_TOKEN, max_concurrency=1) as c:
        yield c


@pytest_asyncio.fixture
async def user_db(
    client: notion.NotionClient, request: pytest.FixtureRequest
) -> AsyncIterator[str]:
    """Create a pre-existing data source for user-mode tests; teardown archives it."""
    assert NOTION_TEST_PARENT_PAGE is not None
    title = _unique_title(request.node.name)
    ds_id = await _create_test_db(
        client, NOTION_TEST_PARENT_PAGE, title, PERSON_SCHEMA_PROPS
    )
    yield ds_id
    await _archive_db(client, ds_id)


# ---------------------------------------------------------------------------
# Schema validation (no Notion access needed)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_property_map_typo_raises() -> None:
    """Bug fix #2: typoed key in property_map should error, not silently drop."""

    @dataclass
    class R:
        name: str
        email: str

    with pytest.raises(ValueError, match="property_map keys"):
        await notion.DatabaseSchema.from_class(
            R,
            primary_key=["name"],
            property_map={"naame": notion.TitleProp("Name")},
        )


@pytest.mark.asyncio
async def test_schema_requires_at_most_one_title() -> None:
    @dataclass
    class TwoTitles:
        name: Annotated[str, notion.TitleProp("Name")]
        other: Annotated[str, notion.TitleProp("Other")]

    with pytest.raises(ValueError, match="only one TitleProp"):
        await notion.DatabaseSchema.from_class(TwoTitles, primary_key=["name"])


@pytest.mark.asyncio
async def test_duplicate_notion_property_names_raise() -> None:
    @dataclass
    class DuplicateNames:
        name: Annotated[str, notion.TitleProp("Name")]
        display_name: Annotated[str, notion.RichTextProp("Name")]

    with pytest.raises(ValueError, match="same Notion property"):
        await notion.DatabaseSchema.from_class(DuplicateNames, primary_key=["name"])


@pytest.mark.asyncio
async def test_relation_prop_encode_decode() -> None:
    """RelationProp writes page-id lists and creates data-source relations."""
    p = notion.RelationProp("Account")
    assert p.encode(["page1", "page2"]) == {
        "relation": [{"id": "page1"}, {"id": "page2"}]
    }
    assert p.encode("page1") == {"relation": [{"id": "page1"}]}
    assert p.encode(None) == {"relation": []}
    assert p.encode([]) == {"relation": []}
    assert p.decode({"relation": [{"id": "p1"}, {}, {"id": "p2"}]}) == [
        "p1",
        "p2",
    ]
    with pytest.raises(ValueError, match="target_data_source_id"):
        p.to_notion_schema()

    p_typed = notion.RelationProp("Account", target_data_source_id="ds-related")
    assert p_typed.to_notion_schema() == {"relation": {"data_source_id": "ds-related"}}


def test_date_primary_key_filter_uses_isoformat() -> None:
    prop = notion.DateProp("Published")

    assert _property_filter(prop, date(2026, 6, 1)) == {
        "property": "Published",
        "date": {"equals": "2026-06-01"},
    }
    assert _property_filter(prop, datetime(2026, 6, 1, 12, 34, 56)) == {
        "property": "Published",
        "date": {"equals": "2026-06-01T12:34:56"},
    }


@pytest.mark.asyncio
async def test_managed_by_args_validation() -> None:
    """User mode rejects parent_/title kwargs; system mode rejects data_source_id."""

    @dataclass
    class R:
        name: Annotated[str, notion.TitleProp("Name")]

    client_key = coco.ContextKey[notion.NotionClient]("validation_test")
    schema = await notion.DatabaseSchema.from_class(R, primary_key=["name"])

    # User mode: missing data_source_id
    with pytest.raises(ValueError, match="managed_by='user' requires data_source_id"):
        notion.database_target(client_key, None, schema, managed_by="user")

    # User mode: extra system kwargs
    with pytest.raises(ValueError, match="only valid with managed_by='system'"):
        notion.database_target(
            client_key, "ds-id", schema, managed_by="user", title="Foo"
        )

    # System mode: missing title
    with pytest.raises(ValueError, match="managed_by='system' requires title"):
        notion.database_target(
            client_key, None, schema, managed_by="system", parent_page_id="p"
        )

    # System mode: needs exactly one parent
    with pytest.raises(ValueError, match="exactly one of parent_page_id"):
        notion.database_target(
            client_key, None, schema, managed_by="system", title="Foo"
        )


# ---------------------------------------------------------------------------
# API-contract regressions (no Notion access)
# ---------------------------------------------------------------------------
#
# The approved design's acceptance path is real Notion API coverage. These unit
# tests are intentionally narrower: they cover hard-to-force edge contracts from
# Notion's public API docs, while the integration tests below keep exercising
# the actual connector lifecycle against a real workspace when env vars exist.
#


@pytest.mark.asyncio
async def test_system_lookup_paginates_parent_children() -> None:
    @dataclass
    class R:
        name: Annotated[str, notion.TitleProp("Name")]

    schema = await notion.DatabaseSchema.from_class(R, primary_key=["name"])
    client = _FakeSystemClient()
    client.child_pages = [
        {"type": "child_database", "id": "db-other"},
        {"type": "child_database", "id": "db-match"},
    ]
    client.databases = {
        "db-other": {
            "title": [{"plain_text": "Other"}],
            "data_sources": [{"id": "other-ds"}],
        },
        "db-match": {
            "title": [{"plain_text": "Wanted"}],
            "data_sources": [{"id": "wanted-ds"}],
        },
    }

    data_source_id = await _find_or_create_data_source(
        client,
        _DatabaseSpec(
            schema=schema,
            managed_by="system",
            parent_page_id="parent-page",
            title="Wanted",
        ),
    )

    assert data_source_id == "wanted-ds"
    assert client.created_databases == []
    assert len(client.request_paths) == 2
    assert "start_cursor=cursor-2" in client.request_paths[1]


@pytest.mark.asyncio
async def test_system_lookup_matches_data_source_name_when_database_has_many() -> None:
    @dataclass
    class R:
        name: Annotated[str, notion.TitleProp("Name")]

    schema = await notion.DatabaseSchema.from_class(R, primary_key=["name"])
    client = _FakeSystemClient()
    client.child_pages = [{"type": "child_database", "id": "db-match"}]
    client.databases = {
        "db-match": {
            "title": [{"plain_text": "People"}],
            "data_sources": [
                {"id": "wrong-ds", "name": "Archive"},
                {"id": "right-ds", "name": "People"},
            ],
        },
    }

    data_source_id = await _find_or_create_data_source(
        client,
        _DatabaseSpec(
            schema=schema,
            managed_by="system",
            parent_page_id="parent-page",
            title="People",
        ),
    )

    assert data_source_id == "right-ds"


@pytest.mark.asyncio
async def test_system_lookup_matches_data_source_when_database_title_differs() -> None:
    @dataclass
    class R:
        name: Annotated[str, notion.TitleProp("Name")]

    schema = await notion.DatabaseSchema.from_class(R, primary_key=["name"])
    client = _FakeSystemClient()
    client.child_pages = [{"type": "child_database", "id": "db-match"}]
    client.databases = {
        "db-match": {
            "title": [{"plain_text": "Workspace CRM"}],
            "data_sources": [
                {"id": "right-ds", "name": "People"},
            ],
        },
    }

    data_source_id = await _find_or_create_data_source(
        client,
        _DatabaseSpec(
            schema=schema,
            managed_by="system",
            parent_page_id="parent-page",
            title="People",
        ),
    )

    assert data_source_id == "right-ds"


@pytest.mark.asyncio
async def test_system_lookup_rejects_duplicate_data_source_names() -> None:
    @dataclass
    class R:
        name: Annotated[str, notion.TitleProp("Name")]

    schema = await notion.DatabaseSchema.from_class(R, primary_key=["name"])
    client = _FakeSystemClient()
    client.child_pages = [{"type": "child_database", "id": "db-match"}]
    client.databases = {
        "db-match": {
            "title": [{"plain_text": "Workspace CRM"}],
            "data_sources": [
                {"id": "people-a", "name": "People"},
                {"id": "people-b", "name": "People"},
            ],
        },
    }

    with pytest.raises(ValueError, match="multiple Notion data sources"):
        await _find_or_create_data_source(
            client,
            _DatabaseSpec(
                schema=schema,
                managed_by="system",
                parent_page_id="parent-page",
                title="People",
            ),
        )


@pytest.mark.asyncio
async def test_system_lookup_rejects_duplicate_data_sources_across_databases() -> None:
    @dataclass
    class R:
        name: Annotated[str, notion.TitleProp("Name")]

    schema = await notion.DatabaseSchema.from_class(R, primary_key=["name"])
    client = _FakeSystemClient()
    client.child_pages = [
        {"type": "child_database", "id": "db-a"},
        {"type": "child_database", "id": "db-b"},
    ]
    client.databases = {
        "db-a": {
            "title": [{"plain_text": "First CRM"}],
            "data_sources": [{"id": "people-a", "name": "People"}],
        },
        "db-b": {
            "title": [{"plain_text": "Second CRM"}],
            "data_sources": [{"id": "people-b", "name": "People"}],
        },
    }

    with pytest.raises(ValueError, match="multiple Notion data sources"):
        await _find_or_create_data_source(
            client,
            _DatabaseSpec(
                schema=schema,
                managed_by="system",
                parent_page_id="parent-page",
                title="People",
            ),
        )


@pytest.mark.asyncio
async def test_system_lookup_rejects_duplicate_single_source_title_fallbacks() -> None:
    @dataclass
    class R:
        name: Annotated[str, notion.TitleProp("Name")]

    schema = await notion.DatabaseSchema.from_class(R, primary_key=["name"])
    client = _FakeSystemClient()
    client.child_pages = [
        {"type": "child_database", "id": "db-a"},
        {"type": "child_database", "id": "db-b"},
    ]
    client.databases = {
        "db-a": {
            "title": [{"plain_text": "People"}],
            "data_sources": [{"id": "ds-a"}],
        },
        "db-b": {
            "title": [{"plain_text": "People"}],
            "data_sources": [{"id": "ds-b"}],
        },
    }

    with pytest.raises(ValueError, match="single-source Notion databases"):
        await _find_or_create_data_source(
            client,
            _DatabaseSpec(
                schema=schema,
                managed_by="system",
                parent_page_id="parent-page",
                title="People",
            ),
        )


@pytest.mark.asyncio
async def test_system_destructive_evolution_patches_type_change() -> None:
    @dataclass
    class R:
        name: Annotated[str, notion.TitleProp("Name")]
        email: Annotated[str, notion.RichTextProp("Email")]

    schema = await notion.DatabaseSchema.from_class(R, primary_key=["name"])
    client = _FakeSystemClient()
    client.data_source = {
        "properties": {
            "Name": {"type": "title"},
            "Email": {"type": "email"},
        }
    }
    spec = _DatabaseSpec(
        schema=schema,
        managed_by="system",
        parent_page_id="parent-page",
        title="People",
        allow_destructive=True,
    )

    await _evolve_schema_if_needed(client, "ds-1", spec)

    assert client.schema_updates == [{"Email": {"rich_text": {}}}]


@pytest.mark.asyncio
async def test_system_destructive_evolution_still_rejects_title_changes() -> None:
    @dataclass
    class R:
        name: Annotated[str, notion.TitleProp("Name")]

    schema = await notion.DatabaseSchema.from_class(R, primary_key=["name"])
    client = _FakeSystemClient()
    client.data_source = {"properties": {"Title": {"type": "title"}}}
    spec = _DatabaseSpec(
        schema=schema,
        managed_by="system",
        parent_page_id="parent-page",
        title="People",
        allow_destructive=True,
    )

    with pytest.raises(ValueError, match="title property cannot"):
        await _evolve_schema_if_needed(client, "ds-1", spec)


@pytest.mark.asyncio
async def test_rate_limit_sleep_releases_concurrency_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    second_request_started = asyncio.Event()
    first_sleep_started = asyncio.Event()
    finish_sleep = asyncio.Event()
    session = _FakeSession(second_request_started)
    client = notion.NotionClient(
        token="fake", max_concurrency=1, session=cast(Any, session)
    )

    async def fake_sleep(delay: float) -> None:
        assert delay == 1.0
        first_sleep_started.set()
        await finish_sleep.wait()

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    first = asyncio.create_task(client._request("GET", "/first"))
    await first_sleep_started.wait()
    second = asyncio.create_task(client._request("GET", "/second"))
    await asyncio.wait_for(second_request_started.wait(), timeout=1)
    assert await second == {"ok": True}
    finish_sleep.set()
    assert await first == {"ok": True}


# ---------------------------------------------------------------------------
# User-mode end-to-end (require Notion env)
# ---------------------------------------------------------------------------


def _user_mode_main(
    user_db_id: str,
    rows: list[Person],
    on_delete: notion.OnDelete = notion.OnDelete.ARCHIVE,
) -> Callable[[], Coroutine[Any, Any, None]]:
    """Return an async main fn that mounts a user-mode target and declares ``rows``."""

    async def app_main() -> None:
        target = await notion.mount_database_target(
            NOTION_CK,
            user_db_id,
            await notion.DatabaseSchema.from_class(Person, primary_key=["name"]),
            managed_by="user",
            on_delete=on_delete,
        )
        for r in rows:
            target.declare_row(row=r)

    return app_main


@requires_notion_env
@pytest.mark.asyncio
async def test_insert_update_archive(
    client: notion.NotionClient,
    user_db: str,
    request: pytest.FixtureRequest,
) -> None:
    """End-to-end: insert 3 rows, change one, drop one — see PATCH and archive."""
    env = _make_env(client, request.node.name)

    rows = [
        Person(name="Ada", email="ada@x.com", role="Engineer", active=True),
        Person(name="Grace", email="grace@x.com", role="Engineer", active=True),
        Person(name="Alan", email="alan@x.com", role="Engineer", active=False),
    ]

    # All three steps reuse the same App name so cocoindex's tracking record
    # carries across; without that, step 3 wouldn't know Alan was previously
    # declared and the archive wouldn't fire.
    app_name = "lifecycle"

    # 1. Insert
    await coco.App(
        coco.AppConfig(name=app_name, environment=env),
        _user_mode_main(user_db, rows),
    ).update()
    pages = await _active_pages(client, user_db)
    assert {_title_of(p) for p in pages} == {"Ada", "Grace", "Alan"}

    # 2. Update Ada's email
    rows[0] = Person(name="Ada", email="ada@new.com", role="Engineer", active=True)
    await coco.App(
        coco.AppConfig(name=app_name, environment=env),
        _user_mode_main(user_db, rows),
    ).update()
    pages = await _active_pages(client, user_db)
    ada = next(p for p in pages if _title_of(p) == "Ada")
    assert ada["properties"]["Email"]["email"] == "ada@new.com"

    # 3. Drop Alan -> page archived
    rows.pop()  # remove Alan
    await coco.App(
        coco.AppConfig(name=app_name, environment=env),
        _user_mode_main(user_db, rows),
    ).update()
    pages = await _active_pages(client, user_db)
    assert {_title_of(p) for p in pages} == {"Ada", "Grace"}


@requires_notion_env
@pytest.mark.asyncio
async def test_on_delete_ignore_leaves_page(
    client: notion.NotionClient,
    user_db: str,
    request: pytest.FixtureRequest,
) -> None:
    env = _make_env(client, request.node.name)
    rows = [Person(name="Ada", email="ada@x.com", role="Engineer", active=True)]
    app_name = "ignore"
    await coco.App(
        coco.AppConfig(name=app_name, environment=env),
        _user_mode_main(user_db, rows, on_delete=notion.OnDelete.IGNORE),
    ).update()
    assert len(await _active_pages(client, user_db)) == 1

    # Undeclare — reuse the same app name so cocoindex's prior tracking is
    # carried across; otherwise it wouldn't know the row "went missing".
    await coco.App(
        coco.AppConfig(name=app_name, environment=env),
        _user_mode_main(user_db, [], on_delete=notion.OnDelete.IGNORE),
    ).update()
    # Page is still there — IGNORE doesn't archive.
    assert len(await _active_pages(client, user_db)) == 1


@requires_notion_env
@pytest.mark.asyncio
async def test_noop_when_no_changes(
    client: notion.NotionClient,
    user_db: str,
    request: pytest.FixtureRequest,
) -> None:
    """Second run with identical data must not touch Notion.

    Concretely: capture last_edited_time after run 1, run 2 with the same rows,
    confirm the timestamps are unchanged (no PATCH was issued).
    """
    env = _make_env(client, request.node.name)
    rows = [
        Person(name="Ada", email="ada@x.com", role="Engineer", active=True),
        Person(name="Grace", email="grace@x.com", role="Engineer", active=True),
    ]
    app_name = "noop"
    await coco.App(
        coco.AppConfig(name=app_name, environment=env),
        _user_mode_main(user_db, rows),
    ).update()
    timestamps_run1 = {
        _title_of(p): p["last_edited_time"]
        for p in await _active_pages(client, user_db)
    }

    # Re-run with identical rows; cocoindex's fingerprint should short-circuit
    # the reconcile and no PATCH should be issued.
    await coco.App(
        coco.AppConfig(name=app_name, environment=env),
        _user_mode_main(user_db, rows),
    ).update()
    timestamps_run2 = {
        _title_of(p): p["last_edited_time"]
        for p in await _active_pages(client, user_db)
    }
    assert timestamps_run1 == timestamps_run2, "no-op run somehow touched the pages"


@requires_notion_env
@pytest.mark.asyncio
async def test_on_delete_hard(
    client: notion.NotionClient,
    user_db: str,
    request: pytest.FixtureRequest,
) -> None:
    """OnDelete.HARD trashes the page (DELETE /blocks/{id}). Verify it's gone
    from active queries (same as archive from the user POV, but the page is
    in trash rather than archived)."""
    env = _make_env(client, request.node.name)
    app_name = "hard"
    rows = [Person(name="Ada", email="ada@x.com", role="Engineer", active=True)]
    await coco.App(
        coco.AppConfig(name=app_name, environment=env),
        _user_mode_main(user_db, rows, on_delete=notion.OnDelete.HARD),
    ).update()
    assert len(await _active_pages(client, user_db)) == 1

    await coco.App(
        coco.AppConfig(name=app_name, environment=env),
        _user_mode_main(user_db, [], on_delete=notion.OnDelete.HARD),
    ).update()
    assert len(await _active_pages(client, user_db)) == 0


@requires_notion_env
@pytest.mark.asyncio
async def test_property_types_roundtrip(
    client: notion.NotionClient,
    request: pytest.FixtureRequest,
) -> None:
    """Title + rich_text + number + url + checkbox + select + date all round-trip
    through encode -> Notion -> query -> decode without corruption.
    """

    @dataclass
    class AllTypes:
        name: Annotated[str, notion.TitleProp("Name")]
        notes: Annotated[str, notion.RichTextProp("Notes")]
        score: Annotated[float, notion.NumberProp("Score")]
        homepage: Annotated[str, notion.UrlProp("Homepage")]
        active: Annotated[bool, notion.CheckboxProp("Active")]
        role: Annotated[str, notion.SelectProp("Role")]
        joined: Annotated[date, notion.DateProp("Joined")]

    assert NOTION_TEST_PARENT_PAGE is not None
    title = _unique_title(request.node.name)
    ds_id = await _create_test_db(
        client,
        NOTION_TEST_PARENT_PAGE,
        title,
        {
            "Name": {"title": {}},
            "Notes": {"rich_text": {}},
            "Score": {"number": {}},
            "Homepage": {"url": {}},
            "Active": {"checkbox": {}},
            "Role": {"select": {"options": [{"name": "Engineer"}]}},
            "Joined": {"date": {}},
        },
    )
    try:
        env = _make_env(client, request.node.name)
        row = AllTypes(
            name="Alice",
            notes="Likes long walks",
            score=3.14,
            homepage="https://example.com",
            active=True,
            role="Engineer",
            joined=date(2026, 1, 15),
        )

        async def app_main() -> None:
            target = await notion.mount_database_target(
                NOTION_CK,
                ds_id,
                await notion.DatabaseSchema.from_class(AllTypes, primary_key=["name"]),
                managed_by="user",
            )
            target.declare_row(row=row)

        await coco.App(
            coco.AppConfig(name="alltypes", environment=env), app_main
        ).update()

        pages = await _active_pages(client, ds_id)
        assert len(pages) == 1
        props = pages[0]["properties"]
        assert _title_of(pages[0]) == "Alice"
        assert (
            "".join(p.get("plain_text", "") for p in props["Notes"]["rich_text"])
            == "Likes long walks"
        )
        assert props["Score"]["number"] == 3.14
        assert props["Homepage"]["url"] == "https://example.com"
        assert props["Active"]["checkbox"] is True
        assert props["Role"]["select"]["name"] == "Engineer"
        assert props["Joined"]["date"]["start"] == "2026-01-15"
    finally:
        await _archive_db(client, ds_id)


@requires_notion_env
@pytest.mark.asyncio
async def test_first_run_against_existing_page(
    client: notion.NotionClient,
    user_db: str,
    request: pytest.FixtureRequest,
) -> None:
    """If a page with the declared PK already exists in Notion (e.g. the user
    pre-seeded it), the first run should PATCH it — not create a duplicate.
    Exercises the query-on-miss path returning a hit on first attempt.
    """
    # Pre-seed: create a page directly via the API.
    await client.create_page(
        user_db,
        {
            "Name": {"title": [{"text": {"content": "Ada"}}]},
            "Email": {"email": "ada@old.com"},
            "Role": {"select": {"name": "Engineer"}},
            "Active": {"checkbox": False},
        },
    )
    assert len(await _active_pages(client, user_db)) == 1

    env = _make_env(client, request.node.name)
    await coco.App(
        coco.AppConfig(name="preseed", environment=env),
        _user_mode_main(
            user_db,
            [Person(name="Ada", email="ada@updated.com", role="Engineer", active=True)],
        ),
    ).update()
    pages = await _active_pages(client, user_db)
    assert len(pages) == 1, "should have updated the pre-existing page, not duplicated"
    assert pages[0]["properties"]["Email"]["email"] == "ada@updated.com"
    assert pages[0]["properties"]["Active"]["checkbox"] is True


@requires_notion_env
@pytest.mark.asyncio
async def test_schema_validation_type_mismatch(
    client: notion.NotionClient,
    user_db: str,
    request: pytest.FixtureRequest,
) -> None:
    """Bug fix #1: declared rich_text on a notion email field → mount fails."""
    env = _make_env(client, request.node.name)

    @dataclass
    class WrongPerson:
        name: Annotated[str, notion.TitleProp("Name")]
        # Email is email type in Notion; we declare it as rich_text → mismatch.
        email: Annotated[str, notion.RichTextProp("Email")]

    async def app_main() -> None:
        await notion.mount_database_target(
            NOTION_CK,
            user_db,
            await notion.DatabaseSchema.from_class(WrongPerson, primary_key=["name"]),
            managed_by="user",
        )

    app = coco.App(coco.AppConfig(name="mismatch", environment=env), app_main)
    with pytest.raises(Exception, match="type mismatches"):
        await app.update()


@requires_notion_env
@pytest.mark.asyncio
async def test_schema_validation_missing_property(
    client: notion.NotionClient,
    user_db: str,
    request: pytest.FixtureRequest,
) -> None:
    env = _make_env(client, request.node.name)

    @dataclass
    class ExtraField:
        name: Annotated[str, notion.TitleProp("Name")]
        ghost: Annotated[str, notion.RichTextProp("Ghost")]  # not in DS

    async def app_main() -> None:
        await notion.mount_database_target(
            NOTION_CK,
            user_db,
            await notion.DatabaseSchema.from_class(ExtraField, primary_key=["name"]),
            managed_by="user",
        )

    app = coco.App(coco.AppConfig(name="missing", environment=env), app_main)
    with pytest.raises(Exception, match="missing properties"):
        await app.update()


@requires_notion_env
@pytest.mark.asyncio
async def test_multiple_targets_in_one_app(
    client: notion.NotionClient,
    request: pytest.FixtureRequest,
) -> None:
    """Two ``mount_database_target`` calls in one app sync independently.

    Catches the class of bug where per-target state (page_id cache, locks,
    tracking record identity) would accidentally be shared across targets.
    Verifies it both at insert time (each target gets its own row) and on
    undeclare (dropping rows from one target doesn't affect the other).
    """
    assert NOTION_TEST_PARENT_PAGE is not None
    title_a = _unique_title(request.node.name + "_a")
    title_b = _unique_title(request.node.name + "_b")
    ds_a = await _create_test_db(
        client, NOTION_TEST_PARENT_PAGE, title_a, PERSON_SCHEMA_PROPS
    )
    ds_b = await _create_test_db(
        client, NOTION_TEST_PARENT_PAGE, title_b, PERSON_SCHEMA_PROPS
    )
    try:
        env = _make_env(client, request.node.name)
        rows_a = [Person(name="A1", email="a1@x.com", role="Engineer", active=True)]
        rows_b = [Person(name="B1", email="b1@x.com", role="Engineer", active=True)]

        async def app_main() -> None:
            schema = await notion.DatabaseSchema.from_class(
                Person, primary_key=["name"]
            )
            target_a = await coco.use_mount(
                coco.component_subpath("setup", "target_a"),
                notion.declare_database_target,
                NOTION_CK,
                ds_a,
                schema,
                managed_by="user",
            )
            target_b = await coco.use_mount(
                coco.component_subpath("setup", "target_b"),
                notion.declare_database_target,
                NOTION_CK,
                ds_b,
                schema,
                managed_by="user",
            )
            for r in rows_a:
                target_a.declare_row(row=r)
            for r in rows_b:
                target_b.declare_row(row=r)

        app_name = "multitarget"

        # Step 1: each target gets its own row.
        await coco.App(
            coco.AppConfig(name=app_name, environment=env), app_main
        ).update()
        assert {_title_of(p) for p in await _active_pages(client, ds_a)} == {"A1"}
        assert {_title_of(p) for p in await _active_pages(client, ds_b)} == {"B1"}

        # Step 2: drop A1 only. ds_a should empty out; ds_b should keep B1.
        rows_a.clear()
        await coco.App(
            coco.AppConfig(name=app_name, environment=env), app_main
        ).update()
        assert {_title_of(p) for p in await _active_pages(client, ds_a)} == set()
        assert {_title_of(p) for p in await _active_pages(client, ds_b)} == {"B1"}
    finally:
        await _archive_db(client, ds_a)
        await _archive_db(client, ds_b)


# ---------------------------------------------------------------------------
# System mode
# ---------------------------------------------------------------------------


@requires_notion_env
@pytest.mark.asyncio
async def test_system_creates_and_evolves(
    client: notion.NotionClient,
    request: pytest.FixtureRequest,
) -> None:
    """First run: DS doesn't exist -> connector creates it.
    Second run with extended schema: connector PATCHes the new property.
    """
    assert NOTION_TEST_PARENT_PAGE is not None
    title = _unique_title(request.node.name)
    env = _make_env(client, request.node.name)

    async def app_create() -> None:
        target = await notion.mount_database_target(
            NOTION_CK,
            schema=await notion.DatabaseSchema.from_class(Person, primary_key=["name"]),
            managed_by="system",
            parent_page_id=NOTION_TEST_PARENT_PAGE,
            title=title,
        )
        target.declare_row(
            row=Person(name="Seed", email="seed@x.com", role="Engineer", active=True)
        )

    created_db_id: str | None = None
    try:
        await coco.App(coco.AppConfig(name="s1", environment=env), app_create).update()

        # Find the DS the connector just created by enumerating children.
        children = await client._request(
            "GET", f"/blocks/{NOTION_TEST_PARENT_PAGE}/children?page_size=100"
        )
        ds_id: str | None = None
        for c in children.get("results", []):
            if c.get("type") != "child_database":
                continue
            db = await client.get_database(c["id"])
            if _title_of_db(db) == title:
                created_db_id = c["id"]
                ds_id = (db.get("data_sources") or [{}])[0].get("id")
                break
        assert ds_id is not None, "system mode should have created the DS"
        ds = await client.get_data_source(ds_id)
        assert set(ds["properties"].keys()) >= {"Name", "Email", "Role", "Active"}
        assert len(await _active_pages(client, ds_id)) == 1

        # Now extend the schema with a Notes column and re-run.
        async def app_evolve() -> None:
            target = await notion.mount_database_target(
                NOTION_CK,
                schema=await notion.DatabaseSchema.from_class(
                    PersonWithNotes, primary_key=["name"]
                ),
                managed_by="system",
                parent_page_id=NOTION_TEST_PARENT_PAGE,
                title=title,
            )
            target.declare_row(
                row=PersonWithNotes(
                    name="Seed",
                    email="seed@x.com",
                    role="Engineer",
                    active=True,
                    notes="updated",
                )
            )

        await coco.App(coco.AppConfig(name="s2", environment=env), app_evolve).update()
        ds = await client.get_data_source(ds_id)
        assert "Notes" in ds["properties"]
        assert ds["properties"]["Notes"]["type"] == "rich_text"
    finally:
        if created_db_id:
            try:
                await client._request(
                    "PATCH", f"/databases/{created_db_id}", {"in_trash": True}
                )
            except Exception:
                pass


def _title_of_db(db: dict[str, Any]) -> str:
    parts = db.get("title") or []
    return "".join(p.get("plain_text", "") for p in parts)
