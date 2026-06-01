"""Notion database target.

Two-level structure that mirrors ``connectors.postgres`` /
``connectors.sqlite``:

- ``_DatabaseHandler`` (root) owns the data source. Supports two modes:

  - ``managed_by="user"`` (default): the data source must already exist;
    the connector validates that the declared property schema matches the
    live data source on every apply.
  - ``managed_by="system"``: the connector looks under the parent (page or
    database) for a data source with the given title; creates it on first
    run if missing, and PATCH-adds new properties on subsequent runs when
    the dataclass grows. Destructive schema changes are rejected unless
    ``allow_destructive=True``.

- ``_RowHandler`` (child) reconciles individual pages against their
  fingerprints and applies upsert / archive actions.

Page-id persistence uses the query-on-miss approach: when an action
needs a page_id and the in-memory cache doesn't have one, the row
handler queries Notion with a primary-key filter (one HTTP call per
unique missing PK). New rows POST and cache the returned page_id;
unchanged rows never need a query because cocoindex's tracking
records short-circuit reconcile before the sink runs. Trade-off:
heavier per-miss cost than pre-fetching the whole data source, but
much cheaper than pre-fetch on the common case where most signals
are no-ops between runs.
"""

from __future__ import annotations

import asyncio
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, Literal, NamedTuple
from urllib.parse import urlencode

from typing_extensions import TypeVar

import cocoindex as coco
from cocoindex.connectorkits.fingerprint import fingerprint_object
from cocoindex._internal.context_keys import ContextKey, ContextProvider

from ._client import NotionClient
from ._types import DatabaseSchema, PropType

RowT = TypeVar("RowT")


# ---------------------------------------------------------------------------
# Row-level
# ---------------------------------------------------------------------------

_PageKey = tuple[Any, ...]
_PageValue = dict[str, Any]  # Python field name -> Python value
_PageFingerprint = bytes


class OnDelete(Enum):
    """What to do with a Notion page whose source row is no longer declared."""

    ARCHIVE = "archive"  # PATCH archived=true — reversible (default).
    HARD = "hard"  # DELETE /blocks/{id} — moves to trash, recoverable for 30d.
    IGNORE = "ignore"  # Leave the page alone; tracking record drops only.


class _PageAction(NamedTuple):
    """Action on a single page: upsert (value!=None) or delete (value=None)."""

    key: _PageKey
    value: _PageValue | None


def _property_filter(prop: PropType, value: Any) -> dict[str, Any]:
    """Build a Notion data-source query filter for ``prop == value``.

    Each Notion property type wants a different filter shape; this dispatches
    on the property's ``notion_type``. Only the property types usable as
    primary keys need to be listed here — title (almost always), rich_text,
    url, email, number, date, checkbox, select.
    """
    name = prop.name
    nt = prop.notion_type
    if nt == "title":
        return {"property": name, "title": {"equals": str(value)}}
    if nt == "rich_text":
        return {"property": name, "rich_text": {"equals": str(value)}}
    if nt == "url":
        return {"property": name, "url": {"equals": str(value)}}
    if nt == "email":
        return {"property": name, "email": {"equals": str(value)}}
    if nt == "number":
        return {"property": name, "number": {"equals": float(value)}}
    if nt == "checkbox":
        return {"property": name, "checkbox": {"equals": bool(value)}}
    if nt == "date":
        return {"property": name, "date": {"equals": str(value)}}
    if nt == "select":
        return {"property": name, "select": {"equals": str(value)}}
    raise ValueError(
        f"Notion property type {nt!r} (prop {name!r}) is not supported as a "
        "primary key. Use title / rich_text / url / email / number / date / "
        "checkbox / select instead."
    )


class _RowHandler(coco.TargetHandler[_PageValue, _PageFingerprint]):
    """Handler for each page within a Notion data source."""

    def __init__(
        self,
        client: NotionClient,
        data_source_id: str,
        schema: DatabaseSchema[Any],
        on_delete: OnDelete,
    ) -> None:
        self._client = client
        self._data_source_id = data_source_id
        self._schema = schema
        self._on_delete = on_delete
        self._sink = coco.TargetActionSink[_PageAction, None].from_async_fn(
            self._apply_actions
        )
        # In-memory cache. Values are page_id strings; sentinel _ABSENT marks
        # PKs we've confirmed don't exist in Notion (avoids re-querying).
        self._page_id_cache: dict[_PageKey, str] = {}
        # One lock per PK so concurrent actions for the same key serialize their
        # lookup-or-create, but actions for different keys still run in parallel.
        self._key_locks: dict[_PageKey, asyncio.Lock] = {}
        self._key_locks_lock = asyncio.Lock()

    async def _lock_for(self, key: _PageKey) -> asyncio.Lock:
        async with self._key_locks_lock:
            lock = self._key_locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._key_locks[key] = lock
            return lock

    async def _resolve_page_id(self, key: _PageKey) -> str | None:
        """Return the page_id for ``key``, querying Notion on cache miss.

        Returns ``None`` if no page with this primary key exists in the data
        source. The caller decides whether to POST a new page in that case.
        """
        cached = self._page_id_cache.get(key)
        if cached is not None:
            return cached
        pk_filter = self._build_pk_filter(key)
        res = await self._client.query_data_source(
            self._data_source_id, filter=pk_filter, page_size=1
        )
        results = res.get("results", [])
        if not results:
            return None
        page_id: str = results[0]["id"]
        self._page_id_cache[key] = page_id
        return page_id

    def _build_pk_filter(self, key: _PageKey) -> dict[str, Any]:
        pk_fields = self._schema.primary_key
        prop_by_field = self._schema.properties_by_field
        terms = [
            _property_filter(prop_by_field[field], key[i])
            for i, field in enumerate(pk_fields)
        ]
        return terms[0] if len(terms) == 1 else {"and": terms}

    async def _apply_actions(
        self,
        context_provider: ContextProvider,
        actions: Sequence[_PageAction],
    ) -> None:
        if not actions:
            return

        async def handle(action: _PageAction) -> None:
            lock = await self._lock_for(action.key)
            async with lock:
                existing_id = await self._resolve_page_id(action.key)

                if action.value is None:
                    if existing_id is None:
                        return
                    if self._on_delete is OnDelete.IGNORE:
                        return
                    if self._on_delete is OnDelete.HARD:
                        await self._client.delete_page(existing_id)
                    else:
                        await self._client.archive_page(existing_id)
                    self._page_id_cache.pop(action.key, None)
                    return

                properties = self._schema.encode_row(action.value)
                if existing_id:
                    await self._client.update_page_properties(existing_id, properties)
                else:
                    created = await self._client.create_page(
                        self._data_source_id, properties
                    )
                    self._page_id_cache[action.key] = created["id"]

        await asyncio.gather(*(handle(a) for a in actions))

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _PageValue | coco.NonExistenceType,
        prev_possible_records: Collection[_PageFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_PageAction, _PageFingerprint] | None:
        # Coerce StableKey -> _PageKey
        if not isinstance(key, tuple):
            page_key: _PageKey = (key,)
        else:
            page_key = key

        if coco.is_non_existence(desired_state):
            if not prev_possible_records and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_PageAction(key=page_key, value=None),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        target_fp = fingerprint_object(desired_state)
        if (
            not prev_may_be_missing
            and prev_possible_records
            and all(prev == target_fp for prev in prev_possible_records)
        ):
            return None

        return coco.TargetReconcileOutput(
            action=_PageAction(key=page_key, value=desired_state),
            sink=self._sink,
            tracking_record=target_fp,
        )


# ---------------------------------------------------------------------------
# Database-level (parent)
# ---------------------------------------------------------------------------


ManagedBy = Literal["user", "system"]


@dataclass(frozen=True)
class _DatabaseSpec:
    """User-declared spec for a Notion database (data source) target.

    In ``user`` mode, ``data_source_id`` identifies a pre-existing data source.
    In ``system`` mode, the connector creates (and additively evolves) the data
    source under ``parent_page_id`` or ``parent_database_id`` with ``title``.
    """

    schema: DatabaseSchema[Any]
    on_delete: OnDelete = OnDelete.ARCHIVE
    managed_by: ManagedBy = "user"
    # user mode:
    data_source_id: str | None = None
    # system mode:
    parent_page_id: str | None = None
    parent_database_id: str | None = None
    title: str | None = None
    allow_destructive: bool = False


class _DatabaseKey(NamedTuple):
    """Stable identity for a database target across runs.

    For user mode: ``identity`` is the data_source_id.
    For system mode: ``identity`` is ``"system:<parent_id>:<title>"`` —
    derived from the (immutable, user-supplied) inputs so cocoindex tracking
    records survive even before the data source has been created.
    """

    client_key: str  # ContextKey.key for the NotionClient
    identity: str


class _DatabaseTracking(NamedTuple):
    """Persisted tracking record for a database target."""

    identity: str


class _DatabaseAction(NamedTuple):
    """Parent-level action: ensure (spec set) or teardown (spec=NON_EXISTENCE)."""

    key: _DatabaseKey
    spec: _DatabaseSpec | coco.NonExistenceType


def _system_identity(spec: _DatabaseSpec) -> str:
    """Stable string identity for a system-managed target."""
    parent = spec.parent_page_id or spec.parent_database_id or ""
    return f"system:{parent}:{spec.title or ''}"


def _read_notion_title(title_array: list[dict[str, Any]] | None) -> str:
    """Extract plain text from a Notion ``title`` rich-text array."""
    return "".join(p.get("plain_text", "") for p in (title_array or []))


async def _find_or_create_data_source(client: NotionClient, spec: _DatabaseSpec) -> str:
    """Resolve the data_source_id for a system-managed target.

    Strategy: look under the parent for a database / data source with
    matching title. If found, reuse it. If not, create it with the declared
    schema. Returns the data_source_id.
    """
    assert spec.managed_by == "system"
    assert spec.title is not None

    if spec.parent_page_id is not None:
        # Notion pagination docs, checked 2026-06-01:
        # GET list endpoints return at most 100 results and require passing
        # next_cursor as start_cursor to continue.
        cursor: str | None = None
        while True:
            params = {"page_size": "100"}
            if cursor is not None:
                params["start_cursor"] = cursor
            children = await client._request(
                "GET",
                f"/blocks/{spec.parent_page_id}/children?{urlencode(params)}",
            )
            for child in children.get("results", []):
                if child.get("type") != "child_database":
                    continue
                try:
                    db = await client.get_database(child["id"])
                except Exception:
                    continue
                if _read_notion_title(db.get("title")) == spec.title:
                    data_sources = db.get("data_sources") or []
                    if data_sources:
                        ds_id: str = data_sources[0]["id"]
                        return ds_id
            if not children.get("has_more"):
                break
            next_cursor = children.get("next_cursor")
            if next_cursor is None:
                break
            cursor = str(next_cursor)
        # Not found — create.
        new_db = await client.create_database(
            parent_page_id=spec.parent_page_id,
            title=spec.title,
            properties=spec.schema.to_notion_properties(),
        )
        created_id: str = (new_db.get("data_sources") or [{}])[0]["id"]
        return created_id

    if spec.parent_database_id is not None:
        # Look for an existing data source with matching name on the database.
        db = await client.get_database(spec.parent_database_id)
        for ds_info in db.get("data_sources") or []:
            if ds_info.get("name") == spec.title:
                existing_id: str = ds_info["id"]
                return existing_id
        new_ds = await client.create_data_source(
            parent_database_id=spec.parent_database_id,
            title=spec.title,
            properties=spec.schema.to_notion_properties(),
        )
        new_id: str = new_ds["id"]
        return new_id

    raise ValueError(
        "managed_by='system' requires parent_page_id or parent_database_id."
    )


async def _evolve_schema_if_needed(
    client: NotionClient, data_source_id: str, spec: _DatabaseSpec
) -> None:
    """For system mode: PATCH-add any properties the dataclass declares that
    aren't yet on the live data source. Destructive changes (type mismatches)
    are rejected unless ``allow_destructive=True``.
    """
    ds = await client.get_data_source(data_source_id)
    notion_props = ds.get("properties") or {}
    missing, type_mismatch = spec.schema.diff_against(notion_props)
    prop_by_name = spec.schema.properties_by_notion_name

    # Notion data-source property docs, checked 2026-06-01:
    # every data source requires exactly one title property, and its type
    # cannot be changed or added/removed through schema PATCHes.
    missing_title = [
        name for name in missing if prop_by_name[name].notion_type == "title"
    ]
    mismatched_title = [
        name
        for name, declared, actual in type_mismatch
        if declared == "title" or actual == "title"
    ]
    if missing_title or mismatched_title:
        raise ValueError(
            f"{spec.schema.record_type.__name__}: Notion title property cannot "
            "be added or type-changed via the API. Edit the data source title "
            "property in Notion's UI to match the declared schema."
        )

    if type_mismatch and not spec.allow_destructive:
        details = ", ".join(
            f"{name!r} declared {declared!r} but Notion has {actual!r}"
            for name, declared, actual in type_mismatch
        )
        raise ValueError(
            f"{spec.schema.record_type.__name__}: destructive schema change "
            f"rejected ({details}). Either edit the schema in Notion's UI to "
            "match, or pass allow_destructive=True to apply the change."
        )

    updates: dict[str, dict[str, Any] | None] = {
        name: prop_by_name[name].to_notion_schema() for name in missing
    }
    if spec.allow_destructive:
        # Notion update-data-source docs, checked 2026-06-01:
        # property type changes are PATCHed by sending the new type schema.
        # Not all conversions are accepted by Notion; surface that API error.
        for name, _, _ in type_mismatch:
            updates[name] = prop_by_name[name].to_notion_schema()
    if updates:
        await client.update_data_source_properties(data_source_id, updates)


async def _apply_database_actions(
    context_provider: ContextProvider,
    actions: Sequence[_DatabaseAction],
) -> list[coco.ChildTargetDef[_RowHandler] | None]:
    outputs: list[coco.ChildTargetDef[_RowHandler] | None] = [None] * len(actions)

    for i, action in enumerate(actions):
        if coco.is_non_existence(action.spec):
            # Target un-mounted. We don't touch the data source itself — even
            # in system mode, the user may want to recover the data. Pages
            # declared via this target on prior runs are not archived here
            # because we drop the child handler; keep the target declared and
            # stop declaring rows individually for that.
            outputs[i] = None
            continue

        spec = action.spec
        client = context_provider.get(action.key.client_key, NotionClient)

        # Resolve the data source. In user mode, it's spec.data_source_id and
        # must already exist. In system mode, we find-or-create it.
        if spec.managed_by == "user":
            assert spec.data_source_id is not None
            data_source_id = spec.data_source_id
            try:
                ds = await client.get_data_source(data_source_id)
            except Exception as e:
                raise RuntimeError(
                    f"Notion data source {data_source_id!r} is not "
                    "accessible. Confirm the integration is shared with the "
                    "containing page (top-right ··· → Connections)."
                ) from e
            spec.schema.validate_against(ds.get("properties") or {})
        else:
            data_source_id = await _find_or_create_data_source(client, spec)
            await _evolve_schema_if_needed(client, data_source_id, spec)

        outputs[i] = coco.ChildTargetDef(
            handler=_RowHandler(
                client=client,
                data_source_id=data_source_id,
                schema=spec.schema,
                on_delete=spec.on_delete,
            )
        )

    return outputs


_database_action_sink = coco.TargetActionSink[
    _DatabaseAction, _RowHandler
].from_async_fn(_apply_database_actions)


class _DatabaseHandler(
    coco.TargetHandler[_DatabaseSpec, _DatabaseTracking, _RowHandler]
):
    """Parent handler — owns the data source identity across runs."""

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _DatabaseSpec | coco.NonExistenceType,
        prev_possible_records: Collection[_DatabaseTracking],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[_DatabaseAction, _DatabaseTracking, _RowHandler]
        | None
    ):
        # StableKey -> _DatabaseKey
        if isinstance(key, tuple) and len(key) == 2:
            db_key = _DatabaseKey(client_key=str(key[0]), identity=str(key[1]))
        elif isinstance(key, _DatabaseKey):
            db_key = key
        else:
            raise TypeError(
                f"_DatabaseHandler: expected _DatabaseKey, got {type(key).__name__}"
            )

        if coco.is_non_existence(desired_state):
            return coco.TargetReconcileOutput(
                action=_DatabaseAction(key=db_key, spec=coco.NON_EXISTENCE),
                sink=_database_action_sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        tracking = _DatabaseTracking(identity=db_key.identity)
        return coco.TargetReconcileOutput(
            action=_DatabaseAction(key=db_key, spec=desired_state),
            sink=_database_action_sink,
            tracking_record=tracking,
        )


_database_provider = coco.register_root_target_states_provider(
    "cocoindex/notion/database", _DatabaseHandler()
)


# ---------------------------------------------------------------------------
# User-facing API
# ---------------------------------------------------------------------------


class DatabaseTarget(
    Generic[RowT, coco.MaybePendingS],
    coco.ResolvesTo["DatabaseTarget[RowT]"],
):
    """A target for writing rows to a Notion database (data source).

    Acquired via :func:`mount_database_target` or :func:`declare_database_target`.
    Call :meth:`declare_row` for each row to upsert; rows declared in a previous
    run but not in this run are automatically archived (subject to ``on_delete``).
    """

    _provider: "coco.TargetStateProvider[_PageValue, None, coco.MaybePendingS]"
    _schema: DatabaseSchema[RowT]

    def __init__(
        self,
        provider: "coco.TargetStateProvider[_PageValue, None, coco.MaybePendingS]",
        schema: DatabaseSchema[RowT],
    ) -> None:
        self._provider = provider
        self._schema = schema

    def declare_row(self: "DatabaseTarget[RowT]", *, row: RowT) -> None:
        """Declare a row to be upserted to this database.

        ``row`` is a dataclass / NamedTuple / dict / Pydantic model bound to
        the :class:`DatabaseSchema`. Primary-key fields must be set.
        """
        # PK tuple
        if isinstance(row, dict):
            pk_values: _PageKey = tuple(row.get(pk) for pk in self._schema.primary_key)
        else:
            pk_values = tuple(getattr(row, pk) for pk in self._schema.primary_key)

        # Field-name -> Python value (the row itself, frozen as a dict)
        value: _PageValue = {}
        for field_name, _ in self._schema.properties:
            if isinstance(row, dict):
                value[field_name] = row.get(field_name)
            else:
                value[field_name] = getattr(row, field_name, None)

        coco.declare_target_state(self._provider.target_state(pk_values, value))

    def __coco_memo_key__(self) -> str:
        key: str = self._provider.memo_key
        return key


def _build_spec(
    data_source_id: str | None,
    schema: DatabaseSchema[RowT],
    on_delete: OnDelete,
    managed_by: ManagedBy,
    parent_page_id: str | None,
    parent_database_id: str | None,
    title: str | None,
    allow_destructive: bool,
) -> tuple[str, _DatabaseSpec]:
    """Validate the combination of args and return ``(identity, spec)``.

    User mode requires ``data_source_id`` and forbids the system-mode kwargs.
    System mode requires ``title`` plus exactly one of ``parent_page_id`` /
    ``parent_database_id``.
    """
    if managed_by == "user":
        if data_source_id is None:
            raise ValueError(
                "managed_by='user' requires data_source_id (the existing "
                "Notion data source's ID)."
            )
        if any(v is not None for v in (parent_page_id, parent_database_id, title)):
            raise ValueError(
                "parent_page_id / parent_database_id / title are only valid "
                "with managed_by='system'."
            )
        return data_source_id, _DatabaseSpec(
            schema=schema,
            on_delete=on_delete,
            managed_by="user",
            data_source_id=data_source_id,
        )

    # managed_by == "system"
    if data_source_id is not None:
        raise ValueError(
            "managed_by='system' creates the data source; don't pass "
            "data_source_id. Use parent_page_id or parent_database_id + title."
        )
    if title is None:
        raise ValueError("managed_by='system' requires title.")
    if (parent_page_id is None) == (parent_database_id is None):
        raise ValueError(
            "managed_by='system' requires exactly one of parent_page_id or "
            "parent_database_id."
        )
    spec = _DatabaseSpec(
        schema=schema,
        on_delete=on_delete,
        managed_by="system",
        parent_page_id=parent_page_id,
        parent_database_id=parent_database_id,
        title=title,
        allow_destructive=allow_destructive,
    )
    return _system_identity(spec), spec


def database_target(
    client: ContextKey[NotionClient],
    data_source_id: str | None = None,
    schema: DatabaseSchema[RowT] | None = None,
    *,
    managed_by: ManagedBy = "user",
    parent_page_id: str | None = None,
    parent_database_id: str | None = None,
    title: str | None = None,
    on_delete: OnDelete = OnDelete.ARCHIVE,
    allow_destructive: bool = False,
) -> "coco.TargetState[_RowHandler]":
    """Create a TargetState for a Notion database target. Prefer the
    :func:`mount_database_target` wrapper.
    """
    if schema is None:
        raise ValueError("schema is required.")
    identity, spec = _build_spec(
        data_source_id,
        schema,
        on_delete,
        managed_by,
        parent_page_id,
        parent_database_id,
        title,
        allow_destructive,
    )
    key = _DatabaseKey(client_key=client.key, identity=identity)
    return _database_provider.target_state(key, spec)


def declare_database_target(
    client: ContextKey[NotionClient],
    data_source_id: str | None = None,
    schema: DatabaseSchema[RowT] | None = None,
    *,
    managed_by: ManagedBy = "user",
    parent_page_id: str | None = None,
    parent_database_id: str | None = None,
    title: str | None = None,
    on_delete: OnDelete = OnDelete.ARCHIVE,
    allow_destructive: bool = False,
) -> "DatabaseTarget[RowT, coco.PendingS]":
    """Declare a database target and return a ready-to-declare DatabaseTarget."""
    if schema is None:
        raise ValueError("schema is required.")
    provider = coco.declare_target_state_with_child(
        database_target(
            client,
            data_source_id,
            schema,
            managed_by=managed_by,
            parent_page_id=parent_page_id,
            parent_database_id=parent_database_id,
            title=title,
            on_delete=on_delete,
            allow_destructive=allow_destructive,
        )
    )
    return DatabaseTarget(provider, schema)


async def mount_database_target(
    client: ContextKey[NotionClient],
    data_source_id: str | None = None,
    schema: DatabaseSchema[RowT] | None = None,
    *,
    managed_by: ManagedBy = "user",
    parent_page_id: str | None = None,
    parent_database_id: str | None = None,
    title: str | None = None,
    on_delete: OnDelete = OnDelete.ARCHIVE,
    allow_destructive: bool = False,
) -> "DatabaseTarget[RowT]":
    """Mount a Notion database target and return a ready-to-use DatabaseTarget.

    Two modes:

    - ``managed_by="user"`` (default): pass an existing ``data_source_id``.
      The connector validates that the live property schema matches.
    - ``managed_by="system"``: pass ``parent_page_id`` or
      ``parent_database_id`` plus ``title``. The connector creates the data
      source on first run if it doesn't exist, and PATCH-adds new properties
      on subsequent runs when the dataclass grows. Destructive changes
      (existing property type changed) are rejected unless
      ``allow_destructive=True``.
    """
    if schema is None:
        raise ValueError("schema is required.")
    provider = await coco.mount_target(
        database_target(
            client,
            data_source_id,
            schema,
            managed_by=managed_by,
            parent_page_id=parent_page_id,
            parent_database_id=parent_database_id,
            title=title,
            on_delete=on_delete,
            allow_destructive=allow_destructive,
        )
    )
    return DatabaseTarget(provider, schema)


__all__ = [
    "DatabaseTarget",
    "ManagedBy",
    "OnDelete",
    "database_target",
    "declare_database_target",
    "mount_database_target",
]
