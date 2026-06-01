"""Notion database target.

Two-level structure that mirrors ``connectors.postgres`` /
``connectors.sqlite``:

- ``_DatabaseHandler`` (root) owns the data source. MVP scope is
  ``managed_by="user"``: on first apply it validates the data source is
  reachable AND that the declared property schema (names + Notion types)
  matches the live data source. Schema creation / evolution lands in a
  follow-up (additive ``managed_by="system"``).
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
from typing import Any, Generic, NamedTuple

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


@dataclass(frozen=True)
class _DatabaseSpec:
    """User-declared spec for a Notion database (data source) target."""

    schema: DatabaseSchema[Any]
    on_delete: OnDelete = OnDelete.ARCHIVE


class _DatabaseKey(NamedTuple):
    """Stable identity for a database target across runs."""

    client_key: str  # ContextKey.key for the NotionClient
    data_source_id: str


class _DatabaseTracking(NamedTuple):
    """Persisted tracking record for a database target."""

    data_source_id: str


class _DatabaseAction(NamedTuple):
    """Parent-level action: ensure (spec set) or teardown (spec=NON_EXISTENCE)."""

    key: _DatabaseKey
    spec: _DatabaseSpec | coco.NonExistenceType


async def _apply_database_actions(
    context_provider: ContextProvider,
    actions: Sequence[_DatabaseAction],
) -> list[coco.ChildTargetDef[_RowHandler] | None]:
    outputs: list[coco.ChildTargetDef[_RowHandler] | None] = [None] * len(actions)

    for i, action in enumerate(actions):
        if coco.is_non_existence(action.spec):
            # Database target was un-mounted. Per managed_by="user" semantics,
            # we don't touch the data source itself — that's owned by the user.
            # Known limitation: pages declared on prior runs are NOT archived
            # automatically when the parent target goes away, because we drop
            # the child handler here and the framework has nothing to route
            # the row-level NON_EXISTENCE reconciles through. If you need
            # cleanup, keep the target declared and stop declaring rows
            # individually instead. Future managed_by="system" will own the
            # full lifecycle and handle this.
            outputs[i] = None
            continue

        client = context_provider.get(action.key.client_key, NotionClient)

        # Validate the data source exists and the integration can see it.
        # One GET per database per run — cheap and catches config errors loudly.
        try:
            ds = await client.get_data_source(action.key.data_source_id)
        except Exception as e:
            raise RuntimeError(
                f"Notion data source {action.key.data_source_id!r} is not "
                "accessible. Confirm the integration is shared with the "
                "containing page in Notion (top-right ··· → Connections)."
            ) from e

        # Validate the declared schema against the live data source schema.
        # Catches typos in property names and type mismatches at mount, instead
        # of failing silently at write time (Notion ignores unknown properties).
        action.spec.schema.validate_against(ds.get("properties") or {})

        outputs[i] = coco.ChildTargetDef(
            handler=_RowHandler(
                client=client,
                data_source_id=action.key.data_source_id,
                schema=action.spec.schema,
                on_delete=action.spec.on_delete,
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
            db_key = _DatabaseKey(client_key=str(key[0]), data_source_id=str(key[1]))
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

        tracking = _DatabaseTracking(data_source_id=db_key.data_source_id)
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


def database_target(
    client: ContextKey[NotionClient],
    data_source_id: str,
    schema: DatabaseSchema[RowT],
    *,
    on_delete: OnDelete = OnDelete.ARCHIVE,
) -> "coco.TargetState[_RowHandler]":
    """Create a TargetState for a Notion database target.

    Use with :func:`coco.mount_target` to get a child provider, or use the
    convenience wrappers :func:`declare_database_target` /
    :func:`mount_database_target`.
    """
    key = _DatabaseKey(client_key=client.key, data_source_id=data_source_id)
    spec = _DatabaseSpec(schema=schema, on_delete=on_delete)
    return _database_provider.target_state(key, spec)


def declare_database_target(
    client: ContextKey[NotionClient],
    data_source_id: str,
    schema: DatabaseSchema[RowT],
    *,
    on_delete: OnDelete = OnDelete.ARCHIVE,
) -> "DatabaseTarget[RowT, coco.PendingS]":
    """Declare a database target and return a ready-to-declare DatabaseTarget."""
    provider = coco.declare_target_state_with_child(
        database_target(client, data_source_id, schema, on_delete=on_delete)
    )
    return DatabaseTarget(provider, schema)


async def mount_database_target(
    client: ContextKey[NotionClient],
    data_source_id: str,
    schema: DatabaseSchema[RowT],
    *,
    on_delete: OnDelete = OnDelete.ARCHIVE,
) -> "DatabaseTarget[RowT]":
    """Mount a Notion database target and return a ready-to-use DatabaseTarget.

    Sugar over ``database_target()`` + ``coco.mount_target()``.

    Args:
        client: ContextKey for the :class:`NotionClient` (provided via lifespan).
        data_source_id: Notion data source ID (a UUID, with or without dashes).
        schema: :class:`DatabaseSchema` describing the row class and its mapping
            onto Notion properties.
        on_delete: Strategy for pages whose source row is no longer declared.
    """
    provider = await coco.mount_target(
        database_target(client, data_source_id, schema, on_delete=on_delete)
    )
    return DatabaseTarget(provider, schema)


__all__ = [
    "DatabaseTarget",
    "OnDelete",
    "database_target",
    "declare_database_target",
    "mount_database_target",
]
