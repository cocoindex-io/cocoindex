"""Child slots survive the engine's retry of a failed merged batch.

When a sink call merged from several components fails, the engine re-applies
the components' actions in smaller batches (see
``test_target_sink_failure_isolation``). A container sink may already have
fulfilled the child slots of the actions in the failed call, so the engine
resets those slots before re-applying them; the surviving components' child
providers then resolve from the retry, and only the poisoned component fails.
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Collection, Mapping, Sequence

import cocoindex as coco

from tests import common

_NUM_TABLES = 8

_failed_paths: list[str] = []


def _record_failure(exc: BaseException, ctx: coco.ExceptionContext) -> None:
    _failed_paths.append(ctx.stable_path)


coco_env = common.create_test_env(__file__, exception_handler=_record_failure)

# (table id, value)
_TableAction = tuple[int, str]
# (table id, row key, value)
_RowAction = tuple[int, str, str]


class _RunState:
    """Per-run input and observations, shared across threads."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        # Whether the sink poisons a table of the first merged table batch in
        # this run, and the table it poisoned.
        self.poison_merged_batch = False
        self.poison_table: int | None = None
        self.tables: dict[int, str] = {}
        self.rows: dict[tuple[int, str], str] = {}
        self.table_batches: list[list[int]] = []
        # Each table's child provider, with its memo key when declared.
        self.child_providers: dict[
            int, tuple[coco.PendingTargetStateProvider[str, None], str]
        ] = {}
        self.gate_taken = False

    def reset_run(self, poison_merged_batch: bool) -> None:
        with self.lock:
            self.poison_merged_batch = poison_merged_batch
            self.poison_table = None
            self.table_batches.clear()
            self.child_providers.clear()
            self.gate_taken = False


_run = _RunState()


def _committed_tables() -> set[int]:
    """Tables whose action the engine has handed to the table sink.

    The engine assigns a container's child provider its generation, which
    shows in the provider's memo key, only once the precommit that declared the
    container has committed, and queues the container's action for its sink
    right after, with nothing to await in between. The state store may retry a
    precommit on a write conflict, and each attempt runs ``reconcile``, so
    counting ``reconcile`` calls can run ahead of the commit; this can't.
    """
    with _run.lock:
        watched = list(_run.child_providers.items())
    return {
        table
        for table, (provider, declared_key) in watched
        if provider.memo_key != declared_key
    }


async def _wait_until_others_committed(holder: int) -> None:
    """Hold the first table batch until every other table's action is queued
    behind it, so they all merge into the next sink call."""
    others = set(range(_NUM_TABLES)) - {holder}
    deadline = time.monotonic() + 10
    while not others <= _committed_tables():
        if time.monotonic() > deadline:
            raise TimeoutError("tables did not all commit in time")
        await asyncio.sleep(0.01)


async def _apply_rows(
    context_provider: coco.ContextProvider, actions: Sequence[_RowAction], /
) -> None:
    with _run.lock:
        for table, key, value in actions:
            _run.rows[(table, key)] = value


_row_sink = coco.TargetActionSink.from_async_fn(_apply_rows)


class _RowHandler(coco.TargetHandler[str, str, None]):
    def __init__(self, table: int) -> None:
        self._table = table

    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: str | coco.NonExistenceType,
        prev_possible_records: Collection[str],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_RowAction, str] | None:
        assert isinstance(key, str)
        if coco.is_non_existence(desired_target_state):
            return None
        if not prev_may_be_missing and all(
            prev == desired_target_state for prev in prev_possible_records
        ):
            return None
        return coco.TargetReconcileOutput(
            action=(self._table, key, desired_target_state),
            sink=_row_sink,
            tracking_record=desired_target_state,
        )


async def _apply_tables(
    context_provider: coco.ContextProvider,
    actions: Sequence[_TableAction],
    child_slots: Mapping[int, coco.ChildSlot[_RowHandler]],
    /,
) -> None:
    tables = [table for table, _ in actions]
    with _run.lock:
        _run.table_batches.append(tables)
        takes_gate = not _run.gate_taken
        _run.gate_taken = True
        if _run.poison_merged_batch and _run.poison_table is None and len(tables) > 1:
            # Choosing the poisoned table from a merged batch, rather than up
            # front, merges its actions with other components' whatever order
            # the tables commit in.
            _run.poison_table = tables[-1]
        poisoned = _run.poison_table in tables
    if takes_gate:
        await _wait_until_others_committed(tables[0])
    # Fulfill before failing: a failed attempt leaves its slots fulfilled, and
    # the retry must be able to fulfill them again.
    for i, table in enumerate(tables):
        child_slots[i].fulfill(_RowHandler(table))
    if poisoned:
        raise ValueError("poisoned batch")
    with _run.lock:
        for table, value in actions:
            _run.tables[table] = value


_table_sink = coco.TargetActionSink.from_async_fn_with_children(_apply_tables)


class _TableHandler(coco.TargetHandler[str, None, _RowHandler]):
    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: str | coco.NonExistenceType,
        prev_possible_records: Collection[None],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_TableAction, None, _RowHandler] | None:
        assert isinstance(key, int)
        if coco.is_non_existence(desired_target_state):
            return None
        # A container always emits an action so its child provider is fulfilled.
        return coco.TargetReconcileOutput(
            action=(key, desired_target_state), sink=_table_sink, tracking_record=None
        )


_table_provider = coco.register_root_target_states_provider(
    "test_target_child_slot_retry/tables", _TableHandler()
)


@coco.fn
def _declare_table(table: int) -> coco.PendingTargetStateProvider[str, None]:
    rows = coco.declare_target_state_with_child(
        _table_provider.target_state(table, f"v{table}")
    )
    with _run.lock:
        _run.child_providers[table] = (rows, rows.memo_key)
    return rows


@coco.fn
async def _process_table(table: int) -> None:
    rows = await coco.use_mount(_declare_table, table)
    coco.declare_target_state(rows.target_state(f"row{table}", "x"))


async def _root() -> None:
    await coco.mount_each(
        coco.component_subpath("table"),
        _process_table,
        [(i, i) for i in range(_NUM_TABLES)],
    )


def test_child_slots_are_fulfilled_by_the_retry_of_a_failed_merged_batch() -> None:
    app = coco.App(
        coco.AppConfig(name="test_target_child_slot_retry", environment=coco_env),
        _root,
    )

    _run.reset_run(poison_merged_batch=True)
    _failed_paths.clear()
    app.update_blocking()

    # The sink poisons a table only in a merged batch, so there was one.
    poison = _run.poison_table
    assert poison is not None, _run.table_batches
    survivors = {i for i in range(_NUM_TABLES) if i != poison}
    assert _run.tables == {i: f"v{i}" for i in survivors}
    # The survivors merged with the poisoned table had their child slots
    # fulfilled again by the retry, so every survivor's row reached the row
    # sink; only the poisoned table failed.
    assert _run.rows == {(i, f"row{i}"): "x" for i in survivors}
    assert _failed_paths == [str(coco.ROOT_PATH / "table" / poison)]

    _run.reset_run(poison_merged_batch=False)
    _failed_paths.clear()
    app.update_blocking()

    assert _run.tables == {i: f"v{i}" for i in range(_NUM_TABLES)}
    assert _run.rows == {(i, f"row{i}"): "x" for i in range(_NUM_TABLES)}
    assert _failed_paths == []
