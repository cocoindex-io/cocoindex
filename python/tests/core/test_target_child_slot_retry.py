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
_POISON_TABLE = 3

_failed_paths: list[str] = []


def _record_failure(exc: BaseException, ctx: coco.ExceptionContext) -> None:
    _failed_paths.append(ctx.stable_path)


coco_env = common.create_test_env(__file__, exception_handler=_record_failure)

# (table id, value); the value "poison" makes the sink reject the batch.
_TableAction = tuple[int, str]
# (table id, row key, value)
_RowAction = tuple[int, str, str]


class _RunState:
    """Per-run input and observations, shared across threads."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.poison_table: int | None = None
        self.tables: dict[int, str] = {}
        self.rows: dict[tuple[int, str], str] = {}
        self.table_batches: list[list[_TableAction]] = []
        self.num_reconciled = 0
        self.gate_taken = False

    def reset_run(self, poison_table: int | None) -> None:
        with self.lock:
            self.poison_table = poison_table
            self.table_batches.clear()
            self.num_reconciled = 0
            self.gate_taken = False


_run = _RunState()


async def _wait_until_all_reconciled() -> None:
    """Hold the first table batch until every table has reconciled, so the
    other tables' actions merge into the batch queued behind it."""
    deadline = time.monotonic() + 10
    while True:
        with _run.lock:
            if _run.num_reconciled >= _NUM_TABLES:
                break
        if time.monotonic() > deadline:
            raise TimeoutError("tables did not all reconcile in time")
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.3)


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
    batch = list(actions)
    with _run.lock:
        _run.table_batches.append(batch)
        takes_gate = not _run.gate_taken
        _run.gate_taken = True
    if takes_gate:
        await _wait_until_all_reconciled()
    # Fulfill before failing: a failed attempt leaves its slots fulfilled, and
    # the retry must be able to fulfill them again.
    for i, (table, _) in enumerate(batch):
        child_slots[i].fulfill(_RowHandler(table))
    if any(value == "poison" for _, value in batch):
        raise ValueError("poisoned batch")
    with _run.lock:
        for table, value in batch:
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
        with _run.lock:
            _run.num_reconciled += 1
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
async def _process_table(table: int) -> None:
    poisoned = table == _run.poison_table
    if poisoned:
        # Finish last, so the poisoned actions never take the gated first sink
        # call but always land in the merged batch queued behind it.
        await asyncio.sleep(0.1)
    value = "poison" if poisoned else f"v{table}"
    rows = await coco.mount_target(_table_provider.target_state(table, value))
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

    _run.reset_run(poison_table=_POISON_TABLE)
    _failed_paths.clear()
    app.update_blocking()

    survivors = {i for i in range(_NUM_TABLES) if i != _POISON_TABLE}
    merged = [
        b for b in _run.table_batches if len(b) > 1 and (_POISON_TABLE, "poison") in b
    ]
    assert merged, _run.table_batches
    assert _run.tables == {i: f"v{i}" for i in survivors}
    # Every surviving table's child provider resolved from the retry, so its
    # row reached the row sink; only the poisoned table failed.
    assert _run.rows == {(i, f"row{i}"): "x" for i in survivors}
    assert _failed_paths == [str(coco.ROOT_PATH / "table" / _POISON_TABLE)]

    _run.reset_run(poison_table=None)
    _failed_paths.clear()
    app.update_blocking()

    assert _run.tables == {i: f"v{i}" for i in range(_NUM_TABLES)}
    assert _run.rows == {(i, f"row{i}"): "x" for i in range(_NUM_TABLES)}
    assert _failed_paths == []
