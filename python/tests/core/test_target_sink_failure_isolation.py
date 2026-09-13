"""A failing component must not fail the components the engine batched with it.

Target action sinks batch through the engine: while one sink call is in
flight, the actions of every component that finishes meanwhile merge into the
next call. Merging is an optimization, not a transaction boundary between
components. When a merged call fails, the engine retries in smaller batches
along component boundaries, so only the component whose actions fail actually
fails; the others commit as usual.
"""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass
from typing import Any, Collection

import cocoindex as coco

from tests import common

_NUM_ITEMS = 16
_POISON_ITEM = 7

_failed_paths: list[str] = []


def _record_failure(exc: BaseException, ctx: coco.ExceptionContext) -> None:
    _failed_paths.append(ctx.stable_path)


coco_env = common.create_test_env(__file__, exception_handler=_record_failure)


class _Observations:
    """Shared across threads: ``reconcile`` runs on an engine thread."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.store: dict[int, str] = {}
        self.batches: list[list[tuple[int, str]]] = []
        self.num_reconciled = 0
        self.gate_taken = False

    def reset_run(self) -> None:
        with self.lock:
            self.batches.clear()
            self.num_reconciled = 0
            self.gate_taken = False


_obs = _Observations()


async def _wait_until_all_reconciled() -> None:
    """Hold the batcher until every component has reconciled its target state.

    A component reconciles right before handing its actions to the sink, so
    once every component has reconciled (plus a drain period for the last
    precommit to land), every other component's actions are queued behind this
    sink call and will merge into the next batch.
    """
    deadline = time.monotonic() + 10
    while True:
        with _obs.lock:
            if _obs.num_reconciled >= _NUM_ITEMS:
                break
        if time.monotonic() > deadline:
            raise TimeoutError("components did not all reconcile in time")
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.3)


@dataclass(frozen=True)
class _TransactionalSink:
    """A batch lands wholly or not at all, like a database transaction."""

    db: str

    async def __call__(
        self,
        context_provider: coco.ContextProvider,
        actions: Collection[tuple[int, str]],
        /,
    ) -> None:
        batch = list(actions)
        with _obs.lock:
            _obs.batches.append(batch)
            takes_gate = not _obs.gate_taken
            _obs.gate_taken = True
        if takes_gate:
            await _wait_until_all_reconciled()
        if any(value == "poison" for _, value in batch):
            raise ValueError("poisoned batch")
        with _obs.lock:
            for key, value in batch:
                _obs.store[key] = value


class _Handler:
    def reconcile(
        self,
        key: Any,
        desired_state: Any | coco.NonExistenceType,
        prev_possible_records: Collection[Any],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[tuple[int, str], Any] | None:
        with _obs.lock:
            _obs.num_reconciled += 1
        if coco.is_non_existence(desired_state):
            return None
        if not prev_may_be_missing and all(
            prev == desired_state for prev in prev_possible_records
        ):
            return None
        return coco.TargetReconcileOutput(
            action=(key, desired_state),
            sink=coco.TargetActionSink.from_async_fn(_TransactionalSink("db")),
            tracking_record=desired_state,
        )


_provider = coco.register_root_target_states_provider(
    "test_target_sink_failure_isolation/rows", _Handler()
)


@coco.fn
async def _process_item(item: int, poison_item: int | None) -> None:
    if item == poison_item:
        # Finish last, so the poisoned actions never take the gated first sink
        # call but always land in the merged batch queued behind it.
        await asyncio.sleep(0.1)
    value = "poison" if item == poison_item else f"v{item}"
    coco.declare_target_state(_provider.target_state(item, value))


async def _root(poison_item: int | None) -> None:
    await coco.mount_each(
        coco.component_subpath("item"),
        _process_item,
        [(i, i) for i in range(_NUM_ITEMS)],
        poison_item,
    )


def test_merged_batch_failure_is_confined_to_the_failing_component() -> None:
    config = coco.AppConfig(
        name="test_target_sink_failure_isolation", environment=coco_env
    )

    _obs.reset_run()
    _failed_paths.clear()
    coco.App(config, _root, poison_item=_POISON_ITEM).update_blocking()

    # Every other component's actions landed, and only the poisoned component
    # failed, even though its actions were merged into a batch with others.
    assert _obs.store == {i: f"v{i}" for i in range(_NUM_ITEMS) if i != _POISON_ITEM}
    assert _failed_paths == [str(coco.ROOT_PATH / "item" / _POISON_ITEM)]
    merged = [b for b in _obs.batches if len(b) > 1 and (_POISON_ITEM, "poison") in b]
    assert merged, _obs.batches

    _obs.reset_run()
    _failed_paths.clear()
    coco.App(config, _root, poison_item=None).update_blocking()

    assert _obs.store == {i: f"v{i}" for i in range(_NUM_ITEMS)}
    assert _failed_paths == []
    # Only the previously failed component had anything left to apply: the
    # others' target states were committed by the first run.
    assert _obs.batches == [[(_POISON_ITEM, f"v{_POISON_ITEM}")]]
