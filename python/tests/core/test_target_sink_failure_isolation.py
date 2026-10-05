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
from typing import Collection, Mapping, Sequence

import cocoindex as coco

from tests import common

_NUM_ITEMS = 16

_failed_paths: list[str] = []


def _record_failure(exc: BaseException, ctx: coco.ExceptionContext) -> None:
    _failed_paths.append(ctx.stable_path)


coco_env = common.create_test_env(__file__, exception_handler=_record_failure)

# (item, value)
_Action = tuple[int, str]


class _RunState:
    """Per-run input and observations, shared across threads."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        # Whether the sink merges and poisons a batch in this run: it holds its
        # first batch until every other item is queued behind it, and poisons
        # an item of the first merged batch.
        self.poison_merged_batch = False
        self.poison_item: int | None = None
        self.store: dict[int, str] = {}
        self.batches: list[list[_Action]] = []
        # Each item's child provider, with its memo key when declared.
        self.child_providers: dict[
            int, tuple[coco.PendingTargetStateProvider[None, None], str]
        ] = {}
        self.gate_taken = False

    def reset_run(self, poison_merged_batch: bool) -> None:
        with self.lock:
            self.poison_merged_batch = poison_merged_batch
            self.poison_item = None
            self.batches.clear()
            self.child_providers.clear()
            self.gate_taken = False


_run = _RunState()


def _committed_items() -> set[int]:
    """Items whose action the engine has handed to the sink.

    The engine assigns an item's child provider its generation, which shows in
    the provider's memo key, only once the precommit that declared the item
    has committed, and queues the item's action for its sink right after, with
    nothing to await in between. The state store may retry a precommit (LMDB
    re-runs a whole write batch after growing its map), and each attempt runs
    ``reconcile``, so counting ``reconcile`` calls can run ahead of the commit;
    this can't.
    """
    with _run.lock:
        watched = list(_run.child_providers.items())
    return {
        item
        for item, (provider, declared_key) in watched
        if provider.memo_key != declared_key
    }


async def _wait_until_others_committed(holder: int) -> None:
    """Hold the first batch until every other item's action is queued behind
    it, so they all merge into the next sink call."""
    others = set(range(_NUM_ITEMS)) - {holder}
    deadline = time.monotonic() + 10
    while not others <= _committed_items():
        if time.monotonic() > deadline:
            raise TimeoutError("items did not all commit in time")
        await asyncio.sleep(0.01)


class _ChildHandler(coco.TargetHandler[None, None, None]):
    """Handler for the target states under an item; the test declares none."""

    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: None | coco.NonExistenceType,
        prev_possible_records: Collection[None],
        prev_may_be_missing: bool,
        /,
    ) -> None:
        return None


@dataclass(frozen=True)
class _TransactionalSink:
    """A batch lands wholly or not at all, like a database transaction."""

    db: str

    async def __call__(
        self,
        context_provider: coco.ContextProvider,
        actions: Sequence[_Action],
        child_slots: Mapping[int, coco.ChildSlot[_ChildHandler]],
        /,
    ) -> None:
        batch = list(actions)
        items = [item for item, _ in batch]
        with _run.lock:
            _run.batches.append(batch)
            takes_gate = _run.poison_merged_batch and not _run.gate_taken
            _run.gate_taken = True
            if _run.poison_merged_batch and _run.poison_item is None and len(items) > 1:
                # Choosing the poisoned item from a merged batch, rather than up
                # front, merges its actions with other components' whatever
                # order the items commit in.
                _run.poison_item = items[-1]
            poisoned = _run.poison_item in items
        if takes_gate:
            await _wait_until_others_committed(items[0])
        if poisoned:
            raise ValueError("poisoned batch")
        for slot in child_slots.values():
            slot.fulfill(_ChildHandler())
        with _run.lock:
            for item, value in batch:
                _run.store[item] = value


class _ItemHandler(coco.TargetHandler[str, str, _ChildHandler]):
    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: str | coco.NonExistenceType,
        prev_possible_records: Collection[str],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_Action, str, _ChildHandler] | None:
        assert isinstance(key, int)
        if coco.is_non_existence(desired_target_state):
            return None
        # Nothing is declared under an item, so an unchanged item needs no
        # action to fulfill its child provider.
        if not prev_may_be_missing and all(
            prev == desired_target_state for prev in prev_possible_records
        ):
            return None
        return coco.TargetReconcileOutput(
            action=(key, desired_target_state),
            sink=coco.TargetActionSink.from_async_fn_with_children(
                _TransactionalSink("db")
            ),
            tracking_record=desired_target_state,
        )


_provider = coco.register_root_target_states_provider(
    "test_target_sink_failure_isolation/items", _ItemHandler()
)


@coco.fn
def _process_item(item: int) -> None:
    # An item carries a child provider only for its generation, the signal the
    # sink's gate waits for (see `_committed_items`).
    children = coco.declare_target_state_with_child(
        _provider.target_state(item, f"v{item}")
    )
    with _run.lock:
        _run.child_providers[item] = (children, children.memo_key)


async def _root() -> None:
    await coco.mount_each(
        coco.component_subpath("item"),
        _process_item,
        [(i, i) for i in range(_NUM_ITEMS)],
    )


def test_merged_batch_failure_is_confined_to_the_failing_component() -> None:
    # One app for both runs: the second run must see the first run's tracking
    # records, and a second `App` with the same name would collide with the
    # first while it is still registered.
    app = coco.App(
        coco.AppConfig(name="test_target_sink_failure_isolation", environment=coco_env),
        _root,
    )

    _run.reset_run(poison_merged_batch=True)
    _failed_paths.clear()
    app.update_blocking()

    # The sink poisons an item only in a merged batch, so there was one.
    poison = _run.poison_item
    assert poison is not None, _run.batches
    # Every other component's actions landed, and only the poisoned component
    # failed, even though its actions were merged into a batch with others.
    assert _run.store == {i: f"v{i}" for i in range(_NUM_ITEMS) if i != poison}
    assert _failed_paths == [str(coco.ROOT_PATH / "item" / poison)]

    _run.reset_run(poison_merged_batch=False)
    _failed_paths.clear()
    app.update_blocking()

    assert _run.store == {i: f"v{i}" for i in range(_NUM_ITEMS)}
    assert _failed_paths == []
    # Only the previously failed component had anything left to apply: the
    # others' target states were committed by the first run. Its value is
    # unchanged, so it re-applies only because the engine recorded that its
    # failed apply may not have landed.
    assert _run.batches == [[(poison, f"v{poison}")]]
