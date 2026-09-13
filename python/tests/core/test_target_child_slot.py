"""End-to-end tests for the child-slot contract of ``TargetActionSink``.

A sink built with ``from_fn_with_children`` receives a ``ChildSlot`` for exactly
the actions whose target state was declared with a child provider, keyed by
action index, and must fulfill each exactly once. The engine enforces this
instead of trusting an index-aligned return list, so a batch that mixes
container and leaf actions (one shared sink) carries slots only where a child
provider exists, and a sink built with ``from_fn`` rejects child-bearing actions.
"""

from __future__ import annotations

import threading
from typing import Any, Callable, Collection, Coroutine, Literal, Mapping, Sequence

import pytest

import cocoindex as coco
from tests import common

coco_env = common.create_test_env(__file__)

# (key, exists): the whole action; ``exists`` is False for an orphan delete.
_Action = tuple[str, bool]

_batches: list[tuple[list[_Action], list[int]]] = []
_batches_lock = threading.Lock()
_slot_mode: dict[str, Literal["fulfill", "skip", "double"]] = {"mode": "fulfill"}


class _ChildHandler(coco.TargetHandler[str, None, None]):
    """Handler for the states under a container; never emits actions."""

    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: str | coco.NonExistenceType,
        prev_possible_records: Collection[None],
        prev_may_be_missing: bool,
        /,
    ) -> None:
        return None


def _apply_with_children(
    context_provider: coco.ContextProvider,
    actions: Sequence[_Action],
    child_slots: Mapping[int, coco.ChildSlot[_ChildHandler]],
    /,
) -> None:
    with _batches_lock:
        _batches.append((list(actions), sorted(child_slots)))
    if _slot_mode["mode"] == "skip":
        return
    for slot in child_slots.values():
        slot.fulfill(_ChildHandler())
        if _slot_mode["mode"] == "double":
            slot.fulfill(_ChildHandler())


def _apply_leaf(
    context_provider: coco.ContextProvider, actions: Sequence[_Action], /
) -> None:
    pass


# One sink shared by a container handler and a leaf handler, as a connector
# sharing a per-database sink between table DDL and row writes would.
_shared_sink = coco.TargetActionSink.from_fn_with_children(_apply_with_children)
# A leaf-only sink, wrongly used by a container handler below.
_leaf_sink = coco.TargetActionSink.from_fn(_apply_leaf)


def _reconcile(
    sink: coco.TargetActionSink[_Action],
    key: coco.StableKey,
    desired_target_state: str | coco.NonExistenceType,
    prev_possible_records: Collection[None],
    prev_may_be_missing: bool,
) -> coco.TargetReconcileOutput[_Action, None] | None:
    assert isinstance(key, str)
    if coco.is_non_existence(desired_target_state):
        if not prev_possible_records and not prev_may_be_missing:
            return None
        return coco.TargetReconcileOutput(
            action=(key, False), sink=sink, tracking_record=coco.NON_EXISTENCE
        )
    # A container always emits an action so its child provider gets fulfilled.
    return coco.TargetReconcileOutput(action=(key, True), sink=sink, tracking_record=None)


class _ContainerHandler(coco.TargetHandler[str, None, _ChildHandler]):
    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: str | coco.NonExistenceType,
        prev_possible_records: Collection[None],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_Action, None] | None:
        return _reconcile(
            _shared_sink, key, desired_target_state, prev_possible_records, prev_may_be_missing
        )


class _LeafHandler(coco.TargetHandler[str, None, None]):
    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: str | coco.NonExistenceType,
        prev_possible_records: Collection[None],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_Action, None] | None:
        return _reconcile(
            _shared_sink, key, desired_target_state, prev_possible_records, prev_may_be_missing
        )


class _MisbuiltContainerHandler(coco.TargetHandler[str, None, _ChildHandler]):
    """Container handler whose sink was built with ``from_fn`` (no child slots)."""

    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: str | coco.NonExistenceType,
        prev_possible_records: Collection[None],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_Action, None] | None:
        return _reconcile(
            _leaf_sink, key, desired_target_state, prev_possible_records, prev_may_be_missing
        )


_container_provider = coco.register_root_target_states_provider(
    "test_child_slot/container", _ContainerHandler()
)
_leaf_provider = coco.register_root_target_states_provider(
    "test_child_slot/leaf", _LeafHandler()
)
_misbuilt_provider = coco.register_root_target_states_provider(
    "test_child_slot/misbuilt", _MisbuiltContainerHandler()
)


def _run(name: str, main: Callable[[], Coroutine[Any, Any, None]]) -> None:
    coco.App(coco.AppConfig(name=name, environment=coco_env), main).update_blocking()


def test_child_slots_only_for_child_bearing_actions() -> None:
    _batches.clear()

    async def declare_all() -> None:
        coco.declare_target_state(_leaf_provider.target_state("l1", "v"))
        coco.declare_target_state_with_child(_container_provider.target_state("c1", "v"))
        coco.declare_target_state(_leaf_provider.target_state("l2", "v"))
        coco.declare_target_state_with_child(_container_provider.target_state("c2", "v"))

    _run("test_child_slots_mixed_batch", declare_all)
    assert len(_batches) == 1
    actions, slot_indexes = _batches[0]
    assert {key for key, _ in actions} == {"l1", "l2", "c1", "c2"}
    # Slots exactly for the two container declarations, keyed by their index.
    assert {actions[i] for i in slot_indexes} == {("c1", True), ("c2", True)}

    # Drop one container and one leaf: their orphan deletes carry no slot, the
    # remaining container is re-applied with one.
    _batches.clear()

    async def declare_fewer() -> None:
        coco.declare_target_state(_leaf_provider.target_state("l1", "v"))
        coco.declare_target_state_with_child(_container_provider.target_state("c2", "v"))

    _run("test_child_slots_mixed_batch", declare_fewer)
    assert len(_batches) == 1
    actions, slot_indexes = _batches[0]
    assert set(actions) == {("l1", True), ("l2", False), ("c1", False), ("c2", True)}
    assert [actions[i] for i in slot_indexes] == [("c2", True)]


def test_leaf_sink_rejects_child_bearing_action() -> None:
    async def declare() -> None:
        coco.declare_target_state_with_child(_misbuilt_provider.target_state("c", "v"))

    with pytest.raises(Exception, match="from_fn_with_children"):
        _run("test_child_slot_leaf_sink", declare)


@pytest.mark.parametrize(
    "mode, message",
    [
        ("skip", "did not fulfill the child target slot"),
        ("double", "fulfilled more than once"),
    ],
)
def test_misfulfilled_child_slot_fails(
    mode: Literal["skip", "double"], message: str
) -> None:
    async def declare() -> None:
        coco.declare_target_state_with_child(_container_provider.target_state("c", "v"))

    _slot_mode["mode"] = mode
    try:
        with pytest.raises(Exception, match=message):
            _run(f"test_child_slot_{mode}", declare)
    finally:
        _slot_mode["mode"] = "fulfill"
