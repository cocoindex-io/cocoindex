"""End-to-end tests for the child-slot contract of ``TargetActionSink``.

A sink built with ``from_fn_with_children`` receives a ``ChildSlot`` for exactly
the actions whose target state was declared with a child provider, keyed by
action index, and must fulfill each exactly once. The engine enforces this
instead of trusting an index-aligned return list, so a batch that mixes
container and leaf actions (one shared sink) carries slots only where a child
provider exists, and a sink built with ``from_fn`` rejects child-bearing actions.
"""

from __future__ import annotations

import contextlib
import sys
import threading
import typing
import warnings
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
) -> coco.TargetReconcileOutput[_Action, None, Any] | None:
    assert isinstance(key, str)
    if coco.is_non_existence(desired_target_state):
        if not prev_possible_records and not prev_may_be_missing:
            return None
        return coco.TargetReconcileOutput(
            action=(key, False), sink=sink, tracking_record=coco.NON_EXISTENCE
        )
    # A container always emits an action so its child provider gets fulfilled.
    return coco.TargetReconcileOutput(
        action=(key, True), sink=sink, tracking_record=None
    )


class _ContainerHandler(coco.TargetHandler[str, None, _ChildHandler]):
    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: str | coco.NonExistenceType,
        prev_possible_records: Collection[None],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_Action, None, _ChildHandler] | None:
        return _reconcile(
            _shared_sink,
            key,
            desired_target_state,
            prev_possible_records,
            prev_may_be_missing,
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
            _shared_sink,
            key,
            desired_target_state,
            prev_possible_records,
            prev_may_be_missing,
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
    ) -> coco.TargetReconcileOutput[_Action, None, _ChildHandler] | None:
        return _reconcile(
            _leaf_sink,
            key,
            desired_target_state,
            prev_possible_records,
            prev_may_be_missing,
        )


# --- The deprecated pre-slot contract: a `from_fn` sink returning ChildTargetDefs ---

_legacy_child_keys: list[str] = []
_legacy_mode: dict[str, Literal["aligned", "short", "missing"]] = {"mode": "aligned"}


def _record_child_actions(
    context_provider: coco.ContextProvider, actions: Sequence[_Action], /
) -> None:
    with _batches_lock:
        _legacy_child_keys.extend(key for key, _ in actions)


_recording_child_sink = coco.TargetActionSink.from_fn(_record_child_actions)


class _RecordingChildHandler(coco.TargetHandler[str, None, None]):
    """Child handler whose sink records the keys it applies."""

    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: str | coco.NonExistenceType,
        prev_possible_records: Collection[None],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_Action, None] | None:
        return _reconcile(
            _recording_child_sink,
            key,
            desired_target_state,
            prev_possible_records,
            prev_may_be_missing,
        )


def _legacy_apply(
    context_provider: coco.ContextProvider, actions: Sequence[_Action], /
) -> list[coco.ChildTargetDef[_RecordingChildHandler] | None]:
    if _legacy_mode["mode"] == "short":
        return []
    if _legacy_mode["mode"] == "missing":
        return [None] * len(actions)
    return [
        coco.ChildTargetDef(handler=_RecordingChildHandler()) if exists else None
        for _, exists in actions
    ]


# Pre-slot annotations must keep type-checking: two sink type arguments and a
# sink built with `from_fn` whose callback returns child handler definitions.
_legacy_sink: coco.TargetActionSink[_Action, _RecordingChildHandler] = (
    coco.TargetActionSink.from_fn(_legacy_apply)
)


class _LegacyContainerHandler(coco.TargetHandler[str, None, _RecordingChildHandler]):
    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: str | coco.NonExistenceType,
        prev_possible_records: Collection[None],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_Action, None, _RecordingChildHandler] | None:
        return _reconcile(
            _legacy_sink,
            key,
            desired_target_state,
            prev_possible_records,
            prev_may_be_missing,
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
_legacy_provider = coco.register_root_target_states_provider(
    "test_child_slot/legacy", _LegacyContainerHandler()
)


@coco.fn
def _declare_legacy_container(name: str) -> coco.PendingTargetStateProvider[str, None]:
    return coco.declare_target_state_with_child(
        _legacy_provider.target_state(name, "v")
    )


def _run(name: str, main: Callable[[], Coroutine[Any, Any, None]]) -> None:
    coco.App(coco.AppConfig(name=name, environment=coco_env), main).update_blocking()


def _expect_legacy_deprecation() -> contextlib.AbstractContextManager[Any]:
    """Expect the legacy-contract ``DeprecationWarning`` where it is observable.

    The engine emits it from one of its own threads. With context-aware
    warnings (the default on free-threaded builds) a thread without a warnings
    context resolves against the global filters, which ignore
    ``DeprecationWarning`` unless the interpreter was started with ``-W``, so
    ``pytest.warns`` cannot observe it there and only the behaviour is checked.
    """
    if getattr(sys.flags, "context_aware_warnings", 0):
        return contextlib.nullcontext()
    return pytest.warns(DeprecationWarning, match="from_fn_with_children")


def test_child_slots_only_for_child_bearing_actions() -> None:
    _batches.clear()

    async def declare_all() -> None:
        coco.declare_target_state(_leaf_provider.target_state("l1", "v"))
        coco.declare_target_state_with_child(
            _container_provider.target_state("c1", "v")
        )
        coco.declare_target_state(_leaf_provider.target_state("l2", "v"))
        coco.declare_target_state_with_child(
            _container_provider.target_state("c2", "v")
        )

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
        coco.declare_target_state_with_child(
            _container_provider.target_state("c2", "v")
        )

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


def test_legacy_child_defs_still_fulfill_children() -> None:
    _legacy_child_keys.clear()

    async def declare() -> None:
        with coco.component_subpath("legacy"):
            child = await coco.use_mount(
                coco.component_subpath("c"), _declare_legacy_container, "c"
            )
            coco.declare_target_state(child.target_state("row", "v"))

    with _expect_legacy_deprecation():
        _run("test_child_slot_legacy_defs", declare)
    # The child provider was fulfilled from the returned definition, so the
    # child declaration reached the child handler's sink.
    assert _legacy_child_keys == ["row"]


@pytest.mark.parametrize(
    "mode, message",
    [
        ("short", "child handler definitions for"),
        ("missing", "returned no child handler for the action"),
    ],
)
def test_legacy_child_defs_misaligned_fails(
    mode: Literal["short", "missing"], message: str
) -> None:
    async def declare() -> None:
        coco.declare_target_state_with_child(_legacy_provider.target_state("c", "v"))

    _legacy_mode["mode"] = mode
    try:
        with _expect_legacy_deprecation(), pytest.raises(Exception, match=message):
            _run(f"test_child_slot_legacy_{mode}", declare)
    finally:
        _legacy_mode["mode"] = "aligned"


def test_two_argument_sink_type_subscript_is_deprecated() -> None:
    with pytest.warns(DeprecationWarning, match="one type argument"):
        legacy_alias = coco.TargetActionSink[_Action, None]
    assert typing.get_origin(legacy_alias) is coco.TargetActionSink
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        current_alias = coco.TargetActionSink[_Action]
    assert typing.get_args(current_alias) == (_Action, Any)
