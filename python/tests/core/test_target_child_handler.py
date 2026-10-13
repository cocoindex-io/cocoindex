"""End-to-end tests for container sinks built with ``child_handler=``.

The engine builds a container's child handlers from the sink's
``child_handler`` factory: before the sink callback in an update, in place of
it in a preview. The callback only applies the actions.
"""

from __future__ import annotations

import threading
from typing import Any, Collection, Mapping, Sequence

import pytest

import cocoindex as coco
from tests import common

coco_env = common.create_test_env(__file__)

# (key, exists): the whole container action; ``exists`` is False for a delete.
_ContainerAction = tuple[str, bool]
# (container key, row key)
_RowAction = tuple[str, str]

_calls: list[str] = []
_calls_lock = threading.Lock()
_mode: dict[str, Any] = {"factory_raises": False, "apply_returns": None}


def _record(call: str) -> None:
    with _calls_lock:
        _calls.append(call)


def _apply_rows(
    context_provider: coco.ContextProvider, actions: Sequence[_RowAction], /
) -> None:
    for container, row in actions:
        _record(f"row:{container}/{row}")


_row_sink = coco.TargetActionSink.from_fn(_apply_rows)


class _RowHandler(coco.TargetHandler[str, str, None]):
    def __init__(self, container: str) -> None:
        self._container = container

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
        return coco.TargetReconcileOutput(
            action=(self._container, key),
            sink=_row_sink,
            tracking_record=desired_target_state,
        )


def _apply_containers(
    context_provider: coco.ContextProvider,
    actions: Sequence[_ContainerAction],
    /,
) -> Any:
    _record("apply:" + ",".join(key for key, _ in actions))
    return _mode["apply_returns"]


def _row_handler(
    context_provider: coco.ContextProvider, action: _ContainerAction, /
) -> _RowHandler:
    key, exists = action
    assert exists, "the factory never sees a delete"
    _record(f"factory:{key}")
    if _mode["factory_raises"]:
        raise ValueError("factory failed")
    return _RowHandler(key)


_container_sink = coco.TargetActionSink.from_fn(
    _apply_containers, child_handler=_row_handler
)


class _ContainerHandler(coco.TargetHandler[str, None, _RowHandler]):
    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: str | coco.NonExistenceType,
        prev_possible_records: Collection[None],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_ContainerAction, None, _RowHandler] | None:
        assert isinstance(key, str)
        if coco.is_non_existence(desired_target_state):
            return coco.TargetReconcileOutput(
                action=(key, False),
                sink=_container_sink,
                tracking_record=coco.NON_EXISTENCE,
            )
        # A container always emits an action: it is what readies its children.
        return coco.TargetReconcileOutput(
            action=(key, True), sink=_container_sink, tracking_record=None
        )


_containers = coco.register_root_target_states_provider(
    "test_target_child_handler/containers", _ContainerHandler()
)

_rows: dict[str, list[str]] = {}


async def _declare_containers() -> None:
    for container, rows in _rows.items():
        provider = await coco.mount_target(_containers.target_state(container, "spec"))
        for row in rows:
            coco.declare_target_state(provider.target_state(row, f"{row}-value"))


@pytest.fixture(autouse=True)
def _reset() -> Any:
    _calls.clear()
    _rows.clear()
    _mode.update(factory_raises=False, apply_returns=None)
    yield


def _app(name: str) -> coco.App[[], None]:
    return coco.App(
        coco.AppConfig(name=name, environment=coco_env), _declare_containers
    )


def test_factory_runs_in_place_of_apply_in_preview_and_before_it_in_update() -> None:
    _rows["c"] = ["r1", "r2"]
    app = _app("test_child_handler_modes")

    actions = app.update_blocking(preview=True)
    assert actions is not None
    assert sorted(map(repr, actions)) == sorted(
        map(repr, [("c", True), ("c", "r1"), ("c", "r2")])
    )
    assert _calls == ["factory:c"]

    _calls.clear()
    app.update_blocking()
    assert _calls[:2] == ["factory:c", "apply:c"]
    assert sorted(_calls[2:]) == ["row:c/r1", "row:c/r2"]


def test_factory_error_fails_the_component_before_apply() -> None:
    _rows["c"] = ["r1"]
    _mode["factory_raises"] = True
    app = _app("test_child_handler_factory_error")

    with pytest.raises(Exception, match="factory failed"):
        app.update_blocking()
    assert _calls == ["factory:c"]


def test_child_handler_sink_returning_a_value_fails() -> None:
    _rows["c"] = ["r1"]
    _mode["apply_returns"] = [coco.ChildTargetDef(handler=_RowHandler("c"))]
    app = _app("test_child_handler_returns_value")

    with pytest.raises(Exception, match="must return `None`"):
        app.update_blocking()


def test_declare_target_state_with_child_rejects_non_existence() -> None:
    async def _declare() -> None:
        coco.declare_target_state_with_child(
            # The misuse under test: no value to declare children under.
            _containers.target_state("c", coco.NON_EXISTENCE)  # type: ignore[arg-type]
        )

    app = coco.App(
        coco.AppConfig(name="test_child_handler_non_existence", environment=coco_env),
        _declare,
    )
    with pytest.raises(Exception, match="needs a value"):
        app.update_blocking()


def _apply_with_slots(
    context_provider: coco.ContextProvider,
    actions: Sequence[_ContainerAction],
    child_slots: Mapping[int, coco.ChildSlot[_RowHandler]],
    /,
) -> None:
    for i, (key, _) in enumerate(actions):
        if i in child_slots:
            child_slots[i].fulfill(_RowHandler(key))


def test_slot_sink_is_deprecated_and_cannot_be_previewed() -> None:
    with pytest.warns(DeprecationWarning, match="from_fn_with_children"):
        slot_sink = coco.TargetActionSink.from_fn_with_children(_apply_with_slots)

    class _SlotContainerHandler(coco.TargetHandler[str, None, _RowHandler]):
        def reconcile(
            self,
            key: coco.StableKey,
            desired_target_state: str | coco.NonExistenceType,
            prev_possible_records: Collection[None],
            prev_may_be_missing: bool,
            /,
        ) -> coco.TargetReconcileOutput[_ContainerAction, None, _RowHandler] | None:
            assert isinstance(key, str)
            return coco.TargetReconcileOutput(
                action=(key, True), sink=slot_sink, tracking_record=None
            )

    slot_containers = coco.register_root_target_states_provider(
        "test_target_child_handler/slot_containers", _SlotContainerHandler()
    )

    async def _declare() -> None:
        provider = await coco.mount_target(slot_containers.target_state("c", "spec"))
        coco.declare_target_state(provider.target_state("r1", "v"))

    app = coco.App(
        coco.AppConfig(name="test_child_handler_slot_preview", environment=coco_env),
        _declare,
    )
    with pytest.raises(Exception, match="does not support preview of child target"):
        app.update_blocking(preview=True)
    app.update_blocking()


def test_preview_keeps_container_actions_with_equal_tracking_records() -> None:
    """An existence-only record cannot prove an emitted action has no effect."""
    applied: dict[str, str] = {}
    desired = "v1"

    def apply(
        context_provider: coco.ContextProvider, actions: Sequence[tuple[str, str]], /
    ) -> None:
        applied.update(actions)

    def child_handler(
        context_provider: coco.ContextProvider, action: tuple[str, str], /
    ) -> _RowHandler:
        return _RowHandler(action[0])

    sink = coco.TargetActionSink.from_fn(apply, child_handler=child_handler)

    class Container(coco.TargetHandler[str, None, _RowHandler]):
        def reconcile(
            self,
            key: coco.StableKey,
            value: str | coco.NonExistenceType,
            prev: Collection[None],
            missing: bool,
            /,
        ) -> coco.TargetReconcileOutput[tuple[str, str], None, _RowHandler] | None:
            assert isinstance(key, str)
            if coco.is_non_existence(value):
                return None
            return coco.TargetReconcileOutput(
                action=(key, value), sink=sink, tracking_record=None
            )

    provider = coco.register_root_target_states_provider(
        "test_target_child_handler/existence_only", Container()
    )

    async def main() -> None:
        await coco.mount_target(provider.target_state("c", desired))

    app = coco.App(
        coco.AppConfig(name="preview_equal_container_record", environment=coco_env),
        main,
    )
    app.update_blocking()
    desired = "v2"
    assert app.update_blocking(preview=True) == [("c", "v2")]
    assert applied == {"c": "v1"}
    app.update_blocking()
    assert applied == {"c": "v2"}
