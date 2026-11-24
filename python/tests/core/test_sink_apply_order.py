"""Sinks at one provider depth apply in a fixed order: by their providers'
stable keys from the root provider down, whatever order the target states were
declared in. A preview lists a component's actions in that order too."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Collection

import cocoindex as coco
from tests import common

coco_env = common.create_test_env(__file__)

_applied: list[str] = []
_applied_lock = threading.Lock()


@dataclass(frozen=True)
class _RecordingSink:
    provider_name: str

    async def __call__(
        self,
        context_provider: coco.ContextProvider,
        actions: Collection[tuple[str, str]],
        /,
    ) -> None:
        with _applied_lock:
            _applied.extend(name for name, _ in actions)


class _Handler:
    def __init__(self, provider_name: str) -> None:
        self._provider_name = provider_name
        self._sink = coco.TargetActionSink.from_async_fn(_RecordingSink(provider_name))

    def reconcile(
        self,
        key: Any,
        desired_state: Any | coco.NonExistenceType,
        prev_possible_records: Collection[Any],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[Any, Any] | None:
        if coco.is_non_existence(desired_state):
            return None
        return coco.TargetReconcileOutput(
            action=(self._provider_name, desired_state),
            sink=self._sink,
            tracking_record=desired_state,
        )


_NAMES = [f"test_sink_apply_order/{c}" for c in "hcfagbed"]
_providers = [
    coco.register_root_target_states_provider(name, _Handler(name)) for name in _NAMES
]


_run = 0


def _declare() -> None:
    for provider in _providers:
        coco.declare_target_state(provider.target_state("k", f"v{_run}"))


def test_sinks_at_one_depth_apply_in_provider_key_order() -> None:
    global _run
    app = coco.App(
        coco.AppConfig(name="test_sink_apply_order", environment=coco_env),
        _declare,
    )
    # With the sinks in hash-map order, eight runs would almost surely see more
    # than one order.
    for _run in range(8):
        _applied.clear()
        app.update_blocking()
        assert _applied == sorted(_NAMES), f"run {_run}"


def test_a_preview_lists_actions_in_apply_order() -> None:
    global _run
    app = coco.App(
        coco.AppConfig(name="test_sink_apply_order_preview", environment=coco_env),
        _declare,
    )
    for _run in range(8):
        plan = app.update_blocking(preview=True)
        assert plan is not None
        assert [name for name, _ in plan] == sorted(_NAMES), f"run {_run}"
