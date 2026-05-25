from __future__ import annotations

import asyncio
import threading
from typing import Any, Collection

import cocoindex as coco

from tests import common
from tests.common.target_states import GlobalDictTarget

coco_env = common.create_test_env(__file__)


class _ParentOrderingStore:
    def __init__(self) -> None:
        self.observed_child_ready: list[bool] = []
        self._lock = threading.Lock()

    def clear(self) -> None:
        with self._lock:
            self.observed_child_ready.clear()

    def _sink(
        self,
        context_provider: coco.ContextProvider,
        actions: Collection[tuple[str, Any]],
        /,
    ) -> None:
        del context_provider, actions
        with self._lock:
            self.observed_child_ready.append("child" in GlobalDictTarget.store.data)

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: Any | coco.NonExistenceType,
        prev_possible_records: Collection[Any],
        prev_may_be_missing: bool,
    ) -> coco.TargetReconcileOutput[tuple[str, Any], Any] | None:
        assert isinstance(key, str)
        if coco.is_non_existence(desired_state):
            return None
        if not prev_may_be_missing and all(
            prev == desired_state for prev in prev_possible_records
        ):
            return None
        return coco.TargetReconcileOutput(
            action=(key, desired_state),
            sink=coco.TargetActionSink.from_fn(self._sink),
            tracking_record=desired_state,
        )


_parent_ordering_store = _ParentOrderingStore()
_parent_ordering_provider = coco.register_root_target_states_provider(
    "test_target_state/parent_submit_after_children", _parent_ordering_store
)


@coco.fn
async def _delayed_child_target() -> None:
    await asyncio.sleep(0.05)
    coco.declare_target_state(GlobalDictTarget.target_state("child", "ready"))


async def _parent_with_background_child() -> None:
    await coco.mount(coco.component_subpath("child"), _delayed_child_target)
    coco.declare_target_state(_parent_ordering_provider.target_state("parent", "ready"))


def test_parent_submit_waits_for_background_child_ready() -> None:
    GlobalDictTarget.store.clear()
    _parent_ordering_store.clear()

    app = coco.App(
        coco.AppConfig(
            name="test_parent_submit_waits_for_background_child_ready",
            environment=coco_env,
        ),
        _parent_with_background_child,
    )

    app.update_blocking()

    assert "child" in GlobalDictTarget.store.data
    assert _parent_ordering_store.observed_child_ready == [True]
