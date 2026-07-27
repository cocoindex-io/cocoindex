"""
State-store benchmark pipeline.

Mounts N child components. Each runs a memoized function that declares M
target states against a no-op fake target — the engine still writes the
per-target tracking records into the state store (and runs the full
pre_commit / commit lifecycle), but the user-facing sink does nothing,
so we isolate cocoindex-side cost.

`M = 0` reproduces the original "component-path bookkeeping + memo only"
shape (no target-state traffic).

Environment knobs:
    BENCH_N — number of mounted child components (default 100).
    BENCH_M — number of target states declared per component (default 0).
"""

from __future__ import annotations

import os
from typing import Collection

import cocoindex as coco


_N: int = int(os.environ.get("BENCH_N", "100"))
_M: int = int(os.environ.get("BENCH_M", "0"))


class _NoopTargetHandler:
    """Target handler that drives the per-state tracking record through
    pre_commit + commit but does nothing user-visible. `desired_state` is
    `None` for upsert and `NonExistence` for delete; the sink is a no-op so
    we measure cocoindex's own per-target write path without external IO."""

    def __init__(self) -> None:
        self._sink: coco.TargetActionSink[tuple[coco.StableKey, bool], None] = (
            coco.TargetActionSink.from_fn(self._apply)
        )

    @staticmethod
    def _apply(
        _ctx: coco.ContextProvider,
        _actions: Collection[tuple[coco.StableKey, bool]],
        /,
    ) -> None:
        return None

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: None | coco.NonExistenceType,
        _prev_records: Collection[None],
        _prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[tuple[coco.StableKey, bool], None] | None:
        is_delete = coco.is_non_existence(desired_state)
        tracking_record: None | coco.NonExistenceType = (
            coco.NON_EXISTENCE if is_delete else None
        )
        return coco.TargetReconcileOutput(
            action=(key, is_delete),
            sink=self._sink,
            tracking_record=tracking_record,
        )


_noop_provider = coco.register_root_target_states_provider(
    "cocoindex/bench/noop", _NoopTargetHandler()
)


@coco.fn(memo=True)
async def noop_component(idx: int) -> None:
    """Memoized component: declares _M no-op target states. Memo lets warm
    runs short-circuit recomputation; declared target states still flow
    through pre_commit / commit so the target-state write path is
    exercised."""
    for j in range(_M):
        coco.declare_target_state(_noop_provider.target_state(f"{idx}-{j}", None))


@coco.fn
async def app_main() -> None:
    items = [(str(i), i) for i in range(_N)]
    await coco.mount_each(noop_component, items)


app = coco.App("StateStoreBench", app_main)
