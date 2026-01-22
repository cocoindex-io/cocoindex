"""Module version without memo=True for testing memoization invalidation."""

import cocoindex as coco
from ..common.target_states import GlobalDictTarget, Metrics

# Shared metrics object to track calls across module reloads.
_metrics: Metrics | None = None


def set_metrics(metrics: Metrics) -> None:
    global _metrics
    _metrics = metrics


@coco.function
def process_entry(scope: coco.Scope, key: str, value: str) -> None:
    assert _metrics is not None
    _metrics.increment("calls")
    coco.declare_target_state(scope, GlobalDictTarget.target_state(key, value))
