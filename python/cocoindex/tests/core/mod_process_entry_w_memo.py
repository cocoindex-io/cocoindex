"""Module version with memo=True for testing memoization invalidation."""

import cocoindex as coco
from ..common.effects import GlobalDictTarget, Metrics

# Shared metrics object to track calls across module reloads.
_metrics: Metrics | None = None


def set_metrics(metrics: Metrics) -> None:
    global _metrics
    _metrics = metrics


@coco.function(memo=True)
def process_entry(scope: coco.Scope, key: str, value: str) -> None:
    assert _metrics is not None
    _metrics.increment("calls")
    coco.declare_effect(scope, GlobalDictTarget.effect(key, value))
