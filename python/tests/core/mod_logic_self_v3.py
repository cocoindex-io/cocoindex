"""Module v3 for logic_tracking="self" tests.

foo_self itself changed (vs v1). bar same as v1.
"""

import cocoindex as coco
from tests.common.target_states import Metrics

_metrics: Metrics | None = None


def set_metrics(metrics: Metrics) -> None:
    global _metrics
    _metrics = metrics


@coco.fn
def bar(s: str) -> str:
    assert _metrics is not None
    _metrics.increment("bar")
    return "bar_v1: " + s


@coco.fn(memo=True, logic_tracking="self")
def foo_self(key: str, value: str) -> str:
    assert _metrics is not None
    _metrics.increment("foo_self")
    return bar(value) + ""
