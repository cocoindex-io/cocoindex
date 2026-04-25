"""Module v2 for logic_tracking=None tests.

Both foo_none and bar changed (vs v1).
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
    return "bar_v2: " + s


@coco.fn(memo=True, logic_tracking=None)
def foo_none(key: str, value: str) -> str:
    assert _metrics is not None
    _metrics.increment("foo_none")
    return bar(value) + ""
