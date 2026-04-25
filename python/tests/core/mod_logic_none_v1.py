"""Module v1 for logic_tracking=None tests.

foo_none (memo, logic_tracking=None) calls bar.
Both differ between v1 and v2.
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


@coco.fn(memo=True, logic_tracking=None)
def foo_none(key: str, value: str) -> str:
    assert _metrics is not None
    _metrics.increment("foo_none")
    return bar(value)
