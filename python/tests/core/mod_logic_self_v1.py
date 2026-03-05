"""Module v1 for logic_tracking="self" tests.

foo_self (memo, logic_tracking="self") calls bar.
bar differs between v1/v2. foo_self differs between v1/v3.
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
    return bar(value)
