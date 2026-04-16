"""Module v1 for transitive deps-change propagation tests.

bar (memo, deps='v1') is called by both foo_full (memo, "full") and
foo_self (memo, "self"). Function bodies are IDENTICAL in v1 and v2 — the
only difference is the value passed to ``deps=`` on bar.
"""

import cocoindex as coco
from tests.common.target_states import Metrics

_metrics: Metrics | None = None

_BAR_PROMPT = "deps_chain_v1_prompt"


def set_metrics(metrics: Metrics) -> None:
    global _metrics
    _metrics = metrics


@coco.fn(memo=True, deps=_BAR_PROMPT)
def bar(s: str) -> str:
    assert _metrics is not None
    _metrics.increment("bar")
    return "bar: " + s


@coco.fn(memo=True, logic_tracking="full")
def foo_full(key: str, value: str) -> str:
    assert _metrics is not None
    _metrics.increment("foo_full")
    return bar(value)


@coco.fn(memo=True, logic_tracking="self")
def foo_self(key: str, value: str) -> str:
    assert _metrics is not None
    _metrics.increment("foo_self")
    return bar(value)
