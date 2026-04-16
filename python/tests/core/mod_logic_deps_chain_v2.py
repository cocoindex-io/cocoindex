"""Module v2 for transitive deps-change propagation tests.

Identical to v1 except bar's ``deps=`` value differs. Function bodies for
bar, foo_full, and foo_self are byte-for-byte the same as v1 — the goal is
to isolate the effect of a deps-only change on memo invalidation.
"""

import cocoindex as coco
from tests.common.target_states import Metrics

_metrics: Metrics | None = None

_BAR_PROMPT = "deps_chain_v2_prompt"


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
