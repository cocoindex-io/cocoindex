"""Module with deps='v1' for external-dependency change detection testing.

The function body is IDENTICAL in deps_v1 and deps_v2 — only the value
declared via the ``deps=`` parameter on ``@coco.fn`` differs.
"""

import cocoindex as coco
from tests.common.target_states import Metrics

_metrics: Metrics | None = None

_PROMPT = "deps_v1_prompt"


def set_metrics(metrics: Metrics) -> None:
    global _metrics
    _metrics = metrics


@coco.fn(memo=True, deps=_PROMPT)
def transform_memo_deps(key: str, value: str) -> str:
    assert _metrics is not None
    _metrics.increment("transform_memo_deps")
    return f"deps: {value}"
