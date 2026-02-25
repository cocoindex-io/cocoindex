"""Module with version=1 for explicit version change detection testing.

The function body is IDENTICAL in ver1 and ver2 — only the version number differs.
"""

import cocoindex as coco
from tests.common.target_states import Metrics

_metrics: Metrics | None = None


def set_metrics(metrics: Metrics) -> None:
    global _metrics
    _metrics = metrics


@coco.function(memo=True, version=1)
def transform_memo_ver(key: str, value: str) -> str:
    assert _metrics is not None
    _metrics.increment("transform_memo_ver")
    return "ver: " + value
