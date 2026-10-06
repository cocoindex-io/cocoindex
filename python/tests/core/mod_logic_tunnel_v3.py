"""Module v3 for callback-tunnel tests.

``cb2`` and ``acb2`` changed (vs v1). ``cb`` and ``acb`` are identical to v1.
"""

import cocoindex as coco
from tests.common.target_states import Metrics

_metrics: Metrics | None = None


def set_metrics(metrics: Metrics) -> None:
    global _metrics
    _metrics = metrics


@coco.fn
def cb(s: str) -> str:
    assert _metrics is not None
    _metrics.increment("cb")
    return "cb_v1: " + s


@coco.fn
def cb2(s: str) -> str:
    assert _metrics is not None
    _metrics.increment("cb2")
    return "cb2_v3: " + s


@coco.fn
async def acb(s: str) -> str:
    assert _metrics is not None
    _metrics.increment("acb")
    return "acb_v1: " + s


@coco.fn
async def acb2(s: str) -> str:
    assert _metrics is not None
    _metrics.increment("acb2")
    return "acb2_v3: " + s
