"""Module v2 for ``coco.logic_tracked`` tests.

``cb`` and ``acb`` changed (vs v1). ``cb2`` and ``acb2`` are identical to v1.
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
    return "cb_v2: " + s


@coco.fn
def cb2(s: str) -> str:
    assert _metrics is not None
    _metrics.increment("cb2")
    return "cb2_v1: " + s


@coco.fn
async def acb(s: str) -> str:
    assert _metrics is not None
    _metrics.increment("acb")
    return "acb_v2: " + s


@coco.fn
async def acb2(s: str) -> str:
    assert _metrics is not None
    _metrics.increment("acb2")
    return "acb2_v1: " + s
