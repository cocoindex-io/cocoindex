"""Module v1 for ``coco.logic_tracked`` tests.

Only the callbacks live here; the traverse / higher-level / owner functions are
defined in ``test_logic_tracked.py`` and never change. Across versions:
v2 edits ``cb`` and ``acb``; v3 edits ``cb2`` and ``acb2``. Everything else is
identical.
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
    return "cb2_v1: " + s


@coco.fn
async def acb(s: str) -> str:
    assert _metrics is not None
    _metrics.increment("acb")
    return "acb_v1: " + s


@coco.fn
async def acb2(s: str) -> str:
    assert _metrics is not None
    _metrics.increment("acb2")
    return "acb2_v1: " + s
