"""Module with v2 class method bodies for logic change detection testing.

The method bodies differ from v1 (return "v2: " instead of "v1: ").
"""

import cocoindex as coco
from tests.common.target_states import GlobalDictTarget, Metrics

_metrics: Metrics | None = None


def set_metrics(metrics: Metrics) -> None:
    global _metrics
    _metrics = metrics


class Processor:
    """A class with @coco.fn decorated methods — v2 bodies."""

    @coco.fn(memo=True)
    def transform_memo(self, key: str, value: str) -> str:
        assert _metrics is not None
        _metrics.increment("transform_memo")
        return "v2: " + value

    @coco.fn(memo=True)
    def declare_entry_memo(self, key: str, value: str) -> None:
        assert _metrics is not None
        _metrics.increment("declare_entry_memo")
        coco.declare_target_state(GlobalDictTarget.target_state(key, "v2: " + value))


processor = Processor()
