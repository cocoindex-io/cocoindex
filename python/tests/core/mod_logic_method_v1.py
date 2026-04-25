"""Module with v1 class method bodies for logic change detection testing.

Tests that @coco.fn on class methods correctly participates in logic change
detection when the bound method is used via mount() or called directly.
"""

import cocoindex as coco
from tests.common.target_states import GlobalDictTarget, Metrics

_metrics: Metrics | None = None


def set_metrics(metrics: Metrics) -> None:
    global _metrics
    _metrics = metrics


class Processor:
    """A class with @coco.fn decorated methods."""

    @coco.fn(memo=True)
    def transform_memo(self, key: str, value: str) -> str:
        assert _metrics is not None
        _metrics.increment("transform_memo")
        return "v1: " + value

    @coco.fn(memo=True)
    def declare_entry_memo(self, key: str, value: str) -> None:
        assert _metrics is not None
        _metrics.increment("declare_entry_memo")
        coco.declare_target_state(GlobalDictTarget.target_state(key, "v1: " + value))


processor = Processor()
