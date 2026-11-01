"""Shared in-memory target store for CLI fixture apps."""

from collections.abc import Collection

import cocoindex as coco


class FlatTargetStore:
    """Like tests.common DictTargetStateStore, minus `prev` wrapping so `show`
    output stays plain."""

    def __init__(self) -> None:
        self.data: dict[coco.StableKey, object] = {}

    def _sink(
        self,
        context_provider: coco.ContextProvider,
        actions: Collection[tuple[coco.StableKey, object]],
        /,
    ) -> None:
        for key, value in actions:
            if coco.is_non_existence(value):
                self.data.pop(key, None)
            else:
                self.data[key] = value

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: object,
        prev_possible_records: Collection[object],
        prev_may_be_missing: bool,
    ) -> coco.TargetReconcileOutput[tuple[coco.StableKey, object], object] | None:
        return coco.TargetReconcileOutput(
            action=(key, desired_state),
            sink=coco.TargetActionSink.from_fn(self._sink),
            tracking_record=desired_state,
        )
