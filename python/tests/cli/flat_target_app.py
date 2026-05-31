"""Test app with flat/leaf target states only (no child providers)."""

from __future__ import annotations

import pathlib
from typing import Any, Collection

import cocoindex as coco

_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "cocoindex.db"

env = coco.Environment(coco.Settings.from_env(db_path=DB_PATH))


class _FlatStore:
    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    def _sink(
        self,
        context_provider: coco.ContextProvider,
        actions: Collection[tuple[str, Any | coco.NonExistenceType]],
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
        desired_state: Any | coco.NonExistenceType,
        prev_possible_records: Collection[Any],
        prev_may_be_missing: bool,
    ) -> (
        coco.TargetReconcileOutput[tuple[str, Any | coco.NonExistenceType], Any] | None
    ):
        assert isinstance(key, str)
        return coco.TargetReconcileOutput(
            action=(key, desired_state),
            sink=coco.TargetActionSink.from_fn(self._sink),
            tracking_record=desired_state,
        )


_flat_store = _FlatStore()
_provider = coco.register_root_target_states_provider(
    "test_cli/flat_preview", _flat_store
)


@coco.fn
def build() -> None:
    coco.declare_target_state(_provider.target_state("x", 42))


app = coco.App(coco.AppConfig(name="FlatPreviewApp", environment=env), build)
