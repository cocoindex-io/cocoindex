"""Test app with flat/leaf target states only (no child providers)."""

from __future__ import annotations

import pathlib

import cocoindex as coco
from tests.cli.target_store import FlatTargetStore

_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "cocoindex.db"

env = coco.Environment(coco.Settings.from_env(db_path=DB_PATH))


_flat_store = FlatTargetStore()
_provider = coco.register_root_target_states_provider(
    "test_cli/flat_preview", _flat_store
)


@coco.fn
def build() -> None:
    coco.declare_target_state(_provider.target_state("x", 42))


app = coco.App(coco.AppConfig(name="FlatPreviewApp", environment=env), build)
