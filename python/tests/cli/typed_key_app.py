"""Test app whose component paths and target-state keys use tuple/bytes keys.

These segments ride through the state store as MessagePack; a codec that
collapsed a tuple to bytes (or bytes to a string) broke owner resolution, which
`show --target-states` surfaces as a `[dangling]` marker and a `#fingerprint`
path instead of a readable one.
"""

from __future__ import annotations

import pathlib

import cocoindex as coco
from tests.cli.target_store import FlatTargetStore

_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "cocoindex.db"

env = coco.Environment(coco.Settings.from_env(db_path=DB_PATH))

_SEGMENTS: list[coco.StableKey] = [(1, 2), b"abc"]


_store = FlatTargetStore()
_provider = coco.register_root_target_states_provider("test_cli/typed_keys", _store)


@coco.fn
def _declare(segment: coco.StableKey) -> None:
    coco.declare_target_state(_provider.target_state(segment, 42))


@coco.fn
async def build() -> None:
    with coco.component_subpath("process"):
        for segment in _SEGMENTS:
            await coco.mount(coco.component_subpath(segment), _declare, segment)


app = coco.App(coco.AppConfig(name="TypedKeyApp", environment=env), build)
