"""Test app whose stable paths mix symbol and string keys.

`mount_each` auto-derives its subpath from the function name, so each item
lands at `/@process_files/"<name>"` — the shape `cocoindex show` has to
round-trip through its `STABLE_PATH` argument. One item name contains a `/`,
which only stays a single path part because string keys are quoted.
"""

from __future__ import annotations

import pathlib

import cocoindex as coco
from cocoindex.connectors.localfs import DirTarget, declare_dir_target

_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "cocoindex.db"
OUT_DIR = _HERE / "out_symbol_path"

_FILE_NAMES = ["rfc8259.md", "with/slash.md"]


@coco.fn
def process_files(name: str, target: DirTarget) -> None:
    target.declare_file(name.replace("/", "__"), f"Content for {name}\n")


@coco.fn
async def app_main() -> None:
    dir_target = await coco.use_mount(declare_dir_target, OUT_DIR)
    await coco.mount_each(
        process_files, [(name, name) for name in _FILE_NAMES], dir_target
    )


app = coco.App(
    coco.AppConfig(
        name="SymbolPathApp",
        environment=coco.Environment(coco.Settings.from_env(db_path=DB_PATH)),
    ),
    app_main,
)
