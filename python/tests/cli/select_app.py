"""Test module for `cocoindex update --select`: one output file per input file."""

from __future__ import annotations

import pathlib
from typing import Iterator

import cocoindex as coco
from cocoindex.connectors import localfs


@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex_select.db")
    yield


@coco.fn
async def process(file: localfs.File, target: localfs.DirTarget) -> None:
    name = file.file_path.path.name
    target.declare_file(name, (await file.read_text()).upper())


@coco.fn
async def app_main() -> None:
    target = await coco.use_mount(
        coco.component_subpath("setup"),
        localfs.declare_dir_target,
        pathlib.Path("./out_select"),
    )
    files = localfs.walk_dir(pathlib.Path("./in_select"))
    # No subpath: the items mount under the Symbol `process`.
    await coco.mount_each(process, files.items(), target)


app = coco.App(coco.AppConfig(name="SelectApp"), app_main)
