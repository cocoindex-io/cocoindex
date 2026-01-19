"""Test module with multiple apps to demonstrate app specifier syntax."""

from __future__ import annotations

import pathlib
from typing import AsyncGenerator

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors.localfs import declare_dir_target


_ROOT_PATH = coco.ContextKey[pathlib.Path]("root_path")


@coco_aio.lifespan
async def lifespan(builder: coco_aio.EnvironmentBuilder) -> AsyncGenerator[None]:
    root_path = pathlib.Path(__file__).resolve().parent

    builder.provide(_ROOT_PATH, root_path)
    builder.settings.db_path = root_path / "cocoindex.db"
    yield


@coco.function
def build1(scope: coco.Scope) -> None:
    dir_target = coco.mount_run(
        declare_dir_target,
        scope / "out",
        scope.use(_ROOT_PATH) / "out_multi_1",
        stable_key="out_dir",
        managed_by="system",
    ).result()
    dir_target.declare_file(
        scope, filename="hello.txt", content="Hello from MultiApp1\n"
    )


@coco.function
def build2(scope: coco.Scope) -> None:
    dir_target = coco.mount_run(
        declare_dir_target,
        scope / "out",
        scope.use(_ROOT_PATH) / "out_multi_2",
        stable_key="out_dir",
        managed_by="system",
    ).result()
    dir_target.declare_file(
        scope, filename="world.txt", content="Hello from MultiApp2\n"
    )


# Two apps in the same module
app1 = coco.App(build1, "MultiApp1")
app2 = coco_aio.App(build2, "MultiApp2")

# Default app (what gets run if you don't specify :app_name)
app = app1
