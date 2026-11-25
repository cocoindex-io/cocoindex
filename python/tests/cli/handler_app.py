"""Test module with apps whose failing components run under an exception handler."""

from __future__ import annotations

import pathlib

import cocoindex as coco

_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "cocoindex_handler.db"

env = coco.Environment(coco.Settings.from_env(db_path=DB_PATH))


@coco.fn
async def boom(i: int) -> None:
    raise ValueError(f"item {i} is poison")


def _swallow(exc: BaseException, ctx: object) -> None:
    """Return normally: take ownership of the failure."""


def _reraise(exc: BaseException, ctx: object) -> None:
    raise exc


@coco.fn
async def swallowing_main() -> None:
    async with coco.exception_handler(_swallow):
        for i in range(3):
            await coco.mount(coco.component_subpath("boom", str(i)), boom, i)


@coco.fn
async def reraising_main() -> None:
    async with coco.exception_handler(_reraise):
        for i in range(3):
            await coco.mount(coco.component_subpath("boom", str(i)), boom, i)


swallowing_app = coco.App(
    coco.AppConfig(name="SwallowingHandlerApp", environment=env), swallowing_main
)
reraising_app = coco.App(
    coco.AppConfig(name="ReraisingHandlerApp", environment=env), reraising_main
)
