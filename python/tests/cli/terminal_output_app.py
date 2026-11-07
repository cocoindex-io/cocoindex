"""Apps for test_terminal_output.py, which runs them in a pseudo-terminal.

Run as a script, this cancels a progress display, then opens a stats group that
asks to report.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import pathlib

import cocoindex as coco

env = coco.Environment(
    coco.Settings.from_env(db_path=pathlib.Path(os.environ["TERMINAL_OUTPUT_DB"]))
)


@coco.fn
async def _item(i: int) -> None:
    await asyncio.sleep(0)


@coco.fn
async def _fail(i: int) -> None:
    raise ValueError(f"boom {i}")


@coco.fn
async def _slow_main() -> None:
    await asyncio.sleep(0.5)


@coco.fn
async def _group_main() -> None:
    with coco.stats_group("LaterGroup", report_to_stdout=True):
        await coco.mount_each(_item, [("a", 1)])


@coco.fn
async def _items_main() -> None:
    await coco.mount_each(_item, [("a", 1)])


@coco.fn
async def _failures_main() -> None:
    await coco.mount_each(_fail, [("a", 1)])


_items_app = coco.App(coco.AppConfig(name="Items", environment=env), _items_main)
_failures_app = coco.App(
    coco.AppConfig(name="Failures", environment=env), _failures_main
)
_slow_app = coco.App(coco.AppConfig(name="Slow", environment=env), _slow_main)
_group_app = coco.App(coco.AppConfig(name="Group", environment=env), _group_main)


async def _cancelled_display_then_group() -> None:
    handle = _slow_app.update()
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(coco.show_progress(handle), timeout=0.1)
    await asyncio.sleep(1)
    await _group_app.update()
    # The group's detached reporter prints its final block.
    await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(_cancelled_display_then_group())
