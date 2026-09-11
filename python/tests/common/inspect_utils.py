"""Helpers for asserting on engine-internal state in end-to-end tests."""

from __future__ import annotations

import asyncio
from typing import Any

import cocoindex as coco
import cocoindex.inspect as coco_inspect

__all__ = [
    "list_target_state_owners",
    "list_target_state_owners_sync",
]


async def list_target_state_owners(
    app: coco.App[Any, Any],
) -> dict[str, coco.StablePath]:
    """Map each tracked target-state path (readable form) to its owner component path.

    Also asserts the inverted owner index is consistent: every row must resolve
    against its owner component's tracking info. A dangling row is a leak —
    left behind by a component deletion or an interrupted cleanup.
    """
    owners: dict[str, coco.StablePath] = {}
    async for entry in coco_inspect.iter_target_states(app):
        assert not entry.dangling, (
            f"dangling target-state owner row {entry.readable_path} "
            f"owned by {entry.owner_component_path}"
        )
        owners[entry.readable_path] = coco.StablePath(entry.owner_component_path)
    return owners


def list_target_state_owners_sync(
    app: coco.App[Any, Any],
) -> dict[str, coco.StablePath]:
    return asyncio.run(list_target_state_owners(app))
