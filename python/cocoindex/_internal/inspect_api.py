import asyncio
from typing import Any

from . import core
from .app import AppBase
from .stable_path import StablePath


async def list_stable_paths(app: AppBase[Any, Any]) -> list[StablePath]:
    core_app = await app._get_core()
    return [StablePath(path) for path in core.list_stable_paths(core_app)]


def list_stable_paths_with_types(
    app: AppBase[Any, Any],
) -> list[core.StablePathWithType]:
    """
    Return stable paths along with their node types.

    The returned list includes both:
    - component nodes (node_type=StablePathNodeType.component)
    - intermediate directory nodes (node_type=StablePathNodeType.directory)
    """
    _env, core_app = app._get_core_env_app_sync()
    return core.list_stable_paths_with_types(core_app)


def list_stable_paths_sync(app: AppBase[Any, Any]) -> list[StablePath]:
    return asyncio.run(list_stable_paths(app))


def list_stable_paths_with_types_sync(
    app: AppBase[Any, Any],
) -> list[core.StablePathWithType]:
    """Synchronous wrapper for list_stable_paths_with_types (now just an alias)."""
    return list_stable_paths_with_types(app)


__all__ = [
    "list_stable_paths",
    "list_stable_paths_sync",
    "list_stable_paths_with_types",
    "list_stable_paths_with_types_sync",
]
