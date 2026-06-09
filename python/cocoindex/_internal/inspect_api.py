import asyncio
from typing import Any, AsyncIterator

from . import core
from .app import App
from .environment import Environment
from .stable_path import StablePath


async def list_stable_paths(app: App[Any, Any]) -> list[StablePath]:
    """Convenience: collect paths from iter_stable_paths (delegates to new API)."""
    return [StablePath(item.path) async for item in iter_stable_paths(app)]


async def iter_stable_paths(
    app: App[Any, Any],
) -> AsyncIterator[core.StablePathInfo]:
    """
    Async iterator of stable paths with metadata (e.g. node type; no buffering).

    Yields both:
    - component nodes (node_type=StablePathNodeType.component)
    - intermediate directory nodes (node_type=StablePathNodeType.directory)
    """
    core_app = await app._get_core()
    async for item in core.iter_stable_paths(core_app):
        yield item


async def iter_stable_paths_by_name(
    env: Environment,
    app_name: str,
) -> AsyncIterator[core.StablePathInfo]:
    """
    Async iterator of stable paths for an app identified by name in a given environment.

    Like :func:`iter_stable_paths`, but does not require a full ``App`` object —
    only an :class:`Environment` and the app name.
    """
    async for item in core.iter_stable_paths_by_name(env._core_env, app_name):
        yield item


def list_stable_paths_sync(app: App[Any, Any]) -> list[StablePath]:
    return asyncio.run(list_stable_paths(app))


async def _iter_stable_paths_collected(
    app: App[Any, Any],
) -> list[core.StablePathInfo]:
    return [item async for item in iter_stable_paths(app)]


def list_stable_paths_info_sync(
    app: App[Any, Any],
) -> list[core.StablePathInfo]:
    return asyncio.run(_iter_stable_paths_collected(app))


async def get_stable_path_detail(
    app: App[Any, Any],
    path: StablePath,
) -> core.StablePathDetail | None:
    """Get detailed information about a single stable path from LMDB."""
    core_app = await app._get_core()
    return core.get_stable_path_detail(core_app, path._core)


async def get_stable_path_detail_by_name(
    env: Environment,
    app_name: str,
    path: StablePath,
) -> core.StablePathDetail | None:
    """Get detailed information about a single stable path from LMDB (by app name)."""
    return core.get_stable_path_detail_by_name(env._core_env, app_name, path._core)


async def query_stable_path_details(
    app: App[Any, Any],
    path: StablePath,
    include_children: bool = False,
    recursive: bool = False,
    include_parents: bool = False,
) -> list[core.StablePathDetail]:
    """Query details for a path with optional children/parents from a live App."""
    core_app = await app._get_core()
    return core.query_stable_path_details(
        core_app, path._core, include_children, recursive, include_parents
    )


async def query_stable_path_details_by_name(
    env: Environment,
    app_name: str,
    path: StablePath,
    include_children: bool = False,
    recursive: bool = False,
    include_parents: bool = False,
) -> list[core.StablePathDetail]:
    """Query details for a path with optional children/parents (by app name)."""
    return core.query_stable_path_details_by_name(
        env._core_env,
        app_name,
        path._core,
        include_children,
        recursive,
        include_parents,
    )


__all__ = [
    "iter_stable_paths",
    "iter_stable_paths_by_name",
    "list_stable_paths",
    "list_stable_paths_info_sync",
    "list_stable_paths_sync",
    "get_stable_path_detail",
    "get_stable_path_detail_by_name",
    "query_stable_path_details",
    "query_stable_path_details_by_name",
]
