from __future__ import annotations

import os
import threading
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Generic,
    ParamSpec,
    TypeVar,
    overload,
)

from . import core
from .environment import Environment, LazyEnvironment, _default_env
from .function import AnyCallable, AsyncCallable, create_core_component_processor
from .serde import fn_ret_deserializer
from .update_stats import (
    ComponentStats,
    UpdateSnapshot,
    UpdateStats,
    UpdateStatus,
)


P = ParamSpec("P")
R = TypeVar("R")

_TERMINATED_VERSION = 2**64 - 1  # u64::MAX

_ENV_MAX_INFLIGHT_COMPONENTS = "COCOINDEX_MAX_INFLIGHT_COMPONENTS"
_DEFAULT_MAX_INFLIGHT_COMPONENTS = 1024


class UpdateHandle(Generic[R]):
    """Handle for a running or completed update, providing access to stats and results.

    The handle is also ``Awaitable[R]``, so ``result = await app.update()`` works
    for backward compatibility.
    """

    def __init__(
        self,
        init_coro: Any,  # Coroutine that returns core.UpdateHandle
        main_fn: Any = None,
    ) -> None:
        self._init_coro = init_coro
        self._core_handle: core.UpdateHandle | None = None
        self._main_fn = main_fn  # used for return type inspection

    async def _ensure_started(self) -> core.UpdateHandle:
        if self._core_handle is None:
            self._core_handle = await self._init_coro
            self._init_coro = None
        return self._core_handle

    @staticmethod
    def _make_update_stats(raw: dict[str, dict[str, int]]) -> UpdateStats:
        by_component = {name: ComponentStats(**group) for name, group in raw.items()}
        return UpdateStats(by_component=by_component)

    def _snapshot_from_handle(
        self,
        handle: core.UpdateHandle,
    ) -> tuple[int, UpdateStats | None]:
        """Returns (version, stats). Stats is None if empty."""
        version, raw = handle.stats_snapshot()
        if not raw:
            return version, None
        return version, self._make_update_stats(raw)

    def stats(self) -> UpdateStats | None:
        """Returns a snapshot of the latest stats, or None if not yet started."""
        if self._core_handle is None:
            return None
        _, stats = self._snapshot_from_handle(self._core_handle)
        return stats

    async def watch(self) -> AsyncIterator[UpdateSnapshot[R]]:
        """Async iterator that yields progress snapshots.

        Yields UpdateSnapshot with status RUNNING while the update is in progress,
        then a final DONE snapshot with the result. On error, raises the exception
        directly from the iterator.
        """
        handle = await self._ensure_started()
        last_version = 0
        while True:
            version = await handle.changed()

            # Check termination before dedup — notify_terminated() sends
            # TERMINATED_VERSION on the watch channel without updating the
            # stats version, so the dedup check would skip it.
            if version >= _TERMINATED_VERSION:
                snap_version, stats = self._snapshot_from_handle(handle)
                pyvalue: Any = await handle.result()
                result: R = pyvalue.get(fn_ret_deserializer(self._main_fn))
                if stats is not None:
                    yield UpdateSnapshot(
                        stats=stats, status=UpdateStatus.DONE, result=result
                    )
                return

            # Snapshot the actual stats (version may differ from notification)
            snap_version, stats = self._snapshot_from_handle(handle)

            if snap_version == last_version:
                continue  # no actual change since last yield
            last_version = snap_version

            if stats is not None:
                yield UpdateSnapshot(
                    stats=stats, status=UpdateStatus.RUNNING, result=None
                )

    async def result(self) -> R:
        """Await the update result. Raises on error."""
        handle = await self._ensure_started()
        pyvalue: Any = await handle.result()
        return pyvalue.get(fn_ret_deserializer(self._main_fn))  # type: ignore[no-any-return]

    def __await__(self) -> Any:
        return self.result().__await__()


@dataclass(frozen=True)
class AppConfig:
    name: str
    environment: Environment | LazyEnvironment = _default_env
    max_inflight_components: int | None = None


class App(Generic[P, R]):
    """Unified App class with both async and sync methods."""

    _name: str
    _main_fn: AnyCallable[P, R]
    _app_args: tuple[Any, ...]
    _app_kwargs: dict[str, Any]
    _environment: Environment | LazyEnvironment

    _lock: threading.Lock
    _core_env_app: tuple[Environment, core.App] | None

    @overload
    def __init__(
        self,
        name_or_config: str | AppConfig,
        main_fn: AsyncCallable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None: ...
    @overload
    def __init__(
        self,
        name_or_config: str | AppConfig,
        main_fn: Callable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None: ...
    def __init__(
        self,
        name_or_config: str | AppConfig,
        main_fn: Any,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        if isinstance(name_or_config, str):
            config = AppConfig(name=name_or_config)
        else:
            config = name_or_config

        self._name = config.name
        self._main_fn = main_fn
        self._app_args = tuple(args)
        self._app_kwargs = dict(kwargs)
        self._environment = config.environment

        max_inflight = config.max_inflight_components
        if max_inflight is None:
            env_val = os.environ.get(_ENV_MAX_INFLIGHT_COMPONENTS)
            if env_val is not None:
                max_inflight = int(env_val)
            else:
                max_inflight = _DEFAULT_MAX_INFLIGHT_COMPONENTS
        self._max_inflight_components = max_inflight

        self._lock = threading.Lock()
        self._core_env_app = None

        # Register this app with its environment's info
        config.environment._info.register_app(self._name, self)

    async def _get_core_env_app(self) -> tuple[Environment, core.App]:
        with self._lock:
            if self._core_env_app is not None:
                return self._core_env_app
        env = await self._environment._get_env()
        return self._ensure_core_env_app(env)

    def _get_core_env_app_sync(self) -> tuple[Environment, core.App]:
        with self._lock:
            if self._core_env_app is not None:
                return self._core_env_app
        env = self._environment._get_env_sync()
        return self._ensure_core_env_app(env)

    async def _get_core(self) -> core.App:
        _env, core_app = await self._get_core_env_app()
        return core_app

    def _ensure_core_env_app(self, env: Environment) -> tuple[Environment, core.App]:
        with self._lock:
            if self._core_env_app is None:
                self._core_env_app = (
                    env,
                    core.App(self._name, env._core_env, self._max_inflight_components),
                )
            return self._core_env_app

    def update(
        self,
        *,
        report_to_stdout: bool = False,
        full_reprocess: bool = False,
        live: bool = False,
    ) -> UpdateHandle[R]:
        """
        Start an update and return a handle for tracking progress and awaiting the result.

        The handle is ``Awaitable[R]``, so ``result = await app.update()`` works
        for backward compatibility.

        Args:
            report_to_stdout: If True, periodically report processing stats to stdout.
            full_reprocess: If True, reprocess everything and invalidate existing caches.
            live: If True, run in live mode (live components continue processing
                after mark_ready).

        Returns:
            An UpdateHandle that provides access to stats(), watch(), and result().
        """

        async def _init() -> core.UpdateHandle:
            env, core_app = await self._get_core_env_app()
            root_path = core.StablePath()
            processor = create_core_component_processor(
                self._main_fn, env, root_path, self._app_args, self._app_kwargs
            )
            return core_app.update_async(
                processor,
                report_to_stdout=report_to_stdout,
                full_reprocess=full_reprocess,
                live=live,
                host_ctx=env._context_provider,
            )

        return UpdateHandle(_init(), main_fn=self._main_fn)

    def update_blocking(
        self,
        *,
        report_to_stdout: bool = False,
        full_reprocess: bool = False,
        live: bool = False,
    ) -> R:
        """
        Update the app synchronously (run the app once to process all pending changes).

        Args:
            report_to_stdout: If True, periodically report processing stats to stdout.
            full_reprocess: If True, reprocess everything and invalidate existing caches.
            live: If True, run in live mode (live components continue processing
                after mark_ready).

        Returns:
            The result of the main function.
        """
        env, core_app = self._get_core_env_app_sync()
        root_path = core.StablePath()
        processor = create_core_component_processor(
            self._main_fn, env, root_path, self._app_args, self._app_kwargs
        )
        pyvalue: Any = core_app.update(
            processor,
            report_to_stdout=report_to_stdout,
            full_reprocess=full_reprocess,
            live=live,
            host_ctx=env._context_provider,
        )
        return pyvalue.get(fn_ret_deserializer(self._main_fn))  # type: ignore[no-any-return]

    async def drop(self, *, report_to_stdout: bool = False) -> None:
        """
        Drop the app asynchronously, reverting all its target states and clearing its database.

        This will:
        - Delete all target states created by the app (e.g., drop tables, delete rows)
        - Clear the app's internal state database

        Args:
            report_to_stdout: If True, periodically report processing stats to stdout.
        """
        env, core_app = await self._get_core_env_app()
        await core_app.drop_async(
            report_to_stdout=report_to_stdout, host_ctx=env._context_provider
        )

    def drop_blocking(self, *, report_to_stdout: bool = False) -> None:
        """
        Drop the app synchronously, reverting all its target states and clearing its database.

        This will:
        - Delete all target states created by the app (e.g., drop tables, delete rows)
        - Clear the app's internal state database

        Args:
            report_to_stdout: If True, periodically report processing stats to stdout.
        """
        env, core_app = self._get_core_env_app_sync()
        core_app.drop(report_to_stdout=report_to_stdout, host_ctx=env._context_provider)
