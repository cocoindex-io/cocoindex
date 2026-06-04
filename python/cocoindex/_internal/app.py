from __future__ import annotations

import os
import threading
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Any,
    Callable,
    Generic,
    NamedTuple,
    ParamSpec,
    TypeVar,
    overload,
)

from . import core
from .environment import Environment, LazyEnvironment, _default_env
from .function import (
    AnyCallable,
    AsyncCallable,
    create_core_component_processor,
    fn_ret_deserializer,
)
from .update_stats import (
    UpdateSnapshot,
    UpdateStats,
    UpdateStatus,
    _StatsView,
    _decode_update_stats,
    _resolve_report_to_stdout,
    _TERMINATED_VERSION,
)


P = ParamSpec("P")
R = TypeVar("R")


class _StatsSnapshot(NamedTuple):
    version: int
    ready: bool
    stats: UpdateStats | None


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
        preview: bool = False,
    ) -> None:
        self._init_coro = init_coro
        self._core_handle: core.UpdateHandle | None = None
        self._main_fn = main_fn  # used for return type inspection
        self._preview = preview

    async def _ensure_started(self) -> core.UpdateHandle:
        if self._core_handle is None:
            self._core_handle = await self._init_coro
            self._init_coro = None
        return self._core_handle

    def _snapshot_from_handle(
        self,
        handle: core.UpdateHandle,
    ) -> _StatsSnapshot:
        version, ready, raw = handle.stats_snapshot()
        if not raw:
            return _StatsSnapshot(version, ready, None)
        return _StatsSnapshot(version, ready, _decode_update_stats(raw))

    def stats(self) -> UpdateStats | None:
        """Returns a snapshot of the latest stats, or None if not yet started."""
        if self._core_handle is None:
            return None
        return self._snapshot_from_handle(self._core_handle).stats

    async def watch(self) -> AsyncIterator[UpdateSnapshot[R]]:
        """Async iterator that yields progress snapshots.

        Yields UpdateSnapshot with status:
        - RUNNING while the update is in progress (not yet ready)
        - READY when the root component is ready (initial processing caught up)

        In live mode, after the initial READY, continues yielding RUNNING snapshots
        as stats update from incremental processing. When terminated, yields a final
        READY snapshot with the result set.

        On error, raises the exception directly from the iterator.
        """
        if self._preview:
            raise TypeError("watch() is not supported when preview=True")
        handle = await self._ensure_started()
        last_version = 0
        while True:
            version = await handle.changed()

            # Check termination before dedup — notify_terminated() sends
            # TERMINATED_VERSION on the watch channel without updating the
            # stats version, so the dedup check would skip it.
            if version >= _TERMINATED_VERSION:
                snap = self._snapshot_from_handle(handle)
                pyvalue: Any = await handle.result()
                result: R = pyvalue.get(fn_ret_deserializer(self._main_fn))
                if snap.stats is not None:
                    yield UpdateSnapshot(
                        stats=snap.stats, status=UpdateStatus.READY, result=result
                    )
                return

            # Snapshot the actual stats (version may differ from notification)
            snap = self._snapshot_from_handle(handle)

            if snap.version == last_version:
                continue  # no actual change since last yield
            last_version = snap.version

            if snap.stats is not None:
                status = UpdateStatus.READY if snap.ready else UpdateStatus.RUNNING
                yield UpdateSnapshot(stats=snap.stats, status=status, result=None)

    async def result(self) -> R:
        """Await the update result. Raises on error."""
        handle = await self._ensure_started()
        if self._preview:
            await handle.result()
            return handle.take_preview_actions()  # type: ignore[return-value]
        pyvalue: Any = await handle.result()
        return pyvalue.get(fn_ret_deserializer(self._main_fn))  # type: ignore[no-any-return]

    def __await__(self) -> Any:
        return self.result().__await__()


class DropHandle(_StatsView[core.DropHandle]):
    """Handle for a running or completed drop operation."""

    def __init__(self, core_handle: core.DropHandle) -> None:
        self._core_handle = core_handle

    async def result(self) -> None:
        """Await the drop completion. Raises on error."""
        await self._core_handle.result()

    def __await__(self) -> Any:
        return self.result().__await__()


async def show_progress(
    handle: UpdateHandle[R], *, refresh_interval: timedelta | None = None
) -> R:
    """Run the operation with progress display (async). Consumes the handle.

    ``refresh_interval`` overrides the default refresh interval.
    """
    core_handle = await handle._ensure_started()
    refresh_interval_secs = (
        refresh_interval.total_seconds() if refresh_interval is not None else None
    )
    pyvalue: Any = await core.show_progress(core_handle, refresh_interval_secs)
    return pyvalue.get(fn_ret_deserializer(handle._main_fn))  # type: ignore[no-any-return]


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
        full_reprocess: bool = False,
        live: bool = False,
        preview: bool = False,
    ) -> UpdateHandle[R]:
        """
        Start an update and return a handle for tracking progress and awaiting the result.

        The handle is ``Awaitable[R]``, so ``result = await app.update()`` works
        for backward compatibility.

        Args:
            full_reprocess: If True, reprocess everything and invalidate existing caches.
            live: If True, run in live mode (live components continue processing
                after mark_ready).
            preview: If True, compute target actions without applying them.
                The handle's result will be a list of raw action objects.

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
                full_reprocess=full_reprocess,
                live=live,
                preview=preview,
                host_ctx=env._context_provider,
            )

        return UpdateHandle(_init(), main_fn=self._main_fn, preview=preview)

    def update_blocking(
        self,
        *,
        report_to_stdout: bool | timedelta = False,
        full_reprocess: bool = False,
        live: bool = False,
        preview: bool = False,
    ) -> R | list[Any]:
        """
        Update the app synchronously (run the app once to process all pending changes).

        Args:
            report_to_stdout: If truthy, periodically report processing stats to
                stdout. Pass a ``timedelta`` to set the refresh interval; ``True``
                uses the default interval.
            full_reprocess: If True, reprocess everything and invalidate existing caches.
            live: If True, run in live mode (live components continue processing
                after mark_ready).
            preview: If True, compute target actions without applying them.
                Returns a list of raw action objects instead of the main function result.

        Returns:
            The result of the main function, or a list of actions in preview mode.
        """
        env, core_app = self._get_core_env_app_sync()
        root_path = core.StablePath()
        processor = create_core_component_processor(
            self._main_fn, env, root_path, self._app_args, self._app_kwargs
        )
        report, refresh_interval_secs = _resolve_report_to_stdout(report_to_stdout)
        pyvalue: Any = core_app.update(
            processor,
            full_reprocess=full_reprocess,
            host_ctx=env._context_provider,
            report_to_stdout=report,
            refresh_interval_secs=refresh_interval_secs,
            live=live,
            preview=preview,
        )
        if preview:
            return pyvalue  # type: ignore[no-any-return]
        return pyvalue.get(fn_ret_deserializer(self._main_fn))  # type: ignore[no-any-return]

    async def drop(self) -> None:
        """
        Drop the app asynchronously, reverting all its target states and clearing its database.

        This will:
        - Delete all target states created by the app (e.g., drop tables, delete rows)
        - Clear the app's internal state database
        """
        env, core_app = await self._get_core_env_app()
        drop_handle = core_app.drop_async(env._context_provider)
        await drop_handle.result()

    def drop_blocking(self, *, report_to_stdout: bool | timedelta = False) -> None:
        """
        Drop the app synchronously, reverting all its target states and clearing its database.

        This will:
        - Delete all target states created by the app (e.g., drop tables, delete rows)
        - Clear the app's internal state database

        Args:
            report_to_stdout: If truthy, periodically report processing stats to
                stdout. Pass a ``timedelta`` to set the refresh interval; ``True``
                uses the default interval.
        """
        env, core_app = self._get_core_env_app_sync()
        report, refresh_interval_secs = _resolve_report_to_stdout(report_to_stdout)
        core_app.drop(
            env._context_provider,
            report_to_stdout=report,
            refresh_interval_secs=refresh_interval_secs,
        )
