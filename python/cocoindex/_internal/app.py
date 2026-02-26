from __future__ import annotations

import os
import threading
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


P = ParamSpec("P")
R = TypeVar("R")

_ENV_MAX_INFLIGHT_COMPONENTS = "COCOINDEX_MAX_INFLIGHT_COMPONENTS"
_DEFAULT_MAX_INFLIGHT_COMPONENTS = 1024


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

    async def update(
        self, *, report_to_stdout: bool = False, full_reprocess: bool = False
    ) -> R:
        """
        Update the app asynchronously (run the app once to process all pending changes).

        Args:
            report_to_stdout: If True, periodically report processing stats to stdout.
            full_reprocess: If True, reprocess everything and invalidate existing caches.

        Returns:
            The result of the main function.
        """
        env, core_app = await self._get_core_env_app()
        root_path = core.StablePath()
        processor = create_core_component_processor(
            self._main_fn, env, root_path, self._app_args, self._app_kwargs
        )
        return await core_app.update_async(
            processor, report_to_stdout=report_to_stdout, full_reprocess=full_reprocess
        )

    def update_blocking(
        self, *, report_to_stdout: bool = False, full_reprocess: bool = False
    ) -> R:
        """
        Update the app synchronously (run the app once to process all pending changes).

        Args:
            report_to_stdout: If True, periodically report processing stats to stdout.
            full_reprocess: If True, reprocess everything and invalidate existing caches.

        Returns:
            The result of the main function.
        """
        env, core_app = self._get_core_env_app_sync()
        root_path = core.StablePath()
        processor = create_core_component_processor(
            self._main_fn, env, root_path, self._app_args, self._app_kwargs
        )
        return core_app.update(
            processor, report_to_stdout=report_to_stdout, full_reprocess=full_reprocess
        )

    async def drop(self, *, report_to_stdout: bool = False) -> None:
        """
        Drop the app asynchronously, reverting all its target states and clearing its database.

        This will:
        - Delete all target states created by the app (e.g., drop tables, delete rows)
        - Clear the app's internal state database

        Args:
            report_to_stdout: If True, periodically report processing stats to stdout.
        """
        _env, core_app = await self._get_core_env_app()
        await core_app.drop_async(report_to_stdout=report_to_stdout)

    def drop_blocking(self, *, report_to_stdout: bool = False) -> None:
        """
        Drop the app synchronously, reverting all its target states and clearing its database.

        This will:
        - Delete all target states created by the app (e.g., drop tables, delete rows)
        - Clear the app's internal state database

        Args:
            report_to_stdout: If True, periodically report processing stats to stdout.
        """
        _env, core_app = self._get_core_env_app_sync()
        core_app.drop(report_to_stdout=report_to_stdout)
