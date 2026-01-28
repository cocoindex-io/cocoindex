from __future__ import annotations

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
from .function import AnyCallable, AsyncCallable


P = ParamSpec("P")
R = TypeVar("R")


@dataclass(frozen=True)
class AppConfig:
    name: str
    environment: Environment | LazyEnvironment = _default_env


class AppBase(Generic[P, R]):
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
                self._core_env_app = (env, core.App(self._name, env._core_env))
            return self._core_env_app
