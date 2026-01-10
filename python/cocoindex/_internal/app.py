from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import (
    Any,
    Concatenate,
    Generic,
    ParamSpec,
    TypeVar,
)

from . import core
from .environment import Environment
from .function import Function
from .environment import default_env
from .scope import Scope


P = ParamSpec("P")
R = TypeVar("R")


@dataclass(frozen=True)
class AppConfig:
    name: str
    environment: Environment | None = None


class AppBase(Generic[P, R]):
    _name: str
    _main_fn: Function[Concatenate[Scope, P], R]
    _app_args: tuple[Any, ...]
    _app_kwargs: dict[str, Any]

    _lock: asyncio.Lock
    _inner: tuple[Environment, core.App] | None

    def __init__(
        self,
        main_fn: Function[Concatenate[Scope, P], R],
        name_or_config: str | AppConfig,
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

        self._lock = asyncio.Lock()
        self._inner = (
            self._create_inner(config.environment) if config.environment else None
        )

    async def _ensure_inner(self) -> tuple[Environment, core.App]:
        if self._inner is not None:
            return self._inner

        async with self._lock:
            if self._inner is None:
                self._inner = self._create_inner(await default_env())
            return self._inner

    async def _get_core(self) -> core.App:
        _env, core_app = await self._ensure_inner()
        return core_app

    def _create_inner(self, env: Environment) -> tuple[Environment, core.App]:
        return (env, core.App(self._name, env._core_env))
