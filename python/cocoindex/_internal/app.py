from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any,
    Concatenate,
    Generic,
    ParamSpec,
    TypeVar,
)

from . import core  # type: ignore
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

    _environment: Environment | None
    _core: core.App | None

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

        self._environment = config.environment
        self._core = None

    async def _get_core(self) -> core.App:
        if self._core is None:
            env = self._environment or await default_env()
            self._core = core.App(self._name, env._core_env)
        return self._core
