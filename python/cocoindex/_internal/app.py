from typing import (
    Any,
    Concatenate,
    Generic,
    Mapping,
    ParamSpec,
    Sequence,
    TypeVar,
    NamedTuple,
)

from . import core  # type: ignore
from .environment import Environment
from .function import Function
from .state import StatePath
from .environment import default_env


P = ParamSpec("P")
R = TypeVar("R")


class AppConfig(NamedTuple):
    name: str
    environment: Environment | None = None


class AppBase(Generic[P, R]):
    _config: AppConfig
    _core: core.App

    def __init__(
        self,
        main_fn: Function[Concatenate[StatePath, P], R],
        config: str | AppConfig,
        *args: P.args,
        **kwargs: P.kwargs,
    ):
        if isinstance(config, str):
            self._config = AppConfig(name=config)
        else:
            self._config = config

        component_builder = main_fn._as_core_component_builder(
            StatePath(), *args, **kwargs
        )
        env = self._config.environment or default_env()
        self._core = core.App(
            self._config.name,
            env._core_env,
            component_builder,
        )
