from typing import (
    Generic,
    ParamSpec,
    TypeVar,
)

from . import core  # type: ignore
from .environment import Environment
from .function import Function
from .environment import default_env


P = ParamSpec("P")
R = TypeVar("R")


class AppBase(Generic[P, R]):
    _name: str
    _main_fn: Function[P, R]

    _core: core.App

    def __init__(
        self,
        name: str,
        main_fn: Function[P, R],
        /,
        *,
        environment: Environment | None = None,
    ):
        self._name = name
        self._main_fn = main_fn
        env = environment or default_env()
        self._core = core.App(
            name,
            env._core_env,
        )
