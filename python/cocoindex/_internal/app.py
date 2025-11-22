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

from .state import StatePath
from .fn import Function
from .lib import Environment


P = ParamSpec("P")
R = TypeVar("R")


class AppConfig(NamedTuple):
    name: str
    environment: Environment | None = None


class App(Generic[P, R]):
    _config: AppConfig
    _main_fn: Function[Concatenate[StatePath, P], R]
    _args: Sequence[Any]
    _kwargs: Mapping[str, Any]

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
        self._main_fn = main_fn
        self._args = args
        self._kwargs = kwargs

    def update(self) -> R:
        return self._main_fn.call(StatePath(), *self._args, **self._kwargs)

    async def update_async(self) -> R:
        return await self._main_fn.acall(StatePath(), *self._args, **self._kwargs)
