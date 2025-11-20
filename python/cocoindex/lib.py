"""
Library level functions and states.
"""

import threading
import warnings

from . import _core  # type: ignore
from . import setting
from .engine_object import dump_engine_object
from typing import Any, Callable, Iterator, overload


class EnvironmentBuilder:
    """Builder for the Environment."""

    _settings: setting.Settings

    def __init__(self, settings: setting.Settings | None = None):
        self._settings = settings or setting.Settings.from_env()

    @property
    def settings(self) -> setting.Settings:
        return self._settings


LifespanFn = Callable[[EnvironmentBuilder], Iterator[None]]


def _noop_lifespan_fn(_builder: EnvironmentBuilder) -> Iterator[None]:
    yield


class Environment:
    _core_env: _core.Environment | None
    _lifespan_iter: Iterator[None] | None

    def __init__(self, lifespan_fn: LifespanFn | None = None):
        lifespan_fn = lifespan_fn or _noop_lifespan_fn
        env_builder = EnvironmentBuilder()
        lifespan_iter = lifespan_fn(env_builder)
        next(lifespan_iter)

        settings = env_builder.settings
        if not settings.db_path:
            raise ValueError("EnvironmentBuilder.Settings.db_path must be provided")

        self._core_env = _core.Environment(dump_engine_object(env_builder.settings))
        self._lifespan_iter = lifespan_iter

    def __del__(self) -> None:
        if self._lifespan_iter is not None:
            try:
                next(self._lifespan_iter)
            except StopIteration:
                pass


_default_env_lock: threading.Lock = threading.Lock()
_default_env: Environment | None = None
_default_env_lifespan_fn: LifespanFn | None = None


@overload
def lifespan(fn: LifespanFn) -> LifespanFn: ...
@overload
def lifespan(fn: None) -> Callable[[LifespanFn], LifespanFn]: ...
def lifespan(fn: LifespanFn | None = None) -> Any:
    """
    Decorate a function that returns a lifespan.
    It registers the function as a lifespan provider.
    """

    def _inner(fn: LifespanFn) -> LifespanFn:
        global _default_env_lifespan_fn  # pylint: disable=global-statement
        with _default_env_lock:
            if _default_env is not None:
                warnings.warn(
                    f"Default environment already initialized with lifespan function {_default_env_lifespan_fn}. "
                    f"Setting a lifespan function will be a no-op."
                )
            if _default_env_lifespan_fn is not None:
                warnings.warn(
                    f"Overriding the default lifespan function {_default_env_lifespan_fn} with {fn}."
                )
            _default_env_lifespan_fn = fn
        return fn

    if fn is not None:
        return _inner(fn)
    else:
        return _inner


def default_env() -> Environment:
    """
    Get the default environment.
    """
    global _default_env  # pylint: disable=global-statement
    with _default_env_lock:
        if _default_env is None:
            _default_env = Environment(_default_env_lifespan_fn)
        return _default_env
