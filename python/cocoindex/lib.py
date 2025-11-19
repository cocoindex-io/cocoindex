"""
Library level functions and states.
"""

import atexit
import threading
import warnings

from . import _core  # type: ignore
from . import setting
from .engine_object import dump_engine_object
from typing import Any, Callable, overload


def prepare_settings(settings: setting.Settings) -> Any:
    """Prepare the settings for the engine."""
    return dump_engine_object(settings)


_settings_fn: Callable[[], setting.Settings] | None = None
_init_called: bool = False
_global_init_lock: threading.Lock = threading.Lock()


@overload
def settings(fn: Callable[[], setting.Settings]) -> Callable[[], setting.Settings]: ...
@overload
def settings(
    fn: None,
) -> Callable[[Callable[[], setting.Settings]], Callable[[], setting.Settings]]: ...
def settings(fn: Callable[[], setting.Settings] | None = None) -> Any:
    """
    Decorate a function that returns a settings.Settings object.
    It registers the function as a settings provider.
    """

    def _inner(fn: Callable[[], setting.Settings]) -> Callable[[], setting.Settings]:
        global _settings_fn  # pylint: disable=global-statement
        with _global_init_lock:
            if _settings_fn is not None:
                warnings.warn(
                    f"Setting a new settings function will override the previous one {_settings_fn}."
                )
            _settings_fn = fn
        return fn

    if fn is not None:
        return _inner(fn)
    else:
        return _inner


def init(settings: setting.Settings | None = None) -> None:
    """
    Initialize the cocoindex library.

    If the settings are not provided, they are loaded from the environment variables.
    """
    global _init_called

    with _global_init_lock:
        if _init_called:
            if settings is None:
                return
            raise ValueError("CocoIndex library already initialized")

        effective_settings = settings
        if settings and _settings_fn is not None:
            effective_settings = _settings_fn()
        if effective_settings is None:
            raise ValueError("No settings provided")

        _core.init_planet(prepare_settings(effective_settings))
        atexit.register(_core.close_planet)
        _init_called = True
