"""
Library level functions and states.
"""

import threading
import warnings

from . import _engine  # type: ignore
from . import flow, setting
from .convert import dump_engine_object
from .validation import validate_app_namespace_name
from typing import Any, Callable, overload


def prepare_settings(settings: setting.Settings) -> Any:
    """Prepare the settings for the engine."""
    if settings.app_namespace:
        validate_app_namespace_name(settings.app_namespace)
    return dump_engine_object(settings)


_engine.set_settings_fn(lambda: prepare_settings(setting.Settings.from_env()))


_prev_settings_fn: Callable[[], setting.Settings] | None = None
_prev_settings_fn_lock: threading.Lock = threading.Lock()


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
        global _prev_settings_fn  # pylint: disable=global-statement
        with _prev_settings_fn_lock:
            if _prev_settings_fn is not None:
                warnings.warn(
                    f"Setting a new settings function will override the previous one {_prev_settings_fn}."
                )
            _prev_settings_fn = fn
        _engine.set_settings_fn(lambda: prepare_settings(fn()))
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
    _engine.init(prepare_settings(settings) if settings is not None else None)


def start_server(settings: setting.ServerSettings) -> None:
    """Start the cocoindex server."""
    flow.ensure_all_flows_built()
    _engine.start_server(settings.__dict__)


def stop() -> None:
    """Stop the cocoindex library."""
    _engine.stop()
