"""
Library level functions and states.
"""

import warnings
from typing import Callable, Any

from . import _engine  # type: ignore
from . import flow, setting
from .convert import dump_engine_object


def init(settings: setting.Settings | None = None) -> None:
    """
    Initialize the cocoindex library.

    If the settings are not provided, they are loaded from the environment variables.
    """
    settings = settings or setting.Settings.from_env()
    _engine.init(dump_engine_object(settings))
    setting.set_app_namespace(settings.app_namespace)


def start_server(settings: setting.ServerSettings) -> None:
    """Start the cocoindex server."""
    flow.ensure_all_flows_built()
    _engine.start_server(settings.__dict__)


def stop() -> None:
    """Stop the cocoindex library."""
    _engine.stop()
