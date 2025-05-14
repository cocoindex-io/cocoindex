"""
Library level functions and states.
"""
import warnings
from typing import Callable, Any

from . import _engine, setting
from .convert import dump_engine_object


def init(settings: setting.Settings):
    """Initialize the cocoindex library."""
    _engine.init(dump_engine_object(settings))


def start_server(settings: setting.ServerSettings):
    """Start the cocoindex server."""
    _engine.start_server(settings.__dict__)

def stop():
    """Stop the cocoindex library."""
    _engine.stop()

def main_fn(
        settings: Any | None = None,
        cocoindex_cmd: str | None = None,
        ) -> Callable[[Callable], Callable]:
    """
    DEPRECATED: Using @cocoindex.main_fn() is no longer supported and has no effect.
    This decorator will be removed in a future version, which will cause an AttributeError.
    Please remove it from your code and use the standalone 'cocoindex' CLI.
    """
    warnings.warn(
        "\n\n"
        "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"
        "CRITICAL DEPRECATION NOTICE from CocoIndex:\n"
        "The @cocoindex.main_fn() decorator found in your script is DEPRECATED and IGNORED.\n"
        "It provides NO functionality and will be REMOVED entirely in a future version.\n"
        "If not removed, your script will FAIL with an AttributeError in the future.\n\n"
        "ACTION REQUIRED: Please REMOVE @cocoindex.main_fn() from your Python script.\n\n"
        "To use CocoIndex commands, invoke the standalone 'cocoindex' CLI:\n"
        "  cocoindex <command> [options] --app <your_script.py>\n"
        "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n",
        DeprecationWarning,
        stacklevel=2
    )

    def _main_wrapper(fn: Callable) -> Callable:
        return fn
    return _main_wrapper
