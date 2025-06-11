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


def main_fn(
    settings: Any | None = None,
    cocoindex_cmd: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    DEPRECATED: The @cocoindex.main_fn() decorator is obsolete and has no effect.
    It will be removed in a future version, which will cause an AttributeError.

    Please remove this decorator from your code and use the standalone 'cocoindex' CLI.
    See the updated CLI usage examples in the warning message.
    """
    warnings.warn(
        "\n\n"
        "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"
        "CRITICAL DEPRECATION NOTICE from CocoIndex:\n"
        "The @cocoindex.main_fn() decorator in your script is DEPRECATED and IGNORED.\n"
        "It provides NO functionality and will be REMOVED entirely in a future version.\n"
        "If not removed, your script will FAIL with an AttributeError in the future.\n\n"
        "ACTION REQUIRED: Please REMOVE @cocoindex.main_fn() from your Python script.\n\n"
        "To use CocoIndex, invoke the standalone 'cocoindex' CLI."
        " Examples of new CLI usage:\n\n"
        "  To list flows from 'main.py' (previously 'python main.py cocoindex ls'):\n"
        "    cocoindex ls main.py\n\n"
        "  To list all persisted flows (previously 'python main.py cocoindex ls --all'):\n"
        "    cocoindex ls\n\n"
        "  To show 'MyFlow' defined in 'main.py' (previously 'python main.py cocoindex show MyFlow'):\n"
        "    cocoindex show main.py:MyFlow\n\n"
        "  To update all flows in 'my_package.flows_module':\n"
        "    cocoindex update my_package.flows_module\n\n"
        "  To update 'SpecificFlow' in 'my_package.flows_module':\n"
        "    cocoindex update my_package.flows_module:SpecificFlow\n\n"
        "See cocoindex <command> --help for more details.\n"
        "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n",
        DeprecationWarning,
        stacklevel=2,
    )

    def _main_wrapper(fn: Callable[..., Any]) -> Callable[..., Any]:
        return fn

    return _main_wrapper
