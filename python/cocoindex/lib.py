"""
Library level functions and states.
"""
import sys
import functools
import inspect

from typing import Callable

from . import _engine
from . import flow, query, cli, setting
from .convert import dump_engine_object


def init(settings: setting.Settings):
    """Initialize the cocoindex library."""
    _engine.init(dump_engine_object(settings))


def start_server(settings: setting.ServerSettings):
    """Start the cocoindex server."""
    flow.ensure_all_flows_built()
    query.ensure_all_handlers_built()
    _engine.start_server(settings.__dict__)

def stop():
    """Stop the cocoindex library."""
    _engine.stop()

def main_fn(
        settings: setting.Settings | None = None,
        cocoindex_cmd: str = 'cocoindex',
        ) -> Callable[[Callable], Callable]:
    """
    A decorator to wrap the main function.
    If the python binary is called with the given command, it yields control to the cocoindex CLI.

    If the settings are not provided, they are loaded from the environment variables.
    """

    def _pre_init() -> None:
        effective_settings = settings or setting.Settings.from_env()
        init(effective_settings)

    def _should_run_cli() -> bool:
        return len(sys.argv) > 1 and sys.argv[1] == cocoindex_cmd

    def _run_cli():
        return cli.cli.main(sys.argv[2:], prog_name=f"{sys.argv[0]} {sys.argv[1]}")

    def _main_wrapper(fn: Callable) -> Callable:
        if inspect.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def _inner(*args, **kwargs):
                _pre_init()
                try:
                    if _should_run_cli():
                        # Schedule to a separate thread as it invokes nested event loop.
                        # return await asyncio.to_thread(_run_cli)
                        return _run_cli()
                    return await fn(*args, **kwargs)
                finally:
                    stop()
            return _inner
        else:
            @functools.wraps(fn)
            def _inner(*args, **kwargs):
                _pre_init()
                try:
                    if _should_run_cli():
                        return _run_cli()
                    return fn(*args, **kwargs)
                finally:
                    stop()
            return _inner

    return _main_wrapper
