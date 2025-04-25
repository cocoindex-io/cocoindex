"""
Library level functions and states.
"""
import os
import sys
import functools
import inspect

from typing import Callable, Self
from dataclasses import dataclass

from . import _engine
from . import flow, query, cli
from .convert import dump_engine_object


def _load_field(target: dict[str, str], name: str, env_name: str, required: bool = False):
    value = os.getenv(env_name)
    if value is None:
        if required:
            raise ValueError(f"{env_name} is not set")
    else:
        target[name] = value

@dataclass
class DatabaseConnectionSpec:
    url: str
    user: str | None = None
    password: str | None = None

@dataclass
class Settings:
    """Settings for the cocoindex library."""
    database: DatabaseConnectionSpec

    @classmethod
    def from_env(cls) -> Self:
        """Load settings from environment variables."""

        db_kwargs: dict[str, str] = dict()
        _load_field(db_kwargs, "url", "COCOINDEX_DATABASE_URL", required=True)
        _load_field(db_kwargs, "user", "COCOINDEX_DATABASE_USER")
        _load_field(db_kwargs, "password", "COCOINDEX_DATABASE_PASSWORD")
        database = DatabaseConnectionSpec(**db_kwargs)
        return cls(database=database)


def init(settings: Settings):
    """Initialize the cocoindex library."""
    _engine.init(dump_engine_object(settings))

@dataclass
class ServerSettings:
    """Settings for the cocoindex server."""

    # The address to bind the server to.
    address: str = "127.0.0.1:8080"

    # The origin of the client (e.g. CocoInsight UI) to allow CORS from.
    cors_origin: str | None = None

    @classmethod
    def from_env(cls) -> Self:
        """Load settings from environment variables."""

        kwargs: dict[str, str] = dict()
        _load_field(kwargs, "address", "COCOINDEX_SERVER_ADDRESS")
        _load_field(kwargs, "cors_origin", "COCOINDEX_SERVER_CORS_ORIGIN")

        return cls(**kwargs)


def start_server(settings: ServerSettings):
    """Start the cocoindex server."""
    flow.ensure_all_flows_built()
    query.ensure_all_handlers_built()
    _engine.start_server(settings.__dict__)

def stop():
    """Stop the cocoindex library."""
    _engine.stop()

def main_fn(
        settings: Settings | None = None,
        cocoindex_cmd: str = 'cocoindex',
        ) -> Callable[[Callable], Callable]:
    """
    A decorator to wrap the main function.
    If the python binary is called with the given command, it yields control to the cocoindex CLI.

    If the settings are not provided, they are loaded from the environment variables.
    """

    def _pre_init() -> None:
        effective_settings = settings or Settings.from_env()
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
