"""
Data types for settings of the cocoindex library.
"""

import os

from typing import Callable, Self, Any, overload
from dataclasses import dataclass
from . import _engine  # type: ignore


def get_app_namespace(*, trailing_delimiter: str | None = None) -> str:
    """Get the application namespace. Append the `trailing_delimiter` if not empty."""
    app_namespace: str = _engine.get_app_namespace()
    if app_namespace == "" or trailing_delimiter is None:
        return app_namespace
    return f"{app_namespace}{trailing_delimiter}"


def split_app_namespace(full_name: str, delimiter: str) -> tuple[str, str]:
    """Split the full name into the application namespace and the rest."""
    parts = full_name.split(delimiter, 1)
    if len(parts) == 1:
        return "", parts[0]
    return (parts[0], parts[1])


@dataclass
class DatabaseConnectionSpec:
    """
    Connection spec for relational database.
    Used by both internal and target storage.
    """

    url: str
    user: str | None = None
    password: str | None = None
    max_connections: int = 25
    min_connections: int = 5


@dataclass
class GlobalExecutionOptions:
    """Global execution options."""

    # The maximum number of concurrent inflight requests, shared among all sources from all flows.
    source_max_inflight_rows: int | None = 1024
    source_max_inflight_bytes: int | None = None


def _load_field(
    target: dict[str, Any],
    name: str,
    env_name: str,
    required: bool = False,
    parse: Callable[[str], Any] | None = None,
) -> None:
    value = os.getenv(env_name)
    if value is None:
        if required:
            raise ValueError(f"{env_name} is not set")
    else:
        if parse is None:
            target[name] = value
        else:
            try:
                target[name] = parse(value)
            except Exception as e:
                raise ValueError(
                    f"failed to parse environment variable {env_name}: {value}"
                ) from e


@dataclass
class Settings:
    """Settings for the cocoindex library."""

    database: DatabaseConnectionSpec | None = None
    app_namespace: str = ""
    global_execution_options: GlobalExecutionOptions | None = None

    @classmethod
    def from_env(cls) -> Self:
        """Load settings from environment variables."""

        database_url = os.getenv("COCOINDEX_DATABASE_URL")
        if database_url is not None:
            db_kwargs: dict[str, Any] = {"url": database_url}
            _load_field(db_kwargs, "user", "COCOINDEX_DATABASE_USER")
            _load_field(db_kwargs, "password", "COCOINDEX_DATABASE_PASSWORD")
            _load_field(
                db_kwargs,
                "max_connections",
                "COCOINDEX_DATABASE_MAX_CONNECTIONS",
                parse=int,
            )
            _load_field(
                db_kwargs,
                "min_connections",
                "COCOINDEX_DATABASE_MIN_CONNECTIONS",
                parse=int,
            )
            database = DatabaseConnectionSpec(**db_kwargs)
        else:
            database = None

        exec_kwargs: dict[str, Any] = dict()
        _load_field(
            exec_kwargs,
            "source_max_inflight_rows",
            "COCOINDEX_SOURCE_MAX_INFLIGHT_ROWS",
            parse=int,
        )
        _load_field(
            exec_kwargs,
            "source_max_inflight_bytes",
            "COCOINDEX_SOURCE_MAX_INFLIGHT_BYTES",
            parse=int,
        )
        global_execution_options = GlobalExecutionOptions(**exec_kwargs)

        app_namespace = os.getenv("COCOINDEX_APP_NAMESPACE", "")

        return cls(
            database=database,
            app_namespace=app_namespace,
            global_execution_options=global_execution_options,
        )


@dataclass
class ServerSettings:
    """Settings for the cocoindex server."""

    # The address to bind the server to.
    address: str = "127.0.0.1:49344"

    # The origins of the clients (e.g. CocoInsight UI) to allow CORS from.
    cors_origins: list[str] | None = None

    @classmethod
    def from_env(cls) -> Self:
        """Load settings from environment variables."""
        kwargs: dict[str, Any] = dict()
        _load_field(kwargs, "address", "COCOINDEX_SERVER_ADDRESS")
        _load_field(
            kwargs,
            "cors_origins",
            "COCOINDEX_SERVER_CORS_ORIGINS",
            parse=ServerSettings.parse_cors_origins,
        )
        return cls(**kwargs)

    @overload
    @staticmethod
    def parse_cors_origins(s: str) -> list[str]: ...

    @overload
    @staticmethod
    def parse_cors_origins(s: str | None) -> list[str] | None: ...

    @staticmethod
    def parse_cors_origins(s: str | None) -> list[str] | None:
        """
        Parse the CORS origins from a string.
        """
        return (
            [o for e in s.split(",") if (o := e.strip()) != ""]
            if s is not None
            else None
        )
