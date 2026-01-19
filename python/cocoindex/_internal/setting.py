"""
Data types for settings of the cocoindex library.
"""

import os

from typing import Callable, Self, Any, overload
from dataclasses import dataclass


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

    db_path: os.PathLike[str] | None = None
    global_execution_options: GlobalExecutionOptions | None = None

    @classmethod
    def from_env(cls, db_path: os.PathLike[str] | None = None) -> Self:
        """Load settings from environment variables."""

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

        return cls(
            db_path=db_path,
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
