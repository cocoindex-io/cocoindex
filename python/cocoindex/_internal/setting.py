"""
Data types for settings of the cocoindex library.
"""

import os
import pathlib

from typing import Callable, Self, Any
from dataclasses import dataclass


def get_default_db_path() -> pathlib.Path | None:
    """
    Get the default database path from the COCOINDEX_DB environment variable.

    Returns:
        The path from COCOINDEX_DB if set, otherwise None.
    """
    db_path = os.getenv("COCOINDEX_DB")
    return pathlib.Path(db_path) if db_path else None


@dataclass
class LmdbSettings:
    """Settings for the internal LMDB-backed state store."""

    max_dbs: int = 1024
    map_size: int = 0x1_0000_0000  # 4 GiB


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


@dataclass(init=False)
class Settings:
    """Settings for the cocoindex library."""

    db_path: os.PathLike[str] | None
    db_settings: LmdbSettings
    # Deprecated v0 leftover; has no effect in v1. Kept (always `None`) so callers
    # that still pass `global_execution_options=None` don't break.
    global_execution_options: None

    def __init__(
        self,
        db_path: os.PathLike[str] | None = None,
        db_settings: LmdbSettings | None = None,
        *,
        lmdb_max_dbs: int | None = None,
        lmdb_map_size: int | None = None,
        global_execution_options: None = None,  # Deprecated; ignored.
    ) -> None:
        if db_settings is not None and (
            lmdb_max_dbs is not None or lmdb_map_size is not None
        ):
            raise ValueError(
                "Specify either `db_settings=` or the legacy "
                "`lmdb_max_dbs=`/`lmdb_map_size=` keyword arguments, not both."
            )
        if db_settings is None:
            db_settings = LmdbSettings()
            if lmdb_max_dbs is not None:
                db_settings.max_dbs = lmdb_max_dbs
            if lmdb_map_size is not None:
                db_settings.map_size = lmdb_map_size

        self.db_path = db_path
        self.db_settings = db_settings
        self.global_execution_options = None

    @property
    def lmdb_max_dbs(self) -> int:
        return self.db_settings.max_dbs

    @lmdb_max_dbs.setter
    def lmdb_max_dbs(self, value: int) -> None:
        self.db_settings.max_dbs = value

    @property
    def lmdb_map_size(self) -> int:
        return self.db_settings.map_size

    @lmdb_map_size.setter
    def lmdb_map_size(self, value: int) -> None:
        self.db_settings.map_size = value

    def _to_engine_dict(self) -> dict[str, Any]:
        """Produce the flat wire-format dict consumed by the Rust engine."""
        d: dict[str, Any] = {
            "lmdb_max_dbs": self.db_settings.max_dbs,
            "lmdb_map_size": self.db_settings.map_size,
        }
        if self.db_path is not None:
            d["db_path"] = str(self.db_path)
        return d

    @classmethod
    def from_env(cls, db_path: os.PathLike[str] | None = None) -> Self:
        """Load settings from environment variables."""

        lmdb_kwargs: dict[str, Any] = {}
        _load_field(
            lmdb_kwargs,
            "max_dbs",
            "COCOINDEX_LMDB_MAX_DBS",
            parse=int,
        )
        _load_field(
            lmdb_kwargs,
            "map_size",
            "COCOINDEX_LMDB_MAP_SIZE",
            parse=int,
        )

        return cls(
            db_path=db_path,
            db_settings=LmdbSettings(**lmdb_kwargs),
        )
