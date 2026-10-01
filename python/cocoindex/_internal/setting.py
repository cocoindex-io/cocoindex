"""
Data types for settings of the cocoindex library.
"""

import os
import pathlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Self


def _is_backend_url(value: str) -> bool:
    scheme, separator, _ = value.partition("://")
    return bool(separator) and scheme.lower() in {"postgres", "postgresql"}


def redact_db_url(value: str) -> str:
    """Return a connection string without credentials for logs and reprs."""
    if "://" not in value:
        return value
    scheme, rest = value.split("://", 1)
    authority_end = min(
        [idx for idx in (rest.find("/"), rest.find("?"), rest.find("#")) if idx >= 0],
        default=len(rest),
    )
    authority = rest[:authority_end]
    if "@" in authority:
        authority = "***@" + authority.rsplit("@", 1)[1]
    suffix = rest[authority_end:]
    query_start = suffix.find("?")
    fragment_start = suffix.find("#")
    if query_start < 0 or (fragment_start >= 0 and fragment_start < query_start):
        return f"{scheme}://{authority}{suffix}"

    query_end = fragment_start if fragment_start >= 0 else len(suffix)
    query = suffix[query_start + 1 : query_end]
    redacted_parts = []
    for part in query.split("&"):
        key = part.partition("=")[0]
        redacted_parts.append(
            f"{key}=***" if key.lower().endswith("password") else part
        )
    redacted_query = "&".join(redacted_parts)
    return f"{scheme}://{authority}{suffix[: query_start + 1]}{redacted_query}{suffix[query_end:]}"


def get_default_db_path() -> pathlib.Path | str | None:
    """
    Get the default database path from the COCOINDEX_DB environment variable.

    Returns a string unchanged for backend URLs (pathlib would normalize
    ``postgres://`` into a filesystem-looking path). Filesystem paths remain
    ``pathlib.Path`` as before.
    """
    db_path = os.getenv("COCOINDEX_DB")
    if not db_path:
        return None
    return db_path if _is_backend_url(db_path) else pathlib.Path(db_path)


@dataclass
class LmdbSettings:
    """Settings for the internal LMDB-backed state store.

    `map_size` is the *initial* size of the LMDB memory map, not a cap: the
    engine doubles the map and retries whenever a write runs out of space.
    """

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


@dataclass(init=False, repr=False)
class Settings:
    """Settings for the cocoindex library."""

    db_path: os.PathLike[str] | str | None
    db_settings: LmdbSettings
    # Deprecated v0 leftover; has no effect in v1. Kept (always `None`) so callers
    # that still pass `global_execution_options=None` don't break.
    global_execution_options: None

    def __init__(
        self,
        db_path: os.PathLike[str] | str | None = None,
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

    def __repr__(self) -> str:
        db_path = redact_db_url(str(self.db_path)) if self.db_path is not None else None
        return (
            f"Settings(db_path={db_path!r}, "
            f"db_settings={self.db_settings!r}, "
            "global_execution_options=None)"
        )

    @classmethod
    def from_env(cls, db_path: os.PathLike[str] | str | None = None) -> Self:
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
