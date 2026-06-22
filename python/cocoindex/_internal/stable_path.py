from __future__ import annotations

import os as _os
import uuid

from . import core

Symbol = core.Symbol

StableKey = (
    None | bool | int | str | bytes | uuid.UUID | Symbol | tuple["StableKey", ...]
)

_ROOT_PATH = core.StablePath()


def _stable_key_to_selector_part(key: StableKey) -> str:
    """Convert a single StableKey to a human-readable selector-path segment.

    This produces clean, unquoted strings suitable for fnmatch glob matching.
    """
    if key is None:
        return "null"
    if isinstance(key, bool):
        return "true" if key else "false"
    if isinstance(key, int):
        return str(key)
    if isinstance(key, str):
        return key
    if isinstance(key, bytes):
        return _os.fsdecode(key)
    if isinstance(key, uuid.UUID):
        return str(key)
    if isinstance(key, Symbol):
        return key.name
    if isinstance(key, tuple):
        return "[" + ",".join(_stable_key_to_selector_part(p) for p in key) + "]"
    raise TypeError(f"Unsupported StableKey type: {type(key)}")


def stable_path_to_selector(path: core.StablePath) -> str:
    """Convert a ``core.StablePath`` to a human-readable selector string.

    Each ``StableKey`` part is converted to a clean, unquoted string and
    joined with ``/``. For example, a path with ``Symbol("process")`` and
    ``Str("doc.md")`` produces ``"process/doc.md"``.

    This function works on raw ``core.StablePath`` values (the Rust-backed
    type), so it can be used with ``child_path.concat(key)`` results
    without wrapping in the Python ``StablePath``.
    """
    return "/".join(_stable_key_to_selector_part(p) for p in path.parts())


class StablePath:
    __slots__ = ("_core",)

    _core: core.StablePath

    def __init__(self, core_path: core.StablePath | None = None) -> None:
        self._core = core_path or _ROOT_PATH

    def concat_part(self, part: StableKey) -> "StablePath":
        result = StablePath()
        result._core = self._core.concat(part)
        return result

    def __div__(self, part: StableKey) -> "StablePath":
        return self.concat_part(part)

    def __truediv__(self, part: StableKey) -> "StablePath":
        return self.concat_part(part)

    def __str__(self) -> str:
        return self._core.to_string()

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, StablePath):
            return False
        return self._core == other._core

    def __hash__(self) -> int:
        return hash(self._core)

    def parts(self) -> list[StableKey]:
        """
        Return the sequence of StableKey parts that make up this path.

        Returns:
            List of StableKey values (None, bool, int, str, bytes, uuid.UUID, or tuple)
        """
        return self._core.parts()

    def selector_path(self) -> str:
        """
        Return a human-readable path string for use with component selectors.

        Each StableKey part is converted to a clean, unquoted string and
        joined with ``/``. For example, a path with Symbol("process") and
        Str("doc.md") produces ``"process/doc.md"``.

        Returns:
            Selector-ready path string (e.g. ``"process/doc.md"``).
        """
        return stable_path_to_selector(self._core)


ROOT_PATH = StablePath()
