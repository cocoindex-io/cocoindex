from __future__ import annotations

import fnmatch as _fnmatch
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


ROOT_PATH = StablePath()


def build_selector_path(*parts: StableKey) -> core.StablePath:
    """Build a ``core.StablePath`` from *parts* for use in ``component_selector``.

    Each part must be a valid ``StableKey`` (``str``, ``int``, ``Symbol``, etc.).
    Glob wildcards like ``"*"`` can be passed as string parts.

    Example::

        path = build_selector_path(Symbol("process"), "*.md")
        app.update(component_selector=[path])
    """
    path = _ROOT_PATH
    for part in parts:
        path = path.concat(part)
    return path


def _has_glob(s: str) -> bool:
    """Check if a string contains glob wildcard characters."""
    return any(c in s for c in "*?[")


def _selector_part_matches(path_part: StableKey, sel_part: StableKey) -> bool:
    """Check if a single path part matches a selector part.

    String selector parts may contain ``fnmatch`` glob patterns (``*``, ``?``,
    ``[...]``).  A ``str`` and ``Symbol`` are treated as matching when the
    string equals the symbol's name (the two representations are
    interchangeable in CocoIndex paths).
    """
    # Glob matching for string selector parts.
    if isinstance(sel_part, str) and _has_glob(sel_part):
        path_str = _stable_key_to_selector_part(path_part)
        return _fnmatch.fnmatch(path_str, sel_part)

    # Cross-type: str ↔ Symbol (interchangeable in paths).
    if isinstance(path_part, str) and isinstance(sel_part, Symbol):
        return path_part == sel_part.name
    if isinstance(path_part, Symbol) and isinstance(sel_part, str):
        return path_part.name == sel_part

    # Exact match: types must be the same.
    if type(path_part) is not type(sel_part):
        return False

    if isinstance(path_part, Symbol) and isinstance(sel_part, Symbol):
        return path_part.name == sel_part.name
    if isinstance(path_part, tuple) and isinstance(sel_part, tuple):
        if len(path_part) != len(sel_part):
            return False
        return all(_selector_part_matches(p, s) for p, s in zip(path_part, sel_part))
    return path_part == sel_part


def is_path_selected(
    path: core.StablePath,
    selector: tuple[core.StablePath, ...] | None,
) -> bool:
    """Check whether *path* matches any entry in the component *selector*.

    Returns ``True`` when *selector* is ``None`` (meaning "run everything").
    Each selector entry is compared part-by-part against *path*; string
    selector parts may use ``fnmatch`` glob patterns.

    Args:
        path: The full component path to check.
        selector: The active selector tuple, or ``None``.

    Returns:
        ``True`` if the path is selected (should execute), ``False`` otherwise.
    """
    if selector is None:
        return True
    path_parts = path.parts()
    for sel_path in selector:
        sel_parts = sel_path.parts()
        if len(path_parts) != len(sel_parts):
            continue
        if all(_selector_part_matches(p, s) for p, s in zip(path_parts, sel_parts)):
            return True
    return False
