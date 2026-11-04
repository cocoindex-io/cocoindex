"""Handles: stable names for live objects, and the cards that describe them.

A handle (``src:docs``, ``tgt:wiki``, ``coll:notes``, ``res:report``,
``job:audit``) is what the transcript refers to; the object behind it lives
only for the run and is rebuilt deterministically as tools execute. Opening an
object returns a card listing its methods, each tagged with how volatile its
output is (DESIGN.md §3).

Handles cross a mount boundary explicitly: a tool's ``inputs`` names the
handles a child session may see, and the child's table is seeded with them
under the same names. Only objects with ``closed_input_ok`` may be passed to a
closed (memoized) session, because the child's memo key must cover everything
it can read.
"""

from __future__ import annotations

import enum
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Protocol

from .errors import ToolError
from .spec import InputCard, Json
from .stable_hash import stable_hash64

_PREFIXES = ("src", "tgt", "coll", "res", "job")


class Volatility(enum.Enum):
    STABLE = "stable"  # same answer unless the data changed in a way that matters
    PER_ITEM = "per-item"  # depends on one item's content
    VOLATILE = "volatile"  # any change anywhere changes it; only inside observe()


@dataclass(frozen=True)
class MethodSpec:
    name: str
    description: str
    volatility: Volatility

    def as_json(self) -> dict[str, Json]:
        return {
            "name": self.name,
            "description": self.description,
            "volatility": self.volatility.value,
        }


class HandleObject(Protocol):
    @property
    def kind(self) -> str: ...

    @property
    def closed_input_ok(self) -> bool:
        """May be an input of a closed session (then it must define ``__coco_memo_key__``)."""
        ...

    def methods(self) -> tuple[MethodSpec, ...]: ...

    async def call(self, method: str, args: dict[str, Json]) -> Json: ...


def input_card(handle: str, obj: HandleObject) -> InputCard:
    return InputCard(
        handle=handle, kind=obj.kind, methods=tuple(m.name for m in obj.methods())
    )


def int_arg(args: dict[str, Json], name: str, default: int) -> int:
    """A method argument that must be a non-negative integer; anything else is the model's error."""
    value = args.get(name, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ToolError(f"{name} must be a non-negative integer")
    return value


@dataclass
class ResultValue:
    """A tool or agent result behind a ``res:`` handle.

    Every agent/reduce result gets one, whatever its size, so later actions
    can refer to it (``declare(..., value="res:x")``) instead of pasting text —
    a replayed action then uses *this* run's value (DESIGN.md §3).
    """

    text: str

    kind = "result"
    closed_input_ok = True

    def __coco_memo_key__(self) -> object:
        return ("result", stable_hash64(self.text))

    def methods(self) -> tuple[MethodSpec, ...]:
        return (
            MethodSpec("read", "The full text.", Volatility.PER_ITEM),
            MethodSpec(
                "head",
                "The first `chars` characters (default 500).",
                Volatility.PER_ITEM,
            ),
        )

    async def call(self, method: str, args: dict[str, Json]) -> Json:
        if method == "read":
            return self.text
        if method == "head":
            return self.text[: int_arg(args, "chars", 500)]
        raise ToolError(f"unknown method {method!r} on a result handle")


def is_handle(value: object) -> bool:
    return (
        isinstance(value, str)
        and ":" in value
        and value.split(":", 1)[0] in _PREFIXES
        and bool(value.split(":", 1)[1])
    )


class HandleTable:
    """The handles of one session run. Not persisted; rebuilt as tools execute.

    ``fork``/``merge`` give the parallel calls of one turn deterministic
    visibility: each call sees the table as it was before the turn, and two
    calls registering the same name both fail regardless of which ran first.
    A fork whose registrations conflict publishes nothing.
    """

    def __init__(self, parent: HandleTable | None = None) -> None:
        self._objects: dict[str, HandleObject] = {}
        self._parent = parent

    def seed(self, handle: str, obj: HandleObject) -> None:
        """Add a handle under a name decided elsewhere (an input from the parent)."""
        if not is_handle(handle):
            raise ValueError(f"not a handle: {handle!r}")
        if handle in self:
            raise ValueError(f"handle {handle!r} already exists")
        self._objects[handle] = obj

    def register(self, prefix: str, name: str, obj: HandleObject) -> str:
        if prefix not in _PREFIXES:
            raise ValueError(f"unknown handle prefix {prefix!r}")
        handle = f"{prefix}:{name}"
        if handle in self:
            raise ToolError(
                f"handle {handle!r} already exists in this session; pick another name"
            )
        self._objects[handle] = obj
        return handle

    def get(self, handle: str) -> HandleObject:
        obj = self._lookup(handle)
        if obj is None:
            raise ToolError(f"unknown handle {handle!r}; known: {sorted(self.names())}")
        return obj

    def resolve(self, handles: Iterable[str]) -> dict[str, HandleObject]:
        return {h: self.get(h) for h in handles}

    def names(self) -> set[str]:
        own = set(self._objects)
        return own | self._parent.names() if self._parent is not None else own

    def __contains__(self, handle: str) -> bool:
        return self._lookup(handle) is not None

    def fork(self) -> HandleTable:
        return HandleTable(parent=self)

    def merge(self, forks: Sequence[HandleTable]) -> set[int]:
        """Fold the forks' registrations in; return the indices of forks that conflicted.

        A name registered by two forks marks both as conflicting, and a
        conflicting fork publishes none of its names, so a failed call leaves
        no partial handles behind. A name already in this table cannot appear
        in a fork: ``register`` sees the parent and rejects it at call time.
        """
        owners: dict[str, list[int]] = {}
        for i, fork in enumerate(forks):
            for handle in fork._objects:
                owners.setdefault(handle, []).append(i)
        conflicts: set[int] = set()
        for idxs in owners.values():
            if len(idxs) > 1:
                conflicts.update(idxs)
        for i, fork in enumerate(forks):
            if i not in conflicts:
                self._objects.update(fork._objects)
        return conflicts

    def card(self, handle: str, **fields: Json) -> dict[str, Json]:
        obj = self.get(handle)
        return {
            "handle": handle,
            "kind": obj.kind,
            **fields,
            "methods": [m.as_json() for m in obj.methods()],
        }

    def _lookup(self, handle: str) -> HandleObject | None:
        obj = self._objects.get(handle)
        if obj is None and self._parent is not None:
            return self._parent._lookup(handle)
        return obj
