"""Specs shared by every layer: roles, sessions, tools, inputs.

These are plain frozen dataclasses so CocoIndex can fingerprint them as
memoization inputs and serialize them as return values. ``SessionSpec`` is the
model-visible contract of a session and is part of every turn's memo key and
of a closed session's component key; build it with ``HarnessConfig.spec()`` so
its tool specs, model id and version are resolved.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cocoindex.resources.file import FileLike

# A JSON-like value: tool arguments and tool results. The shape is decided by
# each tool, so this is one of the few places a truly generic type is right.
Json = Any


@dataclass(frozen=True)
class ToolSpec:
    """What the model sees for one tool: name, description, JSON-schema parameters."""

    name: str
    description: str
    parameters: dict[str, Json]


@dataclass(frozen=True)
class RoleSpec:
    """A session role: prompt, allowed tools, model, turn budget.

    ``tool_names`` selects from the harness's fixed registry. ``model`` may be
    ``"default"`` in a role definition; ``HarnessConfig.role()`` replaces it
    with the configured model so every spec carries a concrete model id.
    """

    name: str
    system_prompt: str
    tool_names: tuple[str, ...]
    model: str = "default"
    max_turns: int = 50


@dataclass(frozen=True)
class InputCard:
    """One handle a parent passed to a child, as the child's model is told about it."""

    handle: str
    kind: str
    methods: tuple[str, ...]


@dataclass(frozen=True)
class SessionSpec:
    """One session's model-visible contract.

    - ``role`` and ``task``: who and what.
    - ``tools``: the role's tool specs, so a description or schema edit
      invalidates closed sessions too, not only turns.
    - ``inputs``: the handles seeded into the session, so the model knows
      their names and the key changes when the namespace changes.
    - ``version``: ``HarnessConfig.version``; bump it when a helper the engine
      does not track (a handle object's method) changes behaviour.
    """

    role: RoleSpec
    task: str
    tools: tuple[ToolSpec, ...] = ()
    inputs: tuple[InputCard, ...] = ()
    version: int = 1


@dataclass(frozen=True)
class SessionResult:
    """What a session returns to its mounter. A session always ends with an answer."""

    answer: str
    num_steps: int


@dataclass(frozen=True)
class Item:
    """One source item as a closed session's input: its key within the source, and the file.

    Fingerprinted as a dataclass: the key by value, the file by its own memo
    key and memo state (path identity, then mtime/content fingerprint).
    """

    key: str
    file: FileLike[Any]


# What a session was mounted for: a source item (closed worker), the partial
# results to combine (reducer), or nothing (open session).
SessionInput = Item | tuple[str, ...] | None
