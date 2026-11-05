"""coco-harness: an incremental agent harness on CocoIndex.

See DESIGN.md for the model. The short version: a session is a memoized
CocoIndex component whose body runs the tools; each model turn is a memoized
component keyed on the transcript so far, so an unchanged prefix replays with
no inference. Closed sessions (workers over one input) are keyed on their
inputs; open sessions (the orchestrator) also record what they read, and are
skipped whole while those reads are unchanged.

Public surface, until the harness moves to its own repository: what an app
needs to configure and run a flow. Everything else is implementation and is
imported from its module.
"""

from __future__ import annotations

from .errors import ToolError
from .model import MODEL_CLIENT, ModelClient
from .session import run_open_session
from .spec import RoleSpec, SessionResult, SessionSpec, ToolSpec
from .tools import HARNESS, HarnessConfig, Tool, ToolRegistry, tool

__all__ = [
    "HARNESS",
    "HarnessConfig",
    "MODEL_CLIENT",
    "ModelClient",
    "RoleSpec",
    "SessionResult",
    "SessionSpec",
    "Tool",
    "ToolError",
    "ToolRegistry",
    "ToolSpec",
    "run_open_session",
    "tool",
]
