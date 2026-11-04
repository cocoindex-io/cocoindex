"""Error classes shared by tools and handles."""

from __future__ import annotations


class ToolError(Exception):
    """A model-facing error: bad arguments, unknown handle, invalid reference.

    The registry turns it into a ``ToolResult(ok=False)`` the model can read
    and recover from. Any other exception raised by a tool is an infrastructure
    failure and propagates, so the session component fails, nothing is
    committed, and the next run retries (DESIGN.md §7, "Failures").
    """
