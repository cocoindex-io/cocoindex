"""Shared helpers for CocoIndex connector implementations."""

from __future__ import annotations

from typing import Any

__all__ = ["default_subpath_name"]


def default_subpath_name(processor_fn: Any) -> str | None:
    """Resolve the default subpath name for a mount target.

    Honors an explicit ``__coco_subpath_name__`` attribute (set by wrappers
    like ``coco.auto_refresh`` so the wrapper class can keep an honest
    ``__name__`` while still mounting under the wrapped function's name),
    falling back to ``__name__``.
    """
    name = getattr(processor_fn, "__coco_subpath_name__", None)
    if isinstance(name, str):
        return name
    name = getattr(processor_fn, "__name__", None)
    if isinstance(name, str):
        return name
    return None
