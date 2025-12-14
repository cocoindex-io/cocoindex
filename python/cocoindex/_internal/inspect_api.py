from typing import Any

from . import core  # type: ignore
from .app import AppBase
from .stable_path import StablePath


def list_stable_paths(app: AppBase[Any, Any]) -> list[StablePath]:
    return [StablePath(path) for path in core.list_stable_paths(app._core)]


__all__ = [
    "list_stable_paths",
]
