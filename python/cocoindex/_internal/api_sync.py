from typing import (
    ParamSpec,
    TypeVar,
)

from .app import AppBase


P = ParamSpec("P")
R = TypeVar("R")


class App(AppBase[P, R]):
    def update(self) -> R:
        return self._core.update()  # type: ignore


__all__ = ["App"]
