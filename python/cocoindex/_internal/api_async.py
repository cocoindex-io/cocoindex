from typing import (
    ParamSpec,
    TypeVar,
)

from .app import AppBase


P = ParamSpec("P")
R = TypeVar("R")


class App(AppBase[P, R]):
    async def update(self) -> R:
        return await self._core.update_async()  # type: ignore


__all__ = ["App"]
