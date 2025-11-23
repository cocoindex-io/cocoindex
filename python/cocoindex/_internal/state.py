import uuid
from typing import Self, Any

from . import core  # type: ignore

StatePathPart = None | bool | int | str | bytes | uuid.UUID | list["StatePathPart"]

_ROOT_PATH = core.StatePath()


class StatePath:
    _path: core.StatePath

    def __init__(self) -> None:
        self._path = _ROOT_PATH

    def concat(self, part: StatePathPart) -> "StatePath":
        result = StatePath()
        result._path = self._path.concat(part)
        return result

    def __div__(self, part: StatePathPart) -> "StatePath":
        return self.concat(part)

    def __truediv__(self, part: StatePathPart) -> "StatePath":
        return self.concat(part)

    def __str__(self) -> str:
        return self._path.to_string()  # type: ignore

    def __repr__(self) -> str:
        return str(self)
