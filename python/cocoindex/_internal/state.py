import uuid

from . import core  # type: ignore

StateKey = None | bool | int | str | bytes | uuid.UUID | list["StateKey"]

_ROOT_PATH = core.StatePath()


class StatePath:
    __slots__ = ("_core",)

    _core: core.StatePath

    def __init__(self) -> None:
        self._core = _ROOT_PATH

    def concat(self, part: StateKey) -> "StatePath":
        result = StatePath()
        result._core = self._core.concat(part)
        return result

    def __div__(self, part: StateKey) -> "StatePath":
        return self.concat(part)

    def __truediv__(self, part: StateKey) -> "StatePath":
        return self.concat(part)

    def __str__(self) -> str:
        return self._core.to_string()  # type: ignore

    def __repr__(self) -> str:
        return str(self)
