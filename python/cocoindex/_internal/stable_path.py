from __future__ import annotations

import uuid

from . import core

StableKey = None | bool | int | str | bytes | uuid.UUID | tuple["StableKey", ...]

_ROOT_PATH = core.StablePath()


class StablePath:
    __slots__ = ("_core",)

    _core: core.StablePath

    def __init__(self, core_path: core.StablePath | None = None) -> None:
        self._core = core_path or _ROOT_PATH

    def concat_part(self, part: StableKey) -> "StablePath":
        result = StablePath()
        result._core = self._core.concat(part)
        return result

    def __div__(self, part: StableKey) -> "StablePath":
        return self.concat_part(part)

    def __truediv__(self, part: StableKey) -> "StablePath":
        return self.concat_part(part)

    def __str__(self) -> str:
        return self._core.to_string()

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, StablePath):
            return False
        return self._core == other._core

    def __hash__(self) -> int:
        return hash(self._core)


ROOT_PATH = StablePath()
