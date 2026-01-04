from __future__ import annotations

from dataclasses import dataclass

from . import core
from .stable_path import StableKey


@dataclass(frozen=True, slots=True)
class Scope:
    """
    Explicit scope object passed to orchestration APIs.

    Combines the stable path for component identification with the processor context
    for effect declaration and component mounting.

    Supports path composition via the `/` operator:
        scope / "part" / "subpart"
    """

    _core_path: core.StablePath
    _core_processor_ctx: core.ComponentProcessorContext

    def concat_part(self, part: StableKey) -> Scope:
        """Return a new Scope with the given part appended to the path."""
        return Scope(self._core_path.concat(part), self._core_processor_ctx)

    def __div__(self, part: StableKey) -> Scope:
        return self.concat_part(part)

    def __truediv__(self, part: StableKey) -> Scope:
        return self.concat_part(part)

    def __str__(self) -> str:
        return self._core_path.to_string()

    def __repr__(self) -> str:
        return f"Scope({self._core_path.to_string()})"
