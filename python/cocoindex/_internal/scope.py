from __future__ import annotations

from cocoindex._internal.context_keys import ContextKey
from dataclasses import dataclass
from typing import TypeVar

from cocoindex._internal.environment import Environment

from . import core
from .stable_path import StableKey

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Scope:
    """
    Explicit scope object passed to orchestration APIs.

    Combines the stable path for component identification with the processor context
    for effect declaration and component mounting.

    Supports path composition via the `/` operator:
        scope / "part" / "subpart"
    """

    _env: Environment
    _core_path: core.StablePath
    _core_processor_ctx: core.ComponentProcessorContext
    _core_fn_call_ctx: core.FnCallContext

    def concat_part(self, part: StableKey) -> Scope:
        """Return a new Scope with the given part appended to the path."""
        return Scope(
            self._env,
            self._core_path.concat(part),
            self._core_processor_ctx,
            self._core_fn_call_ctx,
        )

    def use(self, key: ContextKey[T]) -> T:
        return self._env.context_provider.use(key)

    def _with_fn_call_ctx(self, fn_call_ctx: core.FnCallContext) -> Scope:
        return Scope(
            self._env,
            self._core_path,
            self._core_processor_ctx,
            fn_call_ctx,
        )

    def __div__(self, part: StableKey) -> Scope:
        return self.concat_part(part)

    def __truediv__(self, part: StableKey) -> Scope:
        return self.concat_part(part)

    def __str__(self) -> str:
        return self._core_path.to_string()

    def __repr__(self) -> str:
        return f"Scope({self._core_path.to_string()})"

    def __coco_memo_key__(self) -> object:
        core_path_memo_key = self._core_path.__coco_memo_key__()
        if self._core_path == self._core_processor_ctx.stable_path:
            return core_path_memo_key
        return (
            core_path_memo_key,
            self._core_processor_ctx.stable_path.__coco_memo_key__(),
        )
