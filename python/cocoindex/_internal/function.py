from __future__ import annotations

import functools
import inspect

from typing import (
    Callable,
    Any,
    Concatenate,
    TypeVar,
    ParamSpec,
    Coroutine,
    Protocol,
    cast,
    overload,
)

from . import core  # type: ignore

from .scope import Scope
from .memo_key import fingerprint_call


P = ParamSpec("P")
R = TypeVar("R")
R_co = TypeVar("R_co", covariant=True)
P0 = ParamSpec("P0")


class Function(Protocol[P, R_co]):
    def _as_core_component_processor(
        self: Function[Concatenate[Scope, P0], R_co],
        path: core.StablePath,
        *args: P0.args,
        **kwargs: P0.kwargs,
    ) -> core.ComponentProcessor: ...


class SyncFunction(Function[P, R_co]):
    _fn: Callable[P, R_co]
    _memo: bool

    def __init__(self, fn: Callable[P, R_co], *, memo: bool):
        self._fn = fn
        self._memo = memo

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R_co:
        return self._fn(*args, **kwargs)

    def _as_core_component_processor(
        self: SyncFunction[Concatenate[Scope, P0], R_co],
        path: core.StablePath,
        *args: P0.args,
        **kwargs: P0.kwargs,
    ) -> core.ComponentProcessor:
        def _build(builder_ctx: core.ComponentProcessorContext) -> R_co:
            scope = Scope(path, builder_ctx)
            return self._fn(scope, *args, **kwargs)  # type: ignore

        memo_fp = fingerprint_call(self._fn, args, kwargs) if self._memo else None
        return core.ComponentProcessor.new_sync(_build, memo_fp)


class AsyncFunction(Function[P, R_co]):
    _fn: Callable[P, Coroutine[Any, Any, R_co]]
    _memo: bool

    def __init__(
        self,
        fn: Callable[P, Coroutine[Any, Any, R_co]],
        *,
        memo: bool,
    ):
        self._fn = fn
        self._memo = memo

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R_co]:
        return self._fn(*args, **kwargs)

    def _as_core_component_processor(
        self: AsyncFunction[Concatenate[Scope, P0], R_co],
        path: core.StablePath,
        *args: P0.args,
        **kwargs: P0.kwargs,
    ) -> core.ComponentProcessor:
        async def _build(builder_ctx: core.ComponentProcessorContext) -> R_co:
            scope = Scope(path, builder_ctx)
            return await self._fn(scope, *args, **kwargs)  # type: ignore

        memo_fp = fingerprint_call(self._fn, args, kwargs) if self._memo else None
        return core.ComponentProcessor.new_async(_build, memo_fp)


class FunctionBuilder:
    def __init__(self, *, memo: bool = False) -> None:
        self._memo = memo

    @overload
    def __call__(  # type: ignore[overload-overlap]
        self,
        fn: Callable[P, Coroutine[Any, Any, R_co]],
    ) -> AsyncFunction[P, R_co]: ...
    @overload
    def __call__(self, fn: Callable[P, R_co]) -> SyncFunction[P, R_co]: ...
    def __call__(
        self,
        fn: Callable[P, Coroutine[Any, Any, R_co]] | Callable[P, R_co],
    ) -> Function[P, R_co]:
        wrapper: Function[P, R_co]
        if inspect.iscoroutinefunction(fn):
            wrapper = AsyncFunction(fn, memo=self._memo)
        else:
            wrapper = SyncFunction(cast(Callable[P, R_co], fn), memo=self._memo)
        functools.update_wrapper(wrapper, fn)
        return wrapper


@overload
def function(*, memo: bool = False) -> FunctionBuilder: ...
@overload
def function(  # type: ignore[overload-overlap]
    fn: Callable[P, Coroutine[Any, Any, R_co]],
    /,
    *,
    memo: bool = False,
) -> AsyncFunction[P, R_co]: ...
@overload
def function(
    fn: Callable[P, R_co], /, *, memo: bool = False
) -> SyncFunction[P, R_co]: ...
def function(fn: Any = None, /, *, memo: bool = False) -> Any:
    builder = FunctionBuilder(memo=memo)
    if fn is not None:
        return builder(fn)
    else:
        return builder
