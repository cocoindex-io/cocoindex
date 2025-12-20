import asyncio
import functools

from typing import (
    Callable,
    Any,
    Concatenate,
    TypeVar,
    ParamSpec,
    Awaitable,
    Coroutine,
    Protocol,
    overload,
)

from . import core

from .scope import Scope
from .runtime import execution_context, is_coroutine_fn, get_async_context


P = ParamSpec("P")
R = TypeVar("R")
R_co = TypeVar("R_co", covariant=True)
P0 = ParamSpec("P0")


class Function(Protocol[P, R_co]):
    def call(self, *args: P.args, **kwargs: P.kwargs) -> R_co: ...

    def acall(self, *args: P.args, **kwargs: P.kwargs) -> Awaitable[R_co]: ...

    def _as_core_component_processor(
        self: "Function[Concatenate[Scope, P0], R_co]",
        path: core.StablePath,
        *args: P0.args,
        **kwargs: P0.kwargs,
    ) -> core.ComponentProcessor: ...


class SyncFunction(Function[P, R_co]):
    _fn: Callable[P, R_co]

    def __init__(self, fn: Callable[P, R_co]):
        self._fn = fn

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R_co:
        return self._fn(*args, **kwargs)

    def _as_core_component_processor(
        self: "Function[Concatenate[Scope, P0], R_co]",
        path: core.StablePath,
        *args: P0.args,
        **kwargs: P0.kwargs,
    ) -> core.ComponentProcessor:
        def _build(builder_ctx: core.ComponentProcessorContext) -> R_co:
            scope = Scope(path, builder_ctx)
            return self._fn(scope, *args, **kwargs)  # type: ignore

        return core.ComponentProcessor.new_sync(_build)

    def call(self, *args: P.args, **kwargs: P.kwargs) -> R_co:
        return self._fn(*args, **kwargs)

    def acall(self, *args: P.args, **kwargs: P.kwargs) -> Awaitable[R_co]:
        return asyncio.to_thread(self._fn, *args, **kwargs)


class AsyncFunction(Function[P, R_co]):
    _fn: Callable[P, Coroutine[Any, Any, R_co]]

    def __init__(self, fn: Callable[P, Coroutine[Any, Any, R_co]]):
        self._fn = fn

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R_co]:
        return self._fn(*args, **kwargs)

    def _as_core_component_processor(
        self: "Function[Concatenate[Scope, P0], R_co]",
        path: core.StablePath,
        *args: P0.args,
        **kwargs: P0.kwargs,
    ) -> core.ComponentProcessor:
        async def _build(builder_ctx: core.ComponentProcessorContext) -> R_co:
            scope = Scope(path, builder_ctx)
            return await self._fn(scope, *args, **kwargs)  # type: ignore

        return core.ComponentProcessor.new_async(_build, get_async_context())

    def call(self, *args: P.args, **kwargs: P.kwargs) -> R_co:
        return execution_context.run(self._fn(*args, **kwargs))

    def acall(self, *args: P.args, **kwargs: P.kwargs) -> Awaitable[R_co]:
        return self._fn(*args, **kwargs)


class FunctionBuilder:
    def __init__(self) -> None:
        pass

    @overload
    def __call__(  # type: ignore[overload-overlap]
        self,
        fn: Callable[P, Coroutine[Any, Any, R_co]],
    ) -> AsyncFunction[P, R_co]: ...
    @overload
    def __call__(self, fn: Callable[P, R_co]) -> SyncFunction[P, R_co]: ...
    def __call__(
        self, fn: Callable[P, Coroutine[Any, Any, R_co]] | Callable[P, R_co]
    ) -> Function[P, R_co]:
        wrapper: Function[P, R_co]
        if is_coroutine_fn(fn):
            wrapper = AsyncFunction(fn)
        else:
            wrapper = SyncFunction(fn)
        functools.update_wrapper(wrapper, fn)
        return wrapper


@overload
def function() -> FunctionBuilder: ...
@overload
def function(  # type: ignore[overload-overlap]
    fn: Callable[P, Coroutine[Any, Any, R_co]], /
) -> AsyncFunction[P, R_co]: ...
@overload
def function(fn: Callable[P, R_co], /) -> SyncFunction[P, R_co]: ...
def function(fn: Any = None, /) -> Any:
    builder = FunctionBuilder()
    if fn is not None:
        return builder(fn)
    else:
        return builder
