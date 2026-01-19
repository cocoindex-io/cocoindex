from __future__ import annotations

import functools
import inspect
from contextvars import ContextVar
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
    TypeAlias,
)

from cocoindex._internal.environment import Environment

from . import core

from .scope import Scope
from .memo_key import fingerprint_call


P = ParamSpec("P")
R = TypeVar("R")
R_co = TypeVar("R_co", covariant=True)
P0 = ParamSpec("P0")


AsyncCallable: TypeAlias = Callable[P, Coroutine[Any, Any, R_co]]
AnyCallable: TypeAlias = Callable[P, R_co] | AsyncCallable[P, R_co]


_scope_var: ContextVar[Scope] = ContextVar("coco_scope")


def _get_scope_from_args(args: tuple[Any, ...]) -> Scope | None:
    if args and isinstance(args[0], Scope):
        return args[0]
    return None


def _get_scope_from_ctx() -> Scope:
    ctx_var = _scope_var.get(None)
    if ctx_var is not None:
        return ctx_var
    raise RuntimeError(
        "No Scope available. Pass a Scope as the first argument or call this from within an active component context."
    )


class Function(Protocol[P, R_co]):
    def _core_processor(
        self: Function[Concatenate[Scope, P0], R_co],
        env: Environment,
        path: core.StablePath,
        *args: P0.args,
        **kwargs: P0.kwargs,
    ) -> core.ComponentProcessor[R_co]: ...


def _build_sync_core_processor(
    fn: Callable[Concatenate[Scope, P0], R_co],
    env: Environment,
    path: core.StablePath,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    processor_info: core.ComponentProcessorInfo,
    memo_fp: core.Fingerprint | None = None,
) -> core.ComponentProcessor[R_co]:
    def _build(comp_ctx: core.ComponentProcessorContext) -> R_co:
        fn_ctx = core.FnCallContext()
        scope = Scope(env, path, comp_ctx, fn_ctx)
        tok = _scope_var.set(scope)
        try:
            return fn(scope, *args, **kwargs)
        finally:
            _scope_var.reset(tok)
            comp_ctx.join_fn_call(fn_ctx)

    return core.ComponentProcessor.new_sync(_build, processor_info, memo_fp)


class SyncFunction(Function[P, R_co]):
    _fn: Callable[P, R_co]
    _memo: bool
    _processor_info: core.ComponentProcessorInfo

    def __init__(self, fn: Callable[P, R_co], *, memo: bool):
        self._fn = fn
        self._memo = memo
        self._processor_info = core.ComponentProcessorInfo(fn.__qualname__)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R_co:
        scope_arg = _get_scope_from_args(args)
        parent_scope = scope_arg if scope_arg is not None else _get_scope_from_ctx()

        def _call_in_context(ctx: core.FnCallContext) -> R_co:
            scope = parent_scope._with_fn_call_ctx(ctx)
            tok = _scope_var.set(scope)
            try:
                if scope_arg is None:
                    return self._fn(*args, **kwargs)
                else:
                    return self._fn(scope, *args[1:], **kwargs)  # type: ignore[arg-type]
            finally:
                _scope_var.reset(tok)

        fn_ctx: core.FnCallContext | None = None
        try:
            if self._memo:
                memo_fp = fingerprint_call(self._fn, args, kwargs)
                r = core.reserve_memoization(parent_scope._core_processor_ctx, memo_fp)
                if isinstance(r, core.PendingFnCallMemo):
                    try:
                        fn_ctx = core.FnCallContext()
                        ret = _call_in_context(fn_ctx)
                        if r.resolve(fn_ctx, ret):
                            parent_scope._core_fn_call_ctx.join_child_memo(memo_fp)
                        return ret
                    finally:
                        r.close()
                else:
                    parent_scope._core_fn_call_ctx.join_child_memo(memo_fp)
                    return cast(R_co, r)
            else:
                fn_ctx = core.FnCallContext()
                return _call_in_context(fn_ctx)
        finally:
            if fn_ctx is not None:
                parent_scope._core_fn_call_ctx.join_child(fn_ctx)

    def _core_processor(
        self: SyncFunction[Concatenate[Scope, P0], R_co],
        env: Environment,
        path: core.StablePath,
        *args: P0.args,
        **kwargs: P0.kwargs,
    ) -> core.ComponentProcessor[R_co]:
        memo_fp = fingerprint_call(self._fn, args, kwargs) if self._memo else None
        return _build_sync_core_processor(
            self._fn, env, path, args, kwargs, self._processor_info, memo_fp
        )


def _build_async_core_processor(
    fn: AsyncCallable[Concatenate[Scope, P0], R_co],
    env: Environment,
    path: core.StablePath,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    processor_info: core.ComponentProcessorInfo,
    memo_fp: core.Fingerprint | None = None,
) -> core.ComponentProcessor[R_co]:
    async def _build(comp_ctx: core.ComponentProcessorContext) -> R_co:
        fn_ctx = core.FnCallContext()
        scope = Scope(env, path, comp_ctx, fn_ctx)
        tok = _scope_var.set(scope)
        try:
            return await fn(scope, *args, **kwargs)
        finally:
            _scope_var.reset(tok)
            comp_ctx.join_fn_call(fn_ctx)

    return core.ComponentProcessor.new_async(_build, processor_info, memo_fp)


class AsyncFunction(Function[P, R_co]):
    _fn: Callable[P, Coroutine[Any, Any, R_co]]
    _memo: bool
    _processor_info: core.ComponentProcessorInfo

    def __init__(
        self,
        fn: Callable[P, Coroutine[Any, Any, R_co]],
        *,
        memo: bool,
    ):
        self._fn = fn
        self._memo = memo
        self._processor_info = core.ComponentProcessorInfo(fn.__qualname__)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R_co:
        scope_arg = _get_scope_from_args(args)
        parent_scope = scope_arg if scope_arg is not None else _get_scope_from_ctx()

        async def _call_in_context(ctx: core.FnCallContext) -> R_co:
            scope = parent_scope._with_fn_call_ctx(ctx)
            tok = _scope_var.set(scope)
            try:
                if scope_arg is None:
                    return await self._fn(*args, **kwargs)
                else:
                    return await self._fn(scope, *args[1:], **kwargs)  # type: ignore[arg-type]
            finally:
                _scope_var.reset(tok)

        fn_ctx: core.FnCallContext | None = None
        try:
            if self._memo:
                memo_fp = fingerprint_call(self._fn, args, kwargs)
                comp_ctx = parent_scope._core_processor_ctx
                r = await core.reserve_memoization_async(comp_ctx, memo_fp)
                if isinstance(r, core.PendingFnCallMemo):
                    try:
                        fn_ctx = core.FnCallContext()
                        ret = await _call_in_context(fn_ctx)
                        if r.resolve(fn_ctx, ret):
                            parent_scope._core_fn_call_ctx.join_child_memo(memo_fp)
                        return ret
                    finally:
                        r.close()
                else:
                    parent_scope._core_fn_call_ctx.join_child_memo(memo_fp)
                    return cast(R_co, r)
            else:
                fn_ctx = core.FnCallContext()
                return await _call_in_context(fn_ctx)
        finally:
            if fn_ctx is not None:
                parent_scope._core_fn_call_ctx.join_child(fn_ctx)

    def _core_processor(
        self: AsyncFunction[Concatenate[Scope, P0], R_co],
        env: Environment,
        path: core.StablePath,
        *args: P0.args,
        **kwargs: P0.kwargs,
    ) -> core.ComponentProcessor[R_co]:
        memo_fp = (
            fingerprint_call(self._fn, (path, *args), kwargs) if self._memo else None
        )
        return _build_async_core_processor(
            self._fn, env, path, args, kwargs, self._processor_info, memo_fp
        )


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


def create_core_component_processor(
    fn: AnyCallable[Concatenate[Scope, P], R_co],
    env: Environment,
    path: core.StablePath,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    /,
) -> core.ComponentProcessor[R_co]:
    if (as_processor := getattr(fn, "_core_processor", None)) is not None:
        return as_processor(env, path, *args, **kwargs)  # type: ignore[no-any-return]

    # For non-decorated functions, create a new ComponentProcessorInfo each time.
    # This is less efficient than using the decorated version which shares the same instance.
    processor_info = core.ComponentProcessorInfo(fn.__qualname__)
    if inspect.iscoroutinefunction(fn):
        return _build_async_core_processor(fn, env, path, args, kwargs, processor_info)
    else:
        return _build_sync_core_processor(
            cast(Callable[Concatenate[Scope, P], R_co], fn),
            env,
            path,
            args,
            kwargs,
            processor_info,
        )
