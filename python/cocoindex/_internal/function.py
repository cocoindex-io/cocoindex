from __future__ import annotations

import ast
import asyncio
import functools
import hashlib
import importlib
import inspect
import pickle
import textwrap
import threading
from typing import (
    Callable,
    Any,
    Concatenate,
    Generic,
    TypeVar,
    ParamSpec,
    Coroutine,
    Protocol,
    cast,
    overload,
    TypeAlias,
    Literal,
    Awaitable,
    TYPE_CHECKING,
)

from cocoindex._internal.environment import Environment, get_event_loop_or_default

from . import core
from .runner import Runner, in_subprocess as _in_subprocess

from .component_ctx import (
    ComponentContext,
    _context_var,
    get_context_from_ctx,
)
from .memo_key import fingerprint_call


P = ParamSpec("P")
R = TypeVar("R")
R_co = TypeVar("R_co", covariant=True)
P0 = ParamSpec("P0")

# TypeVars for batched function signature transformation
T = TypeVar("T")  # Input element type
U = TypeVar("U")  # Output element type
SelfT = TypeVar("SelfT")  # For method's self parameter


AsyncCallable: TypeAlias = Callable[P, Coroutine[Any, Any, R_co]]
AnyCallable: TypeAlias = Callable[P, R_co] | AsyncCallable[P, R_co]


# ============================================================================
# Type protocols for batched function decorators
# ============================================================================


if TYPE_CHECKING:

    class _AsyncBatchedDecorator(Protocol):
        """Protocol for batched function decorator used by @cocoindex.function.

        Only accepts async underlying functions, since @cocoindex.function preserves
        sync/async and batching requires an async interface.

        Transforms:
        - Async: Callable[[list[T]], Awaitable[list[U]]] -> Callable[[T], Awaitable[U]]

        For methods (functions with self parameter), the type transformation
        is handled at runtime via descriptor protocol, but static typing is less
        precise. The decorated method will work correctly when called on an instance.
        """

        # Async standalone functions (single list[T] parameter)
        @overload
        def __call__(
            self, fn: Callable[[list[T]], Awaitable[list[U]]]
        ) -> AsyncFunction[[T], U]: ...
        # Sync standalone functions (single list[T] parameter) - still returns AsyncFunction
        @overload
        def __call__(
            self, fn: Callable[[list[T]], list[U]]
        ) -> AsyncFunction[[T], U]: ...
        # Methods with self parameter
        def __call__(self, fn: Any) -> Any: ...

    class _BatchedDecorator(Protocol):
        """Protocol for batched function decorator used by @cocoindex.asyncio.function.

        Accepts both sync and async underlying functions, since @cocoindex.asyncio.function
        always ensures the result is async.

        Transforms:
        - Sync: Callable[[list[T]], list[U]] -> Callable[[T], Awaitable[U]]
        - Async: Callable[[list[T]], Awaitable[list[U]]] -> Callable[[T], Awaitable[U]]

        For methods (functions with self parameter), the type transformation
        is handled at runtime via descriptor protocol, but static typing is less
        precise. The decorated method will work correctly when called on an instance.
        """

        # Async standalone functions (single list[T] parameter)
        @overload
        def __call__(
            self, fn: Callable[[list[T]], Awaitable[list[U]]]
        ) -> AsyncFunction[[T], U]: ...
        # Sync standalone functions (single list[T] parameter) - still returns AsyncFunction
        @overload
        def __call__(
            self, fn: Callable[[list[T]], list[U]]
        ) -> AsyncFunction[[T], U]: ...
        # Methods with self parameter
        @overload
        def __call__(  # type: ignore[overload-overlap]
            self, fn: Callable[[SelfT, list[T]], Awaitable[list[U]]]
        ) -> AsyncFunction[[SelfT, T], U]: ...
        @overload
        def __call__(  # type: ignore[overload-overlap]
            self, fn: Callable[[SelfT, list[T]], list[U]]
        ) -> AsyncFunction[[SelfT, T], U]: ...
        def __call__(self, fn: Any) -> Any: ...


class Function(Protocol[P, R_co]):
    def _core_processor(
        self: Function[P0, R_co],
        env: Environment,
        path: core.StablePath,
        *args: P0.args,
        **kwargs: P0.kwargs,
    ) -> core.ComponentProcessor[R_co]: ...


def _has_self_parameter(fn: Callable[..., Any]) -> bool:
    """Check if function has 'self' as first parameter (i.e., is a method)."""
    sig = inspect.signature(fn)
    params = list(sig.parameters.values())
    if not params:
        return False
    first = params[0]
    return first.name == "self" and first.kind in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    )


# ============================================================================
# Sync Function
# ============================================================================


def _build_sync_core_processor(
    fn: Callable[P0, R_co],
    env: Environment,
    path: core.StablePath,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    processor_info: core.ComponentProcessorInfo,
    memo_fp: core.Fingerprint | None = None,
    logic_fp: core.Fingerprint | None = None,
) -> core.ComponentProcessor[R_co]:
    def _build(comp_ctx: core.ComponentProcessorContext) -> R_co:
        fn_ctx = core.FnCallContext()
        if logic_fp is not None:
            fn_ctx.add_logic_dep(logic_fp)
        context = ComponentContext(env, path, comp_ctx, fn_ctx)
        tok = _context_var.set(context)
        try:
            return fn(*args, **kwargs)
        finally:
            _context_var.reset(tok)
            comp_ctx.join_fn_call(fn_ctx)

    return core.ComponentProcessor.new_sync(_build, processor_info, memo_fp)


def _strip_docstring(body: list[ast.stmt]) -> None:
    """Remove leading docstring from a function/class body in-place."""
    if (
        body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
        and isinstance(body[0].value.value, str)
    ):
        body.pop(0)


def _compute_logic_fingerprint(
    fn: Callable[..., Any], *, version: int | None = None
) -> core.Fingerprint:
    """Compute a fingerprint from the function's canonical AST.

    Uses AST instead of raw source text so that comment, whitespace,
    formatting, and docstring changes do not cause false cache invalidations.
    Falls back to bytecode hashing when source is unavailable.

    When *version* is provided, it is used as the canonical representation
    instead of the AST — bumping version forces re-execution.

    The fully-qualified module + qualname is always included so that
    identical function bodies in different modules don't collide.
    """
    if version is not None:
        canonical = f"<version>({version})"
    else:
        try:
            source = textwrap.dedent(inspect.getsource(fn))
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    node.decorator_list = []
                    _strip_docstring(node.body)
            canonical = ast.dump(tree, include_attributes=False, annotate_fields=True)
        except (OSError, SyntaxError):
            canonical = f"<bytecode>{hashlib.sha256(fn.__code__.co_code).hexdigest()}"
    payload = f"{fn.__module__}.{fn.__qualname__}\n{canonical}"
    return core.fingerprint_str(payload)


class SyncFunction(Function[P, R_co]):
    """Sync function with optional memoization.

    Does not support batching or runner — those require an async interface
    and produce AsyncFunction (via @cocoindex.asyncio.function).
    """

    __slots__ = ("_fn", "_memo", "_processor_info", "_logic_fp")

    _fn: Callable[P, R_co]
    _memo: bool
    _processor_info: core.ComponentProcessorInfo
    _logic_fp: core.Fingerprint

    def __init__(
        self, fn: Callable[P, R_co], *, memo: bool, version: int | None = None
    ):
        self._fn = fn
        self._memo = memo
        self._processor_info = core.ComponentProcessorInfo(fn.__qualname__)
        self._logic_fp = _compute_logic_fingerprint(fn, version=version)
        core.register_logic_fingerprint(self._logic_fp)

    def __del__(self) -> None:
        fp = getattr(self, "_logic_fp", None)
        if fp is not None:
            core.unregister_logic_fingerprint(fp)

    @overload
    def __get__(self, instance: None, owner: type) -> SyncFunction[P, R_co]: ...
    @overload
    def __get__(
        self: SyncFunction[Concatenate[SelfT, P0], R_co],
        instance: SelfT,
        owner: type[SelfT] | None = None,
    ) -> _BoundSyncMethod[SelfT]: ...
    def __get__(
        self, instance: SelfT | None, owner: type | None = None
    ) -> _BoundSyncMethod[SelfT] | SyncFunction[P, R_co]:
        """Descriptor protocol for method binding."""
        if instance is None:
            return self
        return _BoundSyncMethod(self, instance)  # type: ignore[arg-type]

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R_co:
        # In subprocess, execute the raw function directly (no memo)
        if _in_subprocess():
            return self._fn(*args, **kwargs)

        parent_ctx = get_context_from_ctx()
        if parent_ctx is None:
            return self._fn(*args, **kwargs)

        def _call_in_context(ctx: core.FnCallContext) -> R_co:
            context = parent_ctx._with_fn_call_ctx(ctx)
            tok = _context_var.set(context)
            try:
                return self._fn(*args, **kwargs)
            finally:
                _context_var.reset(tok)

        fn_ctx: core.FnCallContext | None = None
        try:
            if self._memo:
                memo_fp = fingerprint_call(self._fn, args, kwargs)
                r = core.reserve_memoization(parent_ctx._core_processor_ctx, memo_fp)
                if isinstance(r, core.PendingFnCallMemo):
                    try:
                        fn_ctx = core.FnCallContext()
                        fn_ctx.add_logic_dep(self._logic_fp)
                        ret = _call_in_context(fn_ctx)
                        if r.resolve(fn_ctx, ret):
                            parent_ctx._core_fn_call_ctx.join_child_memo(memo_fp)
                        return ret
                    finally:
                        r.close()
                else:
                    parent_ctx._core_fn_call_ctx.join_child_memo(memo_fp)
                    return cast(R_co, r)
            else:
                fn_ctx = core.FnCallContext()
                fn_ctx.add_logic_dep(self._logic_fp)
                return _call_in_context(fn_ctx)
        finally:
            if fn_ctx is not None:
                parent_ctx._core_fn_call_ctx.join_child(fn_ctx)

    def _core_processor(
        self: SyncFunction[P0, R_co],
        env: Environment,
        path: core.StablePath,
        *args: P0.args,
        **kwargs: P0.kwargs,
    ) -> core.ComponentProcessor[R_co]:
        memo_fp = fingerprint_call(self._fn, args, kwargs) if self._memo else None
        return _build_sync_core_processor(
            self._fn,
            env,
            path,
            args,
            kwargs,
            self._processor_info,
            memo_fp,
            self._logic_fp,
        )


class _BoundSyncMethod(Generic[SelfT]):
    """Bound method wrapper for SyncFunction."""

    __slots__ = ("_func", "_instance")

    def __init__(
        self, func: SyncFunction[Concatenate[SelfT, ...], Any], instance: SelfT
    ):
        self._func = func
        self._instance = instance

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._func(self._instance, *args, **kwargs)


# ============================================================================
# Async Function
# ============================================================================


def _build_async_core_processor(
    fn: AsyncCallable[P0, R_co],
    env: Environment,
    path: core.StablePath,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    processor_info: core.ComponentProcessorInfo,
    memo_fp: core.Fingerprint | None = None,
    logic_fp: core.Fingerprint | None = None,
) -> core.ComponentProcessor[R_co]:
    async def _build(comp_ctx: core.ComponentProcessorContext) -> R_co:
        fn_ctx = core.FnCallContext()
        if logic_fp is not None:
            fn_ctx.add_logic_dep(logic_fp)
        context = ComponentContext(env, path, comp_ctx, fn_ctx)
        tok = _context_var.set(context)
        try:
            return await fn(*args, **kwargs)
        finally:
            _context_var.reset(tok)
            comp_ctx.join_fn_call(fn_ctx)

    return core.ComponentProcessor.new_async(_build, processor_info, memo_fp)


# Cache for expensive self objects in subprocess (keyed by pickle bytes).
# This avoids re-initializing objects like SentenceTransformerEmbedder
# (which loads models) on every subprocess call.
_self_obj_cache: dict[bytes, Any] = {}
_self_obj_cache_lock = threading.Lock()


class _BoundAsyncMethod(Generic[SelfT]):
    """Bound method wrapper for AsyncFunction with batching/runner."""

    __slots__ = ("_func", "_instance")

    def __init__(
        self, func: AsyncFunction[Concatenate[SelfT, ...], Any], instance: SelfT
    ):
        self._func = func
        self._instance = instance

    def __reduce__(self) -> tuple[Any, ...]:
        return _BoundAsyncMethod._unpickle, (
            self._func,
            pickle.dumps(self._instance, protocol=pickle.HIGHEST_PROTOCOL),
        )

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self._func(self._instance, *args, **kwargs)

    async def _execute_orig_async_fn(self, *args: Any, **kwargs: Any) -> Any:
        return await self._func._execute_orig_async_fn(self._instance, *args, **kwargs)

    def _execute_orig_sync_fn(self, *args: Any, **kwargs: Any) -> Any:
        return self._func._execute_orig_sync_fn(self._instance, *args, **kwargs)

    @staticmethod
    def _unpickle(
        func: AsyncFunction[Concatenate[SelfT, ...], Any], self_obj_bytes: bytes
    ) -> _BoundAsyncMethod[SelfT]:
        with _self_obj_cache_lock:
            self_obj = _self_obj_cache.get(self_obj_bytes, None)
            if self_obj is None:
                self_obj = pickle.loads(self_obj_bytes)
                _self_obj_cache[self_obj_bytes] = self_obj
        return _BoundAsyncMethod(func, self_obj)


class AsyncFunction(Function[P, R_co]):
    """Async function with optional memoization and batching/runner support."""

    __slots__ = (
        "_orig_async_fn",
        "_orig_sync_fn",
        "_fn_is_async",
        "_memo",
        "_processor_info",
        "_logic_fp",
        "_batching",
        "_max_batch_size",
        "_runner",
        "_has_self",
        "_queues",
        "_batchers",
        "_batchers_lock",
    )

    _orig_async_fn: AsyncCallable[..., Any] | None
    _orig_sync_fn: Callable[..., Any] | None
    _memo: bool
    _processor_info: core.ComponentProcessorInfo
    _batching: bool
    _max_batch_size: int | None
    _runner: Runner | None
    _has_self: bool
    _queues: dict[object, core.BatchQueue]

    _batchers: dict[object, core.Batcher[Any, R_co]]
    _batchers_lock: threading.Lock

    def __init__(
        self,
        async_fn: AsyncCallable[..., Any] | None,
        sync_fn: Callable[..., Any] | None,
        *,
        memo: bool,
        batching: bool = False,
        max_batch_size: int | None = None,
        runner: Runner | None = None,
        version: int | None = None,
    ) -> None:
        fn = async_fn or sync_fn
        if fn is None:
            raise ValueError("Either async_fn or sync_fn must be provided")
        self._orig_async_fn = async_fn
        self._orig_sync_fn = sync_fn
        self._memo = memo
        self._processor_info = core.ComponentProcessorInfo(fn.__qualname__)
        self._logic_fp = _compute_logic_fingerprint(fn, version=version)
        core.register_logic_fingerprint(self._logic_fp)
        self._batching = batching
        self._max_batch_size = max_batch_size
        self._runner = runner
        self._has_self = _has_self_parameter(fn) if (batching or runner) else False
        self._queues = {}
        self._batchers = {}
        self._batchers_lock = threading.Lock()

    def __del__(self) -> None:
        fp = getattr(self, "_logic_fp", None)
        if fp is not None:
            core.unregister_logic_fingerprint(fp)

    @property
    def _any_fn(self) -> AnyCallable[P, R_co]:
        if self._orig_async_fn is not None:
            return self._orig_async_fn
        else:
            assert self._orig_sync_fn is not None
            return self._orig_sync_fn

    def __reduce__(self) -> tuple[Any, ...]:
        fn = (
            self._orig_async_fn
            if self._orig_async_fn is not None
            else self._orig_sync_fn
        )
        assert fn is not None
        return AsyncFunction._unpickle, (fn.__module__, fn.__qualname__)

    @staticmethod
    def _unpickle(module_name: str, qualname: str) -> AsyncFunction[P, R_co]:
        module = importlib.import_module(module_name)
        return functools.reduce(getattr, qualname.split("."), module)  # type: ignore[arg-type]

    @overload
    def __get__(self, instance: None, owner: type) -> AsyncFunction[P, R_co]: ...
    @overload
    def __get__(
        self: AsyncFunction[Concatenate[SelfT, P0], R_co],
        instance: SelfT,
        owner: type[SelfT] | None = None,
    ) -> _BoundAsyncMethod[SelfT]: ...
    def __get__(
        self, instance: SelfT | None, owner: type | None = None
    ) -> _BoundAsyncMethod[SelfT] | AsyncFunction[P, R_co]:
        """Descriptor protocol for method binding (only for batching/runner)."""
        if instance is None:
            return self
        return _BoundAsyncMethod(self, instance)  # type: ignore[arg-type]

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R_co:
        """Core implementation."""

        parent_ctx = _context_var.get(None)
        pending_memo: core.PendingFnCallMemo | None = None
        memo_fp: core.Fingerprint | None = None
        fn_ctx = core.FnCallContext()
        fn_ctx.add_logic_dep(self._logic_fp)

        try:
            # Check memo (when enabled and context available)
            if self._memo and parent_ctx is not None:
                memo_fp = fingerprint_call(self._any_fn, args, kwargs)
                r = await core.reserve_memoization_async(
                    parent_ctx._core_processor_ctx, memo_fp
                )
                if not isinstance(r, core.PendingFnCallMemo):
                    parent_ctx._core_fn_call_ctx.join_child_memo(memo_fp)
                    return cast(R_co, r)
                pending_memo = r

            # Execute
            if parent_ctx is None:
                async_ctx = core.AsyncContext(get_event_loop_or_default())
                result = await self._execute(async_ctx, *args, **kwargs)
            else:
                comp_ctx = parent_ctx._with_fn_call_ctx(fn_ctx)
                tok = _context_var.set(comp_ctx)
                try:
                    result = await self._execute(
                        parent_ctx._env.async_context, *args, **kwargs
                    )
                finally:
                    _context_var.reset(tok)

            # Resolve memo if pending
            if pending_memo is not None:
                if pending_memo.resolve(fn_ctx, result):
                    assert parent_ctx is not None and memo_fp is not None
                    parent_ctx._core_fn_call_ctx.join_child_memo(memo_fp)

            return result
        finally:
            if pending_memo is not None:
                pending_memo.close()
            if fn_ctx is not None and parent_ctx is not None:
                parent_ctx._core_fn_call_ctx.join_child(fn_ctx)

    async def _execute(
        self,
        async_ctx: core.AsyncContext,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R_co:
        """Execute via batcher/runner."""
        if not self._is_scheduled:
            if self._orig_async_fn is not None:
                return await self._orig_async_fn(*args, **kwargs)  # type: ignore
            else:
                assert self._orig_sync_fn is not None
                return await asyncio.to_thread(self._orig_sync_fn, *args, **kwargs)

        if self._has_self:
            if len(args) < 1:
                raise ValueError("Expected self argument")
            self_obj = args[0]
            actual_args = args[1:]
        else:
            self_obj = None
            actual_args = args

        # Parse args based on mode
        if self._batching:
            # Batching mode: single input element, no kwargs
            if kwargs:
                raise ValueError("Batched functions do not support keyword arguments")
            if len(actual_args) < 1:
                raise ValueError("Expected at least one input argument")
            input_val = actual_args[0]
        else:
            # Runner-only mode: wrap (args, kwargs) as single input
            input_val = (actual_args, kwargs)

        batcher = self._get_or_create_batcher(async_ctx, self_obj)
        return await batcher.run(input_val)

    async def _execute_orig_async_fn(self, *args: Any, **kwargs: Any) -> Any:
        assert self._orig_async_fn is not None
        return await self._orig_async_fn(*args, **kwargs)

    def _execute_orig_sync_fn(self, *args: Any, **kwargs: Any) -> Any:
        assert self._orig_sync_fn is not None
        return self._orig_sync_fn(*args, **kwargs)

    def _create_batch_runner_fn(
        self, self_obj: Any
    ) -> AnyCallable[[list[Any]], list[R_co]]:
        """Create the batch execution function.

        Always returns an async function (or sync for Batcher.new_sync).
        Handles both sync and async underlying functions.
        """
        if self._runner is not None:
            # Choose appropriate callable and runner method based on underlying fn type
            bound_fn_obj = self.__get__(self_obj)
            batch_callable, runner_run = (
                (bound_fn_obj._execute_orig_async_fn, self._runner.run)
                if self._orig_async_fn is not None
                else (bound_fn_obj._execute_orig_sync_fn, self._runner.run_sync_fn)
            )
            if self._batching:

                async def runner_batch_fn_async(inputs: list[Any]) -> list[R_co]:
                    return await runner_run(batch_callable, inputs)  # type: ignore
            else:

                async def runner_batch_fn_async(inputs: list[Any]) -> list[R_co]:
                    args, kwargs = inputs[0]
                    return [await runner_run(batch_callable, *args, **kwargs)]  # type: ignore[arg-type]

            return runner_batch_fn_async

        # No runner - use local closures (no pickling needed)
        assert self._batching, "No runner and no batching"

        # User function is a batch function: list[T] -> list[R]
        if self_obj is None:
            return self._any_fn  # type: ignore

        if (orig_async_fn := self._orig_async_fn) is not None:

            async def batch_fn_async_self(inputs: list[Any]) -> list[Any]:
                return await orig_async_fn(self_obj, inputs)  # type: ignore

            return batch_fn_async_self
        else:
            orig_sync_fn = self._orig_sync_fn
            assert orig_sync_fn is not None
            return lambda inputs: orig_sync_fn(self_obj, inputs)  # type: ignore

    @property
    def _is_scheduled(self) -> bool:
        """Whether this function uses batching or runner."""
        return self._batching or self._runner is not None

    def _get_batcher_key(self, self_obj: Any) -> object:
        """Key for batcher lookup (different from queue_id)."""
        if self_obj is not None:
            return (id(self._any_fn), id(self_obj))
        else:
            return id(self._any_fn)

    def _get_or_create_batcher(
        self, async_ctx: core.AsyncContext, self_obj: Any
    ) -> core.Batcher[Any, R_co]:
        """Get or create batcher for this function/self combination."""
        batcher_key = self._get_batcher_key(self_obj)

        with self._batchers_lock:
            if (batcher := self._batchers.get(batcher_key, None)) is not None:
                return batcher

            batch_runner_fn = self._create_batch_runner_fn(self_obj)

            # Get queue: from runner (if present) or owned by this function
            if self._runner is not None:
                queue = self._runner.get_queue()
            else:
                if batcher_key not in self._queues:
                    self._queues[batcher_key] = core.BatchQueue()
                queue = self._queues[batcher_key]

            # When runner is specified without batching, use max_batch_size=1
            # to process items individually through the shared queue.
            options = core.BatchingOptions(
                max_batch_size=self._max_batch_size if self._batching else 1
            )
            if inspect.iscoroutinefunction(batch_runner_fn):
                batcher = core.Batcher.new_async(
                    queue, options, batch_runner_fn, async_ctx
                )
            else:
                batcher = core.Batcher.new_sync(
                    queue,
                    options,
                    batch_runner_fn,  # type: ignore[arg-type]
                    async_ctx,
                )

            self._batchers[batcher_key] = batcher
            return batcher

    def _core_processor(
        self,
        env: Environment,
        path: core.StablePath,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> core.ComponentProcessor[R_co]:
        memo_fp = (
            fingerprint_call(self._any_fn, (path, *args), kwargs)
            if self._memo
            else None
        )
        if self._is_scheduled:
            async_ctx = env.async_context
            return _build_async_core_processor(
                lambda *args, **kwargs: self._execute(async_ctx, *args, **kwargs),
                env,
                path,
                args,
                kwargs,
                self._processor_info,
                memo_fp,
                self._logic_fp,
            )

        orig_async_fn = self._orig_async_fn
        if orig_async_fn is not None:
            return _build_async_core_processor(
                orig_async_fn,
                env,
                path,
                args,
                kwargs,
                self._processor_info,
                memo_fp,
                self._logic_fp,
            )

        assert self._orig_sync_fn is not None
        return _build_sync_core_processor(
            self._orig_sync_fn,
            env,
            path,
            args,
            kwargs,
            self._processor_info,
            memo_fp,
            self._logic_fp,
        )


# ============================================================================
# Function Builder and Decorator
# ============================================================================


class _GenericFunctionBuilder:
    def __init__(
        self,
        *,
        memo: bool = False,
        batching: bool = False,
        max_batch_size: int | None = None,
        runner: Runner | None = None,
        version: int | None = None,
    ) -> None:
        self._memo = memo
        self._batching = batching
        self._max_batch_size = max_batch_size
        self._runner = runner
        self._version = version

    def _build_sync(self, fn: Callable[P, R_co]) -> SyncFunction[P, R_co]:
        if self._batching or self._runner is not None:
            raise ValueError(
                "Batching and runner require the function to be async. "
                "Use @cocoindex.asyncio.function instead, or rewrite the function to be async."
            )
        wrapper = SyncFunction(fn, memo=self._memo, version=self._version)
        functools.update_wrapper(wrapper, fn)
        return wrapper

    def _build_async(
        self,
        fn: AnyCallable[P, R_co],
    ) -> AsyncFunction[P, R_co]:
        async_fn, sync_fn = (
            (fn, None) if inspect.iscoroutinefunction(fn) else (None, fn)
        )
        wrapper = AsyncFunction[P, R_co](
            async_fn,
            sync_fn,
            memo=self._memo,
            batching=self._batching,
            max_batch_size=self._max_batch_size,
            runner=self._runner,
            version=self._version,
        )
        functools.update_wrapper(wrapper, fn)
        return wrapper


# Only supports sync function -> sync function
class _SyncFunctionBuilder(_GenericFunctionBuilder):
    def __call__(self, fn: Callable[P, R_co]) -> SyncFunction[P, R_co]:
        if inspect.iscoroutinefunction(fn):
            raise ValueError(
                "Async functions are not supported by @cocoindex.function decorator. "
                "Please use @cocoindex.asyncio.function instead."
            )
        return self._build_sync(fn)


# Supports sync function -> sync function and async function -> async function
class _AutoFunctionBuilder(_GenericFunctionBuilder):
    def __init__(self, *, memo: bool = False, version: int | None = None) -> None:
        super().__init__(memo=memo, version=version)

    @overload
    def __call__(  # type: ignore[overload-overlap]
        self, fn: AsyncCallable[P, R_co]
    ) -> AsyncFunction[P, R_co]: ...
    @overload
    def __call__(self, fn: Callable[P, R_co]) -> SyncFunction[P, R_co]: ...
    def __call__(
        self, fn: Callable[P, R_co]
    ) -> AsyncFunction[P, R_co] | SyncFunction[P, R_co]:
        if inspect.iscoroutinefunction(fn):
            return self._build_async(fn)
        return self._build_sync(fn)


# Supports async function -> async function and sync function -> async function
class _AsyncFunctionBuilder(_GenericFunctionBuilder):
    @overload
    def __call__(
        self,
        fn: AsyncCallable[P, R_co],
    ) -> AsyncFunction[P, R_co]: ...
    @overload
    def __call__(
        self,
        fn: Callable[P, R_co],
    ) -> AsyncFunction[P, R_co]: ...
    def __call__(
        self,
        fn: AnyCallable[P, R_co],
    ) -> AsyncFunction[P, R_co]:
        return self._build_async(fn)


# Without batching / runner, supports both sync and async functions
@overload
def function(
    *,
    memo: bool = False,
    version: int | None = None,
) -> _AutoFunctionBuilder: ...
# Overload for batching=True
@overload
def function(
    *,
    memo: bool = False,
    batching: Literal[True],
    max_batch_size: int | None = None,
    runner: Runner | None = None,
    version: int | None = None,
) -> _AsyncBatchedDecorator: ...
# With batching / runner, only supports sync functions
@overload
def function(
    *,
    memo: bool = False,
    batching: Literal[False] = False,
    max_batch_size: int | None = None,
    runner: Runner | None = None,
    version: int | None = None,
) -> _SyncFunctionBuilder: ...
# Overloads for direct function decoration
@overload
def function(  # type: ignore[overload-overlap]
    fn: AsyncCallable[P, R_co], /
) -> AsyncFunction[P, R_co]: ...
@overload
def function(fn: Callable[P, R_co], /) -> SyncFunction[P, R_co]: ...
def function(
    fn: Callable[P, R_co] | None = None,
    /,
    *,
    memo: bool = False,
    batching: bool = False,
    max_batch_size: int | None = None,
    runner: Runner | None = None,
    version: int | None = None,
) -> Any:
    """Decorator for CocoIndex functions (exposed as @cocoindex.function).

    Preserves the sync/async nature of the underlying function:
    - Sync function -> SyncFunction (sync)
    - Async function -> AsyncFunction (async)

    Args:
        fn: The function to decorate (optional, for use without parentheses)
        memo: Enable memoization (skip execution when inputs unchanged)
        batching: Enable batching (function receives list[T], returns list[R])
        max_batch_size: Maximum batch size (only with batching=True)
        runner: Runner to execute the function (e.g., GPU for subprocess)
        version: Explicit version number for change tracking. When specified,
            the version is used as the logic fingerprint instead of the AST.
            Bump this to force re-execution even when code looks the same.

    Batching and runner require an async interface. With this decorator, only sync
    underlying functions are accepted when batching/runner is specified. Use
    @cocoindex.asyncio.function for sync underlying functions that need batching/runner.

    Memoization works with all modes:
        - Without batching/runner: requires ComponentContext
        - With batching/runner: ComponentContext optional, memo checked when available
    """
    builder = (
        _SyncFunctionBuilder(
            memo=memo,
            batching=batching,
            max_batch_size=max_batch_size,
            runner=runner,
            version=version,
        )
        if batching or runner or max_batch_size is not None
        else _AutoFunctionBuilder(memo=memo, version=version)
    )
    if fn is not None:
        return builder(fn)
    else:
        return builder


# Overload for batching=True
@overload
def async_function(
    *,
    memo: bool = False,
    batching: Literal[True],
    max_batch_size: int | None = None,
    runner: Runner | None = None,
    version: int | None = None,
) -> _BatchedDecorator: ...


# Overload for keyword-only args without batching
@overload
def async_function(
    *,
    memo: bool = False,
    batching: Literal[False] = False,
    max_batch_size: int | None = None,
    runner: Runner | None = None,
    version: int | None = None,
) -> _AsyncFunctionBuilder: ...


# Overloads for direct function decoration
@overload
def async_function(
    fn: AsyncCallable[P, R_co],
    /,
) -> AsyncFunction[P, R_co]: ...
@overload
def async_function(
    fn: Callable[P, R_co],
    /,
) -> AsyncFunction[P, R_co]: ...
def async_function(
    fn: Any = None,
    /,
    *,
    memo: bool = False,
    batching: bool = False,
    max_batch_size: int | None = None,
    runner: Runner | None = None,
    version: int | None = None,
) -> Any:
    """Decorator for CocoIndex functions (exposed as @cocoindex.asyncio.function).

    Always yields an async function, equivalent to @cocoindex.function plus ensuring
    the result is async. Accepts both sync and async underlying functions.

    Args:
        fn: The function to decorate (optional, for use without parentheses)
        memo: Enable memoization (skip execution when inputs unchanged)
        batching: Enable batching (function receives list[T], returns list[R])
        max_batch_size: Maximum batch size (only with batching=True)
        runner: Runner to execute the function (e.g., GPU for subprocess)
        version: Explicit version number for change tracking. When specified,
            the version is used as the logic fingerprint instead of the AST.
            Bump this to force re-execution even when code looks the same.

    Batching and runner are fully supported since the result is always async.

    Memoization works with all modes:
        - Without batching/runner: requires ComponentContext
        - With batching/runner: ComponentContext optional, memo checked when available
    """
    builder = _AsyncFunctionBuilder(
        memo=memo,
        batching=batching,
        max_batch_size=max_batch_size,
        runner=runner,
        version=version,
    )
    if fn is not None:
        return builder(fn)
    else:
        return builder


def create_core_component_processor(
    fn: AnyCallable[P, R_co],
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
            cast(Callable[P, R_co], fn),
            env,
            path,
            args,
            kwargs,
            processor_info,
        )
