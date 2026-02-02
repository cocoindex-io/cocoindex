from __future__ import annotations

import functools
import inspect
import threading
from typing import (
    Callable,
    Any,
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
from .runner import Runner

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

# Type alias for Rust Batcher type (mypy doesn't know about it)
_Batcher = Any


# ============================================================================
# Type protocols for batched function decorators
# ============================================================================


if TYPE_CHECKING:

    class _BatchedDecorator(Protocol):
        """Protocol for batched function decorator.

        Transforms:
        - Sync: Callable[[list[T]], list[U]] -> Callable[[T], U]
        - Async: Callable[[list[T]], Awaitable[list[U]]] -> Callable[[T], Awaitable[U]]

        Note: For methods (functions with self parameter), the type transformation
        is handled at runtime via descriptor protocol, but static typing is less
        precise. The decorated method will work correctly when called on an instance.
        """

        # Async standalone functions (single list[T] parameter)
        @overload
        def __call__(
            self, fn: Callable[[list[T]], Awaitable[list[U]]]
        ) -> AsyncFunction[[T], U]: ...
        # Sync standalone functions (single list[T] parameter)
        @overload
        def __call__(
            self, fn: Callable[[list[T]], list[U]]
        ) -> SyncFunction[[T], U]: ...
        # Fallback for methods with self parameter (less precise typing)
        # These overlap with above but handle multi-parameter functions like methods
        @overload
        def __call__(  # type: ignore[overload-overlap]
            self, fn: Callable[..., Awaitable[list[U]]]
        ) -> AsyncFunction[..., U]: ...
        @overload
        def __call__(  # type: ignore[overload-overlap]
            self, fn: Callable[..., list[U]]
        ) -> SyncFunction[..., U]: ...
        def __call__(self, fn: Any) -> Any: ...


def _create_batcher(
    queue: core.BatchQueue,
    options: core.BatchingOptions,
    batch_runner_fn: Callable[[list[Any]], Any],
    parent_ctx: ComponentContext | None,
) -> core.Batcher:
    """Create a batcher that uses the given queue with the specified runner function."""
    async_ctx = (
        parent_ctx._env.async_context
        if parent_ctx is not None
        else core.AsyncContext(get_event_loop_or_default())
    )
    is_async = inspect.iscoroutinefunction(batch_runner_fn)
    if is_async:
        return core.Batcher.new_async(queue, options, batch_runner_fn, async_ctx)
    else:
        return core.Batcher.new_sync(queue, options, batch_runner_fn, async_ctx)


# ============================================================================
# Function base class
# ============================================================================


class Function(Generic[P, R_co]):
    """Base class for sync and async functions with optional batching/runner support."""

    _fn: Callable[..., Any]
    _memo: bool
    _processor_info: core.ComponentProcessorInfo
    _batching: bool
    _max_batch_size: int | None
    _runner: Runner | None
    _has_self: bool
    _queues: dict[object, core.BatchQueue]
    _batchers: dict[object, _Batcher]
    _lock: threading.Lock

    def __init__(
        self,
        fn: Callable[..., Any],
        *,
        memo: bool,
        batching: bool = False,
        max_batch_size: int | None = None,
        runner: Runner | None = None,
    ) -> None:
        self._fn = fn
        self._memo = memo
        self._processor_info = core.ComponentProcessorInfo(fn.__qualname__)
        self._batching = batching
        self._max_batch_size = max_batch_size
        self._runner = runner
        self._has_self = _has_self_parameter(fn) if (batching or runner) else False
        self._queues = {}
        self._batchers = {}
        self._lock = threading.Lock()

    @property
    def _is_scheduled(self) -> bool:
        """Whether this function uses batching or runner."""
        return self._batching or self._runner is not None

    def _get_batcher_key(self, self_obj: Any) -> object:
        """Key for batcher lookup (different from queue_id)."""
        if self_obj is not None:
            return (id(self._fn), id(self_obj))
        else:
            return id(self._fn)

    def _get_or_create_batcher(
        self, parent_ctx: ComponentContext | None, self_obj: Any
    ) -> _Batcher:
        """Get or create batcher for this function/self combination."""
        batcher_key = self._get_batcher_key(self_obj)

        with self._lock:
            if batcher_key not in self._batchers:
                batch_runner_fn = self._create_batch_runner_fn(self_obj)

                # Get queue: from runner (if present) or owned by this function
                if self._runner is not None:
                    queue = self._runner.get_queue()
                else:
                    if batcher_key not in self._queues:
                        self._queues[batcher_key] = core.BatchQueue()
                    queue = self._queues[batcher_key]

                options = core.BatchingOptions(max_batch_size=self._max_batch_size)
                batcher = _create_batcher(queue, options, batch_runner_fn, parent_ctx)

                self._batchers[batcher_key] = batcher
            return self._batchers[batcher_key]

    def _create_batch_runner_fn(self, self_obj: Any) -> Callable[[list[Any]], Any]:
        """Create the batch execution function. Subclasses must implement."""
        raise NotImplementedError

    def _core_processor(
        self: Function[P0, R_co],
        env: Environment,
        path: core.StablePath,
        *args: P0.args,
        **kwargs: P0.kwargs,
    ) -> core.ComponentProcessor[R_co]:
        """Create a core component processor. Subclasses must implement."""
        raise NotImplementedError


def _build_sync_core_processor(
    fn: Callable[P0, R_co],
    env: Environment,
    path: core.StablePath,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    processor_info: core.ComponentProcessorInfo,
    memo_fp: core.Fingerprint | None = None,
) -> core.ComponentProcessor[R_co]:
    def _build(comp_ctx: core.ComponentProcessorContext) -> R_co:
        fn_ctx = core.FnCallContext()
        context = ComponentContext(env, path, comp_ctx, fn_ctx)
        tok = _context_var.set(context)
        try:
            return fn(*args, **kwargs)
        finally:
            _context_var.reset(tok)
            comp_ctx.join_fn_call(fn_ctx)

    return core.ComponentProcessor.new_sync(_build, processor_info, memo_fp)


def _build_async_core_processor(
    fn: AsyncCallable[P0, R_co],
    env: Environment,
    path: core.StablePath,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    processor_info: core.ComponentProcessorInfo,
    memo_fp: core.Fingerprint | None = None,
) -> core.ComponentProcessor[R_co]:
    async def _build(comp_ctx: core.ComponentProcessorContext) -> R_co:
        fn_ctx = core.FnCallContext()
        context = ComponentContext(env, path, comp_ctx, fn_ctx)
        tok = _context_var.set(context)
        try:
            return await fn(*args, **kwargs)
        finally:
            _context_var.reset(tok)
            comp_ctx.join_fn_call(fn_ctx)

    return core.ComponentProcessor.new_async(_build, processor_info, memo_fp)


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


class SyncFunction(Function[P, R_co]):
    """Sync function with optional memoization and batching/runner support."""

    def __get__(
        self, instance: Any, owner: type | None = None
    ) -> _BoundSyncMethod[R_co] | SyncFunction[P, R_co]:
        """Descriptor protocol for method binding (only for batching/runner)."""
        if instance is None or not self._is_scheduled:
            return self
        return _BoundSyncMethod(self, instance)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R_co:
        return self._call(None, args, kwargs)

    def _call(
        self, self_obj: Any, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> R_co:
        """Core implementation. self_obj is the bound instance for methods."""
        parent_ctx = _context_var.get(None)
        pending_memo: core.PendingFnCallMemo | None = None
        memo_fp: core.Fingerprint | None = None
        fn_ctx: core.FnCallContext | None = None

        try:
            # Check memo (when enabled and context available)
            if self._memo and parent_ctx is not None:
                # Include self_obj in fingerprint for methods
                fp_args = (self_obj, *args) if self_obj is not None else args
                memo_fp = fingerprint_call(self._fn, fp_args, kwargs)
                r = core.reserve_memoization(parent_ctx._core_processor_ctx, memo_fp)
                if not isinstance(r, core.PendingFnCallMemo):
                    parent_ctx._core_fn_call_ctx.join_child_memo(memo_fp)
                    return cast(R_co, r)
                pending_memo = r

            # Execute
            if self._is_scheduled:
                result = self._execute_scheduled(parent_ctx, self_obj, args, kwargs)
            else:
                result, fn_ctx = self._execute_direct(args, kwargs)

            # Resolve memo if pending
            if pending_memo is not None:
                resolve_ctx = fn_ctx if fn_ctx is not None else core.FnCallContext()
                if pending_memo.resolve(resolve_ctx, result):
                    assert parent_ctx is not None and memo_fp is not None
                    parent_ctx._core_fn_call_ctx.join_child_memo(memo_fp)

            return result
        finally:
            if pending_memo is not None:
                pending_memo.close()
            if fn_ctx is not None and parent_ctx is not None:
                parent_ctx._core_fn_call_ctx.join_child(fn_ctx)

    def _execute_direct(
        self, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> tuple[R_co, core.FnCallContext]:
        """Execute directly with context propagation."""
        parent_ctx = get_context_from_ctx()
        fn_ctx = core.FnCallContext()
        context = parent_ctx._with_fn_call_ctx(fn_ctx)
        tok = _context_var.set(context)
        try:
            result = self._fn(*args, **kwargs)
            return result, fn_ctx  # type: ignore[return-value]
        finally:
            _context_var.reset(tok)

    def _execute_scheduled(
        self,
        parent_ctx: ComponentContext | None,
        self_obj: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> R_co:
        """Execute via batcher/runner."""
        # Parse args based on mode
        if self._batching:
            # Batching mode: single input element, no kwargs
            if kwargs:
                raise ValueError("Batched functions do not support keyword arguments")
            if len(args) < 1:
                raise ValueError("Expected at least one input argument")
            input_val = args[0]
        else:
            # Runner-only mode: wrap (args, kwargs) as single input
            input_val = (args, kwargs)

        batcher = self._get_or_create_batcher(parent_ctx, self_obj)
        return batcher.run_sync(input_val)  # type: ignore[no-any-return]

    def _create_batch_runner_fn(
        self, self_obj: Any
    ) -> Callable[[list[Any]], list[Any]]:
        """Create the batch execution function."""
        fn = self._fn

        if self._batching:
            # User function is a batch function: list[T] -> list[R]
            if self._has_self:

                def batch_fn(inputs: list[Any]) -> list[Any]:
                    return fn(self_obj, inputs)  # type: ignore[no-any-return]

            else:

                def batch_fn(inputs: list[Any]) -> list[Any]:
                    return fn(inputs)  # type: ignore[no-any-return]

        else:
            # Runner-only mode: input is (args, kwargs) tuple
            # Process each item individually (max_batch_size=1)
            if self._has_self:

                def batch_fn(inputs: list[Any]) -> list[Any]:
                    results = []
                    for args, kwargs in inputs:
                        results.append(fn(self_obj, *args, **kwargs))
                    return results

            else:

                def batch_fn(inputs: list[Any]) -> list[Any]:
                    results = []
                    for args, kwargs in inputs:
                        results.append(fn(*args, **kwargs))
                    return results

        if self._runner is not None:
            runner = self._runner

            def runner_batch_fn(inputs: list[Any]) -> list[Any]:
                return runner.run_sync_fn(batch_fn, inputs)  # type: ignore[no-any-return]

            return runner_batch_fn
        else:
            return batch_fn

    def _core_processor(
        self: SyncFunction[P0, R_co],
        env: Environment,
        path: core.StablePath,
        *args: P0.args,
        **kwargs: P0.kwargs,
    ) -> core.ComponentProcessor[R_co]:
        memo_fp = fingerprint_call(self._fn, args, kwargs) if self._memo else None
        return _build_sync_core_processor(
            self._fn, env, path, args, kwargs, self._processor_info, memo_fp
        )


class _BoundSyncMethod(Generic[R_co]):
    """Bound method wrapper for SyncFunction with batching/runner."""

    def __init__(self, func: SyncFunction[Any, R_co], instance: Any):
        self._func = func
        self._instance = instance

    def __call__(self, *args: Any, **kwargs: Any) -> R_co:
        return self._func._call(self._instance, args, kwargs)


# ============================================================================
# Async Function
# ============================================================================


class _BoundAsyncMethod(Generic[R_co]):
    """Bound method wrapper for AsyncFunction with batching/runner."""

    def __init__(self, func: AsyncFunction[Any, R_co], instance: Any):
        self._func = func
        self._instance = instance

    async def __call__(self, *args: Any, **kwargs: Any) -> R_co:
        return await self._func._call(self._instance, args, kwargs)


class AsyncFunction(Function[P, R_co]):
    """Async function with optional memoization and batching/runner support."""

    def __get__(
        self, instance: Any, owner: type | None = None
    ) -> _BoundAsyncMethod[R_co] | AsyncFunction[P, R_co]:
        """Descriptor protocol for method binding (only for batching/runner)."""
        if instance is None or not self._is_scheduled:
            return self
        return _BoundAsyncMethod(self, instance)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R_co:
        return await self._call(None, args, kwargs)

    async def _call(
        self, self_obj: Any, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> R_co:
        """Core implementation. self_obj is the bound instance for methods."""
        parent_ctx = _context_var.get(None)
        pending_memo: core.PendingFnCallMemo | None = None
        memo_fp: core.Fingerprint | None = None
        fn_ctx: core.FnCallContext | None = None

        try:
            # Check memo (when enabled and context available)
            if self._memo and parent_ctx is not None:
                # Include self_obj in fingerprint for methods
                fp_args = (self_obj, *args) if self_obj is not None else args
                memo_fp = fingerprint_call(self._fn, fp_args, kwargs)
                r = await core.reserve_memoization_async(
                    parent_ctx._core_processor_ctx, memo_fp
                )
                if not isinstance(r, core.PendingFnCallMemo):
                    parent_ctx._core_fn_call_ctx.join_child_memo(memo_fp)
                    return cast(R_co, r)
                pending_memo = r

            # Execute
            if self._is_scheduled:
                result = await self._execute_scheduled(
                    parent_ctx, self_obj, args, kwargs
                )
            else:
                result, fn_ctx = await self._execute_direct(args, kwargs)

            # Resolve memo if pending
            if pending_memo is not None:
                resolve_ctx = fn_ctx if fn_ctx is not None else core.FnCallContext()
                if pending_memo.resolve(resolve_ctx, result):
                    assert parent_ctx is not None and memo_fp is not None
                    parent_ctx._core_fn_call_ctx.join_child_memo(memo_fp)

            return result
        finally:
            if pending_memo is not None:
                pending_memo.close()
            if fn_ctx is not None and parent_ctx is not None:
                parent_ctx._core_fn_call_ctx.join_child(fn_ctx)

    async def _execute_direct(
        self, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> tuple[R_co, core.FnCallContext]:
        """Execute directly with context propagation."""
        parent_ctx = get_context_from_ctx()
        fn_ctx = core.FnCallContext()
        context = parent_ctx._with_fn_call_ctx(fn_ctx)
        tok = _context_var.set(context)
        try:
            result = await self._fn(*args, **kwargs)
            return result, fn_ctx  # type: ignore[return-value]
        finally:
            _context_var.reset(tok)

    async def _execute_scheduled(
        self,
        comp_ctx: ComponentContext | None,
        self_obj: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> R_co:
        """Execute via batcher/runner."""
        # Parse args based on mode
        if self._batching:
            # Batching mode: single input element, no kwargs
            if kwargs:
                raise ValueError("Batched functions do not support keyword arguments")
            if len(args) < 1:
                raise ValueError("Expected at least one input argument")
            input_val = args[0]
        else:
            # Runner-only mode: wrap (args, kwargs) as single input
            input_val = (args, kwargs)

        batcher = self._get_or_create_batcher(comp_ctx, self_obj)
        return await batcher.run(input_val)  # type: ignore[no-any-return]

    def _create_batch_runner_fn(self, self_obj: Any) -> Callable[[list[Any]], Any]:
        """Create the batch execution function."""
        fn = self._fn

        if self._batching:
            # User function is an async batch function: list[T] -> list[R]
            if self._has_self:

                async def batch_fn(inputs: list[Any]) -> list[Any]:
                    return await fn(self_obj, inputs)  # type: ignore[no-any-return]

            else:

                async def batch_fn(inputs: list[Any]) -> list[Any]:
                    return await fn(inputs)  # type: ignore[no-any-return]

        else:
            # Runner-only mode: input is (args, kwargs) tuple
            # Process each item individually (max_batch_size=1)
            if self._has_self:

                async def batch_fn(inputs: list[Any]) -> list[Any]:
                    results = []
                    for args, kwargs in inputs:
                        results.append(await fn(self_obj, *args, **kwargs))
                    return results

            else:

                async def batch_fn(inputs: list[Any]) -> list[Any]:
                    results = []
                    for args, kwargs in inputs:
                        results.append(await fn(*args, **kwargs))
                    return results

        if self._runner is not None:
            runner = self._runner
            async_batch_fn = batch_fn

            async def runner_batch_fn(inputs: list[Any]) -> list[Any]:
                return await runner.run(async_batch_fn, inputs)  # type: ignore[arg-type]

            return runner_batch_fn
        else:
            return batch_fn

    def _core_processor(
        self: AsyncFunction[P0, R_co],
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


# ============================================================================
# Legacy aliases for backwards compatibility
# ============================================================================

# These are kept for any code that might reference them directly
ScheduledSyncFunction = SyncFunction
ScheduledAsyncFunction = AsyncFunction


# ============================================================================
# Function Builder and Decorator
# ============================================================================


class FunctionBuilder:
    def __init__(
        self,
        *,
        memo: bool = False,
        batching: bool = False,
        max_batch_size: int | None = None,
        runner: Runner | None = None,
    ) -> None:
        self._memo = memo
        self._batching = batching
        self._max_batch_size = max_batch_size
        self._runner = runner

    @overload
    def __call__(  # type: ignore[overload-overlap]
        self,
        fn: Callable[P, Coroutine[Any, Any, R_co]],
    ) -> AsyncFunction[P, R_co]: ...
    @overload
    def __call__(  # type: ignore[overload-overlap]
        self, fn: Callable[P, R_co]
    ) -> SyncFunction[P, R_co]: ...
    def __call__(
        self,
        fn: Callable[P, Coroutine[Any, Any, R_co]] | Callable[P, R_co],
    ) -> SyncFunction[P, R_co] | AsyncFunction[P, R_co]:
        wrapper: Any

        # When runner is specified without batching, use max_batch_size=1
        # to process items individually through the shared queue.
        max_batch_size = self._max_batch_size
        if not self._batching and self._runner is not None:
            max_batch_size = 1

        if inspect.iscoroutinefunction(fn):
            wrapper = AsyncFunction(
                fn,
                memo=self._memo,
                batching=self._batching,
                max_batch_size=max_batch_size,
                runner=self._runner,
            )
        else:
            wrapper = SyncFunction(
                fn,
                memo=self._memo,
                batching=self._batching,
                max_batch_size=max_batch_size,
                runner=self._runner,
            )

        functools.update_wrapper(wrapper, fn)
        return wrapper  # type: ignore[no-any-return]


# Overload for batching=True without fn (returns decorator that transforms list[T] -> T)
@overload
def function(
    fn: None = None,
    /,
    *,
    batching: Literal[True],
    max_batch_size: int | None = None,
    memo: bool = False,
    runner: Runner | None = None,
) -> _BatchedDecorator: ...


# Overload for keyword-only args without batching=True
@overload
def function(
    fn: None = None,
    /,
    *,
    memo: bool = False,
    batching: Literal[False] = False,
    max_batch_size: int | None = None,
    runner: Runner | None = None,
) -> FunctionBuilder: ...


# Overload for direct async function decoration
@overload
def function(  # type: ignore[overload-overlap]
    fn: Callable[P, Coroutine[Any, Any, R_co]],
    /,
    *,
    memo: bool = False,
    batching: Literal[False] = False,
    max_batch_size: int | None = None,
    runner: Runner | None = None,
) -> AsyncFunction[P, R_co]: ...


# Overload for direct sync function decoration
@overload
def function(  # type: ignore[overload-overlap]
    fn: Callable[P, R_co],
    /,
    *,
    memo: bool = False,
    batching: Literal[False] = False,
    max_batch_size: int | None = None,
    runner: Runner | None = None,
) -> SyncFunction[P, R_co]: ...
def function(
    fn: Any = None,
    /,
    *,
    memo: bool = False,
    batching: bool = False,
    max_batch_size: int | None = None,
    runner: Runner | None = None,
) -> Any:
    """Decorator for CocoIndex functions.

    Args:
        fn: The function to decorate (optional, for use without parentheses)
        memo: Enable memoization (skip execution when inputs unchanged)
        batching: Enable batching (function receives list[T], returns list[R])
        max_batch_size: Maximum batch size (only with batching=True)
        runner: Runner to execute the function (e.g., GPU for subprocess)

    When batching is enabled:
        - The function should take list[T] as input and return list[R]
        - The external signature becomes T -> R (single input, single output)
        - Multiple concurrent calls are batched together

    When runner is specified:
        - The function executes via the runner (e.g., in subprocess for GPU)
        - All functions using the same runner share a queue
        - If batching is not enabled, items are processed individually

    Memoization works with all modes:
        - Without batching/runner: requires ComponentContext
        - With batching/runner: ComponentContext optional, memo checked when available
    """
    builder = FunctionBuilder(
        memo=memo,
        batching=batching,
        max_batch_size=max_batch_size,
        runner=runner,
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
