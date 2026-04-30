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
import typing
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Concatenate,
    Coroutine,
    Generic,
    Literal,
    Mapping,
    NamedTuple,
    ParamSpec,
    Protocol,
    TypeAlias,
    TypeVar,
    cast,
    overload,
)

from cocoindex._internal.environment import Environment, get_event_loop_or_default

from . import core
from .component_ctx import (
    _context_var,
    _enter_component_context,
    get_context_from_ctx,
)
from .context_keys import resolve_awaitables_sync
from .memo_fingerprint import StateFnEntry, fingerprint_call, memo_fingerprint
from .runner import Runner
from .runner import in_subprocess as _in_subprocess
from .serde import (
    DeserializeFn,
    make_deserialize_fn,
    qualified_name,
    unwrap_element_type,
)
from .typing import NOT_SET, NON_EXISTENCE, NotSetType

P = ParamSpec("P")
R = TypeVar("R")
R_co = TypeVar("R_co", covariant=True)
P0 = ParamSpec("P0")

# TypeVars for batched function signature transformation
T = TypeVar("T")  # Input element type
U = TypeVar("U")  # Output element type
SelfT = TypeVar("SelfT")  # For method's self parameter


AsyncCallable: TypeAlias = Callable[P, Coroutine[Any, Any, R_co]]
AnyCallable: TypeAlias = AsyncCallable[P, R_co] | Callable[P, R_co]

LogicTracking: TypeAlias = Literal["full", "self"] | None
MemoKeyTransform: TypeAlias = Callable[[Any], Any]
MemoKeySpec: TypeAlias = Mapping[str, MemoKeyTransform | None] | None
CompiledMemoKeyPositionalSpec: TypeAlias = tuple[
    MemoKeyTransform | None | NotSetType, ...
]


class PreparedMemoKeySpec(NamedTuple):
    """Precompiled memo-key plan for fast runtime application."""

    positional_specs: CompiledMemoKeyPositionalSpec
    keyword_specs: dict[str, MemoKeyTransform | None]
    varargs_override: MemoKeyTransform | None | NotSetType
    varkw_override: MemoKeyTransform | None | NotSetType


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
        # Methods with self parameter
        @overload
        def __call__(  # type: ignore[overload-overlap]
            self, fn: Callable[[SelfT, list[T]], Awaitable[list[U]]]
        ) -> AsyncFunction[[SelfT, T], U]: ...
        def __call__(self, fn: Any) -> Any: ...

    class _BatchedDecorator(Protocol):
        """Protocol for batched function decorator used by @coco.fn.as_async.

        Accepts both sync and async underlying functions, since @coco.fn.as_async
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


class StateMethodsResult(NamedTuple):
    """Result of calling memo state methods.

    Both positional (argument-borne) and context-borne (change-detected context value)
    state methods flow through the same result type: their outcomes are
    aggregated into `can_reuse` / `states_changed`, while the new states are
    kept in separate slots so they can be persisted alongside the existing
    storage shape.
    """

    new_states: list[Any]
    new_context_states: dict[core.Fingerprint, list[Any]]
    can_reuse: bool
    states_changed: bool


class _StateCallResult(NamedTuple):
    prev: Any
    outcome: Any  # MemoStateOutcome


def _call_entries(
    entries: list[StateFnEntry], stored: list[Any] | None
) -> list[_StateCallResult]:
    """Call each state method once with its stored prev (or NON_EXISTENCE).

    Returns a list of :class:`_StateCallResult` with the same length and
    order as *entries*. If *stored* is length-mismatched with *entries*
    (shook-tag invariant violation), it's treated as ``None`` — force
    initial collection rather than indexing into the wrong cells.
    """
    if stored is not None and len(stored) != len(entries):
        stored = None
    results: list[_StateCallResult] = []
    for i, entry in enumerate(entries):
        prev = (
            entry.deserialize_prev(stored[i]) if stored is not None else NON_EXISTENCE
        )
        results.append(_StateCallResult(prev=prev, outcome=entry.call(prev)))
    return results


def _aggregate_results(
    results: list[_StateCallResult],
    stored_present: bool,
    can_reuse: bool,
    states_changed: bool,
) -> tuple[list[Any], bool, bool]:
    """Fold a list of results into (new_states, can_reuse, states_changed).

    ``can_reuse`` is AND-ed with each outcome's ``memo_valid``.
    ``states_changed`` is OR-ed with a per-entry equality check — only when
    a stored prev was present, i.e. not a cache-miss path.
    """
    new_states: list[Any] = []
    for r in results:
        new_states.append(r.outcome.state)
        if not r.outcome.memo_valid:
            can_reuse = False
        if stored_present and r.outcome.state != r.prev:
            states_changed = True
    return new_states, can_reuse, states_changed


def _resolve_results_awaitables_sync(
    results: list[_StateCallResult], running_loop_error_msg: str
) -> None:
    """Bridge sync state fns that returned an awaitable in place."""
    outcomes = [r.outcome for r in results]
    resolved = resolve_awaitables_sync(
        outcomes, running_loop_error_msg=running_loop_error_msg
    )
    if resolved is outcomes:
        return
    for i, val in enumerate(resolved):
        if val is not outcomes[i]:
            results[i] = results[i]._replace(outcome=val)


async def _resolve_results_awaitables_async(
    results: list[_StateCallResult],
) -> None:
    """Bridge async state fns that returned an awaitable in place."""
    awaitable_indices = [
        i for i, r in enumerate(results) if isinstance(r.outcome, Awaitable)
    ]
    if not awaitable_indices:
        return
    resolved = await asyncio.gather(*(results[i].outcome for i in awaitable_indices))
    for idx, val in zip(awaitable_indices, resolved):
        results[idx] = results[idx]._replace(outcome=val)


def _call_state_methods_sync(
    positional_entries: list[StateFnEntry],
    positional_stored: list[Any] | None,
    context_entries: list[tuple[core.Fingerprint, list[StateFnEntry]]] | None = None,
    context_stored: dict[core.Fingerprint, list[Any]] | None = None,
) -> StateMethodsResult:
    """Call state methods synchronously and return a :class:`StateMethodsResult`.

    *can_reuse* is the conjunction of all per-method ``memo_valid`` flags.
    *states_changed* is the disjunction of per-entry equality checks against
    the stored previous states (for chunks where a stored prev is present).

    If any method returns an ``Awaitable`` the awaitables are resolved:
    - Running event loop → raise (suggest ``@coco.fn.as_async``).
    - No loop → ``asyncio.run(asyncio.gather(...))``.
    """
    # Normalize empty stored-context (`{}` or `None`) to `None` so downstream
    # `is not None` checks consistently identify "cache-hit with entries."
    context_stored = context_stored if context_stored else None
    running_loop_error_msg = (
        "Memo state function returned an awaitable from a sync context "
        "with a running event loop. Use @coco.fn.as_async for the "
        "decorated function instead."
    )

    positional_results = _call_entries(positional_entries, positional_stored)
    _resolve_results_awaitables_sync(positional_results, running_loop_error_msg)

    context_results: list[tuple[core.Fingerprint, list[_StateCallResult]]] = []
    if context_entries:
        for fp, entries in context_entries:
            stored_for_fp = (
                context_stored.get(fp) if context_stored is not None else None
            )
            fp_results = _call_entries(entries, stored_for_fp)
            _resolve_results_awaitables_sync(fp_results, running_loop_error_msg)
            context_results.append((fp, fp_results))

    return _aggregate_state_results(
        positional_results,
        positional_stored is not None,
        context_results,
        context_stored,
    )


async def _call_state_methods_async(
    positional_entries: list[StateFnEntry],
    positional_stored: list[Any] | None,
    context_entries: list[tuple[core.Fingerprint, list[StateFnEntry]]] | None = None,
    context_stored: dict[core.Fingerprint, list[Any]] | None = None,
) -> StateMethodsResult:
    """Async variant of :func:`_call_state_methods_sync`."""
    context_stored = context_stored if context_stored else None

    positional_results = _call_entries(positional_entries, positional_stored)
    await _resolve_results_awaitables_async(positional_results)

    context_results: list[tuple[core.Fingerprint, list[_StateCallResult]]] = []
    if context_entries:
        for fp, entries in context_entries:
            stored_for_fp = (
                context_stored.get(fp) if context_stored is not None else None
            )
            fp_results = _call_entries(entries, stored_for_fp)
            await _resolve_results_awaitables_async(fp_results)
            context_results.append((fp, fp_results))

    return _aggregate_state_results(
        positional_results,
        positional_stored is not None,
        context_results,
        context_stored,
    )


def _aggregate_state_results(
    positional_results: list[_StateCallResult],
    positional_stored_present: bool,
    context_results: list[tuple[core.Fingerprint, list[_StateCallResult]]],
    context_stored: dict[core.Fingerprint, list[Any]] | None,
) -> StateMethodsResult:
    """Fold positional + context results into a :class:`StateMethodsResult`.

    Note on the fp-set: the current set of context fps always equals the
    stored set by construction. On cache hit, ``context_entries`` (and
    therefore ``context_results``) is built from stored fps via
    :func:`_collect_context_entries_from_stored`. On cache miss,
    ``context_stored is None`` and per-entry comparison is skipped. No
    fp-set mismatch check is needed.
    """
    new_positional, can_reuse, states_changed = _aggregate_results(
        positional_results, positional_stored_present, True, False
    )

    new_context: dict[core.Fingerprint, list[Any]] = {}
    for fp, fp_results in context_results:
        stored_present = (
            context_stored is not None and context_stored.get(fp) is not None
        )
        values, can_reuse, states_changed = _aggregate_results(
            fp_results, stored_present, can_reuse, states_changed
        )
        new_context[fp] = values

    return StateMethodsResult(
        new_states=new_positional,
        new_context_states=new_context,
        can_reuse=can_reuse,
        states_changed=states_changed,
    )


def _collect_context_entries_from_stored(
    env: Environment,
    stored: dict[core.Fingerprint, list[Any]] | None,
) -> list[tuple[core.Fingerprint, list[StateFnEntry]]]:
    """Build `(fp, state_fns)` pairs corresponding to a stored context-state dict.

    Used on cache-hit validation: for each stored ``fp → states`` entry we
    look up the matching state functions in the env registry.

    Under the shook-tag invariant this lookup always succeeds: if the user
    had removed ``__coco_memo_state__`` from the value's type between runs,
    the canonicalization would produce a different fingerprint (``hook`` vs
    ``shook`` tag) and the entry would already have been invalidated by
    `all_contained_with_env` before we reach this point. If a stored fp has
    no registered state fns we raise — that indicates registry/state drift
    or a bug in the shook-tag machinery.
    """
    if not stored:
        return []
    provider = env.context_provider
    entries: list[tuple[core.Fingerprint, list[StateFnEntry]]] = []
    for fp in stored:
        state_fns = provider.get_context_state_fns(fp)
        if state_fns is None:
            raise RuntimeError(
                f"change fingerprint {fp} is present in a cached memo "
                "entry but has no registered state functions in the current "
                "ContextProvider — this should be unreachable under the "
                "shook-tag canonicalization invariant."
            )
        entries.append((fp, state_fns))
    return entries


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


def _apply_memo_key(
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    memo_key_plan: PreparedMemoKeySpec | None,
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Apply precompiled per-parameter memo-key overrides before fingerprinting.

    Positional arguments remain positional, and keyword arguments remain keyword
    arguments. Parameters absent from *memo_key_plan* pass through unchanged.
    Parameters mapped to ``None`` are excluded from the fingerprint input, and
    parameters mapped to a callable are transformed by that callable.
    """
    if memo_key_plan is None:
        return args, kwargs

    num_fixed = len(memo_key_plan.positional_specs)

    # Separate fixed positional args from varargs
    fixed_args = args[:num_fixed] if len(args) >= num_fixed else args
    varargs = args[num_fixed:] if len(args) > num_fixed else ()

    # Process fixed positional args (may exclude or transform)
    new_fixed_args: list[Any] = []
    for i, arg in enumerate(fixed_args):
        key_fn = memo_key_plan.positional_specs[i]
        if key_fn is NOT_SET:
            new_fixed_args.append(arg)
        elif key_fn is None:
            continue  # Exclude this positional arg
        else:
            new_fixed_args.append(cast(MemoKeyTransform, key_fn)(arg))

    # Apply varargs override if present (whole *args parameter)
    if memo_key_plan.varargs_override is not NOT_SET:
        if memo_key_plan.varargs_override is None:
            # Exclude entire *args
            varargs = ()
        else:
            # Transform entire *args tuple
            varargs = cast(MemoKeyTransform, memo_key_plan.varargs_override)(varargs)
            if not isinstance(varargs, tuple):
                raise TypeError(
                    f"memo_key transform for *args must return tuple, "
                    f"got {type(varargs).__name__}"
                )

    # Combine fixed args and varargs
    final_args = tuple(new_fixed_args) + varargs

    # Process kwargs: separate matched (keyword-only/POSITIONAL_OR_KEYWORD passed as kwarg)
    # from unmatched (extra **kwargs)
    new_kwargs: dict[str, Any] = {}
    unmatched_kwargs: dict[str, Any] = {}

    for key, value in kwargs.items():
        if key in memo_key_plan.keyword_specs:
            key_fn = memo_key_plan.keyword_specs[key]
            if key_fn is None:
                continue  # Exclude this kwarg
            new_kwargs[key] = key_fn(value)
        else:
            unmatched_kwargs[key] = value

    # Apply varkw override if present (whole **kwargs parameter)
    if memo_key_plan.varkw_override is not NOT_SET:
        if memo_key_plan.varkw_override is None:
            # Exclude entire **kwargs
            unmatched_kwargs = {}
        else:
            # Transform entire unmatched kwargs dict
            transformed = cast(MemoKeyTransform, memo_key_plan.varkw_override)(
                unmatched_kwargs
            )
            if not isinstance(transformed, dict):
                raise TypeError(
                    f"memo_key transform for **kwargs must return dict, "
                    f"got {type(transformed).__name__}"
                )
            unmatched_kwargs = transformed

    # Merge matched and unmatched kwargs
    new_kwargs.update(unmatched_kwargs)

    return final_args, new_kwargs


def _normalize_memo_key(
    fn: Callable[..., Any], memo_key: MemoKeySpec
) -> PreparedMemoKeySpec | None:
    """Validate and compile per-parameter memo-key overrides once."""
    if memo_key is None:
        return None

    normalized = dict(memo_key)
    if not normalized:
        return None

    sig = inspect.signature(fn)
    param_names = [param.name for param in sig.parameters.values()]
    unknown = sorted(name for name in normalized if name not in param_names)
    if unknown:
        raise ValueError(
            f"Unknown memo_key parameter(s) for {qualified_name(fn)}(): "
            + ", ".join(unknown)
        )

    for name, transform in normalized.items():
        if transform is not None and not callable(transform):
            raise TypeError(
                f"memo_key[{name!r}] for {qualified_name(fn)}() must be a callable or None"
            )

    positional: list[MemoKeyTransform | None | NotSetType] = []
    varargs_override: MemoKeyTransform | None | NotSetType = NOT_SET
    varkw_override: MemoKeyTransform | None | NotSetType = NOT_SET

    for param in sig.parameters.values():
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            if param.name in normalized:
                positional.append(normalized[param.name])
            else:
                positional.append(NOT_SET)
        elif param.kind == inspect.Parameter.VAR_POSITIONAL:
            if param.name in normalized:
                varargs_override = normalized[param.name]
        elif param.kind == inspect.Parameter.VAR_KEYWORD:
            if param.name in normalized:
                varkw_override = normalized[param.name]

    return PreparedMemoKeySpec(
        tuple(positional), normalized, varargs_override, varkw_override
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
    state_handler: Callable[..., Coroutine[Any, Any, Any]] | None = None,
    propagate_children_fn_logic: bool = True,
) -> core.ComponentProcessor[R_co]:
    def _build(comp_ctx: core.ComponentProcessorContext) -> R_co:
        with _enter_component_context(
            env,
            path,
            comp_ctx,
            propagate_children_fn_logic=propagate_children_fn_logic,
            logic_fp=logic_fp,
        ):
            return fn(*args, **kwargs)

    return core.ComponentProcessor.new_sync(
        _build, processor_info, memo_fp, state_handler
    )


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
    fn: Callable[..., Any],
    *,
    version: int | None = None,
    deps: Any = None,
) -> core.Fingerprint:
    """Compute a fingerprint from the function's canonical AST.

    Uses AST instead of raw source text so that comment, whitespace,
    formatting, and docstring changes do not cause false cache invalidations.
    Falls back to bytecode hashing when source is unavailable.

    When *version* is provided, it is used as the canonical representation
    instead of the AST — bumping version forces re-execution.

    The fully-qualified module + qualname is always included so that
    identical function bodies in different modules don't collide.

    When *deps* is not ``None``, it is canonicalized via the memoization-key
    pipeline and its fingerprint is folded into the payload, so changing the
    value invalidates the result just like a code change does.
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
    if deps is not None:
        # Use an explicit stable encoding (hex of the 16-byte digest) rather
        # than Fingerprint.__str__ — the deps fingerprint ends up embedded in
        # the logic fingerprint that's persisted in memo entries, and we don't
        # want any future change to the Fingerprint Display impl to silently
        # reshape every cached entry that uses deps=. Hex matches the
        # `<bytecode>` fallback above (`hashlib.sha256(...).hexdigest()`).
        payload += f"\n<deps>{memo_fingerprint(deps).as_bytes().hex()}"
    return core.fingerprint_str(payload)


class SyncFunction(Function[P, R_co]):
    """Sync function with optional memoization.

    Does not support batching or runner — those require an async interface
    and produce AsyncFunction (via @coco.fn.as_async).
    """

    __slots__ = (
        "_fn",
        "_memo",
        "_memo_key",
        "_processor_info",
        "_logic_fp",
        "_logic_tracking",
        "_return_deserializer",
        "_return_deserializer_lock",
    )

    _fn: Callable[P, R_co]
    _memo: bool
    _memo_key: PreparedMemoKeySpec | None
    _processor_info: core.ComponentProcessorInfo
    _logic_fp: core.Fingerprint | None
    _logic_tracking: LogicTracking
    _return_deserializer: DeserializeFn | None
    _return_deserializer_lock: threading.Lock

    def __init__(
        self,
        fn: Callable[P, R_co],
        *,
        memo: bool,
        memo_key: MemoKeySpec = None,
        version: int | None = None,
        logic_tracking: LogicTracking = "full",
        deps: Any = None,
    ):
        if logic_tracking is None and deps is not None:
            raise ValueError(
                "deps= requires logic_tracking to be enabled; with "
                "logic_tracking=None the function's logic is not tracked at "
                "all, so the deps value would be silently ignored."
            )
        self._fn = fn
        self._memo = memo
        self._memo_key = _normalize_memo_key(fn, memo_key)
        self._processor_info = core.ComponentProcessorInfo(fn.__qualname__)
        self._logic_tracking = logic_tracking
        self._return_deserializer = None
        self._return_deserializer_lock = threading.Lock()

        if logic_tracking is not None:
            self._logic_fp = _compute_logic_fingerprint(fn, version=version, deps=deps)
            core.register_logic_fingerprint(self._logic_fp)
        else:
            self._logic_fp = None

    @property
    def _resolved_return_deserializer(self) -> DeserializeFn:
        if self._return_deserializer is None:
            with self._return_deserializer_lock:
                if self._return_deserializer is None:
                    try:
                        hint = typing.get_type_hints(self._fn).get("return", Any)
                    except Exception:
                        hint = Any
                    self._return_deserializer = make_deserialize_fn(
                        hint,
                        source_label=f"return type of {qualified_name(self._fn)}()",
                    )
        return self._return_deserializer

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

        propagate = self._logic_tracking == "full"
        fn_ctx: core.FnCallContext | None = None
        try:
            if self._memo:
                state_methods: list[StateFnEntry] = []
                memo_args: tuple[Any, ...] = args  # type: ignore[assignment]
                memo_kwargs: dict[str, Any] = kwargs  # type: ignore[assignment]
                if self._memo_key:
                    memo_args, memo_kwargs = _apply_memo_key(
                        memo_args, memo_kwargs, self._memo_key
                    )
                memo_fp = fingerprint_call(
                    self._fn, memo_args, memo_kwargs, state_methods=state_methods
                )
                guard = core.reserve_memoization(
                    parent_ctx._core_processor_ctx, memo_fp
                )
                env = parent_ctx._env
                try:
                    # Check if cached result is still valid
                    use_cache = False
                    memo_states_for_resolve: list[Any] | None = None
                    context_states_for_resolve: (
                        dict[core.Fingerprint, list[Any]] | None
                    ) = None
                    if guard.is_cached:
                        use_cache = True
                        stored_context_states = guard.cached_context_memo_states
                        context_entries = _collect_context_entries_from_stored(
                            env, stored_context_states
                        )
                        if state_methods or context_entries:
                            state_result = _call_state_methods_sync(
                                state_methods,
                                guard.cached_memo_states,
                                context_entries=context_entries,
                                context_stored=stored_context_states,
                            )
                            if not state_result.can_reuse:
                                use_cache = False
                                # Positional state methods are derived from args
                                # (same across runs), so the validation result is
                                # safe to reuse on the re-execution path.
                                memo_states_for_resolve = state_result.new_states
                                # Context state is re-collected fresh from fn_ctx
                                # below — re-execution may observe a different set
                                # of change-detection context fps than the stored entry,
                                # and validation only covers the stored set.
                            elif state_result.states_changed:
                                guard.update_memo_states(
                                    state_result.new_states,
                                    state_result.new_context_states,
                                )

                    if use_cache:
                        parent_ctx._core_fn_call_ctx.join_child_memo(memo_fp)
                        assert guard.cached_value is not None
                        return cast(
                            R_co,
                            guard.cached_value.get(self._resolved_return_deserializer),
                        )

                    # Execute (cache miss or stale states)
                    fn_ctx = core.FnCallContext(propagate_children_fn_logic=propagate)
                    if self._logic_fp is not None:
                        fn_ctx.add_fn_logic_dep(self._logic_fp)
                    ret = _call_in_context(fn_ctx)
                    # Positional: collect only if not already set (cache-miss path).
                    # Context: look up eager initial states from the Rust
                    # registry in a single call — no state fn calls on cache
                    # miss, and Rust iterates `context_change_deps` without
                    # snapshotting it through Python.
                    if memo_states_for_resolve is None and state_methods:
                        initial = _call_state_methods_sync(state_methods, None)
                        memo_states_for_resolve = initial.new_states
                    fresh_context_states = fn_ctx.initial_context_memo_states(
                        env._core_env
                    )
                    if fresh_context_states:
                        context_states_for_resolve = fresh_context_states
                    if guard.resolve(
                        fn_ctx,
                        ret,
                        memo_states_for_resolve,
                        context_states_for_resolve,
                    ):
                        parent_ctx._core_fn_call_ctx.join_child_memo(memo_fp)
                    return ret
                finally:
                    guard.close()
            else:
                fn_ctx = core.FnCallContext(propagate_children_fn_logic=propagate)
                if self._logic_fp is not None:
                    fn_ctx.add_fn_logic_dep(self._logic_fp)
                return _call_in_context(fn_ctx)
        finally:
            if fn_ctx is not None:
                parent_ctx._core_fn_call_ctx.join_child(fn_ctx)

    async def as_async(self, *args: P.args, **kwargs: P.kwargs) -> R_co:
        """Call this sync function wrapped in async (runs via asyncio.to_thread)."""
        return await asyncio.to_thread(self, *args, **kwargs)

    def _core_processor(
        self: SyncFunction[P0, R_co],
        env: Environment,
        path: core.StablePath,
        *args: P0.args,
        **kwargs: P0.kwargs,
    ) -> core.ComponentProcessor[R_co]:
        state_methods: list[StateFnEntry] = []
        memo_fp: core.Fingerprint | None = None
        if self._memo:
            memo_args: tuple[Any, ...] = args  # type: ignore[assignment]
            memo_kwargs: dict[str, Any] = kwargs  # type: ignore[assignment]
            if self._memo_key:
                memo_args, memo_kwargs = _apply_memo_key(
                    memo_args, memo_kwargs, self._memo_key
                )
            memo_fp = fingerprint_call(
                self._fn,
                memo_args,
                memo_kwargs,
                state_methods=state_methods,
            )
        # Attach the component-level state handler only if there's something
        # for it to do. Two triggers:
        # 1. Positional state methods collected from argument canonicalization.
        # 2. The env has at least one change-detected context value with registered
        #    state functions — any of them *might* be observed during the
        #    function body and need validation at memo-hit time.
        # If neither is true, we can skip the Rust↔Python round-trip that the
        # state handler would otherwise incur on every cache miss.
        #
        # TODO(future simplification): make this handler cache-hit-only.
        # The cache-miss branch below is pure data collection (look up eager
        # initial states for context, call positional state fns with
        # NON_EXISTENCE). Both could be pre-computed at _core_processor time
        # and passed into the Rust processor as new fields, letting
        # execute_once's cache-miss path skip the Rust→Python callback
        # entirely. Not urgent; see "Future simplification" in
        # specs/memo_validation/spec.md and specs/core/internal_states.md.
        state_handler: Callable[..., Coroutine[Any, Any, Any]] | None = None
        if memo_fp is not None and (
            state_methods or env.context_provider.has_any_context_state_fns()
        ):
            captured = state_methods

            async def _state_handler(
                comp_ctx: core.ComponentProcessorContext,
                positional_stored: list[Any] | None,
                context_stored: dict[core.Fingerprint, list[Any]] | None,
            ) -> StateMethodsResult:
                # Note: `_enter_component_context` creates a fresh FnCallContext
                # and joins it into comp_ctx on exit. If state functions call
                # `use_context(...)`, their observed fps flow into
                # comp_ctx.logic_deps — on cache-miss write that's correctly
                # persisted as part of the new entry's logic_deps.
                with _enter_component_context(env, path, comp_ctx):
                    if context_stored is not None:
                        # Cache hit: validate stored context states by calling
                        # the registered state functions with their stored prev.
                        context_entries = _collect_context_entries_from_stored(
                            env, context_stored
                        )
                        return await _call_state_methods_async(
                            captured,
                            positional_stored,
                            context_entries=context_entries,
                            context_stored=context_stored,
                        )
                    # Cache miss: look up the eager initial states for the
                    # context fps observed during function execution, in a
                    # single Rust call — no Python-side iteration over the
                    # logic deps set.
                    # TODO(future simplification): this branch is pure data
                    # collection and should be moved out of the handler so
                    # Rust's execute_once can skip the callback on cache miss.
                    new_context = comp_ctx.initial_context_memo_states()
                    if captured:
                        positional_result = await _call_state_methods_async(
                            captured, None
                        )
                        new_positional = positional_result.new_states
                    else:
                        new_positional = []
                    # can_reuse / states_changed are unread by the core on the
                    # cache-miss path, so their values are conventional.
                    return StateMethodsResult(
                        new_states=new_positional,
                        new_context_states=new_context,
                        can_reuse=True,
                        states_changed=False,
                    )

            state_handler = _state_handler

        return _build_sync_core_processor(
            self._fn,
            env,
            path,
            args,
            kwargs,
            self._processor_info,
            memo_fp,
            self._logic_fp,
            state_handler,
            propagate_children_fn_logic=self._logic_tracking == "full",
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

    def _core_processor(
        self, env: Environment, path: core.StablePath, *args: Any, **kwargs: Any
    ) -> core.ComponentProcessor[Any]:
        return self._func._core_processor(env, path, self._instance, *args, **kwargs)

    async def as_async(self, *args: Any, **kwargs: Any) -> Any:
        """Call this bound sync method wrapped in async (runs via asyncio.to_thread)."""
        return await asyncio.to_thread(self, *args, **kwargs)


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
    state_handler: Callable[..., Coroutine[Any, Any, Any]] | None = None,
    propagate_children_fn_logic: bool = True,
) -> core.ComponentProcessor[R_co]:
    async def _build(comp_ctx: core.ComponentProcessorContext) -> R_co:
        with _enter_component_context(
            env,
            path,
            comp_ctx,
            propagate_children_fn_logic=propagate_children_fn_logic,
            logic_fp=logic_fp,
        ):
            return await fn(*args, **kwargs)

    return core.ComponentProcessor.new_async(
        _build, processor_info, memo_fp, state_handler
    )


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

    def _core_processor(
        self, env: Environment, path: core.StablePath, *args: Any, **kwargs: Any
    ) -> core.ComponentProcessor[Any]:
        return self._func._core_processor(env, path, self._instance, *args, **kwargs)

    async def as_async(self, *args: Any, **kwargs: Any) -> Any:
        """Call this bound async method (same as __call__)."""
        return await self(*args, **kwargs)

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
        "_memo_key",
        "_processor_info",
        "_logic_fp",
        "_logic_tracking",
        "_batching",
        "_max_batch_size",
        "_runner",
        "_has_self",
        "_queues",
        "_batchers",
        "_batchers_lock",
        "_return_deserializer",
        "_return_deserializer_lock",
    )

    _orig_async_fn: AsyncCallable[..., Any] | None
    _orig_sync_fn: Callable[..., Any] | None
    _memo: bool
    _memo_key: PreparedMemoKeySpec | None
    _processor_info: core.ComponentProcessorInfo
    _logic_fp: core.Fingerprint | None
    _logic_tracking: LogicTracking
    _batching: bool
    _max_batch_size: int | None
    _runner: Runner | None
    _has_self: bool
    _queues: dict[object, core.BatchQueue]
    _return_deserializer: DeserializeFn | None
    _return_deserializer_lock: threading.Lock

    _batchers: dict[object, core.Batcher[Any, R_co]]
    _batchers_lock: threading.Lock

    def __init__(
        self,
        async_fn: AsyncCallable[..., Any] | None,
        sync_fn: Callable[..., Any] | None,
        *,
        memo: bool,
        memo_key: MemoKeySpec = None,
        batching: bool = False,
        max_batch_size: int | None = None,
        runner: Runner | None = None,
        version: int | None = None,
        logic_tracking: LogicTracking = "full",
        deps: Any = None,
    ) -> None:
        fn = async_fn or sync_fn
        if fn is None:
            raise ValueError("Either async_fn or sync_fn must be provided")
        if logic_tracking is None and deps is not None:
            raise ValueError(
                "deps= requires logic_tracking to be enabled; with "
                "logic_tracking=None the function's logic is not tracked at "
                "all, so the deps value would be silently ignored."
            )
        self._orig_async_fn = async_fn
        self._orig_sync_fn = sync_fn
        self._memo = memo
        self._memo_key = _normalize_memo_key(fn, memo_key)
        self._processor_info = core.ComponentProcessorInfo(fn.__qualname__)
        self._logic_tracking = logic_tracking
        self._return_deserializer = None
        self._return_deserializer_lock = threading.Lock()

        if logic_tracking is not None:
            self._logic_fp = _compute_logic_fingerprint(fn, version=version, deps=deps)
            core.register_logic_fingerprint(self._logic_fp)
        else:
            self._logic_fp = None
        self._batching = batching
        self._max_batch_size = max_batch_size
        self._runner = runner
        self._has_self = _has_self_parameter(fn) if (batching or runner) else False
        self._queues = {}
        self._batchers = {}
        self._batchers_lock = threading.Lock()

    @property
    def _resolved_return_deserializer(self) -> DeserializeFn:
        if self._return_deserializer is None:
            with self._return_deserializer_lock:
                if self._return_deserializer is None:
                    fn = self._orig_async_fn or self._orig_sync_fn
                    assert fn is not None
                    try:
                        hint = typing.get_type_hints(fn).get("return", Any)
                    except Exception:
                        hint = Any
                    # For batched functions, the return type is list[U] but
                    # individual memoized values are U. Unwrap the element type.
                    if self._batching and hint is not Any:
                        hint = unwrap_element_type(hint)
                    self._return_deserializer = make_deserialize_fn(
                        hint,
                        source_label=f"return type of {qualified_name(fn)}()",
                    )
        return self._return_deserializer

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

    async def as_async(self, *args: P.args, **kwargs: P.kwargs) -> R_co:
        """Call this async function (same as __call__)."""
        return await self(*args, **kwargs)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R_co:
        """Core implementation."""

        parent_ctx = _context_var.get(None)
        guard: core.FnCallMemoGuard | None = None
        memo_fp: core.Fingerprint | None = None
        propagate = self._logic_tracking == "full"
        fn_ctx = core.FnCallContext(propagate_children_fn_logic=propagate)
        if self._logic_fp is not None:
            fn_ctx.add_fn_logic_dep(self._logic_fp)
        state_methods: list[StateFnEntry] = []

        try:
            # Check memo (when enabled and context available)
            memo_states_for_resolve: list[Any] | None = None
            context_states_for_resolve: dict[core.Fingerprint, list[Any]] | None = None
            if self._memo and parent_ctx is not None:
                env = parent_ctx._env
                async_memo_args: tuple[Any, ...] = args  # type: ignore[assignment]
                async_memo_kwargs: dict[str, Any] = kwargs  # type: ignore[assignment]
                if self._memo_key:
                    async_memo_args, async_memo_kwargs = _apply_memo_key(
                        async_memo_args, async_memo_kwargs, self._memo_key
                    )
                memo_fp = fingerprint_call(
                    self._any_fn,
                    async_memo_args,
                    async_memo_kwargs,
                    state_methods=state_methods,
                )
                guard = await core.reserve_memoization_async(
                    parent_ctx._core_processor_ctx, memo_fp
                )
                if guard.is_cached:
                    # Check if cached result is still valid
                    use_cache = True
                    stored_context_states = guard.cached_context_memo_states
                    context_entries = _collect_context_entries_from_stored(
                        env, stored_context_states
                    )
                    if state_methods or context_entries:
                        state_result = await _call_state_methods_async(
                            state_methods,
                            guard.cached_memo_states,
                            context_entries=context_entries,
                            context_stored=stored_context_states,
                        )
                        if not state_result.can_reuse:
                            use_cache = False
                            # Positional states are stable across runs (same
                            # args ⇒ same state method list) — safe to reuse.
                            memo_states_for_resolve = state_result.new_states
                            # Context states are re-collected fresh from fn_ctx
                            # below: re-execution may observe a different set
                            # of change-detection context fps than the stored entry.
                        elif state_result.states_changed:
                            guard.update_memo_states(
                                state_result.new_states,
                                state_result.new_context_states,
                            )
                    if use_cache:
                        parent_ctx._core_fn_call_ctx.join_child_memo(memo_fp)
                        assert guard.cached_value is not None
                        return cast(
                            R_co,
                            guard.cached_value.get(self._resolved_return_deserializer),
                        )

            # Execute (no memo, cache miss, or stale states)
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

            # Resolve memo if guard is held
            if guard is not None:
                assert parent_ctx is not None
                env = parent_ctx._env
                # Positional: only re-collect if validation didn't already set
                # it (cache-miss path). Context: look up eager initial states
                # via a single Rust call — iterates the fn_ctx's change deps
                # and reads the env registry directly.
                if memo_states_for_resolve is None and state_methods:
                    initial = await _call_state_methods_async(state_methods, None)
                    memo_states_for_resolve = initial.new_states
                fresh_context_states = fn_ctx.initial_context_memo_states(env._core_env)
                if fresh_context_states:
                    context_states_for_resolve = fresh_context_states
                if guard.resolve(
                    fn_ctx,
                    result,
                    memo_states_for_resolve,
                    context_states_for_resolve,
                ):
                    assert memo_fp is not None
                    parent_ctx._core_fn_call_ctx.join_child_memo(memo_fp)

            return result
        finally:
            if guard is not None:
                guard.close()
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
            if len(actual_args) < 1:
                raise ValueError("Expected at least one input argument")
            input_val = actual_args[0]
            extra_args = tuple(actual_args[1:])
            extra_kwargs = dict(kwargs)
        else:
            # Runner-only mode: wrap (args, kwargs) as single input
            input_val = (actual_args, kwargs)
            extra_args = ()
            extra_kwargs = {}

        batcher = self._get_or_create_batcher(
            async_ctx, self_obj, extra_args, extra_kwargs
        )
        return await batcher.run(input_val)

    async def _execute_orig_async_fn(self, *args: Any, **kwargs: Any) -> Any:
        assert self._orig_async_fn is not None
        return await self._orig_async_fn(*args, **kwargs)

    def _execute_orig_sync_fn(self, *args: Any, **kwargs: Any) -> Any:
        assert self._orig_sync_fn is not None
        return self._orig_sync_fn(*args, **kwargs)

    def _create_batch_runner_fn(
        self,
        self_obj: Any,
        extra_args: tuple[Any, ...],
        extra_kwargs: dict[str, Any] | None = None,
    ) -> AnyCallable[[list[Any]], list[R_co]]:
        """Create the batch execution function.

        Always returns an async function (or sync for Batcher.new_sync).
        Handles both sync and async underlying functions.
        """
        if extra_kwargs is None:
            extra_kwargs = {}

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
                    return await runner_run(  # type: ignore[no-any-return]
                        batch_callable, inputs, *extra_args, **extra_kwargs
                    )
            else:

                async def runner_batch_fn_async(inputs: list[Any]) -> list[R_co]:
                    args, kwargs = inputs[0]
                    return [await runner_run(batch_callable, *args, **kwargs)]  # type: ignore[arg-type]

            return runner_batch_fn_async

        # No runner - use local closures (no pickling needed)
        assert self._batching, "No runner and no batching"

        # User function is a batch function: list[T] -> list[R]
        if self_obj is None:
            if not extra_args and not extra_kwargs:
                return self._any_fn  # type: ignore
            if (orig_async_fn := self._orig_async_fn) is not None:

                async def batch_fn_async_extra(inputs: list[Any]) -> list[Any]:
                    return await orig_async_fn(inputs, *extra_args, **extra_kwargs)  # type: ignore

                return batch_fn_async_extra
            else:
                orig_sync_fn = self._orig_sync_fn
                assert orig_sync_fn is not None
                return lambda inputs: orig_sync_fn(inputs, *extra_args, **extra_kwargs)  # type: ignore
        if (orig_async_fn := self._orig_async_fn) is not None:

            async def batch_fn_async_self(inputs: list[Any]) -> list[Any]:
                return await orig_async_fn(  # type: ignore[no-any-return]
                    self_obj, inputs, *extra_args, **extra_kwargs
                )

            return batch_fn_async_self
        else:
            orig_sync_fn = self._orig_sync_fn
            assert orig_sync_fn is not None
            return lambda inputs: orig_sync_fn(
                self_obj, inputs, *extra_args, **extra_kwargs
            )  # type: ignore

    @property
    def _is_scheduled(self) -> bool:
        """Whether this function uses batching or runner."""
        return self._batching or self._runner is not None

    def _get_batcher_key(
        self,
        self_obj: Any,
        extra_args: tuple[Any, ...],
        extra_kwargs: tuple[tuple[str, Any], ...],
    ) -> object:
        """Key for batcher lookup (different from queue_id)."""
        if self_obj is not None:
            return (id(self._any_fn), id(self_obj), extra_args, extra_kwargs)
        else:
            return (id(self._any_fn), extra_args, extra_kwargs)

    def _get_or_create_batcher(
        self,
        async_ctx: core.AsyncContext,
        self_obj: Any,
        extra_args: tuple[Any, ...] = (),
        extra_kwargs: dict[str, Any] | None = None,
    ) -> core.Batcher[Any, R_co]:
        """Get or create batcher for this function/self combination."""
        if extra_kwargs is None:
            extra_kwargs = {}
        extra_kwargs_key = tuple(sorted(extra_kwargs.items()))
        batcher_key = self._get_batcher_key(self_obj, extra_args, extra_kwargs_key)
        with self._batchers_lock:
            if (batcher := self._batchers.get(batcher_key, None)) is not None:
                return batcher

            batch_runner_fn = self._create_batch_runner_fn(
                self_obj, extra_args, extra_kwargs
            )

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
        state_methods: list[StateFnEntry] = []
        memo_fp: core.Fingerprint | None = None
        if self._memo:
            async_proc_args: tuple[Any, ...] = args  # type: ignore[assignment]
            async_proc_kwargs: dict[str, Any] = kwargs  # type: ignore[assignment]
            if self._memo_key:
                async_proc_args, async_proc_kwargs = _apply_memo_key(
                    async_proc_args, async_proc_kwargs, self._memo_key
                )
            memo_fp = fingerprint_call(
                self._any_fn,
                async_proc_args,
                async_proc_kwargs,
                state_methods=state_methods,
                prefix_args=(path,),
            )

        # See SyncFunction._core_processor for rationale on the attachment
        # condition — avoids a Python callback round-trip on every cache miss
        # when neither positional nor context state validation is needed.
        #
        # TODO(future simplification): same as SyncFunction — make this
        # handler cache-hit-only by pre-computing initial states at
        # _core_processor time. See specs/memo_validation/spec.md.
        state_handler: Callable[..., Coroutine[Any, Any, Any]] | None = None
        if memo_fp is not None and (
            state_methods or env.context_provider.has_any_context_state_fns()
        ):
            captured = state_methods

            async def _state_handler(
                comp_ctx: core.ComponentProcessorContext,
                positional_stored: list[Any] | None,
                context_stored: dict[core.Fingerprint, list[Any]] | None,
            ) -> StateMethodsResult:
                # See SyncFunction._core_processor for the rationale on the
                # cache-hit vs cache-miss split.
                with _enter_component_context(env, path, comp_ctx):
                    if context_stored is not None:
                        context_entries = _collect_context_entries_from_stored(
                            env, context_stored
                        )
                        return await _call_state_methods_async(
                            captured,
                            positional_stored,
                            context_entries=context_entries,
                            context_stored=context_stored,
                        )
                    # TODO(future simplification): this branch is pure data
                    # collection; should move out of the handler.
                    new_context = comp_ctx.initial_context_memo_states()
                    if captured:
                        positional_result = await _call_state_methods_async(
                            captured, None
                        )
                        new_positional = positional_result.new_states
                    else:
                        new_positional = []
                    return StateMethodsResult(
                        new_states=new_positional,
                        new_context_states=new_context,
                        can_reuse=True,
                        states_changed=False,
                    )

            state_handler = _state_handler

        propagate = self._logic_tracking == "full"
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
                state_handler,
                propagate_children_fn_logic=propagate,
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
                state_handler,
                propagate_children_fn_logic=propagate,
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
            state_handler,
            propagate_children_fn_logic=propagate,
        )


# ============================================================================
# Function Builder and Decorator
# ============================================================================


class _GenericFunctionBuilder:
    def __init__(
        self,
        *,
        memo: bool = False,
        memo_key: MemoKeySpec = None,
        batching: bool = False,
        max_batch_size: int | None = None,
        runner: Runner | None = None,
        version: int | None = None,
        logic_tracking: LogicTracking = "full",
        deps: Any = None,
    ) -> None:
        self._memo = memo
        self._memo_key = memo_key
        self._batching = batching
        self._max_batch_size = max_batch_size
        self._runner = runner
        self._version = version
        self._logic_tracking = logic_tracking
        self._deps = deps

    def _build_sync(self, fn: Callable[P, R_co]) -> SyncFunction[P, R_co]:
        if self._batching or self._runner is not None:
            raise ValueError(
                "Batching and runner require the function to be async. "
                "Use @coco.fn.as_async instead, or rewrite the function to be async."
            )
        wrapper = SyncFunction(
            fn,
            memo=self._memo,
            memo_key=self._memo_key,
            version=self._version,
            logic_tracking=self._logic_tracking,
            deps=self._deps,
        )
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
            memo_key=self._memo_key,
            batching=self._batching,
            max_batch_size=self._max_batch_size,
            runner=self._runner,
            version=self._version,
            logic_tracking=self._logic_tracking,
            deps=self._deps,
        )
        functools.update_wrapper(wrapper, fn)
        return wrapper


# Only supports sync function -> sync function
class _SyncFunctionBuilder(_GenericFunctionBuilder):
    def __call__(self, fn: Callable[P, R_co]) -> SyncFunction[P, R_co]:
        if inspect.iscoroutinefunction(fn):
            raise ValueError(
                "Async functions are not supported by @coco.fn decorator "
                "when batching or runner is specified. "
                "Please use @coco.fn.as_async instead."
            )
        return self._build_sync(fn)


# Supports sync function -> sync function and async function -> async function
class _AutoFunctionBuilder(_GenericFunctionBuilder):
    def __init__(
        self,
        *,
        memo: bool = False,
        memo_key: MemoKeySpec = None,
        version: int | None = None,
        logic_tracking: LogicTracking = "full",
        deps: Any = None,
    ) -> None:
        super().__init__(
            memo=memo,
            memo_key=memo_key,
            version=version,
            logic_tracking=logic_tracking,
            deps=deps,
        )

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


class _FunctionDecorator:
    """Namespace for @coco.fn and @coco.fn.as_async decorators."""

    # --- @coco.fn(...) / @coco.fn ---

    # Without batching / runner, supports both sync and async functions
    @overload
    def __call__(
        self,
        *,
        memo: bool = False,
        memo_key: MemoKeySpec = None,
        version: int | None = None,
        logic_tracking: LogicTracking = "full",
        deps: Any = None,
    ) -> _AutoFunctionBuilder: ...
    # Overload for batching=True
    @overload
    def __call__(
        self,
        *,
        memo: bool = False,
        memo_key: MemoKeySpec = None,
        batching: Literal[True],
        max_batch_size: int | None = None,
        runner: Runner | None = None,
        version: int | None = None,
        logic_tracking: LogicTracking = "full",
        deps: Any = None,
    ) -> _AsyncBatchedDecorator: ...
    # With batching / runner, only supports sync functions
    @overload
    def __call__(
        self,
        *,
        memo: bool = False,
        memo_key: MemoKeySpec = None,
        batching: Literal[False] = False,
        max_batch_size: int | None = None,
        runner: Runner | None = None,
        version: int | None = None,
        logic_tracking: LogicTracking = "full",
        deps: Any = None,
    ) -> _SyncFunctionBuilder: ...
    # Overloads for direct function decoration
    @overload
    def __call__(  # type: ignore[overload-overlap]
        self, fn: AsyncCallable[P, R_co], /
    ) -> AsyncFunction[P, R_co]: ...
    @overload
    def __call__(self, fn: Callable[P, R_co], /) -> SyncFunction[P, R_co]: ...
    def __call__(  # type: ignore[misc]
        self,
        fn: Callable[P, R_co] | None = None,
        /,
        *,
        memo: bool = False,
        memo_key: MemoKeySpec = None,
        batching: bool = False,
        max_batch_size: int | None = None,
        runner: Runner | None = None,
        version: int | None = None,
        logic_tracking: LogicTracking = "full",
        deps: Any = None,
    ) -> Any:
        """Decorator for CocoIndex functions (exposed as @coco.fn).

        Preserves the sync/async nature of the underlying function:
        - Sync function -> SyncFunction (sync)
        - Async function -> AsyncFunction (async)

        Args:
            fn: The function to decorate (optional, for use without parentheses)
            memo: Enable memoization (skip execution when inputs unchanged)
            memo_key: Optional per-parameter memoization key overrides. For a
                parameter name, ``None`` excludes that argument from the memo
                key and a callable maps the runtime value to the value that
                should be fingerprinted for memoization.
            batching: Enable batching (function receives list[T], returns list[R])
            max_batch_size: Maximum batch size (only with batching=True)
            runner: Runner to execute the function (e.g., GPU for subprocess)
            version: Explicit version number for change tracking. When specified,
                the version is used as the logic fingerprint instead of the AST.
                Bump this to force re-execution even when code looks the same.
            logic_tracking: Controls logic change tracking granularity.
                "full" (default): Track own code + transitive children.
                "self": Track own code only, not children.
                None: No function logic tracking (incompatible with ``deps``).
            deps: External value(s) the function logic depends on but which
                aren't visible in its body — for example a module-level prompt
                string or model identifier. The value is canonicalized via the
                memoization-key pipeline (see
                :doc:`/advanced_topics/memoization_keys` for the full contract,
                including ``__coco_memo_key__()`` and registered key functions)
                and folded into the function's logic fingerprint; when the
                canonical form changes, memoized results are invalidated and
                the change propagates to callers according to ``logic_tracking``
                (transitively under ``"full"``, only to the function itself
                under ``"self"``). For multiple dependencies, pass a tuple or
                dict, e.g. ``deps={"prompt": SYSTEM_PROMPT, "model": MODEL_NAME}``.
                ``None`` (the default) means no external dependency.

                **Snapshotted once at decoration time** (typically module
                import), not re-evaluated per call. For per-call or per-instance
                values — instance attributes in a bound method, request-scoped
                config, anything that changes at runtime — pass them as regular
                function arguments instead.

                Requires ``logic_tracking`` to be enabled; raises ``ValueError`` if
                combined with ``logic_tracking=None``.

        Batching and runner require an async interface. With this decorator, only
        async underlying functions are accepted when batching/runner is specified.
        Use @coco.fn.as_async for sync underlying functions that need
        batching/runner.

        Memoization works with all modes:
            - Without batching/runner: requires ComponentContext
            - With batching/runner: ComponentContext optional, memo checked when available
        """
        builder = (
            _SyncFunctionBuilder(
                memo=memo,
                memo_key=memo_key,
                batching=batching,
                max_batch_size=max_batch_size,
                runner=runner,
                version=version,
                logic_tracking=logic_tracking,
                deps=deps,
            )
            if batching or runner or max_batch_size is not None
            else _AutoFunctionBuilder(
                memo=memo,
                memo_key=memo_key,
                version=version,
                logic_tracking=logic_tracking,
                deps=deps,
            )
        )
        if fn is not None:
            return builder(fn)
        else:
            return builder

    # --- @coco.fn.as_async(...) / @coco.fn.as_async ---

    # Overload for batching=True
    @overload
    def as_async(
        self,
        *,
        memo: bool = False,
        memo_key: MemoKeySpec = None,
        batching: Literal[True],
        max_batch_size: int | None = None,
        runner: Runner | None = None,
        version: int | None = None,
        logic_tracking: LogicTracking = "full",
        deps: Any = None,
    ) -> _BatchedDecorator: ...
    # Overload for keyword-only args without batching
    @overload
    def as_async(
        self,
        *,
        memo: bool = False,
        memo_key: MemoKeySpec = None,
        batching: Literal[False] = False,
        max_batch_size: int | None = None,
        runner: Runner | None = None,
        version: int | None = None,
        logic_tracking: LogicTracking = "full",
        deps: Any = None,
    ) -> _AsyncFunctionBuilder: ...
    # Overloads for direct function decoration
    @overload
    def as_async(
        self,
        fn: AsyncCallable[P, R_co],
        /,
    ) -> AsyncFunction[P, R_co]: ...
    @overload
    def as_async(
        self,
        fn: Callable[P, R_co],
        /,
    ) -> AsyncFunction[P, R_co]: ...
    def as_async(  # type: ignore[misc]
        self,
        fn: Any = None,
        /,
        *,
        memo: bool = False,
        memo_key: MemoKeySpec = None,
        batching: bool = False,
        max_batch_size: int | None = None,
        runner: Runner | None = None,
        version: int | None = None,
        logic_tracking: LogicTracking = "full",
        deps: Any = None,
    ) -> Any:
        """Decorator for CocoIndex functions (exposed as @coco.fn.as_async).

        Always yields an async function, equivalent to @coco.fn plus ensuring
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
            logic_tracking: Controls logic change tracking granularity.
                "full" (default): Track own code + transitive children.
                "self": Track own code only, not children.
                None: No function logic tracking (incompatible with ``deps``).
            deps: Additional value(s) the function logic depends on but that
                aren't visible in its body. See :func:`fn` for the full
                contract — the value is canonicalized through the memoization
                key pipeline and folded into the function's logic fingerprint.

        Batching and runner are fully supported since the result is always async.

        Memoization works with all modes:
            - Without batching/runner: requires ComponentContext
            - With batching/runner: ComponentContext optional, memo checked when available
        """
        builder = _AsyncFunctionBuilder(
            memo=memo,
            memo_key=memo_key,
            batching=batching,
            max_batch_size=max_batch_size,
            runner=runner,
            version=version,
            logic_tracking=logic_tracking,
            deps=deps,
        )
        if fn is not None:
            return builder(fn)
        else:
            return builder


fn = _FunctionDecorator()


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


def fn_ret_deserializer(fn: typing.Any) -> DeserializeFn:
    """Return a ``DeserializeFn`` that deserializes *fn*'s return type.

    Zero upfront cost — all work is deferred to the first call.
    For ``@coco.fn``-decorated functions the pre-built ``DeserializeFn`` is reused.
    For plain functions the return-type annotation is inspected at call time.
    """
    # Unwrap bound methods to get the underlying Function object.
    if isinstance(fn, (_BoundSyncMethod, _BoundAsyncMethod)):
        fn = fn._func
    fn_label = qualified_name(fn)

    def _deserialize(data: bytes | memoryview) -> typing.Any:
        cached: DeserializeFn | None = getattr(
            fn, "_resolved_return_deserializer", None
        )
        if cached is not None:
            return cached(data)
        try:
            hint = typing.get_type_hints(fn).get("return", typing.Any)
        except Exception:
            hint = typing.Any
        return make_deserialize_fn(hint, source_label=f"return type of {fn_label}()")(
            data
        )

    return _deserialize
