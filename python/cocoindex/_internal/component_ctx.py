from __future__ import annotations

import contextlib
import inspect
import logging
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    AsyncIterator,
    Awaitable,
    Callable,
    Generator,
    Literal,
    NamedTuple,
    TypeAlias,
    TypeVar,
)

from cocoindex._internal.context_keys import ContextKey
from cocoindex._internal.environment import Environment

from . import core
from .stable_path import StableKey
from .update_stats import StatsGroupHandle, _resolve_report_to_stdout

_logger = logging.getLogger(__name__)

T = TypeVar("T")

ExceptionHandler: TypeAlias = Callable[
    [BaseException, "ExceptionContext"],
    None | Awaitable[None],
]


class ExceptionHandlerChain(NamedTuple):
    handler: ExceptionHandler
    base: ExceptionHandlerChain | None = None


# ContextVar for the current ComponentContext
_context_var: ContextVar[ComponentContext] = ContextVar("coco_component_context")

MountKind: TypeAlias = Literal[
    "mount", "mount_each", "delete_background", "process_live"
]


@dataclass(frozen=True, slots=True)
class ExceptionContext:
    env_name: str
    stable_path: str
    processor_name: str | None
    mount_kind: MountKind
    parent_stable_path: str | None
    is_background: bool
    source: Literal["component", "handler"]
    original_exception: BaseException | None


@dataclass(frozen=True, slots=True)
class ComponentContext:
    """
    Internal context object for component execution.

    This class is NOT exposed to users. It carries:
    - Environment reference
    - Core stable path
    - Processor context for target state declaration
    - Function call context for memoization tracking
    """

    _env: Environment
    _core_path: core.StablePath
    _core_processor_ctx: core.ComponentProcessorContext
    _core_fn_call_ctx: core.FnCallContext
    _exception_handler_chain: ExceptionHandlerChain | None
    _in_memo_fn: bool = False

    def _with_fn_call_ctx(
        self, fn_call_ctx: core.FnCallContext, *, in_memo_fn: bool = False
    ) -> ComponentContext:
        return ComponentContext(
            self._env,
            self._core_path,
            self._core_processor_ctx,
            fn_call_ctx,
            self._exception_handler_chain,
            in_memo_fn,
        )

    def _with_extended_path(self, *parts: StableKey) -> ComponentContext:
        """Create a new context with the path extended by the given parts."""
        new_path = self._core_path
        for part in parts:
            new_path = new_path.concat(part)
        return ComponentContext(
            self._env,
            new_path,
            self._core_processor_ctx,
            self._core_fn_call_ctx,
            self._exception_handler_chain,
        )

    def _with_core_processor_ctx(
        self, core_processor_ctx: core.ComponentProcessorContext
    ) -> ComponentContext:
        """New context reporting through a different core processor ctx (e.g. a
        stats-group view), keeping the same path / fn-call ctx / handler chain."""
        return ComponentContext(
            self._env,
            self._core_path,
            core_processor_ctx,
            self._core_fn_call_ctx,
            self._exception_handler_chain,
        )

    def _with_exception_handler(self, handler: ExceptionHandler) -> ComponentContext:
        return ComponentContext(
            self._env,
            self._core_path,
            self._core_processor_ctx,
            self._core_fn_call_ctx,
            ExceptionHandlerChain(handler=handler, base=self._exception_handler_chain),
        )

    def resolve_exception_handler(
        self,
        *,
        stable_path: str,
        processor_name: str | None,
        mount_kind: MountKind,
    ) -> Callable[[str], Awaitable[None]]:
        """Build the exception-handler resolver for a child mounted under this context.

        Returns a callable that takes a stringified error and:
        - walks this context's handler chain (innermost first);
        - if a handler raises, calls the next outer handler with the
          new exception;
        - on chain exhaustion (every handler re-raised), **re-raises the
          final handler's exception** — the Rust side propagates that
          back through ``handle.ready()``. Handlers therefore control
          propagation: returning swallows; raising propagates;
        - when no handler is registered at all, logs at ``ERROR`` via
          the Python logger and returns normally (today's contract for
          "no chain → not propagated"; ``app.drop`` uses a Rust-side
          raising handler explicitly to surface root-delete failures).

        Always non-None. Single canonical entry point used by ``coco.mount``,
        ``coco.mount_each``, and the live-component operator so all
        component-failure routing goes through the same Python path.
        """

        # Defer all derivations into the closure body so the happy path
        # (no exception) pays only for closure construction. `self` is
        # captured implicitly. `self._core_path.to_string()` in particular
        # is a PyO3 → Rust → string allocation we don't want to pay on
        # every mount.
        async def _run(err_str: str) -> None:
            node = self._exception_handler_chain
            if node is None:
                # No handlers registered — log directly without building
                # the ExceptionContext metadata at all. Don't propagate.
                _logger.error("component build failed:\n%s", err_str)
                return

            env_name = self._env.name
            parent_stable_path = self._core_path.to_string()
            original_exc: BaseException = RuntimeError(err_str)
            current_exc: BaseException = original_exc
            source: Literal["component", "handler"] = "component"
            while node is not None:
                ctx = ExceptionContext(
                    env_name=env_name,
                    stable_path=stable_path,
                    processor_name=processor_name,
                    mount_kind=mount_kind,
                    parent_stable_path=parent_stable_path,
                    is_background=True,
                    source=source,
                    original_exception=None if source == "component" else original_exc,
                )
                try:
                    ret = node.handler(current_exc, ctx)
                    if inspect.isawaitable(ret):
                        await ret
                    return  # Handler swallowed → don't propagate.
                except BaseException as handler_exc:
                    current_exc = handler_exc
                    source = "handler"
                    node = node.base
            # Every handler in the chain raised. Propagate the final
            # raise — Rust's on_error catches it and the spawned task
            # returns Err, so `handle.ready()` raises. Handlers thus
            # control propagation: return (swallow) vs raise (propagate).
            raise current_exc

        return _run

    @contextlib.contextmanager
    def attach(self) -> Generator[None, None, None]:
        """
        Context manager to attach this ComponentContext to the current thread.

        Use this when running code in a ThreadPoolExecutor where context vars
        are not automatically preserved.

        Example:
            component_context = coco.get_component_context()
            with ThreadPoolExecutor() as executor:
                def task():
                    with component_context.attach():
                        # Now coco APIs work correctly
                        ...
                executor.submit(task)
        """
        tok = _context_var.set(self)
        try:
            yield
        finally:
            _context_var.reset(tok)

    def __str__(self) -> str:
        return self._core_path.to_string()

    def __repr__(self) -> str:
        return f"ComponentContext({self._core_path.to_string()})"

    def __coco_memo_key__(self) -> object:
        core_path_memo_key = self._core_path.__coco_memo_key__()
        if self._core_path == self._core_processor_ctx.stable_path:
            return core_path_memo_key
        return (
            core_path_memo_key,
            self._core_processor_ctx.stable_path.__coco_memo_key__(),
        )


class ComponentSubpath:
    """
    Represents a relative path to create a sub-scope.

    Can be:
    - Passed to mount()/use_mount() as the first argument
    - Used as a context manager to apply the subpath to all nested mount calls

    Example:
        with coco.component_subpath("process_file"):
            for f in files:
                coco.mount(coco.component_subpath(str(f.relative_path)), process_file, f, target)

    This is equivalent to:
        for f in files:
            coco.mount(coco.component_subpath("process_file", str(f.relative_path)), process_file, f, target)
    """

    __slots__ = ("_parts", "_token")

    _parts: tuple[StableKey, ...]
    _token: Token[ComponentContext] | None

    def __init__(self, *key_parts: StableKey) -> None:
        self._parts = key_parts
        self._token = None

    @property
    def parts(self) -> tuple[StableKey, ...]:
        return self._parts

    def __enter__(self) -> ComponentSubpath:
        # Create a new ComponentContext with extended path
        current_ctx = get_context_from_ctx()
        new_ctx = current_ctx._with_extended_path(*self._parts)
        self._token = _context_var.set(new_ctx)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if self._token is not None:
            _context_var.reset(self._token)
            self._token = None

    def __truediv__(self, part: StableKey) -> ComponentSubpath:
        """Allows chaining: coco.component_subpath("a") / "b" / "c" """
        return ComponentSubpath(*self._parts, part)

    def __repr__(self) -> str:
        return f"ComponentSubpath({', '.join(repr(p) for p in self._parts)})"


def component_subpath(*key_parts: StableKey) -> ComponentSubpath:
    """
    Create a component subpath for use with mount()/use_mount() or as a context manager.

    Args:
        *key_parts: One or more StableKey values to form the subpath

    Returns:
        A ComponentSubpath that can be passed to mount/use_mount or used as a context manager

    Examples:
        # As first argument to mount
        coco.mount(coco.component_subpath("process", filename), process_file, file, target)

        # As context manager
        with coco.component_subpath("process_file"):
            for f in files:
                coco.mount(coco.component_subpath(str(f.relative_path)), process_file, f, target)
    """
    return ComponentSubpath(*key_parts)


@contextlib.contextmanager
def _enter_component_context(
    env: Environment,
    path: core.StablePath,
    comp_ctx: core.ComponentProcessorContext,
    /,
    *,
    propagate_children_fn_logic: bool = True,
    logic_fp: core.Fingerprint | None = None,
) -> Generator[None, None, None]:
    """Set up ComponentContext in the context var, join fn call on exit.

    Creates a FnCallContext, wraps it in a ComponentContext, sets the context var,
    yields, then resets the context var and joins the fn call into the processor context.
    """
    fn_ctx = core.FnCallContext(propagate_children_fn_logic=propagate_children_fn_logic)
    if logic_fp is not None:
        fn_ctx.add_fn_logic_dep(logic_fp)
    base_chain = (
        ExceptionHandlerChain(handler=env.exception_handler)
        if env.exception_handler
        else None
    )
    context = ComponentContext(env, path, comp_ctx, fn_ctx, base_chain)
    tok = _context_var.set(context)
    try:
        yield
    finally:
        _context_var.reset(tok)
        comp_ctx.join_fn_call(fn_ctx)


def get_context_from_ctx() -> ComponentContext:
    """Get the current ComponentContext from ContextVar."""
    ctx_var = _context_var.get(None)
    if ctx_var is not None:
        return ctx_var
    raise RuntimeError(
        "No ComponentContext available. This function must be called from within "
        "an active component context (inside a mount/use_mount call or App.update)."
    )


def build_child_path(
    parent_ctx: ComponentContext, subpath: ComponentSubpath
) -> core.StablePath:
    """Build the child path from parent context and subpath."""
    child_path = parent_ctx._core_path
    for part in subpath.parts:
        child_path = child_path.concat(part)
    return child_path


def use_context(key: ContextKey[T]) -> T:
    """
    Retrieve a value from the context.

    This replaces the old `scope.use(key)` API.

    Args:
        key: The ContextKey to look up

    Returns:
        The value associated with the key

    Raises:
        RuntimeError: If called outside an active component context
        KeyError: If the key was not provided in the lifespan

    Example:
        PG_DB = coco.ContextKey[postgres.PgDatabase]("pg_db")

        @coco.fn
        def app_main() -> None:
            db = coco.use_context(PG_DB)
            ...
    """
    ctx = get_context_from_ctx()
    value = ctx._env.context_provider.get(key)
    if key.detect_change:
        fp = ctx._env.context_provider.get_fingerprint(key)
        ctx._core_fn_call_ctx.add_context_change_dep(fp)
    return value


def get_component_context() -> ComponentContext:
    """
    Get the current ComponentContext explicitly.

    Use this when you need to pass the context to code that runs
    in a different execution context (e.g., ThreadPoolExecutor).

    Returns:
        The current ComponentContext

    Raises:
        RuntimeError: If called outside an active component context

    Example:
        component_context = coco.get_component_context()
        with ThreadPoolExecutor() as executor:
            def task():
                with component_context.attach():
                    # coco APIs work correctly here
                    coco.mount(...)
            executor.submit(task)
    """
    return get_context_from_ctx()


@contextlib.contextmanager
def stats_group(
    title: str, *, report_to_stdout: bool | timedelta = False
) -> Generator[StatsGroupHandle, None, None]:
    """Aggregate the stats of everything mounted within this scope separately,
    under ``title``, split out of the enclosing report.

    The returned handle mirrors ``UpdateHandle``'s ``stats()`` / ``watch()``.
    Entering and exiting the block only bound *member registration* — exit is
    non-blocking; the group becomes ready asynchronously once the body has
    exited and all members are ready. With ``report_to_stdout`` truthy the group
    is also rendered as plain, title-prefixed log lines (interleaved above the
    progress region when a TTY display is active); pass a ``timedelta`` to set
    the refresh interval.

    This is a plain (synchronous) ``with`` block — like ``component_subpath`` —
    even though the body typically ``await``s ``mount``/``use_mount``.

    Example::

        with coco.stats_group("Indexing docs") as sg:
            await coco.mount_each(process_file, files.items(), target)
        async for snap in sg.watch():
            ...
    """
    current_ctx = get_context_from_ctx()
    report, refresh_interval_secs = _resolve_report_to_stdout(report_to_stdout)
    derived_core_ctx, core_handle = current_ctx._core_processor_ctx.begin_stats_group(
        title, report, refresh_interval_secs
    )
    new_ctx = current_ctx._with_core_processor_ctx(derived_core_ctx)
    tok = _context_var.set(new_ctx)
    try:
        yield StatsGroupHandle(core_handle)
    finally:
        # Mark registration done (non-blocking) and pop the scope, even if the
        # body raised — so the group's readiness/watchers always unblock.
        derived_core_ctx.end_stats_group()
        _context_var.reset(tok)


@contextlib.asynccontextmanager
async def exception_handler(handler: ExceptionHandler) -> AsyncIterator[None]:
    """
    Push an exception handler for background-mounted components within this dynamic scope.
    """
    current_ctx = get_context_from_ctx()
    new_ctx = current_ctx._with_exception_handler(handler)
    tok = _context_var.set(new_ctx)
    try:
        yield
    finally:
        _context_var.reset(tok)


async def next_id(key: StableKey = None) -> int:
    """
    Get the next unique ID for the given key.

    This is an internal function that generates unique IDs within the current app.
    IDs are allocated in batches for efficiency.

    Args:
        key: Optional stable key for the ID sequencer. If None, uses a default sequencer.

    Returns:
        The next unique ID as an integer.

    Raises:
        RuntimeError: If called outside an active component context.
    """
    ctx = get_context_from_ctx()
    return await ctx._core_processor_ctx.next_id(key)
