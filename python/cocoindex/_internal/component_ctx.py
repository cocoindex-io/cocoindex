from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Generator, TypeVar

from cocoindex._internal.context_keys import ContextKey
from cocoindex._internal.environment import Environment

from . import core
from .stable_path import StableKey

T = TypeVar("T")


# ContextVar for the current ComponentContext
_context_var: ContextVar[ComponentContext] = ContextVar("coco_component_context")

# ContextVar for the current subpath parts (used by component_subpath context manager)
_subpath_var: ContextVar[tuple[StableKey, ...]] = ContextVar("coco_subpath", default=())


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

    def concat_part(self, part: StableKey) -> ComponentContext:
        """Return a new ComponentContext with the given part appended to the path."""
        return ComponentContext(
            self._env,
            self._core_path.concat(part),
            self._core_processor_ctx,
            self._core_fn_call_ctx,
        )

    def event_loop(self) -> asyncio.AbstractEventLoop:
        return self._env.event_loop

    def _with_fn_call_ctx(self, fn_call_ctx: core.FnCallContext) -> ComponentContext:
        return ComponentContext(
            self._env,
            self._core_path,
            self._core_processor_ctx,
            fn_call_ctx,
        )

    @contextmanager
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


# Alias for backward compatibility during transition
Scope = ComponentContext


class ComponentSubpath:
    """
    Represents a relative path to create a sub-scope.

    Can be:
    - Passed to mount()/mount_run() as the first argument
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
    _token: Token[tuple[StableKey, ...]] | None

    def __init__(self, *key_parts: StableKey) -> None:
        self._parts = key_parts
        self._token = None

    @property
    def parts(self) -> tuple[StableKey, ...]:
        return self._parts

    def __enter__(self) -> ComponentSubpath:
        # Push our parts onto the subpath stack
        current = _subpath_var.get()
        self._token = _subpath_var.set(current + self._parts)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if self._token is not None:
            _subpath_var.reset(self._token)
            self._token = None

    def __truediv__(self, part: StableKey) -> ComponentSubpath:
        """Allows chaining: coco.component_subpath("a") / "b" / "c" """
        return ComponentSubpath(*self._parts, part)

    def __repr__(self) -> str:
        return f"ComponentSubpath({', '.join(repr(p) for p in self._parts)})"


def component_subpath(*key_parts: StableKey) -> ComponentSubpath:
    """
    Create a component subpath for use with mount()/mount_run() or as a context manager.

    Args:
        *key_parts: One or more StableKey values to form the subpath

    Returns:
        A ComponentSubpath that can be passed to mount/mount_run or used as a context manager

    Examples:
        # As first argument to mount
        coco.mount(coco.component_subpath("process", filename), process_file, file, target)

        # As context manager
        with coco.component_subpath("process_file"):
            for f in files:
                coco.mount(coco.component_subpath(str(f.relative_path)), process_file, f, target)
    """
    return ComponentSubpath(*key_parts)


def _get_context_from_ctx() -> ComponentContext:
    """Get the current ComponentContext from ContextVar."""
    ctx_var = _context_var.get(None)
    if ctx_var is not None:
        return ctx_var
    raise RuntimeError(
        "No ComponentContext available. This function must be called from within "
        "an active component context (inside a mount/mount_run call or App.update)."
    )


def _get_current_subpath() -> tuple[StableKey, ...]:
    """Get the current subpath parts from context var."""
    return _subpath_var.get()


def _resolve_subpath(subpath: ComponentSubpath | None) -> tuple[StableKey, ...]:
    """
    Resolve a ComponentSubpath to absolute path parts.

    Combines the context manager subpath with any explicit subpath argument.
    """
    context_parts = _get_current_subpath()
    if subpath is not None:
        return context_parts + subpath.parts
    return context_parts


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

        @coco.function
        def app_main() -> None:
            db = coco.use_context(PG_DB)
            ...
    """
    ctx = _get_context_from_ctx()
    return ctx._env.context_provider.use(key)


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
    return _get_context_from_ctx()
