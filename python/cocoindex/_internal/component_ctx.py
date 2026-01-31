from __future__ import annotations

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

    def _with_fn_call_ctx(self, fn_call_ctx: core.FnCallContext) -> ComponentContext:
        return ComponentContext(
            self._env,
            self._core_path,
            self._core_processor_ctx,
            fn_call_ctx,
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


def get_context_from_ctx() -> ComponentContext:
    """Get the current ComponentContext from ContextVar."""
    ctx_var = _context_var.get(None)
    if ctx_var is not None:
        return ctx_var
    raise RuntimeError(
        "No ComponentContext available. This function must be called from within "
        "an active component context (inside a mount/mount_run call or App.update)."
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

        @coco.function
        def app_main() -> None:
            db = coco.use_context(PG_DB)
            ...
    """
    ctx = get_context_from_ctx()
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
    return get_context_from_ctx()


def next_id(key: StableKey = None) -> int:
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
    return ctx._core_processor_ctx.next_id(key)
