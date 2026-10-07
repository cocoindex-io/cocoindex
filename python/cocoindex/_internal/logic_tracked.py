"""``logic_tracked(fn)``: a callback whose logic changes are tracked through
``logic_tracking="self"`` / ``None`` layers.

Those modes hide the logic of everything a function calls, including callbacks
it merely received. ``logic_tracked(fn)`` lets the owner of a callback (the
frame that hands it down, typically a library's entry point) have the
callback's logic recorded in every memo entry between the call site and the
owner, regardless of their modes. The engine calls the mechanism a "callback
tunnel": the deps ride through frames that would otherwise drop them.

See ``specs/logic_change_detection/callback_tunnel.md``.
"""

from __future__ import annotations

import contextlib
import functools
import inspect
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Iterator,
    ParamSpec,
    TypeVar,
    overload,
)

from . import core
from .component_ctx import _context_var, get_context_from_ctx

P = ParamSpec("P")
R = TypeVar("R")


def _is_async_callable(fn: Callable[..., Any]) -> bool:
    # `inspect` unwraps partials of plain functions only; unwrap them here so a
    # partial of a callable object with an ``async def __call__`` (such as an
    # ``AsyncFunction``) is recognized too.
    while isinstance(fn, functools.partial):
        fn = fn.func
    return inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(
        getattr(fn, "__call__", None)
    )


class _LogicTracked:
    """Shared half of the two wrappers.

    Transparent for memoization keys: the key is the wrapped callable's, so a
    logic-tracked callback keys the same memo entries as the bare one would.
    """

    __slots__ = ("_fn", "_owner_fn_ctx")

    _fn: Callable[..., Any]
    _owner_fn_ctx: core.FnCallContext

    def __init__(
        self, fn: Callable[..., Any], owner_fn_ctx: core.FnCallContext
    ) -> None:
        self._fn = fn
        self._owner_fn_ctx = owner_fn_ctx

    def __coco_memo_key__(self) -> object:
        return self._fn

    def __repr__(self) -> str:
        return f"coco.logic_tracked({self._fn!r})"

    @contextlib.contextmanager
    def _collecting(self) -> Iterator[None]:
        """Run the body under a fresh collector frame; on exit hand what it
        gathered to the dynamic parent, tagged for the owner.

        Raises on an extent violation rather than running the body untracked:
        a silently untracked callback is the trap this API exists to remove.
        """
        parent_ctx = _context_var.get(None)
        if parent_ctx is None:
            raise RuntimeError(
                f"{self!r} was called with no active component context. A "
                "logic-tracked callback must run inside the component that "
                "wrapped it (use ComponentContext.attach() in worker threads)."
            )
        if self._owner_fn_ctx.closed:
            raise RuntimeError(
                f"{self!r} was called after the function that wrapped it "
                "returned. A logic-tracked callback is only valid inside the "
                "dynamic extent of that call; wrap it in a frame that encloses "
                "every call site."
            )
        # The collector propagates everything, so its fn_logic_deps is exactly
        # what the callback offers under the callback's own logic_tracking.
        collector = core.FnCallContext(propagate_children_fn_logic=True)
        tok = _context_var.set(
            parent_ctx._with_fn_call_ctx(collector, in_memo_fn=parent_ctx._in_memo_fn)
        )
        try:
            yield
        finally:
            _context_var.reset(tok)
            parent_ctx._core_fn_call_ctx.join_tunneled_child(
                collector, self._owner_fn_ctx
            )


class _SyncLogicTracked(_LogicTracked):
    __slots__ = ()

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        with self._collecting():
            result = self._fn(*args, **kwargs)
        if inspect.isawaitable(result):
            # A plain function that returns a coroutine would run its body
            # only when awaited, after the collector frame above has closed:
            # the callback's logic would be silently untracked.
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError(
                f"{self!r} wraps a plain callable that returned an awaitable, "
                "whose body would run after the tracked frame closed. Wrap the "
                "coroutine function itself (an `async def`, or an object whose "
                "`__call__` is one) instead."
            )
        return result


class _AsyncLogicTracked(_LogicTracked):
    __slots__ = ()

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        with self._collecting():
            return await self._fn(*args, **kwargs)


@overload
def logic_tracked(  # type: ignore[overload-overlap]
    fn: Callable[P, Awaitable[R]],
) -> Callable[P, Coroutine[Any, Any, R]]: ...
@overload
def logic_tracked(fn: Callable[P, R]) -> Callable[P, R]: ...
def logic_tracked(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Return ``fn`` with its logic tracked through ``logic_tracking="self"`` /
    ``None`` layers.

    Call this in the function that hands the callback down (the **owner**):
    typically the library entry point that receives it, or your own code when
    the library does not. The returned wrapper runs ``fn`` as usual;
    afterwards the logic ``fn`` offered (its own fingerprint and its callees',
    per ``fn``'s own ``logic_tracking``) is recorded in **every** memo entry
    between the call site and the owner's frame, regardless of their
    ``logic_tracking``. The owner's own entry records them too; the owner's
    mode only decides whether they are offered to its callers (``"full"``) or
    stop there (``"self"`` / ``None``).

    - Decorate the callback with ``@coco.fn`` for its own code to be tracked;
      an undecorated callable contributes only the ``@coco.fn`` functions it
      calls, as always.
    - The wrapper is valid only while the owner's call is running. Calling it
      after the owner returned raises ``RuntimeError``; mount the components
      that call it with ``use_mount``, or ``await handle.ready()`` inside the
      owner.
    - A coroutine function (or an object whose ``__call__`` is one) gets an
      async wrapper; anything else a sync one. A plain callable that merely
      *returns* an awaitable is rejected with ``TypeError`` when called, since
      its body would run after the tracked frame closed.
    - The wrapper is transparent for memoization keys (it keys as ``fn``).

    Example (library entry point)::

        @coco.fn
        async def walk(repo: Repo, visitor: RepoVisitor) -> None:
            visit_file = coco.logic_tracked(visitor.visit_file)  # walk owns it
            for file in repo.files():
                await coco.use_mount(
                    coco.component_subpath(file.path), accept, file, visit_file
                )

    Every memo entry below ``walk`` — e.g. a ``"self"`` ``accept`` — records
    the logic of ``visit_file``; the user only decorates it with ``@coco.fn``.

    Raises:
        RuntimeError: when called outside an active component context.
    """
    owner_fn_ctx = get_context_from_ctx()._core_fn_call_ctx
    if _is_async_callable(fn):
        return _AsyncLogicTracked(fn, owner_fn_ctx)
    return _SyncLogicTracked(fn, owner_fn_ctx)
