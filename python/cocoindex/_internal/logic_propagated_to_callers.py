"""``logic_propagated_to_callers(fn)``: carry the logic a callback already
offers up to the frame that wraps it, through ``logic_tracking="self"`` /
``None`` layers that would otherwise hide it.

Those modes hide the logic of everything a function calls, including
callbacks it merely received. ``logic_propagated_to_callers(fn)`` lets the
owner of a callback (the frame that hands it down, typically a library's
entry point) have the callback's logic recorded in every memo entry between
the call site and the owner, regardless of their modes. It decides nothing
about *what* is tracked: ``fn``'s own ``@coco.fn`` and ``logic_tracking``
do, exactly as for any function. The engine calls the mechanism a "callback
tunnel": the deps ride through frames that would otherwise drop them.

``logic_tracked`` is the former name, kept as a deprecated alias.
"""

from __future__ import annotations

import contextlib
import functools
import inspect
import warnings
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


class _PropagatedCallback:
    """Shared half of the two wrappers.

    Keys by the wrapped callable: two wrappers of the same callable key the
    same memo entries. The wrapper does not key *as* the bare callable, so
    adopting it where the bare callable was passed before moves that key once.
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
        return f"coco.logic_propagated_to_callers({self._fn!r})"

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
                "propagated callback must run inside the component that wrapped "
                "it (use ComponentContext.attach() in worker threads)."
            )
        if self._owner_fn_ctx.closed:
            raise RuntimeError(
                f"{self!r} was called after the function that wrapped it "
                "returned. A propagated callback is only valid inside the dynamic "
                "extent of that call; wrap it in a frame that encloses every "
                "call site."
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


class _SyncPropagatedCallback(_PropagatedCallback):
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


class _AsyncPropagatedCallback(_PropagatedCallback):
    __slots__ = ()

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        with self._collecting():
            return await self._fn(*args, **kwargs)


@overload
def logic_propagated_to_callers(  # type: ignore[overload-overlap]
    fn: Callable[P, Awaitable[R]],
) -> Callable[P, Coroutine[Any, Any, R]]: ...
@overload
def logic_propagated_to_callers(fn: Callable[P, R]) -> Callable[P, R]: ...
def logic_propagated_to_callers(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Return ``fn`` with the logic it offers propagated to its callers' memo
    entries, through ``logic_tracking="self"`` / ``None`` layers.

    This does not make ``fn`` tracked. ``fn``'s own ``@coco.fn`` and
    ``logic_tracking`` decide what it offers, as for any function; the wrapper
    only carries that up to the frame that called this helper.

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
    - The wrapper keys by ``fn``: two wrappers of the same ``fn`` key alike,
      but adopting the wrapper where bare ``fn`` was passed before moves
      that memo key once.

    Example (library entry point)::

        @coco.fn
        async def walk(repo: Repo, visitor: RepoVisitor) -> None:
            visit_file = coco.logic_propagated_to_callers(visitor.visit_file)  # walk owns it
            for file in repo.files():
                await coco.use_mount(
                    coco.component_subpath(file.path), accept, file, visit_file
                )

    Every memo entry below ``walk`` — e.g. a ``"self"`` ``accept`` — records
    the logic of ``visit_file``; the user only decorates it with ``@coco.fn``.

    - Adopting the wrapper later does not re-store existing memo entries: an
      entry records only the fingerprints its writer offered when it was
      stored, so entries stored before the wrap stay valid until their key or
      the memoized caller's ``version`` moves. Ship the adoption with one such
      change.

    Raises:
        RuntimeError: when called outside an active component context.
    """
    owner_fn_ctx = get_context_from_ctx()._core_fn_call_ctx
    if _is_async_callable(fn):
        return _AsyncPropagatedCallback(fn, owner_fn_ctx)
    return _SyncPropagatedCallback(fn, owner_fn_ctx)


@overload
def logic_tracked(  # type: ignore[overload-overlap]
    fn: Callable[P, Awaitable[R]],
) -> Callable[P, Coroutine[Any, Any, R]]: ...
@overload
def logic_tracked(fn: Callable[P, R]) -> Callable[P, R]: ...
def logic_tracked(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Deprecated former name of :func:`logic_propagated_to_callers`; same
    behavior, plus a :class:`DeprecationWarning`."""
    warnings.warn(
        "coco.logic_tracked is deprecated and will be removed; use "
        "coco.logic_propagated_to_callers (same behavior)",
        DeprecationWarning,
        stacklevel=2,
    )
    return logic_propagated_to_callers(fn)
