"""Callback tunnel for logic change tracking.

``logic_tracking="self"`` / ``None`` hide the logic of everything a function
calls, including callbacks it merely received. ``tunnel(fn)`` lets the owner of
a callback (the frame that creates it) track the callback's logic into every
memo entry between the call site and the owner, regardless of their modes.
``record_callee_logic()`` lets a library record its callbacks' logic in its own
entry when the owner forgot to tunnel.

See ``specs/logic_change_detection/callback_tunnel.md``.
"""

from __future__ import annotations

import contextlib
import inspect
from contextvars import Token
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Generator,
    ParamSpec,
    TypeVar,
    overload,
)

from . import core
from .component_ctx import ComponentContext, _context_var, get_context_from_ctx

P = ParamSpec("P")
R = TypeVar("R")


def _is_async_callable(fn: Callable[..., Any]) -> bool:
    # Covers plain coroutine functions (and partials of them) as well as
    # callable objects with an ``async def __call__`` such as ``AsyncFunction``.
    return inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(
        getattr(fn, "__call__", None)
    )


class _Tunnel:
    """Shared half of the two tunnel wrappers.

    Transparent for memoization keys: the key is the wrapped callable's, so a
    tunneled callback keys the same memo entries as the bare callback would.
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
        return f"coco.tunnel({self._fn!r})"

    def _enter(
        self,
    ) -> tuple[ComponentContext, core.FnCallContext, Token[ComponentContext]]:
        """Open the collector frame under the dynamic parent.

        Raises on an extent violation rather than running the callback
        untracked: a silently untracked callback is the trap the tunnel exists
        to remove.
        """
        parent_ctx = _context_var.get(None)
        if parent_ctx is None:
            raise RuntimeError(
                f"{self!r} was called with no active component context. A "
                "tunneled callback must run inside the component that created "
                "it (use ComponentContext.attach() in worker threads)."
            )
        if self._owner_fn_ctx.closed:
            raise RuntimeError(
                f"{self!r} was called after the function that created the tunnel "
                "returned. A tunneled callback is only valid inside the dynamic "
                "extent of that call; create the tunnel in a frame that encloses "
                "every call site."
            )
        # The collector propagates everything, so its fn_logic_deps is exactly
        # what the callback offers under the callback's own logic_tracking.
        collector = core.FnCallContext(propagate_children_fn_logic=True)
        ctx = parent_ctx._with_fn_call_ctx(collector, in_memo_fn=parent_ctx._in_memo_fn)
        return parent_ctx, collector, _context_var.set(ctx)

    def _exit(
        self,
        parent_ctx: ComponentContext,
        collector: core.FnCallContext,
        tok: Token[ComponentContext],
    ) -> None:
        _context_var.reset(tok)
        parent_ctx._core_fn_call_ctx.join_tunneled_child(collector, self._owner_fn_ctx)


class _SyncTunnel(_Tunnel):
    __slots__ = ()

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        parent_ctx, collector, tok = self._enter()
        try:
            return self._fn(*args, **kwargs)
        finally:
            self._exit(parent_ctx, collector, tok)


class _AsyncTunnel(_Tunnel):
    __slots__ = ()

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        parent_ctx, collector, tok = self._enter()
        try:
            return await self._fn(*args, **kwargs)
        finally:
            self._exit(parent_ctx, collector, tok)


@overload
def tunnel(  # type: ignore[overload-overlap]
    fn: Callable[P, Awaitable[R]],
) -> Callable[P, Coroutine[Any, Any, R]]: ...
@overload
def tunnel(fn: Callable[P, R]) -> Callable[P, R]: ...
def tunnel(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Track a callback's logic through ``logic_tracking="self"`` / ``None`` layers.

    Call this in the function that creates the callback (the **owner**). The
    returned wrapper runs ``fn`` as usual; afterwards the logic ``fn`` offered
    (its own fingerprint and its callees', per ``fn``'s own ``logic_tracking``)
    is recorded in **every** memo entry between the call site and the owner's
    frame, regardless of their ``logic_tracking``. At the owner the deps become
    ordinary child deps and the owner's own mode applies: a ``"self"`` ancestor
    of the owner sees nothing extra.

    - Decorate the callback with ``@coco.fn`` for its own code to be tracked;
      an undecorated callable contributes only the ``@coco.fn`` functions it
      calls, as always.
    - The wrapper is valid only inside the dynamic extent of the owner's call.
      Calling it after the owner returned raises ``RuntimeError``.
    - The wrapper is transparent for memoization keys (it keys as ``fn``).

    Example::

        @coco.fn
        async def app_main(repo: Repo) -> None:
            visitor = MyVisitor()
            visit_file = coco.tunnel(visitor.visit_file)  # owner: app_main
            await walk(repo, visit_file)  # walk's "self" layers can't hide it

    Raises:
        RuntimeError: when called outside an active component context.
    """
    owner_fn_ctx = get_context_from_ctx()._core_fn_call_ctx
    if _is_async_callable(fn):
        return _AsyncTunnel(fn, owner_fn_ctx)
    return _SyncTunnel(fn, owner_fn_ctx)


@contextlib.contextmanager
def record_callee_logic() -> Generator[None, None, None]:
    """Record the logic of the callees in this block into the enclosing
    function's own memo entry, whatever its ``logic_tracking``.

    For library code: at call sites you know are callbacks, this closes the hole
    left when the callback's owner forgot to ``tunnel`` it — the library's
    ``"self"`` function would otherwise serve a stale memo after the callback is
    edited. Nothing extra is offered to the enclosing function's callers; under
    ``logic_tracking="full"`` the block is a no-op.

    A plain (synchronous) ``with`` block — like ``component_subpath`` — even
    though the body typically ``await``s::

        @coco.fn(memo=True, logic_tracking="self", version=1)
        async def accept(self, visitor: RepoVisitor) -> None:
            with coco.record_callee_logic():
                await visitor.visit_file(self)
    """
    parent_ctx = get_context_from_ctx()
    collector = core.FnCallContext(propagate_children_fn_logic=True)
    ctx = parent_ctx._with_fn_call_ctx(collector, in_memo_fn=parent_ctx._in_memo_fn)
    tok = _context_var.set(ctx)
    try:
        yield
    finally:
        _context_var.reset(tok)
        parent_ctx._core_fn_call_ctx.join_recorded_child(collector)
