from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
import threading
from typing import (
    Any,
    AsyncContextManager,
    Awaitable,
    ContextManager,
    Generic,
    TypeVar,
    cast,
    overload,
)

from . import core
from .memo_key import StateFnEntry, _canonicalize
from .typing import NON_EXISTENCE

_lock = threading.Lock()
_used_keys = set[str]()

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


def resolve_awaitables_sync(
    items: list[Any],
    running_loop_error_msg: str,
) -> list[Any]:
    """Resolve any ``Awaitable`` values in *items* in place, returning a new list.

    Synchronously walks *items*; each entry that is an ``Awaitable`` is
    gathered via ``asyncio.run(asyncio.gather(...))``. Non-awaitable entries
    pass through unchanged.

    This is the shared sync/async bridge used by state-function resolution at
    both ``provide()`` time (here, in ``_compute_initial_context_states``) and
    at cache-hit validation time (in ``function.py``'s
    ``_call_state_methods_sync``). Both call sites have the same requirement:
    the call returned a mix of values and awaitables, and we must block the
    caller until all awaitables resolve — but only if we're not already inside
    a running event loop, in which case we raise with a caller-specific
    message.

    *running_loop_error_msg* is the ``RuntimeError`` message used when we
    detect a running event loop — callers supply a message that points at
    their own remediation (e.g. ``@coco.fn.as_async`` for per-call state fns,
    or "provide the value outside an async context" for ``provide()``).
    """
    awaitable_indices = [i for i, o in enumerate(items) if isinstance(o, Awaitable)]
    if not awaitable_indices:
        return items
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError(running_loop_error_msg)

    async def _gather() -> list[Any]:
        return list(await asyncio.gather(*(items[i] for i in awaitable_indices)))

    resolved = asyncio.run(_gather())
    out = list(items)
    for idx, val in zip(awaitable_indices, resolved):
        out[idx] = val
    return out


def _compute_initial_context_states(
    state_fns: list[StateFnEntry], key_name: str
) -> list[Any]:
    """Call each state function with ``NON_EXISTENCE`` and return their states.

    This is the one-time initial-state collection at ``provide()`` time. The
    resulting states are cached on the :class:`ContextProvider` and reused on
    every cache-miss that observes this fingerprint, instead of re-running
    the state functions per call.

    Handles async state functions via ``asyncio.run`` when called from a sync
    context. From within a running event loop, async state functions raise —
    async provide support would need a separate `aprovide` entry point.
    """
    outcomes: list[Any] = [entry.call(NON_EXISTENCE) for entry in state_fns]
    outcomes = resolve_awaitables_sync(
        outcomes,
        running_loop_error_msg=(
            f"Async state function on tracked context key {key_name!r} "
            "cannot be called from a running event loop at provide() time. "
            "Use a sync state function, or provide the value outside of an "
            "async context."
        ),
    )
    return [outcome.state for outcome in outcomes]


class ContextKey(Generic[T_co]):
    __slots__ = ("_key", "_tracked")
    _key: str
    _tracked: bool

    def __init__(self, key: str, *, tracked: bool = True):
        with _lock:
            if key in _used_keys:
                raise ValueError(f"Context key {key} already used")
            _used_keys.add(key)
        self._key = key
        self._tracked = tracked

    @property
    def tracked(self) -> bool:
        return self._tracked

    @property
    def key(self) -> str:
        return self._key

    def __coco_memo_key__(self) -> str:
        return self._key


class ContextProvider:
    __slots__ = (
        "_values",
        "_exit_stack",
        "_tracked_fingerprints",
        "_context_state_fns",
        "_pending_initial_states",
        "_core_env",
    )

    _values: dict[str, Any]
    _exit_stack: AsyncExitStack
    _tracked_fingerprints: dict[ContextKey[Any], core.Fingerprint]
    # State functions per tracked-context fingerprint, used at cache-hit
    # validation time. Python-only (state functions are closures that can't
    # cross into Rust). The list is in canonicalization order and has 1:1
    # correspondence with the stored `context_memo_states` Vec on each memo
    # entry.
    _context_state_fns: dict[core.Fingerprint, list[StateFnEntry]]
    # Initial state values computed eagerly at `provide()` time, held only
    # until `set_core_env` can push them into the Rust env registry (the
    # permanent home). After `set_core_env`, new `provide()` calls write
    # directly to Rust and this dict stays empty. Keeping it as a separate
    # (explicitly short-lived) buffer makes it obvious that Python is not
    # the source of truth for initial states once the env is attached.
    _pending_initial_states: dict[core.Fingerprint, list[Any]]
    _core_env: core.Environment | None

    def __init__(self) -> None:
        self._values = {}
        self._exit_stack = AsyncExitStack()
        self._tracked_fingerprints = {}
        self._context_state_fns = {}
        self._pending_initial_states = {}
        self._core_env = None

    def set_core_env(self, core_env: core.Environment) -> None:
        """Attach the Rust environment and drain buffered per-fp state into it.

        ``provide()`` can be called before ``set_core_env`` (common when a
        standalone ``ContextProvider`` is constructed and passed into
        ``Environment(...)``). This method replays any fingerprints and
        initial states collected up to that point into the Rust logic set
        and initial-states registry, then clears the Python-side buffer so
        Rust is the single source of truth from here on.
        """
        self._core_env = core_env
        for fp in self._tracked_fingerprints.values():
            core_env.register_logic(fp)
        for fp, initial_states in self._pending_initial_states.items():
            core_env.register_context_initial_states(fp, initial_states)
        self._pending_initial_states.clear()

    def provide(self, key: ContextKey[T], value: T) -> T:
        self._values[key._key] = value
        if key.tracked:
            state_fns: list[StateFnEntry] = []
            canonical = _canonicalize(
                ("context_key", key._key, value),
                _seen=None,
                state_methods=state_fns,
            )
            fp = core.fingerprint_simple_object(canonical)
            # If this key was previously provided with a different value,
            # unregister the old fp from both sides. Closes a pre-existing
            # re-provide leak.
            old_fp = self._tracked_fingerprints.get(key)
            if old_fp is not None and old_fp != fp:
                if self._core_env is not None:
                    self._core_env.unregister_logic(old_fp)
                    self._core_env.unregister_context_initial_states(old_fp)
                self._context_state_fns.pop(old_fp, None)
                self._pending_initial_states.pop(old_fp, None)
            self._tracked_fingerprints[key] = fp
            if self._core_env is not None:
                self._core_env.register_logic(fp)
            if state_fns:
                self._context_state_fns[fp] = state_fns
                # Eagerly compute initial states. Context values are
                # conceptually immutable between provide() and use — any
                # observable change lives in the state function's return
                # value at cache-hit validation time, not in re-running the
                # state fn at cache-miss. The cache-miss path reads these
                # straight from the Rust env registry.
                initial_states = _compute_initial_context_states(state_fns, key._key)
                if self._core_env is not None:
                    self._core_env.register_context_initial_states(fp, initial_states)
                else:
                    # Buffer until set_core_env drains into Rust.
                    self._pending_initial_states[fp] = initial_states
        return value

    def get_tracked_fingerprint(self, key: ContextKey[Any]) -> core.Fingerprint:
        """Get the tracked fingerprint for a key. Raises KeyError if not tracked."""
        return self._tracked_fingerprints[key]

    def get_context_state_fns(self, fp: core.Fingerprint) -> list[StateFnEntry] | None:
        """Return the ordered state-fn list registered for *fp*, or None.

        Called on cache-hit validation to re-run state functions against the
        stored previous states.
        """
        return self._context_state_fns.get(fp)

    def has_any_context_state_fns(self) -> bool:
        """Whether any tracked context value in this provider carries state fns.

        Used by the memoization layer to decide whether to attach a
        component-level state handler: if no context value has state fns and
        the function has no argument-borne state methods, there's nothing for
        the handler to do and it can be skipped entirely.
        """
        return bool(self._context_state_fns)

    def provide_with(self, key: ContextKey[T], cm: ContextManager[T]) -> T:
        value = self._exit_stack.enter_context(cm)
        return self.provide(key, value)

    async def provide_async_with(
        self, key: ContextKey[T], cm: AsyncContextManager[T]
    ) -> T:
        value = await self._exit_stack.enter_async_context(cm)
        return self.provide(key, value)

    @overload
    def get(self, key: ContextKey[T]) -> T: ...
    @overload
    def get(self, key: str) -> Any: ...
    @overload
    def get(self, key: str, t: type[T]) -> T: ...
    def get(self, key: ContextKey[T] | str, t: type[T] | None = None) -> Any:
        """Get a value from the context. Raises KeyError if not found.

        Overloads:
          get(key: ContextKey[T]) -> T
          get(key: str) -> Any
          get(key: str, t: type[T]) -> T  — also verifies the type at runtime
        """
        if isinstance(key, str):
            value = self._values[key]
            if t is not None and not isinstance(value, t):
                raise TypeError(
                    f"Context key '{key}': expected {t.__name__}, got {type(value).__name__}"
                )
            return value
        return cast(T, self._values[key._key])

    async def aclose(self) -> None:
        await self._exit_stack.aclose()
