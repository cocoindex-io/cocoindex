from contextlib import AsyncExitStack
import threading
from typing import Any, AsyncContextManager, ContextManager, Generic, TypeVar, cast

from . import core
from .memo_key import _canonicalize

_lock = threading.Lock()
_used_keys = set[str]()

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


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


class ContextProvider:
    __slots__ = ("_values", "_exit_stack", "_tracked_fingerprints", "_core_env")

    _values: dict[ContextKey[Any], Any]
    _exit_stack: AsyncExitStack
    _tracked_fingerprints: dict[ContextKey[Any], core.Fingerprint]
    _core_env: core.Environment | None

    def __init__(self) -> None:
        self._values = {}
        self._exit_stack = AsyncExitStack()
        self._tracked_fingerprints = {}
        self._core_env = None

    def set_core_env(self, core_env: core.Environment) -> None:
        """Set the Rust environment and retroactively register tracked fingerprints."""
        self._core_env = core_env
        for fp in self._tracked_fingerprints.values():
            core_env.register_logic(fp)

    def provide(self, key: ContextKey[T], value: T) -> T:
        self._values[key] = value
        if key.tracked:
            canonical = _canonicalize(("context_key", key._key, value), _seen=None)
            fp = core.fingerprint_simple_object(canonical)
            self._tracked_fingerprints[key] = fp
            if self._core_env is not None:
                self._core_env.register_logic(fp)
        return value

    def get_tracked_fingerprint(self, key: ContextKey[Any]) -> core.Fingerprint:
        """Get the tracked fingerprint for a key. Raises KeyError if not tracked."""
        return self._tracked_fingerprints[key]

    def provide_with(self, key: ContextKey[T], cm: ContextManager[T]) -> T:
        value = self._exit_stack.enter_context(cm)
        return self.provide(key, value)

    async def provide_async_with(
        self, key: ContextKey[T], cm: AsyncContextManager[T]
    ) -> T:
        value = await self._exit_stack.enter_async_context(cm)
        return self.provide(key, value)

    def use(self, key: ContextKey[T]) -> T:
        return cast(T, self._values[key])

    async def aclose(self) -> None:
        await self._exit_stack.aclose()
