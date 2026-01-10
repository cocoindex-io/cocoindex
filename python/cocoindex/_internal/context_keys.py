from contextlib import AsyncExitStack
import threading
from typing import Any, AsyncContextManager, ContextManager, Generic, TypeVar, cast

_lock = threading.Lock()
_used_keys = set[str]()

T = TypeVar("T")


class ContextKey(Generic[T]):
    __slots__ = ("_key",)
    _key: str

    def __init__(self, key: str):
        with _lock:
            if key in _used_keys:
                raise ValueError(f"Context key {key} already used")
            _used_keys.add(key)
        self._key = key


class ContextProvider:
    __slots__ = ("_values", "_exit_stack")

    _values: dict[ContextKey[Any], Any]
    _exit_stack: AsyncExitStack

    def __init__(self) -> None:
        self._values = {}
        self._exit_stack = AsyncExitStack()

    def provide(self, key: ContextKey[T], value: T) -> T:
        self._values[key] = value
        return value

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
