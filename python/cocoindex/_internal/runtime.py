from functools import cache

from .core import AsyncContext  # type: ignore

from ..runtime import execution_context, is_coroutine_fn


@cache
def get_async_context() -> AsyncContext:
    return AsyncContext(execution_context.event_loop)


__all__ = ["get_async_context", "is_coroutine_fn", "execution_context"]
