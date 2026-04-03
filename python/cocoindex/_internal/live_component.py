from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator
from typing import (
    Any,
    Generic,
    ParamSpec,
    TypeVar,
    runtime_checkable,
    Protocol,
    TYPE_CHECKING,
)

from . import core
from .component_ctx import ComponentSubpath
from .function import AnyCallable, create_core_component_processor
from .environment import Environment

if TYPE_CHECKING:
    from .api import ComponentMountHandle

_P = ParamSpec("_P")
_K = TypeVar("_K")
_V = TypeVar("_V")


@runtime_checkable
class LiveComponent(Protocol):
    """Protocol for live components that process continuously."""

    async def process(self) -> None: ...
    async def process_live(self, operator: LiveComponentOperator) -> None: ...


def is_live_component_class(cls: Any) -> bool:
    """Check if cls is a class with process and process_live methods."""
    return (
        isinstance(cls, type)
        and hasattr(cls, "process")
        and hasattr(cls, "process_live")
        and callable(getattr(cls, "process"))
        and callable(getattr(cls, "process_live"))
    )


class LiveComponentOperator:
    """Passed to process_live(). Wraps the Rust LiveComponentController."""

    __slots__ = ("_controller", "_instance", "_env", "_path")

    def __init__(
        self,
        controller: core.LiveComponentController,
        instance: Any,  # The LiveComponent instance
        env: Environment,
        path: core.StablePath,
    ) -> None:
        self._controller = controller
        self._instance = instance
        self._env = env
        self._path = path

    async def update_full(self) -> None:
        """Trigger a full update via instance.process(). Blocks until fully ready."""
        processor = create_core_component_processor(
            self._instance.process, self._env, self._path, (), {}
        )
        await self._controller.update_full_async(processor)

    async def update(
        self,
        subpath: ComponentSubpath,
        processor_fn: AnyCallable[_P, Any],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> Any:  # Returns ComponentMountHandle
        from .api import ComponentMountHandle

        if is_live_component_class(processor_fn):
            raise TypeError(
                "Nested LiveComponent classes in operator.update() are not yet supported. "
                f"Got: {processor_fn}"
            )
        child_path = self._path
        for part in subpath.parts:
            child_path = child_path.concat(part)
        processor = create_core_component_processor(
            processor_fn, self._env, child_path, args, kwargs
        )
        core_handle = await self._controller.update_async(child_path, processor)
        return ComponentMountHandle([core_handle])

    async def delete(self, subpath: ComponentSubpath) -> Any:
        from .api import ComponentMountHandle

        child_path = self._path
        for part in subpath.parts:
            child_path = child_path.concat(part)
        core_handle = await self._controller.delete_async(child_path)
        return ComponentMountHandle([core_handle])

    async def mark_ready(self) -> None:
        """Signal readiness. In non-live mode, this never returns (terminates process_live)."""
        await self._controller.mark_ready_async()


@runtime_checkable
class LiveItemsView(Protocol[_K, _V]):
    """A keyed items view that can be iterated and watched for live changes.

    Returned by sources like ``LiveDirWalker.items()`` and consumed by ``mount_each()``.
    """

    def __aiter__(self) -> AsyncIterator[tuple[_K, _V]]: ...
    async def watch(self, subscriber: LiveItemsSubscriber[_K, _V]) -> None: ...


class LiveItemsSubscriber(Generic[_K, _V]):
    """Callback interface for ``LiveItemsView.watch()`` to deliver changes.

    Wraps a ``LiveComponentOperator`` at a higher level of abstraction — callers
    provide keys and values instead of component subpaths and processor functions.
    """

    __slots__ = ("_operator", "_fn", "_args", "_kwargs")

    def __init__(
        self,
        operator: LiveComponentOperator,
        fn: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        self._operator = operator
        self._fn = fn
        self._args = args
        self._kwargs = kwargs

    async def update_all(self) -> None:
        """Trigger a full re-iteration of all items."""
        await self._operator.update_full()

    async def mark_ready(self) -> None:
        """Signal readiness. In non-live mode, this terminates ``watch()``."""
        await self._operator.mark_ready()

    async def update(self, key: _K, value: _V) -> ComponentMountHandle:
        """Incrementally update a single entry."""
        return await self._operator.update(  # type: ignore[no-any-return]
            ComponentSubpath(key),  # type: ignore[arg-type]
            self._fn,
            value,
            *self._args,
            **self._kwargs,
        )

    async def delete(self, key: _K) -> ComponentMountHandle:
        """Incrementally delete a single entry."""
        return await self._operator.delete(ComponentSubpath(key))  # type: ignore[no-any-return,arg-type]


class _MountEachLiveComponent:
    """Internal LiveComponent created by mount_each() for LiveItemsView items."""

    def __init__(
        self,
        items: LiveItemsView[Any, Any],
        fn: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        self._items = items
        self._fn = fn
        self._args = args
        self._kwargs = kwargs

    async def process(self) -> None:
        from .api import mount

        async for key, value in self._items:
            await mount(
                ComponentSubpath(key), self._fn, value, *self._args, **self._kwargs
            )  # type: ignore[arg-type]

    async def process_live(self, operator: LiveComponentOperator) -> None:
        subscriber: LiveItemsSubscriber[Any, Any] = LiveItemsSubscriber(
            operator, self._fn, self._args, self._kwargs
        )
        await self._items.watch(subscriber)
