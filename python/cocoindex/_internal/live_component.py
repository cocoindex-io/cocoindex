from __future__ import annotations

from typing import Any, ParamSpec, runtime_checkable, Protocol

from . import core
from .component_ctx import ComponentSubpath
from .function import AnyCallable, create_core_component_processor
from .environment import Environment

_P = ParamSpec("_P")


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
