"""A sink's exception reaches the caller as itself, with a note that names the
target states the sink was applying."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Collection

import pytest

import cocoindex as coco
from tests import common

coco_env = common.create_test_env(__file__)

_PROVIDER_NAME = "test_sink_error_context/orders"


class _SinkFailure(Exception):
    pass


@dataclass(frozen=True)
class _FailingSink:
    async def __call__(
        self,
        context_provider: coco.ContextProvider,
        actions: Collection[Any],
        /,
    ) -> None:
        raise _SinkFailure("the orders table is gone")


class _Handler:
    def __init__(self) -> None:
        self._sink = coco.TargetActionSink.from_async_fn(_FailingSink())

    def reconcile(
        self,
        key: Any,
        desired_state: Any | coco.NonExistenceType,
        prev_possible_records: Collection[Any],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[Any, Any] | None:
        if coco.is_non_existence(desired_state):
            return None
        return coco.TargetReconcileOutput(
            action=desired_state, sink=self._sink, tracking_record=desired_state
        )


_provider = coco.register_root_target_states_provider(_PROVIDER_NAME, _Handler())


def _declare() -> None:
    coco.declare_target_state(_provider.target_state("k", "v"))


def test_a_failing_sinks_exception_names_its_target_states() -> None:
    app = coco.App(
        coco.AppConfig(name="test_sink_error_context", environment=coco_env),
        _declare,
    )
    with pytest.raises(_SinkFailure, match="the orders table is gone") as exc_info:
        app.update_blocking()
    notes = getattr(exc_info.value, "__notes__", [])
    assert any(
        "failed to apply changes to target states under" in note
        and _PROVIDER_NAME in note
        for note in notes
    ), notes


@coco.fn
async def _declare_in_child() -> None:
    _declare()


def test_the_handler_and_the_result_see_one_exception_with_its_note() -> None:
    """A child's sink failure reaches the exception handler, which re-raises
    it, and then the update's result: both see the same exception object, and
    the note names the target states once."""
    seen: list[BaseException] = []

    def handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
        seen.append(exc)
        raise exc

    async def _root() -> None:
        async with coco.exception_handler(handler):
            handle = await coco.mount(
                coco.component_subpath("child"), _declare_in_child
            )
            # The handler re-raised: the error reaches the root through `ready()`.
            await handle.ready()

    app = coco.App(
        coco.AppConfig(name="test_sink_error_context_twice", environment=coco_env),
        _root,
    )
    with pytest.raises(_SinkFailure) as exc_info:
        app.update_blocking()
    assert len(seen) == 1 and seen[0] is exc_info.value
    notes = [
        note
        for note in getattr(exc_info.value, "__notes__", [])
        if "failed to apply changes to target states under" in note
    ]
    assert len(notes) == 1, notes
