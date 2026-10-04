"""A Python reference to a finished component's processor context must not
keep the component alive.

The engine decides live-mode termination by polling whether any child
component is still referenced. The processor context handed to Python holds
such a reference, and Python keeps it alive in ways user code cannot see:
an exception's traceback retains every frame it passed through, and the
`@coco.fn` wrapper frames hold the context in their locals. The engine
therefore severs the context from its component when the component's
processing task ends, so retaining the exception (or the context itself)
no longer pins the component.
"""

from __future__ import annotations

from typing import Callable, Coroutine, Any

import pytest

import cocoindex as coco

from tests import common
from tests.common.target_states import GlobalDictTarget

coco_env = common.create_test_env(__file__)

_retained: list[BaseException] = []
_kept_ctx: list[coco.ComponentContext] = []


@coco.fn
async def _raise_directly() -> None:
    raise ValueError("child boom")


@coco.fn
async def _nested_plain() -> None:
    raise ValueError("nested boom")


@coco.fn(memo=True)
async def _nested_memo() -> None:
    raise ValueError("nested memo boom")


@coco.fn
async def _raise_via_nested_plain_fn() -> None:
    await _nested_plain()


@coco.fn
async def _raise_via_nested_memo_fn() -> None:
    await _nested_memo()


_CHILDREN: dict[str, Callable[[], Coroutine[Any, Any, None]]] = {
    "direct": _raise_directly,
    "nested_plain_fn": _raise_via_nested_plain_fn,
    "nested_memo_fn": _raise_via_nested_memo_fn,
}


@coco.fn
async def _root_retaining(child_name: str) -> None:
    try:
        await coco.use_mount(coco.component_subpath("child"), _CHILDREN[child_name])
    except ValueError as exc:
        # Keep the exception past the child's build. Its traceback holds the
        # child's frames, whose locals include the child's processor context.
        _retained.append(exc)


@pytest.mark.timeout(20, method="thread")
@pytest.mark.parametrize("child_name", list(_CHILDREN))
def test_retained_child_exception_does_not_pin_component(child_name: str) -> None:
    _retained.clear()
    app = coco.App(
        coco.AppConfig(name=f"test_ctx_release_{child_name}", environment=coco_env),
        _root_retaining,
        child_name,
    )
    # Live mode waits for every child component to become inactive before
    # terminating; a child pinned by the retained exception would hang here.
    app.update_blocking(live=True)
    assert len(_retained) == 1
    assert isinstance(_retained[0], ValueError)


@coco.fn
async def _root_keeping_ctx() -> None:
    _kept_ctx.append(coco.get_component_context())


def test_kept_component_context_rejects_use_after_finish() -> None:
    _kept_ctx.clear()
    GlobalDictTarget.store.clear()
    app = coco.App(
        coco.AppConfig(name="test_ctx_release_kept_ctx", environment=coco_env),
        _root_keeping_ctx,
    )
    app.update_blocking()
    assert len(_kept_ctx) == 1
    with _kept_ctx[0].attach():
        with pytest.raises(RuntimeError, match="already finished"):
            coco.declare_target_state(GlobalDictTarget.target_state("late", 1))
    assert "late" not in GlobalDictTarget.store.data
