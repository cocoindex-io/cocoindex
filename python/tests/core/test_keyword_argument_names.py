"""A keyword argument reaches the user's function, whatever its name.

CocoIndex's wrappers and mount APIs forward the user's `*args, **kwargs` to the
user's function. Their own parameters (`self`, `fn`, `async_ctx`, `items`, ...)
are positional-only, so a keyword argument of the same name doesn't collide.
"""

from __future__ import annotations

import asyncio
import datetime
import inspect
from collections.abc import Callable
from typing import Any

import pytest

import cocoindex as coco
from cocoindex._internal.runner import GPURunner

from tests import common

coco_env = common.create_test_env(__file__)

# Every name a wrapper or mount API uses for a parameter of its own.
_KWARGS: dict[str, int] = {
    name: i
    for i, name in enumerate(
        ["self", "fn", "async_ctx", "processor_fn", "items", "subpath"]
    )
}


# A method's first parameter is already `self`, in plain Python too.
_METHOD_KWARGS = {name: i for name, i in _KWARGS.items() if name != "self"}


def _echo(**kwargs: int) -> dict[str, int]:
    return kwargs


async def _aecho(**kwargs: int) -> dict[str, int]:
    return kwargs


class _Holder:
    @coco.fn
    def echo(self, **kwargs: int) -> dict[str, int]:
        return kwargs

    @coco.fn
    async def aecho(self, **kwargs: int) -> dict[str, int]:
        return kwargs


# Built fresh for each call: a runner's batch queue is bound to its first event loop.
_FUNCTIONS: dict[str, Callable[[], Callable[..., Any]]] = {
    "sync": lambda: coco.fn(_echo),
    "sync, memo": lambda: coco.fn(memo=True)(_echo),
    "async": lambda: coco.fn(_aecho),
    "async, memo": lambda: coco.fn(memo=True)(_aecho),
    "coco.fn.as_async": lambda: coco.fn.as_async(_echo),
    "sync, .as_async()": lambda: coco.fn(_echo).as_async,
    "async, .as_async()": lambda: coco.fn(_aecho).as_async,
    "GPU runner": lambda: coco.fn.as_async(runner=GPURunner(fraction=1.0))(_echo),
    "GPU runner, async": lambda: (
        coco.fn.as_async(runner=GPURunner(fraction=1.0))(_aecho)
    ),
    "GPU runner, memo": lambda: (
        coco.fn.as_async(runner=GPURunner(fraction=1.0), memo=True)(_echo)
    ),
    "method": lambda: _Holder().echo,
    "async method": lambda: _Holder().aecho,
    "logic_propagated_to_callers": lambda: coco.logic_propagated_to_callers(_echo),
    "logic_propagated_to_callers, async": lambda: (
        coco.logic_propagated_to_callers(_aecho)
    ),
}


# These run only inside a component.
_IN_APP_ONLY = {"logic_propagated_to_callers", "logic_propagated_to_callers, async"}


async def _call(make: Callable[[], Callable[..., Any]], kwargs: dict[str, int]) -> Any:
    result = make()(**kwargs)
    return await result if inspect.isawaitable(result) else result


@pytest.mark.parametrize("variant", list(_FUNCTIONS))
def test_a_function_receives_keyword_arguments_named_like_wrapper_parameters(
    monkeypatch: pytest.MonkeyPatch, variant: str
) -> None:
    monkeypatch.delenv("COCOINDEX_RUN_GPU_IN_SUBPROCESS", raising=False)
    make = _FUNCTIONS[variant]
    kwargs = _METHOD_KWARGS if "method" in variant else _KWARGS

    if variant not in _IN_APP_ONLY:
        # Called directly, outside any app.
        assert asyncio.run(_call(make, kwargs)) == kwargs

    # Called from inside an app's component.
    seen: list[Any] = []

    async def _main() -> None:
        seen.append(await _call(make, kwargs))

    coco.App(
        coco.AppConfig(name=f"test_kwarg_names_{variant}", environment=coco_env),
        _main,
    ).update_blocking()
    assert seen == [kwargs]


# The mount APIs' own parameter names, passed by keyword below so that mypy checks
# the overloads accept them too.
_MOUNT_KWARGS = {"self": 1, "fn": 2, "items": 3, "processor_fn": 4, "subpath": 5}
_MOUNTED: dict[str, dict[str, int]] = {}


def _seen(
    label: str, *, self: int, fn: int, items: int, processor_fn: int, subpath: int
) -> dict[str, int]:
    _MOUNTED[label] = dict(
        self=self, fn=fn, items=items, processor_fn=processor_fn, subpath=subpath
    )
    return _MOUNTED[label]


@coco.fn
async def _record(
    label: str, *, self: int, fn: int, items: int, processor_fn: int, subpath: int
) -> dict[str, int]:
    return _seen(
        label, self=self, fn=fn, items=items, processor_fn=processor_fn, subpath=subpath
    )


async def _record_plain(
    label: str, *, self: int, fn: int, items: int, processor_fn: int, subpath: int
) -> None:
    _seen(
        label, self=self, fn=fn, items=items, processor_fn=processor_fn, subpath=subpath
    )


def test_mount_apis_pass_keyword_arguments_named_like_their_parameters() -> None:
    _MOUNTED.clear()
    returned: dict[str, Any] = {}
    auto_refresh = coco.auto_refresh(
        _record_plain, interval=datetime.timedelta(milliseconds=10)
    )

    async def _main() -> None:
        returned["use_mount"] = await coco.use_mount(
            coco.component_subpath("use_mount"),
            _record,
            "use_mount",
            self=1,
            fn=2,
            items=3,
            processor_fn=4,
            subpath=5,
        )
        mount = await coco.mount(
            coco.component_subpath("mount"),
            _record,
            "mount",
            self=1,
            fn=2,
            items=3,
            processor_fn=4,
            subpath=5,
        )
        await mount.ready()
        mount_each = await coco.mount_each(
            _record,
            [("k", "mount_each")],
            self=1,
            fn=2,
            items=3,
            processor_fn=4,
            subpath=5,
        )
        await mount_each.ready()
        returned["map"] = await coco.map(
            _record, ["map"], self=1, fn=2, items=3, processor_fn=4, subpath=5
        )
        refreshed = await coco.mount(
            coco.component_subpath("auto_refresh"),
            auto_refresh,
            "auto_refresh",
            self=1,
            fn=2,
            items=3,
            processor_fn=4,
            subpath=5,
        )
        await refreshed.ready()

    coco.App(
        coco.AppConfig(name="test_kwarg_names_mount", environment=coco_env), _main
    ).update_blocking()

    assert _MOUNTED == {
        api: _MOUNT_KWARGS
        for api in ["use_mount", "mount", "mount_each", "map", "auto_refresh"]
    }
    assert returned == {"use_mount": _MOUNT_KWARGS, "map": [_MOUNT_KWARGS]}


class _Feed:
    """A `LiveMapFeed` that delivers one item, then is ready."""

    async def watch(
        self, subscriber: coco.LiveMapSubscriber[coco.StableKey, str]
    ) -> None:
        await (await subscriber.update("k", "live mount_each")).ready()
        await subscriber.mark_ready()


def test_live_mount_each_passes_keyword_arguments_named_like_its_parameters() -> None:
    """A live feed's items go through `LiveComponentOperator.update`."""
    _MOUNTED.clear()

    async def _main() -> None:
        await coco.mount_each(
            _record, _Feed(), self=1, fn=2, items=3, processor_fn=4, subpath=5
        )

    coco.App(
        coco.AppConfig(name="test_kwarg_names_live", environment=coco_env), _main
    ).update_blocking(live=True)

    assert _MOUNTED == {"live mount_each": _MOUNT_KWARGS}
