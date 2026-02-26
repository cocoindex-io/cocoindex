"""Module with v2 function bodies for logic change detection testing.

Functions that differ between v1 and v2: transform_memo, declare_entry_memo,
bar_memo, bar_plain, bar_comp_memo, bar_comp_plain.

Functions that are IDENTICAL in v1 and v2 (same source text): foo_calls_bar_memo,
foo_calls_bar_plain, foo_comp_calls_bar_memo, foo_comp_mounts_bar_comp_plain,
foo_comp_mounts_bar_comp_memo.
"""

import cocoindex as coco
from tests.common.target_states import GlobalDictTarget, Metrics

_metrics: Metrics | None = None


def set_metrics(metrics: Metrics) -> None:
    global _metrics
    _metrics = metrics


# --- Direct functions (DIFFER between v1 and v2) ---


@coco.fn(memo=True)
def transform_memo(key: str, value: str) -> str:
    assert _metrics is not None
    _metrics.increment("transform_memo")
    return "v2: " + value


@coco.fn(memo=True)
def declare_entry_memo(key: str, value: str) -> None:
    assert _metrics is not None
    _metrics.increment("declare_entry_memo")
    coco.declare_target_state(GlobalDictTarget.target_state(key, "v2: " + value))


# --- Bar functions (DIFFER between v1 and v2) ---


@coco.fn(memo=True)
def bar_memo(s: str) -> str:
    assert _metrics is not None
    _metrics.increment("bar_memo")
    return "bar_v2: " + s


@coco.fn
def bar_plain(s: str) -> str:
    assert _metrics is not None
    _metrics.increment("bar_plain")
    return "bar_v2: " + s


@coco.fn(memo=True)
def bar_comp_memo(key: str, value: str) -> None:
    assert _metrics is not None
    _metrics.increment("bar_comp_memo")
    coco.declare_target_state(GlobalDictTarget.target_state(key, "bar_v2: " + value))


@coco.fn
def bar_comp_plain(key: str, value: str) -> None:
    assert _metrics is not None
    _metrics.increment("bar_comp_plain")
    coco.declare_target_state(GlobalDictTarget.target_state(key, "bar_v2: " + value))


# --- Foo functions (IDENTICAL in v1 and v2) ---


@coco.fn(memo=True)
def foo_calls_bar_memo(key: str, value: str) -> str:
    assert _metrics is not None
    _metrics.increment("foo_calls_bar_memo")
    return bar_memo(value)


@coco.fn(memo=True)
def foo_calls_bar_plain(key: str, value: str) -> str:
    assert _metrics is not None
    _metrics.increment("foo_calls_bar_plain")
    return bar_plain(value)


@coco.fn
def foo_comp_calls_bar_memo(key: str, value: str) -> None:
    assert _metrics is not None
    _metrics.increment("foo_comp_calls_bar_memo")
    result = bar_memo(value)
    coco.declare_target_state(GlobalDictTarget.target_state(key, result))


@coco.fn
async def foo_comp_mounts_bar_comp_plain(key: str, value: str) -> None:
    assert _metrics is not None
    _metrics.increment("foo_comp_mounts_bar_comp_plain")
    await coco.mount(coco.component_subpath(key), bar_comp_plain, key, value)


@coco.fn
async def foo_comp_mounts_bar_comp_memo(key: str, value: str) -> None:
    assert _metrics is not None
    _metrics.increment("foo_comp_mounts_bar_comp_memo")
    await coco.mount(coco.component_subpath(key), bar_comp_memo, key, value)
