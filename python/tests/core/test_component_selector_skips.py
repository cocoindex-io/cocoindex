"""Sliced execution: an unselected component prepares nothing and runs no user code."""

from __future__ import annotations

from typing import Any


import cocoindex as coco

from tests import common
from tests.common.target_states import GlobalDictTarget

coco_env = common.create_test_env(__file__)

_log: list[str] = []


def _reset() -> None:
    GlobalDictTarget.store.clear()
    _log.clear()


def _stored() -> dict[str, Any]:
    return {k: v.data for k, v in GlobalDictTarget.store.data.items()}


def _declare(key: str, value: Any) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(key, value))


def _app(name: str, main: Any) -> coco.App[Any, Any]:
    return coco.App(
        coco.AppConfig(name=f"test_selector_skips_{name}", environment=coco_env), main
    )


# --- an unselected LiveComponent's constructor (user code) runs ----------

_constructed: list[str] = []


class _Live:
    def __init__(self, key: str) -> None:
        _constructed.append(key)
        self._key = key

    async def process(self) -> None:
        _declare(self._key, 1)

    async def process_live(self, operator: Any) -> None:
        await operator.update_full()
        await operator.mark_ready()


def test_unselected_live_component_is_not_constructed() -> None:
    _reset()
    _constructed.clear()

    @coco.fn
    async def main() -> None:
        await coco.mount_each(
            coco.component_subpath("live"), _Live, [("a", "a"), ("b", "b")]
        )

    app = _app("p2", main)
    app.update_blocking()
    _constructed.clear()

    app.update_blocking(component_selector=[coco.ROOT_PATH / "live" / "a"])
    assert _constructed == ["a"], f"constructed for unselected too: {_constructed}"


# --- per-item work for every unselected mount_each item ------------------

_keyed: list[str] = []


class _Keyed:
    def __init__(self, key: str) -> None:
        self.key = key

    def __coco_memo_key__(self) -> object:
        _keyed.append(self.key)
        return self.key


@coco.fn(memo=True)
async def _keyed_item(item: _Keyed) -> None:
    _log.append(item.key)
    _declare(item.key, 1)


def test_unselected_items_are_not_fingerprinted() -> None:
    _reset()
    _keyed.clear()
    keys = [f"k{i}" for i in range(50)]

    @coco.fn
    async def main() -> None:
        await coco.mount_each(
            coco.component_subpath("items"), _keyed_item, [(k, _Keyed(k)) for k in keys]
        )

    app = _app("p3", main)
    app.update_blocking()
    _keyed.clear()
    _log.clear()

    app.update_blocking(component_selector=[coco.ROOT_PATH / "items" / "k0"])
    assert _log == [] or _log == ["k0"]
    assert set(_keyed) <= {"k0"}, (
        f"user __coco_memo_key__ ran for {len(set(_keyed))} items, "
        f"{len(set(_keyed) - {'k0'})} of them unselected"
    )


_helper_input = [1]


@coco.fn(memo=True)
async def _memo_helper(version: int) -> int:
    _log.append("helper")
    _declare("helper", version)
    return version


@coco.fn
async def _item(key: str) -> None:
    _log.append(key)
    _declare(key, _helper_input[0])


# --- an unselected use_mount child that mounts nothing keeps a reusable memo


def test_unselected_use_mount_child_keeps_a_reusable_memo() -> None:
    _reset()

    @coco.fn
    async def main() -> None:
        await coco.use_mount(
            coco.component_subpath("helper"), _memo_helper, _helper_input[0]
        )
        await coco.mount_each(coco.component_subpath("items"), _item, [("a", "a")])

    app = _app("p6", main)
    _helper_input[0] = 1
    app.update_blocking()

    _helper_input[0] = 2
    app.update_blocking(component_selector=[coco.ROOT_PATH / "items" / "a"])
    _log.clear()

    # Same input as the selective run, and "helper" mounts nothing: its run was
    # complete, so a plain update could reuse a memo of it.
    app.update_blocking()
    assert "helper" not in _log, f"helper re-ran: memo dropped by selective run: {_log}"
