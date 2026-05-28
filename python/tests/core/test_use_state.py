"""Integration tests for coco.use_state() — persistent per-component state."""

import cocoindex as coco

from tests import common

coco_env = common.create_test_env(__file__)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_source_items: list[str] = []
_captured: dict[str, object] = {}


@coco.fn
async def _root(items: list[str]) -> None:
    for item in items:
        await coco.mount(coco.component_subpath(item), _process_item, item)


@coco.fn
def _process_item(item: str) -> None:
    handle = coco.use_state("counter", 0)
    _captured[item] = handle.value
    handle.value = handle.value + 1


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _make_app(name: str) -> coco.App:  # type: ignore[type-arg]
    return coco.App(
        coco.AppConfig(name=name, environment=coco_env),
        _root,
        items=_source_items,
    )


def test_use_state_returns_initial_on_first_run() -> None:
    _source_items.clear()
    _captured.clear()

    app = _make_app("use_state_initial")
    _source_items[:] = ["a"]
    app.update_blocking()

    assert _captured["a"] == 0


def test_use_state_persists_across_runs() -> None:
    _source_items.clear()
    _captured.clear()

    app = _make_app("use_state_persist")
    _source_items[:] = ["a"]

    app.update_blocking()
    assert _captured["a"] == 0  # initial

    app.update_blocking()
    assert _captured["a"] == 1  # stored from previous run

    app.update_blocking()
    assert _captured["a"] == 2  # stored from previous run


def test_use_state_independent_per_component() -> None:
    _source_items.clear()
    _captured.clear()

    app = _make_app("use_state_independent")
    _source_items[:] = ["x", "y"]

    app.update_blocking()
    assert _captured["x"] == 0
    assert _captured["y"] == 0

    app.update_blocking()
    assert _captured["x"] == 1
    assert _captured["y"] == 1


def test_use_state_resets_after_component_deleted() -> None:
    _source_items.clear()
    _captured.clear()

    app = _make_app("use_state_delete")
    _source_items[:] = ["a"]

    app.update_blocking()
    assert _captured["a"] == 0

    app.update_blocking()
    assert _captured["a"] == 1

    # Delete the component by removing "a" from source.
    _source_items.clear()
    app.update_blocking()

    # Re-add: state should have been cleaned up, initial_value returned.
    _source_items[:] = ["a"]
    app.update_blocking()
    assert _captured["a"] == 0


def test_use_state_defaults_to_none() -> None:
    _source_items.clear()
    _captured.clear()

    @coco.fn
    def _process_no_initial(item: str) -> None:
        handle = coco.use_state("flag")  # no initial_value
        _captured[item] = handle.value
        handle.value = "set"

    @coco.fn
    async def _root_no_initial(items: list[str]) -> None:
        for item in items:
            await coco.mount(coco.component_subpath(item), _process_no_initial, item)

    app = coco.App(  # type: ignore[type-arg]
        coco.AppConfig(name="use_state_no_initial", environment=coco_env),
        _root_no_initial,
        items=_source_items,
    )
    _source_items[:] = ["a"]
    app.update_blocking()
    assert _captured["a"] is None  # first run: no stored value → None

    app.update_blocking()
    assert _captured["a"] == "set"  # second run: stored value returned
