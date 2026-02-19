"""Tests for logic change detection: memoized results are invalidated when function code changes."""

import gc
import pathlib
import sys
from types import ModuleType
from collections.abc import Iterator
from typing import Any

import pytest

import cocoindex as coco
from cocoindex._internal import core

from tests import common
from tests.common.target_states import GlobalDictTarget, Metrics
from tests.common.module_utils import load_module_as


coco_env = common.create_test_env(__file__)

_TEST_DIR = pathlib.Path(__file__).parent
_V1_PATH = str(_TEST_DIR / "mod_logic_v1.py")
_V2_PATH = str(_TEST_DIR / "mod_logic_v2.py")
_FAKE_MODULE = "tests.core._dynamic_logic_change_module"


def _unload_module_functions(mod: ModuleType) -> None:
    """Unregister logic fingerprints for all coco functions in a module."""
    for attr_name in dir(mod):
        obj = getattr(mod, attr_name)
        fp = getattr(obj, "_logic_fp", None)
        if fp is not None:
            core.unregister_logic_fingerprint(fp)


def _load_module(
    module_path: str,
    metrics: Metrics,
    current_module: list[Any],
    old_module: ModuleType | None = None,
) -> ModuleType:
    """Load a module version, unregistering the old module's fingerprints first."""
    if old_module is not None:
        _unload_module_functions(old_module)
    current_module.clear()
    mod = load_module_as(module_path, _FAKE_MODULE)
    mod.set_metrics(metrics)
    current_module.append(mod)
    return mod


@pytest.fixture(autouse=True)
def _cleanup_dynamic_module() -> Iterator[None]:
    """Ensure the fake module and its logic fingerprints are cleaned up after each test."""
    gc.collect()  # Collect stale SyncFunction objects from prior tests so their __del__ runs now.
    yield
    mod = sys.modules.get(_FAKE_MODULE)
    if mod is not None:
        _unload_module_functions(mod)
        del sys.modules[_FAKE_MODULE]


# ============================================================================
# E2: Memoized function cache invalidated on logic change
# ============================================================================


def test_fn_memo_invalidated_on_logic_change() -> None:
    """A memoized function's cached result is invalidated when its code changes."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.function
    def app_main() -> None:
        mod = current_module[0]
        result = mod.transform_memo("A", "value1")
        coco.declare_target_state(GlobalDictTarget.target_state("A", result))

    app = coco.App(
        coco.AppConfig(
            name="test_fn_memo_invalidated_on_logic_change", environment=coco_env
        ),
        app_main,
    )

    # v1: first run — function executes
    mod = _load_module(_V1_PATH, metrics, current_module)
    app.update()
    assert metrics.collect() == {"transform_memo": 1}
    assert GlobalDictTarget.store.data["A"].data == "v1: value1"

    # v1: second run — memo hit
    app.update()
    assert metrics.collect() == {}

    # v2: logic changed — memo invalidated, function re-executes
    mod = _load_module(_V2_PATH, metrics, current_module, old_module=mod)
    app.update()
    assert metrics.collect() == {"transform_memo": 1}
    assert GlobalDictTarget.store.data["A"].data == "v2: value1"

    # v2: second run — memo hit again
    app.update()
    assert metrics.collect() == {}


# ============================================================================
# E3: Component memo invalidated on logic change
# ============================================================================


def test_component_memo_invalidated_on_logic_change() -> None:
    """A memoized component's memo is invalidated when its code changes."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.function
    def app_main() -> None:
        mod = current_module[0]
        coco.mount(coco.component_subpath("A"), mod.declare_entry_memo, "A", "value1")

    app = coco.App(
        coco.AppConfig(
            name="test_component_memo_invalidated_on_logic_change", environment=coco_env
        ),
        app_main,
    )

    # v1: first run — component executes
    mod = _load_module(_V1_PATH, metrics, current_module)
    app.update()
    assert metrics.collect() == {"declare_entry_memo": 1}
    assert GlobalDictTarget.store.data["A"].data == "v1: value1"

    # v1: second run — component memo hit
    app.update()
    assert metrics.collect() == {}

    # v2: logic changed — component memo invalidated
    mod = _load_module(_V2_PATH, metrics, current_module, old_module=mod)
    app.update()
    assert metrics.collect() == {"declare_entry_memo": 1}
    assert GlobalDictTarget.store.data["A"].data == "v2: value1"

    # v2: second run — component memo hit again
    app.update()
    assert metrics.collect() == {}


# ============================================================================
# E4: Transitive — foo (memoized fn) calls bar, bar changes
# ============================================================================


def test_transitive_fn_memo_bar_memo_changes() -> None:
    """foo (memo) calls bar (memo). bar's code changes → foo's memo invalidated."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.function
    def app_main() -> None:
        mod = current_module[0]
        result = mod.foo_calls_bar_memo("A", "value1")
        coco.declare_target_state(GlobalDictTarget.target_state("A", result))

    app = coco.App(
        coco.AppConfig(
            name="test_transitive_fn_memo_bar_memo_changes", environment=coco_env
        ),
        app_main,
    )

    # v1: first run — both foo and bar execute
    mod = _load_module(_V1_PATH, metrics, current_module)
    app.update()
    assert metrics.collect() == {"foo_calls_bar_memo": 1, "bar_memo": 1}
    assert GlobalDictTarget.store.data["A"].data == "bar_v1: value1"

    # v1: second run — foo memo hit (bar not called either)
    app.update()
    assert metrics.collect() == {}

    # v2: bar's logic changed — foo's memo invalidated transitively
    mod = _load_module(_V2_PATH, metrics, current_module, old_module=mod)
    app.update()
    assert metrics.collect() == {"foo_calls_bar_memo": 1, "bar_memo": 1}
    assert GlobalDictTarget.store.data["A"].data == "bar_v2: value1"

    # v2: second run — memo hit again
    app.update()
    assert metrics.collect() == {}


def test_transitive_fn_memo_bar_plain_changes() -> None:
    """foo (memo) calls bar (non-memo). bar's code changes → foo's memo invalidated."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.function
    def app_main() -> None:
        mod = current_module[0]
        result = mod.foo_calls_bar_plain("A", "value1")
        coco.declare_target_state(GlobalDictTarget.target_state("A", result))

    app = coco.App(
        coco.AppConfig(
            name="test_transitive_fn_memo_bar_plain_changes", environment=coco_env
        ),
        app_main,
    )

    # v1: first run — both foo and bar execute
    mod = _load_module(_V1_PATH, metrics, current_module)
    app.update()
    assert metrics.collect() == {"foo_calls_bar_plain": 1, "bar_plain": 1}
    assert GlobalDictTarget.store.data["A"].data == "bar_v1: value1"

    # v1: second run — foo memo hit (bar not called since foo is cached)
    app.update()
    assert metrics.collect() == {}

    # v2: bar's logic changed — foo's memo invalidated transitively
    mod = _load_module(_V2_PATH, metrics, current_module, old_module=mod)
    app.update()
    assert metrics.collect() == {"foo_calls_bar_plain": 1, "bar_plain": 1}
    assert GlobalDictTarget.store.data["A"].data == "bar_v2: value1"

    # v2: second run — memo hit again
    app.update()
    assert metrics.collect() == {}


# ============================================================================
# E5: Transitive — foo (component) calls/mounts bar, bar changes
# ============================================================================


def test_transitive_component_calls_bar_memo() -> None:
    """foo (component) calls bar (memo fn). bar changes → bar's fn memo invalidated."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.function
    def app_main() -> None:
        mod = current_module[0]
        coco.mount(
            coco.component_subpath("A"), mod.foo_comp_calls_bar_memo, "A", "value1"
        )

    app = coco.App(
        coco.AppConfig(
            name="test_transitive_component_calls_bar_memo", environment=coco_env
        ),
        app_main,
    )

    # v1: first run — foo runs, bar_memo executes
    mod = _load_module(_V1_PATH, metrics, current_module)
    app.update()
    assert metrics.collect() == {"foo_comp_calls_bar_memo": 1, "bar_memo": 1}
    assert GlobalDictTarget.store.data["A"].data == "bar_v1: value1"

    # v1: second run — foo re-runs (non-memo component), bar_memo cached
    app.update()
    assert metrics.collect() == {"foo_comp_calls_bar_memo": 1}

    # v2: bar's logic changed — bar's fn memo invalidated
    mod = _load_module(_V2_PATH, metrics, current_module, old_module=mod)
    app.update()
    assert metrics.collect() == {"foo_comp_calls_bar_memo": 1, "bar_memo": 1}
    assert GlobalDictTarget.store.data["A"].data == "bar_v2: value1"

    # v2: second run — bar_memo cached again
    app.update()
    assert metrics.collect() == {"foo_comp_calls_bar_memo": 1}


def test_transitive_component_mounts_bar_comp_plain() -> None:
    """foo (component) mounts bar (non-memo component). bar changes → bar re-runs with new code."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.function
    def app_main() -> None:
        mod = current_module[0]
        coco.mount(
            coco.component_subpath("A"),
            mod.foo_comp_mounts_bar_comp_plain,
            "A",
            "value1",
        )

    app = coco.App(
        coco.AppConfig(
            name="test_transitive_component_mounts_bar_comp_plain", environment=coco_env
        ),
        app_main,
    )

    # v1: first run — foo and bar both execute
    mod = _load_module(_V1_PATH, metrics, current_module)
    app.update()
    assert metrics.collect() == {
        "foo_comp_mounts_bar_comp_plain": 1,
        "bar_comp_plain": 1,
    }
    assert GlobalDictTarget.store.data["A"].data == "bar_v1: value1"

    # v1: second run — both non-memo, both re-run
    app.update()
    assert metrics.collect() == {
        "foo_comp_mounts_bar_comp_plain": 1,
        "bar_comp_plain": 1,
    }

    # v2: bar's code changed — bar produces new output
    mod = _load_module(_V2_PATH, metrics, current_module, old_module=mod)
    app.update()
    assert metrics.collect() == {
        "foo_comp_mounts_bar_comp_plain": 1,
        "bar_comp_plain": 1,
    }
    assert GlobalDictTarget.store.data["A"].data == "bar_v2: value1"


def test_transitive_component_mounts_bar_comp_memo() -> None:
    """foo (component) mounts bar (memo component). bar changes → bar's component memo invalidated."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.function
    def app_main() -> None:
        mod = current_module[0]
        coco.mount(
            coco.component_subpath("A"),
            mod.foo_comp_mounts_bar_comp_memo,
            "A",
            "value1",
        )

    app = coco.App(
        coco.AppConfig(
            name="test_transitive_component_mounts_bar_comp_memo", environment=coco_env
        ),
        app_main,
    )

    # v1: first run — foo runs, bar_comp_memo executes
    mod = _load_module(_V1_PATH, metrics, current_module)
    app.update()
    assert metrics.collect() == {
        "foo_comp_mounts_bar_comp_memo": 1,
        "bar_comp_memo": 1,
    }
    assert GlobalDictTarget.store.data["A"].data == "bar_v1: value1"

    # v1: second run — foo re-runs (non-memo), bar_comp_memo cached (memo component)
    app.update()
    assert metrics.collect() == {"foo_comp_mounts_bar_comp_memo": 1}

    # v2: bar's code changed — bar's component memo invalidated
    mod = _load_module(_V2_PATH, metrics, current_module, old_module=mod)
    app.update()
    assert metrics.collect() == {
        "foo_comp_mounts_bar_comp_memo": 1,
        "bar_comp_memo": 1,
    }
    assert GlobalDictTarget.store.data["A"].data == "bar_v2: value1"

    # v2: second run — bar_comp_memo cached again
    app.update()
    assert metrics.collect() == {"foo_comp_mounts_bar_comp_memo": 1}
