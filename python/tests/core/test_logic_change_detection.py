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
from tests.common.environment import get_env_db_path
from tests.common.target_states import GlobalDictTarget, Metrics
from tests.common.module_utils import load_module_as


coco_env = common.create_test_env(__file__)

_TEST_DIR = pathlib.Path(__file__).parent
_V1_PATH = str(_TEST_DIR / "mod_logic_v1.py")
_V2_PATH = str(_TEST_DIR / "mod_logic_v2.py")
_VER1_PATH = str(_TEST_DIR / "mod_logic_ver1.py")
_VER2_PATH = str(_TEST_DIR / "mod_logic_ver2.py")
_SELF_V1_PATH = str(_TEST_DIR / "mod_logic_self_v1.py")
_SELF_V2_PATH = str(_TEST_DIR / "mod_logic_self_v2.py")
_SELF_V3_PATH = str(_TEST_DIR / "mod_logic_self_v3.py")
_NONE_V1_PATH = str(_TEST_DIR / "mod_logic_none_v1.py")
_NONE_V2_PATH = str(_TEST_DIR / "mod_logic_none_v2.py")
_CHAIN_V1_PATH = str(_TEST_DIR / "mod_logic_chain_v1.py")
_CHAIN_V2_PATH = str(_TEST_DIR / "mod_logic_chain_v2.py")
_CHAIN_V3_PATH = str(_TEST_DIR / "mod_logic_chain_v3.py")
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

    @coco.fn
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
    app.update_blocking()
    assert metrics.collect() == {"transform_memo": 1}
    assert GlobalDictTarget.store.data["A"].data == "v1: value1"

    # v1: second run — memo hit
    app.update_blocking()
    assert metrics.collect() == {}

    # v2: logic changed — memo invalidated, function re-executes
    mod = _load_module(_V2_PATH, metrics, current_module, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == {"transform_memo": 1}
    assert GlobalDictTarget.store.data["A"].data == "v2: value1"

    # v2: second run — memo hit again
    app.update_blocking()
    assert metrics.collect() == {}


# ============================================================================
# E3: Component memo invalidated on logic change
# ============================================================================


def test_component_memo_invalidated_on_logic_change() -> None:
    """A memoized component's memo is invalidated when its code changes."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.fn
    async def app_main() -> None:
        mod = current_module[0]
        await coco.mount(
            coco.component_subpath("A"), mod.declare_entry_memo, "A", "value1"
        )

    app = coco.App(
        coco.AppConfig(
            name="test_component_memo_invalidated_on_logic_change", environment=coco_env
        ),
        app_main,
    )

    # v1: first run — component executes
    mod = _load_module(_V1_PATH, metrics, current_module)
    app.update_blocking()
    assert metrics.collect() == {"declare_entry_memo": 1}
    assert GlobalDictTarget.store.data["A"].data == "v1: value1"

    # v1: second run — component memo hit
    app.update_blocking()
    assert metrics.collect() == {}

    # v2: logic changed — component memo invalidated
    mod = _load_module(_V2_PATH, metrics, current_module, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == {"declare_entry_memo": 1}
    assert GlobalDictTarget.store.data["A"].data == "v2: value1"

    # v2: second run — component memo hit again
    app.update_blocking()
    assert metrics.collect() == {}


# ============================================================================
# E4: Transitive — foo (memoized fn) calls bar, bar changes
# ============================================================================


def test_transitive_fn_memo_bar_memo_changes() -> None:
    """foo (memo) calls bar (memo). bar's code changes → foo's memo invalidated."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.fn
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
    app.update_blocking()
    assert metrics.collect() == {"foo_calls_bar_memo": 1, "bar_memo": 1}
    assert GlobalDictTarget.store.data["A"].data == "bar_v1: value1"

    # v1: second run — foo memo hit (bar not called either)
    app.update_blocking()
    assert metrics.collect() == {}

    # v2: bar's logic changed — foo's memo invalidated transitively
    mod = _load_module(_V2_PATH, metrics, current_module, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == {"foo_calls_bar_memo": 1, "bar_memo": 1}
    assert GlobalDictTarget.store.data["A"].data == "bar_v2: value1"

    # v2: second run — memo hit again
    app.update_blocking()
    assert metrics.collect() == {}


def test_transitive_fn_memo_bar_plain_changes() -> None:
    """foo (memo) calls bar (non-memo). bar's code changes → foo's memo invalidated."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.fn
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
    app.update_blocking()
    assert metrics.collect() == {"foo_calls_bar_plain": 1, "bar_plain": 1}
    assert GlobalDictTarget.store.data["A"].data == "bar_v1: value1"

    # v1: second run — foo memo hit (bar not called since foo is cached)
    app.update_blocking()
    assert metrics.collect() == {}

    # v2: bar's logic changed — foo's memo invalidated transitively
    mod = _load_module(_V2_PATH, metrics, current_module, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == {"foo_calls_bar_plain": 1, "bar_plain": 1}
    assert GlobalDictTarget.store.data["A"].data == "bar_v2: value1"

    # v2: second run — memo hit again
    app.update_blocking()
    assert metrics.collect() == {}


# ============================================================================
# E5: Transitive — foo (component) calls/mounts bar, bar changes
# ============================================================================


def test_transitive_component_calls_bar_memo() -> None:
    """foo (component) calls bar (memo fn). bar changes → bar's fn memo invalidated."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.fn
    async def app_main() -> None:
        mod = current_module[0]
        await coco.mount(
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
    app.update_blocking()
    assert metrics.collect() == {"foo_comp_calls_bar_memo": 1, "bar_memo": 1}
    assert GlobalDictTarget.store.data["A"].data == "bar_v1: value1"

    # v1: second run — foo re-runs (non-memo component), bar_memo cached
    app.update_blocking()
    assert metrics.collect() == {"foo_comp_calls_bar_memo": 1}

    # v2: bar's logic changed — bar's fn memo invalidated
    mod = _load_module(_V2_PATH, metrics, current_module, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == {"foo_comp_calls_bar_memo": 1, "bar_memo": 1}
    assert GlobalDictTarget.store.data["A"].data == "bar_v2: value1"

    # v2: second run — bar_memo cached again
    app.update_blocking()
    assert metrics.collect() == {"foo_comp_calls_bar_memo": 1}


def test_transitive_component_mounts_bar_comp_plain() -> None:
    """foo (component) mounts bar (non-memo component). bar changes → bar re-runs with new code."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.fn
    async def app_main() -> None:
        mod = current_module[0]
        await coco.mount(
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
    app.update_blocking()
    assert metrics.collect() == {
        "foo_comp_mounts_bar_comp_plain": 1,
        "bar_comp_plain": 1,
    }
    assert GlobalDictTarget.store.data["A"].data == "bar_v1: value1"

    # v1: second run — both non-memo, both re-run
    app.update_blocking()
    assert metrics.collect() == {
        "foo_comp_mounts_bar_comp_plain": 1,
        "bar_comp_plain": 1,
    }

    # v2: bar's code changed — bar produces new output
    mod = _load_module(_V2_PATH, metrics, current_module, old_module=mod)
    app.update_blocking()
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

    @coco.fn
    async def app_main() -> None:
        mod = current_module[0]
        await coco.mount(
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
    app.update_blocking()
    assert metrics.collect() == {
        "foo_comp_mounts_bar_comp_memo": 1,
        "bar_comp_memo": 1,
    }
    assert GlobalDictTarget.store.data["A"].data == "bar_v1: value1"

    # v1: second run — foo re-runs (non-memo), bar_comp_memo cached (memo component)
    app.update_blocking()
    assert metrics.collect() == {"foo_comp_mounts_bar_comp_memo": 1}

    # v2: bar's code changed — bar's component memo invalidated
    mod = _load_module(_V2_PATH, metrics, current_module, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == {
        "foo_comp_mounts_bar_comp_memo": 1,
        "bar_comp_memo": 1,
    }
    assert GlobalDictTarget.store.data["A"].data == "bar_v2: value1"

    # v2: second run — bar_comp_memo cached again
    app.update_blocking()
    assert metrics.collect() == {"foo_comp_mounts_bar_comp_memo": 1}


# ============================================================================
# E8: Explicit version bump invalidates memo (identical function body)
# ============================================================================


def test_fn_memo_invalidated_on_version_bump() -> None:
    """Bumping the version= parameter invalidates memo even when the function body is identical."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.fn
    def app_main() -> None:
        mod = current_module[0]
        result = mod.transform_memo_ver("A", "value1")
        coco.declare_target_state(GlobalDictTarget.target_state("A", result))

    app = coco.App(
        coco.AppConfig(
            name="test_fn_memo_invalidated_on_version_bump", environment=coco_env
        ),
        app_main,
    )

    # version=1: first run — function executes
    mod = _load_module(_VER1_PATH, metrics, current_module)
    app.update_blocking()
    assert metrics.collect() == {"transform_memo_ver": 1}
    assert GlobalDictTarget.store.data["A"].data == "ver: value1"

    # version=1: second run — memo hit
    app.update_blocking()
    assert metrics.collect() == {}

    # version=2: identical body but version bumped — memo invalidated
    mod = _load_module(_VER2_PATH, metrics, current_module, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == {"transform_memo_ver": 1}
    assert GlobalDictTarget.store.data["A"].data == "ver: value1"

    # version=2: second run — memo hit again
    app.update_blocking()
    assert metrics.collect() == {}


# ============================================================================
# J1: "self" memoized function — NOT invalidated when child changes,
#     IS invalidated when own code changes
# ============================================================================


def test_self_mode_not_invalidated_when_child_changes() -> None:
    """A logic_tracking='self' memo fn is NOT invalidated when its child function changes."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.fn
    def app_main() -> None:
        mod = current_module[0]
        result = mod.foo_self("A", "value1")
        coco.declare_target_state(GlobalDictTarget.target_state("A", result))

    app = coco.App(
        coco.AppConfig(
            name="test_self_mode_not_invalidated_when_child_changes",
            environment=coco_env,
        ),
        app_main,
    )

    # v1: first run — foo_self and bar execute
    mod = _load_module(_SELF_V1_PATH, metrics, current_module)
    app.update_blocking()
    assert metrics.collect() == {"foo_self": 1, "bar": 1}
    assert GlobalDictTarget.store.data["A"].data == "bar_v1: value1"

    # v1: second run — memo hit
    app.update_blocking()
    assert metrics.collect() == {}

    # v2: bar changed but foo_self unchanged — memo NOT invalidated (self mode)
    mod = _load_module(_SELF_V2_PATH, metrics, current_module, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == {}
    # Still has v1 output since memo was reused
    assert GlobalDictTarget.store.data["A"].data == "bar_v1: value1"


def test_self_mode_invalidated_when_own_code_changes() -> None:
    """A logic_tracking='self' memo fn IS invalidated when its own code changes."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.fn
    def app_main() -> None:
        mod = current_module[0]
        result = mod.foo_self("A", "value1")
        coco.declare_target_state(GlobalDictTarget.target_state("A", result))

    app = coco.App(
        coco.AppConfig(
            name="test_self_mode_invalidated_when_own_code_changes",
            environment=coco_env,
        ),
        app_main,
    )

    # v1: first run — foo_self and bar execute
    mod = _load_module(_SELF_V1_PATH, metrics, current_module)
    app.update_blocking()
    assert metrics.collect() == {"foo_self": 1, "bar": 1}
    assert GlobalDictTarget.store.data["A"].data == "bar_v1: value1"

    # v1: second run — memo hit
    app.update_blocking()
    assert metrics.collect() == {}

    # v3: foo_self itself changed — memo invalidated
    mod = _load_module(_SELF_V3_PATH, metrics, current_module, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == {"foo_self": 1, "bar": 1}
    assert GlobalDictTarget.store.data["A"].data == "bar_v1: value1"

    # v3: second run — memo hit again
    app.update_blocking()
    assert metrics.collect() == {}


# ============================================================================
# J2: None memoized function — NOT invalidated on any function logic change
# ============================================================================


def test_none_mode_not_invalidated_on_any_logic_change() -> None:
    """A logic_tracking=None memo fn is NOT invalidated when any function code changes."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.fn
    def app_main() -> None:
        mod = current_module[0]
        result = mod.foo_none("A", "value1")
        coco.declare_target_state(GlobalDictTarget.target_state("A", result))

    app = coco.App(
        coco.AppConfig(
            name="test_none_mode_not_invalidated_on_any_logic_change",
            environment=coco_env,
        ),
        app_main,
    )

    # v1: first run — foo_none and bar execute
    mod = _load_module(_NONE_V1_PATH, metrics, current_module)
    app.update_blocking()
    assert metrics.collect() == {"foo_none": 1, "bar": 1}
    assert GlobalDictTarget.store.data["A"].data == "bar_v1: value1"

    # v1: second run — memo hit
    app.update_blocking()
    assert metrics.collect() == {}

    # v2: both foo_none and bar changed — memo NOT invalidated (None mode)
    mod = _load_module(_NONE_V2_PATH, metrics, current_module, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == {}
    # Still has v1 output since memo was reused
    assert GlobalDictTarget.store.data["A"].data == "bar_v1: value1"


# ============================================================================
# J3: None function still tracks context key deps
# ============================================================================

_TRACKED_KEY_J3 = coco.ContextKey[str]("_test_logic_tracking_none_ctx_j3")


def _create_env_with_ctx(db_name: str, value: str) -> coco.Environment:
    """Create an Environment with a tracked context value."""
    ctx = coco.ContextProvider()
    ctx.provide(_TRACKED_KEY_J3, value)
    settings = coco.Settings.from_env(db_path=get_env_db_path(db_name))
    return coco.Environment(settings, context_provider=ctx)


def test_none_mode_still_tracks_context_key_deps() -> None:
    """A logic_tracking=None memo fn is still invalidated when a tracked context value changes."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()

    db_name = "test_none_mode_ctx_deps"

    @coco.fn(memo=True, logic_tracking=None)
    def process(name: str, content: str) -> None:
        val = coco.use_context(_TRACKED_KEY_J3)
        metrics.increment("process")
        coco.declare_target_state(
            GlobalDictTarget.target_state(name, f"{val}:{content}")
        )

    @coco.fn
    async def app_main() -> None:
        await coco.mount(coco.component_subpath("A"), process, "A", "data")

    # Phase 1: value="v1" — executes then memo hit
    env1 = _create_env_with_ctx(db_name, "v1")
    app1 = coco.App(coco.AppConfig(name=db_name, environment=env1), app_main)
    app1.update_blocking()
    assert metrics.collect() == {"process": 1}
    assert GlobalDictTarget.store.data["A"].data == "v1:data"
    app1.update_blocking()
    assert metrics.collect() == {}

    del app1, env1
    gc.collect()

    # Phase 2: value="v2" — tracked key changed, memo invalidated despite None mode
    env2 = _create_env_with_ctx(db_name, "v2")
    app2 = coco.App(coco.AppConfig(name=db_name, environment=env2), app_main)
    app2.update_blocking()
    assert metrics.collect() == {"process": 1}
    assert GlobalDictTarget.store.data["A"].data == "v2:data"
    app2.update_blocking()
    assert metrics.collect() == {}


# ============================================================================
# J4: Transitive — foo("full") → bar("self") → baz
#     foo invalidated when bar changes, NOT when baz changes
# ============================================================================


def test_transitive_full_self_plain_bar_changes() -> None:
    """foo(full) → bar(self) → baz. bar changes → foo invalidated."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.fn
    def app_main() -> None:
        mod = current_module[0]
        result = mod.foo_full("A", "value1")
        coco.declare_target_state(GlobalDictTarget.target_state("A", result))

    app = coco.App(
        coco.AppConfig(
            name="test_transitive_full_self_plain_bar_changes",
            environment=coco_env,
        ),
        app_main,
    )

    # v1: first run — all three execute
    mod = _load_module(_CHAIN_V1_PATH, metrics, current_module)
    app.update_blocking()
    assert metrics.collect() == {"foo_full": 1, "bar_self": 1, "baz": 1}
    assert GlobalDictTarget.store.data["A"].data == "baz_v1: value1"

    # v1: second run — foo memo hit (bar memo also hit inside)
    app.update_blocking()
    assert metrics.collect() == {}

    # v2: bar_self changed — foo invalidated (full propagates bar's fp)
    mod = _load_module(_CHAIN_V2_PATH, metrics, current_module, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == {"foo_full": 1, "bar_self": 1, "baz": 1}
    assert GlobalDictTarget.store.data["A"].data == "baz_v1: value1"

    # v2: second run — memo hit again
    app.update_blocking()
    assert metrics.collect() == {}


def test_transitive_full_self_plain_baz_changes() -> None:
    """foo(full) → bar(self) → baz. baz changes → foo NOT invalidated."""
    GlobalDictTarget.store.clear()
    metrics = Metrics()
    current_module: list[Any] = []

    @coco.fn
    def app_main() -> None:
        mod = current_module[0]
        result = mod.foo_full("A", "value1")
        coco.declare_target_state(GlobalDictTarget.target_state("A", result))

    app = coco.App(
        coco.AppConfig(
            name="test_transitive_full_self_plain_baz_changes",
            environment=coco_env,
        ),
        app_main,
    )

    # v1: first run — all three execute
    mod = _load_module(_CHAIN_V1_PATH, metrics, current_module)
    app.update_blocking()
    assert metrics.collect() == {"foo_full": 1, "bar_self": 1, "baz": 1}
    assert GlobalDictTarget.store.data["A"].data == "baz_v1: value1"

    # v1: second run — foo memo hit
    app.update_blocking()
    assert metrics.collect() == {}

    # v3: baz changed but bar_self is "self" mode — baz's fp not propagated past bar
    # foo NOT invalidated
    mod = _load_module(_CHAIN_V3_PATH, metrics, current_module, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == {}
    # Still has v1 output since foo memo was reused
    assert GlobalDictTarget.store.data["A"].data == "baz_v1: value1"
