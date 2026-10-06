"""End-to-end tests for the callback tunnel (``coco.tunnel``) and the
record-only marker (``coco.record_callee_logic``).

Case numbers refer to the matrix in
``specs/logic_change_detection/callback_tunnel.md``.

Roles (names from the spec):

- ``T`` — ``traverse*``: memoized library functions with
  ``logic_tracking="self"`` that call a callback they received.
- ``H`` — ``higher_level*``: ``"self"`` layers that forward (or create)
  callbacks.
- ``O`` — the owner: a ``"full"`` frame that creates the callback and tunnels
  it. Either a memoized module-level ``owner*`` function (so its own entry is
  observable) or the test's ``app_main`` (a *root owner*, whose body always
  runs, which isolates what the entries below it recorded).

T, H and O are defined here and never change. Only the callbacks (``cb``,
``cb2``, ``acb``, ``acb2``) live in the versioned fixture modules: v2 edits
``cb``/``acb``, v3 edits ``cb2``/``acb2``.
"""

import asyncio
import gc
import pathlib
import sys
from collections.abc import Awaitable, Callable, Iterator
from types import ModuleType
from typing import Any

import pytest

import cocoindex as coco

from tests import common
from tests.common.module_utils import load_module_as, unload_module_functions
from tests.common.target_states import GlobalDictTarget, Metrics

coco_env = common.create_test_env(__file__)

_TEST_DIR = pathlib.Path(__file__).parent
_V1_PATH = str(_TEST_DIR / "mod_logic_tunnel_v1.py")
_V2_PATH = str(_TEST_DIR / "mod_logic_tunnel_v2.py")
_V3_PATH = str(_TEST_DIR / "mod_logic_tunnel_v3.py")
_FAKE_MODULE = "tests.core._dynamic_callback_tunnel_module"

# Per-test state read by the module-level T/H/O functions below.
_metrics: list[Metrics] = []
_current_module: list[Any] = []


def _m() -> Metrics:
    return _metrics[0]


def _mod() -> Any:
    return _current_module[0]


def _reset() -> Metrics:
    GlobalDictTarget.store.clear()
    _metrics.clear()
    _metrics.append(Metrics())
    return _metrics[0]


def _load_module(module_path: str, old_module: ModuleType | None = None) -> ModuleType:
    """Load a module version, unregistering the old module's fingerprints first."""
    if old_module is not None:
        unload_module_functions(old_module)
    _current_module.clear()
    mod = load_module_as(module_path, _FAKE_MODULE)
    mod.set_metrics(_m())
    _current_module.append(mod)
    return mod


def _declare(key: str, result: str) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(key, result))


def _stored(key: str) -> Any:
    return GlobalDictTarget.store.data[key].data


@pytest.fixture(autouse=True)
def _cleanup_dynamic_module() -> Iterator[None]:
    gc.collect()
    yield
    mod = sys.modules.get(_FAKE_MODULE)
    if mod is not None:
        unload_module_functions(mod)
        del sys.modules[_FAKE_MODULE]


# ============================================================================
# T: memoized "self" library functions that call a callback they received.
# ============================================================================


@coco.fn(memo=True, logic_tracking="self", version=1)
def traverse(fn: Callable[[str], str], value: str) -> str:
    _m().increment("traverse")
    return fn(value)


@coco.fn(memo=True, logic_tracking="self", version=1)
def traverse_recording(fn: Callable[[str], str], value: str) -> str:
    """T that records its callback's logic in its own entry (library marker)."""
    _m().increment("traverse_recording")
    with coco.record_callee_logic():
        return fn(value)


@coco.fn(memo=True, logic_tracking="self", version=1)
def traverse2(
    fn1: Callable[[str], str], fn2: Callable[[str], str], value: str, which: str
) -> str:
    """T with two callbacks; ``which`` selects which of them actually run."""
    _m().increment("traverse2")
    parts: list[str] = []
    if "1" in which:
        parts.append(fn1(value))
    if "2" in which:
        parts.append(fn2(value))
    return " | ".join(parts)


@coco.fn(memo=True, logic_tracking="self", version=1)
async def traverse_gather(
    fn1: Callable[[str], Awaitable[str]],
    fn2: Callable[[str], Awaitable[str]],
    value: str,
) -> str:
    """T calling two tunneled callbacks concurrently under one frame."""
    _m().increment("traverse_gather")
    results = await asyncio.gather(fn1(value + "a"), fn2(value + "b"), fn1(value + "c"))
    return " | ".join(results)


@coco.fn(memo=True, logic_tracking="self", version=1)
async def traverse_comp(fn: Callable[[str], str], key: str, value: str) -> None:
    """T mounted as a component (the git walk mounts ``accept``)."""
    _m().increment("traverse_comp")
    _declare(key, fn(value))


# ============================================================================
# H: "self" layers between the owner and T.
# ============================================================================


@coco.fn(logic_tracking="self", version=1)
def higher_level(fn: Callable[[str], str], value: str) -> str:
    _m().increment("higher_level")
    return traverse(fn, value)


@coco.fn(memo=True, logic_tracking="self", version=1)
def higher_level_memo(fn: Callable[[str], str], value: str) -> str:
    _m().increment("higher_level_memo")
    return traverse(fn, value)


@coco.fn(logic_tracking="self", version=1)
def higher_level_outer(fn: Callable[[str], str], value: str) -> str:
    """Two "self" layers: H1 (this) -> H2 (memoized) -> T."""
    _m().increment("higher_level")
    return higher_level_memo(fn, value)


@coco.fn(logic_tracking="self", version=1)
def higher_level_internal_cb2(value: str) -> str:
    """H that creates its own callback and does NOT tunnel it."""
    _m().increment("higher_level")
    return traverse(_mod().cb2, value)


@coco.fn(logic_tracking="self", version=1)
def higher_level_internal_cb2_recording(value: str) -> str:
    """Same, but T records its callees' logic itself."""
    _m().increment("higher_level")
    return traverse_recording(_mod().cb2, value)


@coco.fn(logic_tracking="self", version=1)
def higher_level_two(fn1: Callable[[str], str], value: str, which: str) -> str:
    """H receives ``fn1`` from above and creates ``cb2``, tunneled to itself."""
    _m().increment("higher_level")
    return traverse2(fn1, coco.tunnel(_mod().cb2), value, which)


@coco.fn(logic_tracking="self", version=1)
async def higher_level_gather(fn1: Callable[[str], Awaitable[str]], value: str) -> str:
    """H tunnels ``acb2`` to itself and runs two T calls concurrently."""
    _m().increment("higher_level")
    fn2 = coco.tunnel(_mod().acb2)
    r1, r2 = await asyncio.gather(
        traverse_gather(fn1, fn2, value + "1"),
        traverse_gather(fn1, fn2, value + "2"),
    )
    return r1 + " || " + r2


@coco.fn(logic_tracking="self", version=1)
async def higher_level_mounts(fn: Callable[[str], str], key: str, value: str) -> None:
    _m().increment("higher_level")
    await coco.use_mount(coco.component_subpath(key), traverse_comp, fn, key, value)


# ============================================================================
# O: owners. "full" frames that create a callback and tunnel it.
# ============================================================================


@coco.fn(memo=True)
def owner(key: str, value: str) -> str:
    _m().increment("owner")
    return higher_level(coco.tunnel(_mod().cb), value)


@coco.fn(memo=True)
def owner_internal(key: str, value: str) -> str:
    _m().increment("owner")
    return higher_level_internal_cb2(value)


@coco.fn(memo=True)
def owner_two(key: str, value: str, which: str) -> str:
    _m().increment("owner")
    return higher_level_two(coco.tunnel(_mod().cb), value, which)


@coco.fn(memo=True)
def owner_forwarding(key: str, value: str) -> str:
    _m().increment("owner")
    return higher_level_outer(coco.tunnel(_mod().cb), value)


@coco.fn
def owner_plain(value: str) -> str:
    """A "full" owner that is not memoized (so a "self" ancestor is observable)."""
    _m().increment("owner")
    return traverse(coco.tunnel(_mod().cb), value)


@coco.fn(memo=True, logic_tracking="self", version=1)
def self_ancestor(key: str, value: str) -> str:
    _m().increment("self_ancestor")
    return owner_plain(value)


@coco.fn(memo=True)
async def owner_gather(key: str, value: str) -> str:
    _m().increment("owner")
    return await higher_level_gather(coco.tunnel(_mod().acb), value)


@coco.fn
async def owner_mounting(key: str, value: str) -> None:
    """A "full" owner inside component P's body; mounts, so cannot be memoized."""
    _m().increment("owner")
    await higher_level_mounts(coco.tunnel(_mod().cb), key, value)


@coco.fn(memo=True)
async def parent_comp_memo(key: str, value: str) -> None:
    _m().increment("parent_comp")
    await owner_mounting(key, value)


@coco.fn
async def parent_comp_plain(key: str, value: str) -> None:
    _m().increment("parent_comp")
    await owner_mounting(key, value)


# ============================================================================
# Case 1 — every entry on the path records the tunneled callback.
# ============================================================================


def test_tunneled_callback_recorded_in_every_entry_on_the_path() -> None:
    """O (memo, full) tunnels cb; H (self) forwards it; T (memo, self) calls it.
    Editing cb must miss both T's and O's entries."""
    metrics = _reset()

    @coco.fn
    def app_main() -> None:
        _declare("A", owner("A", "value1"))

    app = coco.App(
        coco.AppConfig(name="test_tunnel_every_entry_on_path", environment=coco_env),
        app_main,
    )

    mod = _load_module(_V1_PATH)
    app.update_blocking()
    assert metrics.collect() == {"owner": 1, "higher_level": 1, "traverse": 1, "cb": 1}
    assert _stored("A") == "cb_v1: value1"

    app.update_blocking()
    assert metrics.collect() == {}

    # Only cb changed. O's entry recorded cb (resolved at the owner, "full"),
    # T's entry recorded it too (tagged set stored in transit).
    mod = _load_module(_V2_PATH, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == {"owner": 1, "higher_level": 1, "traverse": 1, "cb": 1}
    assert _stored("A") == "cb_v2: value1"

    app.update_blocking()
    assert metrics.collect() == {}


# ============================================================================
# Case 6 — stale intermediate: T alone must catch the edit when the owner
# re-runs anyway (goal 3).
# ============================================================================


def test_intermediate_memo_misses_after_callback_edit_under_root_owner() -> None:
    """The owner is app_main, whose body always runs. Without the tunnel T
    would hit and serve the v1 result after cb is edited."""
    metrics = _reset()

    @coco.fn
    def app_main() -> None:
        _declare("A", higher_level(coco.tunnel(_mod().cb), "value1"))

    app = coco.App(
        coco.AppConfig(name="test_tunnel_stale_intermediate", environment=coco_env),
        app_main,
    )

    mod = _load_module(_V1_PATH)
    app.update_blocking()
    assert metrics.collect() == {"higher_level": 1, "traverse": 1, "cb": 1}

    app.update_blocking()
    assert metrics.collect() == {"higher_level": 1}  # T hit

    mod = _load_module(_V2_PATH, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == {"higher_level": 1, "traverse": 1, "cb": 1}
    assert _stored("A") == "cb_v2: value1"


# ============================================================================
# Case 2 — an untunneled callback created inside H is H's own detail.
# ============================================================================


def test_untunneled_callback_is_invisible_above_traverse() -> None:
    """H creates cb2 and passes it to T without tunneling. Editing cb2 changes
    nothing above T: O's entry never saw it (H is "self")."""
    metrics = _reset()

    @coco.fn
    def app_main() -> None:
        _declare("A", owner_internal("A", "value1"))

    app = coco.App(
        coco.AppConfig(name="test_untunneled_invisible_above_T", environment=coco_env),
        app_main,
    )

    mod = _load_module(_V1_PATH)
    app.update_blocking()
    assert metrics.collect() == {"owner": 1, "higher_level": 1, "traverse": 1, "cb2": 1}

    mod = _load_module(_V3_PATH, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == {}
    assert _stored("A") == "cb2_v1: value1"


def test_untunneled_callback_leaves_traverse_stale_unless_recorded() -> None:
    """With a root owner, T's own entry is what matters. A plain "self" T
    misses the edit (documented hole); a T using ``record_callee_logic`` does
    not."""
    metrics = _reset()

    @coco.fn
    def app_main_plain() -> None:
        _declare("A", higher_level_internal_cb2("value1"))

    @coco.fn
    def app_main_recording() -> None:
        _declare("B", higher_level_internal_cb2_recording("value1"))

    app_plain = coco.App(
        coco.AppConfig(name="test_untunneled_stale_plain", environment=coco_env),
        app_main_plain,
    )
    app_recording = coco.App(
        coco.AppConfig(name="test_untunneled_stale_recording", environment=coco_env),
        app_main_recording,
    )

    mod = _load_module(_V1_PATH)
    app_plain.update_blocking()
    assert metrics.collect() == {"higher_level": 1, "traverse": 1, "cb2": 1}
    app_recording.update_blocking()
    assert metrics.collect() == {"higher_level": 1, "traverse_recording": 1, "cb2": 1}

    app_plain.update_blocking()
    assert metrics.collect() == {"higher_level": 1}
    app_recording.update_blocking()
    assert metrics.collect() == {"higher_level": 1}

    mod = _load_module(_V3_PATH, old_module=mod)
    # Hole: T is "self" and nobody tunneled cb2 — T hits with the old callback.
    app_plain.update_blocking()
    assert metrics.collect() == {"higher_level": 1}
    assert _stored("A") == "cb2_v1: value1"
    # Closed by the marker: T's own entry recorded cb2.
    app_recording.update_blocking()
    assert metrics.collect() == {"higher_level": 1, "traverse_recording": 1, "cb2": 1}
    assert _stored("B") == "cb2_v3: value1"


# ============================================================================
# Case 3 — two owners; each tag resolves at its own frame.
# ============================================================================


def test_two_owners_edit_of_outer_callback_reaches_everything() -> None:
    """O tunnels cb; H tunnels cb2 to itself; T calls both. Editing cb misses
    T and O."""
    metrics = _reset()

    @coco.fn
    def app_main() -> None:
        _declare("A", owner_two("A", "value1", "12"))

    app = coco.App(
        coco.AppConfig(name="test_two_owners_outer_edit", environment=coco_env),
        app_main,
    )

    mod = _load_module(_V1_PATH)
    app.update_blocking()
    assert metrics.collect() == {
        "owner": 1,
        "higher_level": 1,
        "traverse2": 1,
        "cb": 1,
        "cb2": 1,
    }
    assert _stored("A") == "cb_v1: value1 | cb2_v1: value1"

    app.update_blocking()
    assert metrics.collect() == {}

    mod = _load_module(_V2_PATH, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == {
        "owner": 1,
        "higher_level": 1,
        "traverse2": 1,
        "cb": 1,
        "cb2": 1,
    }
    assert _stored("A") == "cb_v2: value1 | cb2_v1: value1"


def test_callback_owned_by_self_layer_reaches_traverse_and_stops_there() -> None:
    """cb2 is tunneled by H ("self"): T's entry records it, but at H the tag
    resolves and H's mode drops it, so O above sees nothing."""
    metrics = _reset()

    @coco.fn
    def app_main_memo_owner() -> None:
        _declare("A", owner_two("A", "value1", "12"))

    @coco.fn
    def app_main_root_owner() -> None:
        _declare("B", higher_level_two(coco.tunnel(_mod().cb), "value1", "12"))

    app_memo = coco.App(
        coco.AppConfig(name="test_self_owner_stops_memo", environment=coco_env),
        app_main_memo_owner,
    )
    app_root = coco.App(
        coco.AppConfig(name="test_self_owner_stops_root", environment=coco_env),
        app_main_root_owner,
    )

    mod = _load_module(_V1_PATH)
    app_memo.update_blocking()
    metrics.collect()
    app_root.update_blocking()
    assert metrics.collect() == {"higher_level": 1, "traverse2": 1, "cb": 1, "cb2": 1}

    app_memo.update_blocking()
    assert metrics.collect() == {}
    app_root.update_blocking()
    assert metrics.collect() == {"higher_level": 1}

    mod = _load_module(_V3_PATH, old_module=mod)
    # Stops at H: O's entry does not depend on cb2.
    app_memo.update_blocking()
    assert metrics.collect() == {}
    assert _stored("A") == "cb_v1: value1 | cb2_v1: value1"
    # Reaches T: with the owner above H always running, T misses.
    app_root.update_blocking()
    assert metrics.collect() == {"higher_level": 1, "traverse2": 1, "cb": 1, "cb2": 1}
    assert _stored("B") == "cb_v1: value1 | cb2_v3: value1"


def test_only_callbacks_that_ran_are_recorded() -> None:
    """T received two tunneled callbacks but called only fn1. Editing the one
    that never ran leaves T a hit; editing the one that ran misses."""
    metrics = _reset()

    @coco.fn
    def app_main() -> None:
        _declare("A", higher_level_two(coco.tunnel(_mod().cb), "value1", "1"))

    app = coco.App(
        coco.AppConfig(name="test_only_ran_callbacks_recorded", environment=coco_env),
        app_main,
    )

    mod = _load_module(_V1_PATH)
    app.update_blocking()
    assert metrics.collect() == {"higher_level": 1, "traverse2": 1, "cb": 1}

    mod = _load_module(_V3_PATH, old_module=mod)  # cb2 edited; it never ran
    app.update_blocking()
    assert metrics.collect() == {"higher_level": 1}
    assert _stored("A") == "cb_v1: value1"

    mod = _load_module(_V2_PATH, old_module=mod)  # cb edited
    app.update_blocking()
    assert metrics.collect() == {"higher_level": 1, "traverse2": 1, "cb": 1}
    assert _stored("A") == "cb_v2: value1"


# ============================================================================
# Case 4 — forwarding through two "self" layers, one of them memoized.
# ============================================================================


def test_tunnel_forwards_through_two_self_layers() -> None:
    metrics = _reset()

    @coco.fn
    def app_main_memo_owner() -> None:
        _declare("A", owner_forwarding("A", "value1"))

    @coco.fn
    def app_main_root_owner() -> None:
        _declare("B", higher_level_outer(coco.tunnel(_mod().cb), "value1"))

    app_memo = coco.App(
        coco.AppConfig(name="test_forward_two_layers_memo", environment=coco_env),
        app_main_memo_owner,
    )
    app_root = coco.App(
        coco.AppConfig(name="test_forward_two_layers_root", environment=coco_env),
        app_main_root_owner,
    )

    mod = _load_module(_V1_PATH)
    app_memo.update_blocking()
    assert metrics.collect() == {
        "owner": 1,
        "higher_level": 1,
        "higher_level_memo": 1,
        "traverse": 1,
        "cb": 1,
    }
    app_root.update_blocking()
    assert metrics.collect() == {
        "higher_level": 1,
        "higher_level_memo": 1,
        "traverse": 1,
        "cb": 1,
    }

    app_memo.update_blocking()
    assert metrics.collect() == {}
    app_root.update_blocking()
    assert metrics.collect() == {"higher_level": 1}

    mod = _load_module(_V2_PATH, old_module=mod)
    app_memo.update_blocking()
    assert metrics.collect() == {
        "owner": 1,
        "higher_level": 1,
        "higher_level_memo": 1,
        "traverse": 1,
        "cb": 1,
    }
    assert _stored("A") == "cb_v2: value1"
    # H2's entry recorded cb on its own account, not just via O.
    app_root.update_blocking()
    assert metrics.collect() == {
        "higher_level": 1,
        "higher_level_memo": 1,
        "traverse": 1,
        "cb": 1,
    }
    assert _stored("B") == "cb_v2: value1"


# ============================================================================
# Case 5 — a "self" ancestor above the owner sees nothing.
# ============================================================================


def test_self_ancestor_above_owner_sees_nothing() -> None:
    """S (memo, self) -> O (full) which tunnels cb into T. The tag resolves at
    O; S's entry does not depend on cb, so S hits after cb is edited."""
    metrics = _reset()

    @coco.fn
    def app_main() -> None:
        _declare("A", self_ancestor("A", "value1"))

    app = coco.App(
        coco.AppConfig(name="test_self_ancestor_sees_nothing", environment=coco_env),
        app_main,
    )

    mod = _load_module(_V1_PATH)
    app.update_blocking()
    assert metrics.collect() == {"self_ancestor": 1, "owner": 1, "traverse": 1, "cb": 1}

    mod = _load_module(_V2_PATH, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == {}
    assert _stored("A") == "cb_v1: value1"


# ============================================================================
# Case 7 — a mounted component on the path.
# ============================================================================


def test_mounted_component_on_the_path_records_the_callback() -> None:
    """P -> O (full, tunnels cb) -> H (self) -> use_mount(T_comp, cb) -> cb.
    T_comp's component memo records cb (the tagged set is flattened at the
    component boundary); a memoized P records it through the component tree."""
    metrics = _reset()

    @coco.fn
    async def app_main_plain_parent() -> None:
        await coco.mount(coco.component_subpath("P"), parent_comp_plain, "A", "value1")

    @coco.fn
    async def app_main_memo_parent() -> None:
        await coco.mount(coco.component_subpath("P"), parent_comp_memo, "B", "value1")

    app_plain = coco.App(
        coco.AppConfig(name="test_mounted_T_plain_parent", environment=coco_env),
        app_main_plain_parent,
    )
    app_memo = coco.App(
        coco.AppConfig(name="test_mounted_T_memo_parent", environment=coco_env),
        app_main_memo_parent,
    )
    all_ran = {
        "parent_comp": 1,
        "owner": 1,
        "higher_level": 1,
        "traverse_comp": 1,
        "cb": 1,
    }

    mod = _load_module(_V1_PATH)
    app_plain.update_blocking()
    assert metrics.collect() == all_ran
    app_memo.update_blocking()
    assert metrics.collect() == all_ran

    app_plain.update_blocking()
    assert metrics.collect() == {"parent_comp": 1, "owner": 1, "higher_level": 1}
    app_memo.update_blocking()
    assert metrics.collect() == {}

    mod = _load_module(_V2_PATH, old_module=mod)
    app_plain.update_blocking()
    assert metrics.collect() == all_ran
    assert _stored("A") == "cb_v2: value1"
    app_memo.update_blocking()
    assert metrics.collect() == all_ran
    assert _stored("B") == "cb_v2: value1"


# ============================================================================
# Case 8 — extent violations raise.
# ============================================================================


def test_tunnel_extent_violations_raise() -> None:
    metrics = _reset()
    escaped: list[Callable[[str], str]] = []
    checked: list[str] = []

    @coco.fn
    def owner_leaks_tunnel(value: str) -> None:
        escaped.append(coco.tunnel(_mod().cb))

    @coco.fn
    def app_main() -> None:
        owner_leaks_tunnel("x")
        # The owner's frame has closed; the call site is a sibling, not a descendant.
        with pytest.raises(RuntimeError, match="after the function that created"):
            escaped[0]("y")
        checked.append("inside")

    app = coco.App(
        coco.AppConfig(name="test_tunnel_extent_violation", environment=coco_env),
        app_main,
    )
    _load_module(_V1_PATH)
    app.update_blocking()
    assert checked == ["inside"]
    assert metrics.collect() == {}  # cb never ran

    # No component context at all.
    with pytest.raises(RuntimeError, match="no active component context"):
        escaped[0]("y")

    # Creating a tunnel needs a frame to own it.
    with pytest.raises(RuntimeError, match="No ComponentContext available"):
        coco.tunnel(_mod().cb)


# ============================================================================
# Case 9 — concurrency: gathered tunneled callbacks under one frame.
# ============================================================================


def test_gathered_tunneled_callbacks_tag_without_cross_talk() -> None:
    """O tunnels acb; H tunnels acb2 and gathers two T calls; each T gathers
    acb, acb2, acb. Editing acb misses O and both T entries."""
    metrics = _reset()

    @coco.fn
    async def app_main() -> None:
        _declare("A", await owner_gather("A", "v"))

    app = coco.App(
        coco.AppConfig(name="test_gather_tunnels_outer_edit", environment=coco_env),
        app_main,
    )
    all_ran = {"owner": 1, "higher_level": 1, "traverse_gather": 2, "acb": 4, "acb2": 2}

    mod = _load_module(_V1_PATH)
    app.update_blocking()
    assert metrics.collect() == all_ran
    assert _stored("A") == (
        "acb_v1: v1a | acb2_v1: v1b | acb_v1: v1c"
        " || acb_v1: v2a | acb2_v1: v2b | acb_v1: v2c"
    )

    app.update_blocking()
    assert metrics.collect() == {}

    mod = _load_module(_V2_PATH, old_module=mod)
    app.update_blocking()
    assert metrics.collect() == all_ran
    assert _stored("A") == (
        "acb_v2: v1a | acb2_v1: v1b | acb_v2: v1c"
        " || acb_v2: v2a | acb2_v1: v2b | acb_v2: v2c"
    )


def test_gathered_callback_owned_by_self_layer_stops_there() -> None:
    """Editing acb2 (tunneled by "self" H): O's entry is untouched, while both
    T entries recorded it."""
    metrics = _reset()

    @coco.fn
    async def app_main_memo_owner() -> None:
        _declare("A", await owner_gather("A", "v"))

    @coco.fn
    async def app_main_root_owner() -> None:
        _declare("B", await higher_level_gather(coco.tunnel(_mod().acb), "v"))

    app_memo = coco.App(
        coco.AppConfig(name="test_gather_self_owner_memo", environment=coco_env),
        app_main_memo_owner,
    )
    app_root = coco.App(
        coco.AppConfig(name="test_gather_self_owner_root", environment=coco_env),
        app_main_root_owner,
    )
    below_owner = {"higher_level": 1, "traverse_gather": 2, "acb": 4, "acb2": 2}

    mod = _load_module(_V1_PATH)
    app_memo.update_blocking()
    metrics.collect()
    app_root.update_blocking()
    assert metrics.collect() == below_owner

    app_memo.update_blocking()
    assert metrics.collect() == {}
    app_root.update_blocking()
    assert metrics.collect() == {"higher_level": 1}

    mod = _load_module(_V3_PATH, old_module=mod)
    app_memo.update_blocking()
    assert metrics.collect() == {}
    app_root.update_blocking()
    assert metrics.collect() == below_owner
    assert _stored("B") == (
        "acb_v1: v1a | acb2_v3: v1b | acb_v1: v1c"
        " || acb_v1: v2a | acb2_v3: v2b | acb_v1: v2c"
    )
