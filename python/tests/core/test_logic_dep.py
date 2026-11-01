"""``coco.LogicDep`` / ``coco.use_logic_dep``: a logic dependency declared at
import and recorded per call (the function guide, § Per-call dependencies).

A later release that changes a dependency's value is simulated by constructing
a new ``LogicDep`` under the same name and dropping the old one: its ``__del__``
leaves the process's logic set, as an edited function's fingerprint does when
its module is reloaded. The functions below never change; only the recorded
dependency does.
"""

from __future__ import annotations

import copy
import gc
from collections.abc import Callable, Iterator
from typing import Any

import pytest

import cocoindex as coco

from tests import common
from tests.common.target_states import GlobalDictTarget, Metrics

coco_env = common.create_test_env(__file__)

_metrics: list[Metrics] = []
# The current "release" of each named dependency, swapped by the tests.
_deps: dict[str, coco.LogicDep] = {}


def _m() -> Metrics:
    return _metrics[0]


def _dep(name: str) -> coco.LogicDep:
    return _deps[name]


def _release(name: str, version: int) -> None:
    """Ship ``name`` at ``version``: the previous object, if any, is dropped and
    leaves the logic set."""
    _deps[name] = coco.LogicDep(("dep", name, version))
    gc.collect()


def _declare(key: str, result: str) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(key, result))


def _stored(key: str) -> object:
    return GlobalDictTarget.store.data[key].data


@pytest.fixture(autouse=True)
def _fresh_state() -> Iterator[None]:
    GlobalDictTarget.store.clear()
    _metrics.clear()
    _metrics.append(Metrics())
    _deps.clear()
    gc.collect()
    yield
    _deps.clear()
    gc.collect()


# ============================================================================
# Functions under test. Defined once, never edited.
# ============================================================================


@coco.fn(memo=True)
def leaf(key: str, value: str) -> str:
    """A memoized function recording a dependency in its own frame."""
    coco.use_logic_dep(_dep("rust"))
    _m().increment("leaf")
    return f"leaf: {value}"


@coco.fn(memo=True)
def caller_of_leaf(key: str, value: str) -> str:
    """A memoized caller with a dependency of its own, over the memoized leaf."""
    coco.use_logic_dep(_dep("caller"))
    _m().increment("caller_of_leaf")
    return leaf(key, value)


@coco.fn
def records(name: str, value: str) -> str:
    """A plain "full" callee that records ``name``'s dependency."""
    coco.use_logic_dep(_dep(name))
    _m().increment("records")
    return f"{name}: {value}"


@coco.fn(memo=True)
def caller_full(key: str, value: str) -> str:
    _m().increment("caller_full")
    return records("rust", value)


@coco.fn(memo=True, logic_tracking="self", version=1)
def caller_self(key: str, value: str) -> str:
    _m().increment("caller_self")
    return records("rust", value)


@coco.fn(memo=True, logic_tracking="self", version=1)
def traverse(fn: Callable[[str], str], value: str) -> str:
    """A memoized "self" library function calling a callback it received."""
    _m().increment("traverse")
    return fn(value)


@coco.fn(logic_tracking="self", version=1)
def visit(value: str) -> str:
    """A "self" callback whose dependency is decided per call by the value's
    "language" (its suffix)."""
    coco.use_logic_dep(_dep(value.rsplit(".", 1)[-1]))
    _m().increment(f"visit:{value}")
    return f"visit: {value}"


@coco.fn(memo=True)
def owner(key: str, value: str) -> str:
    _m().increment("owner")
    return traverse(coco.logic_propagated_to_callers(visit), value)


@coco.fn(memo=True, logic_tracking="self", version=1)
def owner_self(key: str, value: str) -> str:
    """A memoized "self" owner with a dependency of its own: on a miss the
    tunneled callback's dependency stops here, recorded in this entry."""
    coco.use_logic_dep(_dep("owner"))
    _m().increment("owner_self")
    return traverse(coco.logic_propagated_to_callers(visit), value)


@coco.fn(memo=True, logic_tracking="self", version=1)
async def file_comp(name: str) -> None:
    """A file of a tree walk, mounted as a memoized component: records the
    dependency of its "language"."""
    coco.use_logic_dep(_dep(name.rsplit(".", 1)[-1]))
    _m().increment(f"file:{name}")
    _declare(name, f"ok {name}")


@coco.fn(memo=True, logic_tracking="self", version=1)
async def dir_comp(name: str, files: tuple[str, ...]) -> None:
    _m().increment(f"dir:{name}")
    for file in files:
        await coco.use_mount(coco.component_subpath(file), file_comp, file)


@coco.fn(memo=True, logic_tracking="self", version=1)
async def root_comp(dirs: tuple[tuple[str, tuple[str, ...]], ...]) -> None:
    _m().increment("dir:root")
    for name, files in dirs:
        await coco.use_mount(coco.component_subpath(name), dir_comp, name, files)


# ============================================================================
# Tests
# ============================================================================


def _app(name: str, main: Callable[..., object]) -> coco.App[Any, Any]:
    return coco.App(coco.AppConfig(name=name, environment=coco_env), main)


def test_a_dependency_recorded_in_a_memoized_function_moves_its_entry() -> None:
    @coco.fn
    def app_main() -> None:
        _declare("A", leaf("A", "value1"))

    app = _app("test_logic_dep_memoized_function", app_main)
    _release("rust", 1)
    app.update_blocking()
    assert _m().collect() == {"leaf": 1}
    assert _stored("A") == "leaf: value1"

    app.update_blocking()
    assert _m().collect() == {}, "an unchanged dependency must memo-hit"

    _release("rust", 2)
    app.update_blocking()
    assert _m().collect() == {"leaf": 1}, "a new release of the dependency re-runs"

    app.update_blocking()
    assert _m().collect() == {}


def test_a_memo_hit_re_offers_the_dependencies_its_entry_recorded() -> None:
    """A caller re-run for its own reason, over a callee that memo-hits: the
    caller's re-stored entry must still carry the dependency the callee's
    entry recorded, or a later release of it would leave the caller stale."""

    @coco.fn
    def app_main() -> None:
        _declare("A", caller_of_leaf("A", "value1"))

    app = _app("test_logic_dep_hit_re_offers", app_main)
    _release("caller", 1)
    _release("rust", 1)
    app.update_blocking()
    assert _m().collect() == {"caller_of_leaf": 1, "leaf": 1}

    _release("caller", 2)
    app.update_blocking()
    assert _m().collect() == {"caller_of_leaf": 1}, (
        "the leaf memo-hits under a re-run caller"
    )

    _release("rust", 2)
    app.update_blocking()
    assert _m().collect() == {"caller_of_leaf": 1, "leaf": 1}

    app.update_blocking()
    assert _m().collect() == {}


def test_a_dependency_travels_like_the_callees_own_fingerprint() -> None:
    """Offered through a "full" caller; hidden by a "self" caller, exactly as
    an edit to the callee's body would be."""

    @coco.fn
    def app_main() -> None:
        _declare("F", caller_full("F", "value1"))
        _declare("S", caller_self("S", "value1"))

    app = _app("test_logic_dep_callee_modes", app_main)
    _release("rust", 1)
    app.update_blocking()
    assert _m().collect() == {"caller_full": 1, "caller_self": 1, "records": 2}

    app.update_blocking()
    assert _m().collect() == {}

    _release("rust", 2)
    app.update_blocking()
    assert _m().collect() == {"caller_full": 1, "records": 1}

    app.update_blocking()
    assert _m().collect() == {}


def test_a_dependency_rides_the_callback_tunnel_through_a_self_layer() -> None:
    """The tree-walk shape: a memoized "self" library function calls a "self"
    callback carried by ``logic_propagated_to_callers``. The dependency the
    callback records per call lands in the library function's entry, so only
    the entries whose call recorded the changed dependency move."""

    @coco.fn
    def app_main() -> None:
        _declare("rs", owner("rs", "main.rs"))
        _declare("py", owner("py", "main.py"))

    app = _app("test_logic_dep_tunnel", app_main)
    _release("rs", 1)
    _release("py", 1)
    app.update_blocking()
    assert _m().collect() == {
        "owner": 2,
        "traverse": 2,
        "visit:main.rs": 1,
        "visit:main.py": 1,
    }

    app.update_blocking()
    assert _m().collect() == {}

    _release("rs", 2)
    app.update_blocking()
    assert _m().collect() == {"owner": 1, "traverse": 1, "visit:main.rs": 1}
    assert _stored("rs") == "visit: main.rs"

    app.update_blocking()
    assert _m().collect() == {}


def test_a_hit_under_a_self_owner_keeps_the_tunneled_dependency() -> None:
    """The owner is "self" and memoized; it re-runs for a reason of its own
    while the library function beneath it hits. On a miss the callback's
    dependency reaches the owner's entry through the tunnel regardless of
    the "self" frames between; the hit must not lose it, or a later release
    of the dependency would leave the owner a stale hit."""

    @coco.fn
    def app_main() -> None:
        _declare("O", owner_self("O", "main.rs"))

    app = _app("test_logic_dep_hit_under_self_owner", app_main)
    _release("owner", 1)
    _release("rs", 1)
    app.update_blocking()
    assert _m().collect() == {"owner_self": 1, "traverse": 1, "visit:main.rs": 1}

    _release("owner", 2)
    app.update_blocking()
    assert _m().collect() == {"owner_self": 1}, "the library function memo-hits"

    _release("rs", 2)
    app.update_blocking()
    assert _m().collect() == {"owner_self": 1, "traverse": 1, "visit:main.rs": 1}

    app.update_blocking()
    assert _m().collect() == {}


def test_a_dependency_reaches_every_ancestor_component_and_no_sibling() -> None:
    """A memoized component tree over files of two "languages": a new release
    of one language's dependency re-runs its files and every directory above
    them, while directories and files without that language memo-hit."""
    tree = (
        ("py", ("a.py",)),
        ("rs", ("b.rs", "c.rs")),
        ("mixed", ("d.py", "e.rs")),
    )

    @coco.fn
    async def app_main() -> None:
        await coco.mount(coco.component_subpath("root"), root_comp, tree)

    app = _app("test_logic_dep_component_tree", app_main)
    _release("py", 1)
    _release("rs", 1)
    everything = {
        "dir:root": 1,
        "dir:py": 1,
        "dir:rs": 1,
        "dir:mixed": 1,
        "file:a.py": 1,
        "file:b.rs": 1,
        "file:c.rs": 1,
        "file:d.py": 1,
        "file:e.rs": 1,
    }
    app.update_blocking()
    assert _m().collect() == everything

    app.update_blocking()
    assert _m().collect() == {}

    _release("rs", 2)
    app.update_blocking()
    assert _m().collect() == {
        "dir:root": 1,
        "dir:rs": 1,
        "dir:mixed": 1,
        "file:b.rs": 1,
        "file:c.rs": 1,
        "file:e.rs": 1,
    }
    assert {k for k in GlobalDictTarget.store.data} == {
        "a.py",
        "b.rs",
        "c.rs",
        "d.py",
        "e.rs",
    }

    app.update_blocking()
    assert _m().collect() == {}

    _release("py", 2)
    app.update_blocking()
    assert _m().collect() == {
        "dir:root": 1,
        "dir:py": 1,
        "dir:mixed": 1,
        "file:a.py": 1,
        "file:d.py": 1,
    }


def test_use_logic_dep_outside_a_component_context_raises() -> None:
    _release("rust", 1)
    with pytest.raises(RuntimeError):
        coco.use_logic_dep(_dep("rust"))


def test_use_logic_dep_rejects_a_bare_value() -> None:
    @coco.fn
    def app_main() -> None:
        coco.use_logic_dep(("dep", "rust", 1))  # type: ignore[arg-type]

    app = _app("test_logic_dep_bare_value", app_main)
    with pytest.raises(TypeError, match="coco.LogicDep constructed at module import"):
        app.update_blocking()


def test_a_dependency_constructed_inside_a_component_raises() -> None:
    @coco.fn
    def app_main() -> None:
        coco.LogicDep(("dep", "late", 1))

    app = _app("test_logic_dep_constructed_late", app_main)
    with pytest.raises(RuntimeError, match="outside any component context"):
        app.update_blocking()


def test_equal_values_are_one_dependency_and_copies_are_the_object() -> None:
    a = coco.LogicDep(("dep", "rust", 1))
    b = coco.LogicDep(("dep", "rust", 1))
    c = coco.LogicDep(("dep", "rust", 2))
    assert a == b and hash(a) == hash(b)
    assert a != c
    assert copy.copy(a) is a and copy.deepcopy(a) is a
    assert repr(a) == "coco.LogicDep(('dep', 'rust', 1))"
