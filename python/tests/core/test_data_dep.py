"""End-to-end tests for data dependencies (`coco.DataDep` / `coco.use_data_dep`).

A memoized component or function records, while it runs, the files it chose to
read; the next probe validates those records from the stored key and state
alone. Spec: cocoindex-specs `data_deps/spec.md`.
"""

import asyncio
import gc
import hashlib
import os
import pathlib
import tempfile
from typing import Any

import pytest

import cocoindex as coco

from tests import common
from tests.common.target_states import GlobalDictTarget, Metrics

coco_env = common.create_test_env(__file__)

_metrics = Metrics()


# ---------------------------------------------------------------------------
# The kind: a file read, keyed by path, with a two-level (mtime, digest) state.
# ---------------------------------------------------------------------------


def _digest(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _validate_file(key: str, prev: tuple[int, bytes]) -> coco.MemoStateOutcome:
    _metrics.increment("validate.file")
    try:
        st = os.stat(key)
    except FileNotFoundError:
        return coco.MemoStateOutcome(state=prev)
    if st.st_mtime_ns == prev[0]:
        return coco.MemoStateOutcome(state=prev, memo_valid=True)
    _metrics.increment("validate.file.digest")
    digest = _digest(pathlib.Path(key).read_bytes())
    return coco.MemoStateOutcome(
        state=(st.st_mtime_ns, digest), memo_valid=digest == prev[1]
    )


FILE = coco.DataDep("test-file", validate=_validate_file)


def _read(path: pathlib.Path) -> bytes:
    """Read a file and record the read: stat *before* reading, so the recorded
    mtime is never newer than the bytes it is paired with."""
    st = os.stat(path)
    data = path.read_bytes()
    coco.use_data_dep(
        FILE, key=str(path.resolve()), state=(st.st_mtime_ns, _digest(data))
    )
    return data


def _write(path: pathlib.Path, content: str) -> None:
    """Write and bump the mtime past any earlier stat."""
    path.write_bytes(content.encode())
    st = os.stat(path)
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))


def _touch(path: pathlib.Path) -> None:
    st = os.stat(path)
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))


def _data() -> dict[str, str]:
    return {k: v.data for k, v in GlobalDictTarget.store.data.items()}


# The files a session reads are decided at run time, not by its arguments.
_reads: dict[str, list[pathlib.Path]] = {}


@coco.fn(memo=True)
def _session(name: str) -> None:
    _metrics.increment(f"run.{name}")
    content = b"".join(_read(p) for p in _reads.get(name, []))
    coco.declare_target_state(GlobalDictTarget.target_state(name, content.decode()))


@coco.fn
async def _sessions_main(names: list[str]) -> None:
    for name in names:
        await coco.use_mount(coco.component_subpath(name), _session, name)


def _reset() -> None:
    GlobalDictTarget.store.clear()
    _metrics.clear()
    _reads.clear()


def _app(name: str, main: Any, *args: Any) -> coco.App[Any, Any]:
    return coco.App(coco.AppConfig(name=name, environment=coco_env), main, *args)


# ---------------------------------------------------------------------------
# Reuse and recording
# ---------------------------------------------------------------------------


def test_open_component_is_reused_while_its_reads_are_unchanged() -> None:
    _reset()
    with tempfile.TemporaryDirectory() as d:
        a = pathlib.Path(d) / "a.txt"
        _write(a, "A1")
        _reads["s"] = [a]
        app = _app("test_dd_reuse", _sessions_main, ["s"])

        app.update_blocking()
        assert _metrics.collect() == {"run.s": 1}
        assert _data() == {"s": "A1"}

        # Nothing changed: the body does not run, the target is carried over.
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 1}
        assert _data() == {"s": "A1"}

        # The read changed: the body runs and the new read set is stored.
        _write(a, "A2")
        app.update_blocking()
        assert _metrics.collect() == {
            "validate.file": 1,
            "validate.file.digest": 1,
            "run.s": 1,
        }
        assert _data() == {"s": "A2"}


def test_changed_but_valid_state_is_refreshed_without_a_run() -> None:
    _reset()
    with tempfile.TemporaryDirectory() as d:
        a = pathlib.Path(d) / "a.txt"
        _write(a, "A1")
        _reads["s"] = [a]
        app = _app("test_dd_refresh", _sessions_main, ["s"])
        app.update_blocking()
        _metrics.clear()

        # mtime moved, content did not: reused, and the stored mtime refreshed.
        _touch(a)
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 1, "validate.file.digest": 1}

        # The refreshed mtime makes the next probe take the fast path.
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 1}


def test_the_read_set_is_replaced_on_a_miss() -> None:
    _reset()
    with tempfile.TemporaryDirectory() as d:
        a = pathlib.Path(d) / "a.txt"
        b = pathlib.Path(d) / "b.txt"
        _write(a, "A1")
        _write(b, "B1")
        _reads["s"] = [a, b]
        app = _app("test_dd_replace", _sessions_main, ["s"])
        app.update_blocking()
        assert _data() == {"s": "A1B1"}
        _metrics.clear()

        # A miss (b changed) during which the body reads only a.
        _reads["s"] = [a]
        _write(b, "B2")
        app.update_blocking()
        assert _metrics.collect()["run.s"] == 1
        assert _data() == {"s": "A1"}

        # b no longer matters.
        _write(b, "B3")
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 1}

        # a newly read file starts to matter on the next miss.
        _reads["s"] = [a, b]
        _write(a, "A2")
        app.update_blocking()
        assert _data() == {"s": "A2B3"}
        _metrics.clear()
        _write(b, "B4")
        app.update_blocking()
        assert _metrics.collect()["run.s"] == 1


_rewrite_to: dict[str, str] = {}


@coco.fn(memo=True)
def _read_write_read(name: str, path: pathlib.Path) -> None:
    """Reads a file, rewrites it to a fixed content, and reads it back: when
    the content differs, one key is observed in two states."""
    _metrics.increment(f"run.{name}")
    _read(path)
    if path.read_bytes().decode() != _rewrite_to[name]:
        _write(path, _rewrite_to[name])
    _read(path)
    coco.declare_target_state(
        GlobalDictTarget.target_state(name, path.read_bytes().decode())
    )


@coco.fn
async def _read_write_read_main(name: str, path: pathlib.Path) -> None:
    await coco.use_mount(coco.component_subpath(name), _read_write_read, name, path)


def test_conflicting_observations_store_no_entry_and_drop_the_old_one() -> None:
    _reset()
    with tempfile.TemporaryDirectory() as d:
        a = pathlib.Path(d) / "a.txt"
        _rewrite_to["c"] = "X"
        app = _app("test_dd_conflict", _read_write_read_main, "c", a)

        # Run 0: the file already holds X, both reads agree: an entry is stored.
        _write(a, "X")
        app.update_blocking()
        assert _metrics.collect() == {"run.c": 1}
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 1}

        # Run 1: an external change makes the body read "orig" then "X": the
        # run stores no entry and removes run 0's, whose state (X) would
        # otherwise validate again next time.
        _write(a, "orig")
        app.update_blocking()
        assert _metrics.collect()["run.c"] == 1
        assert _data() == {"c": "X"}

        # Run 2: the file is X and nothing changed since run 1, yet the body
        # runs: there is no reusable entry. Both reads agree now, so run 2
        # stores one and run 3 is reused.
        app.update_blocking()
        assert _metrics.collect() == {"run.c": 1}
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 1}


# ---------------------------------------------------------------------------
# Nested function calls
# ---------------------------------------------------------------------------

_nested_path: dict[str, pathlib.Path] = {}


@coco.fn(memo=True)
def _inner_reader(name: str) -> str:
    _metrics.increment("run.inner")
    return _read(_nested_path[name]).decode()


@coco.fn(memo=True)
def _outer_full(name: str) -> str:
    _metrics.increment("run.outer")
    return "outer:" + _inner_reader(name)


@coco.fn(memo=True, logic_tracking="self")
def _outer_self(name: str) -> str:
    _metrics.increment("run.outer")
    return "outer:" + _inner_reader(name)


@coco.fn(memo=True, logic_tracking=None)
def _outer_none(name: str) -> str:
    _metrics.increment("run.outer")
    return "outer:" + _inner_reader(name)


@coco.fn
def _nested_main(outer: Any, name: str) -> None:
    _metrics.increment("run.main")
    coco.declare_target_state(GlobalDictTarget.target_state(name, outer(name)))


@pytest.mark.parametrize(
    "outer", [_outer_full, _outer_self, _outer_none], ids=["full", "self", "none"]
)
def test_a_record_reaches_every_enclosing_memoized_function(outer: Any) -> None:
    _reset()
    with tempfile.TemporaryDirectory() as d:
        a = pathlib.Path(d) / "a.txt"
        _write(a, "A1")
        name = f"n-{outer.__name__}"
        _nested_path[name] = a
        app = _app(f"test_dd_nested_{outer.__name__}", _nested_main, outer, name)

        app.update_blocking()
        assert _metrics.collect() == {"run.main": 1, "run.outer": 1, "run.inner": 1}

        # The outer function hits on its own entry, which recorded the read.
        app.update_blocking()
        assert _metrics.collect() == {"run.main": 1, "validate.file": 1}

        # The read changed: the outer entry misses (not only the inner one),
        # whatever its logic_tracking.
        _write(a, "A2")
        app.update_blocking()
        assert _metrics.collect()["run.outer"] == 1
        assert _data() == {name: "outer:A2"}


def test_a_hit_re_offers_its_records_to_a_caller_stored_later() -> None:
    """A caller re-stored after its callee hit still records the callee's
    reads: the memoized outer entry is stored on a run where the inner hit."""
    _reset()
    with tempfile.TemporaryDirectory() as d:
        a = pathlib.Path(d) / "a.txt"
        _write(a, "A1")
        name = "reoffer"
        _nested_path[name] = a

        # Run the inner alone first, so its entry exists before any outer one.
        @coco.fn
        def inner_only(name: str) -> None:
            _inner_reader(name)

        _app("test_dd_reoffer", inner_only, name).update_blocking()
        assert _metrics.collect() == {"run.inner": 1}

        # Now the outer runs for the first time while the inner hits.
        app = _app("test_dd_reoffer", _nested_main, _outer_full, name)
        app.update_blocking()
        assert _metrics.collect() == {"run.main": 1, "run.outer": 1, "validate.file": 1}

        # The outer entry must carry the read: a change makes it miss.
        _write(a, "A2")
        app.update_blocking()
        assert _metrics.collect()["run.outer"] == 1
        assert _data() == {name: "outer:A2"}


# ---------------------------------------------------------------------------
# Snapshots and coarse memo keys
# ---------------------------------------------------------------------------

_seen_states: list[Any] = []


def _validate_recorded(key: str, prev: list[int]) -> coco.MemoStateOutcome:
    _seen_states.append(prev)
    return coco.MemoStateOutcome(state=prev, memo_valid=True)


RECORDED = coco.DataDep("test-recorded", validate=_validate_recorded)


class _CoarseState:
    """Two unequal states whose memo key is the same."""

    def __init__(self, value: int) -> None:
        self.value = value

    def __coco_memo_key__(self) -> object:
        return "coarse"


def _validate_coarse(key: str, prev: int) -> coco.MemoStateOutcome:
    return coco.MemoStateOutcome(state=prev, memo_valid=True)


COARSE = coco.DataDep("test-coarse", validate=_validate_coarse)


@coco.fn(memo=True)
def _snapshot_body(mode: str) -> None:
    _metrics.increment("run.snapshot")
    if mode == "mutate":
        state = [1]
        coco.use_data_dep(RECORDED, key="k", state=state)
        state.append(2)  # must not reach the stored record
    else:
        coco.use_data_dep(COARSE, key="k", state=1)
        coco.use_data_dep(COARSE, key="k", state=2)  # unequal: a conflict


@coco.fn
async def _snapshot_main(mode: str) -> None:
    await coco.use_mount(coco.component_subpath(mode), _snapshot_body, mode)


def test_records_are_snapshots_taken_at_the_call() -> None:
    _reset()
    _seen_states.clear()
    app = _app("test_dd_snapshot", _snapshot_main, "mutate")
    app.update_blocking()
    app.update_blocking()
    assert _metrics.collect() == {"run.snapshot": 1}
    assert _seen_states == [[1]]


def test_two_unequal_states_are_a_conflict_whatever_their_memo_key() -> None:
    _reset()
    app = _app("test_dd_coarse", _snapshot_main, "coarse")
    app.update_blocking()
    app.update_blocking()
    # Equal memo keys do not merge the observations: both runs execute.
    assert _metrics.collect() == {"run.snapshot": 2}


# ---------------------------------------------------------------------------
# The bit and the tree
# ---------------------------------------------------------------------------


@coco.fn(memo=True)
def _closed_child(name: str) -> None:
    _metrics.increment(f"run.{name}")
    coco.declare_target_state(GlobalDictTarget.target_state(name, "closed"))


@coco.fn(memo=True)
async def _parent(name: str, children: list[str]) -> None:
    _metrics.increment(f"run.{name}")
    for child in children:
        if child.startswith("open"):
            await coco.use_mount(coco.component_subpath(child), _session, child)
        else:
            await coco.use_mount(coco.component_subpath(child), _closed_child, child)


@coco.fn(memo=True)
async def _grandparent(
    name: str, parent: str, children: list[str], salt: int = 0
) -> None:
    _metrics.increment(f"run.{name}")
    await coco.use_mount(coco.component_subpath(parent), _parent, parent, children)


# An extra input of the grandparent alone, to make it miss while the parent hits.
_gp_salt = {"value": 0}


@coco.fn
async def _tree_main(gp: str, parent: str, children: list[str]) -> None:
    await coco.use_mount(
        coco.component_subpath(gp),
        _grandparent,
        gp,
        parent,
        children,
        _gp_salt["value"],
    )


def test_a_memoized_ancestor_is_reused_while_its_open_descendants_reads_validate() -> (
    None
):
    """Transitive validation by path: a flagged ancestor validates the open
    descendants its memo lists from their stored rows and is reused without
    mounting anything; one invalid read beneath it re-runs its body."""
    _reset()
    _gp_salt["value"] = 0
    with tempfile.TemporaryDirectory() as d:
        a = pathlib.Path(d) / "a.txt"
        _write(a, "A1")
        _reads["open"] = [a]
        app = _app("test_dd_tree", _tree_main, "gp", "p", ["open", "closed"])

        app.update_blocking()
        assert _metrics.collect() == {
            "run.gp": 1,
            "run.p": 1,
            "run.open": 1,
            "run.closed": 1,
        }

        # Unchanged: the grandparent's probe follows its memo's list to the
        # parent's row and the parent's list to the open child's row, validates
        # that read once and reuses. Nothing beneath is probed or run.
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 1}

        # Touched: the read moved while staying valid. The grandparent's probe
        # refreshes the open child's row in place, so the next probe takes the
        # mtime fast path again.
        _touch(a)
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 1, "validate.file.digest": 1}
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 1}

        # Edited: the read is invalid at the grandparent's probe, then at the
        # parent's, then at the open child's own; each level re-runs its body.
        # This run records nothing, so the ancestors store unflagged memos and
        # the next run reuses the grandparent without validating anything.
        _reads["open"] = []
        _write(a, "A2")
        app.update_blocking()
        assert _metrics.collect() == {
            "run.gp": 1,
            "run.p": 1,
            "run.open": 1,
            "validate.file": 3,
            "validate.file.digest": 3,
        }
        app.update_blocking()
        assert _metrics.collect() == {}


def test_a_reused_ancestor_of_open_descendants_still_flags_its_parent() -> None:
    """A flagged memo reused through the walk still reports the bit upward, so
    a parent whose body re-runs lists it again; otherwise that parent would
    store an unflagged memo and skip the read on its next probe."""
    _reset()
    _gp_salt["value"] = 0
    with tempfile.TemporaryDirectory() as d:
        a = pathlib.Path(d) / "a.txt"
        _write(a, "A1")
        _reads["open"] = [a]
        app = _app("test_dd_tree_bit", _tree_main, "gp", "p", ["open"])
        app.update_blocking()
        _metrics.clear()

        # The grandparent's own key changes; the parent's memo is reused
        # through the walk and must report the bit.
        _gp_salt["value"] = 1
        app.update_blocking()
        assert _metrics.collect() == {"run.gp": 1, "validate.file": 1}
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 1}


def test_several_open_children_are_all_validated_through_the_walk() -> None:
    _reset()
    _gp_salt["value"] = 0
    with tempfile.TemporaryDirectory() as d:
        a = pathlib.Path(d) / "a.txt"
        b = pathlib.Path(d) / "b.txt"
        _write(a, "A1")
        _write(b, "B1")
        _reads["open"] = [a]
        _reads["open2"] = [b]
        app = _app(
            "test_dd_tree_two", _tree_main, "gp", "p", ["open", "open2", "closed"]
        )
        app.update_blocking()
        _metrics.clear()

        # Both open children's reads are validated at the grandparent's probe.
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 2}

        # One of them changed: every level above it re-runs its body, that
        # child re-runs, its open sibling and the closed one hit.
        _write(b, "B2")
        app.update_blocking()
        runs = {k: v for k, v in _metrics.collect().items() if k.startswith("run.")}
        assert runs == {"run.gp": 1, "run.p": 1, "run.open2": 1}
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 2}


def test_a_flagged_ancestor_under_full_reprocess_and_in_a_preview() -> None:
    _reset()
    _gp_salt["value"] = 0
    with tempfile.TemporaryDirectory() as d:
        a = pathlib.Path(d) / "a.txt"
        _write(a, "A1")
        _reads["open"] = [a]
        app = _app("test_dd_tree_modes", _tree_main, "gp", "p", ["open"])
        app.update_blocking()
        _metrics.clear()

        # full_reprocess bypasses every memo from earlier operations, the
        # flagged ones included; the memos it stores are reused next run.
        app.update_blocking(full_reprocess=True)
        assert _metrics.collect() == {"run.gp": 1, "run.p": 1, "run.open": 1}
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 1}

        # A preview reuses through the walk too, and its refresh of the open
        # child's row stays in the preview: the next real run refreshes again.
        _touch(a)
        app.update_blocking(preview=True)
        assert _metrics.collect() == {"validate.file": 1, "validate.file.digest": 1}
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 1, "validate.file.digest": 1}
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 1}


@coco.fn(memo=False)
def _open_non_memoized(name: str) -> None:
    _metrics.increment(f"run.{name}")
    for p in _reads[name]:
        _read(p)


@coco.fn(memo=True)
async def _parent_of_non_memoized(name: str, child: str) -> None:
    _metrics.increment(f"run.{name}")
    await coco.use_mount(coco.component_subpath(child), _open_non_memoized, child)


@coco.fn
async def _non_memoized_main(parent: str, child: str) -> None:
    await coco.use_mount(
        coco.component_subpath(parent), _parent_of_non_memoized, parent, child
    )


def test_a_non_memoized_open_child_still_flags_its_memoized_parent() -> None:
    """A non-memoized child drops its records, so there is no row to validate
    them from: the parent's probe stops there and its body re-runs every run."""
    _reset()
    with tempfile.TemporaryDirectory() as d:
        a = pathlib.Path(d) / "a.txt"
        _write(a, "A1")
        _reads["nm"] = [a]
        app = _app("test_dd_non_memoized", _non_memoized_main, "p", "nm")
        app.update_blocking()
        app.update_blocking()
        assert _metrics.collect() == {"run.p": 2, "run.nm": 2}


@coco.fn(memo=True)
async def _open_with_child(name: str, mount_child: bool) -> None:
    _metrics.increment(f"run.{name}")
    for p in _reads[name]:
        _read(p)
    if mount_child:
        await coco.use_mount(coco.component_subpath("kid"), _closed_child, "kid")


_mount_child = {"value": True}


@coco.fn
async def _open_with_child_main(name: str) -> None:
    await coco.use_mount(
        coco.component_subpath(name), _open_with_child, name, _mount_child["value"]
    )


def test_children_are_carried_over_on_reuse_and_cleaned_up_on_a_rerun() -> None:
    _reset()
    _mount_child["value"] = True
    with tempfile.TemporaryDirectory() as d:
        a = pathlib.Path(d) / "a.txt"
        _write(a, "A1")
        _reads["ow"] = [a]
        app = _app("test_dd_children", _open_with_child_main, "ow")
        app.update_blocking()
        assert _data() == {"kid": "closed"}
        _metrics.clear()

        # Reused: the child is carried over without being probed.
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 1}
        assert _data() == {"kid": "closed"}

        # The read changed and the body no longer mounts the child: cleaned up.
        # (The argument changes too, so this is a plain miss.)
        _mount_child["value"] = False
        _write(a, "A2")
        app.update_blocking()
        assert _data() == {}


# ---------------------------------------------------------------------------
# Failures
# ---------------------------------------------------------------------------

_fail = {"value": False}


@coco.fn(memo=True)
def _record_then_maybe_fail(name: str) -> None:
    _metrics.increment(f"run.{name}")
    for p in _reads[name]:
        _read(p)
    if _fail["value"]:
        raise RuntimeError("boom")


@coco.fn
async def _fail_main(name: str) -> None:
    await coco.use_mount(coco.component_subpath(name), _record_then_maybe_fail, name)


def test_a_failed_run_stores_no_entry() -> None:
    _reset()
    with tempfile.TemporaryDirectory() as d:
        a = pathlib.Path(d) / "a.txt"
        _write(a, "A1")
        _reads["f"] = [a]
        app = _app("test_dd_fail", _fail_main, "f")
        _fail["value"] = True
        with pytest.raises(Exception, match="boom"):
            app.update_blocking()
        _fail["value"] = False
        app.update_blocking()
        assert _metrics.collect() == {"run.f": 2}
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 1}


@coco.fn(memo=True)
def _inner_fails(name: str) -> str:
    _metrics.increment("run.inner")
    _read(_nested_path[name])
    raise RuntimeError("inner boom")


@coco.fn(memo=True)
def _caller_with_fallback(name: str) -> str:
    _metrics.increment("run.caller")
    try:
        return _inner_fails(name)
    except RuntimeError:
        return "fallback"


def test_a_caught_direct_function_failure_keeps_its_records_in_the_fallback() -> None:
    _reset()
    with tempfile.TemporaryDirectory() as d:
        a = pathlib.Path(d) / "a.txt"
        _write(a, "A1")
        _nested_path["cf"] = a
        app = _app("test_dd_caught", _nested_main, _caller_with_fallback, "cf")
        app.update_blocking()
        assert _data() == {"cf": "fallback"}
        assert _metrics.collect() == {"run.main": 1, "run.caller": 1, "run.inner": 1}

        # The fallback entry is reused while the read is unchanged ...
        app.update_blocking()
        assert _metrics.collect() == {"run.main": 1, "validate.file": 1}

        # ... and misses when it changes.
        _write(a, "A2")
        app.update_blocking()
        assert _metrics.collect()["run.caller"] == 1


# ---------------------------------------------------------------------------
# Registry, decoding and errors
# ---------------------------------------------------------------------------


def test_use_data_dep_outside_a_component_context_raises() -> None:
    with pytest.raises(RuntimeError):
        coco.use_data_dep(FILE, key="k", state=(0, b""))


def test_use_data_dep_rejects_a_bare_value() -> None:
    _reset()

    @coco.fn
    def main() -> None:
        with pytest.raises(TypeError):
            coco.use_data_dep("file", key="k", state=1)  # type: ignore[arg-type]

    _app("test_dd_bare", main).update_blocking()


def test_a_kind_constructed_inside_a_component_raises() -> None:
    _reset()

    @coco.fn
    def main() -> None:
        with pytest.raises(RuntimeError):
            coco.DataDep("inside", validate=_validate_file)

    _app("test_dd_inside", main).update_blocking()


def test_recording_after_the_frame_closed_raises() -> None:
    _reset()
    captured: list[Any] = []

    @coco.fn
    def main() -> None:
        captured.append(coco.get_component_context())

    _app("test_dd_closed", main).update_blocking()
    with captured[0].attach():
        with pytest.raises(RuntimeError, match="returned"):
            coco.use_data_dep(FILE, key="k", state=(0, b""))


_async_seen: list[Any] = []


async def _validate_async(key: str, prev: tuple[int, str]) -> coco.MemoStateOutcome:
    await asyncio.sleep(0)
    _async_seen.append((key, prev))
    return coco.MemoStateOutcome(state=prev, memo_valid=prev[1] == "ok")


ASYNC = coco.DataDep("test-async", validate=_validate_async)


@coco.fn(memo=True)
async def _async_body(name: str, verdict: str) -> None:
    _metrics.increment(f"run.{name}")
    coco.use_data_dep(ASYNC, key=name, state=(7, verdict))


@coco.fn
async def _async_main(name: str, verdict: str) -> None:
    await coco.mount(coco.component_subpath(name), _async_body, name, verdict)


def test_async_validators_run_and_types_round_trip() -> None:
    _reset()
    _async_seen.clear()
    app = _app("test_dd_async", _async_main, "ok-case", "ok")
    app.update_blocking()
    app.update_blocking()
    assert _metrics.collect() == {"run.ok-case": 1}
    assert _async_seen == [("ok-case", (7, "ok"))]

    _reset()
    app = _app("test_dd_async_bad", _async_main, "bad-case", "bad")
    app.update_blocking()
    app.update_blocking()
    assert _metrics.collect() == {"run.bad-case": 2}


def _validate_raising(key: str, prev: int) -> coco.MemoStateOutcome:
    raise ValueError("validator boom")


RAISING = coco.DataDep("test-raising", validate=_validate_raising)
# A kind whose stored state (a tuple) does not deserialize as the int its
# validator is annotated with: a decode failure.
NARROW = coco.DataDep("test-narrow", validate=_validate_raising)


@coco.fn(memo=True)
def _record_kind(name: str, which: str) -> None:
    _metrics.increment(f"run.{name}")
    if which == "raising":
        coco.use_data_dep(RAISING, key=name, state=1)
    else:
        coco.use_data_dep(NARROW, key=name, state=("not", "an", "int"))


@coco.fn
async def _record_kind_main(name: str, which: str) -> None:
    await coco.use_mount(coco.component_subpath(name), _record_kind, name, which)


def test_a_validator_exception_propagates() -> None:
    _reset()
    app = _app("test_dd_raising", _record_kind_main, "r", "raising")
    app.update_blocking()
    with pytest.raises(Exception, match="validator boom"):
        app.update_blocking()


def test_a_stored_state_that_no_longer_deserializes_is_a_logged_miss(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _reset()
    app = _app("test_dd_narrow", _record_kind_main, "n", "narrow")
    app.update_blocking()
    with caplog.at_level("WARNING", logger="cocoindex._internal.data_dep"):
        app.update_blocking()
    # The validator (which raises) was never called: the body re-ran instead.
    assert _metrics.collect() == {"run.n": 2}
    assert any("failed to deserialize" in r.message for r in caplog.records)


_kinds: dict[str, coco.DataDep] = {}


@coco.fn(memo=True)
def _record_dynamic_kind(name: str) -> None:
    _metrics.increment(f"run.{name}")
    coco.use_data_dep(_kinds[name], key=name, state=1)


@coco.fn
async def _dynamic_kind_main(name: str) -> None:
    await coco.use_mount(coco.component_subpath(name), _record_dynamic_kind, name)


def _validate_keep(key: str, prev: int) -> coco.MemoStateOutcome:
    return coco.MemoStateOutcome(state=prev, memo_valid=True)


def test_a_released_kind_invalidates_its_entries_and_a_duplicate_name_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _reset()
    _kinds["k"] = coco.DataDep("test-released", validate=_validate_keep)
    app = _app("test_dd_released", _dynamic_kind_main, "k")
    app.update_blocking()
    app.update_blocking()
    assert _metrics.collect() == {"run.k": 1}

    # Released: the kind leaves the registry and its records fail the
    # lookup, so the body re-runs and re-records under another kind.
    _kinds["k"] = RECORDED
    gc.collect()
    with caplog.at_level("WARNING", logger="cocoindex._internal.data_dep"):
        app.update_blocking()
    assert _metrics.collect() == {"run.k": 1}
    assert any("no longer registered" in r.message for r in caplog.records)

    # A second live kind with the same name replaces the first, with a warning.
    with pytest.warns(RuntimeWarning, match="replaces"):
        first = coco.DataDep("test-dup", validate=_validate_keep)
        second = coco.DataDep("test-dup", validate=_validate_keep)
    assert first == second


# ---------------------------------------------------------------------------
# Stop conditions of the walk through open descendants
# ---------------------------------------------------------------------------


_STOP_CHILDREN: dict[str, tuple[Any, tuple[Any, ...]]] = {
    "conflict": (_snapshot_body, ("conflict",)),
    "narrow": (_record_kind, ("n", "narrow")),
    "raising": (_record_kind, ("r", "raising")),
    "dynamic": (_record_dynamic_kind, ("k",)),
}


@coco.fn(memo=True)
async def _parent_of_stop_child(name: str, child: str) -> None:
    _metrics.increment(f"run.{name}")
    fn, args = _STOP_CHILDREN[child]
    await coco.use_mount(coco.component_subpath(child), fn, *args)


@coco.fn
async def _stop_main(name: str, child: str) -> None:
    await coco.use_mount(
        coco.component_subpath(name), _parent_of_stop_child, name, child
    )


def test_an_open_child_without_a_row_stops_the_walk() -> None:
    """A child whose run observed one key in two states stores no row (and
    still reports the bit): the parent's probe finds nothing to validate and
    its body re-runs, which is what lets the child run again."""
    _reset()
    app = _app("test_dd_stop_conflict", _stop_main, "p", "conflict")
    app.update_blocking()
    app.update_blocking()
    assert _metrics.collect() == {"run.p": 2, "run.snapshot": 2}


def test_a_descendant_record_that_fails_to_decode_stops_the_walk(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _reset()
    app = _app("test_dd_stop_narrow", _stop_main, "p", "narrow")
    app.update_blocking()
    with caplog.at_level("WARNING", logger="cocoindex._internal.data_dep"):
        app.update_blocking()
    # Neither the parent's walk nor the child's own probe called the validator
    # (which raises): both bodies re-ran.
    assert _metrics.collect() == {"run.p": 2, "run.n": 2}
    assert any("failed to deserialize" in r.message for r in caplog.records)


def test_a_validator_raising_during_the_walk_propagates() -> None:
    """A validator's own exception propagates from the ancestor's probe, as it
    does from the component's own probe; no body runs."""
    _reset()
    app = _app("test_dd_stop_raising", _stop_main, "p", "raising")
    app.update_blocking()
    _metrics.clear()
    with pytest.raises(Exception, match="validator boom"):
        app.update_blocking()
    assert _metrics.collect() == {}


def test_a_descendant_kind_no_longer_registered_stops_the_walk(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _reset()
    _kinds["k"] = coco.DataDep("test-released-under-parent", validate=_validate_keep)
    app = _app("test_dd_stop_released", _stop_main, "p", "dynamic")
    app.update_blocking()
    app.update_blocking()
    assert _metrics.collect() == {"run.p": 1, "run.k": 1}

    _kinds["k"] = RECORDED
    gc.collect()
    with caplog.at_level("WARNING", logger="cocoindex._internal.data_dep"):
        app.update_blocking()
    assert _metrics.collect() == {"run.p": 1, "run.k": 1}
    assert any("no longer registered" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Legacy entries
# ---------------------------------------------------------------------------


def test_full_reprocess_reruns_and_rerecords() -> None:
    _reset()
    with tempfile.TemporaryDirectory() as d:
        a = pathlib.Path(d) / "a.txt"
        _write(a, "A1")
        _reads["fr"] = [a]
        app = _app("test_dd_full_reprocess", _sessions_main, ["fr"])
        app.update_blocking()
        app.update_blocking(full_reprocess=True)
        assert _metrics.collect() == {"run.fr": 2}
        app.update_blocking()
        assert _metrics.collect() == {"validate.file": 1}
        assert _data() == {"fr": "A1"}


def test_a_direct_function_refreshes_a_changed_but_valid_state_without_a_run() -> None:
    """The function-probe refresh path: a memoized function's record moved
    (mtime) but stayed valid; the entry is refreshed in place and the next
    probe takes the fast path."""
    _reset()
    with tempfile.TemporaryDirectory() as d:
        a = pathlib.Path(d) / "a.txt"
        _write(a, "A1")
        name = "fn-refresh"
        _nested_path[name] = a
        app = _app("test_dd_fn_refresh", _nested_main, _outer_full, name)
        app.update_blocking()
        _metrics.clear()

        _touch(a)
        app.update_blocking()
        assert _metrics.collect() == {
            "run.main": 1,
            "validate.file": 1,
            "validate.file.digest": 1,
        }

        app.update_blocking()
        assert _metrics.collect() == {"run.main": 1, "validate.file": 1}
        assert _data() == {name: "outer:A1"}
