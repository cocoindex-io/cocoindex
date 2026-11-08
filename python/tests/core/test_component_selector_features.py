"""Sliced execution crossed with the features the programming guide documents.

Each test names the theory claim (T1-T16, see THEORY.md) it checks. A test marked
``GAP`` pins down a behavior the docs or the theory get wrong or leave out.
"""

from __future__ import annotations

import pathlib
from typing import Any

import pytest

import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import FileLike, PatternFilePathMatcher

from tests import common
from tests.common.target_states import DictsTarget, GlobalDictTarget

coco_env = common.create_test_env(__file__)

_source: dict[Any, Any] = {}
_runs: list[str] = []


@pytest.fixture(autouse=True)
def _reset() -> None:
    GlobalDictTarget.store.clear()
    DictsTarget.store.clear()
    _source.clear()
    _runs.clear()


def _app(name: str, main: Any, *args: Any, **config: Any) -> coco.App[Any, Any]:
    return coco.App(
        coco.AppConfig(
            name=f"test_selector_features_{name}", environment=coco_env, **config
        ),
        main,
        *args,
    )


def _stored() -> dict[str, Any]:
    return {k: v.data for k, v in GlobalDictTarget.store.data.items()}


def _declare(key: Any, value: Any) -> None:
    coco.declare_target_state(GlobalDictTarget.target_state(str(key), value))


# ---------------------------------------------------------------------------
# The docs example, end to end on a real source and target (T1, T3, T5, T7, T9)
# ---------------------------------------------------------------------------


@coco.fn(memo=True)
async def process_file(file: FileLike[Any], target: localfs.DirTarget) -> None:
    _runs.append("/".join(file.file_path.path.parts[-2:]))
    outname = "__".join(file.file_path.path.parts[-2:]) + ".out"
    target.declare_file(filename=outname, content=(await file.read_text()).upper())


@coco.fn
async def _localfs_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    target = await localfs.mount_dir_target(outdir)
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
    )
    await coco.mount_each(process_file, files.items(), target)


def _outputs(outdir: pathlib.Path) -> dict[str, str]:
    return {p.name: p.read_text() for p in sorted(outdir.iterdir())}


def test_docs_example_on_localfs_with_nested_keys(tmp_path: pathlib.Path) -> None:
    src, out = tmp_path / "src", tmp_path / "out"
    (src / "sub").mkdir(parents=True)
    (src / "a.md").write_text("a1")
    (src / "sub" / "b.md").write_text("b1")
    (src / "c.md").write_text("c1")
    app = _app("localfs_docs", _localfs_main, src, out)
    app.update_blocking()
    assert _outputs(out) == {
        "src__a.md.out": "A1",
        "sub__b.md.out": "B1",
        "src__c.md.out": "C1",
    }

    (src / "a.md").write_text("a2")
    (src / "sub" / "b.md").write_text("b2")
    (src / "c.md").unlink()
    (src / "d.md").write_text("d1")
    _runs.clear()

    # The docs snippet, verbatim: a walk_dir key is one str key, "sub/b.md".
    page_of_files = ["sub/b.md", "d.md"]
    paths = [
        coco.ROOT_PATH / coco.Symbol("process_file") / name for name in page_of_files
    ]
    actions = app.update_blocking(component_selector=paths, preview=True)
    assert _outputs(out) == {
        "src__a.md.out": "A1",
        "sub__b.md.out": "B1",
        "src__c.md.out": "C1",
    }
    assert actions  # the selected changes are planned
    _runs.clear()

    app.update_blocking(component_selector=paths)
    assert sorted(_runs) == ["src/d.md", "sub/b.md"]
    # a.md changed and c.md is gone, both unselected: their outputs stay.
    assert _outputs(out) == {
        "src__a.md.out": "A1",
        "sub__b.md.out": "B2",
        "src__c.md.out": "C1",
        "src__d.md.out": "D1",
    }

    _runs.clear()
    app.update_blocking()
    assert _runs == ["src/a.md"]
    assert _outputs(out) == {
        "src__a.md.out": "A2",
        "sub__b.md.out": "B2",
        "src__d.md.out": "D1",
    }


def test_mount_target_runs_under_the_empty_selector_on_a_first_run(
    tmp_path: pathlib.Path,
) -> None:
    """T4/T14: mount_target is a use_mount, so `[]` creates the target and no file."""
    src, out = tmp_path / "src", tmp_path / "out"
    src.mkdir()
    (src / "a.md").write_text("a1")
    app = _app("mount_target_empty", _localfs_main, src, out)
    app.update_blocking(component_selector=[])
    assert out.is_dir() and _outputs(out) == {}
    assert _runs == []
    app.update_blocking()
    assert _outputs(out) == {"src__a.md.out": "A1"}


# ---------------------------------------------------------------------------
# use_mount children that the root no longer calls (T3, T4, T5)
# ---------------------------------------------------------------------------

_calls_setup = {"on": True}


@coco.fn
async def _setup() -> int:
    _runs.append("setup")
    _declare("setup", 1)
    return 1


@coco.fn
async def _item(key: str, value: int) -> None:
    _runs.append(key)
    _declare(key, value)


@coco.fn
async def _setup_main() -> None:
    if _calls_setup["on"]:
        await coco.use_mount(coco.component_subpath("setup"), _setup)
    for key, value in sorted(_source.items()):
        await coco.mount(coco.component_subpath("items", key), _item, key, value)


@pytest.mark.parametrize("selected", [False, True])
def test_use_mount_child_no_longer_called(selected: bool) -> None:
    _calls_setup["on"] = True
    _source.update(a=1)
    app = _app(f"use_mount_gone_{selected}", _setup_main)
    app.update_blocking()
    assert _stored() == {"setup": 1, "a": 1}

    _calls_setup["on"] = False
    selector = [coco.ROOT_PATH / "items" / "a"]
    if selected:
        selector.append(coco.ROOT_PATH / "setup")
    app.update_blocking(component_selector=selector)
    # Unselected and gone: kept, as any unselected component. Selected and
    # gone: deleted.
    assert _stored() == ({"a": 1} if selected else {"setup": 1, "a": 1})
    app.update_blocking()
    assert _stored() == {"a": 1}


# ---------------------------------------------------------------------------
# Logic change detection: version, LogicDep (T3, T5, T7)
# ---------------------------------------------------------------------------


_current_fn: dict[str, Any] = {}


def _versioned(version: int) -> Any:
    @coco.fn(memo=True, version=version)
    async def versioned_item(key: str, value: int) -> None:
        _runs.append(key)
        _declare(key, value * 100 + version)

    return versioned_item


@coco.fn
async def _versioned_main() -> None:
    for key, value in sorted(_source.items()):
        await coco.mount(
            coco.component_subpath("items", key), _current_fn["fn"], key, value
        )


def test_version_bump_reaches_unselected_memos_on_the_next_plain_update() -> None:
    _source.update(a=1, b=1)
    _current_fn["fn"] = _versioned(1)
    app = _app("version", _versioned_main)
    app.update_blocking()
    assert _stored() == {"a": 101, "b": 101}

    _runs.clear()
    _current_fn["fn"] = _versioned(2)
    app.update_blocking(component_selector=[coco.ROOT_PATH / "items" / "a"])
    assert _runs == ["a"]
    assert _stored() == {"a": 102, "b": 101}

    _runs.clear()
    app.update_blocking()
    assert _runs == ["b"]
    assert _stored() == {"a": 102, "b": 102}


# ---------------------------------------------------------------------------
# DataDep (experimental): a read the arguments do not cover (T3, T5, T7)
# ---------------------------------------------------------------------------

_external: dict[str, int] = {}


def _validate_external(key: str, prev: int) -> coco.MemoStateOutcome:
    current = _external.get(key)
    return coco.MemoStateOutcome(state=current, memo_valid=current == prev)


_EXTERNAL = coco.DataDep("selector_features_external", validate=_validate_external)


@coco.fn(memo=True)
async def _reads_external(key: str) -> None:
    _runs.append(key)
    value = _external[key]
    coco.use_data_dep(_EXTERNAL, key=key, state=value)
    _declare(key, value)


@coco.fn
async def _data_dep_main() -> None:
    for key in sorted(_external):
        await coco.mount(coco.component_subpath("items", key), _reads_external, key)


def test_data_dep_change_of_an_unselected_component_waits_for_a_covering_update() -> (
    None
):
    _external.clear()
    _external.update(a=1, b=1)
    app = _app("data_dep", _data_dep_main)
    app.update_blocking()
    _external.update(a=2, b=2)
    _runs.clear()
    app.update_blocking(component_selector=[coco.ROOT_PATH / "items" / "a"])
    assert _runs == ["a"]
    assert _stored() == {"a": 2, "b": 1}
    _runs.clear()
    app.update_blocking()
    assert _runs == ["b"]
    assert _stored() == {"a": 2, "b": 2}


# ---------------------------------------------------------------------------
# Concurrency control: a limit of one, a deep selected path (T4)
# ---------------------------------------------------------------------------


@coco.fn
async def _level(depth: int, key: str) -> None:
    _runs.append(f"{depth}/{key}")
    if depth == 0:
        _declare(key, 1)
        return
    for child in ("x", "y"):
        await coco.mount(
            coco.component_subpath(child), _level, depth - 1, f"{key}{child}"
        )


def test_deep_selection_under_a_limit_of_one_inflight_component() -> None:
    app = _app("limit_one", _level, 3, "", max_inflight_components=1)
    app.update_blocking()
    assert len(_stored()) == 8
    _runs.clear()
    app.update_blocking(component_selector=[coco.ROOT_PATH / "x" / "y" / "x"])
    assert _runs == ["3/", "2/x", "1/xy", "0/xyx"]


# ---------------------------------------------------------------------------
# use_state in an ancestor that left children out (T10 by analogy)
# ---------------------------------------------------------------------------


def test_use_state_written_by_an_ancestor_under_a_selector_is_persisted() -> None:
    """Unlike write_committed_state, a use_state value an ancestor writes in a run
    that left children out is persisted. The docs say neither."""
    observed: list[int] = []

    @coco.fn
    async def main() -> None:
        state = coco.use_state("scan_marker", 0)
        observed.append(state.value)
        for key, value in sorted(_source.items()):
            await coco.mount(coco.component_subpath("items", key), _item, key, value)
        state.value = len(_source)

    _source.update(a=1, b=1)
    app = _app("ancestor_use_state", main)
    app.update_blocking(component_selector=[coco.ROOT_PATH / "items" / "a"])
    app.update_blocking(component_selector=[])
    assert observed == [0, 2]


# ---------------------------------------------------------------------------
# Containers (T8)
# ---------------------------------------------------------------------------


@coco.fn
async def _row(provider: Any, key: str, value: int) -> None:
    coco.declare_target_state(provider.target_state(key, value))


@coco.fn
async def _sub_pipeline(name: str) -> None:
    provider = await coco.use_mount(
        coco.component_subpath("setup"), DictsTarget.declare_dict_target, name
    )
    for key, value in sorted(_source.items()):
        await coco.mount(
            coco.component_subpath("rows", key), _row, provider, key, value
        )


@coco.fn
async def _two_pipelines_main() -> None:
    await coco.mount(coco.component_subpath("p1"), _sub_pipeline, "D1")
    await coco.mount(coco.component_subpath("p2"), _sub_pipeline, "D2")


def _dicts() -> dict[str, dict[str, Any]]:
    return {
        name: {k: v.data for k, v in rows.items()}
        for name, rows in DictsTarget.store.data.items()
    }


@pytest.mark.parametrize("preview", [False, True])
@pytest.mark.parametrize(
    "selected",
    [
        # Every row of D1 lies in the selected subtree.
        [coco.ROOT_PATH / "p1"],
        # D1's owner runs only as an ancestor of a selected row.
        [coco.ROOT_PATH / "p1" / "rows" / "a"],
    ],
    ids=["whole_owner_selected", "one_row_selected"],
)
def test_container_refusal_under_a_mounted_owner_is_raised(
    selected: list[coco.StablePath], preview: bool
) -> None:
    """The docs: a selective update that would rebuild a container "fails with an
    error", and a preview with the same selector fails alike."""
    _source.update(a=1, b=1)
    name = f"container_mounted_owner_{len(selected[0].parts())}_{preview}"
    app = _app(name, _two_pipelines_main)
    app.update_blocking()
    assert _dicts() == {"D1": {"a": 1, "b": 1}, "D2": {"a": 1, "b": 1}}

    DictsTarget.store.child_invalidation = "destructive"
    _source.update(a=2, b=2)
    try:
        with pytest.raises(Exception, match="A full update is needed"):
            app.update_blocking(component_selector=selected, preview=preview)
    finally:
        DictsTarget.store.child_invalidation = None
        # Whatever happens, nothing under p1 was applied.
        assert _dicts() == {"D1": {"a": 1, "b": 1}, "D2": {"a": 1, "b": 1}}


_declares_container = {"on": True}


@coco.fn
async def _optional_setup() -> Any:
    if _declares_container["on"]:
        return DictsTarget.declare_dict_target("D")
    return None


@coco.fn
async def _optional_container_main() -> None:
    if not _declares_container["on"]:
        return
    provider = await coco.use_mount(coco.component_subpath("setup"), _optional_setup)
    for key, value in sorted(_source.items()):
        await coco.mount(
            coco.component_subpath("rows", key), _row, provider, key, value
        )


def test_gone_selected_container_owner_with_gone_unselected_rows_converges() -> None:
    """T8 exception + T7: the container goes with its selected, gone owner; its
    unselected (also gone) row owners keep their records, and a plain update
    then converges without error."""
    _declares_container["on"] = True
    _source.update(a=1, b=1)
    app = _app("container_owner_gone", _optional_container_main)
    app.update_blocking()
    assert _dicts() == {"D": {"a": 1, "b": 1}}

    _declares_container["on"] = False
    app.update_blocking(component_selector=[coco.ROOT_PATH / "setup"])
    assert "D" not in DictsTarget.store.data
    owners_after_selective = common.list_target_state_owners_sync(app)

    app.update_blocking()
    assert DictsTarget.store.data == {}
    assert common.list_target_state_owners_sync(app) == {}
    # The unselected row owners kept their records through the selective run.
    assert owners_after_selective


# ---------------------------------------------------------------------------
# Taking over a target state in plain updates (T11, T7)
# ---------------------------------------------------------------------------


@coco.fn(memo=True)
async def _memo_declares(name: str, key: str) -> None:
    _runs.append(name)
    _declare(key, "same value")


@coco.fn
async def _two_declarers_main() -> None:
    for name in sorted(_source):
        await coco.mount(
            coco.component_subpath("items", name), _memo_declares, name, _source[name]
        )


def test_two_memoized_owners_of_one_target_state_settle_in_plain_updates() -> None:
    """T11 drops the old owner's memo on a takeover, in plain updates too. Two
    unchanged components that declare one target state must still settle."""
    _source.update(a="shared", b="shared")
    app = _app("two_owners", _two_declarers_main)
    app.update_blocking()
    runs_per_update = []
    for _ in range(3):
        _runs.clear()
        app.update_blocking()
        runs_per_update.append(sorted(_runs))
    assert runs_per_update == [[], [], []]


# ---------------------------------------------------------------------------
# T6: "each ancestor stores no memo", when only a grandchild left something out
# ---------------------------------------------------------------------------


@coco.fn
async def _leaf(key: str, value: int) -> None:
    _runs.append(key)
    _declare(key, value)


@coco.fn
async def _inner(items: tuple[tuple[str, int], ...]) -> None:
    _runs.append("inner")
    for key, value in items:
        await coco.mount(coco.component_subpath("rows", key), _leaf, key, value)


@coco.fn(memo=True)
async def _memo_middle(items: tuple[tuple[str, int], ...]) -> None:
    _runs.append("middle")
    # Mounts one child only, and runs it: the middle leaves nothing out itself.
    await coco.mount(coco.component_subpath("inner"), _inner, items)


@coco.fn
async def _grandchild_main() -> None:
    await coco.mount(
        coco.component_subpath("middle"), _memo_middle, tuple(sorted(_source.items()))
    )


@pytest.mark.parametrize("data_changes", [False, True])
def test_memoized_grandparent_of_a_left_out_child_stores_no_memo(
    data_changes: bool,
) -> None:
    _source.update(a=1, b=1)
    app = _app(f"grandchild_left_out_{data_changes}", _grandchild_main)
    app.update_blocking()
    if data_changes:
        _source.update(a=2, b=2)
    _runs.clear()
    app.update_blocking(
        component_selector=[coco.ROOT_PATH / "middle" / "inner" / "rows" / "a"],
        full_reprocess=True,
    )
    assert _runs == ["middle", "inner", "a"]
    expected_after_selective = {"a": 2, "b": 1} if data_changes else {"a": 1, "b": 1}
    assert _stored() == expected_after_selective

    # The middle's memo must not stand in for "inner", which left "b" out.
    _runs.clear()
    app.update_blocking()
    assert "b" in _runs
