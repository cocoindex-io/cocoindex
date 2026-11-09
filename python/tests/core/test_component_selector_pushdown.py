"""Source pushdown for sliced execution: under ``component_selector``,
``mount_each`` reads only the keys the update may run. A source that reads by
key (``__coco_items_for_keys__``) is asked for just those keys, and a
``mount_each`` whose path the selector leaves out reads nothing.
"""

from __future__ import annotations

import pathlib
from collections.abc import AsyncIterator
from typing import Any

import pytest

import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import PatternFilePathMatcher

from tests import common
from tests.common.target_states import GlobalDictTarget

coco_env = common.create_test_env(__file__)

_source: dict[Any, str] = {}
_runs: list[str] = []
_reads: list[str] = []


@pytest.fixture(autouse=True)
def _reset() -> None:
    GlobalDictTarget.store.clear()
    _source.clear()
    _runs.clear()
    _reads.clear()


def _stored() -> dict[str, Any]:
    return {k: v.data for k, v in GlobalDictTarget.store.data.items()}


class _Items:
    """Items of `_source` that record how they are read."""

    def __aiter__(self) -> AsyncIterator[tuple[Any, str]]:
        _reads.append("list")
        return self._all()

    async def _all(self) -> AsyncIterator[tuple[Any, str]]:
        for key, value in list(_source.items()):
            yield key, value

    async def __coco_items_for_keys__(
        self, keys: list[Any]
    ) -> AsyncIterator[tuple[Any, str]]:
        _reads.append(f"keys {sorted(map(str, keys))}")
        for key in keys:
            if key in _source:
                yield key, _source[key]


class _ListedOnly:
    """Items that can only be listed."""

    def __aiter__(self) -> AsyncIterator[tuple[Any, str]]:
        _reads.append("list")
        return _Items()._all()


@coco.fn
async def _item(value: str, prefix: str) -> None:
    _runs.append(f"{prefix}{value}")
    coco.declare_target_state(GlobalDictTarget.target_state(f"{prefix}{value}", value))


@coco.fn
async def _main(items_factory: Any) -> None:
    await coco.mount_each(coco.component_subpath("items"), _item, items_factory(), "")
    await coco.mount(coco.component_subpath("other"), _item, "o", "other:")


def _app(name: str, items_factory: Any = _Items) -> coco.App[Any, Any]:
    return coco.App(
        coco.AppConfig(name=f"test_pushdown_{name}", environment=coco_env),
        _main,
        items_factory,
    )


def _path(*parts: Any) -> coco.StablePath:
    path = coco.ROOT_PATH
    for part in parts:
        path = path / part
    return path


def test_selected_keys_are_read_by_key_without_listing() -> None:
    app = _app("by_key")
    _source.update({"a": "a1", "b": "b1", "c": "c1"})
    app.update_blocking()
    assert _reads == ["list"]
    _source.update({"a": "a2", "b": "b2", "c": "c2"})
    _reads.clear()
    _runs.clear()

    app.update_blocking(component_selector=[_path("items", "a"), _path("items", "c")])

    assert _reads == ["keys ['a', 'c']"]
    assert sorted(_runs) == ["a2", "c2"]
    assert _stored() == {"a2": "a2", "b1": "b1", "c2": "c2", "other:o": "o"}


def test_a_selected_key_the_source_no_longer_holds_is_deleted() -> None:
    app = _app("gone")
    _source.update({"a": "a1", "b": "b1"})
    app.update_blocking()
    del _source["a"]
    del _source["b"]
    _reads.clear()

    app.update_blocking(component_selector=[_path("items", "a")])

    assert _reads == ["keys ['a']"]
    assert _stored() == {"b1": "b1", "other:o": "o"}
    # The next plain update catches up with what the selector left out.
    app.update_blocking()
    assert _stored() == {"other:o": "o"}


def test_a_selected_path_below_an_item_reads_that_item() -> None:
    app = _app("deeper")
    _source.update({"a": "a1", "b": "b1"})
    app.update_blocking()
    _source["a"] = "a2"
    _reads.clear()

    app.update_blocking(component_selector=[_path("items", "a", "chunk", 3)])

    assert _reads == ["keys ['a']"]
    assert _stored() == {"a2": "a2", "b1": "b1", "other:o": "o"}


def test_a_mount_each_the_selector_leaves_out_reads_nothing() -> None:
    for name, factory in [("left_out", _Items), ("left_out_listed", _ListedOnly)]:
        GlobalDictTarget.store.clear()
        _source.clear()
        app = _app(name, factory)
        _source.update({"a": "a1"})
        app.update_blocking()
        _source["a"] = "a2"
        _reads.clear()
        _runs.clear()

        app.update_blocking(component_selector=[_path("other")])

        assert _reads == []
        assert _runs == ["other:o"]
        assert _stored() == {"a1": "a1", "other:o": "o"}
        app.update_blocking()
        assert _stored() == {"a2": "a2", "other:o": "o"}


def test_a_selected_mount_each_lists_its_source() -> None:
    app = _app("whole")
    _source.update({"a": "a1", "b": "b1"})
    app.update_blocking()
    _source.update({"a": "a2", "b": "b2", "c": "c2"})
    _reads.clear()

    app.update_blocking(component_selector=[_path("items")])

    assert _reads == ["list"]
    assert _stored() == {"a2": "a2", "b2": "b2", "c2": "c2", "other:o": "o"}


def test_a_source_that_only_lists_is_listed_and_filtered() -> None:
    app = _app("listed", _ListedOnly)
    _source.update({"a": "a1", "b": "b1"})
    app.update_blocking()
    _source.update({"a": "a2", "b": "b2"})
    _reads.clear()

    app.update_blocking(component_selector=[_path("items", "a")])

    assert _reads == ["list"]
    assert _stored() == {"a2": "a2", "b1": "b1", "other:o": "o"}


def test_a_preview_reads_by_key_too() -> None:
    app = _app("preview")
    _source.update({"a": "a1", "b": "b1"})
    app.update_blocking()
    _source.update({"a": "a2", "b": "b2"})
    _reads.clear()

    actions = app.update_blocking(
        component_selector=[_path("items", "b")], preview=True
    )

    assert _reads == ["keys ['b']"]
    assert {key for key, _ in actions} == {"b1", "b2"}
    assert _stored() == {"a1": "a1", "b1": "b1", "other:o": "o"}


# --- localfs ---------------------------------------------------------------


@coco.fn
async def _file(file: localfs.File, root: pathlib.Path) -> None:
    key = file.file_path.path.relative_to(root).as_posix()
    _runs.append(key)
    coco.declare_target_state(
        GlobalDictTarget.target_state(key, await file.read_text())
    )


@coco.fn
async def _files_main(root: pathlib.Path) -> None:
    files = localfs.walk_dir(
        root,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
    )
    await coco.mount_each(coco.component_subpath("files"), _file, files.items(), root)


def test_localfs_reads_only_the_selected_files(tmp_path: pathlib.Path) -> None:
    (tmp_path / "docs").mkdir()
    (tmp_path / "a.md").write_text("a1")
    (tmp_path / "docs" / "b.md").write_text("b1")
    (tmp_path / "docs" / "c.md").write_text("c1")
    app = coco.App(
        coco.AppConfig(name="test_pushdown_localfs", environment=coco_env),
        _files_main,
        tmp_path,
    )
    app.update_blocking()
    assert sorted(_runs) == ["a.md", "docs/b.md", "docs/c.md"]

    (tmp_path / "a.md").write_text("a2")
    (tmp_path / "docs" / "b.md").write_text("b2")
    (tmp_path / "docs" / "c.md").unlink()
    _runs.clear()
    app.update_blocking(
        component_selector=[
            _path("files", "docs/b.md"),
            _path("files", "docs/c.md"),
            _path("files", "missing.md"),
        ]
    )
    assert _runs == ["docs/b.md"]
    assert _stored() == {"a.md": "a1", "docs/b.md": "b2"}


def _walk(walker: localfs.DirWalker) -> dict[str, str]:
    async def collect() -> dict[str, str]:
        return {key: await f.read_text() async for key, f in walker.items()}

    import asyncio

    return asyncio.run(collect())


def _read(walker: localfs.DirWalker, keys: list[Any]) -> dict[str, str]:
    async def collect() -> dict[str, str]:
        items = walker.items()
        return {
            key: await f.read_text()
            async for key, f in items.__coco_items_for_keys__(keys)  # type: ignore[attr-defined]
        }

    import asyncio

    return asyncio.run(collect())


@pytest.mark.parametrize("recursive", [True, False])
def test_localfs_reads_by_key_exactly_what_it_walks(
    tmp_path: pathlib.Path, recursive: bool
) -> None:
    for rel in ["a.md", "b.txt", "docs/c.md", "docs/deep/d.md", "skip/e.md"]:
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text(rel)
    walker = localfs.walk_dir(
        tmp_path,
        recursive=recursive,
        path_matcher=PatternFilePathMatcher(
            included_patterns=["**/*.md"], excluded_patterns=["skip"]
        ),
    )
    walked = _walk(walker)
    candidates: list[Any] = [
        "a.md",
        "b.txt",
        "docs/c.md",
        "docs/deep/d.md",
        "skip/e.md",
        "docs",
        "./a.md",
        "docs//c.md",
        "../a.md",
        "/a.md",
        "",
        "missing.md",
        3,
    ]
    assert _read(walker, candidates) == walked
    assert set(walked) == (
        {"a.md", "docs/c.md", "docs/deep/d.md"} if recursive else {"a.md"}
    )
