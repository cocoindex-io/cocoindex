from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import cocoindex as coco
import numpy as np
import pytest
from cocoindex._internal import core
from cocoindex.connectors import turboquant
from cocoindex.connectors.turboquant._target import (
    _vector_fingerprint,
)

from tests import common

coco_env = common.create_test_env(__file__)

_BASIS = np.eye(8, dtype=np.float32)


def _search_ids(
    index_path: Path, query: np.ndarray, k: int, allowlist: list[int] | None = None
) -> list[int]:
    return [
        result.id
        for result in turboquant.search(index_path, query, k, allowlist=allowlist)
    ]


def test_turboquant_target_add_update_delete(tmp_path: Path) -> None:
    index_path = tmp_path / "index.tvim"
    state = {1: _BASIS[0], 2: _BASIS[1]}

    async def flow() -> None:
        target = await turboquant.mount_index_target(index_path, bit_width=4)
        for id_, vector in state.items():
            target.declare_vector(id=id_, vector=vector)

    app = coco.App(
        coco.AppConfig(
            name="test_turboquant_target_add_update_delete", environment=coco_env
        ),
        flow,
    )

    app.update_blocking()
    assert _search_ids(index_path, _BASIS[0], 2) == [1, 2]

    del state[2]
    state[1] = _BASIS[3]
    state[3] = _BASIS[2]
    app.update_blocking()

    index = turboquant.load_index(index_path)
    assert len(index) == 2
    assert 2 not in index
    assert _search_ids(index_path, _BASIS[3], 1) == [1]
    assert _search_ids(index_path, _BASIS[2], 2) == [3, 1]
    assert _search_ids(index_path, _BASIS[2], 2, allowlist=[1, 999]) == [1]
    assert _search_ids(index_path, _BASIS[2], 2, allowlist=[2, 999]) == []


def test_turboquant_target_skips_unchanged_vectors(tmp_path: Path) -> None:
    index_path = tmp_path / "index.tvim"

    async def flow() -> None:
        target = await turboquant.mount_index_target(index_path, bit_width=4)
        target.declare_vector(id=1, vector=_BASIS[0])

    app = coco.App(
        coco.AppConfig(
            name="test_turboquant_target_skips_unchanged_vectors",
            environment=coco_env,
        ),
        flow,
    )

    app.update_blocking()
    first_stat = index_path.stat()

    app.update_blocking()
    second_stat = index_path.stat()

    assert (second_stat.st_mtime_ns, second_stat.st_size) == (
        first_stat.st_mtime_ns,
        first_stat.st_size,
    )


def test_turboquant_target_bit_width_change_rebuilds_index(tmp_path: Path) -> None:
    index_path = tmp_path / "index.tvim"
    bit_width = 4

    async def flow() -> None:
        target = await turboquant.mount_index_target(index_path, bit_width=bit_width)
        target.declare_vector(id=1, vector=_BASIS[0])
        target.declare_vector(id=2, vector=_BASIS[1])

    app = coco.App(
        coco.AppConfig(
            name="test_turboquant_target_bit_width_change_rebuilds_index",
            environment=coco_env,
        ),
        flow,
    )

    app.update_blocking()
    assert turboquant.load_index(index_path).bit_width == 4

    bit_width = 2
    app.update_blocking()

    index = turboquant.load_index(index_path)
    assert index.bit_width == 2
    assert len(index) == 2
    assert _search_ids(index_path, _BASIS[1], 1) == [2]


def test_turboquant_vector_fingerprint_is_compact() -> None:
    vector = np.eye(1536, dtype=np.float32)[0]

    fingerprint = _vector_fingerprint(vector)

    assert len(fingerprint) == 16


def test_turboquant_index_rejects_non_contiguous_vectors() -> None:
    index = core.TurboQuantIdMapIndex(dim=8, bit_width=4)
    vectors = np.eye(16, dtype=np.float32)[:, ::2]

    with pytest.raises(ValueError, match="vectors must be a C-contiguous array"):
        index.add_with_ids(vectors, np.arange(16, dtype=np.uint64))

    assert len(index) == 0


def test_turboquant_index_searches_while_adding() -> None:
    index = core.TurboQuantIdMapIndex(dim=8, bit_width=4)
    index.add_with_ids(_BASIS[:1], np.array([0], dtype=np.uint64))
    num_adds = 200

    def search_repeatedly() -> set[int]:
        found: set[int] = set()
        for _ in range(num_adds):
            _, ids = index.search(_BASIS[:1], 1)
            found.update(int(id_) for id_ in ids.reshape(-1))
        return found

    with ThreadPoolExecutor(max_workers=4) as pool:
        searches = [pool.submit(search_repeatedly) for _ in range(4)]
        for id_ in range(1, num_adds + 1):
            index.add_with_ids(
                _BASIS[1 + id_ % 7][None, :], np.array([id_], dtype=np.uint64)
            )
        found = [search.result() for search in searches]

    assert len(index) == num_adds + 1
    assert found == [{0}] * 4


def test_turboquant_target_removes_index(tmp_path: Path) -> None:
    index_path = tmp_path / "index.tvim"
    enabled = True

    async def flow() -> None:
        if enabled:
            target = await turboquant.mount_index_target(index_path, bit_width=4)
            target.declare_vector(id=1, vector=_BASIS[0])

    app = coco.App(
        coco.AppConfig(
            name="test_turboquant_target_removes_index", environment=coco_env
        ),
        flow,
    )

    app.update_blocking()
    assert index_path.exists()

    enabled = False
    app.update_blocking()
    assert not index_path.exists()
