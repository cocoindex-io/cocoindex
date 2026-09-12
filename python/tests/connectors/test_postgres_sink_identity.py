"""Postgres sink identity must be shared even during concurrent reconciliation."""

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import cocoindex as coco
import pytest

pytest.importorskip("asyncpg")

from cocoindex.connectors.postgres._target import (
    _DbAction,
    _get_db_sink,
    _RowHandler,
)


def test_db_sink_identity_across_threads() -> None:
    # Fresh keys exercise concurrent creation, rather than an existing identity.
    prefix = uuid.uuid4().hex
    keys = [f"{prefix}_{i % 2}" for i in range(8)]
    barrier = threading.Barrier(len(keys))

    def create_sink(db_key: str) -> coco.TargetActionSink[_DbAction, _RowHandler]:
        barrier.wait(timeout=10)
        return _get_db_sink(db_key)

    with ThreadPoolExecutor(max_workers=len(keys)) as executor:
        sinks = list(executor.map(create_sink, keys))

    # Keep all wrappers alive: equal keys must share the actual engine identity,
    # while different keys must never share a transaction batch.
    for i, sink in enumerate(sinks):
        for j, other in enumerate(sinks):
            assert (sink._core == other._core) == (keys[i] == keys[j])
            if keys[i] == keys[j]:
                assert hash(sink._core) == hash(other._core)
