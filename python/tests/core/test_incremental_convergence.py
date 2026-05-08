"""Convergence tests for incremental updates against fresh rebuilds."""

from __future__ import annotations

from dataclasses import dataclass
from typing import NamedTuple

import cocoindex as coco

from tests import common
from tests.common.target_states import DictsTarget, Metrics


@dataclass(frozen=True)
class Document:
    doc_id: str
    version: int
    text: str

    def __coco_memo_key__(self) -> object:
        return (self.doc_id, self.version)


class Chunk(NamedTuple):
    index: int
    text: str


SourceState = dict[str, Document]
TargetSnapshot = dict[str, dict[str, str]]


coco_env = common.create_test_env(__file__)
_source_data: SourceState = {}
_metrics = Metrics()


def _set_source_state(state: SourceState) -> None:
    _source_data.clear()
    _source_data.update(state)


def _normalize_target_snapshot() -> TargetSnapshot:
    return {
        doc_id: {
            chunk_id: row.data
            for chunk_id, row in sorted(chunks.items(), key=lambda item: item[0])
        }
        for doc_id, chunks in sorted(
            DictsTarget.store.data.items(), key=lambda item: item[0]
        )
    }


def _split_words(text: str) -> list[Chunk]:
    return [Chunk(index=i, text=word) for i, word in enumerate(text.split())]


def _expected_snapshot(state: SourceState) -> TargetSnapshot:
    return {
        doc_id: {
            f"chunk:{chunk.index}": f"{doc_id}:{chunk.index}:{chunk.text}"
            for chunk in _split_words(doc.text)
        }
        for doc_id, doc in sorted(state.items(), key=lambda item: item[0])
    }


@coco.fn(memo=True)
def _split_document(doc: Document) -> list[Chunk]:
    """Split text into stable chunk records while exercising function memoization."""
    _metrics.increment(f"split.{doc.doc_id}")
    return _split_words(doc.text)


@coco.fn(memo=True)
def _declare_document_chunks(
    doc: Document, provider: coco.TargetStateProvider[str]
) -> None:
    """Declare all chunk rows for one document under a child target."""
    _metrics.increment(f"declare.{doc.doc_id}")
    for chunk in _split_document(doc):
        coco.declare_target_state(
            provider.target_state(
                f"chunk:{chunk.index}",
                f"{doc.doc_id}:{chunk.index}:{chunk.text}",
            )
        )


@coco.fn
async def _index_documents() -> None:
    """Index documents as child targets, one component per document."""
    for doc_id in sorted(_source_data):
        doc = _source_data[doc_id]
        provider = await coco.use_mount(
            coco.component_subpath("setup", doc_id),
            DictsTarget.declare_dict_target,
            doc_id,
        )
        await coco.mount(
            coco.component_subpath("docs", doc_id),
            _declare_document_chunks,
            doc,
            provider,
        )


def _run_app(app_name: str) -> TargetSnapshot:
    app = coco.App(
        coco.AppConfig(name=app_name, environment=coco_env), _index_documents
    )
    app.update_blocking()
    return _normalize_target_snapshot()


def _run_incremental_sequence(
    app_name: str, history: list[SourceState]
) -> TargetSnapshot:
    app = coco.App(
        coco.AppConfig(name=app_name, environment=coco_env), _index_documents
    )
    for state in history:
        _set_source_state(state)
        app.update_blocking()
    return _normalize_target_snapshot()


def test_incremental_sequence_converges_to_fresh_rebuild() -> None:
    """A complex incremental history must converge to the same target as a fresh rebuild."""
    history: list[SourceState] = [
        {
            "alpha": Document("alpha", 1, "a b c d"),
            "beta": Document("beta", 1, "x y"),
        },
        {
            # alpha shrinks, so stale chunk rows must be deleted.
            "alpha": Document("alpha", 2, "a b"),
            # beta disappears entirely, so its child target must be removed.
            "gamma": Document("gamma", 1, "g h i"),
        },
        {
            # alpha expands again, reusing the same component path with a new memo key.
            "alpha": Document("alpha", 3, "a b c d e"),
            # beta reappears with the same memo key used before deletion. It must not
            # resurrect stale rows from the deleted component.
            "beta": Document("beta", 1, "x y z"),
            "gamma": Document("gamma", 2, "g"),
        },
        {
            # Final state is intentionally ordered differently from earlier states.
            "gamma": Document("gamma", 2, "g"),
            "beta": Document("beta", 2, "x y z q"),
            "alpha": Document("alpha", 3, "a b c d e"),
        },
    ]

    DictsTarget.store.clear()
    _source_data.clear()
    _metrics.clear()
    incremental_snapshot = _run_incremental_sequence(
        "test_incremental_sequence_converges_to_fresh_rebuild_incremental",
        history,
    )

    DictsTarget.store.clear()
    _source_data.clear()
    _metrics.clear()
    _set_source_state(history[-1])
    fresh_snapshot = _run_app(
        "test_incremental_sequence_converges_to_fresh_rebuild_fresh"
    )

    assert incremental_snapshot == fresh_snapshot == _expected_snapshot(history[-1])


def test_deleted_component_reinserted_with_same_memo_key_recomputes_targets() -> None:
    """Reinserted components with the same memo key must not reuse stale target rows."""
    history: list[SourceState] = [
        {"doc": Document("doc", 1, "old stale")},
        {},
        {"doc": Document("doc", 1, "new fresh result")},
    ]

    DictsTarget.store.clear()
    _source_data.clear()
    _metrics.clear()
    snapshot = _run_incremental_sequence(
        "test_deleted_component_reinserted_with_same_memo_key_recomputes_targets",
        history,
    )

    assert snapshot == _expected_snapshot(history[-1])
    assert snapshot == {
        "doc": {
            "chunk:0": "doc:0:new",
            "chunk:1": "doc:1:fresh",
            "chunk:2": "doc:2:result",
        }
    }
    assert _metrics.collect() == {
        "declare.doc": 2,
        "split.doc": 2,
    }


def test_reordered_source_iteration_keeps_target_snapshot_stable() -> None:
    """Changing source iteration order must not perturb the materialized target state."""
    first_order: SourceState = {
        "a": Document("a", 1, "one two"),
        "b": Document("b", 1, "three"),
        "c": Document("c", 1, "four five"),
    }
    second_order: SourceState = {
        "c": first_order["c"],
        "a": first_order["a"],
        "b": first_order["b"],
    }

    DictsTarget.store.clear()
    _source_data.clear()
    _metrics.clear()
    snapshot = _run_incremental_sequence(
        "test_reordered_source_iteration_keeps_target_snapshot_stable",
        [first_order, second_order],
    )

    assert snapshot == _expected_snapshot(first_order)
    assert _metrics.collect() == {
        "declare.a": 1,
        "declare.b": 1,
        "declare.c": 1,
        "split.a": 1,
        "split.b": 1,
        "split.c": 1,
    }
