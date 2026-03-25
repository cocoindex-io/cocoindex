"""Tests for type hint extraction utilities."""

import typing
from dataclasses import dataclass
from typing import Any, Collection

from cocoindex._internal.serde import (
    get_param_annotation,
    serialize,
    unwrap_element_type,
)
from cocoindex._internal.memo_key import _make_state_deserialize_fn
from cocoindex._internal.typing import MemoStateOutcome


# ---- Module-level types used by tests ----


@dataclass
class MyState:
    x: int
    y: str


@dataclass
class MyRecord:
    name: str
    value: float


# ---- Tests ----


def test_extract_return_type_simple() -> None:
    """Basic return type extraction via typing.get_type_hints."""

    def fn() -> list[int]:
        return []

    hints = typing.get_type_hints(fn)
    assert hints["return"] == list[int]


def test_extract_state_type_from_memo_state() -> None:
    """_make_state_deserialize_fn extracts the prev_state type from __coco_memo_state__."""

    class WithMemoState:
        def __coco_memo_state__(self, prev: MyState) -> MemoStateOutcome:
            return MemoStateOutcome(state=prev, memo_valid=True)

    deser = _make_state_deserialize_fn(WithMemoState.__coco_memo_state__)
    assert callable(deser)

    # Round-trip: serialize a MyState, then deserialize with the extracted DeserializeFn.
    original = MyState(x=42, y="hello")
    data = serialize(original)
    restored = deser(data)
    assert restored == original


def test_extract_tracking_record_type_from_reconcile() -> None:
    """get_param_annotation + unwrap_element_type pulls element type from Collection[MyRecord]."""

    class MyHandler:
        def reconcile(
            self,
            key: Any,
            desired: Any,
            prev_possible_records: Collection[MyRecord],
            prev_may_be_missing: bool,
        ) -> Any: ...

    handler = MyHandler()
    ann = get_param_annotation(type(handler).reconcile, 3)
    record_type = unwrap_element_type(ann)
    assert record_type is MyRecord


def test_extract_type_hint_fallback_to_any() -> None:
    """Graceful fallback to Any when annotations are missing."""

    # Function without return annotation
    def no_return():  # type: ignore[no-untyped-def]
        pass

    hints = typing.get_type_hints(no_return)
    assert "return" not in hints

    # Function without type annotations -> _make_state_deserialize_fn returns a DeserializeFn (for Any)
    def untyped_state_fn(self, prev): ...  # type: ignore[no-untyped-def]

    deser = _make_state_deserialize_fn(untyped_state_fn)
    assert callable(deser)

    # Handler without type annotations on reconcile -> unwrap_element_type returns Any
    class BareHandler:
        def reconcile(  # type: ignore[no-untyped-def]
            self, key, desired, prev_possible_records, prev_may_be_missing
        ): ...

    handler = BareHandler()
    ann = get_param_annotation(type(handler).reconcile, 3)
    result = unwrap_element_type(ann)
    assert result is Any
