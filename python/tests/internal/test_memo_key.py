import dataclasses
import math
from typing import Any

import pytest

from cocoindex._internal.memo_key import (
    fingerprint_call,
    register_memo_key_function,
    unregister_memo_key_function,
)


class _PickleableZ:
    pass


def _dummy_fn(*args: Any, **kwargs: Any) -> None:
    raise RuntimeError("not called")


def test_fingerprint_dict_order_independent() -> None:
    a = {"x": 1, "y": 2}
    b = {"y": 2, "x": 1}
    assert fingerprint_call(_dummy_fn, (a,), {}) == fingerprint_call(
        _dummy_fn, (b,), {}
    )


def test_fingerprint_set_order_independent() -> None:
    a = {3, 1, 2}
    b = {2, 3, 1}
    assert fingerprint_call(_dummy_fn, (a,), {}) == fingerprint_call(
        _dummy_fn, (b,), {}
    )


def test_different_types_do_not_collide() -> None:
    f_int = fingerprint_call(_dummy_fn, (1,), {})
    f_str = fingerprint_call(_dummy_fn, ("1",), {})
    f_bytes = fingerprint_call(_dummy_fn, (b"1",), {})
    assert f_int != f_str
    assert f_int != f_bytes
    assert f_str != f_bytes
    # Fingerprint is a stable 16-byte digest.
    assert len(bytes(f_int)) == 16


def test_nan_is_deterministic() -> None:
    nan1 = float("nan")
    nan2 = math.nan
    assert fingerprint_call(_dummy_fn, (nan1,), {}) == fingerprint_call(
        _dummy_fn, (nan2,), {}
    )


def test_hook_overrides_default_behavior() -> None:
    class X:
        def __init__(self, v: object, irrelevant: object) -> None:
            self.v = v
            self.irrelevant = irrelevant

        def __coco_memo_key__(self) -> object:
            return ("x", self.v)

    # Behavioral properties (avoid asserting on canonical form implementation details):
    # - Same memo-key-relevant data => same fingerprint
    # - Different memo-key-relevant data => different fingerprint
    fp_a = fingerprint_call(_dummy_fn, (X(123, "a"),), {})
    fp_irrelevant_changed = fingerprint_call(_dummy_fn, (X(123, "b"),), {})
    fp_b = fingerprint_call(_dummy_fn, (X(124, "a"),), {})
    assert fp_a == fp_irrelevant_changed
    assert fp_a != fp_b


def test_registry_overrides_default_behavior() -> None:
    class Y:
        def __init__(self, v: object, irrelevant: object) -> None:
            self.v = v
            self.irrelevant = irrelevant

    register_memo_key_function(Y, lambda y: ("y", y.v))
    try:
        fp_a = fingerprint_call(_dummy_fn, (Y(5, "a"),), {})
        fp_irrelevant_changed = fingerprint_call(_dummy_fn, (Y(5, "b"),), {})
        fp_b = fingerprint_call(_dummy_fn, (Y(6, "a"),), {})
        assert fp_a == fp_irrelevant_changed
        assert fp_a != fp_b
    finally:
        unregister_memo_key_function(Y)


def test_hook_and_registry_include_type_name_to_avoid_collisions() -> None:
    class A:
        def __init__(self, v: object) -> None:
            self.v = v

        def __coco_memo_key__(self) -> object:
            # Identical payload as B on purpose.
            return ("same", self.v)

    class B:
        def __init__(self, v: object) -> None:
            self.v = v

        def __coco_memo_key__(self) -> object:
            # Identical payload as A on purpose.
            return ("same", self.v)

    assert fingerprint_call(_dummy_fn, (A(1),), {}) != fingerprint_call(
        _dummy_fn, (B(1),), {}
    )

    class C:
        def __init__(self, v: object) -> None:
            self.v = v

    class D:
        def __init__(self, v: object) -> None:
            self.v = v

    register_memo_key_function(C, lambda x: ("same", x.v))
    register_memo_key_function(D, lambda x: ("same", x.v))
    try:
        assert fingerprint_call(_dummy_fn, (C(1),), {}) != fingerprint_call(
            _dummy_fn, (D(1),), {}
        )
    finally:
        unregister_memo_key_function(C)
        unregister_memo_key_function(D)


def test_cycles_are_supported_and_deterministic() -> None:
    # Self-cycle list
    a: Any = []
    a.append(a)
    b: Any = []
    b.append(b)
    assert fingerprint_call(_dummy_fn, (a,), {}) == fingerprint_call(
        _dummy_fn, (b,), {}
    )

    # Self-cycle dict
    d1: dict[str, object] = {}
    d1["self"] = d1
    d2: dict[str, object] = {}
    d2["self"] = d2
    assert fingerprint_call(_dummy_fn, (d1,), {}) == fingerprint_call(
        _dummy_fn, (d2,), {}
    )

    # Mutual reference pair
    x1: list[object] = []
    y1: list[object] = []
    x1.append(y1)
    y1.append(x1)
    x2: list[object] = []
    y2: list[object] = []
    x2.append(y2)
    y2.append(x2)
    assert fingerprint_call(_dummy_fn, (x1,), {}) == fingerprint_call(
        _dummy_fn, (x2,), {}
    )


def test_pickle_fallback_for_unsupported_objects() -> None:
    # complex is not one of the supported primitives/containers, but is picklable.
    assert fingerprint_call(_dummy_fn, (complex(1, 2),), {}) == fingerprint_call(
        _dummy_fn, (complex(1, 2),), {}
    )

    # A simple user-defined type (module-level) should also work via pickle fallback.
    assert fingerprint_call(_dummy_fn, (_PickleableZ(),), {}) == fingerprint_call(
        _dummy_fn, (_PickleableZ(),), {}
    )

    # Unpicklable payloads should raise the original TypeError.
    try:
        fingerprint_call(_dummy_fn, (lambda x: x,), {})
        assert False, "Expected TypeError"
    except TypeError:
        pass


def test_dataclass_memo_key() -> None:
    """Test that dataclass instances are fingerprinted structurally."""

    @dataclasses.dataclass
    class Point:
        x: int
        y: int

    p1 = Point(x=1, y=2)
    p2 = Point(x=1, y=2)
    p3 = Point(x=2, y=1)

    # Same values -> same fingerprint
    assert fingerprint_call(_dummy_fn, (p1,), {}) == fingerprint_call(
        _dummy_fn, (p2,), {}
    )

    # Different values -> different fingerprint
    assert fingerprint_call(_dummy_fn, (p1,), {}) != fingerprint_call(
        _dummy_fn, (p3,), {}
    )


def test_dataclass_field_order_preserved() -> None:
    """Test that dataclass field definition order matters for fingerprinting."""

    @dataclasses.dataclass
    class PointXY:
        x: int
        y: int

    @dataclasses.dataclass
    class PointYX:
        y: int
        x: int

    p1 = PointXY(x=1, y=2)
    p2 = PointYX(y=2, x=1)

    # Different field order -> different fingerprint (by design)
    assert fingerprint_call(_dummy_fn, (p1,), {}) != fingerprint_call(
        _dummy_fn, (p2,), {}
    )


def test_dataclass_nested() -> None:
    """Test nested dataclass fingerprinting."""

    @dataclasses.dataclass
    class Inner:
        value: int

    @dataclasses.dataclass
    class Outer:
        inner: Inner
        name: str

    o1 = Outer(inner=Inner(value=42), name="test")
    o2 = Outer(inner=Inner(value=42), name="test")
    o3 = Outer(inner=Inner(value=43), name="test")

    # Same values -> same fingerprint
    assert fingerprint_call(_dummy_fn, (o1,), {}) == fingerprint_call(
        _dummy_fn, (o2,), {}
    )

    # Different nested values -> different fingerprint
    assert fingerprint_call(_dummy_fn, (o1,), {}) != fingerprint_call(
        _dummy_fn, (o3,), {}
    )


def test_dataclass_different_types_same_fields() -> None:
    """Test that different dataclass types with same fields produce different fingerprints."""

    @dataclasses.dataclass
    class TypeA:
        value: int

    @dataclasses.dataclass
    class TypeB:
        value: int

    a = TypeA(value=1)
    b = TypeB(value=1)

    # Different types -> different fingerprints
    assert fingerprint_call(_dummy_fn, (a,), {}) != fingerprint_call(
        _dummy_fn, (b,), {}
    )


def test_dataclass_override_with_coco_memo_key() -> None:
    """Test that __coco_memo_key__ takes precedence over automatic dataclass handling."""

    @dataclasses.dataclass
    class WithOverride:
        value: int
        ignored: str

        def __coco_memo_key__(self) -> object:
            return ("custom", self.value)

    w1 = WithOverride(value=1, ignored="a")
    w2 = WithOverride(value=1, ignored="b")
    w3 = WithOverride(value=2, ignored="a")

    # Same memo-key-relevant data -> same fingerprint (custom hook ignores 'ignored')
    assert fingerprint_call(_dummy_fn, (w1,), {}) == fingerprint_call(
        _dummy_fn, (w2,), {}
    )

    # Different memo-key-relevant data -> different fingerprint
    assert fingerprint_call(_dummy_fn, (w1,), {}) != fingerprint_call(
        _dummy_fn, (w3,), {}
    )


def test_pydantic_memo_key() -> None:
    """Test that Pydantic v2 models are fingerprinted structurally."""
    try:
        from pydantic import BaseModel
    except ImportError:
        pytest.skip("pydantic not installed")
        return

    class Point(BaseModel):
        x: int
        y: int

    p1 = Point(x=1, y=2)
    p2 = Point(x=1, y=2)
    p3 = Point(x=2, y=1)

    # Same values -> same fingerprint
    assert fingerprint_call(_dummy_fn, (p1,), {}) == fingerprint_call(
        _dummy_fn, (p2,), {}
    )

    # Different values -> different fingerprint
    assert fingerprint_call(_dummy_fn, (p1,), {}) != fingerprint_call(
        _dummy_fn, (p3,), {}
    )


def test_pydantic_includes_unset_fields() -> None:
    """Test that Pydantic models include all fields in fingerprint, even unset ones."""
    try:
        from pydantic import BaseModel
    except ImportError:
        pytest.skip("pydantic not installed")
        return

    class Config(BaseModel):
        name: str
        value: int = 42  # default value

    c1 = Config(name="test")  # uses default value=42
    c2 = Config(name="test", value=42)  # explicitly set value=42
    c3 = Config(name="test", value=43)  # different value

    # Same effective values -> same fingerprint
    assert fingerprint_call(_dummy_fn, (c1,), {}) == fingerprint_call(
        _dummy_fn, (c2,), {}
    )

    # Different values -> different fingerprint
    assert fingerprint_call(_dummy_fn, (c1,), {}) != fingerprint_call(
        _dummy_fn, (c3,), {}
    )


def test_pydantic_nested() -> None:
    """Test nested Pydantic model fingerprinting."""
    try:
        from pydantic import BaseModel
    except ImportError:
        pytest.skip("pydantic not installed")
        return

    class Inner(BaseModel):
        value: int

    class Outer(BaseModel):
        inner: Inner
        name: str

    o1 = Outer(inner=Inner(value=42), name="test")
    o2 = Outer(inner=Inner(value=42), name="test")
    o3 = Outer(inner=Inner(value=43), name="test")

    # Same values -> same fingerprint
    assert fingerprint_call(_dummy_fn, (o1,), {}) == fingerprint_call(
        _dummy_fn, (o2,), {}
    )

    # Different nested values -> different fingerprint
    assert fingerprint_call(_dummy_fn, (o1,), {}) != fingerprint_call(
        _dummy_fn, (o3,), {}
    )


def test_pydantic_different_types_same_fields() -> None:
    """Test that different Pydantic model types with same fields produce different fingerprints."""
    try:
        from pydantic import BaseModel
    except ImportError:
        pytest.skip("pydantic not installed")
        return

    class TypeA(BaseModel):
        value: int

    class TypeB(BaseModel):
        value: int

    a = TypeA(value=1)
    b = TypeB(value=1)

    # Different types -> different fingerprints
    assert fingerprint_call(_dummy_fn, (a,), {}) != fingerprint_call(
        _dummy_fn, (b,), {}
    )


def test_pydantic_override_with_coco_memo_key() -> None:
    """Test that __coco_memo_key__ takes precedence over automatic Pydantic handling."""
    try:
        from pydantic import BaseModel
    except ImportError:
        pytest.skip("pydantic not installed")
        return

    class WithOverride(BaseModel):
        value: int
        ignored: str

        def __coco_memo_key__(self) -> object:
            return ("custom", self.value)

    w1 = WithOverride(value=1, ignored="a")
    w2 = WithOverride(value=1, ignored="b")
    w3 = WithOverride(value=2, ignored="a")

    # Same memo-key-relevant data -> same fingerprint (custom hook ignores 'ignored')
    assert fingerprint_call(_dummy_fn, (w1,), {}) == fingerprint_call(
        _dummy_fn, (w2,), {}
    )

    # Different memo-key-relevant data -> different fingerprint
    assert fingerprint_call(_dummy_fn, (w1,), {}) != fingerprint_call(
        _dummy_fn, (w3,), {}
    )


def test_dataclass_and_pydantic_different_types() -> None:
    """Test that dataclass and Pydantic model with same fields produce different fingerprints."""
    try:
        from pydantic import BaseModel
    except ImportError:
        pytest.skip("pydantic not installed")
        return

    @dataclasses.dataclass
    class DataPoint:
        x: int
        y: int

    class PydanticPoint(BaseModel):
        x: int
        y: int

    d = DataPoint(x=1, y=2)
    p = PydanticPoint(x=1, y=2)

    # Different type kinds -> different fingerprints
    assert fingerprint_call(_dummy_fn, (d,), {}) != fingerprint_call(
        _dummy_fn, (p,), {}
    )
