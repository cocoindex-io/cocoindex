import math
from typing import Any

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


def test_nan_is_deterministic() -> None:
    nan1 = float("nan")
    nan2 = math.nan
    assert fingerprint_call(_dummy_fn, (nan1,), {}) == fingerprint_call(
        _dummy_fn, (nan2,), {}
    )


def test_hook_overrides_default_behavior() -> None:
    class X:
        def __init__(self, v: object) -> None:
            self.v = v

        def __coco_memo_key__(self) -> object:
            return ("x", self.v)

    assert fingerprint_call(_dummy_fn, (X(123),), {}) == fingerprint_call(
        _dummy_fn, (("x", 123),), {}
    )


def test_registry_overrides_default_behavior() -> None:
    class Y:
        def __init__(self, v: object) -> None:
            self.v = v

    register_memo_key_function(Y, lambda y: ("y", y.v))
    try:
        assert fingerprint_call(_dummy_fn, (Y(5),), {}) == fingerprint_call(
            _dummy_fn, (("y", 5),), {}
        )
    finally:
        unregister_memo_key_function(Y)


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
