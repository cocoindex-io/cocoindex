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


# ============================================================================
# "shook" tag and state method collection tests
# ============================================================================


def test_shook_tag_produces_different_fingerprint_from_hook() -> None:
    """Objects with __coco_memo_state__ use 'shook' tag, changing the fingerprint."""

    class HookOnly:
        def __init__(self, v: object) -> None:
            self.v = v

        def __coco_memo_key__(self) -> object:
            return ("key", self.v)

    class HookAndState:
        def __init__(self, v: object) -> None:
            self.v = v

        def __coco_memo_key__(self) -> object:
            return ("key", self.v)

        def __coco_memo_state__(self, prev_state: object) -> object:
            return prev_state

    fp_hook = fingerprint_call(_dummy_fn, (HookOnly(42),), {})
    fp_shook = fingerprint_call(_dummy_fn, (HookAndState(42),), {})
    # "shook" tag intentionally changes fingerprint to force re-execution
    assert fp_hook != fp_shook


def test_shook_fingerprint_is_deterministic() -> None:
    class Stateful:
        def __init__(self, v: object) -> None:
            self.v = v

        def __coco_memo_key__(self) -> object:
            return self.v

        def __coco_memo_state__(self, prev_state: object) -> object:
            return prev_state

    fp1 = fingerprint_call(_dummy_fn, (Stateful(99),), {})
    fp2 = fingerprint_call(_dummy_fn, (Stateful(99),), {})
    assert fp1 == fp2


def test_state_methods_collected_from_hook() -> None:
    class WithState:
        def __init__(self, v: object) -> None:
            self.v = v

        def __coco_memo_key__(self) -> object:
            return self.v

        def __coco_memo_state__(self, prev_state: object) -> object:
            return prev_state

    methods: list[Any] = []
    fingerprint_call(_dummy_fn, (WithState(1),), {}, state_methods=methods)
    assert len(methods) == 1
    assert callable(methods[0])


def test_state_methods_collected_from_registry() -> None:
    class Registered:
        def __init__(self, v: object) -> None:
            self.v = v

    def _state_fn(obj: Any, prev: Any) -> Any:
        return prev

    register_memo_key_function(Registered, lambda r: r.v, state_fn=_state_fn)
    try:
        methods: list[Any] = []
        fingerprint_call(_dummy_fn, (Registered(7),), {}, state_methods=methods)
        assert len(methods) == 1
        assert callable(methods[0])
    finally:
        unregister_memo_key_function(Registered)


def test_state_methods_not_collected_when_list_is_none() -> None:
    class WithState:
        def __init__(self, v: object) -> None:
            self.v = v

        def __coco_memo_key__(self) -> object:
            return self.v

        def __coco_memo_state__(self, prev_state: object) -> object:
            return prev_state

    # Should not raise — state_methods defaults to None
    fp = fingerprint_call(_dummy_fn, (WithState(1),), {})
    assert len(bytes(fp)) == 16


def test_multiple_state_methods_collected_in_order() -> None:
    class S1:
        def __coco_memo_key__(self) -> object:
            return "s1"

        def __coco_memo_state__(self, prev: object) -> object:
            return "from_s1"

    class S2:
        def __coco_memo_key__(self) -> object:
            return "s2"

        def __coco_memo_state__(self, prev: object) -> object:
            return "from_s2"

    methods: list[Any] = []
    fingerprint_call(_dummy_fn, (S1(), S2()), {}, state_methods=methods)
    assert len(methods) == 2
    # Verify order: S1 first (first arg), S2 second
    assert methods[0]("x") == "from_s1"
    assert methods[1]("x") == "from_s2"


def test_no_state_methods_for_hook_only_objects() -> None:
    class HookOnly:
        def __coco_memo_key__(self) -> object:
            return "key"

    methods: list[Any] = []
    fingerprint_call(_dummy_fn, (HookOnly(),), {}, state_methods=methods)
    assert len(methods) == 0
