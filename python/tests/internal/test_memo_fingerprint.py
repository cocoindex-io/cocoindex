import dataclasses
import math
from typing import Any, ClassVar, cast

import pytest

from cocoindex._internal.function import _apply_memo_key, _normalize_memo_key
from cocoindex._internal.memo_fingerprint import (
    fingerprint_call,
    StateFnEntry,
    _unregister_memo_type_identifier,
    register_memo_type_identifier,
    register_memo_key_function,
    unregister_memo_key_function,
)
from cocoindex._internal.typing import MemoStateOutcome


class _PickleableZ:
    pass


def _dummy_fn(*args: Any, **kwargs: Any) -> None:
    raise RuntimeError("not called")


def test_fingerprint_dict_order_independent() -> None:
    a = {"x": 1, "y": 2}
    b = {"y": 2, "x": 1}
    assert fingerprint_call(_dummy_fn, (a,), {}, []) == fingerprint_call(
        _dummy_fn, (b,), {}, []
    )


def test_fingerprint_set_order_independent() -> None:
    a = {3, 1, 2}
    b = {2, 3, 1}
    assert fingerprint_call(_dummy_fn, (a,), {}, []) == fingerprint_call(
        _dummy_fn, (b,), {}, []
    )


def test_different_types_do_not_collide() -> None:
    f_int = fingerprint_call(_dummy_fn, (1,), {}, [])
    f_str = fingerprint_call(_dummy_fn, ("1",), {}, [])
    f_bytes = fingerprint_call(_dummy_fn, (b"1",), {}, [])
    assert f_int != f_str
    assert f_int != f_bytes
    assert f_str != f_bytes
    # Fingerprint is a stable 16-byte digest.
    assert len(bytes(f_int)) == 16


def test_nan_is_deterministic() -> None:
    nan1 = float("nan")
    nan2 = math.nan
    assert fingerprint_call(_dummy_fn, (nan1,), {}, []) == fingerprint_call(
        _dummy_fn, (nan2,), {}, []
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
    fp_a = fingerprint_call(_dummy_fn, (X(123, "a"),), {}, [])
    fp_irrelevant_changed = fingerprint_call(_dummy_fn, (X(123, "b"),), {}, [])
    fp_b = fingerprint_call(_dummy_fn, (X(124, "a"),), {}, [])
    assert fp_a == fp_irrelevant_changed
    assert fp_a != fp_b


def test_registry_overrides_default_behavior() -> None:
    class Y:
        def __init__(self, v: object, irrelevant: object) -> None:
            self.v = v
            self.irrelevant = irrelevant

    register_memo_key_function(Y, lambda y: ("y", y.v))
    try:
        fp_a = fingerprint_call(_dummy_fn, (Y(5, "a"),), {}, [])
        fp_irrelevant_changed = fingerprint_call(_dummy_fn, (Y(5, "b"),), {}, [])
        fp_b = fingerprint_call(_dummy_fn, (Y(6, "a"),), {}, [])
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

    assert fingerprint_call(_dummy_fn, (A(1),), {}, []) != fingerprint_call(
        _dummy_fn, (B(1),), {}, []
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
        assert fingerprint_call(_dummy_fn, (C(1),), {}, []) != fingerprint_call(
            _dummy_fn, (D(1),), {}, []
        )
    finally:
        unregister_memo_key_function(C)
        unregister_memo_key_function(D)


def test_dataclass_stable_type_id_reuses_fingerprint_across_module_move() -> None:
    def make_entry(module: str) -> type[Any]:
        @dataclasses.dataclass
        class Entry:
            __coco_memo_type_id__ = "test.Entry/v1"

            value: int

        Entry.__module__ = module
        return Entry

    OldEntry = make_entry("tests.old_entries")
    NewEntry = make_entry("tests.new_entries")

    assert OldEntry.__qualname__ == NewEntry.__qualname__
    assert OldEntry.__module__ != NewEntry.__module__

    assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) == fingerprint_call(
        _dummy_fn, (NewEntry(1),), {}, []
    )
    assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) != fingerprint_call(
        _dummy_fn, (NewEntry(2),), {}, []
    )


def test_pydantic_stable_type_id_allows_renamed_model_reuse() -> None:
    try:
        from pydantic import BaseModel
    except ImportError:
        pytest.skip("pydantic not installed")
        return

    class OldModel(BaseModel):
        __coco_memo_type_id__: ClassVar[str] = "test.Model/v1"

        value: int

    class NewModel(BaseModel):
        __coco_memo_type_id__: ClassVar[str] = "test.Model/v1"

        value: int

    assert fingerprint_call(
        _dummy_fn, (OldModel(value=1),), {}, []
    ) == fingerprint_call(_dummy_fn, (NewModel(value=1),), {}, [])
    assert fingerprint_call(
        _dummy_fn, (OldModel(value=1),), {}, []
    ) != fingerprint_call(_dummy_fn, (NewModel(value=2),), {}, [])


class _UninspectableZeroArgMemoHook:
    @property
    def __signature__(self) -> object:
        raise ValueError("signature unavailable")

    def __call__(self) -> object:
        return ("class-hook", "stable")


def test_raw_class_object_honors_zero_arg_memo_key() -> None:
    def stable_key() -> object:
        return ("class-hook", "stable")

    def other_key() -> object:
        return ("class-hook", "other")

    class OldEntry:
        __coco_memo_key__ = stable_key

    class NewEntry:
        __coco_memo_key__ = stable_key

    class ChangedEntry:
        __coco_memo_key__ = other_key

    OldEntry.__module__ = "tests.old_raw_class"
    NewEntry.__module__ = "tests.new_raw_class"
    ChangedEntry.__module__ = "tests.new_raw_class"

    assert fingerprint_call(_dummy_fn, (OldEntry,), {}, []) == fingerprint_call(
        _dummy_fn, (NewEntry,), {}, []
    )
    assert fingerprint_call(_dummy_fn, (NewEntry,), {}, []) != fingerprint_call(
        _dummy_fn, (ChangedEntry,), {}, []
    )


def test_raw_class_object_metaclass_hook_collects_state_method() -> None:
    class MemoMeta(type):
        def __coco_memo_key__(cls) -> object:
            return ("metaclass-hook", "stable")

        def __coco_memo_state__(cls, prev_state: object) -> MemoStateOutcome:
            return MemoStateOutcome(
                state=("state", cls.__name__, prev_state), memo_valid=True
            )

    class OldEntry(metaclass=MemoMeta):
        pass

    class NewEntry(metaclass=MemoMeta):
        pass

    OldEntry.__module__ = "tests.old_raw_class"
    NewEntry.__module__ = "tests.new_raw_class"

    old_methods: list[Any] = []
    new_methods: list[Any] = []
    assert fingerprint_call(_dummy_fn, (OldEntry,), {}, old_methods) == fingerprint_call(
        _dummy_fn, (NewEntry,), {}, new_methods
    )

    assert len(old_methods) == 1
    assert len(new_methods) == 1
    assert isinstance(old_methods[0], StateFnEntry)
    assert isinstance(new_methods[0], StateFnEntry)
    assert old_methods[0].call("previous") == MemoStateOutcome(
        state=("state", "OldEntry", "previous"), memo_valid=True
    )
    assert new_methods[0].call("previous") == MemoStateOutcome(
        state=("state", "NewEntry", "previous"), memo_valid=True
    )


def test_raw_class_object_accepts_uninspectable_zero_arg_staticmethod_memo_key() -> None:
    hook = _UninspectableZeroArgMemoHook()

    class OldEntry:
        __coco_memo_key__ = staticmethod(hook)

    class NewEntry:
        __coco_memo_key__ = staticmethod(hook)

    OldEntry.__module__ = "tests.old_raw_class"
    NewEntry.__module__ = "tests.new_raw_class"

    assert fingerprint_call(_dummy_fn, (OldEntry,), {}, []) == fingerprint_call(
        _dummy_fn, (NewEntry,), {}, []
    )


def test_raw_class_object_accepts_uninspectable_zero_arg_plain_memo_key() -> None:
    hook = _UninspectableZeroArgMemoHook()

    class OldEntry:
        __coco_memo_key__ = hook

    class NewEntry:
        __coco_memo_key__ = hook

    OldEntry.__module__ = "tests.old_raw_class"
    NewEntry.__module__ = "tests.new_raw_class"

    assert fingerprint_call(_dummy_fn, (OldEntry,), {}, []) == fingerprint_call(
        _dummy_fn, (NewEntry,), {}, []
    )


def test_raw_class_object_rejects_non_zero_arg_staticmethod_memo_key() -> None:
    """Reject invalid staticmethod hooks before calling class-object memo keys."""
    class Entry:
        @staticmethod
        def __coco_memo_key__(value: object) -> object:
            return ("class-hook", value)

    with pytest.raises(
        TypeError,
        match=(
            r"Entry\.__coco_memo_key__ is a staticmethod that cannot be called "
            r"with zero arguments; class-object hooks must take no arguments "
            r"after binding"
        ),
    ):
        fingerprint_call(_dummy_fn, (Entry,), {}, [])


def test_raw_class_object_rejects_non_zero_arg_classmethod_memo_key() -> None:
    """Reject invalid classmethod hooks after descriptor binding leaves arguments."""
    class Entry:
        @classmethod
        def __coco_memo_key__(cls, value: object) -> object:
            return ("class-hook", cls.__name__, value)

    with pytest.raises(
        TypeError,
        match=(
            r"Entry\.__coco_memo_key__ is a classmethod that cannot be called "
            r"with zero arguments; class-object hooks must take no arguments "
            r"after binding"
        ),
    ):
        fingerprint_call(_dummy_fn, (Entry,), {}, [])


def test_raw_class_object_ignores_instance_memo_state() -> None:
    class Entry:
        @staticmethod
        def __coco_memo_key__() -> object:
            return ("class-hook", "stable")

        def __coco_memo_state__(self, prev_state: object) -> MemoStateOutcome:
            raise AssertionError("instance memo state must not run for class objects")

    methods: list[Any] = []
    fp1 = fingerprint_call(_dummy_fn, (Entry,), {}, methods)
    fp2 = fingerprint_call(_dummy_fn, (Entry,), {}, [])

    assert fp1 == fp2
    assert methods == []


def test_raw_class_object_uses_metaclass_hook_after_ignored_instance_key() -> None:
    class MemoMeta(type):
        def __coco_memo_key__(cls) -> object:
            return ("metaclass-hook", "stable")

    class Base:
        def __coco_memo_key__(self) -> object:
            raise AssertionError("instance memo key must not run for class objects")

    class OldEntry(Base, metaclass=MemoMeta):
        pass

    class NewEntry(Base, metaclass=MemoMeta):
        pass

    OldEntry.__module__ = "tests.old_raw_class"
    NewEntry.__module__ = "tests.new_raw_class"

    assert fingerprint_call(_dummy_fn, (OldEntry,), {}, []) == fingerprint_call(
        _dummy_fn, (NewEntry,), {}, []
    )


def test_raw_class_object_stable_type_id_ignores_instance_memo_key() -> None:
    class OldEntry:
        __coco_memo_type_id__ = "test.RawClass/v1"

        def __coco_memo_key__(self) -> object:
            raise AssertionError("instance memo key must not run for class objects")

    class NewEntry:
        __coco_memo_type_id__ = "test.RawClass/v1"

        def __coco_memo_key__(self) -> object:
            raise AssertionError("instance memo key must not run for class objects")

    class ChangedEntry:
        __coco_memo_type_id__ = "test.RawClass/v2"

        def __coco_memo_key__(self) -> object:
            raise AssertionError("instance memo key must not run for class objects")

    OldEntry.__module__ = "tests.old_raw_class"
    NewEntry.__module__ = "tests.new_raw_class"
    ChangedEntry.__module__ = "tests.new_raw_class"

    assert fingerprint_call(_dummy_fn, (OldEntry,), {}, []) == fingerprint_call(
        _dummy_fn, (NewEntry,), {}, []
    )
    assert fingerprint_call(_dummy_fn, (OldEntry,), {}, []) != fingerprint_call(
        _dummy_fn, (ChangedEntry,), {}, []
    )


def test_raw_class_object_explicit_hook_owns_stable_namespace() -> None:
    """Explicit class-object hooks are custom keys, not stable-ID wrappers."""
    def shared_key() -> object:
        return ("class-hook", "shared")

    def changed_key() -> object:
        return ("class-hook", "changed")

    class OldEntry:
        __coco_memo_type_id__ = "test.RawClassHook/v1"
        __coco_memo_key__ = staticmethod(shared_key)

    class ChangedKeyEntry:
        __coco_memo_type_id__ = "test.RawClassHook/v1"
        __coco_memo_key__ = staticmethod(changed_key)

    class ChangedTypeIdEntry:
        __coco_memo_type_id__ = "test.RawClassHook/v2"
        __coco_memo_key__ = staticmethod(shared_key)

    assert fingerprint_call(_dummy_fn, (OldEntry,), {}, []) != fingerprint_call(
        _dummy_fn, (ChangedKeyEntry,), {}, []
    )
    assert fingerprint_call(_dummy_fn, (OldEntry,), {}, []) == fingerprint_call(
        _dummy_fn, (ChangedTypeIdEntry,), {}, []
    )


def test_registered_memo_type_identifier_allows_renamed_type_reuse() -> None:
    class OldEntry:
        def __init__(self, value: object) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("entry", self.value)

    class NewEntry:
        def __init__(self, value: object) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("entry", self.value)

    register_memo_type_identifier(OldEntry, "test.RegisteredEntry/v1")
    register_memo_type_identifier(NewEntry, "test.RegisteredEntry/v1")
    try:
        assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) == fingerprint_call(
            _dummy_fn, (NewEntry(1),), {}, []
        )
        assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) != fingerprint_call(
            _dummy_fn, (NewEntry(2),), {}, []
        )
    finally:
        _unregister_memo_type_identifier(OldEntry)
        _unregister_memo_type_identifier(NewEntry)


def test_registered_memo_key_function_uses_registered_base_type_identity() -> None:
    class Base:
        def __init__(self, value: object) -> None:
            self.value = value

    class ChildA(Base):
        pass

    class ChildB(Base):
        pass

    register_memo_key_function(Base, lambda entry: ("base", entry.value))
    register_memo_type_identifier(Base, "test.RegisteredBase/v1")
    try:
        assert fingerprint_call(_dummy_fn, (ChildA(1),), {}, []) == fingerprint_call(
            _dummy_fn, (ChildB(1),), {}, []
        )
        assert fingerprint_call(_dummy_fn, (ChildA(1),), {}, []) != fingerprint_call(
            _dummy_fn, (ChildB(2),), {}, []
        )
    finally:
        unregister_memo_key_function(Base)
        _unregister_memo_type_identifier(Base)


def test_registered_memo_type_identifier_is_identity_exact_for_equal_metaclasses() -> (
    None
):
    class EqMeta(type):
        def __eq__(cls, other: object) -> bool:
            return isinstance(other, EqMeta)

        def __hash__(cls) -> int:
            return 1

    class A(metaclass=EqMeta):
        def __coco_memo_key__(self) -> object:
            return ("same",)

    class B(metaclass=EqMeta):
        def __coco_memo_key__(self) -> object:
            return ("same",)

    assert fingerprint_call(_dummy_fn, (A(),), {}, []) != fingerprint_call(
        _dummy_fn, (B(),), {}, []
    )

    register_memo_type_identifier(A, "test.EqualityMetaA/v1")
    try:
        assert fingerprint_call(_dummy_fn, (A(),), {}, []) != fingerprint_call(
            _dummy_fn, (B(),), {}, []
        )
    finally:
        _unregister_memo_type_identifier(A)


def test_stable_type_id_exact_type_and_validation() -> None:
    class Parent:
        __coco_memo_type_id__ = "test.Parent/v1"

        def __coco_memo_key__(self) -> object:
            return ("same", 1)

    class Child(Parent):
        pass

    class BadObjectId:
        __coco_memo_type_id__ = object()

        def __coco_memo_key__(self) -> object:
            return ("bad", 1)

    class EmptyId:
        __coco_memo_type_id__ = ""

        def __coco_memo_key__(self) -> object:
            return ("bad", 1)

    assert fingerprint_call(_dummy_fn, (Parent(),), {}, []) != fingerprint_call(
        _dummy_fn, (Child(),), {}, []
    )
    with pytest.raises(TypeError, match="must be a str"):
        fingerprint_call(_dummy_fn, (BadObjectId(),), {}, [])
    with pytest.raises(ValueError, match="non-empty"):
        fingerprint_call(_dummy_fn, (EmptyId(),), {}, [])


def test_register_memo_type_identifier_validation_and_export() -> None:
    import cocoindex as coco

    class Entry:
        pass

    assert coco.register_memo_type_identifier is register_memo_type_identifier
    with pytest.raises(TypeError, match="expects typ to be a type"):
        register_memo_type_identifier(cast(Any, object()), "test.Invalid/v1")
    with pytest.raises(TypeError, match="must be a str"):
        register_memo_type_identifier(Entry, cast(Any, object()))
    with pytest.raises(ValueError, match="non-empty"):
        register_memo_type_identifier(Entry, "")
    with pytest.raises(ValueError, match="non-empty"):
        register_memo_type_identifier(Entry, "   ")


def test_cycles_are_supported_and_deterministic() -> None:
    # Self-cycle list
    a: Any = []
    a.append(a)
    b: Any = []
    b.append(b)
    assert fingerprint_call(_dummy_fn, (a,), {}, []) == fingerprint_call(
        _dummy_fn, (b,), {}, []
    )

    # Self-cycle dict
    d1: dict[str, object] = {}
    d1["self"] = d1
    d2: dict[str, object] = {}
    d2["self"] = d2
    assert fingerprint_call(_dummy_fn, (d1,), {}, []) == fingerprint_call(
        _dummy_fn, (d2,), {}, []
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
    assert fingerprint_call(_dummy_fn, (x1,), {}, []) == fingerprint_call(
        _dummy_fn, (x2,), {}, []
    )


def test_pickle_fallback_for_unsupported_objects() -> None:
    # complex is not one of the supported primitives/containers, but is picklable.
    assert fingerprint_call(_dummy_fn, (complex(1, 2),), {}, []) == fingerprint_call(
        _dummy_fn, (complex(1, 2),), {}, []
    )

    # A simple user-defined type (module-level) should also work via pickle fallback.
    assert fingerprint_call(_dummy_fn, (_PickleableZ(),), {}, []) == fingerprint_call(
        _dummy_fn, (_PickleableZ(),), {}, []
    )

    # Unpicklable payloads should raise the original TypeError.
    try:
        fingerprint_call(_dummy_fn, (lambda x: x,), {}, [])
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
    assert fingerprint_call(_dummy_fn, (p1,), {}, []) == fingerprint_call(
        _dummy_fn, (p2,), {}, []
    )

    # Different values -> different fingerprint
    assert fingerprint_call(_dummy_fn, (p1,), {}, []) != fingerprint_call(
        _dummy_fn, (p3,), {}, []
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
    assert fingerprint_call(_dummy_fn, (p1,), {}, []) != fingerprint_call(
        _dummy_fn, (p2,), {}, []
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
    assert fingerprint_call(_dummy_fn, (o1,), {}, []) == fingerprint_call(
        _dummy_fn, (o2,), {}, []
    )

    # Different nested values -> different fingerprint
    assert fingerprint_call(_dummy_fn, (o1,), {}, []) != fingerprint_call(
        _dummy_fn, (o3,), {}, []
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
    assert fingerprint_call(_dummy_fn, (a,), {}, []) != fingerprint_call(
        _dummy_fn, (b,), {}, []
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
    assert fingerprint_call(_dummy_fn, (w1,), {}, []) == fingerprint_call(
        _dummy_fn, (w2,), {}, []
    )

    # Different memo-key-relevant data -> different fingerprint
    assert fingerprint_call(_dummy_fn, (w1,), {}, []) != fingerprint_call(
        _dummy_fn, (w3,), {}, []
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
    assert fingerprint_call(_dummy_fn, (p1,), {}, []) == fingerprint_call(
        _dummy_fn, (p2,), {}, []
    )

    # Different values -> different fingerprint
    assert fingerprint_call(_dummy_fn, (p1,), {}, []) != fingerprint_call(
        _dummy_fn, (p3,), {}, []
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
    assert fingerprint_call(_dummy_fn, (c1,), {}, []) == fingerprint_call(
        _dummy_fn, (c2,), {}, []
    )

    # Different values -> different fingerprint
    assert fingerprint_call(_dummy_fn, (c1,), {}, []) != fingerprint_call(
        _dummy_fn, (c3,), {}, []
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
    assert fingerprint_call(_dummy_fn, (o1,), {}, []) == fingerprint_call(
        _dummy_fn, (o2,), {}, []
    )

    # Different nested values -> different fingerprint
    assert fingerprint_call(_dummy_fn, (o1,), {}, []) != fingerprint_call(
        _dummy_fn, (o3,), {}, []
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
    assert fingerprint_call(_dummy_fn, (a,), {}, []) != fingerprint_call(
        _dummy_fn, (b,), {}, []
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
    assert fingerprint_call(_dummy_fn, (w1,), {}, []) == fingerprint_call(
        _dummy_fn, (w2,), {}, []
    )

    # Different memo-key-relevant data -> different fingerprint
    assert fingerprint_call(_dummy_fn, (w1,), {}, []) != fingerprint_call(
        _dummy_fn, (w3,), {}, []
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
    assert fingerprint_call(_dummy_fn, (d,), {}, []) != fingerprint_call(
        _dummy_fn, (p,), {}, []
    )


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

        def __coco_memo_state__(self, prev_state: object) -> MemoStateOutcome:
            return MemoStateOutcome(state=prev_state, memo_valid=True)

    fp_hook = fingerprint_call(_dummy_fn, (HookOnly(42),), {}, [])
    fp_shook = fingerprint_call(_dummy_fn, (HookAndState(42),), {}, [])
    # "shook" tag intentionally changes fingerprint to force re-execution
    assert fp_hook != fp_shook


def test_shook_fingerprint_is_deterministic() -> None:
    class Stateful:
        def __init__(self, v: object) -> None:
            self.v = v

        def __coco_memo_key__(self) -> object:
            return self.v

        def __coco_memo_state__(self, prev_state: object) -> MemoStateOutcome:
            return MemoStateOutcome(state=prev_state, memo_valid=True)

    fp1 = fingerprint_call(_dummy_fn, (Stateful(99),), {}, [])
    fp2 = fingerprint_call(_dummy_fn, (Stateful(99),), {}, [])
    assert fp1 == fp2


def test_state_methods_collected_from_hook() -> None:
    class WithState:
        def __init__(self, v: object) -> None:
            self.v = v

        def __coco_memo_key__(self) -> object:
            return self.v

        def __coco_memo_state__(self, prev_state: object) -> MemoStateOutcome:
            return MemoStateOutcome(state=prev_state, memo_valid=True)

    from cocoindex._internal.memo_fingerprint import StateFnEntry

    methods: list[Any] = []
    fingerprint_call(_dummy_fn, (WithState(1),), {}, state_methods=methods)
    assert len(methods) == 1
    assert isinstance(methods[0], StateFnEntry)
    assert callable(methods[0].call)
    assert callable(methods[0].deserialize_prev)


def test_state_methods_collected_from_registry() -> None:
    from cocoindex._internal.memo_fingerprint import StateFnEntry

    class Registered:
        def __init__(self, v: object) -> None:
            self.v = v

    def _state_fn(obj: Any, prev: Any) -> MemoStateOutcome:
        return MemoStateOutcome(state=prev, memo_valid=True)

    register_memo_key_function(Registered, lambda r: r.v, state_fn=_state_fn)
    try:
        methods: list[Any] = []
        fingerprint_call(_dummy_fn, (Registered(7),), {}, state_methods=methods)
        assert len(methods) == 1
        assert isinstance(methods[0], StateFnEntry)
        assert callable(methods[0].call)
    finally:
        unregister_memo_key_function(Registered)


def test_multiple_state_methods_collected_in_order() -> None:
    class S1:
        def __coco_memo_key__(self) -> object:
            return "s1"

        def __coco_memo_state__(self, prev: object) -> MemoStateOutcome:
            return MemoStateOutcome(state="from_s1", memo_valid=True)

    class S2:
        def __coco_memo_key__(self) -> object:
            return "s2"

        def __coco_memo_state__(self, prev: object) -> MemoStateOutcome:
            return MemoStateOutcome(state="from_s2", memo_valid=True)

    methods: list[Any] = []
    fingerprint_call(_dummy_fn, (S1(), S2()), {}, state_methods=methods)
    assert len(methods) == 2
    # Verify order: S1 first (first arg), S2 second
    assert methods[0].call("x").state == "from_s1"
    assert methods[1].call("x").state == "from_s2"


def test_no_state_methods_for_hook_only_objects() -> None:
    class HookOnly:
        def __coco_memo_key__(self) -> object:
            return "key"

    methods: list[Any] = []
    fingerprint_call(_dummy_fn, (HookOnly(),), {}, state_methods=methods)
    assert len(methods) == 0


def test_state_methods_collected_through_dataclass_field() -> None:
    from cocoindex._internal.memo_fingerprint import StateFnEntry

    class WithState:
        def __init__(self, v: object) -> None:
            self.v = v

        def __coco_memo_key__(self) -> object:
            return self.v

        def __coco_memo_state__(self, prev_state: object) -> MemoStateOutcome:
            return MemoStateOutcome(state=prev_state, memo_valid=True)

    @dataclasses.dataclass
    class Outer:
        inner: WithState

    methods: list[Any] = []
    fingerprint_call(_dummy_fn, (Outer(WithState(42)),), {}, state_methods=methods)
    assert len(methods) == 1
    assert isinstance(methods[0], StateFnEntry)


def test_state_methods_collected_through_pydantic_field() -> None:
    try:
        from pydantic import BaseModel, ConfigDict
    except ImportError:
        pytest.skip("pydantic not installed")
        return
    from cocoindex._internal.memo_fingerprint import StateFnEntry

    class WithState:
        def __init__(self, v: object) -> None:
            self.v = v

        def __coco_memo_key__(self) -> object:
            return self.v

        def __coco_memo_state__(self, prev_state: object) -> MemoStateOutcome:
            return MemoStateOutcome(state=prev_state, memo_valid=True)

    class Outer(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)
        inner: WithState

    methods: list[Any] = []
    fingerprint_call(
        _dummy_fn, (Outer(inner=WithState(42)),), {}, state_methods=methods
    )
    assert len(methods) == 1
    assert isinstance(methods[0], StateFnEntry)


def _named_dummy_fn(n: int, s: str, p: Any) -> None:
    raise RuntimeError("not called")


def test_apply_memo_key_can_ignore_parameter() -> None:
    def foo(a: int, b: int) -> None:
        return None

    compiled = _normalize_memo_key(foo, {"b": None})
    args1, kwargs1 = _apply_memo_key((1, 2), {}, compiled)
    args2, kwargs2 = _apply_memo_key((1, 3), {}, compiled)

    fp1 = fingerprint_call(foo, args1, kwargs1, [])
    fp2 = fingerprint_call(foo, args2, kwargs2, [])
    assert fp1 == fp2


def test_apply_memo_key_can_transform_parameter() -> None:
    def foo(a: int, b: int) -> None:
        return None

    compiled = _normalize_memo_key(foo, {"b": lambda x: x // 10})
    args1, kwargs1 = _apply_memo_key((1, 20), {}, compiled)
    args2, kwargs2 = _apply_memo_key((1, 29), {}, compiled)

    fp1 = fingerprint_call(foo, args1, kwargs1, [])
    fp2 = fingerprint_call(foo, args2, kwargs2, [])
    assert fp1 == fp2


def test_apply_memo_key_preserves_default_fingerprint_shape_for_missing_optional_arg() -> (
    None
):
    def foo(a: int, b: int = 10) -> None:
        return None

    fp_default = fingerprint_call(foo, (1,), {}, [])

    compiled = _normalize_memo_key(foo, {"b": lambda x: x + 1})
    args, kwargs = _apply_memo_key((1,), {}, compiled)
    fp_with_memo_key = fingerprint_call(foo, args, kwargs, [])

    assert fp_with_memo_key == fp_default


def test_fingerprint_call_prefix_args_affect_fingerprint() -> None:
    fp_a = fingerprint_call(
        _named_dummy_fn,
        (1, "same", {"x": 1}),
        {},
        [],
        prefix_args=("path/a",),
    )
    fp_b = fingerprint_call(
        _named_dummy_fn,
        (1, "same", {"x": 1}),
        {},
        [],
        prefix_args=("path/b",),
    )
    assert fp_a != fp_b


def test_apply_memo_key_with_keyword_only_parameters() -> None:
    def foo(a: int, *, b: int, c: int = 1) -> None:
        return None

    compiled = _normalize_memo_key(foo, {"b": None, "c": lambda x: x + 1})
    args, kwargs = _apply_memo_key((1,), {"b": 2, "c": 3}, compiled)

    assert args == (1,)
    assert kwargs == {"c": 4}
    assert "b" not in kwargs


def test_apply_memo_key_excludes_varargs() -> None:
    def foo(a: int, *args: int) -> None:
        return None

    compiled = _normalize_memo_key(foo, {"args": None})
    result_args, result_kwargs = _apply_memo_key((1, 2, 3), {}, compiled)

    assert result_args == (1,)
    assert result_kwargs == {}


def test_apply_memo_key_transforms_varargs() -> None:
    def foo(a: int, *args: int) -> None:
        return None

    compiled = _normalize_memo_key(foo, {"args": lambda x: (len(x),)})
    result_args, result_kwargs = _apply_memo_key((1, 2, 3), {}, compiled)

    assert result_args == (1, 2)
    assert result_kwargs == {}


def test_apply_memo_key_excludes_varkw() -> None:
    def foo(a: int, **kwargs: int) -> None:
        return None

    compiled = _normalize_memo_key(foo, {"kwargs": None})
    result_args, result_kwargs = _apply_memo_key((1,), {"x": 2, "y": 3}, compiled)

    assert result_args == (1,)
    assert result_kwargs == {}


def test_apply_memo_key_transforms_varkw() -> None:
    def foo(a: int, **kwargs: int) -> None:
        return None

    compiled = _normalize_memo_key(foo, {"kwargs": lambda x: {"count": len(x)}})
    result_args, result_kwargs = _apply_memo_key((1,), {"x": 2, "y": 3}, compiled)

    assert result_args == (1,)
    assert result_kwargs == {"count": 2}
