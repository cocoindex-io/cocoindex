import dataclasses
import math
import weakref
from typing import Any, ClassVar, cast

import pytest

from cocoindex._internal import memo_fingerprint as _memo_fingerprint
from cocoindex._internal.function import _apply_memo_key, _normalize_memo_key
from cocoindex._internal.memo_fingerprint import (
    fingerprint_call,
    register_memo_key_function,
    register_not_memo_keyable,
    unregister_memo_key_function,
)
from cocoindex._internal.typing import MemoStateOutcome


class _PickleableZ:
    pass


def _dummy_fn(*args: Any, **kwargs: Any) -> None:
    raise RuntimeError("not called")


def _canonical_contains(value: object, needle: object) -> bool:
    if value == needle:
        return True
    if isinstance(value, tuple):
        return any(_canonical_contains(item, needle) for item in value)
    return False


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

    try:
        register_memo_key_function(Y, lambda y: ("y", y.v))
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

    try:
        register_memo_key_function(C, lambda x: ("same", x.v))
        register_memo_key_function(D, lambda x: ("same", x.v))
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


def test_raw_class_object_stable_type_id_never_calls_staticmethod_memo_key() -> None:
    class OldEntry:
        __coco_memo_type_id__ = "test.RawClass/v1"

        @staticmethod
        def __coco_memo_key__() -> object:
            raise AssertionError("class-object fingerprint must not call memo key")

    class NewEntry:
        __coco_memo_type_id__ = "test.RawClass/v1"

        @staticmethod
        def __coco_memo_key__() -> object:
            raise AssertionError("class-object fingerprint must not call memo key")

    class ChangedEntry:
        __coco_memo_type_id__ = "test.RawClass/v2"

        @staticmethod
        def __coco_memo_key__() -> object:
            raise AssertionError("class-object fingerprint must not call memo key")

    OldEntry.__module__ = "tests.old_raw_class"
    NewEntry.__module__ = "tests.new_raw_class"
    ChangedEntry.__module__ = "tests.new_raw_class"

    assert fingerprint_call(_dummy_fn, (OldEntry,), {}, []) == fingerprint_call(
        _dummy_fn, (NewEntry,), {}, []
    )
    assert fingerprint_call(_dummy_fn, (OldEntry,), {}, []) != fingerprint_call(
        _dummy_fn, (ChangedEntry,), {}, []
    )


def test_registered_stable_type_id_applies_to_class_objects() -> None:
    class OldEntry:
        pass

    class NewEntry:
        pass

    class ChangedEntry:
        pass

    try:
        register_memo_key_function(
            OldEntry, stable_type_id="test.RegisteredRawClass/v1"
        )
        register_memo_key_function(
            NewEntry, stable_type_id="test.RegisteredRawClass/v1"
        )
        register_memo_key_function(
            ChangedEntry, stable_type_id="test.RegisteredRawClass/v2"
        )

        assert fingerprint_call(_dummy_fn, (OldEntry,), {}, []) == fingerprint_call(
            _dummy_fn, (NewEntry,), {}, []
        )
        assert fingerprint_call(_dummy_fn, (OldEntry,), {}, []) != fingerprint_call(
            _dummy_fn, (ChangedEntry,), {}, []
        )
    finally:
        unregister_memo_key_function(OldEntry)
        unregister_memo_key_function(NewEntry)
        unregister_memo_key_function(ChangedEntry)


def test_hook_memo_key_fragment_preserves_parent_cycle() -> None:
    class Entry:
        def __init__(self, parent: list[object]) -> None:
            self.parent = parent

        def __coco_memo_key__(self) -> object:
            return self.parent

    def make_graph() -> list[object]:
        parent: list[object] = []
        parent.append(Entry(parent))
        return parent

    graph = make_graph()
    canonical = _memo_fingerprint._canonicalize(graph, None, [])

    assert canonical == (
        "seq",
        (("hook", *_memo_fingerprint._type_identity_parts(Entry), ("ref", 0)),),
    )
    assert _memo_fingerprint.memo_fingerprint(
        graph
    ) == _memo_fingerprint.memo_fingerprint(make_graph())


def test_registered_memo_key_fragment_preserves_parent_cycle() -> None:
    class Entry:
        def __init__(self, parent: list[object]) -> None:
            self.parent = parent

    def make_graph() -> list[object]:
        parent: list[object] = []
        parent.append(Entry(parent))
        return parent

    try:
        register_memo_key_function(Entry, lambda entry: entry.parent)
        graph = make_graph()
        canonical = _memo_fingerprint._canonicalize(graph, None, [])

        assert canonical == (
            "seq",
            (("hook", *_memo_fingerprint._type_identity_parts(Entry), ("ref", 0)),),
        )
        assert _memo_fingerprint.memo_fingerprint(
            graph
        ) == _memo_fingerprint.memo_fingerprint(make_graph())
    finally:
        unregister_memo_key_function(Entry)


def test_hook_memo_key_fragments_remain_alive_for_root_traversal() -> None:
    class Fragment(list[object]):
        pass

    first_fragment_ref: weakref.ReferenceType[Fragment] | None = None

    class FirstEntry:
        def __coco_memo_key__(self) -> object:
            nonlocal first_fragment_ref
            fragment = Fragment(["first"])
            first_fragment_ref = weakref.ref(fragment)
            return fragment

    class SecondEntry:
        def __coco_memo_key__(self) -> object:
            assert first_fragment_ref is not None
            assert first_fragment_ref() is not None
            return Fragment(["second"])

    canonical = _memo_fingerprint._canonicalize([FirstEntry(), SecondEntry()], None, [])

    assert canonical == (
        "seq",
        (
            (
                "hook",
                *_memo_fingerprint._type_identity_parts(FirstEntry),
                ("seq", ("first",)),
            ),
            (
                "hook",
                *_memo_fingerprint._type_identity_parts(SecondEntry),
                ("seq", ("second",)),
            ),
        ),
    )


def test_memo_key_fragment_preserves_shared_reference_ordinals() -> None:
    class Entry:
        def __init__(self) -> None:
            self.shared = ["shared"]

        def __coco_memo_key__(self) -> object:
            return (self.shared, self.shared)

    top_level = _memo_fingerprint._canonicalize(Entry(), None, [])
    parent_wrapped = _memo_fingerprint._canonicalize([Entry()], None, [])

    assert top_level == (
        "hook",
        *_memo_fingerprint._type_identity_parts(Entry),
        ("seq", (("seq", ("shared",)), ("ref", 1))),
    )
    assert parent_wrapped == (
        "seq",
        (
            (
                "hook",
                *_memo_fingerprint._type_identity_parts(Entry),
                ("seq", (("seq", ("shared",)), ("ref", 2))),
            ),
        ),
    )


def test_raw_class_object_default_identity_never_calls_classmethod_memo_key() -> None:
    class Entry:
        @classmethod
        def __coco_memo_key__(cls) -> object:
            raise AssertionError("class-object fingerprint must not call memo key")

    class OtherEntry:
        @classmethod
        def __coco_memo_key__(cls) -> object:
            raise AssertionError("class-object fingerprint must not call memo key")

    Entry.__module__ = "tests.raw_class_default"
    OtherEntry.__module__ = "tests.raw_class_default"

    assert fingerprint_call(_dummy_fn, (Entry,), {}, []) == fingerprint_call(
        _dummy_fn, (Entry,), {}, []
    )
    assert fingerprint_call(_dummy_fn, (Entry,), {}, []) != fingerprint_call(
        _dummy_fn, (OtherEntry,), {}, []
    )


def test_raw_class_object_honors_registered_metaclass_memo_key_and_state() -> None:
    key_calls: list[type] = []
    state_calls: list[tuple[type, object]] = []

    class MemoMeta(type):
        def __coco_memo_key__(cls) -> object:
            raise AssertionError("raw classes must not call metaclass memo attributes")

        def __coco_memo_state__(cls, prev_state: object) -> MemoStateOutcome:
            raise AssertionError("raw classes must not call metaclass memo attributes")

    class OldEntry(metaclass=MemoMeta):
        pass

    class NewEntry(metaclass=MemoMeta):
        pass

    def metaclass_key(cls: type) -> object:
        key_calls.append(cls)
        return ("metaclass", cls.__name__)

    def metaclass_state(cls: type, prev_state: object) -> MemoStateOutcome:
        state_calls.append((cls, prev_state))
        return MemoStateOutcome(
            state=(cls.__name__, prev_state), memo_valid=prev_state == "reusable"
        )

    stable_type_id = "test.RawClassRegisteredMetaOwner/v1"
    try:
        register_memo_key_function(
            MemoMeta,
            metaclass_key,
            state_fn=metaclass_state,
            stable_type_id=stable_type_id,
        )
        old_state_methods: list[Any] = []
        new_state_methods: list[Any] = []
        old_canonical = _memo_fingerprint._canonicalize(
            OldEntry, None, old_state_methods
        )
        new_canonical = _memo_fingerprint._canonicalize(
            NewEntry, None, new_state_methods
        )

        assert old_canonical == (
            "shook",
            ("__coco_memo_type_id__", stable_type_id),
            None,
            ("seq", ("metaclass", OldEntry.__name__)),
        )
        assert new_canonical == (
            "shook",
            ("__coco_memo_type_id__", stable_type_id),
            None,
            ("seq", ("metaclass", NewEntry.__name__)),
        )
        assert key_calls == [OldEntry, NewEntry]
        assert len(old_state_methods) == 1
        assert len(new_state_methods) == 1
        assert old_state_methods[0].call("old previous") == MemoStateOutcome(
            state=(OldEntry.__name__, "old previous"), memo_valid=False
        )
        assert new_state_methods[0].call("reusable") == MemoStateOutcome(
            state=(NewEntry.__name__, "reusable"), memo_valid=True
        )
        assert state_calls == [
            (OldEntry, "old previous"),
            (NewEntry, "reusable"),
        ]
    finally:
        unregister_memo_key_function(MemoMeta)


def test_raw_class_object_honors_registered_type_memo_key_and_state() -> None:
    key_calls: list[type] = []
    state_calls: list[tuple[type, object]] = []

    class MemoAttributesMustNotRun:
        @classmethod
        def __coco_memo_key__(cls) -> object:
            raise AssertionError("raw classes must not call class memo attributes")

        @classmethod
        def __coco_memo_state__(cls, prev_state: object) -> MemoStateOutcome:
            raise AssertionError("raw classes must not call class memo attributes")

    class OldEntry(MemoAttributesMustNotRun):
        pass

    class NewEntry(MemoAttributesMustNotRun):
        pass

    def type_key(cls: type) -> object:
        key_calls.append(cls)
        return ("type", cls.__name__)

    def type_state(cls: type, prev_state: object) -> MemoStateOutcome:
        state_calls.append((cls, prev_state))
        return MemoStateOutcome(
            state=(cls.__name__, prev_state), memo_valid=prev_state == "reusable"
        )

    stable_type_id = "test.RawClassRegisteredTypeOwner/v1"
    try:
        register_memo_key_function(
            type,
            type_key,
            state_fn=type_state,
            stable_type_id=stable_type_id,
        )
        old_state_methods: list[Any] = []
        new_state_methods: list[Any] = []
        old_canonical = _memo_fingerprint._canonicalize(
            OldEntry, None, old_state_methods
        )
        new_canonical = _memo_fingerprint._canonicalize(
            NewEntry, None, new_state_methods
        )

        assert old_canonical == (
            "shook",
            ("__coco_memo_type_id__", stable_type_id),
            None,
            ("seq", ("type", OldEntry.__name__)),
        )
        assert new_canonical == (
            "shook",
            ("__coco_memo_type_id__", stable_type_id),
            None,
            ("seq", ("type", NewEntry.__name__)),
        )
        assert key_calls == [OldEntry, NewEntry]
        assert len(old_state_methods) == 1
        assert len(new_state_methods) == 1
        assert old_state_methods[0].call("old previous") == MemoStateOutcome(
            state=(OldEntry.__name__, "old previous"), memo_valid=False
        )
        assert new_state_methods[0].call("reusable") == MemoStateOutcome(
            state=(NewEntry.__name__, "reusable"), memo_valid=True
        )
        assert state_calls == [
            (OldEntry, "old previous"),
            (NewEntry, "reusable"),
        ]
    finally:
        unregister_memo_key_function(type)


def test_raw_class_object_ignores_registered_object_memo_key() -> None:
    object_key_calls: list[object] = []

    class Entry:
        pass

    expected = fingerprint_call(_dummy_fn, (Entry,), {}, [])

    def object_key(obj: object) -> object:
        object_key_calls.append(obj)
        return "object memo key ran"

    try:
        register_memo_key_function(object, object_key)
        assert fingerprint_call(_dummy_fn, (Entry,), {}, []) == expected
        assert object_key_calls == []
    finally:
        unregister_memo_key_function(object)


def test_raw_class_object_ignores_registered_type_stable_type_id() -> None:
    class Entry:
        pass

    Entry.__module__ = "tests.raw_class_registered_type_stable_id"
    original = fingerprint_call(_dummy_fn, (Entry,), {}, [])

    try:
        register_memo_key_function(type, stable_type_id="test.RegisteredType/v1")
        assert fingerprint_call(_dummy_fn, (Entry,), {}, []) == original
    finally:
        unregister_memo_key_function(type)


def test_raw_class_object_stable_type_id_is_exact_type() -> None:
    class Parent:
        __coco_memo_type_id__ = "test.RawClassExact/v1"

    class Child(Parent):
        pass

    assert fingerprint_call(_dummy_fn, (Parent,), {}, []) != fingerprint_call(
        _dummy_fn, (Child,), {}, []
    )


def test_register_memo_key_function_registers_key_function_and_stable_type_id_for_owner_base() -> (
    None
):
    class Base:
        def __init__(self, value: object) -> None:
            self.value = value

    class ChildA(Base):
        pass

    class ChildB(Base):
        pass

    try:
        register_memo_key_function(
            Base,
            lambda entry: ("base", entry.value),
            stable_type_id="test.RegisteredBase/v1",
        )
        assert fingerprint_call(_dummy_fn, (ChildA(1),), {}, []) == fingerprint_call(
            _dummy_fn, (ChildB(1),), {}, []
        )
        assert fingerprint_call(_dummy_fn, (ChildA(1),), {}, []) != fingerprint_call(
            _dummy_fn, (ChildB(2),), {}, []
        )
    finally:
        unregister_memo_key_function(Base)


def test_register_memo_key_function_registers_stable_type_id_without_key_function() -> (
    None
):
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

    try:
        register_memo_key_function(OldEntry, stable_type_id="test.RegisteredEntry/v1")
        register_memo_key_function(NewEntry, stable_type_id="test.RegisteredEntry/v1")
        assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) == fingerprint_call(
            _dummy_fn, (NewEntry(1),), {}, []
        )
        assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) != fingerprint_call(
            _dummy_fn, (NewEntry(2),), {}, []
        )
    finally:
        unregister_memo_key_function(OldEntry)
        unregister_memo_key_function(NewEntry)


def test_stable_type_id_only_registration_is_exact_for_subclasses() -> None:
    class Parent:
        def __init__(self, value: object) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("entry", self.value)

    class Child(Parent):
        pass

    class SameStableTypeId:
        def __init__(self, value: object) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("entry", self.value)

    try:
        register_memo_key_function(
            Parent, stable_type_id="test.RegisteredExactParent/v1"
        )
        register_memo_key_function(
            SameStableTypeId, stable_type_id="test.RegisteredExactParent/v1"
        )

        assert fingerprint_call(_dummy_fn, (Parent(1),), {}, []) == fingerprint_call(
            _dummy_fn, (SameStableTypeId(1),), {}, []
        )
        assert fingerprint_call(_dummy_fn, (Parent(1),), {}, []) != fingerprint_call(
            _dummy_fn, (Child(1),), {}, []
        )
        assert fingerprint_call(_dummy_fn, (Parent,), {}, []) != fingerprint_call(
            _dummy_fn, (Child,), {}, []
        )
    finally:
        unregister_memo_key_function(Parent)
        unregister_memo_key_function(SameStableTypeId)


def test_stable_type_id_only_registration_replaces_key_and_state_functions() -> None:
    @dataclasses.dataclass
    class Entry:
        value: int
        marker: str

    def state_fn(obj: Entry, prev_state: object) -> MemoStateOutcome:
        return MemoStateOutcome(state=prev_state, memo_valid=True)

    try:
        register_memo_key_function(
            Entry,
            lambda entry: ("constant",),
            state_fn=state_fn,
        )
        methods: list[Any] = []
        assert fingerprint_call(_dummy_fn, (Entry(1, "a"),), {}, methods) == (
            fingerprint_call(_dummy_fn, (Entry(2, "b"),), {}, [])
        )
        assert len(methods) == 1

        register_memo_key_function(
            Entry,
            stable_type_id="test.ReplaceKeyWithStable/v1",
        )
        methods = []
        assert fingerprint_call(_dummy_fn, (Entry(1, "a"),), {}, methods) != (
            fingerprint_call(_dummy_fn, (Entry(2, "b"),), {}, [])
        )
        assert methods == []
    finally:
        unregister_memo_key_function(Entry)


def test_key_only_registration_replaces_stable_type_id() -> None:
    class Entry:
        def __init__(self, value: object) -> None:
            self.value = value

    class SameStableTypeId:
        def __init__(self, value: object) -> None:
            self.value = value

    try:
        register_memo_key_function(Entry, stable_type_id="test.ReplaceStableWithKey/v1")
        register_memo_key_function(
            SameStableTypeId,
            lambda entry: ("entry", entry.value),
            stable_type_id="test.ReplaceStableWithKey/v1",
        )

        register_memo_key_function(Entry, lambda entry: ("entry", entry.value))
        assert fingerprint_call(_dummy_fn, (Entry(1),), {}, []) != fingerprint_call(
            _dummy_fn, (SameStableTypeId(1),), {}, []
        )
    finally:
        unregister_memo_key_function(Entry)
        unregister_memo_key_function(SameStableTypeId)


def test_key_only_registration_falls_back_to_class_declared_stable_type_id() -> None:
    class Entry:
        __coco_memo_type_id__ = "test.DeclaredFallback/v1"

        def __init__(self, value: object) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("entry", self.value)

    class DeclaredPeer:
        __coco_memo_type_id__ = "test.DeclaredFallback/v1"

        def __init__(self, value: object) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("entry", self.value)

    class RegisteredPeer:
        def __init__(self, value: object) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("entry", self.value)

    assert fingerprint_call(_dummy_fn, (Entry(1),), {}, []) == fingerprint_call(
        _dummy_fn, (DeclaredPeer(1),), {}, []
    )

    try:
        register_memo_key_function(Entry, stable_type_id="test.RegisteredOverride/v1")
        register_memo_key_function(
            RegisteredPeer, stable_type_id="test.RegisteredOverride/v1"
        )

        assert fingerprint_call(_dummy_fn, (Entry(1),), {}, []) == fingerprint_call(
            _dummy_fn, (RegisteredPeer(1),), {}, []
        )
        assert fingerprint_call(_dummy_fn, (Entry(1),), {}, []) != fingerprint_call(
            _dummy_fn, (DeclaredPeer(1),), {}, []
        )

        register_memo_key_function(Entry, lambda entry: ("entry", entry.value))
        assert fingerprint_call(_dummy_fn, (Entry(1),), {}, []) == fingerprint_call(
            _dummy_fn, (DeclaredPeer(1),), {}, []
        )
    finally:
        unregister_memo_key_function(Entry)
        unregister_memo_key_function(RegisteredPeer)


def test_combined_registration_uses_stable_type_id_and_collects_state_fn() -> None:
    class OldEntry:
        def __init__(self, value: object) -> None:
            self.value = value

    class NewEntry:
        def __init__(self, value: object) -> None:
            self.value = value

    def key_fn(entry: Any) -> object:
        return ("entry", entry.value)

    def state_fn(entry: Any, prev_state: object) -> MemoStateOutcome:
        return MemoStateOutcome(
            state=("state", entry.value, prev_state), memo_valid=True
        )

    try:
        register_memo_key_function(
            OldEntry,
            key_fn,
            state_fn=state_fn,
            stable_type_id="test.CombinedStateStable/v1",
        )
        register_memo_key_function(
            NewEntry,
            key_fn,
            state_fn=state_fn,
            stable_type_id="test.CombinedStateStable/v1",
        )

        assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) == (
            fingerprint_call(_dummy_fn, (NewEntry(1),), {}, [])
        )
        assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) != (
            fingerprint_call(_dummy_fn, (NewEntry(2),), {}, [])
        )
        methods: list[Any] = []
        fingerprint_call(_dummy_fn, (OldEntry(1),), {}, methods)
        assert len(methods) == 1
        assert methods[0].call("prev").state == ("state", 1, "prev")
    finally:
        unregister_memo_key_function(OldEntry)
        unregister_memo_key_function(NewEntry)


def test_register_memo_key_function_full_registration_replaces_previous_full_registration() -> (
    None
):
    class Entry:
        def __init__(self, value: object, marker: object) -> None:
            self.value = value
            self.marker = marker

    def old_state_fn(entry: Any, prev_state: object) -> MemoStateOutcome:
        return MemoStateOutcome(state=("old", entry.value, prev_state), memo_valid=True)

    def new_state_fn(entry: Any, prev_state: object) -> MemoStateOutcome:
        return MemoStateOutcome(
            state=("new", entry.marker, prev_state), memo_valid=True
        )

    try:
        register_memo_key_function(
            Entry,
            lambda entry: ("old", entry.value),
            state_fn=old_state_fn,
            stable_type_id="test.ReplaceFull/old",
        )
        first_fingerprint = fingerprint_call(_dummy_fn, (Entry(1, "a"),), {}, [])
        assert first_fingerprint == fingerprint_call(
            _dummy_fn, (Entry(1, "b"),), {}, []
        )
        assert first_fingerprint != fingerprint_call(
            _dummy_fn, (Entry(2, "a"),), {}, []
        )
        methods: list[Any] = []
        fingerprint_call(_dummy_fn, (Entry(1, "a"),), {}, methods)
        assert len(methods) == 1
        assert methods[0].call("prev").state == ("old", 1, "prev")

        register_memo_key_function(
            Entry,
            lambda entry: ("new", entry.marker),
            state_fn=new_state_fn,
            stable_type_id="test.ReplaceFull/new",
        )
        second_fingerprint = fingerprint_call(_dummy_fn, (Entry(1, "a"),), {}, [])
        assert first_fingerprint != second_fingerprint
        assert second_fingerprint == fingerprint_call(
            _dummy_fn, (Entry(2, "a"),), {}, []
        )
        assert second_fingerprint != fingerprint_call(
            _dummy_fn, (Entry(1, "b"),), {}, []
        )
        methods = []
        fingerprint_call(_dummy_fn, (Entry(1, "a"),), {}, methods)
        assert len(methods) == 1
        assert methods[0].call("prev").state == ("new", "a", "prev")
    finally:
        unregister_memo_key_function(Entry)


def test_register_not_memo_keyable_replaces_stable_type_id_for_class_objects() -> None:
    class Entry:
        pass

    class SameStableTypeId:
        pass

    try:
        register_memo_key_function(
            Entry, stable_type_id="test.NotMemoKeyableReplacesStable/v1"
        )
        register_memo_key_function(
            SameStableTypeId,
            stable_type_id="test.NotMemoKeyableReplacesStable/v1",
        )
        assert fingerprint_call(_dummy_fn, (Entry,), {}, []) == fingerprint_call(
            _dummy_fn, (SameStableTypeId,), {}, []
        )

        register_not_memo_keyable(Entry)
        assert fingerprint_call(_dummy_fn, (Entry,), {}, []) != fingerprint_call(
            _dummy_fn, (SameStableTypeId,), {}, []
        )
    finally:
        unregister_memo_key_function(Entry)
        unregister_memo_key_function(SameStableTypeId)


def test_register_memo_key_function_rejects_explicit_none_key_function() -> None:
    class Entry:
        pass

    with pytest.raises(TypeError, match="key_fn"):
        register_memo_key_function(
            Entry, cast(Any, None), stable_type_id="test.ExplicitNone/v1"
        )


@pytest.mark.parametrize(
    ("args", "kwargs", "match"),
    [
        ((), {}, "requires a key_fn or stable_type_id"),
        ((object(),), {}, "key_fn must be callable"),
        (
            (lambda entry: entry,),
            {"state_fn": object()},
            "state_fn must be callable",
        ),
    ],
)
def test_register_memo_key_function_rejects_invalid_forms(
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    match: str,
) -> None:
    class Entry:
        pass

    with pytest.raises(TypeError, match=match):
        register_memo_key_function(Entry, *args, **kwargs)


def test_register_memo_key_function_rejects_state_fn_without_key_function() -> None:
    class Entry:
        pass

    def state_fn(obj: Entry, prev_state: object) -> object:
        return prev_state

    kwargs: Any = {"state_fn": state_fn}

    with pytest.raises(TypeError, match="state_fn requires a memo key function"):
        register_memo_key_function(Entry, **kwargs)


def test_unregister_memo_key_function_clears_key_function_and_stable_type_id() -> None:
    class RegisteredOnly:
        def __init__(self, value: object) -> None:
            self.value = value

    class SameStableTypeId:
        def __init__(self, value: object) -> None:
            self.value = value

    try:
        register_memo_key_function(
            RegisteredOnly,
            lambda entry: ("registered", entry.value),
            stable_type_id="test.UnregisterCombined/v1",
        )
        register_memo_key_function(
            SameStableTypeId,
            lambda entry: ("registered", entry.value),
            stable_type_id="test.UnregisterCombined/v1",
        )
        assert fingerprint_call(
            _dummy_fn, (RegisteredOnly(1),), {}, []
        ) == fingerprint_call(_dummy_fn, (SameStableTypeId(1),), {}, [])
        assert fingerprint_call(_dummy_fn, (RegisteredOnly,), {}, []) == (
            fingerprint_call(_dummy_fn, (SameStableTypeId,), {}, [])
        )

        unregister_memo_key_function(RegisteredOnly)
        with pytest.raises(TypeError, match="Unsupported type for memoization key"):
            fingerprint_call(_dummy_fn, (RegisteredOnly(1),), {}, [])
        assert fingerprint_call(_dummy_fn, (RegisteredOnly,), {}, []) != (
            fingerprint_call(_dummy_fn, (SameStableTypeId,), {}, [])
        )
    finally:
        unregister_memo_key_function(RegisteredOnly)
        unregister_memo_key_function(SameStableTypeId)

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

    try:
        register_memo_key_function(OldEntry, stable_type_id="test.UnregisterStable/v1")
        register_memo_key_function(NewEntry, stable_type_id="test.UnregisterStable/v1")
        assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) == fingerprint_call(
            _dummy_fn, (NewEntry(1),), {}, []
        )
        unregister_memo_key_function(OldEntry)
        assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) != fingerprint_call(
            _dummy_fn, (NewEntry(1),), {}, []
        )
    finally:
        unregister_memo_key_function(OldEntry)
        unregister_memo_key_function(NewEntry)


def test_registered_stable_type_id_is_identity_exact_for_equal_metaclasses() -> None:
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

    try:
        register_memo_key_function(A, stable_type_id="test.EqualityMetaA/v1")
        assert fingerprint_call(_dummy_fn, (A(),), {}, []) != fingerprint_call(
            _dummy_fn, (B(),), {}, []
        )
    finally:
        unregister_memo_key_function(A)


def test_unregister_memo_key_function_handles_unhashable_stable_type_id_only() -> None:
    class EqNoHashMeta(type):
        def __eq__(cls, other: object) -> bool:
            return cls is other

    class OldEntry(metaclass=EqNoHashMeta):
        def __init__(self, value: object) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("entry", self.value)

    class NewEntry(metaclass=EqNoHashMeta):
        def __init__(self, value: object) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("entry", self.value)

    try:
        register_memo_key_function(OldEntry, stable_type_id="test.UnhashableMeta/v1")
        register_memo_key_function(NewEntry, stable_type_id="test.UnhashableMeta/v1")
        assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) == fingerprint_call(
            _dummy_fn, (NewEntry(1),), {}, []
        )
        unregister_memo_key_function(OldEntry)
        assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) != fingerprint_call(
            _dummy_fn, (NewEntry(1),), {}, []
        )
    finally:
        unregister_memo_key_function(OldEntry)
        unregister_memo_key_function(NewEntry)


def test_unregister_memo_key_function_is_identity_exact_for_equal_metaclasses() -> None:
    class EqHashMeta(type):
        def __eq__(cls, other: object) -> bool:
            return isinstance(other, EqHashMeta)

        def __hash__(cls) -> int:
            return 1

    class StableOnly(metaclass=EqHashMeta):
        pass

    class Registered(metaclass=EqHashMeta):
        def __getstate__(self) -> object:
            raise TypeError("registered test object is not picklable")

    try:
        register_memo_key_function(Registered, lambda entry: ("registered",))
        register_memo_key_function(StableOnly, stable_type_id="test.EqualUnregister/v1")
        unregister_memo_key_function(StableOnly)
        fingerprint_call(_dummy_fn, (Registered(),), {}, [])
    finally:
        unregister_memo_key_function(StableOnly)
        unregister_memo_key_function(Registered)


def test_stable_type_id_exact_type_and_validation() -> None:
    class OldEntry:
        __coco_memo_type_id__ = "test.DirectEntry/v1"

        def __init__(self, value: object) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("entry", self.value)

    class NewEntry:
        __coco_memo_type_id__ = "test.DirectEntry/v1"

        def __init__(self, value: object) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("entry", self.value)

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

    assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) == fingerprint_call(
        _dummy_fn, (NewEntry(1),), {}, []
    )
    assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) != fingerprint_call(
        _dummy_fn, (NewEntry(2),), {}, []
    )
    assert fingerprint_call(_dummy_fn, (Parent(),), {}, []) != fingerprint_call(
        _dummy_fn, (Child(),), {}, []
    )
    with pytest.raises(TypeError, match="must be a str"):
        fingerprint_call(_dummy_fn, (BadObjectId(),), {}, [])
    with pytest.raises(ValueError, match="non-empty"):
        fingerprint_call(_dummy_fn, (EmptyId(),), {}, [])


def test_register_memo_key_function_validation_and_public_export() -> None:
    import cocoindex as coco

    class Entry:
        pass

    assert coco.register_memo_key_function is register_memo_key_function
    assert "register_memo_type_identifier" not in coco.__all__
    assert not hasattr(coco, "register_memo_type_identifier")
    assert "register_not_memo_keyable" not in coco.__all__
    assert not hasattr(coco, "register_not_memo_keyable")
    with pytest.raises(TypeError, match="expects typ to be a type"):
        register_memo_key_function(
            cast(Any, object()), stable_type_id="test.Invalid/v1"
        )
    with pytest.raises(TypeError, match="must be a str"):
        register_memo_key_function(Entry, stable_type_id=cast(Any, object()))
    with pytest.raises(ValueError, match="non-empty"):
        register_memo_key_function(Entry, stable_type_id="")
    with pytest.raises(ValueError, match="non-empty"):
        register_memo_key_function(Entry, stable_type_id="   ")


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

    try:
        register_memo_key_function(Registered, lambda r: r.v, state_fn=_state_fn)
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
