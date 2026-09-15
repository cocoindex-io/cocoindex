import copy
import dataclasses
import math
import pickle
import sys
import weakref
from typing import Any, ClassVar, cast

import pytest
from cocoindex._internal import memo_fingerprint as _memo_fingerprint
from cocoindex._internal.function import _apply_memo_key, _normalize_memo_key
from cocoindex._internal.memo_fingerprint import (
    fingerprint_call,
    register_memo_key_function,
    register_memo_type,
    register_not_memo_keyable,
    unregister_memo_key_function,
    unregister_memo_type,
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


def test_prev_type_id_reuses_previous_automatic_identity() -> None:
    import cocoindex as coco

    class OldSourceEntry:
        def __init__(self, value: int) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("source-entry", self.value)

    class MovedSourceEntry:
        __coco_memo_type_id__: ClassVar[str] = coco.prev_type_id(
            "old_package.models", "SourceEntry"
        )

        def __init__(self, value: int) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("source-entry", self.value)

    class DifferentModuleSourceEntry:
        __coco_memo_type_id__: ClassVar[str] = coco.prev_type_id(
            "other_package.models", "SourceEntry"
        )

        def __init__(self, value: int) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("source-entry", self.value)

    class DifferentQualnameSourceEntry:
        __coco_memo_type_id__: ClassVar[str] = coco.prev_type_id(
            "old_package.models", "OtherSourceEntry"
        )

        def __init__(self, value: int) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("source-entry", self.value)

    class StableStringSourceEntry:
        __coco_memo_type_id__: ClassVar[str] = "old_package.models.SourceEntry"

        def __init__(self, value: int) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("source-entry", self.value)

    OldSourceEntry.__module__ = "old_package.models"
    OldSourceEntry.__qualname__ = "SourceEntry"

    old_instance_fingerprint = fingerprint_call(_dummy_fn, (OldSourceEntry(1),), {}, [])
    moved_instance_fingerprint = fingerprint_call(
        _dummy_fn, (MovedSourceEntry(1),), {}, []
    )
    old_class_fingerprint = fingerprint_call(_dummy_fn, (OldSourceEntry,), {}, [])
    moved_class_fingerprint = fingerprint_call(_dummy_fn, (MovedSourceEntry,), {}, [])

    assert old_instance_fingerprint == moved_instance_fingerprint
    assert old_class_fingerprint == moved_class_fingerprint
    assert old_class_fingerprint != old_instance_fingerprint
    assert moved_class_fingerprint != moved_instance_fingerprint

    for changed_type in (DifferentModuleSourceEntry, DifferentQualnameSourceEntry):
        assert old_instance_fingerprint != fingerprint_call(
            _dummy_fn, (changed_type(1),), {}, []
        )
        assert old_class_fingerprint != fingerprint_call(
            _dummy_fn, (changed_type,), {}, []
        )

    assert old_instance_fingerprint != fingerprint_call(
        _dummy_fn, (StableStringSourceEntry(1),), {}, []
    )
    assert old_class_fingerprint != fingerprint_call(
        _dummy_fn, (StableStringSourceEntry,), {}, []
    )


def test_prev_type_id_uses_canonical_main_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import cocoindex as coco

    class OldSourceEntry:
        def __coco_memo_key__(self) -> object:
            return "source-entry"

    class CanonicalModuleSourceEntry:
        __coco_memo_type_id__: ClassVar[str] = coco.prev_type_id("main", "SourceEntry")

        def __coco_memo_key__(self) -> object:
            return "source-entry"

    class LiteralMainModuleSourceEntry:
        __coco_memo_type_id__: ClassVar[str] = coco.prev_type_id(
            "__main__", "SourceEntry"
        )

        def __coco_memo_key__(self) -> object:
            return "source-entry"

    main_module = sys.modules["__main__"]
    monkeypatch.setattr(main_module, "__file__", "/tmp/main.py", raising=False)
    OldSourceEntry.__module__ = "__main__"
    OldSourceEntry.__qualname__ = "SourceEntry"

    old_fingerprint = fingerprint_call(_dummy_fn, (OldSourceEntry(),), {}, [])
    assert old_fingerprint == fingerprint_call(
        _dummy_fn, (CanonicalModuleSourceEntry(),), {}, []
    )
    assert old_fingerprint != fingerprint_call(
        _dummy_fn, (LiteralMainModuleSourceEntry(),), {}, []
    )


def test_prev_type_id_marker_is_immutable_and_round_trips() -> None:
    import cocoindex as coco

    left = coco.prev_type_id("a.b", "C")
    right = coco.prev_type_id("a", "b.C")
    assert str(left) != str(right)
    assert left != right

    module = "old:package.models"
    qualname = "Outer.Source.Entry"
    marker = coco.prev_type_id(module, qualname)
    assert isinstance(marker, _memo_fingerprint._PreviousTypeId)

    class IdentitySourceEntry:
        __coco_memo_type_id__: ClassVar[str] = marker

    assert (
        _memo_fingerprint._type_identity_parts(IdentitySourceEntry, None)
        is marker._identity_parts
    )
    with pytest.raises(AttributeError, match="immutable"):
        marker._identity_parts = ("mutated", "mutated")
    with pytest.raises(AttributeError, match="immutable"):
        delattr(marker, "_identity_parts")

    for variant in (
        marker,
        copy.copy(marker),
        copy.deepcopy(marker),
        pickle.loads(pickle.dumps(marker)),
    ):
        assert type(variant) is type(marker)

        class MovedSourceEntry:
            __coco_memo_type_id__: ClassVar[str] = variant

        assert _memo_fingerprint._type_identity_parts(MovedSourceEntry, None) == (
            module,
            qualname,
        )

    class RegisteredSourceEntry:
        pass

    try:
        register_memo_type(RegisteredSourceEntry, stable_type_id=marker)
        registry = _memo_fingerprint._registered_memo_type_registry(
            RegisteredSourceEntry
        )
        assert registry is not None
        assert _memo_fingerprint._type_identity_parts(
            RegisteredSourceEntry, registry
        ) == (module, qualname)
    finally:
        unregister_memo_type(RegisteredSourceEntry)

    ordinary_marker = str(marker)

    class OrdinaryStringSourceEntry:
        __coco_memo_type_id__: ClassVar[str] = ordinary_marker

    assert _memo_fingerprint._type_identity_parts(OrdinaryStringSourceEntry, None) == (
        ("__coco_memo_type_id__", ordinary_marker),
        None,
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
        register_memo_type(OldEntry, stable_type_id="test.RegisteredRawClass/v1")
        register_memo_type(NewEntry, stable_type_id="test.RegisteredRawClass/v1")
        register_memo_type(ChangedEntry, stable_type_id="test.RegisteredRawClass/v2")

        assert fingerprint_call(_dummy_fn, (OldEntry,), {}, []) == fingerprint_call(
            _dummy_fn, (NewEntry,), {}, []
        )
        assert fingerprint_call(_dummy_fn, (OldEntry,), {}, []) != fingerprint_call(
            _dummy_fn, (ChangedEntry,), {}, []
        )
    finally:
        unregister_memo_type(OldEntry)
        unregister_memo_type(NewEntry)
        unregister_memo_type(ChangedEntry)


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
        (("hook", *_memo_fingerprint._type_identity_parts(Entry, None), ("ref", 0)),),
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
            (
                (
                    "hook",
                    *_memo_fingerprint._type_identity_parts(Entry, None),
                    ("ref", 0),
                ),
            ),
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
                *_memo_fingerprint._type_identity_parts(FirstEntry, None),
                ("seq", ("first",)),
            ),
            (
                "hook",
                *_memo_fingerprint._type_identity_parts(SecondEntry, None),
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
        *_memo_fingerprint._type_identity_parts(Entry, None),
        ("seq", (("seq", ("shared",)), ("ref", 1))),
    )
    assert parent_wrapped == (
        "seq",
        (
            (
                "hook",
                *_memo_fingerprint._type_identity_parts(Entry, None),
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
        register_memo_type(
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
        unregister_memo_type(MemoMeta)


def test_prev_type_id_reuses_registered_metaclass_owner_identity() -> None:
    import cocoindex as coco

    class OldMemoMeta(type):
        pass

    class MovedMemoMeta(type):
        pass

    class OldEntry(metaclass=OldMemoMeta):
        pass

    class MovedEntry(metaclass=MovedMemoMeta):
        pass

    OldMemoMeta.__module__ = "old_package.models"
    OldMemoMeta.__qualname__ = "SourceMeta"

    def metaclass_key(cls: type) -> object:
        return "source-class"

    try:
        register_memo_type(OldMemoMeta, metaclass_key)
        register_memo_type(
            MovedMemoMeta,
            metaclass_key,
            stable_type_id=coco.prev_type_id(
                "old_package.models",
                "SourceMeta",
            ),
        )
        assert fingerprint_call(_dummy_fn, (OldEntry,), {}, []) == fingerprint_call(
            _dummy_fn, (MovedEntry,), {}, []
        )
    finally:
        unregister_memo_type(OldMemoMeta)
        unregister_memo_type(MovedMemoMeta)


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
        register_memo_type(
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
        unregister_memo_type(type)


def test_raw_class_object_ignores_registered_object_memo_key() -> None:
    object_key_calls: list[object] = []

    class Entry:
        pass

    class OtherEntry:
        pass

    expected_entry = fingerprint_call(_dummy_fn, (Entry,), {}, [])
    expected_other = fingerprint_call(_dummy_fn, (OtherEntry,), {}, [])
    assert expected_entry != expected_other

    def object_key(obj: object) -> object:
        object_key_calls.append(obj)
        return "object memo key ran"

    try:
        register_memo_type(
            object,
            object_key,
            stable_type_id="test.IgnoredObjectClass/v1",
        )
        assert fingerprint_call(_dummy_fn, (Entry,), {}, []) == expected_entry
        assert fingerprint_call(_dummy_fn, (OtherEntry,), {}, []) == expected_other
        assert object_key_calls == []
    finally:
        unregister_memo_type(object)


def test_raw_class_object_ignores_registered_type_stable_type_id() -> None:
    class Entry:
        pass

    Entry.__module__ = "tests.raw_class_registered_type_stable_id"
    original = fingerprint_call(_dummy_fn, (Entry,), {}, [])

    try:
        register_memo_type(type, stable_type_id="test.RegisteredType/v1")
        assert fingerprint_call(_dummy_fn, (Entry,), {}, []) == original
    finally:
        unregister_memo_type(type)


def test_raw_class_object_stable_type_id_is_inherited_by_subclasses() -> None:
    class Parent:
        __coco_memo_type_id__ = "test.RawClassExact/v1"

    class Child(Parent):
        pass

    class OverridingChild(Parent):
        __coco_memo_type_id__ = "test.RawClassExactChild/v1"

    assert fingerprint_call(_dummy_fn, (Parent,), {}, []) == fingerprint_call(
        _dummy_fn, (Child,), {}, []
    )
    assert fingerprint_call(_dummy_fn, (Parent,), {}, []) != fingerprint_call(
        _dummy_fn, (OverridingChild,), {}, []
    )


def test_register_memo_type_registers_key_function_and_stable_type_id_for_owner_base() -> (
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
        register_memo_type(
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
        unregister_memo_type(Base)


def test_prev_type_id_reuses_registered_mro_owner_identity() -> None:
    import cocoindex as coco

    class OldBase:
        def __init__(self, value: object) -> None:
            self.value = value

    class OldChild(OldBase):
        pass

    class MovedBase:
        def __init__(self, value: object) -> None:
            self.value = value

    class MovedChild(MovedBase):
        pass

    OldBase.__module__ = "old_package.models"
    OldBase.__qualname__ = "SourceBase"

    def base_key(entry: OldBase | MovedBase) -> object:
        return ("base", entry.value)

    try:
        register_memo_type(OldBase, base_key)
        register_memo_type(
            MovedBase,
            base_key,
            stable_type_id=coco.prev_type_id(
                "old_package.models",
                "SourceBase",
            ),
        )
        assert fingerprint_call(_dummy_fn, (OldChild(1),), {}, []) == fingerprint_call(
            _dummy_fn, (MovedChild(1),), {}, []
        )
        assert fingerprint_call(_dummy_fn, (OldChild(1),), {}, []) != fingerprint_call(
            _dummy_fn, (MovedChild(2),), {}, []
        )
    finally:
        unregister_memo_type(OldBase)
        unregister_memo_type(MovedBase)


def test_register_memo_type_registers_stable_type_id_without_key_function() -> None:
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
        register_memo_type(OldEntry, stable_type_id="test.RegisteredEntry/v1")
        register_memo_type(NewEntry, None, stable_type_id="test.RegisteredEntry/v1")
        assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) == fingerprint_call(
            _dummy_fn, (NewEntry(1),), {}, []
        )
        assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) != fingerprint_call(
            _dummy_fn, (NewEntry(2),), {}, []
        )
    finally:
        unregister_memo_type(OldEntry)
        unregister_memo_type(NewEntry)


def test_stable_type_id_only_registration_propagates_to_subclasses_across_mro() -> None:
    class Parent:
        def __init__(self, value: object) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("entry", self.value)

    class Child(Parent):
        pass

    class OverridingChild(Parent):
        pass

    class SameStableTypeId:
        def __init__(self, value: object) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("entry", self.value)

    try:
        register_memo_type(Parent, stable_type_id="test.RegisteredParent/v1")
        register_memo_type(
            OverridingChild, stable_type_id="test.RegisteredOverridingChild/v1"
        )
        register_memo_type(SameStableTypeId, stable_type_id="test.RegisteredParent/v1")

        # Child inherits Parent's registered stable type ID across MRO:
        assert fingerprint_call(_dummy_fn, (Parent(1),), {}, []) == fingerprint_call(
            _dummy_fn, (Child(1),), {}, []
        )
        assert fingerprint_call(_dummy_fn, (Parent(1),), {}, []) == fingerprint_call(
            _dummy_fn, (SameStableTypeId(1),), {}, []
        )
        assert fingerprint_call(_dummy_fn, (Parent,), {}, []) == fingerprint_call(
            _dummy_fn, (Child,), {}, []
        )
        # OverridingChild has its own more specific registration:
        assert fingerprint_call(_dummy_fn, (Parent(1),), {}, []) != fingerprint_call(
            _dummy_fn, (OverridingChild(1),), {}, []
        )
    finally:
        unregister_memo_type(Parent)
        unregister_memo_type(OverridingChild)
        unregister_memo_type(SameStableTypeId)


def test_declared_stable_type_id_is_inherited_and_takes_precedence_over_registered() -> (
    None
):
    class Base:
        __coco_memo_type_id__: ClassVar[str] = "test.DeclaredBase/v1"

        def __init__(self, value: object) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("entry", self.value)

    class Sub(Base):
        pass

    class OverridingSub(Base):
        __coco_memo_type_id__: ClassVar[str] = "test.DeclaredOverridingSub/v1"

    try:
        # Register an ID on Sub — declared (inherited from Base) must take precedence:
        register_memo_type(Sub, stable_type_id="test.RegisteredSub/v1")

        assert _memo_fingerprint._type_identity_parts(Base, None) == (
            ("__coco_memo_type_id__", "test.DeclaredBase/v1"),
            None,
        )
        # Sub inherits Base's declared ID, beating Sub's registered ID:
        assert _memo_fingerprint._type_identity_parts(Sub, None) == (
            ("__coco_memo_type_id__", "test.DeclaredBase/v1"),
            None,
        )
        assert fingerprint_call(_dummy_fn, (Base(1),), {}, []) == fingerprint_call(
            _dummy_fn, (Sub(1),), {}, []
        )
        # OverridingSub overrides Base's declared ID:
        assert _memo_fingerprint._type_identity_parts(OverridingSub, None) == (
            ("__coco_memo_type_id__", "test.DeclaredOverridingSub/v1"),
            None,
        )
        assert fingerprint_call(_dummy_fn, (Base(1),), {}, []) != fingerprint_call(
            _dummy_fn, (OverridingSub(1),), {}, []
        )
    finally:
        unregister_memo_type(Sub)


def test_prev_type_id_on_base_class_propagates_to_subclasses() -> None:
    import cocoindex as coco

    class OldBase:
        def __init__(self, value: object) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("entry", self.value)

    class MovedBase:
        __coco_memo_type_id__: ClassVar[str] = coco.prev_type_id(
            "old_package.models", "OldBase"
        )

        def __init__(self, value: object) -> None:
            self.value = value

        def __coco_memo_key__(self) -> object:
            return ("entry", self.value)

    class MovedSub(MovedBase):
        pass

    OldBase.__module__ = "old_package.models"
    OldBase.__qualname__ = "OldBase"

    # End-to-end black-box verification: MovedSub matches OldBase fingerprint:
    assert fingerprint_call(_dummy_fn, (MovedSub(1),), {}, []) == fingerprint_call(
        _dummy_fn, (OldBase(1),), {}, []
    )
    assert fingerprint_call(_dummy_fn, (MovedSub(1),), {}, []) != fingerprint_call(
        _dummy_fn, (OldBase(2),), {}, []
    )
    assert fingerprint_call(_dummy_fn, (MovedBase(1),), {}, []) == fingerprint_call(
        _dummy_fn, (MovedSub(1),), {}, []
    )


def test_stable_type_id_only_subclass_registration_does_not_hide_base_key() -> None:
    class Parent:
        def __init__(self, value: object, ignored: object) -> None:
            self.value = value
            self.ignored = ignored

    class Child(Parent):
        pass

    try:
        register_memo_type(Parent, lambda entry: ("parent", entry.value))
        register_memo_type(Child, stable_type_id="test.RegisteredExactChild/v1")

        assert fingerprint_call(_dummy_fn, (Child(1, "a"),), {}, []) == (
            fingerprint_call(_dummy_fn, (Child(1, "b"),), {}, [])
        )
        assert fingerprint_call(_dummy_fn, (Child(1, "a"),), {}, []) != (
            fingerprint_call(_dummy_fn, (Child(2, "a"),), {}, [])
        )
    finally:
        unregister_memo_type(Child)
        unregister_memo_type(Parent)


def test_subclass_declared_stable_type_id_overrides_registered_base_identity() -> None:
    class Base:
        def __init__(self, value: object, ignored: object) -> None:
            self.value = value
            self.ignored = ignored

    class Sub(Base):
        __coco_memo_type_id__: ClassVar[str] = "test.SubDeclaredOverride/v1"

    class TwinSub(Base):
        __coco_memo_type_id__: ClassVar[str] = "test.SubDeclaredOverride/v1"

    try:
        register_memo_type(
            Base,
            lambda entry: ("base", entry.value),
            stable_type_id="test.BaseRegisteredOverride/v1",
        )

        # Base's registered key function still drives Sub's key shape:
        assert fingerprint_call(_dummy_fn, (Sub(1, "a"),), {}, []) == fingerprint_call(
            _dummy_fn, (Sub(1, "b"),), {}, []
        )
        # Sub's declared ID overrides the identity inherited from Base's registration:
        assert fingerprint_call(_dummy_fn, (Sub(1, "a"),), {}, []) != fingerprint_call(
            _dummy_fn, (Base(1, "a"),), {}, []
        )
        assert fingerprint_call(_dummy_fn, (Sub(1, "a"),), {}, []) == fingerprint_call(
            _dummy_fn, (TwinSub(1, "a"),), {}, []
        )
    finally:
        unregister_memo_type(Base)


def test_subclass_registered_stable_type_id_overrides_registered_base_identity() -> (
    None
):
    class Base:
        def __init__(self, value: object, ignored: object) -> None:
            self.value = value
            self.ignored = ignored

    class Sub(Base):
        pass

    class TwinSub(Base):
        pass

    try:
        register_memo_type(
            Base,
            lambda entry: ("base", entry.value),
            stable_type_id="test.BaseRegisteredOverride/v2",
        )
        register_memo_type(Sub, stable_type_id="test.SubRegisteredOverride/v2")
        register_memo_type(TwinSub, stable_type_id="test.SubRegisteredOverride/v2")

        # Base's registered key function still drives Sub's key shape:
        assert fingerprint_call(_dummy_fn, (Sub(1, "a"),), {}, []) == fingerprint_call(
            _dummy_fn, (Sub(1, "b"),), {}, []
        )
        # Sub's registered ID overrides the identity inherited from Base's registration:
        assert fingerprint_call(_dummy_fn, (Sub(1, "a"),), {}, []) != fingerprint_call(
            _dummy_fn, (Base(1, "a"),), {}, []
        )
        assert fingerprint_call(_dummy_fn, (Sub(1, "a"),), {}, []) == fingerprint_call(
            _dummy_fn, (TwinSub(1, "a"),), {}, []
        )
    finally:
        unregister_memo_type(TwinSub)
        unregister_memo_type(Sub)
        unregister_memo_type(Base)


def test_register_memo_type_replaces_and_clears_omitted_state_fn() -> None:
    @dataclasses.dataclass
    class Entry:
        value: int
        ignored: str

    def state_fn(obj: Any, prev_state: object) -> MemoStateOutcome:
        return MemoStateOutcome(state=("state", obj.value, prev_state), memo_valid=True)

    try:
        # 1. Start with (key_fn, state_fn, stable_type_id)
        register_memo_type(
            Entry,
            lambda e: ("key", e.value),
            state_fn=state_fn,
            stable_type_id="test.Replace/v1",
        )
        methods: list[Any] = []
        fp_with_state = fingerprint_call(_dummy_fn, (Entry(1, "a"),), {}, methods)
        assert len(methods) == 1
        assert fp_with_state == fingerprint_call(_dummy_fn, (Entry(1, "b"),), {}, [])

        # 2. Re-register omitting state_fn -> clears state_fn while keeping key_fn and stable ID
        register_memo_type(
            Entry,
            lambda e: ("key", e.value),
            stable_type_id="test.Replace/v1",
        )
        methods = []
        fp_no_state = fingerprint_call(_dummy_fn, (Entry(1, "a"),), {}, methods)
        assert len(methods) == 0
        assert fp_no_state != fp_with_state
        assert fp_no_state == fingerprint_call(
            _dummy_fn, (Entry(1, "b"),), {}, []
        )  # key_fn still active

        # 3. Re-register with new stable ID -> changes fingerprint while keeping key_fn
        register_memo_type(
            Entry,
            lambda e: ("key", e.value),
            stable_type_id="test.Replace/v1_updated",
        )
        fp_new_id = fingerprint_call(_dummy_fn, (Entry(1, "a"),), {}, [])
        assert fp_new_id != fp_no_state
        assert fp_new_id == fingerprint_call(_dummy_fn, (Entry(1, "b"),), {}, [])
    finally:
        unregister_memo_type(Entry)


def test_register_memo_type_stable_id_only_clears_previous_key_and_state() -> None:
    @dataclasses.dataclass
    class Entry:
        value: int
        ignored: str

    @dataclasses.dataclass
    class ReferenceSameId:
        value: int
        ignored: str

    stable_id = "test.Replace/v2"
    try:
        register_memo_type(ReferenceSameId, stable_type_id=stable_id)

        # 1. Start with key_fn
        register_memo_type(Entry, lambda e: ("key", e.value))
        assert fingerprint_call(
            _dummy_fn, (Entry(1, "a"),), {}, []
        ) == fingerprint_call(_dummy_fn, (Entry(1, "b"),), {}, [])

        # 2. Re-register with stable_id only -> clears key_fn (dataclass fields participate again) and sets stable_id
        register_memo_type(Entry, stable_type_id=stable_id)
        assert fingerprint_call(
            _dummy_fn, (Entry(1, "a"),), {}, []
        ) != fingerprint_call(_dummy_fn, (Entry(1, "b"),), {}, [])
        assert fingerprint_call(
            _dummy_fn, (Entry(1, "a"),), {}, []
        ) == fingerprint_call(_dummy_fn, (ReferenceSameId(1, "a"),), {}, [])
    finally:
        unregister_memo_type(Entry)
        unregister_memo_type(ReferenceSameId)


def test_register_memo_type_key_only_clears_previous_stable_id() -> None:
    class Entry:
        def __init__(self, value: int) -> None:
            self.value = value

    class SameIdEntry:
        def __init__(self, value: int) -> None:
            self.value = value

    stable_id = "test.Replace/v3"
    try:
        register_memo_type(SameIdEntry, lambda e: e.value, stable_type_id=stable_id)

        # 1. Start with stable_id
        register_memo_type(Entry, lambda e: e.value, stable_type_id=stable_id)
        assert fingerprint_call(_dummy_fn, (Entry(1),), {}, []) == fingerprint_call(
            _dummy_fn, (SameIdEntry(1),), {}, []
        )

        # 2. Re-register with key_fn only -> clears stable_id (falls back to module.qualname)
        register_memo_type(Entry, lambda e: e.value)
        assert fingerprint_call(_dummy_fn, (Entry(1),), {}, []) != fingerprint_call(
            _dummy_fn, (SameIdEntry(1),), {}, []
        )
    finally:
        unregister_memo_type(Entry)
        unregister_memo_type(SameIdEntry)


def test_intrinsic_hooks_and_declared_stable_id_beat_registration() -> None:
    declared_type_id = "test.DeclaredIntrinsic/v1"

    class Entry:
        __coco_memo_type_id__ = declared_type_id

        def __coco_memo_key__(self) -> object:
            return "intrinsic-key"

        def __coco_memo_state__(self, prev_state: object) -> MemoStateOutcome:
            return MemoStateOutcome(state=prev_state, memo_valid=True)

    def registered_key(_entry: Entry) -> object:
        raise AssertionError("intrinsic key must beat the registered key")

    def registered_state(_entry: Entry, _prev_state: object) -> MemoStateOutcome:
        raise AssertionError("intrinsic state must beat the registered state")

    try:
        register_memo_type(
            Entry,
            registered_key,
            state_fn=registered_state,
            stable_type_id="test.RegisteredIntrinsic/v1",
        )
        state_methods: list[Any] = []
        canonical = _memo_fingerprint._canonicalize(Entry(), None, state_methods)
        assert isinstance(canonical, tuple)
        assert canonical[:3] == (
            "shook",
            ("__coco_memo_type_id__", declared_type_id),
            None,
        )
        assert len(state_methods) == 1
        assert state_methods[0].call("previous") == MemoStateOutcome(
            state="previous", memo_valid=True
        )
    finally:
        unregister_memo_type(Entry)


def test_declared_stable_id_beats_registration_for_selected_key_owner() -> None:
    declared_type_id = "test.DeclaredBaseOwner/v1"

    class BaseEntry:
        __coco_memo_type_id__ = declared_type_id

        def __init__(self, value: object) -> None:
            self.value = value

    class ChildEntry(BaseEntry):
        pass

    try:
        register_memo_type(
            BaseEntry,
            lambda entry: ("registered", entry.value),
            stable_type_id="test.RegisteredBaseOwner/v1",
        )
        assert _memo_fingerprint._canonicalize(ChildEntry(1), None, []) == (
            "hook",
            ("__coco_memo_type_id__", declared_type_id),
            None,
            ("seq", ("registered", 1)),
        )
    finally:
        unregister_memo_type(BaseEntry)


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
        register_memo_type(
            OldEntry,
            key_fn,
            state_fn=state_fn,
            stable_type_id="test.CombinedStateStable/v1",
        )
        register_memo_type(
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
        unregister_memo_type(OldEntry)
        unregister_memo_type(NewEntry)


def test_register_not_memo_keyable_replaces_stable_type_id_for_class_objects() -> None:
    class Entry:
        pass

    class SameStableTypeId:
        pass

    try:
        register_memo_type(Entry, stable_type_id="test.NotMemoKeyableReplacesStable/v1")
        register_memo_type(
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
        unregister_memo_type(Entry)
        unregister_memo_type(SameStableTypeId)


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
        (
            (),
            {"stable_type_id": 123},
            "stable_type_id must be a str",
        ),
    ],
)
def test_register_memo_type_rejects_invalid_forms(
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    match: str,
) -> None:
    class Entry:
        pass

    with pytest.raises(TypeError, match=match):
        register_memo_type(Entry, *args, **kwargs)


def test_register_memo_type_rejects_state_fn_without_key_function() -> None:
    class Entry:
        pass

    def state_fn(obj: Entry, prev_state: object) -> object:
        return prev_state

    kwargs: Any = {"state_fn": state_fn}

    with pytest.raises(TypeError, match="state_fn requires a memo key function"):
        register_memo_type(Entry, **kwargs)


def test_register_and_unregister_memo_key_function_shortcuts_preserve_stable_type_id() -> (
    None
):
    @dataclasses.dataclass
    class Entry:
        value: int
        ignored: str

    @dataclasses.dataclass
    class Twin:
        value: int
        ignored: str

    stable_id = "test.ShortcutPreservesStable/v1"
    try:
        # Register reference twin with stable ID:
        register_memo_type(Twin, stable_type_id=stable_id)

        # 1. Register only stable type ID on Entry:
        register_memo_type(Entry, stable_type_id=stable_id)
        assert fingerprint_call(
            _dummy_fn, (Entry(1, "a"),), {}, []
        ) != fingerprint_call(
            _dummy_fn, (Entry(1, "b"),), {}, []
        )  # No key_fn yet: dataclass hashing includes 'ignored'
        assert fingerprint_call(
            _dummy_fn, (Entry(1, "a"),), {}, []
        ) == fingerprint_call(_dummy_fn, (Twin(1, "a"),), {}, [])  # Matches twin ID

        # 2. Call register_memo_key_function shortcut: delegates to key_fn AND preserves stable_id
        register_memo_key_function(Entry, lambda e: ("key", e.value))
        assert fingerprint_call(
            _dummy_fn, (Entry(1, "a"),), {}, []
        ) == fingerprint_call(
            _dummy_fn, (Entry(1, "b"),), {}, []
        )  # key_fn active (ignores 'ignored')

        # 3. Call unregister_memo_key_function shortcut: removes key_fn AND preserves stable_id
        unregister_memo_key_function(Entry)
        assert fingerprint_call(
            _dummy_fn, (Entry(1, "a"),), {}, []
        ) != fingerprint_call(
            _dummy_fn, (Entry(1, "b"),), {}, []
        )  # key_fn removed: dataclass hashing includes 'ignored'
        assert fingerprint_call(
            _dummy_fn, (Entry(1, "a"),), {}, []
        ) == fingerprint_call(
            _dummy_fn, (Twin(1, "a"),), {}, []
        )  # stable_id still preserved and matches twin

        # 4. Full unregister clears everything:
        unregister_memo_type(Entry)
        assert fingerprint_call(
            _dummy_fn, (Entry(1, "a"),), {}, []
        ) != fingerprint_call(_dummy_fn, (Twin(1, "a"),), {}, [])  # stable_id removed
    finally:
        unregister_memo_type(Entry)
        unregister_memo_type(Twin)


def test_unregister_memo_type_clears_key_function_and_stable_type_id() -> None:
    class RegisteredOnly:
        def __init__(self, value: object) -> None:
            self.value = value

    class SameStableTypeId:
        def __init__(self, value: object) -> None:
            self.value = value

    try:
        register_memo_type(
            RegisteredOnly,
            lambda entry: ("registered", entry.value),
            stable_type_id="test.UnregisterCombined/v1",
        )
        register_memo_type(
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

        unregister_memo_type(RegisteredOnly)
        with pytest.raises(TypeError, match="Unsupported type for memoization key"):
            fingerprint_call(_dummy_fn, (RegisteredOnly(1),), {}, [])
        assert fingerprint_call(_dummy_fn, (RegisteredOnly,), {}, []) != (
            fingerprint_call(_dummy_fn, (SameStableTypeId,), {}, [])
        )
    finally:
        unregister_memo_type(RegisteredOnly)
        unregister_memo_type(SameStableTypeId)

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
        register_memo_type(OldEntry, stable_type_id="test.UnregisterStable/v1")
        register_memo_type(NewEntry, stable_type_id="test.UnregisterStable/v1")
        assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) == fingerprint_call(
            _dummy_fn, (NewEntry(1),), {}, []
        )
        unregister_memo_type(OldEntry)
        assert fingerprint_call(_dummy_fn, (OldEntry(1),), {}, []) != fingerprint_call(
            _dummy_fn, (NewEntry(1),), {}, []
        )
    finally:
        unregister_memo_type(OldEntry)
        unregister_memo_type(NewEntry)


def test_none_declared_stable_type_id_falls_back_to_registered_or_module_qualname() -> (
    None
):
    class NoneId:
        __coco_memo_type_id__ = None

        def __coco_memo_key__(self) -> object:
            return ("bad", 1)

    none_id_canonical = _memo_fingerprint._canonicalize(NoneId(), None, [])
    assert isinstance(none_id_canonical, tuple)
    assert none_id_canonical[:3] == (
        "hook",
        _memo_fingerprint.canonical_module_name(NoneId),
        NoneId.__qualname__,
    )

    stable_type_id = "test.NoneDeclarationFallback/v1"
    try:
        register_memo_type(NoneId, stable_type_id=stable_type_id)
        registered_canonical = _memo_fingerprint._canonicalize(NoneId(), None, [])
        assert isinstance(registered_canonical, tuple)
        assert registered_canonical[:3] == (
            "hook",
            ("__coco_memo_type_id__", stable_type_id),
            None,
        )
    finally:
        unregister_memo_type(NoneId)


def test_non_string_declared_stable_type_id_falls_back_to_registered_or_module_qualname() -> (
    None
):
    class NonStringSourceEntry:
        __coco_memo_type_id__ = 12345

        def __coco_memo_key__(self) -> object:
            return ("bad", 1)

    non_str_canonical = _memo_fingerprint._canonicalize(
        NonStringSourceEntry(), None, []
    )
    assert isinstance(non_str_canonical, tuple)
    assert non_str_canonical[:3] == (
        "hook",
        _memo_fingerprint.canonical_module_name(NonStringSourceEntry),
        NonStringSourceEntry.__qualname__,
    )

    stable_type_id = "test.NonStringDeclarationFallback/v1"
    try:
        register_memo_type(NonStringSourceEntry, stable_type_id=stable_type_id)
        registered_canonical = _memo_fingerprint._canonicalize(
            NonStringSourceEntry(), None, []
        )
        assert isinstance(registered_canonical, tuple)
        assert registered_canonical[:3] == (
            "hook",
            ("__coco_memo_type_id__", stable_type_id),
            None,
        )
    finally:
        unregister_memo_type(NonStringSourceEntry)


def test_register_memo_type_validation_and_public_export() -> None:
    import cocoindex as coco

    assert coco.register_memo_type is _memo_fingerprint.register_memo_type
    assert (
        coco.register_memo_key_function is _memo_fingerprint.register_memo_key_function
    )
    assert coco.prev_type_id is _memo_fingerprint.prev_type_id
    assert "register_memo_type" in coco.__all__
    assert "register_memo_key_function" in coco.__all__
    assert "prev_type_id" in coco.__all__
    previous_type_id = coco.prev_type_id("old_package.models", "SourceEntry")
    assert isinstance(previous_type_id, str)
    assert not hasattr(coco, "register_memo_type_identifier")
    assert not hasattr(coco, "register_not_memo_keyable")
    assert not hasattr(coco, "unregister_memo_type")
    assert not hasattr(coco, "unregister_memo_key_function")
    with pytest.raises(TypeError, match="expects typ to be a type"):
        coco.register_memo_type(cast(Any, object()), stable_type_id="test.Invalid/v1")

    class ExplicitNoneStateEntry:
        pass

    try:
        coco.register_memo_type(
            ExplicitNoneStateEntry,
            state_fn=None,
            stable_type_id="test.ExplicitNoneState/v1",
        )
        assert _memo_fingerprint._type_identity_parts(
            ExplicitNoneStateEntry,
            _memo_fingerprint._registered_memo_type_registry(ExplicitNoneStateEntry),
        ) == (("__coco_memo_type_id__", "test.ExplicitNoneState/v1"), None)
    finally:
        _memo_fingerprint.unregister_memo_type(ExplicitNoneStateEntry)


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


def test_fingerprint_dict_and_set_with_fingerprint_keys_order_independent() -> None:
    from cocoindex._internal import core
    from cocoindex._internal.memo_fingerprint import memo_fingerprint

    fp1 = core.fingerprint_simple_object("alpha")
    fp2 = core.fingerprint_simple_object("beta")

    # Dict order independence with Fingerprint keys
    d1 = {fp1: "val1", fp2: "val2"}
    d2 = {fp2: "val2", fp1: "val1"}
    assert memo_fingerprint(d1) == memo_fingerprint(d2)

    # Set order independence with Fingerprint elements
    s1 = {fp1, fp2}
    s2 = {fp2, fp1}
    assert memo_fingerprint(s1) == memo_fingerprint(s2)

    # Nested container order independence with Fingerprint keys
    nested1 = {"items": [{fp1: 1, fp2: 2}]}
    nested2 = {"items": [{fp2: 2, fp1: 1}]}
    assert memo_fingerprint(nested1) == memo_fingerprint(nested2)
