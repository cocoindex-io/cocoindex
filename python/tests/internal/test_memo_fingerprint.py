import collections
import copy
import dataclasses
import enum
import math
import pickle
import sys
from typing import Any, Callable, ClassVar, cast

import pytest

# The two public names, imported the way users import them.
from cocoindex import prev_type_id, register_memo_type
from cocoindex._internal import core
from cocoindex._internal.function import _apply_memo_key, _normalize_memo_key
from cocoindex._internal.memo_fingerprint import (
    _canonicalize,
    fingerprint_call,
    memo_fingerprint,
    register_memo_key_function,
    register_not_memo_keyable,
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

    register_memo_key_function(Y, lambda y: ("y", y.v))
    try:
        fp_a = fingerprint_call(_dummy_fn, (Y(5, "a"),), {}, [])
        fp_irrelevant_changed = fingerprint_call(_dummy_fn, (Y(5, "b"),), {}, [])
        fp_b = fingerprint_call(_dummy_fn, (Y(6, "a"),), {}, [])
        assert fp_a == fp_irrelevant_changed
        assert fp_a != fp_b
    finally:
        unregister_memo_type(Y)


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
        unregister_memo_type(C)
        unregister_memo_type(D)


# Stable type IDs: each case builds two values and states whether they
# fingerprint the same. Registrations made through `_Case.register` are removed
# after the case runs.

_OLD_MODULE = "old_package.models"


class _Keyed:
    """Keyed by its value through ``__coco_memo_key__``."""

    def __init__(self, value: object, ignored: object = None) -> None:
        self.value = value
        self.ignored = ignored

    def __coco_memo_key__(self) -> object:
        return ("entry", self.value)


class _Unkeyed:
    """No memo key of its own; a registered key function supplies one."""

    def __init__(self, value: object, ignored: object = None) -> None:
        self.value = value
        self.ignored = ignored


def _value_key(entry: Any) -> object:
    # The key `_Keyed` returns, so a value keyed by a registered function and a
    # `_Keyed` value fingerprint the same exactly when their type identities do.
    return ("entry", entry.value)


def _derive(base: type, name: str, module: str | None = None, **attrs: Any) -> Any:
    """Subclass *base*, keeping its metaclass, as if declared as *name* in *module*."""
    cls = type(base)(name, (base,), attrs)
    if module is not None:
        cls.__module__ = module
    return cls


class _Case:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.monkeypatch = monkeypatch
        self.registered: list[type] = []

    def register(
        self,
        typ: type,
        *args: Any,
        via: Callable[..., None] = register_memo_type,
        **kwargs: Any,
    ) -> None:
        via(typ, *args, **kwargs)
        self.registered.append(typ)


_Build = Callable[[_Case], tuple[object, object]]


def _declared_id_survives_module_move(case: _Case) -> tuple[object, object]:
    old = _derive(_Keyed, "Entry", _OLD_MODULE, __coco_memo_type_id__="test.Entry/v1")
    new = _derive(_Keyed, "Entry", "new_package", __coco_memo_type_id__="test.Entry/v1")
    return old(1), new(1)


def _declared_id_versions_differ(case: _Case) -> tuple[object, object]:
    v1 = _derive(_Keyed, "Entry", __coco_memo_type_id__="test.Entry/v1")
    v2 = _derive(_Keyed, "Entry", __coco_memo_type_id__="test.Entry/v2")
    return v1(1), v2(1)


def _pydantic_declared_id_survives_rename(case: _Case) -> tuple[object, object]:
    try:
        from pydantic import BaseModel
    except ImportError:
        pytest.skip("pydantic not installed")

    class Old(BaseModel):
        __coco_memo_type_id__: ClassVar[str] = "test.Model/v1"
        value: int

    class New(BaseModel):
        __coco_memo_type_id__: ClassVar[str] = "test.Model/v1"
        value: int

    return Old(value=1), New(value=1)


def _declared_id_is_inherited(case: _Case) -> tuple[object, object]:
    base = _derive(_Keyed, "Base", __coco_memo_type_id__="test.Base/v1")
    return _derive(base, "Sub")(1), base(1)


def _subclass_redeclares_id(case: _Case) -> tuple[object, object]:
    base = _derive(_Keyed, "Base", __coco_memo_type_id__="test.Base/v1")
    sub = _derive(base, "Sub", __coco_memo_type_id__="test.Sub/v1")
    return sub(1), base(1)


def _inherited_declared_id_beats_own_registration(case: _Case) -> tuple[object, object]:
    base = _derive(_Keyed, "Base", __coco_memo_type_id__="test.Base/v1")
    sub = _derive(base, "Sub")
    case.register(sub, stable_type_id="test.Sub/v1")
    return sub(1), base(1)


def _non_str_declared_id(declared: object) -> _Build:
    def build(case: _Case) -> tuple[object, object]:
        # Shadows the base's ID, and is then ignored like a missing one.
        base = _derive(_Keyed, "Base", __coco_memo_type_id__="test.Base/v1")
        entry = _derive(base, "Entry", __coco_memo_type_id__=declared)
        twin = _derive(_Keyed, "Twin")
        case.register(entry, stable_type_id="test.Registered/v1")
        case.register(twin, stable_type_id="test.Registered/v1")
        return entry(1), twin(1)

    return build


def _metaclass_declared_id(class_objects: bool) -> _Build:
    def build(case: _Case) -> tuple[object, object]:
        class FamilyMeta(type):
            __coco_memo_type_id__ = "test.Family/v1"

        @dataclasses.dataclass
        class Row1(metaclass=FamilyMeta):
            x: int

        @dataclasses.dataclass
        class Row2(metaclass=FamilyMeta):
            x: int

        return (Row1, Row2) if class_objects else (Row1(x=1), Row2(x=1))

    return build


def _old_and_moved(marker: str) -> tuple[Any, Any]:
    old = _derive(_Keyed, "SourceEntry", _OLD_MODULE)
    moved = _derive(_Keyed, "Moved", __coco_memo_type_id__=marker)
    return old, moved


def _prev_type_id_matches_old_identity(case: _Case) -> tuple[object, object]:
    old, moved = _old_and_moved(prev_type_id(_OLD_MODULE, "SourceEntry"))
    return old(1), moved(1)


def _prev_type_id_matches_old_class_object(case: _Case) -> tuple[object, object]:
    return _old_and_moved(prev_type_id(_OLD_MODULE, "SourceEntry"))


def _prev_type_id_names_another_type(case: _Case) -> tuple[object, object]:
    old, moved = _old_and_moved(prev_type_id(_OLD_MODULE, "OtherEntry"))
    return old(1), moved(1)


def _string_id_never_matches_automatic_identity(case: _Case) -> tuple[object, object]:
    old, moved = _old_and_moved(f"{_OLD_MODULE}.SourceEntry")
    return old(1), moved(1)


def _prev_type_id_survives_pickling(case: _Case) -> tuple[object, object]:
    marker = pickle.loads(pickle.dumps(prev_type_id(_OLD_MODULE, "SourceEntry")))
    old, moved = _old_and_moved(marker)
    return old(1), moved(1)


def _prev_type_id_names_main_by_script(case: _Case) -> tuple[object, object]:
    main = sys.modules["__main__"]
    case.monkeypatch.setattr(main, "__file__", "/tmp/main.py", raising=False)
    old = _derive(_Keyed, "SourceEntry", "__main__")
    moved = _derive(
        _Keyed, "Moved", __coco_memo_type_id__=prev_type_id("main", "SourceEntry")
    )
    return old(1), moved(1)


def _prev_type_id_on_base_reaches_subclass(case: _Case) -> tuple[object, object]:
    old = _derive(_Keyed, "SourceBase", _OLD_MODULE)
    moved = _derive(
        _Keyed,
        "MovedBase",
        __coco_memo_type_id__=prev_type_id(_OLD_MODULE, "SourceBase"),
    )
    return _derive(moved, "MovedSub")(1), old(1)


def _prev_type_id_for_registered_key_owner(case: _Case) -> tuple[object, object]:
    old = _derive(_Unkeyed, "SourceBase", _OLD_MODULE)
    moved = _derive(_Unkeyed, "MovedBase")
    case.register(old, _value_key)
    case.register(
        moved, _value_key, stable_type_id=prev_type_id(_OLD_MODULE, "SourceBase")
    )
    return _derive(old, "Child")(1), _derive(moved, "Child")(1)


def _none_declared_id_shadows_key_owner_id(case: _Case) -> tuple[object, object]:
    # `None` on Child hides Base's declared ID, so the automatic fallback is
    # Base's module and qualified name, not its declared ID.
    base = _derive(_Unkeyed, "Base", _OLD_MODULE, __coco_memo_type_id__="test.Base/v1")
    case.register(base, _value_key)
    child = _derive(base, "Child", __coco_memo_type_id__=None)
    ref = _derive(
        _Keyed, "Ref", __coco_memo_type_id__=prev_type_id(_OLD_MODULE, "Base")
    )
    return child(1), ref(1)


def _prev_type_id_for_registered_metaclass(case: _Case) -> tuple[object, object]:
    old = _derive(type, "SourceMeta", _OLD_MODULE)
    moved = _derive(type, "MovedMeta")
    case.register(old, lambda cls: "class")
    case.register(
        moved,
        lambda cls: "class",
        stable_type_id=prev_type_id(_OLD_MODULE, "SourceMeta"),
    )
    return old("Entry", (), {}), moved("Entry", (), {})


def _registered_id_without_key_fn(case: _Case) -> tuple[object, object]:
    old, new = _derive(_Keyed, "Old"), _derive(_Keyed, "New")
    case.register(old, stable_type_id="test.Entry/v1")
    case.register(new, state_fn=None, stable_type_id="test.Entry/v1")
    return old(1), new(1)


def _registered_id_reaches_subclass(class_objects: bool) -> _Build:
    def build(case: _Case) -> tuple[object, object]:
        parent = _derive(_Keyed, "Parent")
        child = _derive(parent, "Child")
        case.register(parent, stable_type_id="test.Parent/v1")
        return (child, parent) if class_objects else (child(1), parent(1))

    return build


def _subclass_registration_overrides_base_id(case: _Case) -> tuple[object, object]:
    parent = _derive(_Keyed, "Parent")
    child = _derive(parent, "Child")
    case.register(parent, stable_type_id="test.Parent/v1")
    case.register(child, stable_type_id="test.Child/v1")
    return child(1), parent(1)


def _registered_key_owner_id_reaches_subclass(case: _Case) -> tuple[object, object]:
    base = _derive(_Unkeyed, "Base")
    case.register(base, _value_key, stable_type_id="test.Base/v1")
    ref = _derive(_Keyed, "Ref", __coco_memo_type_id__="test.Base/v1")
    return _derive(base, "Sub")(1, "ignored"), ref(1)


def _subclass_id_beats_registered_key_owner_id(declared: bool) -> _Build:
    def build(case: _Case) -> tuple[object, object]:
        # The base's key function still applies; only the identity changes.
        base = _derive(_Unkeyed, "Base")
        case.register(base, _value_key, stable_type_id="test.Base/v1")
        if declared:
            sub = _derive(base, "Sub", __coco_memo_type_id__="test.Sub/v1")
        else:
            sub = _derive(base, "Sub")
            case.register(sub, stable_type_id="test.Sub/v1")
        ref = _derive(_Keyed, "Ref", __coco_memo_type_id__="test.Sub/v1")
        return sub(1, "ignored"), ref(1)

    return build


def _declared_id_beats_key_owner_registration(case: _Case) -> tuple[object, object]:
    base = _derive(_Unkeyed, "Base", __coco_memo_type_id__="test.Declared/v1")
    case.register(base, _value_key, stable_type_id="test.Registered/v1")
    ref = _derive(_Keyed, "Ref", __coco_memo_type_id__="test.Declared/v1")
    return _derive(base, "Sub")(1), ref(1)


def _own_memo_key_beats_registration(case: _Case) -> tuple[object, object]:
    def must_not_run(*args: object) -> object:
        raise AssertionError("the type's own memo key wins over its registration")

    entry = _derive(_Keyed, "Entry", __coco_memo_type_id__="test.Declared/v1")
    case.register(
        entry, must_not_run, state_fn=must_not_run, stable_type_id="test.Registered/v1"
    )
    twin = _derive(_Keyed, "Twin", __coco_memo_type_id__="test.Declared/v1")
    return entry(1), twin(1)


def _class_object_ignores_own_memo_key(case: _Case) -> tuple[object, object]:
    def must_not_run(*args: object) -> object:
        raise AssertionError("class objects never call __coco_memo_key__")

    attrs = {"__coco_memo_type_id__": "test.Class/v1"}
    old = _derive(object, "Old", __coco_memo_key__=staticmethod(must_not_run), **attrs)
    new = _derive(object, "New", __coco_memo_key__=classmethod(must_not_run), **attrs)
    return old, new


def _registered_id_applies_to_class_objects(case: _Case) -> tuple[object, object]:
    old, new = _derive(_Unkeyed, "Old"), _derive(_Unkeyed, "New")
    case.register(old, stable_type_id="test.Class/v1")
    case.register(new, stable_type_id="test.Class/v1")
    return old, new


def _class_object_key_registered_on(metaclass: bool) -> _Build:
    def build(case: _Case) -> tuple[object, object]:
        # A class keyed through its metaclass, or through `type` further up the
        # metaclass's MRO, takes the stable type ID registered with the key function.
        meta = _derive(type, "Meta")
        case.register(
            meta if metaclass else type,
            lambda cls: ("entry", 1),
            stable_type_id="test.Key/v1",
        )
        ref = _derive(_Keyed, "Ref", __coco_memo_type_id__="test.Key/v1")
        return meta("Entry", (), {}), ref(1)

    return build


def _class_object_ignores_own_id_under_key_owner(declared: bool) -> _Build:
    def build(case: _Case) -> tuple[object, object]:
        # A metaclass key function with its own ID; the classes' own, differing
        # declared IDs or registrations do not apply to the class values.
        meta = _derive(type, "Meta")
        case.register(meta, lambda cls: "same", stable_type_id="test.Meta/v1")
        base = meta("Base", (_Unkeyed,), {})
        ids = ("test.Own/v1", "test.Other/v1")
        if declared:
            return (
                _derive(base, "One", __coco_memo_type_id__=ids[0]),
                _derive(base, "Two", __coco_memo_type_id__=ids[1]),
            )
        one, two = _derive(base, "One"), _derive(base, "Two")
        case.register(one, stable_type_id=ids[0])
        case.register(two, stable_type_id=ids[1])
        return one, two

    return build


def _class_objects_ignore_object_key_fn(case: _Case) -> tuple[object, object]:
    case.register(object, lambda obj: "same", stable_type_id="test.Object/v1")
    return _derive(_Unkeyed, "Entry"), _derive(_Unkeyed, "Other")


def _class_object_distinct_from_its_instance(
    case: _Case,
) -> tuple[object, object]:
    # The class takes the metaclass identity while the instance takes the
    # class identity, so the same payload and state do not collide.
    meta = _derive(type, "Meta")
    base = meta("Base", (_Unkeyed,), {})
    entry = _derive(
        base,
        "Entry",
        __coco_memo_type_id__="test.Entry/v1",
        __coco_memo_key__=lambda self: ("same", "payload"),
        __coco_memo_state__=lambda self, prev: None,
    )
    case.register(
        meta,
        lambda cls: ("same", "payload"),
        state_fn=lambda cls, prev: None,
    )
    return entry, entry(1)


def _reregistering_drops_state_fn(case: _Case) -> tuple[object, object]:
    entry, twin = _derive(_Unkeyed, "Entry"), _derive(_Unkeyed, "Twin")
    case.register(
        entry, _value_key, state_fn=lambda e, prev: prev, stable_type_id="test.Entry/v1"
    )
    case.register(entry, _value_key, stable_type_id="test.Entry/v1")
    case.register(twin, _value_key, stable_type_id="test.Entry/v1")
    return entry(1), twin(1)


def _reregistering_drops_stable_id(via: Callable[..., None]) -> _Build:
    def build(case: _Case) -> tuple[object, object]:
        entry, twin = _derive(_Unkeyed, "Entry"), _derive(_Unkeyed, "Twin")
        case.register(entry, _value_key, stable_type_id="test.Entry/v1")
        case.register(entry, _value_key, via=via)
        case.register(twin, _value_key, stable_type_id="test.Entry/v1")
        return entry(1), twin(1)

    return build


def _reregistering_drops_key_fn(case: _Case) -> tuple[object, object]:
    entry = dataclasses.make_dataclass("Entry", ["value", "ignored"])
    case.register(entry, _value_key)
    case.register(entry, stable_type_id="test.Entry/v1")
    return entry(1, "a"), entry(1, "b")


def _not_memo_keyable_drops_stable_id(case: _Case) -> tuple[object, object]:
    entry, twin = _derive(_Unkeyed, "Entry"), _derive(_Unkeyed, "Twin")
    case.register(entry, stable_type_id="test.Entry/v1")
    case.register(entry, via=register_not_memo_keyable)
    case.register(twin, stable_type_id="test.Entry/v1")
    return entry, twin


def _registration_after_first_fingerprint(case: _Case) -> tuple[object, object]:
    # The first fingerprint resolves Entry's identity before any registration.
    entry, twin = _derive(_Keyed, "Entry"), _derive(_Keyed, "Twin")
    memo_fingerprint(entry(1))
    case.register(entry, stable_type_id="test.Entry/v1")
    case.register(twin, stable_type_id="test.Entry/v1")
    return entry(1), twin(1)


def _unregistration_after_first_fingerprint(case: _Case) -> tuple[object, object]:
    entry, twin = _derive(_Keyed, "Entry"), _derive(_Keyed, "Twin")
    case.register(entry, stable_type_id="test.Entry/v1")
    case.register(twin, stable_type_id="test.Entry/v1")
    memo_fingerprint(entry(1))
    unregister_memo_type(entry)
    return entry(1), twin(1)


def _memo_key_leads_back_to_parent(registered: bool) -> _Build:
    def build(case: _Case) -> tuple[object, object]:
        def parent_key(node: Any) -> object:
            return node.value

        if registered:
            node = _derive(_Unkeyed, "Node")
            case.register(node, parent_key)
        else:
            node = _derive(_Unkeyed, "Node", __coco_memo_key__=parent_key)

        def graph() -> list[object]:
            parent: list[object] = []
            parent.append(node(parent))
            return parent

        return graph(), graph()

    return build


@pytest.mark.parametrize(
    ("build", "same"),
    [
        # Declared IDs
        pytest.param(_declared_id_survives_module_move, True, id="declared-moved"),
        pytest.param(_declared_id_versions_differ, False, id="declared-versions"),
        pytest.param(
            _pydantic_declared_id_survives_rename, True, id="declared-pydantic"
        ),
        pytest.param(_declared_id_is_inherited, True, id="declared-inherited"),
        pytest.param(_subclass_redeclares_id, False, id="declared-overridden"),
        pytest.param(
            _inherited_declared_id_beats_own_registration,
            True,
            id="declared-beats-registered",
        ),
        pytest.param(_non_str_declared_id(None), True, id="declared-none"),
        pytest.param(_non_str_declared_id(12345), True, id="declared-non-str"),
        pytest.param(_metaclass_declared_id(False), False, id="metaclass-instances"),
        pytest.param(_metaclass_declared_id(True), False, id="metaclass-classes"),
        # prev_type_id
        pytest.param(_prev_type_id_matches_old_identity, True, id="prev-instance"),
        pytest.param(_prev_type_id_matches_old_class_object, True, id="prev-class"),
        pytest.param(_prev_type_id_names_another_type, False, id="prev-other-type"),
        pytest.param(
            _string_id_never_matches_automatic_identity, False, id="string-id-tagged"
        ),
        pytest.param(_prev_type_id_survives_pickling, True, id="prev-pickled"),
        pytest.param(_prev_type_id_names_main_by_script, True, id="prev-main"),
        pytest.param(_prev_type_id_on_base_reaches_subclass, True, id="prev-inherited"),
        pytest.param(_prev_type_id_for_registered_key_owner, True, id="prev-key-owner"),
        pytest.param(
            _none_declared_id_shadows_key_owner_id,
            True,
            id="none-declared-shadows-key-owner-id",
        ),
        pytest.param(_prev_type_id_for_registered_metaclass, True, id="prev-metaclass"),
        # Registered IDs
        pytest.param(_registered_id_without_key_fn, True, id="registered-id-only"),
        pytest.param(
            _registered_id_reaches_subclass(False), True, id="registered-inherited"
        ),
        pytest.param(
            _registered_id_reaches_subclass(True), True, id="registered-inherited-class"
        ),
        pytest.param(
            _subclass_registration_overrides_base_id, False, id="registered-overridden"
        ),
        pytest.param(
            _registered_key_owner_id_reaches_subclass, True, id="key-owner-id-inherited"
        ),
        pytest.param(
            _subclass_id_beats_registered_key_owner_id(True),
            True,
            id="key-owner-id-overridden-by-declared",
        ),
        pytest.param(
            _subclass_id_beats_registered_key_owner_id(False),
            True,
            id="key-owner-id-overridden-by-registered",
        ),
        pytest.param(
            _declared_id_beats_key_owner_registration,
            True,
            id="key-owner-declared-beats-registered",
        ),
        pytest.param(_own_memo_key_beats_registration, True, id="own-memo-key-wins"),
        # Class objects
        pytest.param(
            _class_object_ignores_own_memo_key, True, id="class-own-key-unused"
        ),
        pytest.param(
            _registered_id_applies_to_class_objects, True, id="class-registered-id"
        ),
        pytest.param(
            _class_object_key_registered_on(True), True, id="class-key-metaclass"
        ),
        pytest.param(_class_object_key_registered_on(False), True, id="class-key-type"),
        pytest.param(
            _class_object_ignores_own_id_under_key_owner(True),
            True,
            id="class-own-declared-id-differs",
        ),
        pytest.param(
            _class_object_ignores_own_id_under_key_owner(False),
            True,
            id="class-own-registered-id-differs",
        ),
        pytest.param(
            _class_object_distinct_from_its_instance,
            False,
            id="class-object-vs-instance",
        ),
        pytest.param(_class_objects_ignore_object_key_fn, False, id="class-object-key"),
        # Every registration replaces the type's whole configuration
        pytest.param(_reregistering_drops_state_fn, True, id="replace-state-fn"),
        pytest.param(
            _reregistering_drops_stable_id(register_memo_type), False, id="replace-id"
        ),
        pytest.param(
            _reregistering_drops_stable_id(register_memo_key_function),
            False,
            id="replace-id-via-key-function-shortcut",
        ),
        pytest.param(_reregistering_drops_key_fn, False, id="replace-key-fn"),
        pytest.param(
            _not_memo_keyable_drops_stable_id, False, id="replace-id-via-not-keyable"
        ),
        # Registry changes apply to types already fingerprinted
        pytest.param(
            _registration_after_first_fingerprint, True, id="register-after-use"
        ),
        pytest.param(
            _unregistration_after_first_fingerprint, False, id="unregister-after-use"
        ),
        # A memo key fragment shares the walk of the value holding it
        pytest.param(_memo_key_leads_back_to_parent(False), True, id="cycle-own-key"),
        pytest.param(_memo_key_leads_back_to_parent(True), True, id="cycle-registered"),
    ],
)
def test_type_identity(
    build: _Build, same: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = _Case(monkeypatch)
    try:
        a, b = build(case)
        assert (memo_fingerprint(a) == memo_fingerprint(b)) is same
    finally:
        for typ in case.registered:
            unregister_memo_type(typ)


@pytest.mark.parametrize(
    ("typ", "args", "kwargs", "match"),
    [
        (object(), (), {"stable_type_id": "test.Entry/v1"}, "expects typ to be a type"),
        (_Unkeyed, (), {}, "requires a key_fn or stable_type_id"),
        (_Unkeyed, (object(),), {}, "key_fn must be callable"),
        (_Unkeyed, (_value_key,), {"state_fn": object()}, "state_fn must be callable"),
        (
            _Unkeyed,
            (),
            {"state_fn": _value_key},
            "state_fn requires a memo key function",
        ),
        (_Unkeyed, (), {"stable_type_id": 123}, "stable_type_id must be a str"),
    ],
)
def test_register_memo_type_rejects_invalid_calls(
    typ: Any, args: tuple[Any, ...], kwargs: dict[str, Any], match: str
) -> None:
    try:
        with pytest.raises(TypeError, match=match):
            cast(Any, register_memo_type)(typ, *args, **kwargs)
    finally:
        unregister_memo_type(typ)


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


@pytest.mark.parametrize(
    "shared",
    [[1, 2], {"x": [1, 2]}, ([1, 2],), {1, 2}, frozenset({1, 2})],
    ids=["list", "dict", "tuple", "set", "frozenset"],
)
def test_shared_values_match_independent_values(shared: object) -> None:
    aliased = [shared, shared]
    independent = [copy.deepcopy(shared), copy.deepcopy(shared)]
    assert memo_fingerprint(aliased) == memo_fingerprint(independent)
    assert fingerprint_call(_dummy_fn, (aliased,), {}, []) == fingerprint_call(
        _dummy_fn, (independent,), {}, []
    )
    assert memo_fingerprint({"a": shared, "b": shared}) == memo_fingerprint(
        {"b": copy.deepcopy(shared), "a": copy.deepcopy(shared)}
    )


def test_freed_memo_keys_are_not_back_references() -> None:
    # A key returned by `__coco_memo_key__` (or by the built-in key function
    # for classes) is freed once encoded, so the next one can reuse its `id()`.
    class Row:
        def __init__(self, uid: int, ts: str) -> None:
            self.uid, self.ts = uid, ts

        def __coco_memo_key__(self) -> object:
            return (self.uid, self.ts)

    class A:
        pass

    class B:
        pass

    class C:
        pass

    assert memo_fingerprint([Row(1, "t1"), Row(2, "t2")]) != memo_fingerprint(
        [Row(1, "t1"), Row(3, "t3")]
    )
    assert fingerprint_call(_dummy_fn, ([A, B],), {}, []) != fingerprint_call(
        _dummy_fn, ([A, C],), {}, []
    )


def test_shared_dataclass_matches_independent_values() -> None:
    @dataclasses.dataclass
    class Value:
        items: list[int]

    shared = Value([1, 2])
    assert memo_fingerprint([shared, shared]) == memo_fingerprint(
        [Value([1, 2]), Value([1, 2])]
    )


def test_cycles_after_completed_siblings_are_order_independent() -> None:
    first: list[object] = []
    first.append(first)
    second: list[object] = []
    second.append(second)
    assert memo_fingerprint({"items": [1, 2], "cycle": first}) == memo_fingerprint(
        {"cycle": second, "items": [1, 2]}
    )
    assert memo_fingerprint([first, first]) == memo_fingerprint([first, second])


def test_different_cycle_targets_do_not_collide() -> None:
    root: list[object] = []
    child: list[object] = [root]
    root.append(child)
    self_cycle: list[object] = []
    self_cycle.append(self_cycle)
    assert memo_fingerprint(root) != memo_fingerprint([self_cycle])


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
        unregister_memo_type(Registered)


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


def _canonical_form_fingerprint(obj: object) -> core.Fingerprint:
    return core.fingerprint_simple_object(
        _canonicalize(obj, _seen=None, state_methods=[])
    )


class _ListSubclass(list[Any]):
    pass


class _StrSubclass(str):
    pass


class _IntEnum(enum.IntEnum):
    A = 1


_NamedTuple = collections.namedtuple("_NamedTuple", ["a"])

_SHARED_LIST = [1, 2]
_CYCLIC_LIST: list[Any] = []
_CYCLIC_LIST.append(_CYCLIC_LIST)


def _nested_list(depth: int) -> list[Any]:
    outer: list[Any] = []
    for _ in range(depth):
        outer = [outer]
    return outer


@pytest.mark.parametrize(
    "obj",
    [
        None,
        True,
        0,
        -(2**63),
        2**63,
        -(2**70),
        1.5,
        -0.0,
        math.nan,
        math.inf,
        "",
        "\U0001f600",
        b"\x00x",
        [],
        (),
        {},
        [1, "a", None, [2.0, (b"b", False)]],
        # Keys whose UTF-16 and code point orders differ, declared unsorted.
        {"\U0001f600": 1, "\uffff": 2, "b": 3, "": 4, "a": [5, {"z": 6, "y": ()}]},
        _nested_list(63),
    ],
    ids=repr,
)
def test_plain_data_is_fingerprinted_natively_as_its_canonical_form(
    obj: object,
) -> None:
    fp = core.fingerprint_plain_object(obj)
    assert fp == _canonical_form_fingerprint(obj)
    assert memo_fingerprint(obj) == fp


@pytest.mark.parametrize(
    "obj",
    [
        {1: "a", 2.0: "b", (1, 2): "c", None: "d"},
        {"a": {1, 2}},
        bytearray(b"x"),
        [(), ()],
        {"a": _SHARED_LIST, "b": _SHARED_LIST},
        _CYCLIC_LIST,
        _nested_list(64),
        _ListSubclass([1]),
        _StrSubclass("x"),
        _IntEnum.A,
        _NamedTuple(1),
        [_PickleableZ],
    ],
    ids=repr,
)
def test_non_plain_data_is_fingerprinted_through_the_canonicalizer(
    obj: object,
) -> None:
    assert core.fingerprint_plain_object(obj) is None
    assert memo_fingerprint(obj) == _canonical_form_fingerprint(obj)


def test_memo_fingerprint_of_plain_data_is_stable() -> None:
    # Fingerprints are persisted (e.g. as target state tracking records), so the
    # value for a given object must not change across versions.
    row = {"id": 1, "name": "a", "score": 0.5, "tags": ["x", "y"], "extra": None}
    assert memo_fingerprint(row).as_bytes().hex() == "ca5493129c7af70c360e6bfc6d4e0a4b"
    mixed = [2**70, -5, float("nan"), b"b", True, {"k": (1,)}]
    assert (
        memo_fingerprint(mixed).as_bytes().hex() == "f5a39e4f0b6dbe16fe36856fba5fe012"
    )


def test_memo_fingerprint_of_class_objects_is_stable() -> None:
    # Values measured on main, where a class object is keyed by a temporary
    # (module, qualname) tuple; the cycle after it checks back-reference ordinals.
    schema = type("Schema", (), {"__module__": "app.models"})
    cycle: list[object] = []
    cycle.append(cycle)
    assert memo_fingerprint(schema).as_bytes().hex() == (
        "f4682ab705e4b07c4906721398444971"
    )
    assert memo_fingerprint([schema, cycle]).as_bytes().hex() == (
        "a6716187b9dbbbcdb9a9db2a1b988de5"
    )


def test_memo_fingerprint_honors_registry_for_plain_container_types() -> None:
    register_memo_key_function(list, lambda obj: len(obj))
    try:
        fp = memo_fingerprint({"a": [1, 2]})
        assert fp == memo_fingerprint({"a": [3, 4]})
        assert fp == _canonical_form_fingerprint({"a": [1, 2]})
    finally:
        unregister_memo_type(list)
    assert memo_fingerprint({"a": [1, 2]}) != fp
