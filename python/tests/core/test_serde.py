"""Comprehensive tests for cocoindex._internal.serde serialize/deserialize."""

import datetime
import pathlib
import pickle
import uuid
from dataclasses import dataclass
from typing import Any, NamedTuple

import msgspec.msgpack
import pytest

from cocoindex._internal.serde import (
    DeserializeFn,
    _strict_pickle_dumps,
    deserialize,
    enable_strict_serialize,
    make_deserialize_fn,
    serialize,
    serialize_by_pickle,
    unpickle_safe,
)


# ---------------------------------------------------------------------------
# Module-level test types (required for pickle resolution)
# ---------------------------------------------------------------------------


@dataclass
class _Inner:
    tag: str


@dataclass
class _Outer:
    name: str
    value: int
    items: list[int]
    maybe: str | None
    nested: _Inner


class _MyTuple(NamedTuple):
    x: int
    y: str
    z: float


@serialize_by_pickle
@dataclass
class _PickledDC:
    a: int
    b: str


@unpickle_safe
@dataclass
class _SafeDC:
    v: int


@unpickle_safe
class _UnregisteredLike:
    """A type registered via @unpickle_safe but NOT @serialize_by_pickle."""

    def __init__(self, val: int) -> None:
        self.val = val

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _UnregisteredLike) and self.val == other.val


class _NotRegistered:
    """A type that is neither @serialize_by_pickle nor @unpickle_safe."""

    def __init__(self, v: int) -> None:
        self.v = v


# Module-level pydantic model decorated with @serialize_by_pickle for test 9.
# Defined at module level so pickle can resolve it.
try:
    import pydantic as _pydantic

    @serialize_by_pickle
    class _PydanticPickled(_pydantic.BaseModel):
        x: int

except ImportError:
    _PydanticPickled = None  # type: ignore[assignment, misc]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _routing_byte(data: bytes) -> int:
    return data[0]


# ===========================================================================
# Test classes
# ===========================================================================


class TestRoundtripPrimitives:
    """Test 1: int, float, str, bytes, bool, None round-trip with 0x01."""

    @pytest.mark.parametrize(
        "value",
        [42, 3.14, "hello", b"raw", True, False, None],
        ids=["int", "float", "str", "bytes", "true", "false", "none"],
    )
    def test_primitive_roundtrip(self, value: Any) -> None:
        data = serialize(value)
        assert _routing_byte(data) == 0x01
        result = deserialize(data, type(value) if value is not None else type(None))
        assert result == value
        assert type(result) is type(value) if value is not None else result is None


class TestRoundtripCollections:
    """Test 2: list[int], dict[str, float], tuple[int, str], set[int]."""

    def test_list_int(self) -> None:
        value = [1, 2, 3]
        data = serialize(value)
        assert _routing_byte(data) == 0x01
        assert deserialize(data, list[int]) == value

    def test_dict_str_float(self) -> None:
        value = {"a": 1.0, "b": 2.5}
        data = serialize(value)
        assert _routing_byte(data) == 0x01
        assert deserialize(data, dict[str, float]) == value

    def test_tuple_int_str(self) -> None:
        value = (1, "two")
        data = serialize(value)
        assert _routing_byte(data) == 0x01
        assert deserialize(data, tuple[int, str]) == value

    def test_set_int(self) -> None:
        value = {10, 20, 30}
        data = serialize(value)
        assert _routing_byte(data) == 0x01
        assert deserialize(data, set[int]) == value


class TestRoundtripStdlibTypes:
    """Test 3: datetime, date, timedelta, UUID."""

    def test_datetime(self) -> None:
        value = datetime.datetime(2025, 1, 15, 12, 30, 45)
        data = serialize(value)
        assert _routing_byte(data) == 0x01
        assert deserialize(data, datetime.datetime) == value

    def test_date(self) -> None:
        value = datetime.date(2025, 6, 1)
        data = serialize(value)
        assert _routing_byte(data) == 0x01
        assert deserialize(data, datetime.date) == value

    def test_timedelta(self) -> None:
        value = datetime.timedelta(days=1)
        data = serialize(value)
        assert _routing_byte(data) == 0x01
        assert deserialize(data, datetime.timedelta) == value

    def test_uuid(self) -> None:
        value = uuid.UUID("12345678-1234-5678-1234-567812345678")
        data = serialize(value)
        assert _routing_byte(data) == 0x01
        assert deserialize(data, uuid.UUID) == value


class TestRoundtripDataclass:
    """Test 4: dataclass with nested fields."""

    def test_dataclass_roundtrip(self) -> None:
        value = _Outer(
            name="test",
            value=42,
            items=[1, 2, 3],
            maybe=None,
            nested=_Inner(tag="inner"),
        )
        data = serialize(value)
        assert _routing_byte(data) == 0x01
        result = deserialize(data, _Outer)
        assert result == value
        assert isinstance(result, _Outer)
        assert isinstance(result.nested, _Inner)


class TestRoundtripNamedTuple:
    """Test 5: NamedTuple round-trip."""

    def test_namedtuple_roundtrip(self) -> None:
        value = _MyTuple(x=10, y="hello", z=3.14)
        data = serialize(value)
        assert _routing_byte(data) == 0x01
        result = deserialize(data, _MyTuple)
        assert result == value
        assert isinstance(result, _MyTuple)
        assert result.x == 10
        assert result.y == "hello"


class TestRoundtripPydantic:
    """Test 6: Pydantic BaseModel round-trip with 0x02."""

    def test_pydantic_roundtrip(self) -> None:
        pydantic = pytest.importorskip("pydantic")

        class MyModel(pydantic.BaseModel):  # type: ignore[misc,name-defined]
            name: str
            age: int

        value = MyModel(name="Alice", age=30)
        data = serialize(value)
        assert _routing_byte(data) == 0x02
        result = deserialize(data, MyModel)
        assert result == value
        assert isinstance(result, MyModel)


class TestRoundtripSerializeByPickle:
    """Test 7: @serialize_by_pickle dataclass round-trip with 0x80."""

    def test_pickle_dataclass_roundtrip(self) -> None:
        value = _PickledDC(a=99, b="pickle")
        data = serialize(value)
        assert _routing_byte(data) == 0x80
        result = deserialize(data)
        assert result == value
        assert isinstance(result, _PickledDC)


class TestRoundtripBuiltinPickleTypes:
    """Test 8: complex, pathlib, numpy use pickle (0x80)."""

    def test_complex(self) -> None:
        value = complex(1, 2)
        data = serialize(value)
        assert _routing_byte(data) == 0x80
        result = deserialize(data)
        assert result == value

    def test_pathlib(self) -> None:
        value = pathlib.PurePosixPath("/a/b")
        data = serialize(value)
        assert _routing_byte(data) == 0x80
        result = deserialize(data)
        assert result == value

    def test_numpy_array(self) -> None:
        np = pytest.importorskip("numpy")
        value = np.array([1.0, 2.0, 3.0])
        data = serialize(value)
        assert _routing_byte(data) == 0x80
        result = deserialize(data)
        assert np.array_equal(result, value)


class TestSerializePriority:
    """Test 9: @serialize_by_pickle on pydantic model -- pickle wins."""

    def test_pickle_wins_over_pydantic(self) -> None:
        pytest.importorskip("pydantic")
        assert _PydanticPickled is not None

        value = _PydanticPickled(x=5)
        data = serialize(value)
        assert _routing_byte(data) == 0x80
        result = deserialize(data)
        assert result.x == 5


class TestNestedCrossPollination:
    """Tests 10-12: cross-format nesting."""

    def test_dataclass_with_pydantic_field(self) -> None:
        """Test 10: Pydantic model inside a dataclass -- dec_hook reconstructs."""
        pydantic = pytest.importorskip("pydantic")

        class Inner(pydantic.BaseModel):  # type: ignore[misc,name-defined]
            v: int

        @dataclass
        class Wrapper:
            label: str
            inner: Inner

        value = Wrapper(label="test", inner=Inner(v=7))
        data = serialize(value)
        assert _routing_byte(data) == 0x01
        result = deserialize(data, Wrapper)
        assert result.label == "test"
        assert isinstance(result.inner, Inner)
        assert result.inner.v == 7

    def test_dataclass_with_pathlib_field_any(self) -> None:
        """Test 11: pathlib inside a dataclass with Any field -- quarantined as ExtType(100)."""

        @dataclass
        class WithPath:
            name: str
            path: Any  # Any allows ext_hook to unquarantine the pickled pathlib

        value = WithPath(name="f", path=pathlib.PurePosixPath("/x/y"))
        data = serialize(value)
        assert _routing_byte(data) == 0x01
        # Deserialize with Any hint (ext_hook handles ExtType(100))
        result = deserialize(data, WithPath)
        assert result.name == "f"
        assert result.path == pathlib.PurePosixPath("/x/y")

    def test_pydantic_with_pathlib_field(self) -> None:
        """Test 12: pathlib inside a Pydantic model."""
        pydantic = pytest.importorskip("pydantic")

        class WithPath(pydantic.BaseModel):  # type: ignore[misc,name-defined]
            name: str
            path: str  # Pydantic serializes as json mode, path becomes str

        value = WithPath(name="f", path="/x/y")
        data = serialize(value)
        assert _routing_byte(data) == 0x02
        result = deserialize(data, WithPath)
        assert result.name == "f"
        assert result.path == "/x/y"


class TestDeserializeEdgeCases:
    """Tests 13-14, 18: edge cases in deserialization."""

    def test_deserialize_without_type_hint(self) -> None:
        """Test 13: Deserialize dataclass without type hint returns generic dict."""
        value = _Outer(name="x", value=1, items=[], maybe=None, nested=_Inner(tag="t"))
        data = serialize(value)
        result = deserialize(data)  # no type hint -> Any
        # msgspec decodes to a dict when type is Any
        assert isinstance(result, dict)
        assert result["name"] == "x"

    def test_backward_compat_raw_pickle(self) -> None:
        """Test 14: Raw pickle.dumps data is deserialized (backward compat)."""
        value = _PickledDC(a=1, b="compat")
        raw = pickle.dumps(value, 5)
        # pickle protocol 5 starts with 0x80 0x05
        assert raw[0] == 0x80
        result = deserialize(raw)
        assert result == value

    def test_unknown_routing_byte_raises(self) -> None:
        """Test 18: Unknown routing byte raises DeserializationError."""
        from cocoindex._internal.serde import DeserializationError

        data = b"\xff\x00\x00"
        with pytest.raises(DeserializationError, match="Unknown routing byte"):
            deserialize(data)


class TestStrictPickle:
    """Test 15: strict mode rejects unregistered types."""

    def test_strict_rejects_unregistered_via_strict_pickle_dumps(self) -> None:
        """Enable strict, then pickle an unregistered type -> PicklingError."""
        enable_strict_serialize()

        class _TempUnregistered:
            def __init__(self, v: int) -> None:
                self.v = v

        with pytest.raises(pickle.PicklingError, match="not registered"):
            _strict_pickle_dumps(_TempUnregistered(1))


class TestEncHook:
    """Tests 16-17: enc_hook behavior for nested objects."""

    def test_unpickle_safe_nested_in_any_field(self) -> None:
        """Test 16: @unpickle_safe non-dataclass type nested in a dataclass.

        Since _UnregisteredLike is not a dataclass, msgspec cannot encode it
        natively and calls enc_hook. The enc_hook sees the type in
        _UNPICKLE_SAFE_GLOBALS and quarantines it as ExtType(100). On
        deserialization, ext_hook unpickles it back via _RestrictedUnpickler.
        """

        @dataclass
        class Holder:
            label: str
            obj: Any

        value = Holder(label="wrap", obj=_UnregisteredLike(val=42))
        data = serialize(value)
        assert _routing_byte(data) == 0x01
        result = deserialize(data, Holder)
        assert result.label == "wrap"
        assert isinstance(result.obj, _UnregisteredLike)
        assert result.obj.val == 42

    def test_unregistered_nested_raises(self) -> None:
        """Test 17: Unregistered type nested in dataclass -> NotImplementedError
        from enc_hook (the type is not pickle-safe, not pydantic, and not known)."""
        from cocoindex._internal.serde import _enc_hook

        with pytest.raises(NotImplementedError, match="Cannot serialize"):
            _enc_hook(_NotRegistered(1))


class TestDeserializeFn:
    """Tests 19-20: make_deserialize_fn factory."""

    def test_reused_decoder_list_int(self) -> None:
        """Test 19: make_deserialize_fn(list[int]) reuses decoder across calls."""
        fn: DeserializeFn = make_deserialize_fn(list[int])
        data1 = serialize([1, 2, 3])
        data2 = serialize([4, 5])
        assert fn(data1) == [1, 2, 3]
        assert fn(data2) == [4, 5]

    def test_lazy_pydantic_adapter(self) -> None:
        """Test 20: Pydantic adapter created lazily on first 0x02 data."""
        pydantic = pytest.importorskip("pydantic")

        class M(pydantic.BaseModel):  # type: ignore[misc,name-defined]
            x: int

        fn: DeserializeFn = make_deserialize_fn(M)

        # First call with msgspec data (0x01) -- adapter stays None
        dc_data = b"\x01" + msgspec.msgpack.encode({"x": 10})
        result1 = fn(dc_data)
        # When type_hint is a pydantic model but routing byte is 0x01,
        # dec_hook will reconstruct via model_validate
        assert isinstance(result1, M)
        assert result1.x == 10

        # Second call with pydantic data (0x02) -- adapter created lazily
        pydantic_data = serialize(M(x=20))
        assert _routing_byte(pydantic_data) == 0x02
        result2 = fn(pydantic_data)
        assert isinstance(result2, M)
        assert result2.x == 20
