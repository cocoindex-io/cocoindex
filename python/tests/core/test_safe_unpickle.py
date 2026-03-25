"""Tests for serialization/deserialization in cocoindex._internal.serde."""

import pathlib
import pickle
import uuid
from dataclasses import dataclass
from typing import Any, NamedTuple

import pytest

from cocoindex._internal.serde import (
    add_unpickle_safe_global,
    deserialize,
    serialize,
    serialize_by_pickle,
    unpickle_safe,
)


# -- Test types (defined at module level so pickle can resolve them) ----------


@unpickle_safe
@dataclass
class _Point:
    x: float
    y: float


@unpickle_safe
class _Pair(NamedTuple):
    a: int
    b: str


@dataclass
class _Unregistered:
    value: int


@serialize_by_pickle
@dataclass
class _PickleType:
    value: int


# -- Tests --------------------------------------------------------------------


class TestBuiltinRoundtrip:
    @pytest.mark.parametrize(
        "value,type_hint",
        [
            (True, bool),
            (42, int),
            (3.14, float),
            (1 + 2j, complex),  # complex uses pickle
            ("hello", str),
            (b"bytes", bytes),
            ([1, 2, 3], list),
            ((1, 2), tuple[int, int]),
            ({"a": 1}, dict),
            (None, type(None)),
        ],
    )
    def test_builtin_types_roundtrip(self, value: object, type_hint: Any) -> None:
        assert deserialize(serialize(value), type_hint) == value

    def test_bytearray_roundtrip(self) -> None:
        # bytearray serialized via msgspec becomes bytes; use pickle for exact roundtrip
        ba = bytearray(b"ba")
        data = serialize(ba)
        # msgspec route: bytearray → bytes (acceptable)
        result = deserialize(data, bytearray)
        assert bytes(result) == bytes(ba)

    def test_set_roundtrip(self) -> None:
        # Sets are serialized as arrays by msgspec; need type hint for reconstruction
        s = {1, 2}
        assert deserialize(serialize(s), set[int]) == s

    def test_frozenset_roundtrip(self) -> None:
        fs = frozenset({1, 2})
        assert deserialize(serialize(fs), frozenset[int]) == fs


class TestStdlibRoundtrip:
    def test_pathlib_roundtrip(self) -> None:
        p = pathlib.PurePosixPath("/tmp/foo")
        assert deserialize(serialize(p)) == p

    def test_path_roundtrip(self) -> None:
        p = pathlib.Path("/tmp/bar")
        assert deserialize(serialize(p)) == p

    def test_uuid_roundtrip(self) -> None:
        u = uuid.UUID("12345678-1234-5678-1234-567812345678")
        assert deserialize(serialize(u), uuid.UUID) == u


class TestNumpyRoundtrip:
    def test_ndarray_roundtrip(self) -> None:
        np = pytest.importorskip("numpy")
        arr = np.array([1.0, 2.0], dtype=np.float32)
        result = deserialize(serialize(arr))
        np.testing.assert_array_equal(result, arr)
        assert result.dtype == arr.dtype


class TestRegisteredTypes:
    def test_dataclass_roundtrip(self) -> None:
        p = _Point(1.0, 2.0)
        result = deserialize(serialize(p), _Point)
        assert result == p
        assert isinstance(result, _Point)

    def test_namedtuple_roundtrip(self) -> None:
        p = _Pair(1, "x")
        result = deserialize(serialize(p), _Pair)
        assert result == p
        assert isinstance(result, _Pair)

    def test_nested_containers_with_registered_types(self) -> None:
        data = {"key": [_Point(1.0, 2.0), _Point(3.0, 4.0)]}
        result = deserialize(serialize(data), dict[str, list[_Point]])
        assert result == data
        assert isinstance(result["key"][0], _Point)


class TestRejection:
    def test_unregistered_type_rejected(self) -> None:
        from cocoindex._internal.serde import DeserializationError

        # Use pickle.dumps directly to bypass strict-serialize checking,
        # so we can test that deserialize rejects the unregistered type.
        data = pickle.dumps(_Unregistered(42))
        with pytest.raises(DeserializationError, match="Failed to deserialize pickle"):
            deserialize(data)
        # Verify the original UnpicklingError is chained
        try:
            deserialize(data)
        except DeserializationError as e:
            assert isinstance(e.__cause__, pickle.UnpicklingError)
            assert "Forbidden global" in str(e.__cause__)

    def test_forbidden_global_rejected(self) -> None:
        from cocoindex._internal.serde import DeserializationError

        # pickle.dumps(os.system) on macOS/Linux produces this (posix.system)
        payload = (
            b"\x80\x04\x95\x14\x00\x00\x00\x00\x00\x00\x00"
            b"\x8c\x05posix\x94\x8c\x06system\x94\x93\x94."
        )
        with pytest.raises(DeserializationError, match="Failed to deserialize pickle"):
            deserialize(payload)


class TestAddUnpickleSafeGlobal:
    def test_uuid_roundtrip_via_preregistration(self) -> None:
        # uuid.UUID is pre-registered centrally; verify round-trip works with type hint
        u = uuid.UUID("abcdef01-2345-6789-abcd-ef0123456789")
        assert deserialize(serialize(u), uuid.UUID) == u

    def test_add_unpickle_safe_global(self) -> None:
        # Verify the function registers a global that find_class can resolve
        add_unpickle_safe_global("test_module", "test_name", str)
        from cocoindex._internal.serde import _UNPICKLE_SAFE_GLOBALS

        assert _UNPICKLE_SAFE_GLOBALS[("test_module", "test_name")] is str


class TestSerializeByPickle:
    def test_decorator_registers_type(self) -> None:
        @serialize_by_pickle
        @dataclass
        class _TestType:
            value: int

        from cocoindex._internal.serde import (
            _SERIALIZE_BY_PICKLE_TYPES,
            _UNPICKLE_SAFE_GLOBALS,
        )

        assert _TestType in _SERIALIZE_BY_PICKLE_TYPES
        assert (_TestType.__module__, _TestType.__qualname__) in _UNPICKLE_SAFE_GLOBALS

    def test_roundtrip(self) -> None:
        p = _PickleType(42)
        data = serialize(p)
        assert data[0] == 0x80  # pickle routing byte
        result = deserialize(data)
        assert result == p
        assert isinstance(result, _PickleType)


class TestBackwardCompat:
    def test_old_pickle_data_loads(self) -> None:
        """Old pickle-format data (starting with 0x80) should still load."""
        original = {"key": "value", "num": 42}
        old_data = pickle.dumps(original, 4)
        assert old_data[0] == 0x80
        result = deserialize(old_data)
        assert result == original
