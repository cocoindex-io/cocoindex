"""Tests for restricted unpickling in cocoindex._internal.serde."""

import pathlib
import pickle
import uuid
from dataclasses import dataclass
from typing import NamedTuple

import pytest

from cocoindex._internal.serde import (
    add_unpickle_safe_global,
    deserialize,
    serialize,
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


# -- Tests --------------------------------------------------------------------


class TestBuiltinRoundtrip:
    @pytest.mark.parametrize(
        "value",
        [
            True,
            42,
            3.14,
            1 + 2j,
            "hello",
            b"bytes",
            bytearray(b"ba"),
            [1, 2, 3],
            (1, 2),
            {"a": 1},
            {1, 2},
            frozenset({1, 2}),
            None,
        ],
    )
    def test_builtin_types_roundtrip(self, value: object) -> None:
        assert deserialize(serialize(value)) == value


class TestStdlibRoundtrip:
    def test_pathlib_roundtrip(self) -> None:
        p = pathlib.PurePosixPath("/tmp/foo")
        assert deserialize(serialize(p)) == p

    def test_path_roundtrip(self) -> None:
        p = pathlib.Path("/tmp/bar")
        assert deserialize(serialize(p)) == p

    def test_uuid_roundtrip(self) -> None:
        u = uuid.UUID("12345678-1234-5678-1234-567812345678")
        assert deserialize(serialize(u)) == u


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
        result = deserialize(serialize(p))
        assert result == p
        assert isinstance(result, _Point)

    def test_namedtuple_roundtrip(self) -> None:
        p = _Pair(1, "x")
        result = deserialize(serialize(p))
        assert result == p
        assert isinstance(result, _Pair)

    def test_nested_containers_with_registered_types(self) -> None:
        data = {"key": [_Point(1.0, 2.0), _Point(3.0, 4.0)]}
        result = deserialize(serialize(data))
        assert result == data
        assert isinstance(result["key"][0], _Point)


class TestRejection:
    def test_unregistered_type_rejected(self) -> None:
        data = serialize(_Unregistered(42))
        with pytest.raises(pickle.UnpicklingError, match="Forbidden global"):
            deserialize(data)

    def test_forbidden_global_rejected(self) -> None:
        # pickle.dumps(os.system) on macOS/Linux produces this (posix.system)
        payload = (
            b"\x80\x04\x95\x14\x00\x00\x00\x00\x00\x00\x00"
            b"\x8c\x05posix\x94\x8c\x06system\x94\x93\x94."
        )
        with pytest.raises(pickle.UnpicklingError, match="Forbidden global"):
            deserialize(payload)


class TestAddUnpickleSafeGlobal:
    def test_uuid_roundtrip_via_preregistration(self) -> None:
        # uuid.UUID is pre-registered centrally; verify round-trip works
        u = uuid.UUID("abcdef01-2345-6789-abcd-ef0123456789")
        assert deserialize(serialize(u)) == u

    def test_add_unpickle_safe_global(self) -> None:
        # Verify the function registers a global that find_class can resolve
        add_unpickle_safe_global("test_module", "test_name", str)
        from cocoindex._internal.serde import _UNPICKLE_SAFE_GLOBALS

        assert _UNPICKLE_SAFE_GLOBALS[("test_module", "test_name")] is str
