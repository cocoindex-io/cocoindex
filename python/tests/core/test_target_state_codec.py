"""Unit tests for the compact form the engine holds declared target-state values
and their actions in (`cocoindex._internal.target_state_codec`)."""

import collections
import dataclasses
import datetime
import decimal
import enum
import fractions
import ipaddress
import pathlib
import uuid
import zoneinfo
from typing import Any, NamedTuple

import msgspec
import numpy as np
import pytest

from cocoindex._internal import target_state_codec as codec


class _Point(NamedTuple):
    x: int
    y: int


class _Color(enum.Enum):
    RED = 1


@dataclasses.dataclass
class _Record:
    x: int


class _Spec(msgspec.Struct, frozen=True):
    x: int


class _Str(str):
    pass


class _RowAction(NamedTuple):
    key: tuple[Any, ...]
    value: Any


def _assert_exact(actual: Any, expected: Any) -> None:
    """`actual == expected`, with the same types all the way down."""
    assert type(actual) is type(expected), (actual, expected)
    if isinstance(expected, np.ndarray):
        assert actual.dtype == expected.dtype
        assert actual.shape == expected.shape
        assert (actual == expected).all()
    elif isinstance(expected, dict):
        assert list(actual) == list(expected)
        for key in expected:
            _assert_exact(actual[key], expected[key])
    elif isinstance(expected, (list, tuple)):
        assert len(actual) == len(expected)
        for a, e in zip(actual, expected):
            _assert_exact(a, e)
    else:
        assert actual == expected


def _round_trip(value: Any) -> Any:
    encoded = codec.encode_value(value)
    assert isinstance(encoded, bytes), value
    return codec.decode_value(encoded)


_EXACT_VALUES: list[Any] = [
    {"pair": (1, 2), "items": [1, 2], "none": None, "flag": True, "ratio": 0.5},
    (1, [2, (3, "x")]),
    [b"\x00\x01", bytearray(b"ab"), "text"],
    {"set": {1, 2}, "frozen": frozenset({"a"})},
    {1: "int key", 2.5: "float key", (1, 2): "tuple key"},
    {"nested": {"k": [1, {"z": (None, 1j)}]}},
    {
        "price": decimal.Decimal("9.90"),
        "naive": datetime.datetime(2024, 1, 2, 3, 4, 5),
        "utc": datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc),
        "day": datetime.date(2024, 1, 2),
        "clock": datetime.time(3, 4, 5),
        "span": datetime.timedelta(seconds=90),
        "id": uuid.UUID("12345678-1234-5678-1234-567812345678"),
        "third": fractions.Fraction(1, 3),
    },
    {
        "ip": ipaddress.ip_address("10.0.0.1"),
        "ip6": ipaddress.ip_address("::1"),
        "net": ipaddress.ip_network("10.0.0.0/8"),
        "iface": ipaddress.ip_interface("10.0.0.1/24"),
        "path": pathlib.PurePosixPath("/a/b"),
    },
    {
        "vec": np.arange(6, dtype=np.float32).reshape(2, 3),
        "scalar": np.int16(7),
        "dtype": np.dtype("float64"),
    },
    {"point": _Point(1, 2), "color": _Color.RED},
    _Point(3, 4),
]


@pytest.mark.parametrize("value", _EXACT_VALUES)
def test_round_trip_is_exact(value: Any) -> None:
    _assert_exact(_round_trip(value), value)


def test_round_trip_keeps_zoneinfo() -> None:
    try:
        tz = zoneinfo.ZoneInfo("America/New_York")
    except zoneinfo.ZoneInfoNotFoundError:
        pytest.skip("no IANA time zone database (Windows without tzdata)")
    value = {"aware": datetime.datetime(2024, 1, 2, tzinfo=tz)}
    _assert_exact(_round_trip(value), value)


def test_row_is_encoded_without_its_keys() -> None:
    keys = [f"column_{i}" for i in range(15)]
    row = {key: i * 1000 for i, key in enumerate(keys)}
    encoded = codec.encode_value(row)
    assert isinstance(encoded, bytes)
    assert not any(key.encode() in encoded for key in keys)
    decoded = codec.decode_value(encoded)
    _assert_exact(decoded, row)
    # Decoded rows share their key objects.
    assert all(a is b for a, b in zip(decoded, codec.decode_value(encoded)))


def test_enum_member_keeps_its_identity() -> None:
    assert _round_trip({"color": _Color.RED})["color"] is _Color.RED


def test_sharing_within_a_value_is_kept() -> None:
    shared = [1, 2]
    decoded = _round_trip({"a": shared, "b": shared})
    assert decoded["a"] is decoded["b"]
    decoded = _round_trip({"outer": [shared, shared]})
    assert decoded["outer"][0] is decoded["outer"][1]

    cyclic: dict[str, Any] = {"x": 1}
    cyclic["self"] = cyclic
    decoded = _round_trip(cyclic)
    assert decoded["self"] is decoded


_HELD_AS_OBJECTS: list[Any] = [
    None,
    1,
    1.5,
    True,
    "text",
    b"bytes",
    _Record(1),
    {"record": _Record(1)},
    _Spec(1),
    {"fn": lambda: 1},
    {"method": [].append},
    collections.OrderedDict(a=1),
    {_Str("a"): 1},
    [object()],
]

_OVERSIZED: dict[str, Any] = {"text": "x" * (codec._MAX_ENCODED_SIZE + 1)}


@pytest.mark.parametrize("value", _HELD_AS_OBJECTS)
def test_values_left_as_objects(value: Any) -> None:
    assert codec.encode_value(value) is None


def test_oversized_value_is_left_as_object_with_the_size_reached() -> None:
    assert codec.encode_value(_OVERSIZED) == codec._MAX_ENCODED_SIZE


_SPILLED: list[Any] = [*_EXACT_VALUES, _OVERSIZED, None, 1, 1.5, True, "text", b"bytes"]


@pytest.mark.parametrize("value", _SPILLED)
def test_spill_encoding_is_exact_for_scalars_and_any_size(value: Any) -> None:
    encoded = codec.encode_value_for_spill(value)
    assert encoded is not None
    _assert_exact(codec.decode_value(encoded), value)


_NOT_SPILLED: list[Any] = [
    _Record(1),
    {"record": _Record(1)},
    _Spec(1),
    {"fn": lambda: 1},
    collections.OrderedDict(a=1),
    [object()],
]


@pytest.mark.parametrize("value", _NOT_SPILLED)
def test_other_than_plain_data_does_not_spill(value: Any) -> None:
    assert codec.encode_value_for_spill(value) is None


def test_action_spills_standing_alone() -> None:
    action = _RowAction(key=("k", 1), value={"x": [1, (2, 3)], "big": "y" * 100_000})
    encoded = codec.encode_action_for_spill(action)
    assert encoded is not None
    _assert_exact(codec.decode_action(encoded, None), action)
    assert codec.encode_action_for_spill((_Record(1), {"x": 1})) is None


def test_key_of_a_str_subclass_is_not_taken_for_its_schema() -> None:
    assert codec.encode_value({"a": 1}) is not None
    assert codec.encode_value({_Str("a"): 1}) is None


def test_action_refers_to_the_declared_value() -> None:
    value = {"id": 1, "tags": ["distinctive-tag"]}
    value_data = codec.encode_value(value)
    assert isinstance(value_data, bytes)
    decoded_value = codec.decode_value(value_data)
    action = _RowAction(key=("k", 1), value=decoded_value)

    encoded = codec.encode_action(action, decoded_value)
    assert encoded is not None
    data, refers_to_value = encoded
    assert refers_to_value
    assert b"distinctive-tag" not in data
    _assert_exact(codec.decode_action(data, value_data), action)

    # Two references to the value decode to one object.
    twice = (decoded_value, decoded_value)
    data, refers_to_value = codec.encode_action(twice, decoded_value) or (b"", False)
    assert refers_to_value
    first, second = codec.decode_action(data, value_data)
    assert first is second
    _assert_exact(first, value)


def test_action_without_the_value_stands_alone() -> None:
    action = _RowAction(key=("k", 1), value=None)
    encoded = codec.encode_action(action, {"unrelated": 1})
    assert encoded is not None
    data, refers_to_value = encoded
    assert not refers_to_value
    _assert_exact(codec.decode_action(data, None), action)


def test_action_with_other_than_plain_data_is_held_as_object() -> None:
    assert codec.encode_action((_Record(1), {"x": 1}), {"x": 1}) is None
