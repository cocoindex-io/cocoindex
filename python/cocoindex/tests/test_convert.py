import datetime
import uuid
from dataclasses import dataclass, make_dataclass
from typing import Annotated, Any, Callable, Literal, NamedTuple

import numpy as np
import pytest
from numpy.typing import NDArray

import cocoindex
from cocoindex.convert import (
    dump_engine_object,
    encode_engine_value,
    make_engine_value_decoder,
)
from cocoindex.typing import (
    Float32,
    Float64,
    TypeKind,
    Vector,
    encode_enriched_type,
)


@dataclass
class Order:
    order_id: str
    name: str
    price: float
    extra_field: str = "default_extra"


@dataclass
class Tag:
    name: str


@dataclass
class Basket:
    items: list[str]


@dataclass
class Customer:
    name: str
    order: Order
    tags: list[Tag] | None = None


@dataclass
class NestedStruct:
    customer: Customer
    orders: list[Order]
    count: int = 0


class OrderNamedTuple(NamedTuple):
    order_id: str
    name: str
    price: float
    extra_field: str = "default_extra"


class CustomerNamedTuple(NamedTuple):
    name: str
    order: OrderNamedTuple
    tags: list[Tag] | None = None


def build_engine_value_decoder(
    engine_type_in_py: Any, python_type: Any | None = None
) -> Callable[[Any], Any]:
    """
    Helper to build a converter for the given engine-side type (as represented in Python).
    If python_type is not specified, uses engine_type_in_py as the target.
    """
    engine_type = encode_enriched_type(engine_type_in_py)["type"]
    return make_engine_value_decoder([], engine_type, python_type or engine_type_in_py)


def validate_full_roundtrip(
    value: Any,
    value_type: Any = None,
    *other_decoded_values: tuple[Any, Any],
) -> None:
    """
    Validate the given value doesn't change after encoding, sending to engine (using output_type), receiving back and decoding (using input_type).

    `other_decoded_values` is a tuple of (value, type) pairs.
    If provided, also validate the value can be decoded to the other types.
    """
    from cocoindex import _engine  # type: ignore

    def eq(a: Any, b: Any) -> bool:
        if isinstance(a, np.ndarray) and isinstance(b, np.ndarray):
            return np.array_equal(a, b)
        return type(a) == type(b) and not not (a == b)

    encoded_value = encode_engine_value(value)
    value_type = value_type or type(value)
    encoded_output_type = encode_enriched_type(value_type)["type"]
    value_from_engine = _engine.testutil.seder_roundtrip(
        encoded_value, encoded_output_type
    )
    decoder = make_engine_value_decoder([], encoded_output_type, value_type)
    decoded_value = decoder(value_from_engine)
    assert eq(decoded_value, value)

    if other_decoded_values is not None:
        for other_value, other_type in other_decoded_values:
            decoder = make_engine_value_decoder([], encoded_output_type, other_type)
            other_decoded_value = decoder(value_from_engine)
            assert eq(other_decoded_value, other_value)


def test_encode_engine_value_basic_types() -> None:
    assert encode_engine_value(123) == 123
    assert encode_engine_value(3.14) == 3.14
    assert encode_engine_value("hello") == "hello"
    assert encode_engine_value(True) is True


def test_encode_engine_value_uuid() -> None:
    u = uuid.uuid4()
    assert encode_engine_value(u) == u.bytes


def test_encode_engine_value_date_time_types() -> None:
    d = datetime.date(2024, 1, 1)
    assert encode_engine_value(d) == d
    t = datetime.time(12, 30)
    assert encode_engine_value(t) == t
    dt = datetime.datetime(2024, 1, 1, 12, 30)
    assert encode_engine_value(dt) == dt


def test_encode_scalar_numpy_values() -> None:
    """Test encoding scalar NumPy values to engine-compatible values."""
    test_cases = [
        (np.int64(42), 42),
        (np.float32(3.14), pytest.approx(3.14)),
        (np.float64(2.718), pytest.approx(2.718)),
    ]
    for np_value, expected in test_cases:
        encoded = encode_engine_value(np_value)
        assert encoded == expected
        assert isinstance(encoded, (int, float))


def test_encode_engine_value_struct() -> None:
    order = Order(order_id="O123", name="mixed nuts", price=25.0)
    assert encode_engine_value(order) == ["O123", "mixed nuts", 25.0, "default_extra"]

    order_nt = OrderNamedTuple(order_id="O123", name="mixed nuts", price=25.0)
    assert encode_engine_value(order_nt) == [
        "O123",
        "mixed nuts",
        25.0,
        "default_extra",
    ]


def test_encode_engine_value_list_of_structs() -> None:
    orders = [Order("O1", "item1", 10.0), Order("O2", "item2", 20.0)]
    assert encode_engine_value(orders) == [
        ["O1", "item1", 10.0, "default_extra"],
        ["O2", "item2", 20.0, "default_extra"],
    ]

    orders_nt = [
        OrderNamedTuple("O1", "item1", 10.0),
        OrderNamedTuple("O2", "item2", 20.0),
    ]
    assert encode_engine_value(orders_nt) == [
        ["O1", "item1", 10.0, "default_extra"],
        ["O2", "item2", 20.0, "default_extra"],
    ]


def test_encode_engine_value_struct_with_list() -> None:
    basket = Basket(items=["apple", "banana"])
    assert encode_engine_value(basket) == [["apple", "banana"]]


def test_encode_engine_value_nested_struct() -> None:
    customer = Customer(name="Alice", order=Order("O1", "item1", 10.0))
    assert encode_engine_value(customer) == [
        "Alice",
        ["O1", "item1", 10.0, "default_extra"],
        None,
    ]

    customer_nt = CustomerNamedTuple(
        name="Alice", order=OrderNamedTuple("O1", "item1", 10.0)
    )
    assert encode_engine_value(customer_nt) == [
        "Alice",
        ["O1", "item1", 10.0, "default_extra"],
        None,
    ]


def test_encode_engine_value_empty_list() -> None:
    assert encode_engine_value([]) == []
    assert encode_engine_value([[]]) == [[]]


def test_encode_engine_value_tuple() -> None:
    assert encode_engine_value(()) == []
    assert encode_engine_value((1, 2, 3)) == [1, 2, 3]
    assert encode_engine_value(((1, 2), (3, 4))) == [[1, 2], [3, 4]]
    assert encode_engine_value(([],)) == [[]]
    assert encode_engine_value(((),)) == [[]]


def test_encode_engine_value_none() -> None:
    assert encode_engine_value(None) is None


def test_roundtrip_basic_types() -> None:
    validate_full_roundtrip(42, int, (42, None))
    validate_full_roundtrip(3.25, float, (3.25, Float64))
    validate_full_roundtrip(
        3.25, Float64, (3.25, float), (np.float64(3.25), np.float64)
    )
    validate_full_roundtrip(
        3.25, Float32, (3.25, float), (np.float32(3.25), np.float32)
    )
    validate_full_roundtrip("hello", str, ("hello", None))
    validate_full_roundtrip(True, bool, (True, None))
    validate_full_roundtrip(False, bool, (False, None))
    validate_full_roundtrip(
        datetime.date(2025, 1, 1), datetime.date, (datetime.date(2025, 1, 1), None)
    )

    validate_full_roundtrip(
        datetime.datetime(2025, 1, 2, 3, 4, 5, 123456),
        cocoindex.LocalDateTime,
        (datetime.datetime(2025, 1, 2, 3, 4, 5, 123456), datetime.datetime),
    )
    validate_full_roundtrip(
        datetime.datetime(2025, 1, 2, 3, 4, 5, 123456, datetime.UTC),
        cocoindex.OffsetDateTime,
        (
            datetime.datetime(2025, 1, 2, 3, 4, 5, 123456, datetime.UTC),
            datetime.datetime,
        ),
    )

    uuid_value = uuid.uuid4()
    validate_full_roundtrip(uuid_value, uuid.UUID, (uuid_value, None))


def test_decode_scalar_numpy_values() -> None:
    test_cases = [
        ({"kind": "Int64"}, np.int64, 42, np.int64(42)),
        ({"kind": "Float32"}, np.float32, 3.14, np.float32(3.14)),
        ({"kind": "Float64"}, np.float64, 2.718, np.float64(2.718)),
    ]
    for src_type, dst_type, input_value, expected in test_cases:
        decoder = make_engine_value_decoder(["field"], src_type, dst_type)
        result = decoder(input_value)
        assert isinstance(result, dst_type)
        assert result == expected


def test_non_ndarray_vector_decoding() -> None:
    # Test list[np.float64]
    src_type = {
        "kind": "Vector",
        "element_type": {"kind": "Float64"},
        "dimension": None,
    }
    dst_type_float = list[np.float64]
    decoder = make_engine_value_decoder(["field"], src_type, dst_type_float)
    input_numbers = [1.0, 2.0, 3.0]
    result = decoder(input_numbers)
    assert isinstance(result, list)
    assert all(isinstance(x, np.float64) for x in result)
    assert result == [np.float64(1.0), np.float64(2.0), np.float64(3.0)]

    # Test list[Uuid]
    src_type = {"kind": "Vector", "element_type": {"kind": "Uuid"}, "dimension": None}
    dst_type_uuid = list[uuid.UUID]
    decoder = make_engine_value_decoder(["field"], src_type, dst_type_uuid)
    uuid1 = uuid.uuid4()
    uuid2 = uuid.uuid4()
    input_bytes = [uuid1.bytes, uuid2.bytes]
    result = decoder(input_bytes)
    assert isinstance(result, list)
    assert all(isinstance(x, uuid.UUID) for x in result)
    assert result == [uuid1, uuid2]


@pytest.mark.parametrize(
    "data_type, engine_val, expected",
    [
        # All fields match (dataclass)
        (
            Order,
            ["O123", "mixed nuts", 25.0, "default_extra"],
            Order("O123", "mixed nuts", 25.0, "default_extra"),
        ),
        # All fields match (NamedTuple)
        (
            OrderNamedTuple,
            ["O123", "mixed nuts", 25.0, "default_extra"],
            OrderNamedTuple("O123", "mixed nuts", 25.0, "default_extra"),
        ),
        # Extra field in engine value (should ignore extra)
        (
            Order,
            ["O123", "mixed nuts", 25.0, "default_extra", "unexpected"],
            Order("O123", "mixed nuts", 25.0, "default_extra"),
        ),
        (
            OrderNamedTuple,
            ["O123", "mixed nuts", 25.0, "default_extra", "unexpected"],
            OrderNamedTuple("O123", "mixed nuts", 25.0, "default_extra"),
        ),
        # Fewer fields in engine value (should fill with default)
        (
            Order,
            ["O123", "mixed nuts", 0.0, "default_extra"],
            Order("O123", "mixed nuts", 0.0, "default_extra"),
        ),
        (
            OrderNamedTuple,
            ["O123", "mixed nuts", 0.0, "default_extra"],
            OrderNamedTuple("O123", "mixed nuts", 0.0, "default_extra"),
        ),
        # More fields in engine value (should ignore extra)
        (
            Order,
            ["O123", "mixed nuts", 25.0, "unexpected"],
            Order("O123", "mixed nuts", 25.0, "unexpected"),
        ),
        (
            OrderNamedTuple,
            ["O123", "mixed nuts", 25.0, "unexpected"],
            OrderNamedTuple("O123", "mixed nuts", 25.0, "unexpected"),
        ),
        # Truly extra field (should ignore the fifth field)
        (
            Order,
            ["O123", "mixed nuts", 25.0, "default_extra", "ignored"],
            Order("O123", "mixed nuts", 25.0, "default_extra"),
        ),
        (
            OrderNamedTuple,
            ["O123", "mixed nuts", 25.0, "default_extra", "ignored"],
            OrderNamedTuple("O123", "mixed nuts", 25.0, "default_extra"),
        ),
        # Missing optional field in engine value (tags=None)
        (
            Customer,
            ["Alice", ["O1", "item1", 10.0, "default_extra"], None],
            Customer("Alice", Order("O1", "item1", 10.0, "default_extra"), None),
        ),
        (
            CustomerNamedTuple,
            ["Alice", ["O1", "item1", 10.0, "default_extra"], None],
            CustomerNamedTuple(
                "Alice", OrderNamedTuple("O1", "item1", 10.0, "default_extra"), None
            ),
        ),
        # Extra field in engine value for Customer (should ignore)
        (
            Customer,
            ["Alice", ["O1", "item1", 10.0, "default_extra"], [["vip"]], "extra"],
            Customer(
                "Alice", Order("O1", "item1", 10.0, "default_extra"), [Tag("vip")]
            ),
        ),
        (
            CustomerNamedTuple,
            ["Alice", ["O1", "item1", 10.0, "default_extra"], [["vip"]], "extra"],
            CustomerNamedTuple(
                "Alice",
                OrderNamedTuple("O1", "item1", 10.0, "default_extra"),
                [Tag("vip")],
            ),
        ),
        # Missing optional field with default
        (
            Order,
            ["O123", "mixed nuts", 25.0],
            Order("O123", "mixed nuts", 25.0, "default_extra"),
        ),
        (
            OrderNamedTuple,
            ["O123", "mixed nuts", 25.0],
            OrderNamedTuple("O123", "mixed nuts", 25.0, "default_extra"),
        ),
        # Partial optional fields
        (
            Customer,
            ["Alice", ["O1", "item1", 10.0]],
            Customer("Alice", Order("O1", "item1", 10.0, "default_extra"), None),
        ),
        (
            CustomerNamedTuple,
            ["Alice", ["O1", "item1", 10.0]],
            CustomerNamedTuple(
                "Alice", OrderNamedTuple("O1", "item1", 10.0, "default_extra"), None
            ),
        ),
    ],
)
def test_struct_decoder_cases(data_type: Any, engine_val: Any, expected: Any) -> None:
    decoder = build_engine_value_decoder(data_type)
    assert decoder(engine_val) == expected


def test_make_engine_value_decoder_list_of_struct() -> None:
    # List of structs (dataclass)
    engine_val = [
        ["O1", "item1", 10.0, "default_extra"],
        ["O2", "item2", 20.0, "default_extra"],
    ]
    decoder = build_engine_value_decoder(list[Order])
    assert decoder(engine_val) == [
        Order("O1", "item1", 10.0, "default_extra"),
        Order("O2", "item2", 20.0, "default_extra"),
    ]

    # List of structs (NamedTuple)
    decoder = build_engine_value_decoder(list[OrderNamedTuple])
    assert decoder(engine_val) == [
        OrderNamedTuple("O1", "item1", 10.0, "default_extra"),
        OrderNamedTuple("O2", "item2", 20.0, "default_extra"),
    ]


def test_make_engine_value_decoder_struct_of_list() -> None:
    # Struct with list field
    engine_val = [
        "Alice",
        ["O1", "item1", 10.0, "default_extra"],
        [["vip"], ["premium"]],
    ]
    decoder = build_engine_value_decoder(Customer)
    assert decoder(engine_val) == Customer(
        "Alice",
        Order("O1", "item1", 10.0, "default_extra"),
        [Tag("vip"), Tag("premium")],
    )

    # NamedTuple with list field
    decoder = build_engine_value_decoder(CustomerNamedTuple)
    assert decoder(engine_val) == CustomerNamedTuple(
        "Alice",
        OrderNamedTuple("O1", "item1", 10.0, "default_extra"),
        [Tag("vip"), Tag("premium")],
    )


def test_make_engine_value_decoder_struct_of_struct() -> None:
    # Struct with struct field
    engine_val = [
        ["Alice", ["O1", "item1", 10.0, "default_extra"], [["vip"]]],
        [
            ["O1", "item1", 10.0, "default_extra"],
            ["O2", "item2", 20.0, "default_extra"],
        ],
        2,
    ]
    decoder = build_engine_value_decoder(NestedStruct)
    assert decoder(engine_val) == NestedStruct(
        Customer("Alice", Order("O1", "item1", 10.0, "default_extra"), [Tag("vip")]),
        [
            Order("O1", "item1", 10.0, "default_extra"),
            Order("O2", "item2", 20.0, "default_extra"),
        ],
        2,
    )


def make_engine_order(fields: list[tuple[str, type]]) -> type:
    return make_dataclass("EngineOrder", fields)


def make_python_order(
    fields: list[tuple[str, type]], defaults: dict[str, Any] | None = None
) -> type:
    if defaults is None:
        defaults = {}
    # Move all fields with defaults to the end (Python dataclass requirement)
    non_default_fields = [(n, t) for n, t in fields if n not in defaults]
    default_fields = [(n, t) for n, t in fields if n in defaults]
    ordered_fields = non_default_fields + default_fields
    # Prepare the namespace for defaults (only for fields at the end)
    namespace = {k: defaults[k] for k, _ in default_fields}
    return make_dataclass("PythonOrder", ordered_fields, namespace=namespace)


@pytest.mark.parametrize(
    "engine_fields, python_fields, python_defaults, engine_val, expected_python_val",
    [
        # Extra field in Python (middle)
        (
            [("id", str), ("name", str)],
            [("id", str), ("price", float), ("name", str)],
            {"price": 0.0},
            ["O123", "mixed nuts"],
            ("O123", 0.0, "mixed nuts"),
        ),
        # Missing field in Python (middle)
        (
            [("id", str), ("price", float), ("name", str)],
            [("id", str), ("name", str)],
            {},
            ["O123", 25.0, "mixed nuts"],
            ("O123", "mixed nuts"),
        ),
        # Extra field in Python (start)
        (
            [("name", str), ("price", float)],
            [("extra", str), ("name", str), ("price", float)],
            {"extra": "default"},
            ["mixed nuts", 25.0],
            ("default", "mixed nuts", 25.0),
        ),
        # Missing field in Python (start)
        (
            [("extra", str), ("name", str), ("price", float)],
            [("name", str), ("price", float)],
            {},
            ["unexpected", "mixed nuts", 25.0],
            ("mixed nuts", 25.0),
        ),
        # Field order difference (should map by name)
        (
            [("id", str), ("name", str), ("price", float)],
            [("name", str), ("id", str), ("price", float), ("extra", str)],
            {"extra": "default"},
            ["O123", "mixed nuts", 25.0],
            ("mixed nuts", "O123", 25.0, "default"),
        ),
        # Extra field (Python has extra field with default)
        (
            [("id", str), ("name", str)],
            [("id", str), ("name", str), ("price", float)],
            {"price": 0.0},
            ["O123", "mixed nuts"],
            ("O123", "mixed nuts", 0.0),
        ),
        # Missing field (Engine has extra field)
        (
            [("id", str), ("name", str), ("price", float)],
            [("id", str), ("name", str)],
            {},
            ["O123", "mixed nuts", 25.0],
            ("O123", "mixed nuts"),
        ),
    ],
)
def test_field_position_cases(
    engine_fields: list[tuple[str, type]],
    python_fields: list[tuple[str, type]],
    python_defaults: dict[str, Any],
    engine_val: list[Any],
    expected_python_val: tuple[Any, ...],
) -> None:
    EngineOrder = make_engine_order(engine_fields)
    PythonOrder = make_python_order(python_fields, python_defaults)
    decoder = build_engine_value_decoder(EngineOrder, PythonOrder)
    # Map field names to expected values
    expected_dict = dict(zip([f[0] for f in python_fields], expected_python_val))
    # Instantiate using keyword arguments (order doesn't matter)
    assert decoder(engine_val) == PythonOrder(**expected_dict)


def test_roundtrip_union_simple() -> None:
    t = int | str | float
    value = 10.4
    validate_full_roundtrip(value, t)


def test_roundtrip_union_with_active_uuid() -> None:
    t = str | uuid.UUID | int
    value = uuid.uuid4().bytes
    validate_full_roundtrip(value, t)


def test_roundtrip_union_with_inactive_uuid() -> None:
    t = str | uuid.UUID | int
    value = "5a9f8f6a-318f-4f1f-929d-566d7444a62d"  # it's a string
    validate_full_roundtrip(value, t)


def test_roundtrip_union_offset_datetime() -> None:
    t = str | uuid.UUID | float | int | datetime.datetime
    value = datetime.datetime.now(datetime.UTC)
    validate_full_roundtrip(value, t)


def test_roundtrip_union_date() -> None:
    t = str | uuid.UUID | float | int | datetime.date
    value = datetime.date.today()
    validate_full_roundtrip(value, t)


def test_roundtrip_union_time() -> None:
    t = str | uuid.UUID | float | int | datetime.time
    value = datetime.time()
    validate_full_roundtrip(value, t)


def test_roundtrip_union_timedelta() -> None:
    t = str | uuid.UUID | float | int | datetime.timedelta
    value = datetime.timedelta(hours=39, minutes=10, seconds=1)
    validate_full_roundtrip(value, t)


def test_roundtrip_ltable() -> None:
    t = list[Order]
    value = [Order("O1", "item1", 10.0), Order("O2", "item2", 20.0)]
    validate_full_roundtrip(value, t)

    t_nt = list[OrderNamedTuple]
    value_nt = [
        OrderNamedTuple("O1", "item1", 10.0),
        OrderNamedTuple("O2", "item2", 20.0),
    ]
    validate_full_roundtrip(value_nt, t_nt)


def test_roundtrip_ktable_str_key() -> None:
    t = dict[str, Order]
    value = {"K1": Order("O1", "item1", 10.0), "K2": Order("O2", "item2", 20.0)}
    validate_full_roundtrip(value, t)

    t_nt = dict[str, OrderNamedTuple]
    value_nt = {
        "K1": OrderNamedTuple("O1", "item1", 10.0),
        "K2": OrderNamedTuple("O2", "item2", 20.0),
    }
    validate_full_roundtrip(value_nt, t_nt)


def test_roundtrip_ktable_struct_key() -> None:
    @dataclass(frozen=True)
    class OrderKey:
        shop_id: str
        version: int

    t = dict[OrderKey, Order]
    value = {
        OrderKey("A", 3): Order("O1", "item1", 10.0),
        OrderKey("B", 4): Order("O2", "item2", 20.0),
    }
    validate_full_roundtrip(value, t)

    t_nt = dict[OrderKey, OrderNamedTuple]
    value_nt = {
        OrderKey("A", 3): OrderNamedTuple("O1", "item1", 10.0),
        OrderKey("B", 4): OrderNamedTuple("O2", "item2", 20.0),
    }
    validate_full_roundtrip(value_nt, t_nt)


IntVectorType = cocoindex.Vector[np.int64, Literal[5]]


def test_vector_as_vector() -> None:
    value = np.array([1, 2, 3, 4, 5], dtype=np.int64)
    encoded = encode_engine_value(value)
    assert np.array_equal(encoded, value)
    decoded = build_engine_value_decoder(IntVectorType)(encoded)
    assert np.array_equal(decoded, value)


ListIntType = list[int]


def test_vector_as_list() -> None:
    value: ListIntType = [1, 2, 3, 4, 5]
    encoded = encode_engine_value(value)
    assert encoded == [1, 2, 3, 4, 5]
    decoded = build_engine_value_decoder(ListIntType)(encoded)
    assert np.array_equal(decoded, value)


Float64VectorTypeNoDim = Vector[np.float64]
Float32VectorType = Vector[np.float32, Literal[3]]
Float64VectorType = Vector[np.float64, Literal[3]]
Int64VectorType = Vector[np.int64, Literal[3]]
NDArrayFloat32Type = NDArray[np.float32]
NDArrayFloat64Type = NDArray[np.float64]
NDArrayInt64Type = NDArray[np.int64]


def test_encode_engine_value_ndarray() -> None:
    """Test encoding NDArray vectors to lists for the Rust engine."""
    vec_f32: Float32VectorType = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    assert np.array_equal(encode_engine_value(vec_f32), [1.0, 2.0, 3.0])
    vec_f64: Float64VectorType = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    assert np.array_equal(encode_engine_value(vec_f64), [1.0, 2.0, 3.0])
    vec_i64: Int64VectorType = np.array([1, 2, 3], dtype=np.int64)
    assert np.array_equal(encode_engine_value(vec_i64), [1, 2, 3])
    vec_nd_f32: NDArrayFloat32Type = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    assert np.array_equal(encode_engine_value(vec_nd_f32), [1.0, 2.0, 3.0])


def test_make_engine_value_decoder_ndarray() -> None:
    """Test decoding engine lists to NDArray vectors."""
    decoder_f32 = build_engine_value_decoder(Float32VectorType)
    result_f32 = decoder_f32([1.0, 2.0, 3.0])
    assert isinstance(result_f32, np.ndarray)
    assert result_f32.dtype == np.float32
    assert np.array_equal(result_f32, np.array([1.0, 2.0, 3.0], dtype=np.float32))
    decoder_f64 = build_engine_value_decoder(Float64VectorType)
    result_f64 = decoder_f64([1.0, 2.0, 3.0])
    assert isinstance(result_f64, np.ndarray)
    assert result_f64.dtype == np.float64
    assert np.array_equal(result_f64, np.array([1.0, 2.0, 3.0], dtype=np.float64))
    decoder_i64 = build_engine_value_decoder(Int64VectorType)
    result_i64 = decoder_i64([1, 2, 3])
    assert isinstance(result_i64, np.ndarray)
    assert result_i64.dtype == np.int64
    assert np.array_equal(result_i64, np.array([1, 2, 3], dtype=np.int64))
    decoder_nd_f32 = build_engine_value_decoder(NDArrayFloat32Type)
    result_nd_f32 = decoder_nd_f32([1.0, 2.0, 3.0])
    assert isinstance(result_nd_f32, np.ndarray)
    assert result_nd_f32.dtype == np.float32
    assert np.array_equal(result_nd_f32, np.array([1.0, 2.0, 3.0], dtype=np.float32))


def test_roundtrip_ndarray_vector() -> None:
    """Test roundtrip encoding and decoding of NDArray vectors."""
    value_f32 = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    encoded_f32 = encode_engine_value(value_f32)
    np.array_equal(encoded_f32, [1.0, 2.0, 3.0])
    decoded_f32 = build_engine_value_decoder(Float32VectorType)(encoded_f32)
    assert isinstance(decoded_f32, np.ndarray)
    assert decoded_f32.dtype == np.float32
    assert np.array_equal(decoded_f32, value_f32)
    value_i64 = np.array([1, 2, 3], dtype=np.int64)
    encoded_i64 = encode_engine_value(value_i64)
    assert np.array_equal(encoded_i64, [1, 2, 3])
    decoded_i64 = build_engine_value_decoder(Int64VectorType)(encoded_i64)
    assert isinstance(decoded_i64, np.ndarray)
    assert decoded_i64.dtype == np.int64
    assert np.array_equal(decoded_i64, value_i64)
    value_nd_f64: NDArrayFloat64Type = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    encoded_nd_f64 = encode_engine_value(value_nd_f64)
    assert np.array_equal(encoded_nd_f64, [1.0, 2.0, 3.0])
    decoded_nd_f64 = build_engine_value_decoder(NDArrayFloat64Type)(encoded_nd_f64)
    assert isinstance(decoded_nd_f64, np.ndarray)
    assert decoded_nd_f64.dtype == np.float64
    assert np.array_equal(decoded_nd_f64, value_nd_f64)


def test_ndarray_dimension_mismatch() -> None:
    """Test dimension enforcement for Vector with specified dimension."""
    value = np.array([1.0, 2.0], dtype=np.float32)
    encoded = encode_engine_value(value)
    assert np.array_equal(encoded, [1.0, 2.0])
    with pytest.raises(ValueError, match="Vector dimension mismatch"):
        build_engine_value_decoder(Float32VectorType)(encoded)


def test_list_vector_backward_compatibility() -> None:
    """Test that list-based vectors still work for backward compatibility."""
    value = [1, 2, 3, 4, 5]
    encoded = encode_engine_value(value)
    assert encoded == [1, 2, 3, 4, 5]
    decoded = build_engine_value_decoder(IntVectorType)(encoded)
    assert isinstance(decoded, np.ndarray)
    assert decoded.dtype == np.int64
    assert np.array_equal(decoded, np.array([1, 2, 3, 4, 5], dtype=np.int64))
    value_list: ListIntType = [1, 2, 3, 4, 5]
    encoded = encode_engine_value(value_list)
    assert np.array_equal(encoded, [1, 2, 3, 4, 5])
    decoded = build_engine_value_decoder(ListIntType)(encoded)
    assert np.array_equal(decoded, [1, 2, 3, 4, 5])


def test_encode_complex_structure_with_ndarray() -> None:
    """Test encoding a complex structure that includes an NDArray."""

    @dataclass
    class MyStructWithNDArray:
        name: str
        data: NDArray[np.float32]
        value: int

    original = MyStructWithNDArray(
        name="test_np", data=np.array([1.0, 0.5], dtype=np.float32), value=100
    )
    encoded = encode_engine_value(original)

    assert encoded[0] == original.name
    assert np.array_equal(encoded[1], original.data)
    assert encoded[2] == original.value


def test_decode_nullable_ndarray_none_or_value_input() -> None:
    """Test decoding a nullable NDArray with None or value inputs."""
    src_type_dict = {
        "kind": "Vector",
        "element_type": {"kind": "Float32"},
        "dimension": None,
    }
    dst_annotation = NDArrayFloat32Type | None
    decoder = make_engine_value_decoder([], src_type_dict, dst_annotation)

    none_engine_value = None
    decoded_array = decoder(none_engine_value)
    assert decoded_array is None

    engine_value = [1.0, 2.0, 3.0]
    decoded_array = decoder(engine_value)

    assert isinstance(decoded_array, np.ndarray)
    assert decoded_array.dtype == np.float32
    np.testing.assert_array_equal(
        decoded_array, np.array([1.0, 2.0, 3.0], dtype=np.float32)
    )


def test_decode_vector_string() -> None:
    """Test decoding a vector of strings works for Python native list type."""
    src_type_dict = {
        "kind": "Vector",
        "element_type": {"kind": "Str"},
        "dimension": None,
    }
    decoder = make_engine_value_decoder([], src_type_dict, Vector[str])
    assert decoder(["hello", "world"]) == ["hello", "world"]


def test_decode_error_non_nullable_or_non_list_vector() -> None:
    """Test decoding errors for non-nullable vectors or non-list inputs."""
    src_type_dict = {
        "kind": "Vector",
        "element_type": {"kind": "Float32"},
        "dimension": None,
    }
    decoder = make_engine_value_decoder([], src_type_dict, NDArrayFloat32Type)
    with pytest.raises(ValueError, match="Received null for non-nullable vector"):
        decoder(None)
    with pytest.raises(TypeError, match="Expected NDArray or list for vector"):
        decoder("not a list")


def test_dump_vector_type_annotation_with_dim() -> None:
    """Test dumping a vector type annotation with a specified dimension."""
    expected_dump = {
        "type": {
            "kind": "Vector",
            "element_type": {"kind": "Float32"},
            "dimension": 3,
        }
    }
    assert dump_engine_object(Float32VectorType) == expected_dump


def test_dump_vector_type_annotation_no_dim() -> None:
    """Test dumping a vector type annotation with no dimension."""
    expected_dump_no_dim = {
        "type": {
            "kind": "Vector",
            "element_type": {"kind": "Float64"},
            "dimension": None,
        }
    }
    assert dump_engine_object(Float64VectorTypeNoDim) == expected_dump_no_dim


def test_full_roundtrip_vector_numeric_types() -> None:
    """Test full roundtrip for numeric vector types using NDArray."""
    value_f32 = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    validate_full_roundtrip(
        value_f32,
        Vector[np.float32, Literal[3]],
        ([np.float32(1.0), np.float32(2.0), np.float32(3.0)], list[np.float32]),
        ([1.0, 2.0, 3.0], list[cocoindex.Float32]),
        ([1.0, 2.0, 3.0], list[float]),
    )
    validate_full_roundtrip(
        value_f32,
        np.typing.NDArray[np.float32],
        ([np.float32(1.0), np.float32(2.0), np.float32(3.0)], list[np.float32]),
        ([1.0, 2.0, 3.0], list[cocoindex.Float32]),
        ([1.0, 2.0, 3.0], list[float]),
    )
    validate_full_roundtrip(
        value_f32.tolist(),
        list[np.float32],
        (value_f32, Vector[np.float32, Literal[3]]),
        ([1.0, 2.0, 3.0], list[cocoindex.Float32]),
        ([1.0, 2.0, 3.0], list[float]),
    )

    value_f64 = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    validate_full_roundtrip(
        value_f64,
        Vector[np.float64, Literal[3]],
        ([np.float64(1.0), np.float64(2.0), np.float64(3.0)], list[np.float64]),
        ([1.0, 2.0, 3.0], list[cocoindex.Float64]),
        ([1.0, 2.0, 3.0], list[float]),
    )

    value_i64 = np.array([1, 2, 3], dtype=np.int64)
    validate_full_roundtrip(
        value_i64,
        Vector[np.int64, Literal[3]],
        ([np.int64(1), np.int64(2), np.int64(3)], list[np.int64]),
        ([1, 2, 3], list[int]),
    )

    value_i32 = np.array([1, 2, 3], dtype=np.int32)
    with pytest.raises(ValueError, match="Unsupported NumPy dtype"):
        validate_full_roundtrip(value_i32, Vector[np.int32, Literal[3]])
    value_u8 = np.array([1, 2, 3], dtype=np.uint8)
    with pytest.raises(ValueError, match="Unsupported NumPy dtype"):
        validate_full_roundtrip(value_u8, Vector[np.uint8, Literal[3]])
    value_u16 = np.array([1, 2, 3], dtype=np.uint16)
    with pytest.raises(ValueError, match="Unsupported NumPy dtype"):
        validate_full_roundtrip(value_u16, Vector[np.uint16, Literal[3]])
    value_u32 = np.array([1, 2, 3], dtype=np.uint32)
    with pytest.raises(ValueError, match="Unsupported NumPy dtype"):
        validate_full_roundtrip(value_u32, Vector[np.uint32, Literal[3]])
    value_u64 = np.array([1, 2, 3], dtype=np.uint64)
    with pytest.raises(ValueError, match="Unsupported NumPy dtype"):
        validate_full_roundtrip(value_u64, Vector[np.uint64, Literal[3]])


def test_roundtrip_vector_no_dimension() -> None:
    """Test full roundtrip for vector types without dimension annotation."""
    value_f64 = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    validate_full_roundtrip(
        value_f64,
        Vector[np.float64],
        ([1.0, 2.0, 3.0], list[float]),
        (np.array([1.0, 2.0, 3.0], dtype=np.float64), np.typing.NDArray[np.float64]),
    )


def test_roundtrip_string_vector() -> None:
    """Test full roundtrip for string vector using list."""
    value_str: Vector[str] = ["hello", "world"]
    validate_full_roundtrip(value_str, Vector[str])


def test_roundtrip_empty_vector() -> None:
    """Test full roundtrip for empty numeric vector."""
    value_empty: Vector[np.float32] = np.array([], dtype=np.float32)
    validate_full_roundtrip(value_empty, Vector[np.float32])


def test_roundtrip_dimension_mismatch() -> None:
    """Test that dimension mismatch raises an error during roundtrip."""
    value_f32: Vector[np.float32, Literal[3]] = np.array([1.0, 2.0], dtype=np.float32)
    with pytest.raises(ValueError, match="Vector dimension mismatch"):
        validate_full_roundtrip(value_f32, Vector[np.float32, Literal[3]])


def test_full_roundtrip_scalar_numeric_types() -> None:
    """Test full roundtrip for scalar NumPy numeric types."""
    # Test supported scalar types
    validate_full_roundtrip(np.int64(42), np.int64, (42, int))
    validate_full_roundtrip(np.float32(3.25), np.float32, (3.25, cocoindex.Float32))
    validate_full_roundtrip(np.float64(3.25), np.float64, (3.25, cocoindex.Float64))

    # Test unsupported scalar types
    for unsupported_type in [np.int32, np.uint8, np.uint16, np.uint32, np.uint64]:
        with pytest.raises(ValueError, match="Unsupported NumPy dtype"):
            validate_full_roundtrip(unsupported_type(1), unsupported_type)


def test_full_roundtrip_nullable_scalar() -> None:
    """Test full roundtrip for nullable scalar NumPy types."""
    # Test with non-null values
    validate_full_roundtrip(np.int64(42), np.int64 | None)
    validate_full_roundtrip(np.float32(3.14), np.float32 | None)
    validate_full_roundtrip(np.float64(2.718), np.float64 | None)

    # Test with None
    validate_full_roundtrip(None, np.int64 | None)
    validate_full_roundtrip(None, np.float32 | None)
    validate_full_roundtrip(None, np.float64 | None)


def test_full_roundtrip_scalar_in_struct() -> None:
    """Test full roundtrip for scalar NumPy types in a dataclass."""

    @dataclass
    class NumericStruct:
        int_field: np.int64
        float32_field: np.float32
        float64_field: np.float64

    instance = NumericStruct(
        int_field=np.int64(42),
        float32_field=np.float32(3.14),
        float64_field=np.float64(2.718),
    )
    validate_full_roundtrip(instance, NumericStruct)


def test_full_roundtrip_scalar_in_nested_struct() -> None:
    """Test full roundtrip for scalar NumPy types in a nested struct."""

    @dataclass
    class InnerStruct:
        value: np.float64

    @dataclass
    class OuterStruct:
        inner: InnerStruct
        count: np.int64

    instance = OuterStruct(
        inner=InnerStruct(value=np.float64(2.718)),
        count=np.int64(1),
    )
    validate_full_roundtrip(instance, OuterStruct)


def test_full_roundtrip_scalar_with_python_types() -> None:
    """Test full roundtrip for structs mixing NumPy and Python scalar types."""

    @dataclass
    class MixedStruct:
        numpy_int: np.int64
        python_int: int
        numpy_float: np.float64
        python_float: float
        string: str
        annotated_int: Annotated[np.int64, TypeKind("int")]
        annotated_float: Float32

    instance = MixedStruct(
        numpy_int=np.int64(42),
        python_int=43,
        numpy_float=np.float64(2.718),
        python_float=3.14,
        string="hello, world",
        annotated_int=np.int64(42),
        annotated_float=2.0,
    )
    validate_full_roundtrip(instance, MixedStruct)
