import uuid
import datetime
from dataclasses import dataclass, make_dataclass
from typing import NamedTuple, Literal, Any, Callable
import pytest
import cocoindex
from cocoindex.typing import (
    encode_enriched_type,
    Vector,
)
from cocoindex.convert import (
    encode_engine_value,
    make_engine_value_decoder,
    dump_engine_object,
)
import numpy as np
from numpy.typing import NDArray


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
    items: list


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
    value: Any, output_type: Any, input_type: Any | None = None
) -> None:
    """
    Validate the given value doesn't change after encoding, sending to engine (using output_type), receiving back and decoding (using input_type).

    If `input_type` is not specified, uses `output_type` as the target.
    """
    from cocoindex import _engine

    encoded_value = encode_engine_value(value)
    encoded_output_type = encode_enriched_type(output_type)["type"]
    value_from_engine = _engine.testutil.seder_roundtrip(
        encoded_value, encoded_output_type
    )
    decoded_value = build_engine_value_decoder(input_type or output_type, output_type)(
        value_from_engine
    )
    assert decoded_value == value


def test_encode_engine_value_basic_types():
    assert encode_engine_value(123) == 123
    assert encode_engine_value(3.14) == 3.14
    assert encode_engine_value("hello") == "hello"
    assert encode_engine_value(True) is True


def test_encode_engine_value_uuid():
    u = uuid.uuid4()
    assert encode_engine_value(u) == u.bytes


def test_encode_engine_value_date_time_types():
    d = datetime.date(2024, 1, 1)
    assert encode_engine_value(d) == d
    t = datetime.time(12, 30)
    assert encode_engine_value(t) == t
    dt = datetime.datetime(2024, 1, 1, 12, 30)
    assert encode_engine_value(dt) == dt


def test_encode_engine_value_struct():
    order = Order(order_id="O123", name="mixed nuts", price=25.0)
    assert encode_engine_value(order) == ["O123", "mixed nuts", 25.0, "default_extra"]

    order_nt = OrderNamedTuple(order_id="O123", name="mixed nuts", price=25.0)
    assert encode_engine_value(order_nt) == [
        "O123",
        "mixed nuts",
        25.0,
        "default_extra",
    ]


def test_encode_engine_value_list_of_structs():
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


def test_encode_engine_value_struct_with_list():
    basket = Basket(items=["apple", "banana"])
    assert encode_engine_value(basket) == [["apple", "banana"]]


def test_encode_engine_value_nested_struct():
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


def test_encode_engine_value_empty_list():
    assert encode_engine_value([]) == []
    assert encode_engine_value([[]]) == [[]]


def test_encode_engine_value_tuple():
    assert encode_engine_value(()) == []
    assert encode_engine_value((1, 2, 3)) == [1, 2, 3]
    assert encode_engine_value(((1, 2), (3, 4))) == [[1, 2], [3, 4]]
    assert encode_engine_value(([],)) == [[]]
    assert encode_engine_value(((),)) == [[]]


def test_encode_engine_value_none():
    assert encode_engine_value(None) is None


def test_make_engine_value_decoder_basic_types():
    for engine_type_in_py, value in [
        (int, 42),
        (float, 3.14),
        (str, "hello"),
        (bool, True),
        # (type(None), None),  # Removed unsupported NoneType
    ]:
        decoder = build_engine_value_decoder(engine_type_in_py)
        assert decoder(value) == value


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
def test_struct_decoder_cases(data_type, engine_val, expected):
    decoder = build_engine_value_decoder(data_type)
    assert decoder(engine_val) == expected


def test_make_engine_value_decoder_collections():
    # List of structs (dataclass)
    decoder = build_engine_value_decoder(list[Order])
    engine_val = [
        ["O1", "item1", 10.0, "default_extra"],
        ["O2", "item2", 20.0, "default_extra"],
    ]
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

    # Struct with list field
    decoder = build_engine_value_decoder(Customer)
    engine_val = [
        "Alice",
        ["O1", "item1", 10.0, "default_extra"],
        [["vip"], ["premium"]],
    ]
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

    # Struct with struct field
    decoder = build_engine_value_decoder(NestedStruct)
    engine_val = [
        ["Alice", ["O1", "item1", 10.0, "default_extra"], [["vip"]]],
        [
            ["O1", "item1", 10.0, "default_extra"],
            ["O2", "item2", 20.0, "default_extra"],
        ],
        2,
    ]
    assert decoder(engine_val) == NestedStruct(
        Customer("Alice", Order("O1", "item1", 10.0, "default_extra"), [Tag("vip")]),
        [
            Order("O1", "item1", 10.0, "default_extra"),
            Order("O2", "item2", 20.0, "default_extra"),
        ],
        2,
    )


def make_engine_order(fields):
    return make_dataclass("EngineOrder", fields)


def make_python_order(fields, defaults=None):
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
    engine_fields, python_fields, python_defaults, engine_val, expected_python_val
):
    EngineOrder = make_engine_order(engine_fields)
    PythonOrder = make_python_order(python_fields, python_defaults)
    decoder = build_engine_value_decoder(EngineOrder, PythonOrder)
    # Map field names to expected values
    expected_dict = dict(zip([f[0] for f in python_fields], expected_python_val))
    # Instantiate using keyword arguments (order doesn't matter)
    assert decoder(engine_val) == PythonOrder(**expected_dict)


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


IntVectorType = cocoindex.Vector[np.int32, Literal[5]]


def test_vector_as_vector() -> None:
    value: IntVectorType = [1, 2, 3, 4, 5]
    encoded = encode_engine_value(value)
    assert encoded == [1, 2, 3, 4, 5]
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
Int32VectorType = Vector[np.int32, Literal[3]]
NDArrayFloat32Type = NDArray[np.float32]
NDArrayFloat64Type = NDArray[np.float64]
NDArrayInt64Type = NDArray[np.int64]


def test_encode_engine_value_ndarray():
    """Test encoding NDArray vectors to lists for the Rust engine."""
    vec_f32: Float32VectorType = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    assert np.array_equal(encode_engine_value(vec_f32), [1.0, 2.0, 3.0])
    vec_f64: Float64VectorType = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    assert np.array_equal(encode_engine_value(vec_f64), [1.0, 2.0, 3.0])
    vec_i64: Int64VectorType = np.array([1, 2, 3], dtype=np.int64)
    assert np.array_equal(encode_engine_value(vec_i64), [1, 2, 3])
    vec_nd_f32: NDArrayFloat32Type = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    assert np.array_equal(encode_engine_value(vec_nd_f32), [1.0, 2.0, 3.0])


def test_make_engine_value_decoder_ndarray():
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


def test_roundtrip_ndarray_vector():
    """Test roundtrip encoding and decoding of NDArray vectors."""
    value_f32: Float32VectorType = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    encoded_f32 = encode_engine_value(value_f32)
    np.array_equal(encoded_f32, [1.0, 2.0, 3.0])
    decoded_f32 = build_engine_value_decoder(Float32VectorType)(encoded_f32)
    assert isinstance(decoded_f32, np.ndarray)
    assert decoded_f32.dtype == np.float32
    assert np.array_equal(decoded_f32, value_f32)
    value_i64: Int64VectorType = np.array([1, 2, 3], dtype=np.int64)
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


def test_uint_support():
    """Test encoding and decoding of unsigned integer vectors."""
    value_uint8 = np.array([1, 2, 3, 4], dtype=np.uint8)
    encoded = encode_engine_value(value_uint8)
    assert np.array_equal(encoded, [1, 2, 3, 4])
    decoder = make_engine_value_decoder(
        [], {"kind": "Vector", "element_type": {"kind": "UInt8"}}, NDArray[np.uint8]
    )
    decoded = decoder(encoded)
    assert np.array_equal(decoded, value_uint8)
    assert decoded.dtype == np.uint8
    value_uint16 = np.array([1, 2, 3, 4], dtype=np.uint16)
    encoded = encode_engine_value(value_uint16)
    assert np.array_equal(encoded, [1, 2, 3, 4])
    decoder = make_engine_value_decoder(
        [], {"kind": "Vector", "element_type": {"kind": "UInt16"}}, NDArray[np.uint16]
    )
    decoded = decoder(encoded)
    assert np.array_equal(decoded, value_uint16)
    assert decoded.dtype == np.uint16
    value_uint32 = np.array([1, 2, 3], dtype=np.uint32)
    encoded = encode_engine_value(value_uint32)
    assert np.array_equal(encoded, [1, 2, 3])
    decoder = make_engine_value_decoder(
        [], {"kind": "Vector", "element_type": {"kind": "UInt32"}}, NDArray[np.uint32]
    )
    decoded = decoder(encoded)
    assert np.array_equal(decoded, value_uint32)
    assert decoded.dtype == np.uint32
    value_uint64 = np.array([1, 2, 3], dtype=np.uint64)
    encoded = encode_engine_value(value_uint64)
    assert np.array_equal(encoded, [1, 2, 3])
    decoder = make_engine_value_decoder(
        [], {"kind": "Vector", "element_type": {"kind": "UInt8"}}, NDArray[np.uint64]
    )
    decoded = decoder(encoded)
    assert np.array_equal(decoded, value_uint64)
    assert decoded.dtype == np.uint64


def test_ndarray_dimension_mismatch():
    """Test dimension enforcement for Vector with specified dimension."""
    value: Float32VectorType = np.array([1.0, 2.0], dtype=np.float32)
    encoded = encode_engine_value(value)
    assert np.array_equal(encoded, [1.0, 2.0])
    with pytest.raises(ValueError, match="Vector dimension mismatch"):
        build_engine_value_decoder(Float32VectorType)(encoded)


def test_list_vector_backward_compatibility():
    """Test that list-based vectors still work for backward compatibility."""
    value: IntVectorType = [1, 2, 3, 4, 5]
    encoded = encode_engine_value(value)
    assert encoded == [1, 2, 3, 4, 5]
    decoded = build_engine_value_decoder(IntVectorType)(encoded)
    assert isinstance(decoded, np.ndarray)
    assert decoded.dtype == np.int32
    assert np.array_equal(decoded, np.array([1, 2, 3, 4, 5], dtype=np.int64))
    value_list: ListIntType = [1, 2, 3, 4, 5]
    encoded = encode_engine_value(value_list)
    assert np.array_equal(encoded, [1, 2, 3, 4, 5])
    decoded = build_engine_value_decoder(ListIntType)(encoded)
    assert np.array_equal(decoded, [1, 2, 3, 4, 5])


def test_encode_complex_structure_with_ndarray():
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
    expected = [
        "test_np",
        [1.0, 0.5],
        100,
    ]
    assert encoded[0] == expected[0]
    assert np.array_equal(encoded[1], expected[1])
    assert encoded[2] == expected[2]


def test_decode_nullable_ndarray_none_or_value_input():
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


def test_decode_vector_string():
    """Test decoding a vector of strings works for Python native list type."""
    src_type_dict = {
        "kind": "Vector",
        "element_type": {"kind": "Str"},
        "dimension": None,
    }
    decoder = make_engine_value_decoder([], src_type_dict, Vector[str])
    assert decoder(["hello", "world"]) == ["hello", "world"]


def test_decode_error_non_nullable_or_non_list_vector():
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


def test_dump_vector_type_annotation_with_dim():
    """Test dumping a vector type annotation with a specified dimension."""
    expected_dump = {
        "type": {
            "kind": "Vector",
            "element_type": {"kind": "Float32"},
            "dimension": 3,
        }
    }
    assert dump_engine_object(Float32VectorType) == expected_dump


def test_dump_vector_type_annotation_no_dim():
    """Test dumping a vector type annotation with no dimension."""
    expected_dump_no_dim = {
        "type": {
            "kind": "Vector",
            "element_type": {"kind": "Float64"},
            "dimension": None,
        }
    }
    assert dump_engine_object(Float64VectorTypeNoDim) == expected_dump_no_dim
