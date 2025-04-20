import dataclasses
import uuid
import datetime
from dataclasses import dataclass, make_dataclass
import pytest
from cocoindex.typing import encode_enriched_type
from cocoindex.convert import to_engine_value
from cocoindex.convert import make_engine_value_converter

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
    tags: list[Tag] = None

@dataclass
class NestedStruct:
    customer: Customer
    orders: list[Order]
    count: int = 0

def build_engine_value_converter(engine_type_in_py, python_type=None):
    """
    Helper to build a converter for the given engine-side type (as represented in Python).
    If python_type is not specified, uses engine_type_in_py as the target.
    """
    engine_type = encode_enriched_type(engine_type_in_py)["type"]
    return make_engine_value_converter([], engine_type, python_type or engine_type_in_py)

def test_to_engine_value_basic_types():
    assert to_engine_value(123) == 123
    assert to_engine_value(3.14) == 3.14
    assert to_engine_value("hello") == "hello"
    assert to_engine_value(True) is True

def test_to_engine_value_uuid():
    u = uuid.uuid4()
    assert to_engine_value(u) == u.bytes

def test_to_engine_value_date_time_types():
    d = datetime.date(2024, 1, 1)
    assert to_engine_value(d) == d
    t = datetime.time(12, 30)
    assert to_engine_value(t) == t
    dt = datetime.datetime(2024, 1, 1, 12, 30)
    assert to_engine_value(dt) == dt

def test_to_engine_value_struct():
    order = Order(order_id="O123", name="mixed nuts", price=25.0)
    assert to_engine_value(order) == ["O123", "mixed nuts", 25.0, "default_extra"]

def test_to_engine_value_list_of_structs():
    orders = [Order("O1", "item1", 10.0), Order("O2", "item2", 20.0)]
    assert to_engine_value(orders) == [["O1", "item1", 10.0, "default_extra"], ["O2", "item2", 20.0, "default_extra"]]

def test_to_engine_value_struct_with_list():
    basket = Basket(items=["apple", "banana"])
    assert to_engine_value(basket) == [["apple", "banana"]]

def test_to_engine_value_nested_struct():
    customer = Customer(name="Alice", order=Order("O1", "item1", 10.0))
    assert to_engine_value(customer) == ["Alice", ["O1", "item1", 10.0, "default_extra"], None]

def test_to_engine_value_empty_list():
    assert to_engine_value([]) == []
    assert to_engine_value([[]]) == [[]]

def test_to_engine_value_tuple():
    assert to_engine_value(()) == []
    assert to_engine_value((1, 2, 3)) == [1, 2, 3]
    assert to_engine_value(((1, 2), (3, 4))) == [[1, 2], [3, 4]]
    assert to_engine_value(([],)) == [[]]
    assert to_engine_value(((),)) == [[]]

def test_to_engine_value_none():
    assert to_engine_value(None) is None

def test_make_engine_value_converter_basic_types():
    for engine_type_in_py, value in [
        (int, 42),
        (float, 3.14),
        (str, "hello"),
        (bool, True),
        # (type(None), None),  # Removed unsupported NoneType
    ]:
        converter = build_engine_value_converter(engine_type_in_py)
        assert converter(value) == value

@pytest.mark.parametrize(
    "converter_type, engine_val, expected",
    [
        # All fields match
        (Order, ["O123", "mixed nuts", 25.0, "default_extra"], Order("O123", "mixed nuts", 25.0, "default_extra")),
        # Extra field in engine value (should ignore extra)
        (Order, ["O123", "mixed nuts", 25.0, "default_extra", "unexpected"], Order("O123", "mixed nuts", 25.0, "default_extra")),
        # Fewer fields in engine value (should fill with default)
        (Order, ["O123", "mixed nuts", 0.0, "default_extra"], Order("O123", "mixed nuts", 0.0, "default_extra")),
        # More fields in engine value (should ignore extra)
        (Order, ["O123", "mixed nuts", 25.0, "unexpected"], Order("O123", "mixed nuts", 25.0, "unexpected")),
        # Truly extra field (should ignore the fifth field)
        (Order, ["O123", "mixed nuts", 25.0, "default_extra", "ignored"], Order("O123", "mixed nuts", 25.0, "default_extra")),
        # Missing optional field in engine value (tags=None)
        (Customer, ["Alice", ["O1", "item1", 10.0, "default_extra"], None], Customer("Alice", Order("O1", "item1", 10.0, "default_extra"), None)),
        # Extra field in engine value for Customer (should ignore)
        (Customer, ["Alice", ["O1", "item1", 10.0, "default_extra"], [["vip"]], "extra"], Customer("Alice", Order("O1", "item1", 10.0, "default_extra"), [Tag("vip")])),
    ]
)
def test_struct_conversion_cases(converter_type, engine_val, expected):
    converter = build_engine_value_converter(converter_type)
    assert converter(engine_val) == expected

def test_make_engine_value_converter_collections():
    # List of structs
    converter = build_engine_value_converter(list[Order])
    engine_val = [
        ["O1", "item1", 10.0, "default_extra"],
        ["O2", "item2", 20.0, "default_extra"]
    ]
    assert converter(engine_val) == [Order("O1", "item1", 10.0, "default_extra"), Order("O2", "item2", 20.0, "default_extra")]
    # Struct with list field
    converter = build_engine_value_converter(Customer)
    engine_val = ["Alice", ["O1", "item1", 10.0, "default_extra"], [["vip"], ["premium"]]]
    assert converter(engine_val) == Customer("Alice", Order("O1", "item1", 10.0, "default_extra"), [Tag("vip"), Tag("premium")])
    # Struct with struct field
    converter = build_engine_value_converter(NestedStruct)
    engine_val = [
        ["Alice", ["O1", "item1", 10.0, "default_extra"], [["vip"]]],
        [["O1", "item1", 10.0, "default_extra"], ["O2", "item2", 20.0, "default_extra"]],
        2
    ]
    assert converter(engine_val) == NestedStruct(
        Customer("Alice", Order("O1", "item1", 10.0, "default_extra"), [Tag("vip")]),
        [Order("O1", "item1", 10.0, "default_extra"), Order("O2", "item2", 20.0, "default_extra")],
        2
    )

def make_engine_order(fields):
    return make_dataclass('EngineOrder', fields)

def make_python_order(fields, defaults=None):
    if defaults is None:
        defaults = {}
    # Move all fields with defaults to the end (Python dataclass requirement)
    non_default_fields = [(n, t) for n, t in fields if n not in defaults]
    default_fields = [(n, t) for n, t in fields if n in defaults]
    ordered_fields = non_default_fields + default_fields
    # Prepare the namespace for defaults (only for fields at the end)
    namespace = {k: defaults[k] for k, _ in default_fields}
    return make_dataclass('PythonOrder', ordered_fields, namespace=namespace)

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
    ]
)
def test_field_position_cases(engine_fields, python_fields, python_defaults, engine_val, expected_python_val):
    EngineOrder = make_engine_order(engine_fields)
    PythonOrder = make_python_order(python_fields, python_defaults)
    converter = build_engine_value_converter(EngineOrder, PythonOrder)
    # Map field names to expected values
    expected_dict = dict(zip([f[0] for f in python_fields], expected_python_val))
    # Instantiate using keyword arguments (order doesn't matter)
    assert converter(engine_val) == PythonOrder(**expected_dict)
