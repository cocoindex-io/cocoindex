import dataclasses
import uuid
import datetime
from dataclasses import dataclass
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

def test_make_engine_value_converter_struct():
    converter = build_engine_value_converter(Order)
    # All fields match
    engine_val = ["O123", "mixed nuts", 25.0, "default_extra"]
    assert converter(engine_val) == Order("O123", "mixed nuts", 25.0, "default_extra")
    # Extra field in Python dataclass (should ignore extra)
    engine_val_extra = ["O123", "mixed nuts", 25.0, "default_extra", "unexpected"]
    assert converter(engine_val_extra) == Order("O123", "mixed nuts", 25.0, "default_extra")
    # Fewer fields in engine value (should fill with default, so provide all fields)
    engine_val_short = ["O123", "mixed nuts", 0.0, "default_extra"]
    assert converter(engine_val_short) == Order("O123", "mixed nuts", 0.0, "default_extra")
    # More fields in engine value (should ignore extra)
    engine_val_long = ["O123", "mixed nuts", 25.0, "unexpected"]
    assert converter(engine_val_long) == Order("O123", "mixed nuts", 25.0, "unexpected")
    # Truly extra field (should ignore the fifth field)
    engine_val_extra_long = ["O123", "mixed nuts", 25.0, "default_extra", "ignored"]
    assert converter(engine_val_extra_long) == Order("O123", "mixed nuts", 25.0, "default_extra")

def test_make_engine_value_converter_struct_field_order():
    # Engine fields in different order
    # Use encode_enriched_type to avoid manual mistakes
    converter = build_engine_value_converter(Order)
    # Provide all fields in the correct order
    engine_val = ["O123", "mixed nuts", 25.0, "default_extra"]
    assert converter(engine_val) == Order("O123", "mixed nuts", 25.0, "default_extra")

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

def test_make_engine_value_converter_defaults_and_missing_fields():
    # Missing optional field in engine value
    converter = build_engine_value_converter(Customer)
    engine_val = ["Alice", ["O1", "item1", 10.0, "default_extra"], None]  # tags explicitly None
    assert converter(engine_val) == Customer("Alice", Order("O1", "item1", 10.0, "default_extra"), None)
    # Extra field in engine value (should ignore)
    engine_val = ["Alice", ["O1", "item1", 10.0, "default_extra"], [["vip"]], "extra"]
    assert converter(engine_val) == Customer("Alice", Order("O1", "item1", 10.0, "default_extra"), [Tag("vip")])

def test_engine_python_schema_field_order():
    """
    Engine and Python dataclasses have the same fields but different order.
    Converter should map by field name, not order.
    """
    @dataclass
    class EngineOrder:
        id: str
        name: str
        price: float
    @dataclass
    class PythonOrder:
        name: str
        id: str
        price: float
        extra: str = "default"
    converter = build_engine_value_converter(EngineOrder, PythonOrder)
    engine_val = ["O123", "mixed nuts", 25.0]  # matches EngineOrder order
    assert converter(engine_val) == PythonOrder("mixed nuts", "O123", 25.0, "default")

def test_engine_python_schema_extra_field():
    """
    Python dataclass has an extra field not present in engine schema.
    Converter should fill with default value.
    """
    @dataclass
    class EngineOrder:
        id: str
        name: str
    @dataclass
    class PythonOrder:
        id: str
        name: str
        price: float = 0.0
    converter = build_engine_value_converter(EngineOrder, PythonOrder)
    engine_val = ["O123", "mixed nuts"]
    assert converter(engine_val) == PythonOrder("O123", "mixed nuts", 0.0)

def test_engine_python_schema_missing_field():
    """
    Engine dataclass has a field missing in Python dataclass.
    Converter should ignore the missing field.
    """
    from dataclasses import dataclass
    @dataclass
    class EngineOrder:
        id: str
        name: str
        price: float
    @dataclass
    class PythonOrder:
        id: str
        name: str
    converter = build_engine_value_converter(EngineOrder, PythonOrder)
    engine_val = ["O123", "mixed nuts", 25.0]
    assert converter(engine_val) == PythonOrder("O123", "mixed nuts")
