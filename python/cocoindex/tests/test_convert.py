import dataclasses
import uuid
import datetime
from dataclasses import dataclass
import pytest
from cocoindex.convert import to_engine_value

@dataclass
class Order:
    order_id: str
    name: str
    price: float

@dataclass
class Basket:
    items: list

@dataclass
class Customer:
    name: str
    order: Order

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
    assert to_engine_value(order) == ["O123", "mixed nuts", 25.0]

def test_to_engine_value_list_of_structs():
    orders = [Order("O1", "item1", 10.0), Order("O2", "item2", 20.0)]
    assert to_engine_value(orders) == [["O1", "item1", 10.0], ["O2", "item2", 20.0]]

def test_to_engine_value_struct_with_list():
    basket = Basket(items=["apple", "banana"])
    assert to_engine_value(basket) == [["apple", "banana"]]

def test_to_engine_value_nested_struct():
    customer = Customer(name="Alice", order=Order("O1", "item1", 10.0))
    assert to_engine_value(customer) == ["Alice", ["O1", "item1", 10.0]]

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
