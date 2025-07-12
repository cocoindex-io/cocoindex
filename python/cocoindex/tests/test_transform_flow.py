import typing
from dataclasses import dataclass

import pytest

import cocoindex


@dataclass
class Child:
    value: int


@dataclass
class Parent:
    children: list[Child]


# Fixture to initialize CocoIndex library
@pytest.fixture(scope="session", autouse=True)
def init_cocoindex() -> typing.Generator[None, None, None]:
    cocoindex.init()
    yield


@cocoindex.op.function()
def add_suffix(text: str) -> str:
    """Append ' world' to the input text."""
    return f"{text} world"


@cocoindex.transform_flow()
def simple_transform(text: cocoindex.DataSlice[str]) -> cocoindex.DataSlice[str]:
    """Transform flow that applies add_suffix to input text."""
    return text.transform(add_suffix)


@cocoindex.op.function()
def extract_child_values(parent: Parent) -> list[int]:
    """Extract values from each child in the Parent's children list."""
    return [child.value for child in parent.children]


@cocoindex.transform_flow()
def for_each_transform(
    data: cocoindex.DataSlice[Parent],
) -> cocoindex.DataSlice[list[int]]:
    """Transform flow that processes child rows to extract values."""
    return data.transform(extract_child_values)


def test_simple_transform_flow() -> None:
    """Test the simple transform flow."""
    input_text = "hello"
    result = simple_transform.eval(input_text)
    assert result == "hello world", f"Expected 'hello world', got {result}"

    result = simple_transform.eval("")
    assert result == " world", f"Expected ' world', got {result}"


@pytest.mark.asyncio
async def test_simple_transform_flow_async() -> None:
    """Test the simple transform flow asynchronously."""
    input_text = "async"
    result = await simple_transform.eval_async(input_text)
    assert result == "async world", f"Expected 'async world', got {result}"


def test_for_each_transform_flow() -> None:
    """Test the complex transform flow with child rows."""
    input_data = Parent(children=[Child(1), Child(2), Child(3)])
    result = for_each_transform.eval(input_data)
    expected = [1, 2, 3]
    assert result == expected, f"Expected {expected}, got {result}"

    input_data = Parent(children=[])
    result = for_each_transform.eval(input_data)
    assert result == [], f"Expected [], got {result}"


@pytest.mark.asyncio
async def test_for_each_transform_flow_async() -> None:
    """Test the complex transform flow asynchronously."""
    input_data = Parent(children=[Child(4), Child(5)])
    result = await for_each_transform.eval_async(input_data)
    expected = [4, 5]
    assert result == expected, f"Expected {expected}, got {result}"
