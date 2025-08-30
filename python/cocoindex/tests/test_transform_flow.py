import typing
from dataclasses import dataclass
from typing import Any

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
def extract_value(value: int) -> int:
    """Extracts the value."""
    return value


@cocoindex.transform_flow()
def for_each_transform(
    data: cocoindex.DataSlice[Parent],
) -> cocoindex.DataSlice[Any]:
    """Transform flow that processes child rows to extract values."""
    with data["children"].row() as child:
        child["new_field"] = child["value"].transform(extract_value)
    return data


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
    expected = {
        "children": [
            {"value": 1, "new_field": 1},
            {"value": 2, "new_field": 2},
            {"value": 3, "new_field": 3},
        ]
    }
    assert result == expected, f"Expected {expected}, got {result}"

    input_data = Parent(children=[])
    result = for_each_transform.eval(input_data)
    assert result == {"children": []}, f"Expected {{'children': []}}, got {result}"


@pytest.mark.asyncio
async def test_for_each_transform_flow_async() -> None:
    """Test the complex transform flow asynchronously."""
    input_data = Parent(children=[Child(4), Child(5)])
    result = await for_each_transform.eval_async(input_data)
    expected = {
        "children": [
            {"value": 4, "new_field": 4},
            {"value": 5, "new_field": 5},
        ]
    }

    assert result == expected, f"Expected {expected}, got {result}"


def test_none_arg_yield_none_result() -> None:
    """Test that None arguments yield None results."""

    @cocoindex.op.function()
    def custom_fn(
        required_arg: int,
        optional_arg: int | None,
        required_kwarg: int,
        optional_kwarg: int | None,
    ) -> int:
        return (
            required_arg + (optional_arg or 0) + required_kwarg + (optional_kwarg or 0)
        )

    @cocoindex.transform_flow()
    def transform_flow(
        required_arg: cocoindex.DataSlice[int | None],
        optional_arg: cocoindex.DataSlice[int | None],
        required_kwarg: cocoindex.DataSlice[int | None],
        optional_kwarg: cocoindex.DataSlice[int | None],
    ) -> cocoindex.DataSlice[int | None]:
        return required_arg.transform(
            custom_fn,
            optional_arg,
            required_kwarg=required_kwarg,
            optional_kwarg=optional_kwarg,
        )

    result = transform_flow.eval(1, 2, 4, 8)
    assert result == 15, f"Expected 15, got {result}"

    result = transform_flow.eval(1, None, 4, None)
    assert result == 5, f"Expected 5, got {result}"

    result = transform_flow.eval(None, 2, 4, 8)
    assert result is None, f"Expected None, got {result}"

    result = transform_flow.eval(1, 2, None, None)
    assert result is None, f"Expected None, got {result}"


# Test GPU function behavior.
# They're not really executed on GPU, but we want to make sure they're scheduled on subprocesses correctly.


@cocoindex.op.function(gpu=True)
def gpu_append_world(text: str) -> str:
    """Append ' world' to the input text."""
    return f"{text} world"


class GpuAppendSuffix(cocoindex.op.FunctionSpec):
    suffix: str


@cocoindex.op.executor_class(gpu=True)
class GpuAppendSuffixExecutor:
    spec: GpuAppendSuffix

    def __call__(self, text: str) -> str:
        return f"{text}{self.spec.suffix}"


class GpuAppendSuffixWithAnalyzePrepare(cocoindex.op.FunctionSpec):
    suffix: str


@cocoindex.op.executor_class(gpu=True)
class GpuAppendSuffixWithAnalyzePrepareExecutor:
    spec: GpuAppendSuffixWithAnalyzePrepare
    suffix: str

    def analyze(self) -> Any:
        return str

    def prepare(self) -> None:
        self.suffix = self.spec.suffix

    def __call__(self, text: str) -> str:
        return f"{text}{self.suffix}"


def test_gpu_function() -> None:
    @cocoindex.transform_flow()
    def transform_flow(text: cocoindex.DataSlice[str]) -> cocoindex.DataSlice[str]:
        return text.transform(gpu_append_world).transform(GpuAppendSuffix(suffix="!"))

    result = transform_flow.eval("Hello")
    expected = "Hello world!"
    assert result == expected, f"Expected {expected}, got {result}"

    @cocoindex.transform_flow()
    def transform_flow_with_analyze_prepare(
        text: cocoindex.DataSlice[str],
    ) -> cocoindex.DataSlice[str]:
        return text.transform(gpu_append_world).transform(
            GpuAppendSuffixWithAnalyzePrepare(suffix="!!")
        )

    result = transform_flow_with_analyze_prepare.eval("Hello")
    expected = "Hello world!!"
    assert result == expected, f"Expected {expected}, got {result}"
