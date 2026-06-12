import pytest
import cocoindex as coco


@coco.fn.as_async
def async_wrapped_fn_1(s: str, i: int) -> str:
    return f"{s} {i}"


@coco.fn.as_async()
def async_wrapped_fn_2(s: str, i: int) -> str:
    return f"{s} {i}"


@pytest.mark.asyncio
async def test_async_wrapped_fn() -> None:
    assert await async_wrapped_fn_1("Hello", 3) == "Hello 3"
    assert await async_wrapped_fn_2("Hello", 3) == "Hello 3"


@coco.fn
def sync_fn(s: str, i: int) -> str:
    return f"{s} {i}"


def test_sync_fn_callable_standalone() -> None:
    # A @coco.fn is callable outside any component context: it runs the raw
    # function with no memoization, mirroring the async __call__ path.
    assert sync_fn("Hello", 3) == "Hello 3"


class _StandaloneHolder:
    def __init__(self, factor: int) -> None:
        self._factor = factor

    @coco.fn(version=1, logic_tracking="self")
    def run(self, x: int) -> int:
        return x * self._factor


def test_sync_method_fn_standalone() -> None:
    # A versioned, self-tracked @coco.fn method is directly callable outside a
    # component context.
    assert _StandaloneHolder(2).run(21) == 42
