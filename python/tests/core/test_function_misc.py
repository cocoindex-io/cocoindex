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
