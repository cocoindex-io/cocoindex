from __future__ import annotations

import asyncio

import pytest

from coco_harness.errors import TransientError, retrying


def test_retrying_retries_transient_errors_only() -> None:
    attempts = 0

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise TransientError("blip")
        return "ok"

    assert asyncio.run(retrying(flaky, backoff=lambda _attempt: 0.0)) == "ok"
    assert attempts == 3

    async def broken() -> str:
        raise RuntimeError("not transient")

    with pytest.raises(RuntimeError, match="not transient"):
        asyncio.run(retrying(broken, backoff=lambda _attempt: 0.0))


def test_retrying_gives_up_after_the_attempt_cap() -> None:
    attempts = 0

    async def always() -> str:
        nonlocal attempts
        attempts += 1
        raise TransientError("still down")

    with pytest.raises(TransientError, match="still down"):
        asyncio.run(retrying(always, backoff=lambda _attempt: 0.0))
    assert attempts == 5
