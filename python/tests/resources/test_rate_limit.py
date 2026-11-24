import math

import pytest

from cocoindex.resources.rate_limit import RateLimiter


@pytest.mark.parametrize("rate", [0.0, -1.0, math.nan, -math.inf, 1e-300, 1e-12])
def test_rejects_an_unusable_rate(rate: float) -> None:
    with pytest.raises(ValueError):
        RateLimiter(max_rows_per_second=rate)


@pytest.mark.parametrize("window", [math.inf, 1e300])
def test_rejects_a_burst_window_out_of_range(window: float) -> None:
    with pytest.raises(ValueError):
        RateLimiter(max_rows_per_second=1.0, burst_window_secs=window)


@pytest.mark.parametrize(
    ("rate", "window"),
    [(1e-9, 1.0), (1e12, 1.0), (math.inf, 1.0), (0.01, 1e15), (1.0, math.nan)],
)
@pytest.mark.asyncio
async def test_accepts_extreme_but_valid_settings(rate: float, window: float) -> None:
    limiter = RateLimiter(max_rows_per_second=rate, burst_window_secs=window)
    await limiter.acquire()
