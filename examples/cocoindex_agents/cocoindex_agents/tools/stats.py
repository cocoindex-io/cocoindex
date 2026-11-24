"""Stable observations: widened counts and hash-stable samples (DESIGN.md §5).

Both exist to keep tool results — and therefore transcripts — unchanged when
the population changes a little.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import NamedTuple

from ..stable_hash import stable_hash64


class Bucket(NamedTuple):
    """A widened count: the half-open range ``[lo, hi)``. Compares equal to a plain tuple."""

    lo: int
    hi: int


def widen_count(n: int, prev: Bucket | None = None, slack: float = 0.1) -> Bucket:
    """The log₂ bucket of ``n``, with hysteresis against the previous bucket.

    While ``n`` stays within ``slack`` of the previous bucket's bounds, the
    previous bucket is kept, so a count hovering near 1,024 does not flip the
    reported range back and forth between runs.
    """
    if n < 0:
        raise ValueError("count cannot be negative")
    if prev is not None:
        if prev.lo * (1 - slack) <= n < prev.hi * (1 + slack) and (
            prev.lo > 0 or n == 0
        ):
            return prev
    if n == 0:
        return Bucket(0, 0)
    lo = 1 << (n.bit_length() - 1)
    return Bucket(lo, lo * 2)


def format_range(bucket: Bucket) -> str:
    if bucket.hi == 0:
        return "0"
    return f"{_short(bucket.lo)}–{_short(bucket.hi)}"


def _short(n: int) -> str:
    """1024 → ``1k``, 1048576 → ``1M`` (buckets are powers of two); exact thousands likewise."""
    for unit, size in (("M", 1 << 20), ("k", 1 << 10), ("M", 1_000_000), ("k", 1_000)):
        if n >= size and n % size == 0:
            return f"{n // size}{unit}"
    return str(n)


def bottom_k(keys: Iterable[str], k: int) -> list[str]:
    """The ``k`` keys with the smallest stable hash: a sample that barely moves.

    Adding one item to a population of N changes the sample with probability
    about k/N; removing a sampled item replaces just that one slot.
    """
    if k <= 0:
        return []
    ranked = sorted(keys, key=lambda key: (stable_hash64(key), key))
    return ranked[:k]
