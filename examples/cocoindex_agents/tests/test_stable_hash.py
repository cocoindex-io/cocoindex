from __future__ import annotations

import pytest

from cocoindex_agents.stable_hash import (
    HASH_BITS,
    bit_at,
    ident_from_prefix,
    stable_hash64,
)


def test_deterministic_and_64_bit() -> None:
    a = stable_hash64("docs/a.md")
    assert a == stable_hash64("docs/a.md")
    assert 0 <= a < 2**HASH_BITS
    assert a != stable_hash64("docs/b.md")


def test_parts_are_length_prefixed() -> None:
    assert stable_hash64("ab", "c") != stable_hash64("a", "bc")
    assert stable_hash64("abc") != stable_hash64("ab", "c")


def test_bit_at_is_msb_first() -> None:
    top = 1 << (HASH_BITS - 1)
    assert bit_at(top, 0) == 1
    assert bit_at(top, 1) == 0
    assert bit_at(1, HASH_BITS - 1) == 1
    with pytest.raises(ValueError):
        bit_at(1, HASH_BITS)


def test_ident_from_prefix_round_trips_through_bit_at() -> None:
    prefix = "1011"
    ident = ident_from_prefix(prefix)
    assert "".join(str(bit_at(ident, i)) for i in range(len(prefix))) == prefix
    assert ident_from_prefix("") == 0
    with pytest.raises(ValueError):
        ident_from_prefix("10x")
