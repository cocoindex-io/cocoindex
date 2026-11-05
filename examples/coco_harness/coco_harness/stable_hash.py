"""Stable, seeded hashing for grouping and sampling.

Every hash that decides *structure* — which reduce group an item lands in
(DESIGN.md §6), which items a stable sample shows (§5) — must give the same
answer on every machine and in every process. Python's ``hash()`` is salted
per process and is never used here. Changing ``_SEED`` or the algorithm
reshuffles every group and sample, so treat it as a version bump.
"""

from __future__ import annotations

import hashlib

HASH_BITS = 64

# Keyed blake2b: the key acts as the seed. Bump the suffix on any change.
_SEED = b"coco-harness/stable-hash/v1"


def stable_hash64(*parts: str | bytes) -> int:
    """Hash ``parts`` to a 64-bit unsigned integer.

    Parts are length-prefixed so ``("ab", "c")`` and ``("a", "bc")`` differ.
    """
    h = hashlib.blake2b(digest_size=HASH_BITS // 8, key=_SEED)
    for part in parts:
        data = part.encode("utf-8") if isinstance(part, str) else part
        h.update(len(data).to_bytes(8, "big"))
        h.update(data)
    return int.from_bytes(h.digest(), "big")


def bit_at(ident: int, index: int) -> int:
    """Bit ``index`` of a 64-bit identity, counting from the most significant bit.

    MSB-first numbering makes a group's prefix string a textual prefix of every
    member's bit string, which is what the bisection in ``tools.reduce`` relies on.
    """
    if not 0 <= index < HASH_BITS:
        raise ValueError(f"bit index out of range: {index}")
    return (ident >> (HASH_BITS - 1 - index)) & 1


def ident_from_prefix(prefix: str) -> int:
    """Left-align a bit-string prefix into a 64-bit identity (zero-padded)."""
    if len(prefix) > HASH_BITS or any(c not in "01" for c in prefix):
        raise ValueError(f"not a bit prefix: {prefix!r}")
    if not prefix:
        return 0
    return int(prefix.ljust(HASH_BITS, "0"), 2)
