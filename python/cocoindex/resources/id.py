"""
Stable ID generation utilities.

This module provides two approaches for generating stable IDs/UUIDs:

1. **Simple functions** (`generate_id`, `generate_uuid`):
   - Return the **same** ID/UUID for the **same** `dep` value (within a component)
   - Idempotent: calling multiple times with identical `dep` yields identical results
   - Use when each unique input should map to exactly one ID

2. **Generator classes** (`IdGenerator`, `UuidGenerator`):
   - Return a **distinct** ID/UUID on each call, even for the **same** `dep` value
   - Maintains internal state to track call count per `dep`
   - Use when you need multiple IDs for potentially non-distinct inputs
     (e.g., splitting text into chunks where chunks may have identical content)
"""

from __future__ import annotations

__all__ = ["IdGenerator", "UuidGenerator", "generate_id", "generate_uuid"]

import typing as _typing
import uuid as _uuid

import cocoindex as _coco
from cocoindex._internal import component_ctx as _component_ctx
from cocoindex._internal import memo_key as _memo_key


@_coco.function(memo=True)
def generate_id(_dep: _typing.Any = None) -> int:
    """
    Generate a stable unique ID for a given dependency value.

    Returns the **same** ID for the **same** `dep` value within a processing
    component. This is idempotent: multiple calls with identical `dep` yield
    identical IDs.

    Use this when each unique input should map to exactly one ID. If you need
    distinct IDs for potentially identical inputs (e.g., chunks with duplicate
    content), use `IdGenerator` instead.

    Args:
        dep: Dependency value that determines the ID. The same `dep` always
            produces the same ID within a component. Defaults to None.

    Returns:
        A unique integer ID (IDs start from 1; 0 is reserved).

    Example:
        @coco.function(memo=True)
        def process_item(item: Item) -> Row:
            # Same item.key always gets the same ID
            item_id = generate_id(item.key)
            return Row(id=item_id, data=item.data)
    """
    return _component_ctx.next_id(None)


@_coco.function(memo=True)
def generate_uuid(_dep: _typing.Any = None) -> _uuid.UUID:
    """
    Generate a stable unique UUID for a given dependency value.

    Returns the **same** UUID for the **same** `dep` value within a processing
    component. This is idempotent: multiple calls with identical `dep` yield
    identical UUIDs.

    Use this when each unique input should map to exactly one UUID. If you need
    distinct UUIDs for potentially identical inputs, use `UuidGenerator` instead.

    Args:
        dep: Dependency value that determines the UUID. The same `dep` always
            produces the same UUID within a component. Defaults to None.

    Returns:
        A unique UUID.

    Example:
        @coco.function(memo=True)
        def process_item(item: Item) -> Row:
            # Same item.key always gets the same UUID
            item_uuid = generate_uuid(item.key)
            return Row(id=item_uuid, data=item.data)
    """
    return _uuid.uuid4()


class IdGenerator:
    """
    Generator for stable unique IDs that produces distinct IDs on each call.

    Unlike `generate_id()` which returns the same ID for the same `dep`,
    `IdGenerator.next_id()` returns a **distinct** ID on each call, even when
    called with the same `dep` value. The sequence of IDs is stable across runs.

    Use this when you need multiple IDs for potentially non-distinct inputs,
    such as when splitting text into chunks where chunks may have identical
    content but still need unique IDs.

    Example:
        @coco.function(memo=True)
        def process_document(doc: Document) -> list[Row]:
            id_gen = IdGenerator()
            rows = []
            for chunk in split_into_chunks(doc.content):
                # Each call returns a distinct ID, even if chunks are identical
                chunk_id = id_gen.next_id(chunk.content)
                rows.append(Row(id=chunk_id, content=chunk.content))
            return rows
    """

    __slots__ = ("_ordinals",)
    _ordinals: dict[bytes, int]

    def __init__(self) -> None:
        self._ordinals = {}

    def next_id(self, dep: _typing.Any = None) -> int:
        """
        Generate the next unique ID.

        Returns a **distinct** ID on each call, even when called with the same
        `dep` value. The sequence is stable across runs.

        Args:
            dep: Dependency value for stable sequencing. Multiple calls with
                the same `dep` return distinct IDs in a deterministic order.
                Defaults to None.

        Returns:
            A unique integer ID (IDs start from 1; 0 is reserved).
        """
        # Get fingerprint bytes for dep
        fp = _memo_key.memo_key(dep)
        fp_bytes = bytes(fp)

        # Get and increment ordinal for this fingerprint
        ordinal = self._ordinals.get(fp_bytes, 0)
        self._ordinals[fp_bytes] = ordinal + 1

        # Call internal memoized function with (fp_bytes, ordinal)
        return _generate_next_id(fp_bytes, ordinal)


class UuidGenerator:
    """
    Generator for stable unique UUIDs that produces distinct UUIDs on each call.

    Unlike `generate_uuid()` which returns the same UUID for the same `dep`,
    `UuidGenerator.next_uuid()` returns a **distinct** UUID on each call, even
    when called with the same `dep` value. The sequence of UUIDs is stable
    across runs.

    Use this when you need multiple UUIDs for potentially non-distinct inputs,
    such as when splitting text into chunks where chunks may have identical
    content but still need unique UUIDs.

    Example:
        @coco.function(memo=True)
        def process_document(doc: Document) -> list[Row]:
            uuid_gen = UuidGenerator()
            rows = []
            for chunk in split_into_chunks(doc.content):
                # Each call returns a distinct UUID, even if chunks are identical
                chunk_uuid = uuid_gen.next_uuid(chunk.content)
                rows.append(Row(id=chunk_uuid, content=chunk.content))
            return rows
    """

    __slots__ = ("_ordinals",)
    _ordinals: dict[bytes, int]

    def __init__(self) -> None:
        self._ordinals = {}

    def next_uuid(self, dep: _typing.Any = None) -> _uuid.UUID:
        """
        Generate the next unique UUID.

        Returns a **distinct** UUID on each call, even when called with the same
        `dep` value. The sequence is stable across runs.

        Args:
            dep: Dependency value for stable sequencing. Multiple calls with
                the same `dep` return distinct UUIDs in a deterministic order.
                Defaults to None.

        Returns:
            A unique UUID.
        """
        # Get fingerprint bytes for dep
        fp = _memo_key.memo_key(dep)
        fp_bytes = bytes(fp)

        # Get and increment ordinal for this fingerprint
        ordinal = self._ordinals.get(fp_bytes, 0)
        self._ordinals[fp_bytes] = ordinal + 1

        # Call internal memoized function with (fp_bytes, ordinal)
        return _generate_next_uuid(fp_bytes, ordinal)


@_coco.function(memo=True)
def _generate_next_id(_fp_bytes: bytes, _ordinal: int) -> int:
    """Internal memoized function that generates the actual ID."""
    return _component_ctx.next_id(None)


@_coco.function(memo=True)
def _generate_next_uuid(_fp_bytes: bytes, _ordinal: int) -> _uuid.UUID:
    """Internal memoized function that generates the actual UUID."""
    return _uuid.uuid4()
