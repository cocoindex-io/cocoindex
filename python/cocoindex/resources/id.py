"""Stable ID generation utilities."""

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
    Generate a stable unique ID.

    This function generates unique IDs through the memoization mechanism:
    - If `dep` doesn't change across runs, the same ID is returned consistently
      for the same processing component.
    - If `dep` changes, a new unique ID is generated.

    This is useful for generating stable identifiers for records that need to
    persist across incremental updates.

    Args:
        dep: Optional dependency value. The generated ID is stable as long as
            this value (and the component path) remains the same across runs.
            Defaults to None.

    Returns:
        A unique integer ID (IDs start from 1; 0 is reserved).

    Example:
        @coco.function(memo=True)
        def process_chunk(chunk: Chunk) -> Row:
            # Generate a stable ID for this chunk
            chunk_id = generate_id(chunk.content)
            return Row(id=chunk_id, content=chunk.content)
    """
    del _dep  # Used only for memoization key
    return _component_ctx.next_id(None)


@_coco.function(memo=True)
def generate_uuid(_dep: _typing.Any = None) -> _uuid.UUID:
    """
    Generate a stable unique UUID.

    This function generates unique UUIDs through the memoization mechanism:
    - If `dep` doesn't change across runs, the same UUID is returned consistently
      for the same processing component.
    - If `dep` changes, a new unique UUID is generated.

    This is useful for generating stable identifiers for records that need to
    persist across incremental updates.

    Args:
        dep: Optional dependency value. The generated UUID is stable as long as
            this value (and the component path) remains the same across runs.
            Defaults to None.

    Returns:
        A unique UUID.

    Example:
        @coco.function(memo=True)
        def process_chunk(chunk: Chunk) -> Row:
            # Generate a stable UUID for this chunk
            chunk_uuid = generate_uuid(chunk.content)
            return Row(id=chunk_uuid, content=chunk.content)
    """
    return _uuid.uuid4()


class IdGenerator:
    """
    Generator for stable unique IDs with support for multiple calls per dependency.

    This class maintains a mapping from dependency fingerprints to ordinals,
    allowing multiple unique IDs to be generated for the same dependency value.
    Each call to `next_id()` with the same `dep` value returns a different ID,
    but the sequence of IDs is stable across runs.

    Example:
        @coco.function(memo=True)
        def process_document(doc: Document) -> list[Row]:
            id_gen = IdGenerator()
            rows = []
            for chunk in split_into_chunks(doc.content):
                # Each chunk gets a unique, stable ID
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
        Generate the next unique ID for the given dependency.

        Each call with the same `dep` value returns a different ID, but the
        sequence is stable across runs (for the same processing component).

        Args:
            dep: Optional dependency value. IDs are generated in sequence for
                each unique dependency value. Defaults to None.

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
    Generator for stable unique UUIDs with support for multiple calls per dependency.

    This class maintains a mapping from dependency fingerprints to ordinals,
    allowing multiple unique UUIDs to be generated for the same dependency value.
    Each call to `next_uuid()` with the same `dep` value returns a different UUID,
    but the sequence of UUIDs is stable across runs.

    Example:
        @coco.function(memo=True)
        def process_document(doc: Document) -> list[Row]:
            uuid_gen = UuidGenerator()
            rows = []
            for chunk in split_into_chunks(doc.content):
                # Each chunk gets a unique, stable UUID
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
        Generate the next unique UUID for the given dependency.

        Each call with the same `dep` value returns a different UUID, but the
        sequence is stable across runs (for the same processing component).

        Args:
            dep: Optional dependency value. UUIDs are generated in sequence for
                each unique dependency value. Defaults to None.

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
