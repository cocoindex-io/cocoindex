"""Unit tests for ContextKey and ContextProvider."""

from __future__ import annotations

import pytest

from cocoindex._internal.context_keys import ContextKey, ContextProvider


def test_context_provider_get_by_key_str() -> None:
    """get(str) returns the provided value and raises KeyError for missing keys."""
    key = ContextKey[int]("test_get_by_key_str_unique_42", detect_change=False)
    provider = ContextProvider()
    provider.provide(key, 99)

    assert provider.get("test_get_by_key_str_unique_42") == 99

    with pytest.raises(KeyError):
        provider.get("nonexistent_key_xyz_999")


def test_context_provider_get_by_key_str_with_type() -> None:
    """get(str, type) returns the value and verifies its type at runtime."""
    key = ContextKey[int]("test_get_typed_unique_43", detect_change=False)
    provider = ContextProvider()
    provider.provide(key, 42)

    result = provider.get("test_get_typed_unique_43", int)
    assert result == 42

    with pytest.raises(TypeError, match="expected int, got str"):
        str_key = ContextKey[str]("test_get_typed_wrong_type_44", detect_change=False)
        provider.provide(str_key, "hello")
        provider.get("test_get_typed_wrong_type_44", int)


def test_context_key_coco_memo_key() -> None:
    """ContextKey.__coco_memo_key__() returns the key string."""
    some_unique_key = "test_coco_memo_key_unique_77"
    key = ContextKey[int](some_unique_key, detect_change=False)
    assert key.__coco_memo_key__() == some_unique_key
