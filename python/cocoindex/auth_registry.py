"""
Auth registry is used to register and reference auth entries.
"""

from dataclasses import dataclass
from typing import Generic, TypeVar

from . import _engine  # type: ignore
from .convert import dump_engine_object

T = TypeVar("T")


@dataclass
class AuthEntryReference(Generic[T]):
    """Reference an auth entry by its key."""

    key: str


def add_auth_entry(key: str, value: T) -> AuthEntryReference[T]:
    """Add an auth entry to the registry. Returns its reference."""
    _engine.add_auth_entry(key, dump_engine_object(value))
    return AuthEntryReference(key)


def ref_auth_entry(key: str) -> AuthEntryReference[T]:
    """Reference an auth entry by its key."""
    return AuthEntryReference(key)
