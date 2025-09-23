"""
Auth registry is used to register and reference auth entries.
"""

from dataclasses import dataclass
from typing import Generic, TypeVar

from . import _engine  # type: ignore
from .convert import dump_engine_object, load_engine_object

T = TypeVar("T")


@dataclass
class TransientAuthEntryReference(Generic[T]):
    """Reference an auth entry, may or may not have a stable key."""

    key: str


class AuthEntryReference(TransientAuthEntryReference[T]):
    """Reference an auth entry, with a key stable across ."""


def add_transient_auth_entry(value: T) -> TransientAuthEntryReference[T]:
    """Add an auth entry to the registry. Returns its reference."""
    key = _engine.add_transient_auth_entry(dump_engine_object(value))
    return TransientAuthEntryReference(key)


def add_auth_entry(key: str, value: T) -> AuthEntryReference[T]:
    """Add an auth entry to the registry. Returns its reference."""
    _engine.add_auth_entry(key, dump_engine_object(value))
    return AuthEntryReference(key)


def ref_auth_entry(key: str) -> AuthEntryReference[T]:
    """Reference an auth entry by its key."""
    return AuthEntryReference(key)


def get_auth_entry(cls: type[T], ref: TransientAuthEntryReference[T]) -> T:
    """Get an auth entry by its key."""
    return load_engine_object(cls, _engine.get_auth_entry(ref.key))
