"""
Auth registry is used to register and reference auth entries.
"""

from dataclasses import dataclass
from typing import Generic, TypeVar
import threading

from . import _engine  # type: ignore
from .convert import dump_engine_object

T = TypeVar("T")

# Global atomic counter for generating unique auth entry keys
_counter_lock = threading.Lock()
_auth_key_counter = 0


def _generate_auth_key() -> str:
    """Generate a unique auth entry key using a global atomic counter."""
    global _auth_key_counter  # pylint: disable=global-statement
    with _counter_lock:
        _auth_key_counter += 1
        return f"__auth_{_auth_key_counter}"


@dataclass
class TransientAuthEntryReference(Generic[T]):
    """Reference an auth entry, may or may not have a stable key."""

    key: str


class AuthEntryReference(TransientAuthEntryReference[T]):
    """Reference an auth entry, with a key stable across ."""


def add_transient_auth_entry(value: T) -> TransientAuthEntryReference[T]:
    """Add an auth entry to the registry. Returns its reference."""
    return add_auth_entry(_generate_auth_key(), value)


def add_auth_entry(key: str, value: T) -> AuthEntryReference[T]:
    """Add an auth entry to the registry. Returns its reference."""
    _engine.add_auth_entry(key, dump_engine_object(value))
    return AuthEntryReference(key)


def ref_auth_entry(key: str) -> AuthEntryReference[T]:
    """Reference an auth entry by its key."""
    return AuthEntryReference(key)
