"""
Auth registry is used to register and reference auth entries.
"""

from dataclasses import dataclass

from . import _engine
from .convert import dump_engine_object

@dataclass
class AuthEntryReference:
    """Reference an auth entry by its key."""
    key: str

def add_auth_entry(key: str, value) -> AuthEntryReference:
    """Add an auth entry to the registry. Returns its reference."""
    _engine.add_auth_entry(key, dump_engine_object(value))
    return AuthEntryReference(key)

def ref_auth_entry(key: str) -> AuthEntryReference:
    """Reference an auth entry by its key."""
    return AuthEntryReference(key)