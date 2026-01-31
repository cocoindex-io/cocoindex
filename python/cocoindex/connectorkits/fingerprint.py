"""
Fingerprinting utilities for CocoIndex connectors.

This module provides functions for computing deterministic fingerprints
of values, useful for change detection in target state tracking.
"""

from cocoindex._internal.memo_key import memo_key as _memo_key
from cocoindex._internal.core import Fingerprint as Fingerprint
from cocoindex._internal.core import fingerprint_bytes as _fingerprint_bytes
from cocoindex._internal.core import fingerprint_str as _fingerprint_str
from cocoindex._internal.typing import Fingerprintable as Fingerprintable


def fingerprint_bytes(data: bytes) -> bytes:
    """Compute a fingerprint for raw bytes and return it as bytes.

    This function directly hashes the bytes without any type encoding,
    making it more efficient when the input is always bytes.
    """
    return _fingerprint_bytes(data).as_bytes()


def fingerprint_str(s: str) -> bytes:
    """Compute a fingerprint for a string and return it as bytes.

    This function directly hashes the UTF-8 encoded string without any type
    encoding, making it more efficient when the input is always a string.
    """
    return _fingerprint_str(s).as_bytes()


def fingerprint_object(obj: object) -> bytes:
    """Compute a fingerprint for an object to identify its identity across runs and return it as bytes.

    This uses memo key mechanism to compute the fingerprint.
    """
    return _memo_key(obj).as_bytes()


__all__ = [
    "Fingerprint",
    "Fingerprintable",
    "fingerprint_object",
    "fingerprint_bytes",
    "fingerprint_str",
]
