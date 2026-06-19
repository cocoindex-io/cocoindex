"""Rate limiting.

A token-bucket :class:`RateLimiter` for throttling outbound work — e.g.
API calls in a source/target connector. The implementation is the
``governor``-backed limiter in the Rust core; this module is the public
Python surface.
"""

from cocoindex._internal import core as _core

RateLimiter = _core.RateLimiter

__all__ = ["RateLimiter"]
