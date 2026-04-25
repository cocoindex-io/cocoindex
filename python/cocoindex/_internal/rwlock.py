"""
Fair Read-Write Lock.

Re-exports from the Rust core module (tokio::sync::RwLock via PyO3).
"""

from .core import RWLock, RWLockReadGuard, RWLockWriteGuard

__all__ = ["RWLock", "RWLockReadGuard", "RWLockWriteGuard"]
