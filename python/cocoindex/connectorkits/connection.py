"""
Common utilities for managing keyed connections to external systems.

This module provides base types and registry patterns for connectors that need
to maintain stable connection pools or clients across runs.
"""

from __future__ import annotations

import threading
from typing import Any, Generic, TypeVar

from typing_extensions import Self

# Type variable for the connection/pool type
ConnectionT = TypeVar("ConnectionT")


class KeyedConnection(Generic[ConnectionT]):
    """
    Base class for database/service handles with a stable connection key.

    The key should be stable across runs - it identifies the logical connection/database.
    The underlying connection can be recreated with different parameters as long as
    the same key is used.

    Can be used as a context manager to automatically unregister on exit.

    Type Parameters:
        ConnectionT: The type of connection/pool being managed.
    """

    __slots__ = ("_connection_key", "_registry")

    _connection_key: str
    _registry: ConnectionRegistry[ConnectionT]

    def __init__(self, key: str, registry: ConnectionRegistry[ConnectionT]) -> None:
        self._connection_key = key
        self._registry = registry

    @property
    def connection_key(self) -> str:
        """The stable key for this connection."""
        return self._connection_key

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self._registry.unregister(self._connection_key)

    def __coco_memo_key__(self) -> str:
        return self._connection_key


class ConnectionRegistry(Generic[ConnectionT]):
    """
    Thread-safe registry for managing keyed connections.

    This class provides registration, lookup, and cleanup for connection pools
    or clients that need to be shared across multiple components.

    Type Parameters:
        ConnectionT: The type of connection/pool being managed (e.g., asyncpg.Pool).
    """

    __slots__ = ("_registry", "_lock")

    _registry: dict[str, ConnectionT]
    _lock: threading.Lock

    def __init__(self) -> None:
        self._registry = {}
        self._lock = threading.Lock()

    def register(self, key: str, connection: ConnectionT) -> None:
        """
        Register a connection with a stable key.

        Args:
            key: A stable identifier for this connection.
            connection: The connection/pool to register.

        Raises:
            ValueError: If a connection with the given key is already registered.
        """
        with self._lock:
            if key in self._registry:
                raise ValueError(
                    f"Connection with key '{key}' is already registered. "
                    f"Use a different key or unregister the existing one first."
                )
            self._registry[key] = connection

    def get(self, key: str) -> ConnectionT:
        """
        Get the connection for the given key.

        Args:
            key: The connection key.

        Returns:
            The registered connection.

        Raises:
            RuntimeError: If no connection is registered with the given key.
        """
        with self._lock:
            connection = self._registry.get(key)
        if connection is None:
            raise RuntimeError(
                f"No connection registered with key '{key}'. "
                f"Register the connection first."
            )
        return connection

    def unregister(self, key: str) -> None:
        """
        Unregister a connection.

        Args:
            key: The connection key to unregister.
        """
        with self._lock:
            self._registry.pop(key, None)

    def is_registered(self, key: str) -> bool:
        """
        Check if a connection is registered.

        Args:
            key: The connection key to check.

        Returns:
            True if the key is registered, False otherwise.
        """
        with self._lock:
            return key in self._registry


__all__ = [
    "ConnectionRegistry",
    "KeyedConnection",
]
