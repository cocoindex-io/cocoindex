"""
Common utilities for managing keyed connections to external systems.

This module provides base types and registry patterns for connectors that need
to maintain stable connection pools or clients across runs.
"""

from __future__ import annotations

__all__ = [
    "ConnectionRegistry",
    "KeyedConnection",
]

import threading as _threading
from typing import Any as _Any, Generic as _Generic, TypeVar as _TypeVar

from cocoindex._internal import core as _core
from typing_extensions import Self as _Self

# Type variable for the connection/pool type
ConnectionT = _TypeVar("ConnectionT")

# Global set of registered registry names (thread-safe)
_registry_names: set[str] = set()
_registry_names_lock = _threading.Lock()


class KeyedConnection(_Generic[ConnectionT]):
    """
    A connection/value with a stable key for memoization.

    The key should be stable across runs - it identifies the logical connection/resource.
    The underlying connection can be recreated with different parameters as long as
    the same key is used.

    Can be used as a context manager to automatically unregister on exit.

    Type Parameters:
        ConnectionT: The type of connection/value being managed.
    """

    __slots__ = ("_registry_name", "_key", "_value", "_registry", "_memo_key")

    _registry_name: str
    _key: str
    _value: ConnectionT
    _registry: ConnectionRegistry[ConnectionT] | None
    _memo_key: _core.Fingerprint

    def __init__(
        self,
        registry_name: str,
        key: str,
        value: ConnectionT,
        registry: ConnectionRegistry[ConnectionT] | None = None,
    ) -> None:
        self._registry_name = registry_name
        self._key = key
        self._value = value
        self._registry = registry
        self._memo_key = _core.fingerprint_simple_object(
            (self._registry_name, self._key)
        )

    @property
    def registry_name(self) -> str:
        """The name of the registry this connection belongs to."""
        return self._registry_name

    @property
    def key(self) -> str:
        """The stable key for this connection."""
        return self._key

    @property
    def value(self) -> ConnectionT:
        """The connection/value."""
        return self._value

    def __enter__(self) -> _Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: _Any,
    ) -> None:
        if self._registry is not None:
            self._registry.unregister(self._key)

    def __coco_memo_key__(self) -> _core.Fingerprint:
        return self._memo_key


class ConnectionRegistry(_Generic[ConnectionT]):
    """
    Thread-safe registry for managing keyed connections.

    This class provides registration, lookup, and cleanup for connection pools
    or clients that need to be shared across multiple components.

    Each registry must have a globally unique name (e.g., "cocoindex/localfs",
    "cocoindex/postgres") to ensure memoization keys don't collide across
    different registry types.

    Type Parameters:
        ConnectionT: The type of connection/pool being managed (e.g., asyncpg.Pool).
    """

    __slots__ = ("_name", "_registry", "_lock")

    _name: str
    _registry: dict[str, ConnectionT]
    _lock: _threading.Lock

    def __init__(self, name: str) -> None:
        """
        Create a new ConnectionRegistry.

        Args:
            name: A globally unique name for this registry (e.g., "cocoindex/localfs").

        Raises:
            ValueError: If a registry with the given name already exists.
        """
        with _registry_names_lock:
            if name in _registry_names:
                raise ValueError(
                    f"ConnectionRegistry with name '{name}' already exists. "
                    f"Each registry must have a globally unique name."
                )
            _registry_names.add(name)
        self._name = name
        self._registry = {}
        self._lock = _threading.Lock()

    @property
    def name(self) -> str:
        """The globally unique name of this registry."""
        return self._name

    def register(
        self, key: str, connection: ConnectionT
    ) -> KeyedConnection[ConnectionT]:
        """
        Register a connection with a stable key.

        Args:
            key: A stable identifier for this connection.
            connection: The connection/pool to register.

        Returns:
            A KeyedConnection wrapping the registered connection.

        Raises:
            ValueError: If a connection with the given key is already registered.
        """
        with self._lock:
            if key in self._registry:
                raise ValueError(
                    f"Connection with key '{key}' is already registered in '{self._name}'. "
                    f"Use a different key or unregister the existing one first."
                )
            self._registry[key] = connection
        return KeyedConnection(self._name, key, connection, self)

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
