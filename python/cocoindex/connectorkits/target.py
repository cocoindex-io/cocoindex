"""Shared types for target connectors."""

from __future__ import annotations

__all__ = ["ManagedBy"]

import enum as _enum


class ManagedBy(_enum.StrEnum):
    """Who manages the lifecycle of the target resource."""

    SYSTEM = "system"
    USER = "user"
