"""
Naming validation for CocoIndex identifiers.

This module enforces naming conventions for flow names, field names,
target names, and app namespace names as specified in issue #779.
"""

import re
from typing import Optional

_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_IDENTIFIER_WITH_DOTS_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")


class NamingError(ValueError):
    """Exception raised for naming convention violations."""

    pass


def validate_identifier_name(
    name: str,
    max_length: int = 64,
    allow_dots: bool = False,
    identifier_type: str = "identifier",
) -> Optional[str]:
    """
    Validate identifier names according to CocoIndex naming rules.

    Args:
        name: The name to validate
        max_length: Maximum allowed length (default 64)
        allow_dots: Whether to allow dots in the name (for full flow names)
        identifier_type: Type of identifier for error messages

    Returns:
        None if valid, error message string if invalid
    """
    if not name:
        return f"{identifier_type} name cannot be empty"

    if len(name) > max_length:
        return f"{identifier_type} name '{name}' exceeds maximum length of {max_length} characters"

    if name.startswith("__"):
        return f"{identifier_type} name '{name}' cannot start with double underscores (reserved for internal usage)"

    # Define allowed pattern
    if allow_dots:
        pattern = _IDENTIFIER_WITH_DOTS_PATTERN
        allowed_chars = "letters, digits, underscores, and dots"
    else:
        pattern = _IDENTIFIER_PATTERN
        allowed_chars = "letters, digits, and underscores"

    if not pattern.match(name):
        return f"{identifier_type} name '{name}' must start with a letter or underscore and contain only {allowed_chars}"

    return None


def validate_field_name(name: str) -> None:
    """Validate field names."""
    error = validate_identifier_name(
        name, max_length=64, allow_dots=False, identifier_type="Field"
    )
    if error:
        raise NamingError(error)


def validate_flow_name(name: str) -> None:
    """Validate flow names."""
    error = validate_identifier_name(
        name, max_length=64, allow_dots=False, identifier_type="Flow"
    )
    if error:
        raise NamingError(error)


def validate_full_flow_name(name: str) -> None:
    """Validate full flow names (can contain dots for namespacing)."""
    error = validate_identifier_name(
        name, max_length=64, allow_dots=True, identifier_type="Full flow"
    )
    if error:
        raise NamingError(error)


def validate_app_namespace_name(name: str) -> None:
    """Validate app namespace names."""
    error = validate_identifier_name(
        name, max_length=64, allow_dots=False, identifier_type="App namespace"
    )
    if error:
        raise NamingError(error)


def validate_target_name(name: str) -> None:
    """Validate target names."""
    error = validate_identifier_name(
        name, max_length=64, allow_dots=False, identifier_type="Target"
    )
    if error:
        raise NamingError(error)
