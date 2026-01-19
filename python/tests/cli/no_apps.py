"""Test module with NO apps defined - tests empty module handling."""

from __future__ import annotations

# This module intentionally has no apps defined.
# Used to test: "No apps are defined in '<module>'" error message.

some_variable = "hello"


def some_function() -> str:
    return "world"
