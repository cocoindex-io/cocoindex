"""All builtin sources."""
from . import op

class LocalFile(op.SourceSpec):
    """Import data from local file system."""

    _op_category = op.OpCategory.SOURCE

    path: str
    binary: bool = False

    # If provided, only files matching these patterns will be included.
    # See https://docs.rs/globset/latest/globset/index.html#syntax for the syntax of the patterns.
    included_patterns: list[str] | None = None

    # If provided, files matching these patterns will be excluded.
    # See https://docs.rs/globset/latest/globset/index.html#syntax for the syntax of the patterns.
    excluded_patterns: list[str] | None = None
