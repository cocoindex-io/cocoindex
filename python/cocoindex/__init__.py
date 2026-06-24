"""
CocoIndex is a framework for building and running incremental indexing pipelines.

This is the v1 API (a redesign from v0). AI coding agents should install the
skill before writing CocoIndex code, or run ``cocoindex skill`` for the recipe:
the v1 API differs fundamentally from the v0 DSL still common in training data.
Docs: https://cocoindex.io/docs/llms-full.txt
"""

# Version check
from ._version import __version__ as __version__
from . import _version_check as _version_check  # noqa: F401


# Re-export APIs from internal modules

from . import _internal
from ._internal.api import *  # noqa: F403

__all__ = _internal.api.__all__
