"""
Cocoindex is a framework for building and running indexing pipelines.
"""

# Version check
from ._version import __version__ as __version__
from . import _version_check as _version_check  # noqa: F401


# Re-export APIs from internal modules

from . import _internal
from ._internal.api import *  # noqa: F403
from ._internal.api_sync import *  # noqa: F403

__all__ = _internal.api.__all__ + _internal.api_sync.__all__
