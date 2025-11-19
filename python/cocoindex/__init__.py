"""
Cocoindex is a framework for building and running indexing pipelines.
"""

from .lib import settings, init
from .setting import Settings

__all__ = [
    "settings",
    "init",
    "Settings",
]
