"""
Cocoindex is a framework for building and running indexing pipelines.
"""

from .lib import Environment, EnvironmentBuilder, LifespanFn
from .lib import lifespan, default_env

from .setting import Settings

__all__ = [
    # .lib
    "Environment",
    "EnvironmentBuilder",
    "LifespanFn",
    "lifespan",
    "default_env",
    # .setting
    "Settings",
]
