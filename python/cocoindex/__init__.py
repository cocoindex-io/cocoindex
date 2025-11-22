"""
Cocoindex is a framework for building and running indexing pipelines.
"""

from ._internal.lib import Environment, EnvironmentBuilder, LifespanFn
from ._internal.lib import lifespan, default_env

from ._internal.state import StatePath, StatePathPart

from ._internal.setting import Settings

__all__ = [
    # .lib
    "Environment",
    "EnvironmentBuilder",
    "LifespanFn",
    "lifespan",
    "default_env",
    # .setting
    "Settings",
    # .state
    "StatePath",
    "StatePathPart",
]
