"""
Cocoindex is a framework for building and running indexing pipelines.
"""

from ._version import __version__

from . import _version_check

from ._internal.app import App, AppConfig

from ._internal.effect import (
    NonExistenceType,
    NON_EXISTENCE,
    Effect,
    EffectProvider,
    EffectReconcileOutput,
    EffectReconciler,
    EffectSink,
    declare_effect,
    declare_effect_with_child,
    is_non_existence,
    register_root_effect_provider,
)

from ._internal.environment import Environment, EnvironmentBuilder, LifespanFn
from ._internal.environment import lifespan, default_env

from ._internal.function import function

from ._internal.state import StatePath, StateKey

from ._internal.setting import Settings

__all__ = [
    "__version__",
    # .app
    "App",
    "AppConfig",
    # .effect
    "NonExistenceType",
    "NON_EXISTENCE",
    "Effect",
    "EffectProvider",
    "EffectReconcileOutput",
    "EffectReconciler",
    "EffectSink",
    "declare_effect",
    "declare_effect_with_child",
    "is_non_existence",
    "register_root_effect_provider",
    # .environment
    "Environment",
    "EnvironmentBuilder",
    "LifespanFn",
    "lifespan",
    "default_env",
    # .fn
    "function",
    # .state
    "StatePath",
    "StateKey",
    # .setting
    "Settings",
]
