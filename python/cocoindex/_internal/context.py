from contextvars import ContextVar

from . import core

component_ctx_var: ContextVar[core.ComponentBuilderContext] = ContextVar(
    "component_ctx"
)
