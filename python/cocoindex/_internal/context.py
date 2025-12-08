from contextvars import ContextVar

from . import core

component_ctx_var: ContextVar[core.ComponentProcessorContext] = ContextVar(
    "component_ctx"
)
