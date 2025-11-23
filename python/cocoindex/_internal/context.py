from contextvars import ContextVar

from .core import ComponentBuilderContext  # type: ignore

component_ctx_var: ContextVar[ComponentBuilderContext] = ContextVar("component_ctx")
