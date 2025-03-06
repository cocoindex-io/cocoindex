"""
Facilities for defining cocoindex operations.
"""
import inspect

from typing import get_type_hints, Protocol, Any, Callable, dataclass_transform
from dataclasses import dataclass
from enum import Enum
from threading import Lock

from .typing import dump_type
from . import _engine


class OpCategory(Enum):
    """The category of the operation."""
    FUNCTION = "function"
    SOURCE = "source"
    STORAGE = "storage"

@dataclass_transform()
class SpecMeta(type):
    """Meta class for spec classes."""
    def __new__(mcs, name, bases, attrs, category: OpCategory | None = None):
        cls: type = super().__new__(mcs, name, bases, attrs)
        if category is not None:
            # It's the base class.
            setattr(cls, '_op_category', category)
        else:
            # It's the specific class providing specific fields.
            cls = dataclass(cls)
        return cls

class SourceSpec(metaclass=SpecMeta, category=OpCategory.SOURCE): # pylint: disable=too-few-public-methods
    """A source spec. All its subclass can be instantiated similar to a dataclass, i.e. ClassName(field1=value1, field2=value2, ...)"""

class FunctionSpec(metaclass=SpecMeta, category=OpCategory.FUNCTION): # pylint: disable=too-few-public-methods
    """A function spec. All its subclass can be instantiated similar to a dataclass, i.e. ClassName(field1=value1, field2=value2, ...)"""

class StorageSpec(metaclass=SpecMeta, category=OpCategory.STORAGE): # pylint: disable=too-few-public-methods
    """A storage spec. All its subclass can be instantiated similar to a dataclass, i.e. ClassName(field1=value1, field2=value2, ...)"""

class Executor(Protocol):
    """An executor for an operation."""
    op_category: OpCategory


class _FunctionExecutorFactory:
    _spec_cls: type
    _executor_cls: type

    def __init__(self, spec_cls: type, executor_cls: type):
        self._spec_cls = spec_cls
        self._executor_cls = executor_cls

    def __call__(self, spec: dict[str, Any], *args, **kwargs):
        spec = self._spec_cls(**spec)
        executor = self._executor_cls(spec)
        result_type = executor.analyze(*args, **kwargs)
        return (dump_type(result_type), executor)

_gpu_dispatch_lock = Lock()

def executor_class(gpu: bool = False, cache: bool = False, behavior_version: int | None = None) -> Callable[[type], type]:
    """
    Decorate a class to provide an executor for an op.

    Args:
        gpu: Whether the executor will be executed on GPU.
        cache: Whether the executor will be cached.
        behavior_version: The behavior version of the executor. Cache will be invalidated if it changes. Must be provided if `cache` is True.
    """

    def _inner(cls: type[Executor]) -> type:
        """
        Decorate a class to provide an executor for an op.
        """
        type_hints = get_type_hints(cls)
        if 'spec' not in type_hints:
            raise TypeError("Expect a `spec` field with type hint")

        spec_cls = type_hints['spec']
        op_name = spec_cls.__name__
        category = spec_cls._op_category

        sig = inspect.signature(cls.__call__)
        expected_args = list(sig.parameters.items())[1:]  # First argument is `self`
        expected_return = sig.return_annotation

        cls_type: type = cls

        class _Fallback:
            def enable_cache(self):
                return cache

            def behavior_version(self):
                return behavior_version

        class _WrappedClass(cls_type, _Fallback):
            def __init__(self, spec):
                super().__init__()
                self.spec = spec

            def analyze(self, *args, **kwargs):
                """
                Analyze the spec and arguments. In this phase, argument types should be validated.
                It should return the expected result type for the current op.
                """
                # Match arguments with parameters.
                next_param_idx = 0
                for arg in args:
                    if next_param_idx >= len(expected_args):
                        raise ValueError(f"Too many arguments: {len(args)} > {len(expected_args)}")
                    arg_name, arg_param = expected_args[next_param_idx]
                    if arg_param.kind == inspect.Parameter.KEYWORD_ONLY or arg_param.kind == inspect.Parameter.VAR_KEYWORD:
                        raise ValueError(f"Too many positional arguments: {len(args)} > {next_param_idx}")
                    if arg_param.annotation is not inspect.Parameter.empty:
                        arg.validate_arg(arg_name, dump_type(arg_param.annotation))
                    if arg_param.kind != inspect.Parameter.VAR_POSITIONAL:
                        next_param_idx += 1

                expected_kwargs = expected_args[next_param_idx:]

                for kwarg_name, kwarg in kwargs.items():
                    expected_arg = next(
                        (arg for arg in expected_kwargs
                          if (arg[0] == kwarg_name and arg[1].kind in (inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD))
                            or arg[1].kind == inspect.Parameter.VAR_KEYWORD),
                        None)
                    if expected_arg is None:
                        raise ValueError(f"Unexpected keyword argument: {kwarg_name}")
                    arg_param = expected_arg[1]
                    if arg_param.annotation is not inspect.Parameter.empty:
                        kwarg.validate_arg(kwarg_name, dump_type(arg_param.annotation))

                missing_args = [name for (name, arg) in expected_kwargs
                                if arg.default is inspect.Parameter.empty
                                   and (arg.kind == inspect.Parameter.POSITIONAL_ONLY or
                                        (arg.kind in (inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD) and name not in kwargs))]
                if len(missing_args) > 0:
                    raise ValueError(f"Missing arguments: {', '.join(missing_args)}")

                prepare_method = getattr(cls_type, 'analyze', None)
                if prepare_method is not None:
                    return prepare_method(self, *args, **kwargs)
                else:
                    return expected_return

            def prepare(self):
                """
                Prepare for execution.
                It's executed after `analyze` and before any `__call__` execution.
                """
                setup_method = getattr(cls_type, 'prepare', None)
                if setup_method is not None:
                    setup_method(self)

            def __call__(self, *args, **kwargs):
                if gpu:
                    # For GPU executions, data-level parallelism is applied, so we don't want to execute different tasks in parallel.
                    # Besides, multiprocessing is more appropriate for pytorch.
                    # For now, we use a lock to ensure only one task is executed at a time.
                    # TODO: Implement multi-processing dispatching.
                    with _gpu_dispatch_lock:
                        return super().__call__(*args, **kwargs)
                else:
                    return super().__call__(*args, **kwargs)

        _WrappedClass.__name__ = cls.__name__

        if category == OpCategory.FUNCTION:
            _engine.register_function_factory(op_name, _FunctionExecutorFactory(spec_cls, _WrappedClass))
        else:
            raise ValueError(f"Unsupported executor type {category}")

        return _WrappedClass

    return _inner
