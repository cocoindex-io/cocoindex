"""
Facilities for defining cocoindex operations.
"""
import dataclasses
import inspect

from typing import get_type_hints, Protocol, Any, Callable, dataclass_transform
from enum import Enum
from threading import Lock

from .typing import encode_enriched_type, analyze_type_info, COLLECTION_TYPES
from .convert import to_engine_value
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
            cls = dataclasses.dataclass(cls)
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
        return (encode_enriched_type(result_type), executor)

def _make_engine_struct_value_converter(
        field_path: list[str],
        src_fields: list[dict[str, Any]],
        dst_dataclass_type: type,
    ) -> Callable[[list], Any]:
    """Make a converter from an engine field values to a Python value."""

    src_name_to_idx = {f['name']: i for i, f in enumerate(src_fields)}
    def make_closure_for_value(name: str, param: inspect.Parameter) -> Callable[[list], Any]:
        src_idx = src_name_to_idx.get(name)
        if src_idx is not None:
            field_path.append(f'.{name}')
            field_converter = _make_engine_value_converter(
                field_path, src_fields[src_idx]['type'], param.annotation)
            field_path.pop()
            return lambda values: field_converter(values[src_idx])

        default_value = param.default
        if default_value is inspect.Parameter.empty:
            raise ValueError(
                f"Field without default value is missing in input: {''.join(field_path)}")

        return lambda _: default_value

    field_value_converters = [
        make_closure_for_value(name, param)
        for (name, param) in inspect.signature(dst_dataclass_type).parameters.items()]

    return lambda values: dst_dataclass_type(
        *(converter(values) for converter in field_value_converters))

def _make_engine_value_converter(
        field_path: list[str],
        src_type: dict[str, Any],
        dst_annotation,
    ) -> Callable[[Any], Any]:
    """Make a converter from an engine value to a Python value."""

    src_type_kind = src_type['kind']

    if dst_annotation is inspect.Parameter.empty:
        if src_type_kind == 'Struct' or src_type_kind in COLLECTION_TYPES:
            raise ValueError(f"Missing type annotation for `{''.join(field_path)}`."
                             f"It's required for {src_type_kind} type.")
        return lambda value: value

    dst_type_info = analyze_type_info(dst_annotation)

    if src_type_kind != dst_type_info.kind:
        raise ValueError(
            f"Type mismatch for `{''.join(field_path)}`: "
            f"passed in {src_type_kind}, declared {dst_annotation} ({dst_type_info.kind})")

    if dst_type_info.dataclass_type is not None:
        return _make_engine_struct_value_converter(
            field_path, src_type['fields'], dst_type_info.dataclass_type)

    if src_type_kind in COLLECTION_TYPES:
        field_path.append('[*]')
        elem_type_info = analyze_type_info(dst_type_info.elem_type)
        if elem_type_info.dataclass_type is None:
            raise ValueError(f"Type mismatch for `{''.join(field_path)}`: "
                             f"declared `{dst_type_info.kind}`, a dataclass type expected")
        elem_converter = _make_engine_struct_value_converter(
            field_path, src_type['row']['fields'], elem_type_info.dataclass_type)
        field_path.pop()
        return lambda value: [elem_converter(v) for v in value] if value is not None else None

    return lambda value: value

_gpu_dispatch_lock = Lock()

@dataclasses.dataclass
class OpArgs:
    """
    - gpu: Whether the executor will be executed on GPU.
    - cache: Whether the executor will be cached.
    - behavior_version: The behavior version of the executor. Cache will be invalidated if it
      changes. Must be provided if `cache` is True.
    """
    gpu: bool = False
    cache: bool = False
    behavior_version: int | None = None

def _register_op_factory(
        category: OpCategory,
        expected_args: list[tuple[str, inspect.Parameter]],
        expected_return,
        executor_cls: type,
        spec_cls: type,
        op_args: OpArgs,
    ):
    """
    Register an op factory.
    """
    class _Fallback:
        def enable_cache(self):
            return op_args.cache

        def behavior_version(self):
            return op_args.behavior_version

    class _WrappedClass(executor_cls, _Fallback):
        _args_converters: list[Callable[[Any], Any]]
        _kwargs_converters: dict[str, Callable[[str, Any], Any]]

        def __init__(self, spec):
            super().__init__()
            self.spec = spec

        def analyze(self, *args, **kwargs):
            """
            Analyze the spec and arguments. In this phase, argument types should be validated.
            It should return the expected result type for the current op.
            """
            self._args_converters = []
            self._kwargs_converters = {}

            # Match arguments with parameters.
            next_param_idx = 0
            for arg in  args:
                if next_param_idx >= len(expected_args):
                    raise ValueError(
                        f"Too many arguments passed in: {len(args)} > {len(expected_args)}")
                arg_name, arg_param = expected_args[next_param_idx]
                if arg_param.kind in (
                    inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.VAR_KEYWORD):
                    raise ValueError(
                        f"Too many positional arguments passed in: {len(args)} > {next_param_idx}")
                self._args_converters.append(
                    _make_engine_value_converter(
                        [arg_name], arg.value_type['type'], arg_param.annotation))
                if arg_param.kind != inspect.Parameter.VAR_POSITIONAL:
                    next_param_idx += 1

            expected_kwargs = expected_args[next_param_idx:]

            for kwarg_name, kwarg in kwargs.items():
                expected_arg = next(
                    (arg for arg in expected_kwargs
                      if (arg[0] == kwarg_name and arg[1].kind in (
                          inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD))
                        or arg[1].kind == inspect.Parameter.VAR_KEYWORD),
                    None)
                if expected_arg is None:
                    raise ValueError(f"Unexpected keyword argument passed in: {kwarg_name}")
                arg_param = expected_arg[1]
                self._kwargs_converters[kwarg_name] = _make_engine_value_converter(
                    [kwarg_name], kwarg.value_type['type'], arg_param.annotation)

            missing_args = [name for (name, arg) in expected_kwargs
                            if arg.default is inspect.Parameter.empty
                               and (arg.kind == inspect.Parameter.POSITIONAL_ONLY or
                                    (arg.kind in (inspect.Parameter.KEYWORD_ONLY,
                                                  inspect.Parameter.POSITIONAL_OR_KEYWORD)
                                    and name not in kwargs))]
            if len(missing_args) > 0:
                raise ValueError(f"Missing arguments: {', '.join(missing_args)}")

            prepare_method = getattr(executor_cls, 'analyze', None)
            if prepare_method is not None:
                return prepare_method(self, *args, **kwargs)
            else:
                return expected_return

        def prepare(self):
            """
            Prepare for execution.
            It's executed after `analyze` and before any `__call__` execution.
            """
            setup_method = getattr(executor_cls, 'prepare', None)
            if setup_method is not None:
                setup_method(self)

        def __call__(self, *args, **kwargs):
            converted_args = (converter(arg) for converter, arg in zip(self._args_converters, args))
            converted_kwargs = {arg_name: self._kwargs_converters[arg_name](arg)
                                for arg_name, arg in kwargs.items()}
            if op_args.gpu:
                # For GPU executions, data-level parallelism is applied, so we don't want to
                # execute different tasks in parallel.
                # Besides, multiprocessing is more appropriate for pytorch.
                # For now, we use a lock to ensure only one task is executed at a time.
                # TODO: Implement multi-processing dispatching.
                with _gpu_dispatch_lock:
                    output = super().__call__(*converted_args, **converted_kwargs)
            else:
                output = super().__call__(*converted_args, **converted_kwargs)
            return to_engine_value(output)

    _WrappedClass.__name__ = executor_cls.__name__

    if category == OpCategory.FUNCTION:
        _engine.register_function_factory(
            spec_cls.__name__,
            _FunctionExecutorFactory(spec_cls, _WrappedClass))
    else:
        raise ValueError(f"Unsupported executor type {category}")

    return _WrappedClass

def executor_class(**args) -> Callable[[type], type]:
    """
    Decorate a class to provide an executor for an op.
    """
    op_args = OpArgs(**args)

    def _inner(cls: type[Executor]) -> type:
        """
        Decorate a class to provide an executor for an op.
        """
        type_hints = get_type_hints(cls)
        if 'spec' not in type_hints:
            raise TypeError("Expect a `spec` field with type hint")
        spec_cls = type_hints['spec']
        sig = inspect.signature(cls.__call__)
        return _register_op_factory(
            category=spec_cls._op_category,
            expected_args=list(sig.parameters.items())[1:],  # First argument is `self`
            expected_return=sig.return_annotation,
            executor_cls=cls,
            spec_cls=spec_cls,
            op_args=op_args)

    return _inner

def function(**args) -> Callable[[Callable], FunctionSpec]:
    """
    Decorate a function to provide a function for an op.
    """
    op_args = OpArgs(**args)

    def _inner(fn: Callable) -> FunctionSpec:

        # Convert snake case to camel case.
        op_name = ''.join(word.capitalize() for word in fn.__name__.split('_'))
        sig = inspect.signature(fn)

        class _Executor:
            def __call__(self, *args, **kwargs):
                return fn(*args, **kwargs)

        class _Spec(FunctionSpec):
            pass
        _Spec.__name__ = op_name

        _register_op_factory(
            category=OpCategory.FUNCTION,
            expected_args=list(sig.parameters.items()),
            expected_return=sig.return_annotation,
            executor_cls=_Executor,
            spec_cls=_Spec,
            op_args=op_args)

        return _Spec()

    return _inner
