"""
Facilities for defining cocoindex operations.
"""

import asyncio
import dataclasses
import inspect

from typing import Protocol, Any, Callable, Awaitable, dataclass_transform
from enum import Enum

from .typing import encode_enriched_type, resolve_forward_ref
from .convert import encode_engine_value, make_engine_value_decoder
from . import _engine  # type: ignore


class OpCategory(Enum):
    """The category of the operation."""

    FUNCTION = "function"
    SOURCE = "source"
    TARGET = "target"
    DECLARATION = "declaration"


@dataclass_transform()
class SpecMeta(type):
    """Meta class for spec classes."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        attrs: dict[str, Any],
        category: OpCategory | None = None,
    ) -> type:
        cls: type = super().__new__(mcs, name, bases, attrs)
        if category is not None:
            # It's the base class.
            setattr(cls, "_op_category", category)
        else:
            # It's the specific class providing specific fields.
            cls = dataclasses.dataclass(cls)
        return cls


class SourceSpec(metaclass=SpecMeta, category=OpCategory.SOURCE):  # pylint: disable=too-few-public-methods
    """A source spec. All its subclass can be instantiated similar to a dataclass, i.e. ClassName(field1=value1, field2=value2, ...)"""


class FunctionSpec(metaclass=SpecMeta, category=OpCategory.FUNCTION):  # pylint: disable=too-few-public-methods
    """A function spec. All its subclass can be instantiated similar to a dataclass, i.e. ClassName(field1=value1, field2=value2, ...)"""


class TargetSpec(metaclass=SpecMeta, category=OpCategory.TARGET):  # pylint: disable=too-few-public-methods
    """A target spec. All its subclass can be instantiated similar to a dataclass, i.e. ClassName(field1=value1, field2=value2, ...)"""


class DeclarationSpec(metaclass=SpecMeta, category=OpCategory.DECLARATION):  # pylint: disable=too-few-public-methods
    """A declaration spec. All its subclass can be instantiated similar to a dataclass, i.e. ClassName(field1=value1, field2=value2, ...)"""


class Executor(Protocol):
    """An executor for an operation."""

    op_category: OpCategory


class _FunctionExecutorFactory:
    _spec_cls: type
    _executor_cls: type

    def __init__(self, spec_cls: type, executor_cls: type):
        self._spec_cls = spec_cls
        self._executor_cls = executor_cls

    def __call__(
        self, spec: dict[str, Any], *args: Any, **kwargs: Any
    ) -> tuple[dict[str, Any], Executor]:
        spec = self._spec_cls(**spec)
        executor = self._executor_cls(spec)
        result_type = executor.analyze(*args, **kwargs)
        return (encode_enriched_type(result_type), executor)


_gpu_dispatch_lock = asyncio.Lock()


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


def _to_async_call(call: Callable[..., Any]) -> Callable[..., Awaitable[Any]]:
    if inspect.iscoroutinefunction(call):
        return call
    return lambda *args, **kwargs: asyncio.to_thread(lambda: call(*args, **kwargs))


def _register_op_factory(
    category: OpCategory,
    expected_args: list[tuple[str, inspect.Parameter]],
    expected_return: Any,
    executor_cls: type,
    spec_cls: type,
    op_args: OpArgs,
) -> type:
    """
    Register an op factory.
    """

    class _Fallback:
        def enable_cache(self) -> bool:
            return op_args.cache

        def behavior_version(self) -> int | None:
            return op_args.behavior_version

    class _WrappedClass(executor_cls, _Fallback):  # type: ignore[misc]
        _args_decoders: list[Callable[[Any], Any]]
        _kwargs_decoders: dict[str, Callable[[Any], Any]]
        _acall: Callable[..., Awaitable[Any]]

        def __init__(self, spec: Any) -> None:
            super().__init__()
            self.spec = spec
            self._acall = _to_async_call(super().__call__)

        def analyze(
            self, *args: _engine.OpArgSchema, **kwargs: _engine.OpArgSchema
        ) -> Any:
            """
            Analyze the spec and arguments. In this phase, argument types should be validated.
            It should return the expected result type for the current op.
            """
            self._args_decoders = []
            self._kwargs_decoders = {}

            # Match arguments with parameters.
            next_param_idx = 0
            for arg in args:
                if next_param_idx >= len(expected_args):
                    raise ValueError(
                        f"Too many arguments passed in: {len(args)} > {len(expected_args)}"
                    )
                arg_name, arg_param = expected_args[next_param_idx]
                if arg_param.kind in (
                    inspect.Parameter.KEYWORD_ONLY,
                    inspect.Parameter.VAR_KEYWORD,
                ):
                    raise ValueError(
                        f"Too many positional arguments passed in: {len(args)} > {next_param_idx}"
                    )
                self._args_decoders.append(
                    make_engine_value_decoder(
                        [arg_name], arg.value_type["type"], arg_param.annotation
                    )
                )
                if arg_param.kind != inspect.Parameter.VAR_POSITIONAL:
                    next_param_idx += 1

            expected_kwargs = expected_args[next_param_idx:]

            for kwarg_name, kwarg in kwargs.items():
                expected_arg = next(
                    (
                        arg
                        for arg in expected_kwargs
                        if (
                            arg[0] == kwarg_name
                            and arg[1].kind
                            in (
                                inspect.Parameter.KEYWORD_ONLY,
                                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                            )
                        )
                        or arg[1].kind == inspect.Parameter.VAR_KEYWORD
                    ),
                    None,
                )
                if expected_arg is None:
                    raise ValueError(
                        f"Unexpected keyword argument passed in: {kwarg_name}"
                    )
                arg_param = expected_arg[1]
                self._kwargs_decoders[kwarg_name] = make_engine_value_decoder(
                    [kwarg_name], kwarg.value_type["type"], arg_param.annotation
                )

            missing_args = [
                name
                for (name, arg) in expected_kwargs
                if arg.default is inspect.Parameter.empty
                and (
                    arg.kind == inspect.Parameter.POSITIONAL_ONLY
                    or (
                        arg.kind
                        in (
                            inspect.Parameter.KEYWORD_ONLY,
                            inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        )
                        and name not in kwargs
                    )
                )
            ]
            if len(missing_args) > 0:
                raise ValueError(f"Missing arguments: {', '.join(missing_args)}")

            prepare_method = getattr(executor_cls, "analyze", None)
            if prepare_method is not None:
                return prepare_method(self, *args, **kwargs)
            else:
                return expected_return

        async def prepare(self) -> None:
            """
            Prepare for execution.
            It's executed after `analyze` and before any `__call__` execution.
            """
            setup_method = getattr(super(), "prepare", None)
            if setup_method is not None:
                await _to_async_call(setup_method)()

        async def __call__(self, *args: Any, **kwargs: Any) -> Any:
            decoded_args = (
                decoder(arg) for decoder, arg in zip(self._args_decoders, args)
            )
            decoded_kwargs = {
                arg_name: self._kwargs_decoders[arg_name](arg)
                for arg_name, arg in kwargs.items()
            }

            if op_args.gpu:
                # For GPU executions, data-level parallelism is applied, so we don't want to
                # execute different tasks in parallel.
                # Besides, multiprocessing is more appropriate for pytorch.
                # For now, we use a lock to ensure only one task is executed at a time.
                # TODO: Implement multi-processing dispatching.
                async with _gpu_dispatch_lock:
                    output = await self._acall(*decoded_args, **decoded_kwargs)
            else:
                output = await self._acall(*decoded_args, **decoded_kwargs)
            return encode_engine_value(output)

    _WrappedClass.__name__ = executor_cls.__name__
    _WrappedClass.__doc__ = executor_cls.__doc__
    _WrappedClass.__module__ = executor_cls.__module__
    _WrappedClass.__qualname__ = executor_cls.__qualname__
    _WrappedClass.__wrapped__ = executor_cls

    if category == OpCategory.FUNCTION:
        _engine.register_function_factory(
            spec_cls.__name__, _FunctionExecutorFactory(spec_cls, _WrappedClass)
        )
    else:
        raise ValueError(f"Unsupported executor type {category}")

    return _WrappedClass


def executor_class(**args: Any) -> Callable[[type], type]:
    """
    Decorate a class to provide an executor for an op.
    """
    op_args = OpArgs(**args)

    def _inner(cls: type[Executor]) -> type:
        """
        Decorate a class to provide an executor for an op.
        """
        # Use `__annotations__` instead of `get_type_hints`, to avoid resolving forward references.
        type_hints = cls.__annotations__
        if "spec" not in type_hints:
            raise TypeError("Expect a `spec` field with type hint")
        spec_cls = resolve_forward_ref(type_hints["spec"])
        sig = inspect.signature(cls.__call__)
        return _register_op_factory(
            category=spec_cls._op_category,
            expected_args=list(sig.parameters.items())[1:],  # First argument is `self`
            expected_return=sig.return_annotation,
            executor_cls=cls,
            spec_cls=spec_cls,
            op_args=op_args,
        )

    return _inner


def function(**args: Any) -> Callable[[Callable[..., Any]], FunctionSpec]:
    """
    Decorate a function to provide a function for an op.
    """
    op_args = OpArgs(**args)

    def _inner(fn: Callable[..., Any]) -> FunctionSpec:
        # Convert snake case to camel case.
        op_name = "".join(word.capitalize() for word in fn.__name__.split("_"))
        sig = inspect.signature(fn)

        class _Executor:
            def __call__(self, *args: Any, **kwargs: Any) -> Any:
                return fn(*args, **kwargs)

        class _Spec(FunctionSpec):
            def __call__(self, *args: Any, **kwargs: Any) -> Any:
                return fn(*args, **kwargs)

        _Spec.__name__ = op_name
        _Spec.__doc__ = fn.__doc__
        _Spec.__module__ = fn.__module__
        _Spec.__qualname__ = fn.__qualname__

        _register_op_factory(
            category=OpCategory.FUNCTION,
            expected_args=list(sig.parameters.items()),
            expected_return=sig.return_annotation,
            executor_cls=_Executor,
            spec_cls=_Spec,
            op_args=op_args,
        )

        return _Spec()

    return _inner
