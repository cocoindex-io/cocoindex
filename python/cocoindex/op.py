"""
Facilities for defining cocoindex operations.
"""

import asyncio
import dataclasses
import inspect
from enum import Enum
from typing import (
    Any,
    Awaitable,
    Callable,
    Protocol,
    dataclass_transform,
    Annotated,
    get_args,
)

from . import _engine  # type: ignore
from .subprocess_exec import executor_stub
from .convert import (
    make_engine_value_encoder,
    make_engine_value_decoder,
    make_engine_key_decoder,
    make_engine_struct_decoder,
)
from .typing import (
    TypeAttr,
    encode_enriched_type_info,
    resolve_forward_ref,
    analyze_type_info,
    AnalyzedAnyType,
    AnalyzedDictType,
)


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


def _load_spec_from_engine(
    spec_loader: Callable[..., Any], spec: dict[str, Any]
) -> Any:
    """
    Load a spec from the engine.
    """
    return spec_loader(**spec)


def _get_required_method(cls: type, name: str) -> Callable[..., Any]:
    method = getattr(cls, name, None)
    if method is None:
        raise ValueError(f"Method {name}() is required for {cls.__name__}")
    if not inspect.isfunction(method):
        raise ValueError(f"Method {cls.__name__}.{name}() is not a function")
    return method


class _EngineFunctionExecutorFactory:
    _spec_loader: Callable[..., Any]
    _executor_cls: type

    def __init__(self, spec_loader: Callable[..., Any], executor_cls: type):
        self._spec_loader = spec_loader
        self._executor_cls = executor_cls

    def __call__(
        self, spec: dict[str, Any], *args: Any, **kwargs: Any
    ) -> tuple[dict[str, Any], Executor]:
        spec = _load_spec_from_engine(self._spec_loader, spec)
        executor = self._executor_cls(spec)
        result_type = executor.analyze_schema(*args, **kwargs)
        return (result_type, executor)


_COCOINDEX_ATTR_PREFIX = "cocoindex.io/"


class ArgRelationship(Enum):
    """Specifies the relationship between an input argument and the output."""

    EMBEDDING_ORIGIN_TEXT = _COCOINDEX_ATTR_PREFIX + "embedding_origin_text"
    CHUNKS_BASE_TEXT = _COCOINDEX_ATTR_PREFIX + "chunk_base_text"
    RECTS_BASE_IMAGE = _COCOINDEX_ATTR_PREFIX + "rects_base_image"


@dataclasses.dataclass
class OpArgs:
    """
    - gpu: Whether the executor will be executed on GPU.
    - cache: Whether the executor will be cached.
    - behavior_version: The behavior version of the executor. Cache will be invalidated if it
      changes. Must be provided if `cache` is True.
    - arg_relationship: It specifies the relationship between an input argument and the output,
      e.g. `(ArgRelationship.CHUNKS_BASE_TEXT, "content")` means the output is chunks for the
      input argument with name `content`.
    """

    gpu: bool = False
    cache: bool = False
    behavior_version: int | None = None
    arg_relationship: tuple[ArgRelationship, str] | None = None


def _to_async_call(call: Callable[..., Any]) -> Callable[..., Awaitable[Any]]:
    if inspect.iscoroutinefunction(call):
        return call
    return lambda *args, **kwargs: asyncio.to_thread(lambda: call(*args, **kwargs))


@dataclasses.dataclass
class _ArgInfo:
    decoder: Callable[[Any], Any]
    is_required: bool


def _register_op_factory(
    category: OpCategory,
    expected_args: list[tuple[str, inspect.Parameter]],
    expected_return: Any,
    executor_factory: Any,
    spec_loader: Callable[..., Any],
    op_kind: str,
    op_args: OpArgs,
) -> None:
    """
    Register an op factory.
    """

    class _WrappedExecutor:
        _executor: Any
        _args_info: list[_ArgInfo]
        _kwargs_info: dict[str, _ArgInfo]
        _result_encoder: Callable[[Any], Any]
        _acall: Callable[..., Awaitable[Any]] | None = None

        def __init__(self, spec: Any) -> None:
            executor: Any

            if op_args.gpu:
                executor = executor_stub(executor_factory, spec)
            else:
                executor = executor_factory()
                executor.spec = spec

            self._executor = executor

        def analyze_schema(
            self, *args: _engine.OpArgSchema, **kwargs: _engine.OpArgSchema
        ) -> Any:
            """
            Analyze the spec and arguments. In this phase, argument types should be validated.
            It should return the expected result type for the current op.
            """
            self._args_info = []
            self._kwargs_info = {}
            attributes = []
            potentially_missing_required_arg = False

            def process_arg(
                arg_name: str,
                arg_param: inspect.Parameter,
                actual_arg: _engine.OpArgSchema,
            ) -> _ArgInfo:
                nonlocal potentially_missing_required_arg
                if op_args.arg_relationship is not None:
                    related_attr, related_arg_name = op_args.arg_relationship
                    if related_arg_name == arg_name:
                        attributes.append(
                            TypeAttr(related_attr.value, actual_arg.analyzed_value)
                        )
                type_info = analyze_type_info(arg_param.annotation)
                decoder = make_engine_value_decoder(
                    [arg_name], actual_arg.value_type["type"], type_info
                )
                is_required = not type_info.nullable
                if is_required and actual_arg.value_type.get("nullable", False):
                    potentially_missing_required_arg = True
                return _ArgInfo(
                    decoder=decoder,
                    is_required=is_required,
                )

            # Match arguments with parameters.
            next_param_idx = 0
            for actual_arg in args:
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
                self._args_info.append(process_arg(arg_name, arg_param, actual_arg))
                if arg_param.kind != inspect.Parameter.VAR_POSITIONAL:
                    next_param_idx += 1

            expected_kwargs = expected_args[next_param_idx:]

            for kwarg_name, actual_arg in kwargs.items():
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
                self._kwargs_info[kwarg_name] = process_arg(
                    kwarg_name, arg_param, actual_arg
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

            base_analyze_method = getattr(self._executor, "analyze", None)
            if base_analyze_method is not None:
                result_type = base_analyze_method()
            else:
                result_type = expected_return
            if len(attributes) > 0:
                result_type = Annotated[result_type, *attributes]

            analyzed_result_type_info = analyze_type_info(result_type)
            encoded_type = encode_enriched_type_info(analyzed_result_type_info)
            if potentially_missing_required_arg:
                encoded_type["nullable"] = True

            self._result_encoder = make_engine_value_encoder(analyzed_result_type_info)

            return encoded_type

        async def prepare(self) -> None:
            """
            Prepare for execution.
            It's executed after `analyze` and before any `__call__` execution.
            """
            prepare_method = getattr(self._executor, "prepare", None)
            if prepare_method is not None:
                await _to_async_call(prepare_method)()
            self._acall = _to_async_call(self._executor.__call__)

        async def __call__(self, *args: Any, **kwargs: Any) -> Any:
            decoded_args = []
            for arg_info, arg in zip(self._args_info, args):
                if arg_info.is_required and arg is None:
                    return None
                decoded_args.append(arg_info.decoder(arg))

            decoded_kwargs = {}
            for kwarg_name, arg in kwargs.items():
                kwarg_info = self._kwargs_info.get(kwarg_name)
                if kwarg_info is None:
                    raise ValueError(
                        f"Unexpected keyword argument passed in: {kwarg_name}"
                    )
                if kwarg_info.is_required and arg is None:
                    return None
                decoded_kwargs[kwarg_name] = kwarg_info.decoder(arg)

            assert self._acall is not None
            output = await self._acall(*decoded_args, **decoded_kwargs)
            return self._result_encoder(output)

        def enable_cache(self) -> bool:
            return op_args.cache

        def behavior_version(self) -> int | None:
            return op_args.behavior_version

    if category == OpCategory.FUNCTION:
        _engine.register_function_factory(
            op_kind, _EngineFunctionExecutorFactory(spec_loader, _WrappedExecutor)
        )
    else:
        raise ValueError(f"Unsupported executor type {category}")


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
        _register_op_factory(
            category=spec_cls._op_category,
            expected_args=list(sig.parameters.items())[1:],  # First argument is `self`
            expected_return=sig.return_annotation,
            executor_factory=cls,
            spec_loader=spec_cls,
            op_kind=spec_cls.__name__,
            op_args=op_args,
        )
        return cls

    return _inner


class EmptyFunctionSpec(FunctionSpec):
    pass


class _SimpleFunctionExecutor:
    spec: Callable[..., Any]

    def prepare(self) -> None:
        self.__call__ = staticmethod(self.spec)


def function(**args: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorate a function to provide a function for an op.
    """
    op_args = OpArgs(**args)

    def _inner(fn: Callable[..., Any]) -> Callable[..., Any]:
        # Convert snake case to camel case.
        op_kind = "".join(word.capitalize() for word in fn.__name__.split("_"))
        sig = inspect.signature(fn)
        fn.__cocoindex_op_kind__ = op_kind  # type: ignore
        _register_op_factory(
            category=OpCategory.FUNCTION,
            expected_args=list(sig.parameters.items()),
            expected_return=sig.return_annotation,
            executor_factory=_SimpleFunctionExecutor,
            spec_loader=lambda: fn,
            op_kind=op_kind,
            op_args=op_args,
        )

        return fn

    return _inner


########################################################
# Custom target connector
########################################################


@dataclasses.dataclass
class _TargetConnectorContext:
    target_name: str
    spec: Any
    prepared_spec: Any
    key_decoder: Callable[[Any], Any]
    value_decoder: Callable[[Any], Any]


class _TargetConnector:
    """
    The connector class passed to the engine.
    """

    _spec_cls: type
    _connector_cls: type

    _get_persistent_key_fn: Callable[[_TargetConnectorContext, str], Any]
    _apply_setup_change_async_fn: Callable[
        [Any, dict[str, Any] | None, dict[str, Any] | None], Awaitable[None]
    ]
    _mutate_async_fn: Callable[..., Awaitable[None]]
    _mutatation_type: AnalyzedDictType | None

    def __init__(self, spec_cls: type, connector_cls: type):
        self._spec_cls = spec_cls
        self._connector_cls = connector_cls

        self._get_persistent_key_fn = _get_required_method(
            connector_cls, "get_persistent_key"
        )
        self._apply_setup_change_async_fn = _to_async_call(
            _get_required_method(connector_cls, "apply_setup_change")
        )

        mutate_fn = _get_required_method(connector_cls, "mutate")
        self._mutate_async_fn = _to_async_call(mutate_fn)

        # Store the type annotation for later use
        self._mutatation_type = self._analyze_mutate_mutation_type(
            connector_cls, mutate_fn
        )

    @staticmethod
    def _analyze_mutate_mutation_type(
        connector_cls: type, mutate_fn: Callable[..., Any]
    ) -> AnalyzedDictType | None:
        # Validate mutate_fn signature and extract type annotation
        mutate_sig = inspect.signature(mutate_fn)
        params = list(mutate_sig.parameters.values())

        if len(params) != 1:
            raise ValueError(
                f"Method {connector_cls.__name__}.mutate(*args) must have exactly one parameter, "
                f"got {len(params)}"
            )

        param = params[0]
        if param.kind != inspect.Parameter.VAR_POSITIONAL:
            raise ValueError(
                f"Method {connector_cls.__name__}.mutate(*args) parameter must be *args format, "
                f"got {param.kind.name}"
            )

        # Extract type annotation
        analyzed_args_type = analyze_type_info(param.annotation)
        if isinstance(analyzed_args_type.variant, AnalyzedAnyType):
            return None

        if analyzed_args_type.base_type is tuple:
            args = get_args(analyzed_args_type.core_type)
            if not args:
                return None
            if len(args) == 2:
                mutation_type = analyze_type_info(args[1])
                if isinstance(mutation_type.variant, AnalyzedAnyType):
                    return None
                if isinstance(mutation_type.variant, AnalyzedDictType):
                    return mutation_type.variant

        raise ValueError(
            f"Method {connector_cls.__name__}.mutate(*args) parameter must be a tuple with "
            f"2 elements (tuple[SpecType, dict[str, ValueStruct]], spec and mutation in dict), "
            "got {args_type}"
        )

    def create_export_context(
        self,
        name: str,
        spec: dict[str, Any],
        key_fields_schema: list[Any],
        value_fields_schema: list[Any],
    ) -> _TargetConnectorContext:
        key_annotation, value_annotation = (
            (
                self._mutatation_type.key_type,
                self._mutatation_type.value_type,
            )
            if self._mutatation_type is not None
            else (Any, Any)
        )

        key_decoder = make_engine_key_decoder(
            ["(key)"], key_fields_schema, analyze_type_info(key_annotation)
        )
        value_decoder = make_engine_struct_decoder(
            ["(value)"], value_fields_schema, analyze_type_info(value_annotation)
        )

        loaded_spec = _load_spec_from_engine(self._spec_cls, spec)
        prepare_method = getattr(self._connector_cls, "prepare", None)
        if prepare_method is None:
            prepared_spec = loaded_spec
        else:
            prepared_spec = prepare_method(loaded_spec)

        return _TargetConnectorContext(
            target_name=name,
            spec=loaded_spec,
            prepared_spec=prepared_spec,
            key_decoder=key_decoder,
            value_decoder=value_decoder,
        )

    def get_persistent_key(self, export_context: _TargetConnectorContext) -> Any:
        return self._get_persistent_key_fn(
            export_context.spec, export_context.target_name
        )

    def describe_resource(self, key: Any) -> str:
        describe_fn = getattr(self._connector_cls, "describe", None)
        if describe_fn is None:
            return str(key)
        return str(describe_fn(key))

    async def apply_setup_changes_async(
        self,
        changes: list[tuple[Any, list[dict[str, Any] | None], dict[str, Any] | None]],
    ) -> None:
        for key, previous, current in changes:
            prev_specs = [
                _load_spec_from_engine(self._spec_cls, spec)
                if spec is not None
                else None
                for spec in previous
            ]
            curr_spec = (
                _load_spec_from_engine(self._spec_cls, current)
                if current is not None
                else None
            )
            for prev_spec in prev_specs:
                await self._apply_setup_change_async_fn(key, prev_spec, curr_spec)

    @staticmethod
    def _decode_mutation(
        context: _TargetConnectorContext, mutation: list[tuple[Any, Any | None]]
    ) -> tuple[Any, dict[Any, Any | None]]:
        return (
            context.prepared_spec,
            {
                context.key_decoder(key): (
                    context.value_decoder(value) if value is not None else None
                )
                for key, value in mutation
            },
        )

    async def mutate_async(
        self,
        mutations: list[tuple[_TargetConnectorContext, list[tuple[Any, Any | None]]]],
    ) -> None:
        await self._mutate_async_fn(
            *(
                self._decode_mutation(context, mutation)
                for context, mutation in mutations
            )
        )


def target_connector(spec_cls: type) -> Callable[[type], type]:
    """
    Decorate a class to provide a target connector for an op.
    """

    # Validate the spec_cls is a TargetSpec.
    if not issubclass(spec_cls, TargetSpec):
        raise ValueError(f"Expect a TargetSpec, got {spec_cls}")

    # Register the target connector.
    def _inner(connector_cls: type) -> type:
        connector = _TargetConnector(spec_cls, connector_cls)
        _engine.register_target_connector(spec_cls.__name__, connector)
        return connector_cls

    return _inner
