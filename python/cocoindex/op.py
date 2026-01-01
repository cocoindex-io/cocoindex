"""
Facilities for defining cocoindex operations.
"""

import dataclasses
import inspect
from enum import Enum
from typing import (
    Any,
    Awaitable,
    Callable,
    Iterator,
    Protocol,
    dataclass_transform,
    TypeVar,
    Generic,
    Literal,
    get_args,
)
from collections.abc import AsyncIterator

from . import _engine  # type: ignore
from .subprocess_exec import executor_stub
from .engine_object import dump_engine_object, load_engine_object
from .engine_value import (
    make_engine_key_encoder,
    make_engine_value_encoder,
    make_engine_value_decoder,
    make_engine_key_decoder,
    make_engine_struct_decoder,
)
from .typing import KEY_FIELD_NAME
from ._internal import datatype
from . import engine_type
from .runtime import to_async_call
from .index import IndexOptions
import datetime


class OpCategory(Enum):
    """The category of the operation."""

    FUNCTION = "function"
    SOURCE = "source"
    TARGET = "target"
    DECLARATION = "declaration"
    TARGET_ATTACHMENT = "target_attachment"


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


class TargetAttachmentSpec(metaclass=SpecMeta, category=OpCategory.TARGET_ATTACHMENT):  # pylint: disable=too-few-public-methods
    """A target attachment spec. All its subclass can be instantiated similar to a dataclass, i.e. ClassName(field1=value1, field2=value2, ...)"""


class DeclarationSpec(metaclass=SpecMeta, category=OpCategory.DECLARATION):  # pylint: disable=too-few-public-methods
    """A declaration spec. All its subclass can be instantiated similar to a dataclass, i.e. ClassName(field1=value1, field2=value2, ...)"""


class Executor(Protocol):
    """An executor for an operation."""

    op_category: OpCategory


def _get_required_method(obj: type, name: str) -> Callable[..., Any]:
    method = getattr(obj, name, None)
    if method is None:
        raise ValueError(f"Method {name}() is required for {obj}")
    if not inspect.isfunction(method) and not inspect.ismethod(method):
        raise ValueError(f"{obj}.{name}() is not a function; {method}")
    return method


class _EngineFunctionExecutorFactory:
    _spec_loader: Callable[[Any], Any]
    _executor_cls: type

    def __init__(self, spec_loader: Callable[..., Any], executor_cls: type):
        self._spec_loader = spec_loader
        self._executor_cls = executor_cls

    def __call__(
        self, raw_spec: dict[str, Any], *args: Any, **kwargs: Any
    ) -> tuple[dict[str, Any], Executor]:
        spec = self._spec_loader(raw_spec)
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
    - batching: Whether the executor will be batched.
    - max_batch_size: The maximum batch size for the executor. Only valid if `batching` is True.
    - behavior_version: The behavior version of the executor. Cache will be invalidated if it
      changes. Must be provided if `cache` is True.
    - timeout: Timeout in seconds for this function execution. None means use default.
    - arg_relationship: It specifies the relationship between an input argument and the output,
      e.g. `(ArgRelationship.CHUNKS_BASE_TEXT, "content")` means the output is chunks for the
      input argument with name `content`.
    """

    gpu: bool = False
    cache: bool = False
    batching: bool = False
    max_batch_size: int | None = None
    behavior_version: int | None = None
    timeout: datetime.timedelta | None = None
    arg_relationship: tuple[ArgRelationship, str] | None = None


@dataclasses.dataclass
class _ArgInfo:
    decoder: Callable[[Any], Any]
    is_required: bool


def _make_batched_engine_value_decoder(
    field_path: list[str],
    src_type: engine_type.ValueType,
    dst_type_info: datatype.DataTypeInfo,
) -> Callable[[Any], Any]:
    if not isinstance(dst_type_info.variant, datatype.SequenceType):
        raise ValueError("Expected arguments for batching function to be a list type")
    elem_type_info = datatype.analyze_type_info(dst_type_info.variant.elem_type)
    base_decoder = make_engine_value_decoder(field_path, src_type, elem_type_info)
    return lambda value: [base_decoder(v) for v in value]


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

    if op_args.batching:
        if len(expected_args) != 1:
            raise ValueError("Batching is only supported for single argument functions")

    class _WrappedExecutor:
        _executor: Any
        _spec: Any
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
            attributes = {}
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
                        attributes[related_attr.value] = actual_arg.analyzed_value
                type_info = datatype.analyze_type_info(arg_param.annotation)
                enriched = engine_type.EnrichedValueType.decode(actual_arg.value_type)
                if op_args.batching:
                    decoder = _make_batched_engine_value_decoder(
                        [arg_name], enriched.type, type_info
                    )
                else:
                    decoder = make_engine_value_decoder(
                        [arg_name], enriched.type, type_info
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

            analyzed_expected_return_type = datatype.analyze_type_info(
                expected_return,
                nullable=potentially_missing_required_arg,
                extra_attrs=attributes,
            )
            self._result_encoder = make_engine_value_encoder(
                analyzed_expected_return_type
            )

            base_analyze_method = getattr(self._executor, "analyze", None)
            if base_analyze_method is not None:
                analyzed_result_type = datatype.analyze_type_info(
                    base_analyze_method(),
                    nullable=potentially_missing_required_arg,
                    extra_attrs=attributes,
                )
            else:
                if op_args.batching:
                    if not isinstance(
                        analyzed_expected_return_type.variant, datatype.SequenceType
                    ):
                        raise ValueError(
                            "Expected return type for batching function to be a list type"
                        )
                    analyzed_result_type = datatype.analyze_type_info(
                        analyzed_expected_return_type.variant.elem_type,
                        nullable=potentially_missing_required_arg,
                        extra_attrs=attributes,
                    )
                else:
                    analyzed_result_type = analyzed_expected_return_type
            encoded_type = engine_type.encode_enriched_type_info(analyzed_result_type)

            return encoded_type

        async def prepare(self) -> None:
            """
            Prepare for execution.
            It's executed after `analyze` and before any `__call__` execution.
            """
            prepare_method = getattr(self._executor, "prepare", None)
            if prepare_method is not None:
                await to_async_call(prepare_method)()
            self._acall = to_async_call(self._executor.__call__)

        async def __call__(self, *args: Any, **kwargs: Any) -> Any:
            decoded_args = []
            skipped_idx: list[int] | None = None
            if op_args.batching:
                if len(args) != 1:
                    raise ValueError(
                        "Batching is only supported for single argument functions"
                    )
                arg_info = self._args_info[0]
                if arg_info.is_required and args[0] is None:
                    return None
                decoded = arg_info.decoder(args[0])
                if arg_info.is_required:
                    skipped_idx = [i for i, arg in enumerate(decoded) if arg is None]
                    if len(skipped_idx) > 0:
                        decoded = [v for v in decoded if v is not None]
                        if len(decoded) == 0:
                            return [None for _ in range(len(skipped_idx))]
                    else:
                        skipped_idx = None
                decoded_args.append(decoded)
            else:
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

            if skipped_idx is None:
                return self._result_encoder(output)

            padded_output: list[Any] = []
            next_idx = 0
            for v in output:
                while next_idx < len(skipped_idx) and skipped_idx[next_idx] == len(
                    padded_output
                ):
                    next_idx += 1
                    padded_output.append(None)
                padded_output.append(v)

            while next_idx < len(skipped_idx):
                padded_output.append(None)
                next_idx += 1

            return self._result_encoder(padded_output)

        def enable_cache(self) -> bool:
            return op_args.cache

        def behavior_version(self) -> int | None:
            return op_args.behavior_version

        def timeout(self) -> datetime.timedelta | None:
            return op_args.timeout

        def batching_options(self) -> dict[str, Any] | None:
            if op_args.batching:
                return {
                    "max_batch_size": op_args.max_batch_size,
                }
            else:
                return None

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
        spec_cls = _resolve_forward_ref(type_hints["spec"])
        sig = inspect.signature(cls.__call__)
        _register_op_factory(
            category=spec_cls._op_category,
            expected_args=list(sig.parameters.items())[1:],  # First argument is `self`
            expected_return=sig.return_annotation,
            executor_factory=cls,
            spec_loader=lambda v: load_engine_object(spec_cls, v),
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
            spec_loader=lambda _: fn,
            op_kind=op_kind,
            op_args=op_args,
        )

        return fn

    return _inner


########################################################
# Custom source connector
########################################################


@dataclasses.dataclass
class SourceReadOptions:
    """
    The options for reading a source row.
    This is argument for both `list()` and `get_value()` methods.
    Note that in most cases (unless spelled out otherwise below) it's not a mandatory requirement, but more like a hint to say it's useful under the current context.

    - include_ordinal: Whether to include the ordinal of the source row.
      When provides_ordinal() returns True, you must provide `ordinal` in `list()` when `include_ordinal` is True.
      It's optional for other cases. It's helpful to skip unnecessary reprocessing early, and avoid output from older version of input over-writing the latest one when there's concurrency (especially multiple processes) and source updates frequently.

    - include_content_version_fp: Whether to include the content version fingerprint of the source row.
      It's always optional even if this is True.
      It's helpful to skip unnecessary reprocessing early.
      You should only consider providing it if you can directly get it without computing the hash on the content.

    - include_value: Whether to include the value of the source row.
      You must provide it in `get_value()` when `include_value` is True.
      It's optional for `list()`.
      Consider providing it when it's significantly cheaper then calling another `get_value()` for each row.
      It will save costs of individual `get_value()` calls.
    """

    include_ordinal: bool = False
    include_content_version_fp: bool = False
    include_value: bool = False


K = TypeVar("K")
V = TypeVar("V")

NON_EXISTENCE: Literal["NON_EXISTENCE"] = "NON_EXISTENCE"
NO_ORDINAL: Literal["NO_ORDINAL"] = "NO_ORDINAL"


@dataclasses.dataclass
class PartialSourceRowData(Generic[V]):
    """
    The data of a source row.

    - value: The value of the source row. NON_EXISTENCE means the row does not exist.
    - ordinal: The ordinal of the source row. NO_ORDINAL means ordinal is not available for the source.
    - content_version_fp: The content version fingerprint of the source row.
    """

    value: V | Literal["NON_EXISTENCE"] | None = None
    ordinal: int | Literal["NO_ORDINAL"] | None = None
    content_version_fp: bytes | None = None


@dataclasses.dataclass
class PartialSourceRow(Generic[K, V]):
    key: K
    data: PartialSourceRowData[V]


class _SourceExecutorContext:
    _executor: Any

    _key_encoder: Callable[[Any], Any]
    _key_decoder: Callable[[Any], Any]

    _value_encoder: Callable[[Any], Any]

    _list_fn: Callable[
        [SourceReadOptions],
        AsyncIterator[PartialSourceRow[Any, Any]]
        | Iterator[PartialSourceRow[Any, Any]],
    ]
    _orig_get_value_fn: Callable[..., Any]
    _get_value_fn: Callable[..., Awaitable[PartialSourceRowData[Any]]]
    _provides_ordinal_fn: Callable[[], bool] | None

    def __init__(
        self,
        executor: Any,
        key_type_info: datatype.DataTypeInfo,
        key_decoder: Callable[[Any], Any],
        value_type_info: datatype.DataTypeInfo,
    ):
        self._executor = executor

        self._key_encoder = make_engine_key_encoder(key_type_info)
        self._key_decoder = key_decoder
        self._value_encoder = make_engine_value_encoder(value_type_info)

        self._list_fn = _get_required_method(executor, "list")
        self._orig_get_value_fn = _get_required_method(executor, "get_value")
        self._get_value_fn = to_async_call(self._orig_get_value_fn)
        self._provides_ordinal_fn = getattr(executor, "provides_ordinal", None)

    def provides_ordinal(self) -> bool:
        if self._provides_ordinal_fn is not None:
            result = self._provides_ordinal_fn()
            return bool(result)
        else:
            return False

    async def list_async(
        self, options: dict[str, Any]
    ) -> AsyncIterator[tuple[Any, dict[str, Any]]]:
        """
        Return an async iterator that yields individual rows one by one.
        Each yielded item is a tuple of (key, data).
        """
        read_options = load_engine_object(SourceReadOptions, options)
        args = _build_args(self._list_fn, 0, options=read_options)
        list_result = self._list_fn(*args)

        # Handle both sync and async iterators
        if hasattr(list_result, "__aiter__"):
            async for partial_row in list_result:
                yield (
                    self._key_encoder(partial_row.key),
                    self._encode_source_row_data(partial_row.data),
                )
        else:
            for partial_row in list_result:
                yield (
                    self._key_encoder(partial_row.key),
                    self._encode_source_row_data(partial_row.data),
                )

    async def get_value_async(
        self,
        raw_key: Any,
        options: dict[str, Any],
    ) -> dict[str, Any]:
        key = self._key_decoder(raw_key)
        read_options = load_engine_object(SourceReadOptions, options)
        args = _build_args(self._orig_get_value_fn, 1, key=key, options=read_options)
        row_data = await self._get_value_fn(*args)
        return self._encode_source_row_data(row_data)

    def _encode_source_row_data(
        self, row_data: PartialSourceRowData[Any]
    ) -> dict[str, Any]:
        """Convert Python PartialSourceRowData to the format expected by Rust."""
        return {
            "ordinal": row_data.ordinal,
            "content_version_fp": row_data.content_version_fp,
            "value": (
                NON_EXISTENCE
                if row_data.value == NON_EXISTENCE
                else self._value_encoder(row_data.value)
            ),
        }


class _SourceConnector:
    """
    The connector class passed to the engine.
    """

    _spec_cls: type[Any]
    _key_type_info: datatype.DataTypeInfo
    _key_decoder: Callable[[Any], Any]
    _value_type_info: datatype.DataTypeInfo
    _table_type: engine_type.EnrichedValueType
    _connector_cls: type[Any]

    _create_fn: Callable[[Any], Awaitable[Any]]

    def __init__(
        self,
        spec_cls: type[Any],
        key_type: Any,
        value_type: Any,
        connector_cls: type[Any],
    ):
        self._spec_cls = spec_cls
        self._key_type_info = datatype.analyze_type_info(key_type)
        self._value_type_info = datatype.analyze_type_info(value_type)
        self._connector_cls = connector_cls

        # TODO: We can save the intermediate step after #1083 is fixed.
        encoded_engine_key_type = engine_type.encode_enriched_type_info(
            self._key_type_info
        )
        engine_key_type = engine_type.EnrichedValueType.decode(encoded_engine_key_type)

        # TODO: We can save the intermediate step after #1083 is fixed.
        encoded_engine_value_type = engine_type.encode_enriched_type_info(
            self._value_type_info
        )
        engine_value_type = engine_type.EnrichedValueType.decode(
            encoded_engine_value_type
        )

        if not isinstance(engine_value_type.type, engine_type.StructType):
            raise ValueError(
                f"Expected a engine_type.StructType, got {engine_value_type.type}"
            )

        if isinstance(engine_key_type.type, engine_type.StructType):
            key_fields_schema = engine_key_type.type.fields
        else:
            key_fields_schema = [
                engine_type.FieldSchema(name=KEY_FIELD_NAME, value_type=engine_key_type)
            ]
        self._key_decoder = make_engine_key_decoder(
            [], key_fields_schema, self._key_type_info
        )
        self._table_type = engine_type.EnrichedValueType(
            type=engine_type.TableType(
                kind="KTable",
                row=engine_type.StructSchema(
                    fields=key_fields_schema + engine_value_type.type.fields
                ),
                num_key_parts=len(key_fields_schema),
            ),
        )

        self._create_fn = to_async_call(_get_required_method(connector_cls, "create"))

    async def create_executor(self, raw_spec: dict[str, Any]) -> _SourceExecutorContext:
        spec = load_engine_object(self._spec_cls, raw_spec)
        executor = await self._create_fn(spec)
        return _SourceExecutorContext(
            executor, self._key_type_info, self._key_decoder, self._value_type_info
        )

    def get_table_type(self) -> Any:
        return dump_engine_object(self._table_type)


def source_connector(
    *,
    spec_cls: type[Any],
    key_type: Any = Any,
    value_type: Any = Any,
) -> Callable[[type], type]:
    """
    Decorate a class to provide a source connector for an op.
    """

    # Validate the spec_cls is a SourceSpec.
    if not issubclass(spec_cls, SourceSpec):
        raise ValueError(f"Expect a SourceSpec, got {spec_cls}")

    # Register the source connector.
    def _inner(connector_cls: type) -> type:
        connector = _SourceConnector(spec_cls, key_type, value_type, connector_cls)
        _engine.register_source_connector(spec_cls.__name__, connector)
        return connector_cls

    return _inner


########################################################
# Custom target connector
########################################################


@dataclasses.dataclass
class _TargetConnectorContext:
    target_name: str
    spec: Any
    prepared_spec: Any
    key_fields_schema: list[engine_type.FieldSchema]
    key_decoder: Callable[[Any], Any]
    value_fields_schema: list[engine_type.FieldSchema]
    value_decoder: Callable[[Any], Any]
    index_options: IndexOptions
    setup_state: Any


def _build_args(
    method: Callable[..., Any], num_required_args: int, **kwargs: Any
) -> list[Any]:
    signature = inspect.signature(method)
    for param in signature.parameters.values():
        if param.kind not in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            raise ValueError(
                f"Method {method.__name__} should only have positional arguments, got {param.kind.name}"
            )
    if len(signature.parameters) < num_required_args:
        raise ValueError(
            f"Method {method.__name__} must have at least {num_required_args} required arguments: "
            f"{', '.join(list(kwargs.keys())[:num_required_args])}"
        )
    if len(kwargs) > len(kwargs):
        raise ValueError(
            f"Method {method.__name__} can only have at most {num_required_args} arguments: {', '.join(kwargs.keys())}"
        )
    return [v for _, v in zip(signature.parameters, kwargs.values())]


class TargetStateCompatibility(Enum):
    """The compatibility of the target state."""

    COMPATIBLE = "Compatible"
    PARTIALLY_COMPATIBLE = "PartialCompatible"
    NOT_COMPATIBLE = "NotCompatible"


class _TargetConnector:
    """
    The connector class passed to the engine.
    """

    _spec_cls: type[Any]
    _persistent_key_type: Any
    _setup_state_cls: type[Any]
    _connector_cls: type[Any]

    _get_persistent_key_fn: Callable[[_TargetConnectorContext, str], Any]
    _apply_setup_change_async_fn: Callable[
        [Any, dict[str, Any] | None, dict[str, Any] | None], Awaitable[None]
    ]
    _mutate_async_fn: Callable[..., Awaitable[None]]
    _mutatation_type: datatype.MappingType | None

    def __init__(
        self,
        spec_cls: type[Any],
        persistent_key_type: Any,
        setup_state_cls: type[Any],
        connector_cls: type[Any],
    ):
        self._spec_cls = spec_cls
        self._persistent_key_type = persistent_key_type
        self._setup_state_cls = setup_state_cls
        self._connector_cls = connector_cls

        self._get_persistent_key_fn = _get_required_method(
            connector_cls, "get_persistent_key"
        )
        self._apply_setup_change_async_fn = to_async_call(
            _get_required_method(connector_cls, "apply_setup_change")
        )

        mutate_fn = _get_required_method(connector_cls, "mutate")
        self._mutate_async_fn = to_async_call(mutate_fn)

        # Store the type annotation for later use
        self._mutatation_type = self._analyze_mutate_mutation_type(
            connector_cls, mutate_fn
        )

    @staticmethod
    def _analyze_mutate_mutation_type(
        connector_cls: type, mutate_fn: Callable[..., Any]
    ) -> datatype.MappingType | None:
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
        analyzed_args_type = datatype.analyze_type_info(param.annotation)
        if isinstance(analyzed_args_type.variant, datatype.AnyType):
            return None

        if analyzed_args_type.base_type is tuple:
            args = get_args(analyzed_args_type.core_type)
            if not args:
                return None
            if len(args) == 2:
                mutation_type = datatype.analyze_type_info(args[1])
                if isinstance(mutation_type.variant, datatype.AnyType):
                    return None
                if isinstance(mutation_type.variant, datatype.MappingType):
                    return mutation_type.variant

        raise ValueError(
            f"Method {connector_cls.__name__}.mutate(*args) parameter must be a tuple with "
            f"2 elements (tuple[SpecType, dict[str, ValueStruct]], spec and mutation in dict), "
            f"got {analyzed_args_type.core_type}"
        )

    def create_export_context(
        self,
        name: str,
        raw_spec: dict[str, Any],
        raw_key_fields_schema: list[Any],
        raw_value_fields_schema: list[Any],
        raw_index_options: dict[str, Any],
    ) -> _TargetConnectorContext:
        key_annotation, value_annotation = (
            (
                self._mutatation_type.key_type,
                self._mutatation_type.value_type,
            )
            if self._mutatation_type is not None
            else (Any, Any)
        )

        key_fields_schema = engine_type.decode_field_schemas(raw_key_fields_schema)
        key_decoder = make_engine_key_decoder(
            ["<key>"], key_fields_schema, datatype.analyze_type_info(key_annotation)
        )
        value_fields_schema = engine_type.decode_field_schemas(raw_value_fields_schema)
        value_decoder = make_engine_struct_decoder(
            ["<value>"],
            value_fields_schema,
            datatype.analyze_type_info(value_annotation),
        )

        spec = load_engine_object(self._spec_cls, raw_spec)
        index_options = load_engine_object(IndexOptions, raw_index_options)
        return _TargetConnectorContext(
            target_name=name,
            spec=spec,
            prepared_spec=None,
            key_fields_schema=key_fields_schema,
            key_decoder=key_decoder,
            value_fields_schema=value_fields_schema,
            value_decoder=value_decoder,
            index_options=index_options,
            setup_state=None,
        )

    def get_persistent_key(self, export_context: _TargetConnectorContext) -> Any:
        args = _build_args(
            self._get_persistent_key_fn,
            1,
            spec=export_context.spec,
            target_name=export_context.target_name,
        )
        return dump_engine_object(self._get_persistent_key_fn(*args))

    def get_setup_state(self, export_context: _TargetConnectorContext) -> Any:
        get_setup_state_fn = getattr(self._connector_cls, "get_setup_state", None)
        if get_setup_state_fn is None:
            state = export_context.spec
            if not isinstance(state, self._setup_state_cls):
                raise ValueError(
                    f"Expect a get_setup_state() method for {self._connector_cls} that returns an instance of {self._setup_state_cls}"
                )
        else:
            args = _build_args(
                get_setup_state_fn,
                1,
                spec=export_context.spec,
                key_fields_schema=export_context.key_fields_schema,
                value_fields_schema=export_context.value_fields_schema,
                index_options=export_context.index_options,
            )
            state = get_setup_state_fn(*args)
            if not isinstance(state, self._setup_state_cls):
                raise ValueError(
                    f"Method {get_setup_state_fn.__name__} must return an instance of {self._setup_state_cls}, got {type(state)}"
                )
        export_context.setup_state = state
        return dump_engine_object(state)

    def check_state_compatibility(
        self, raw_desired_state: Any, raw_existing_state: Any
    ) -> Any:
        check_state_compatibility_fn = getattr(
            self._connector_cls, "check_state_compatibility", None
        )
        if check_state_compatibility_fn is not None:
            compatibility = check_state_compatibility_fn(
                load_engine_object(self._setup_state_cls, raw_desired_state),
                load_engine_object(self._setup_state_cls, raw_existing_state),
            )
        else:
            compatibility = (
                TargetStateCompatibility.COMPATIBLE
                if raw_desired_state == raw_existing_state
                else TargetStateCompatibility.PARTIALLY_COMPATIBLE
            )
        return dump_engine_object(compatibility)

    async def prepare_async(
        self,
        export_context: _TargetConnectorContext,
    ) -> None:
        prepare_fn = getattr(self._connector_cls, "prepare", None)
        if prepare_fn is None:
            export_context.prepared_spec = export_context.spec
            return
        args = _build_args(
            prepare_fn,
            1,
            spec=export_context.spec,
            setup_state=export_context.setup_state,
            key_fields_schema=export_context.key_fields_schema,
            value_fields_schema=export_context.value_fields_schema,
        )
        async_prepare_fn = to_async_call(prepare_fn)
        export_context.prepared_spec = await async_prepare_fn(*args)

    def describe_resource(self, raw_key: Any) -> str:
        key = load_engine_object(self._persistent_key_type, raw_key)
        describe_fn = getattr(self._connector_cls, "describe", None)
        if describe_fn is None:
            return str(key)
        return str(describe_fn(key))

    async def apply_setup_changes_async(
        self,
        changes: list[tuple[Any, list[dict[str, Any] | None], dict[str, Any] | None]],
    ) -> None:
        for raw_key, previous, current in changes:
            key = load_engine_object(self._persistent_key_type, raw_key)
            prev_specs = [
                load_engine_object(self._setup_state_cls, spec)
                if spec is not None
                else None
                for spec in previous
            ]
            curr_spec = (
                load_engine_object(self._setup_state_cls, current)
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


def target_connector(
    *,
    spec_cls: type[Any],
    persistent_key_type: Any = Any,
    setup_state_cls: type[Any] | None = None,
) -> Callable[[type], type]:
    """
    Decorate a class to provide a target connector for an op.
    """

    # Validate the spec_cls is a TargetSpec.
    if not issubclass(spec_cls, TargetSpec):
        raise ValueError(f"Expect a TargetSpec, got {spec_cls}")

    # Register the target connector.
    def _inner(connector_cls: type) -> type:
        connector = _TargetConnector(
            spec_cls, persistent_key_type, setup_state_cls or spec_cls, connector_cls
        )
        _engine.register_target_connector(spec_cls.__name__, connector)
        return connector_cls

    return _inner


def _resolve_forward_ref(t: Any) -> Any:
    if isinstance(t, str):
        return eval(t)  # pylint: disable=eval-used
    return t
