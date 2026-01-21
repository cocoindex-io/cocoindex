import collections
import dataclasses
import inspect
import types
import typing
from typing import (
    Annotated,
    Any,
    Iterator,
    NamedTuple,
    get_type_hints,
)

import numpy as np

# Optional Pydantic support
try:
    import pydantic

    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False


def extract_ndarray_elem_dtype(ndarray_type: Any) -> Any:
    args = typing.get_args(ndarray_type)
    _, dtype_spec = args
    dtype_args = typing.get_args(dtype_spec)
    if not dtype_args:
        raise ValueError(f"Invalid dtype specification: {dtype_spec}")
    return dtype_args[0]


def is_numpy_number_type(t: type) -> bool:
    return isinstance(t, type) and issubclass(t, (np.integer, np.floating))


def is_namedtuple_type(t: type) -> bool:
    return isinstance(t, type) and issubclass(t, tuple) and hasattr(t, "_fields")


def is_pydantic_model(t: Any) -> bool:
    """Check if a type is a Pydantic model."""
    if not PYDANTIC_AVAILABLE or not isinstance(t, type):
        return False
    try:
        return issubclass(t, pydantic.BaseModel)
    except TypeError:
        return False


def is_record_type(t: Any) -> bool:
    return isinstance(t, type) and (
        dataclasses.is_dataclass(t) or is_namedtuple_type(t) or is_pydantic_model(t)
    )


class DtypeRegistry:
    """
    Registry for NumPy dtypes used in CocoIndex.
    Maps NumPy dtypes to their CocoIndex type kind.
    """

    _DTYPE_TO_KIND: dict[Any, str] = {
        np.float32: "Float32",
        np.float64: "Float64",
        np.int64: "Int64",
    }

    @classmethod
    def validate_dtype_and_get_kind(cls, dtype: Any) -> str:
        """
        Validate that the given dtype is supported, and get its CocoIndex kind by dtype.
        """
        if dtype is Any:
            raise TypeError(
                "NDArray for Vector must use a concrete numpy dtype, got `Any`."
            )
        kind = cls._DTYPE_TO_KIND.get(dtype)
        if kind is None:
            raise ValueError(
                f"Unsupported NumPy dtype in NDArray: {dtype}. "
                f"Supported dtypes: {cls._DTYPE_TO_KIND.keys()}"
            )
        return kind


class AnyType(NamedTuple):
    """
    When the type annotation is missing or matches any type.
    """


class SequenceType(NamedTuple):
    """
    Any list type, e.g. list[T], Sequence[T], NDArray[T], etc.
    """

    elem_type: Any


class RecordFieldInfo(NamedTuple):
    """
    Info about a field in a record type.
    """

    name: str
    type_hint: Any
    default_value: Any
    description: str | None


class RecordType(NamedTuple):
    """
    Any record type, e.g. dataclass, NamedTuple, etc.
    """

    record_type: type

    @property
    def fields(self) -> Iterator[RecordFieldInfo]:
        type_hints = get_type_hints(self.record_type, include_extras=True)
        if dataclasses.is_dataclass(self.record_type):
            parameters = inspect.signature(self.record_type).parameters
            for name, parameter in parameters.items():
                yield RecordFieldInfo(
                    name=name,
                    type_hint=type_hints.get(name, Any),
                    default_value=parameter.default,
                    description=None,
                )
        elif is_namedtuple_type(self.record_type):
            fields = getattr(self.record_type, "_fields", ())
            defaults = getattr(self.record_type, "_field_defaults", {})
            for name in fields:
                yield RecordFieldInfo(
                    name=name,
                    type_hint=type_hints.get(name, Any),
                    default_value=defaults.get(name, inspect.Parameter.empty),
                    description=None,
                )
        elif is_pydantic_model(self.record_type):
            model_fields = getattr(self.record_type, "model_fields", {})
            for name, field_info in model_fields.items():
                yield RecordFieldInfo(
                    name=name,
                    type_hint=type_hints.get(name, Any),
                    default_value=field_info.default
                    if field_info.default is not ...
                    else inspect.Parameter.empty,
                    description=field_info.description,
                )
        else:
            raise ValueError(f"Unsupported record type: {self.record_type}")


class UnionType(NamedTuple):
    """
    Any union type, e.g. T1 | T2 | ..., etc.
    """

    variant_types: list[Any]


class MappingType(NamedTuple):
    """
    Any dict type, e.g. dict[T1, T2], Mapping[T1, T2], etc.
    """

    key_type: Any
    value_type: Any


class LeafType(NamedTuple):
    """
    Any type that is not supported by CocoIndex.
    """


TypeVariant = AnyType | SequenceType | MappingType | RecordType | UnionType | LeafType


class DataTypeInfo(NamedTuple):
    """
    Analyzed info of a Python type.
    """

    # The type without annotations. e.g. int, list[int], dict[str, int]
    core_type: Any
    # The type without annotations and parameters. e.g. int, list, dict
    base_type: Any
    variant: TypeVariant
    nullable: bool = False
    annotations: tuple[Any, ...] = ()


def analyze_type_info(t: Any, *, nullable: bool = False) -> DataTypeInfo:
    """
    Analyze a Python type annotation and extract CocoIndex-specific type information.
    """

    annotations: tuple[Any, ...] = ()
    base_type = None
    type_args: tuple[Any, ...] = ()
    while True:
        base_type = typing.get_origin(t)
        if base_type is Annotated:
            annotations += t.__metadata__
            t = t.__origin__
        else:
            if base_type is None:
                base_type = t
            else:
                type_args = typing.get_args(t)
            break
    core_type = t

    variant: TypeVariant | None = None

    if base_type is Any or base_type is inspect.Parameter.empty:
        variant = AnyType()
    elif is_record_type(base_type):
        variant = RecordType(record_type=t)
    elif base_type is collections.abc.Sequence or base_type is list:
        elem_type = type_args[0] if len(type_args) > 0 else Any
        variant = SequenceType(elem_type=elem_type)
    elif base_type is np.ndarray:
        np_number_type = t
        elem_type = extract_ndarray_elem_dtype(np_number_type)
        variant = SequenceType(elem_type=elem_type)
    elif base_type is collections.abc.Mapping or base_type is dict or t is dict:
        key_type = type_args[0] if len(type_args) > 0 else Any
        elem_type = type_args[1] if len(type_args) > 1 else Any
        variant = MappingType(key_type=key_type, value_type=elem_type)
    elif base_type in (types.UnionType, typing.Union):
        non_none_types = [arg for arg in type_args if arg not in (None, types.NoneType)]
        if len(non_none_types) == 0:
            return analyze_type_info(None)

        if len(non_none_types) == 1:
            return analyze_type_info(
                non_none_types[0],
                nullable=nullable or len(non_none_types) < len(type_args),
            )

        variant = UnionType(variant_types=non_none_types)
    else:
        variant = LeafType()

    return DataTypeInfo(
        core_type=core_type,
        base_type=base_type,
        variant=variant,
        annotations=annotations,
        nullable=nullable,
    )
