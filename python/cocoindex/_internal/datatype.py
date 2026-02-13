import collections
import dataclasses
import inspect
import types
import typing
from typing import (
    Annotated,
    Any,
    Callable,
    Generic,
    Iterator,
    NamedTuple,
    TypeVar,
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


# =============================================================================
# TypeChecker: Pre-built runtime type validation for StableKey values
# =============================================================================

_CheckFn = Callable[[Any, str], None]


def _build_check_fn(tp: Any) -> _CheckFn:
    """
    Build a validation closure for the given type annotation.

    Returns a function ``(value, path) -> None`` that raises ``TypeError``
    on mismatch.  *path* carries positional context for error messages
    (empty string at top level, ``"[0]"`` for tuple element 0, etc.).
    """
    origin = typing.get_origin(tp)
    args = typing.get_args(tp)

    # NoneType
    if tp is type(None):

        def check_none(value: Any, path: str) -> None:
            if value is not None:
                loc = f" at {path}" if path else ""
                raise TypeError(f"expected None{loc}, got {type(value).__name__}")

        return check_none

    # Any — accept everything
    if tp is Any:
        return lambda _v, _p: None

    # Union: str | int, str | None, etc.
    if origin in (types.UnionType, typing.Union):
        sub_fns = [_build_check_fn(a) for a in args]

        def check_union(value: Any, path: str) -> None:
            for fn in sub_fns:
                try:
                    fn(value, path)
                    return
                except TypeError:
                    continue
            loc = f" at {path}" if path else ""
            raise TypeError(
                f"expected {tp}{loc}, got {type(value).__name__}: {value!r}"
            )

        return check_union

    # Tuple types
    if origin is tuple:
        if len(args) == 2 and args[1] is Ellipsis:
            # Variable-length: tuple[X, ...]
            if args[0] is Any:

                def check_var_tuple_any(value: Any, path: str) -> None:
                    if not isinstance(value, tuple):
                        loc = f" at {path}" if path else ""
                        raise TypeError(
                            f"expected tuple{loc}, got {type(value).__name__}"
                        )

                return check_var_tuple_any

            elem_fn = _build_check_fn(args[0])

            def check_var_tuple(value: Any, path: str) -> None:
                if not isinstance(value, tuple):
                    loc = f" at {path}" if path else ""
                    raise TypeError(f"expected tuple{loc}, got {type(value).__name__}")
                for i, elem in enumerate(value):
                    elem_fn(elem, f"{path}[{i}]")

            return check_var_tuple

        if args:
            # Fixed-length: tuple[X, Y, ...]
            elem_fns = [_build_check_fn(a) for a in args]
            expected_len = len(args)

            def check_fixed_tuple(value: Any, path: str) -> None:
                if not isinstance(value, tuple):
                    loc = f" at {path}" if path else ""
                    raise TypeError(f"expected {tp}{loc}, got {type(value).__name__}")
                if len(value) != expected_len:
                    loc = f" at {path}" if path else ""
                    raise TypeError(
                        f"expected tuple of length {expected_len}{loc}, "
                        f"got length {len(value)}"
                    )
                for i, (elem, fn) in enumerate(zip(value, elem_fns)):
                    fn(elem, f"{path}[{i}]")

            return check_fixed_tuple

        # Bare tuple (no args) — just check isinstance
        def check_bare_tuple(value: Any, path: str) -> None:
            if not isinstance(value, tuple):
                loc = f" at {path}" if path else ""
                raise TypeError(f"expected tuple{loc}, got {type(value).__name__}")

        return check_bare_tuple

    # Simple concrete type: str, int, bytes, uuid.UUID, etc.
    if isinstance(tp, type):

        def check_isinstance(value: Any, path: str) -> None:
            if not isinstance(value, tp):
                loc = f" at {path}" if path else ""
                raise TypeError(
                    f"expected {tp.__name__}{loc}, got {type(value).__name__}: {value!r}"
                )

        return check_isinstance

    raise ValueError(f"Unsupported type for TypeChecker: {tp}")


T = TypeVar("T")


class TypeChecker(Generic[T]):
    """
    Pre-built runtime type checker.

    Analyzes a type annotation once at construction time and builds optimized
    validation closures.  At check time, validation is a fast series of
    ``isinstance`` calls and tuple-length comparisons — no reflection.

    Usage::

        _TABLE_KEY_CHECKER: TypeChecker[tuple[str, str]] = TypeChecker(tuple[str, str])

        # In reconcile():
        key = _TableKey(*_TABLE_KEY_CHECKER.check(key))
    """

    __slots__ = ("_expected_type", "_check_fn")

    def __init__(self, expected_type: type[T]) -> None:
        self._expected_type = expected_type
        self._check_fn = _build_check_fn(expected_type)

    def check(self, value: Any) -> T:
        """Validate *value* and return it typed as ``T``.

        Raises ``TypeError`` with a descriptive message on mismatch.
        """
        self._check_fn(value, "")
        return value  # type: ignore[no-any-return]

    def __repr__(self) -> str:
        return f"TypeChecker({self._expected_type})"
