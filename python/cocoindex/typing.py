import datetime
import typing
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Generic,
    Literal,
    NamedTuple,
    Protocol,
    TypeVar,
)

import numpy as np
from numpy.typing import NDArray


class VectorInfo(NamedTuple):
    dim: int | None


class TypeKind(NamedTuple):
    kind: str


class TypeAttr:
    key: str
    value: Any

    def __init__(self, key: str, value: Any):
        self.key = key
        self.value = value


Annotation = TypeKind | TypeAttr | VectorInfo

Int64 = Annotated[int, TypeKind("Int64")]
Float32 = Annotated[float, TypeKind("Float32")]
Float64 = Annotated[float, TypeKind("Float64")]
Range = Annotated[tuple[int, int], TypeKind("Range")]
Json = Annotated[Any, TypeKind("Json")]
LocalDateTime = Annotated[datetime.datetime, TypeKind("LocalDateTime")]
OffsetDateTime = Annotated[datetime.datetime, TypeKind("OffsetDateTime")]

if TYPE_CHECKING:
    T_co = TypeVar("T_co", covariant=True)
    Dim_co = TypeVar("Dim_co", bound=int | None, covariant=True, default=None)

    class Vector(Protocol, Generic[T_co, Dim_co]):
        """Vector[T, Dim] is a special typing alias for an NDArray[T] with optional dimension info"""

        def __getitem__(self, index: int) -> T_co: ...
        def __len__(self) -> int: ...

else:

    class Vector:  # type: ignore[unreachable]
        """A special typing alias for an NDArray[T] with optional dimension info"""

        def __class_getitem__(self, params):
            if not isinstance(params, tuple):
                # No dimension provided, e.g., Vector[np.float32]
                dtype = params
                vector_info = VectorInfo(dim=None)
            else:
                # Element type and dimension provided, e.g., Vector[np.float32, Literal[3]]
                dtype, dim_literal = params
                # Extract the literal value
                dim_val = (
                    typing.get_args(dim_literal)[0]
                    if typing.get_origin(dim_literal) is Literal
                    else None
                )
                vector_info = VectorInfo(dim=dim_val)

            from cocoindex._internal.datatype import (
                analyze_type_info,
                is_numpy_number_type,
            )

            # Use NDArray for supported numeric dtypes, else list
            base_type = analyze_type_info(dtype).base_type
            if is_numpy_number_type(base_type) or base_type is np.ndarray:
                return Annotated[NDArray[dtype], vector_info]
            return Annotated[list[dtype], vector_info]


TABLE_TYPES: tuple[str, str] = ("KTable", "LTable")
KEY_FIELD_NAME: str = "_key"
