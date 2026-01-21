import dataclasses
import inspect
from typing import (
    Any,
    Literal,
    Self,
    overload,
)

import cocoindex.typing
from cocoindex._internal import datatype


@dataclasses.dataclass
class VectorTypeSchema:
    element_type: "BasicValueType"
    dimension: int | None

    def __str__(self) -> str:
        dimension_str = f", {self.dimension}" if self.dimension is not None else ""
        return f"Vector[{self.element_type}{dimension_str}]"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def decode(obj: dict[str, Any]) -> "VectorTypeSchema":
        return VectorTypeSchema(
            element_type=BasicValueType.decode(obj["element_type"]),
            dimension=obj.get("dimension"),
        )

    def encode(self) -> dict[str, Any]:
        return {
            "element_type": self.element_type.encode(),
            "dimension": self.dimension,
        }


@dataclasses.dataclass
class UnionTypeSchema:
    variants: list["BasicValueType"]

    def __str__(self) -> str:
        types_str = " | ".join(str(t) for t in self.variants)
        return f"Union[{types_str}]"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def decode(obj: dict[str, Any]) -> "UnionTypeSchema":
        return UnionTypeSchema(
            variants=[BasicValueType.decode(t) for t in obj["types"]]
        )

    def encode(self) -> dict[str, Any]:
        return {"types": [variant.encode() for variant in self.variants]}


@dataclasses.dataclass
class BasicValueType:
    """
    Mirror of Rust BasicValueType in JSON form.

    For Vector and Union kinds, extra fields are populated accordingly.
    """

    kind: Literal[
        "Bytes",
        "Str",
        "Bool",
        "Int64",
        "Float32",
        "Float64",
        "Range",
        "Uuid",
        "Date",
        "Time",
        "LocalDateTime",
        "OffsetDateTime",
        "TimeDelta",
        "Json",
        "Vector",
        "Union",
    ]
    vector: VectorTypeSchema | None = None
    union: UnionTypeSchema | None = None

    def __str__(self) -> str:
        if self.kind == "Vector" and self.vector is not None:
            dimension_str = (
                f", {self.vector.dimension}"
                if self.vector.dimension is not None
                else ""
            )
            return f"Vector[{self.vector.element_type}{dimension_str}]"
        elif self.kind == "Union" and self.union is not None:
            types_str = " | ".join(str(t) for t in self.union.variants)
            return f"Union[{types_str}]"
        else:
            return self.kind

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def decode(obj: dict[str, Any]) -> "BasicValueType":
        kind = obj["kind"]
        if kind == "Vector":
            return BasicValueType(
                kind=kind,  # type: ignore[arg-type]
                vector=VectorTypeSchema.decode(obj),
            )
        if kind == "Union":
            return BasicValueType(
                kind=kind,  # type: ignore[arg-type]
                union=UnionTypeSchema.decode(obj),
            )
        return BasicValueType(kind=kind)  # type: ignore[arg-type]

    def encode(self) -> dict[str, Any]:
        result = {"kind": self.kind}
        if self.kind == "Vector" and self.vector is not None:
            result.update(self.vector.encode())
        elif self.kind == "Union" and self.union is not None:
            result.update(self.union.encode())
        return result


@dataclasses.dataclass
class EnrichedValueType:
    type: "ValueType"
    nullable: bool = False
    attrs: dict[str, Any] | None = None

    def __str__(self) -> str:
        result = str(self.type)
        if self.nullable:
            result += "?"
        if self.attrs:
            attrs_str = ", ".join(f"{k}: {v}" for k, v in self.attrs.items())
            result += f" [{attrs_str}]"
        return result

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def decode(obj: dict[str, Any]) -> "EnrichedValueType":
        return EnrichedValueType(
            type=decode_value_type(obj["type"]),
            nullable=obj.get("nullable", False),
            attrs=obj.get("attrs"),
        )

    def encode(self) -> dict[str, Any]:
        result: dict[str, Any] = {"type": self.type.encode()}
        if self.nullable:
            result["nullable"] = True
        if self.attrs is not None:
            result["attrs"] = self.attrs
        return result


@dataclasses.dataclass
class FieldSchema:
    name: str
    value_type: EnrichedValueType
    description: str | None = None

    def __str__(self) -> str:
        return f"{self.name}: {self.value_type}"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def decode(obj: dict[str, Any]) -> "FieldSchema":
        return FieldSchema(
            name=obj["name"],
            value_type=EnrichedValueType.decode(obj),
            description=obj.get("description"),
        )

    def encode(self) -> dict[str, Any]:
        result = self.value_type.encode()
        result["name"] = self.name
        if self.description is not None:
            result["description"] = self.description
        return result


@dataclasses.dataclass
class StructSchema:
    fields: list[FieldSchema]
    description: str | None = None

    def __str__(self) -> str:
        fields_str = ", ".join(str(field) for field in self.fields)
        return f"Struct({fields_str})"

    def __repr__(self) -> str:
        return self.__str__()

    @classmethod
    def decode(cls, obj: dict[str, Any]) -> Self:
        return cls(
            fields=[FieldSchema.decode(f) for f in obj["fields"]],
            description=obj.get("description"),
        )

    def encode(self) -> dict[str, Any]:
        result: dict[str, Any] = {"fields": [field.encode() for field in self.fields]}
        if self.description is not None:
            result["description"] = self.description
        return result


@dataclasses.dataclass
class StructType(StructSchema):
    kind: Literal["Struct"] = "Struct"

    def __str__(self) -> str:
        # Use the parent's __str__ method for consistency
        return super().__str__()

    def __repr__(self) -> str:
        return self.__str__()

    def encode(self) -> dict[str, Any]:
        result = super().encode()
        result["kind"] = self.kind
        return result


@dataclasses.dataclass
class TableType:
    kind: Literal["KTable", "LTable"]
    row: StructSchema
    num_key_parts: int | None = None  # Only for KTable

    def __str__(self) -> str:
        if self.kind == "KTable":
            num_parts = self.num_key_parts if self.num_key_parts is not None else 1
            table_kind = f"KTable({num_parts})"
        else:  # LTable
            table_kind = "LTable"

        return f"{table_kind}({self.row})"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def decode(obj: dict[str, Any]) -> "TableType":
        row_obj = obj["row"]
        row = StructSchema(
            fields=[FieldSchema.decode(f) for f in row_obj["fields"]],
            description=row_obj.get("description"),
        )
        return TableType(
            kind=obj["kind"],  # type: ignore[arg-type]
            row=row,
            num_key_parts=obj.get("num_key_parts"),
        )

    def encode(self) -> dict[str, Any]:
        result: dict[str, Any] = {"kind": self.kind, "row": self.row.encode()}
        if self.num_key_parts is not None:
            result["num_key_parts"] = self.num_key_parts
        return result


ValueType = BasicValueType | StructType | TableType


def decode_field_schemas(objs: list[dict[str, Any]]) -> list[FieldSchema]:
    return [FieldSchema.decode(o) for o in objs]


def decode_value_type(obj: dict[str, Any]) -> ValueType:
    kind = obj["kind"]
    if kind == "Struct":
        return StructType.decode(obj)

    if kind in cocoindex.typing.TABLE_TYPES:
        return TableType.decode(obj)

    # Otherwise it's a basic value
    return BasicValueType.decode(obj)


def encode_value_type(value_type: ValueType) -> dict[str, Any]:
    """Encode a ValueType to its dictionary representation."""
    return value_type.encode()


def _encode_struct_schema(
    struct_info: datatype.StructType, key_type: type | None = None
) -> tuple[dict[str, Any], int | None]:
    fields = []

    def add_field(
        name: str, analyzed_type: datatype.DataTypeInfo, description: str | None = None
    ) -> None:
        try:
            type_info = encode_enriched_type_info(analyzed_type)
        except ValueError as e:
            e.add_note(
                f"Failed to encode annotation for field - "
                f"{struct_info.struct_type.__name__}.{name}: {analyzed_type.core_type}"
            )
            raise
        type_info["name"] = name
        if description is not None:
            type_info["description"] = description
        fields.append(type_info)

    def add_fields_from_struct(struct_info: datatype.StructType) -> None:
        for field in struct_info.fields:
            add_field(
                field.name,
                datatype.analyze_type_info(field.type_hint),
                field.description,
            )

    result: dict[str, Any] = {}
    num_key_parts = None
    if key_type is not None:
        key_type_info = datatype.analyze_type_info(key_type)
        if isinstance(key_type_info.variant, datatype.BasicType):
            add_field(cocoindex.typing.KEY_FIELD_NAME, key_type_info)
            num_key_parts = 1
        elif isinstance(key_type_info.variant, datatype.StructType):
            add_fields_from_struct(key_type_info.variant)
            num_key_parts = len(fields)
        else:
            raise ValueError(f"Unsupported key type: {key_type}")

    add_fields_from_struct(struct_info)

    result["fields"] = fields
    if doc := inspect.getdoc(struct_info.struct_type):
        result["description"] = doc
    return result, num_key_parts


def _encode_type(type_info: datatype.DataTypeInfo) -> dict[str, Any]:
    variant = type_info.variant

    if isinstance(variant, datatype.AnyType):
        raise ValueError("Specific type annotation is expected")

    if isinstance(variant, datatype.OtherType):
        raise ValueError(f"Unsupported type annotation: {type_info.core_type}")

    if isinstance(variant, datatype.BasicType):
        return {"kind": variant.kind}

    if isinstance(variant, datatype.StructType):
        encoded_type, _ = _encode_struct_schema(variant)
        encoded_type["kind"] = "Struct"
        return encoded_type

    if isinstance(variant, datatype.SequenceType):
        elem_type_info = datatype.analyze_type_info(variant.elem_type)
        encoded_elem_type = _encode_type(elem_type_info)
        if isinstance(elem_type_info.variant, datatype.StructType):
            if variant.vector_info is not None:
                raise ValueError("LTable type must not have a vector info")
            row_type, _ = _encode_struct_schema(elem_type_info.variant)
            return {"kind": "LTable", "row": row_type}
        else:
            vector_info = variant.vector_info
            return {
                "kind": "Vector",
                "element_type": encoded_elem_type,
                "dimension": vector_info and vector_info.dim,
            }

    if isinstance(variant, datatype.MappingType):
        value_type_info = datatype.analyze_type_info(variant.value_type)
        if not isinstance(value_type_info.variant, datatype.StructType):
            raise ValueError(
                f"KTable value must have a Struct type, got {value_type_info.core_type}"
            )
        row_type, num_key_parts = _encode_struct_schema(
            value_type_info.variant,
            variant.key_type,
        )
        return {
            "kind": "KTable",
            "row": row_type,
            "num_key_parts": num_key_parts,
        }

    if isinstance(variant, datatype.UnionType):
        return {
            "kind": "Union",
            "types": [
                _encode_type(datatype.analyze_type_info(typ))
                for typ in variant.variant_types
            ],
        }


def encode_enriched_type_info(type_info: datatype.DataTypeInfo) -> dict[str, Any]:
    """
    Encode an `datatype.DataTypeInfo` to a CocoIndex engine's `EnrichedValueType` representation
    """
    encoded: dict[str, Any] = {"type": _encode_type(type_info)}

    if type_info.attrs is not None:
        encoded["attrs"] = type_info.attrs

    if type_info.nullable:
        encoded["nullable"] = True

    return encoded


@overload
def encode_enriched_type(t: None) -> None: ...


@overload
def encode_enriched_type(t: Any) -> dict[str, Any]: ...


def encode_enriched_type(t: Any) -> dict[str, Any] | None:
    """
    Convert a Python type to a CocoIndex engine's type representation
    """
    if t is None:
        return None

    return encode_enriched_type_info(datatype.analyze_type_info(t))


def resolve_forward_ref(t: Any) -> Any:
    if isinstance(t, str):
        return eval(t)  # pylint: disable=eval-used
    return t
