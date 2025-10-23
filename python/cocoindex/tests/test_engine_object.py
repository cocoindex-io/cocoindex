import dataclasses
import datetime
from typing import TypedDict, NamedTuple, Literal

import numpy as np
from numpy.typing import NDArray
import pytest

from cocoindex.typing import Vector
from cocoindex.engine_object import dump_engine_object, load_engine_object

# Optional Pydantic support for testing
try:
    import pydantic

    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False


@dataclasses.dataclass
class LocalTargetFieldMapping:
    source: str
    target: str | None = None


@dataclasses.dataclass
class LocalNodeFromFields:
    label: str
    fields: list[LocalTargetFieldMapping]


@dataclasses.dataclass
class LocalNodes:
    kind = "Node"
    label: str


@dataclasses.dataclass
class LocalRelationships:
    kind = "Relationship"
    rel_type: str
    source: LocalNodeFromFields
    target: LocalNodeFromFields


class LocalPoint(NamedTuple):
    x: int
    y: int


class UserInfo(TypedDict):
    id: str
    age: int


def test_timedelta_roundtrip_via_dump_load() -> None:
    td = datetime.timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=500)
    dumped = dump_engine_object(td)
    loaded = load_engine_object(datetime.timedelta, dumped)
    assert isinstance(loaded, datetime.timedelta)
    assert loaded == td


def test_ndarray_roundtrip_via_dump_load() -> None:
    value: NDArray[np.float32] = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    dumped = dump_engine_object(value)
    assert dumped == [1.0, 2.0, 3.0]
    loaded = load_engine_object(NDArray[np.float32], dumped)
    assert isinstance(loaded, np.ndarray)
    assert loaded.dtype == np.float32
    assert np.array_equal(loaded, value)


def test_nodes_kind_is_carried() -> None:
    node = LocalNodes(label="User")
    dumped = dump_engine_object(node)
    # dumped should include discriminator
    assert dumped.get("kind") == "Node"
    # load back
    loaded = load_engine_object(LocalNodes, dumped)
    assert isinstance(loaded, LocalNodes)
    # class-level attribute is preserved
    assert getattr(loaded, "kind", None) == "Node"
    assert loaded.label == "User"


def test_relationships_union_discriminator() -> None:
    rel = LocalRelationships(
        rel_type="LIKES",
        source=LocalNodeFromFields(
            label="User", fields=[LocalTargetFieldMapping("id")]
        ),
        target=LocalNodeFromFields(
            label="Item", fields=[LocalTargetFieldMapping("id")]
        ),
    )
    dumped = dump_engine_object(rel)
    assert dumped.get("kind") == "Relationship"
    loaded = load_engine_object(LocalNodes | LocalRelationships, dumped)
    assert isinstance(loaded, LocalRelationships)
    assert getattr(loaded, "kind", None) == "Relationship"
    assert loaded.rel_type == "LIKES"
    assert dataclasses.asdict(loaded.source) == {
        "label": "User",
        "fields": [{"source": "id", "target": None}],
    }
    assert dataclasses.asdict(loaded.target) == {
        "label": "Item",
        "fields": [{"source": "id", "target": None}],
    }


def test_typed_dict_roundtrip_via_dump_load() -> None:
    user: UserInfo = {"id": "u1", "age": 30}
    dumped = dump_engine_object(user)
    assert dumped == {"id": "u1", "age": 30}
    loaded = load_engine_object(UserInfo, dumped)
    assert loaded == user


def test_namedtuple_roundtrip_via_dump_load() -> None:
    p = LocalPoint(1, 2)
    dumped = dump_engine_object(p)
    assert dumped == {"x": 1, "y": 2}
    loaded = load_engine_object(LocalPoint, dumped)
    assert isinstance(loaded, LocalPoint)
    assert loaded == p


def test_dataclass_missing_fields_with_auto_defaults() -> None:
    """Test that missing fields are automatically assigned safe default values."""

    @dataclasses.dataclass
    class TestClass:
        required_field: str
        optional_field: str | None  # Should get None
        list_field: list[str]  # Should get []
        dict_field: dict[str, int]  # Should get {}
        explicit_default: str = "default"  # Should use explicit default

    # Input missing optional_field, list_field, dict_field (but has explicit_default via class definition)
    input_data = {"required_field": "test_value"}

    loaded = load_engine_object(TestClass, input_data)

    assert isinstance(loaded, TestClass)
    assert loaded.required_field == "test_value"
    assert loaded.optional_field is None  # Auto-default for Optional
    assert loaded.list_field == []  # Auto-default for list
    assert loaded.dict_field == {}  # Auto-default for dict
    assert loaded.explicit_default == "default"  # Explicit default from class


def test_namedtuple_missing_fields_with_auto_defaults() -> None:
    """Test that missing fields in NamedTuple are automatically assigned safe default values."""
    from typing import NamedTuple

    class TestTuple(NamedTuple):
        required_field: str
        optional_field: str | None  # Should get None
        list_field: list[str]  # Should get []
        dict_field: dict[str, int]  # Should get {}

    # Input missing optional_field, list_field, dict_field
    input_data = {"required_field": "test_value"}

    loaded = load_engine_object(TestTuple, input_data)

    assert isinstance(loaded, TestTuple)
    assert loaded.required_field == "test_value"
    assert loaded.optional_field is None  # Auto-default for Optional
    assert loaded.list_field == []  # Auto-default for list
    assert loaded.dict_field == {}  # Auto-default for dict


def test_dataclass_unsupported_type_still_fails() -> None:
    """Test that fields with unsupported types still cause errors when missing."""

    @dataclasses.dataclass
    class TestClass:
        required_field1: str
        required_field2: int  # No auto-default for int

    # Input missing required_field2 which has no safe auto-default
    input_data = {"required_field1": "test_value"}

    # Should still raise an error because int has no safe auto-default
    try:
        load_engine_object(TestClass, input_data)
        assert False, "Expected TypeError to be raised"
    except TypeError:
        pass  # Expected behavior


def test_dump_vector_type_annotation_with_dim() -> None:
    """Test dumping a vector type annotation with a specified dimension."""
    expected_dump = {
        "type": {
            "kind": "Vector",
            "element_type": {"kind": "Float32"},
            "dimension": 3,
        }
    }
    assert dump_engine_object(Vector[np.float32, Literal[3]]) == expected_dump


def test_dump_vector_type_annotation_no_dim() -> None:
    """Test dumping a vector type annotation with no dimension."""
    expected_dump_no_dim = {
        "type": {
            "kind": "Vector",
            "element_type": {"kind": "Float64"},
            "dimension": None,
        }
    }
    assert dump_engine_object(Vector[np.float64]) == expected_dump_no_dim


@pytest.mark.skipif(not PYDANTIC_AVAILABLE, reason="Pydantic not available")
def test_pydantic_unsupported_type_still_fails() -> None:
    """Test that fields with unsupported types still cause errors when missing."""

    class TestPydantic(pydantic.BaseModel):
        required_field1: str
        required_field2: int  # No auto-default for int
        optional_field: str | None
        list_field: list[str]
        dict_field: dict[str, int]
        field_with_default: str = "default_value"

    # Input missing required_field2 which has no safe auto-default
    input_data = {"required_field1": "test_value"}

    # Should still raise an error because int has no safe auto-default
    with pytest.raises(pydantic.ValidationError):
        load_engine_object(TestPydantic, input_data)

    assert load_engine_object(
        TestPydantic, {"required_field1": "test_value", "required_field2": 1}
    ) == TestPydantic(
        required_field1="test_value",
        required_field2=1,
        field_with_default="default_value",
        optional_field=None,
        list_field=[],
        dict_field={},
    )


@pytest.mark.skipif(not PYDANTIC_AVAILABLE, reason="Pydantic not available")
def test_pydantic_field_descriptions() -> None:
    """Test that Pydantic field descriptions are extracted and included in schema."""
    from pydantic import BaseModel, Field

    class UserWithDescriptions(BaseModel):
        """A user model with field descriptions."""

        name: str = Field(description="The user's full name")
        age: int = Field(description="The user's age in years", ge=0, le=150)
        email: str = Field(description="The user's email address")
        is_active: bool = Field(
            description="Whether the user account is active", default=True
        )

    # Test that field descriptions are extracted
    encoded_schema = dump_engine_object(UserWithDescriptions)

    # Check that the schema contains field descriptions
    assert "fields" in encoded_schema["type"]
    fields = encoded_schema["type"]["fields"]

    # Find fields by name and check descriptions
    field_descriptions = {field["name"]: field.get("description") for field in fields}

    assert field_descriptions["name"] == "The user's full name"
    assert field_descriptions["age"] == "The user's age in years"
    assert field_descriptions["email"] == "The user's email address"
    assert field_descriptions["is_active"] == "Whether the user account is active"


@pytest.mark.skipif(not PYDANTIC_AVAILABLE, reason="Pydantic not available")
def test_pydantic_field_descriptions_without_field() -> None:
    """Test that Pydantic models without field descriptions work correctly."""
    from pydantic import BaseModel

    class UserWithoutDescriptions(BaseModel):
        """A user model without field descriptions."""

        name: str
        age: int
        email: str

    # Test that the schema works without descriptions
    encoded_schema = dump_engine_object(UserWithoutDescriptions)

    # Check that the schema contains fields but no descriptions
    assert "fields" in encoded_schema["type"]
    fields = encoded_schema["type"]["fields"]

    # Verify no descriptions are present
    for field in fields:
        assert "description" not in field or field["description"] is None


@pytest.mark.skipif(not PYDANTIC_AVAILABLE, reason="Pydantic not available")
def test_pydantic_mixed_descriptions() -> None:
    """Test Pydantic model with some fields having descriptions and others not."""
    from pydantic import BaseModel, Field

    class MixedDescriptions(BaseModel):
        """A model with mixed field descriptions."""

        name: str = Field(description="The name field")
        age: int  # No description
        email: str = Field(description="The email field")
        active: bool  # No description

    # Test that only fields with descriptions have them in the schema
    encoded_schema = dump_engine_object(MixedDescriptions)

    assert "fields" in encoded_schema["type"]
    fields = encoded_schema["type"]["fields"]

    # Find fields by name and check descriptions
    field_descriptions = {field["name"]: field.get("description") for field in fields}

    assert field_descriptions["name"] == "The name field"
    assert field_descriptions["age"] is None
    assert field_descriptions["email"] == "The email field"
    assert field_descriptions["active"] is None
