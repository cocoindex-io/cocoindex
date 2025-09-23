import dataclasses
import datetime
from typing import TypedDict, NamedTuple

import numpy as np
from numpy.typing import NDArray

from cocoindex.convert import dump_engine_object, load_engine_object


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
