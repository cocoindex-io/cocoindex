from __future__ import annotations

import uuid
from typing import Any, Mapping, Sequence, TYPE_CHECKING, Union
from typing_extensions import TypeIs

if TYPE_CHECKING:
    from cocoindex._internal.core import Fingerprint

# --- StableKey type alias (accepted by StablePath.concat) ---
StableKey = Union[None, bool, int, str, bytes, uuid.UUID, tuple["StableKey", ...]]

# --- Fingerprintable type alias (accepted by fingerprint_simple_object) ---
Fingerprintable = Union[
    None,
    bool,
    int,
    float,
    str,
    bytes,
    uuid.UUID,
    "Fingerprint",
    Sequence["Fingerprintable"],
    Mapping["Fingerprintable", "Fingerprintable"],
    set["Fingerprintable"],
]


class NotSetType:
    __slots__ = ()
    _instance: NotSetType | None = None

    def __new__(cls) -> NotSetType:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "NOT_SET"


NOT_SET = NotSetType()


class NonExistenceType:
    __slots__ = ()
    _instance: NonExistenceType | None = None

    def __new__(cls) -> NonExistenceType:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "NON_EXISTENCE"


NON_EXISTENCE = NonExistenceType()


def is_non_existence(obj: Any) -> TypeIs[NonExistenceType]:
    return obj is NON_EXISTENCE
