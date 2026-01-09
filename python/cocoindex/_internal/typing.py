from __future__ import annotations

from typing import Any
from typing_extensions import TypeIs


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
