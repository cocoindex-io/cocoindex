from __future__ import annotations


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
