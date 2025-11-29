import pickle
from typing import Any


def serialize(value: Any) -> bytes:
    return pickle.dumps(value, 5)


def deserialize(data: bytes) -> Any:
    return pickle.loads(data)
