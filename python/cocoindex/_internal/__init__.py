"""
Library level functions and states.
"""


def _init() -> None:
    from .core import init_runtime  # type: ignore
    from .seder import serialize, deserialize

    init_runtime(
        serialize_fn=serialize,
        deserialize_fn=deserialize,
    )


_init()
