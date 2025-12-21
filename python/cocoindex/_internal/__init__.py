"""
Library level functions and states.
"""

from . import core as _core
from . import serde as _serde


_core.init_runtime(  # type: ignore
    serialize_fn=_serde.serialize,
    deserialize_fn=_serde.deserialize,
)
