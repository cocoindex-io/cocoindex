"""
Library level functions and states.
"""

from . import core as _core
from . import seder as _seder


_core.init_runtime(  # type: ignore
    serialize_fn=_seder.serialize,
    deserialize_fn=_seder.deserialize,
)
