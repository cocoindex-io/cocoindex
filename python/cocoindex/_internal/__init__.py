"""
Library level functions and states.
"""

from . import core as _core
from . import serde as _serde
from . import typing as _typing
from .memo_key import register_memo_key_function as _register_memo_key_function


_core.init_runtime(
    serialize_fn=_serde.serialize,
    deserialize_fn=_serde.deserialize,
    non_existence=_typing.NON_EXISTENCE,
    not_set=_typing.NOT_SET,
)

# Make core stable-path objects usable in memo key fingerprints.
_register_memo_key_function(_core.StablePath, lambda p: p.to_string())
