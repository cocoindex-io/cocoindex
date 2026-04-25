"""
Library level functions and states.
"""

import sys as _sys
import sysconfig as _sysconfig

from . import core as _core
from . import serde as _serde
from . import typing as _typing
from .._version import __version__ as _package_version
from .memo_fingerprint import register_memo_key_function as _register_memo_key_function
from .target_state import _TypedTargetHandlerWrapper as _TypedTargetHandlerWrapper


_package_id = f"python-{_package_version}"
_gil_suffix = "t" if _sysconfig.get_config_var("Py_GIL_DISABLED") else ""
_lang = f"python{_sys.version_info.major}.{_sys.version_info.minor}{_gil_suffix}"

_core.init_runtime(
    package_id=_package_id,
    lang=_lang,
    serialize_fn=_serde.serialize,
    handler_wrapper_fn=_TypedTargetHandlerWrapper,
    non_existence=_typing.NON_EXISTENCE,
    not_set=_typing.NOT_SET,
)

# Make core stable-path objects usable in memo key fingerprints.
_register_memo_key_function(_core.StablePath, lambda p: p.to_string())
