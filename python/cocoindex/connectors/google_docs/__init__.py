# `_target` first: when Google's client libraries are missing, it says which
# extra to install before anything imports the Google Drive connector.
from . import _target
from . import _auth
from . import _reader
from ._target import *
from ._auth import *
from ._reader import *
from cocoindex.connectors.google_drive._oauth import SignInError as SignInError
from cocoindex.connectors.google_drive._oauth import (
    SignInRequiredError as SignInRequiredError,
)

__all__ = (
    _target.__all__
    + _reader.__all__
    + _auth.__all__
    + ["SignInError", "SignInRequiredError"]
)
