from typing import NamedTuple

from . import flow
from . import _engine

class CheckSetupStatusOptions(NamedTuple):
    delete_legacy_flows: bool

def check_setup_status(options: CheckSetupStatusOptions) -> _engine.SetupStatusCheck:
    flow.ensure_all_flows_built()
    return _engine.check_setup_status(options)

def apply_setup_changes(status_check: _engine.SetupStatusCheck):
    _engine.apply_setup_changes(status_check)
