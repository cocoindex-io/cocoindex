from . import flow
from . import _engine

def sync_setup() -> _engine.SetupStatusCheck:
    flow.ensure_all_flows_built()
    return _engine.sync_setup()

def drop_setup(flow_names: list[str]) -> _engine.SetupStatusCheck:
    flow.ensure_all_flows_built()
    return _engine.drop_setup(flow_names)

def flow_names_with_setup() -> list[str]:
    return _engine.flow_names_with_setup()

def apply_setup_changes(status_check: _engine.SetupStatusCheck):
    _engine.apply_setup_changes(status_check)
