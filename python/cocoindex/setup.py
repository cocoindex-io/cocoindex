from . import flow
from . import _engine

def sync_setup() -> _engine.SetupStatus:
    flow.ensure_all_flows_built()
    return _engine.sync_setup()

def drop_setup(flow_names: list[str]) -> _engine.SetupStatus:
    flow.ensure_all_flows_built()
    return _engine.drop_setup(flow_names)

def flow_names_with_setup() -> list[str]:
    return _engine.flow_names_with_setup()

def apply_setup_changes(setup_status: _engine.SetupStatus):
    _engine.apply_setup_changes(setup_status)
