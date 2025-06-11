from . import flow
from . import setting
from . import _engine  # type: ignore


def sync_setup() -> _engine.SetupStatus:
    flow.ensure_all_flows_built()
    return _engine.sync_setup()


def drop_setup(flow_names: list[str]) -> _engine.SetupStatus:
    flow.ensure_all_flows_built()
    return _engine.drop_setup([flow.get_full_flow_name(name) for name in flow_names])


def flow_names_with_setup() -> list[str]:
    result = []
    for name in _engine.flow_names_with_setup():
        app_namespace, name = setting.split_app_namespace(name, ".")
        if app_namespace == setting.get_app_namespace():
            result.append(name)
    return result


def apply_setup_changes(setup_status: _engine.SetupStatus) -> None:
    _engine.apply_setup_changes(setup_status)
