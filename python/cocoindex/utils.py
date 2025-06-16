from .flow import Flow
from .setting import get_app_namespace


def get_target_default_name(flow: Flow, target_name: str, delimiter: str = "__") -> str:
    """
    Get the default name for a target.
    It's used as the underlying target name (e.g. a table, a collection, etc.) followed by most targets, if not explicitly specified.
    """
    return (
        get_app_namespace(trailing_delimiter=delimiter)
        + flow.name
        + delimiter
        + target_name
    )


get_target_storage_default_name = (
    get_target_default_name  # Deprecated: Use get_target_default_name instead
)
