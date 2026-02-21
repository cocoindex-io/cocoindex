"""
Cocoindex is a framework for building and running indexing pipelines.
"""

from . import (
    _engine,  # type: ignore
    _version_check,
    cli,
    functions,
    sources,
    targets,
    utils,
)
from . import targets as storages  # Deprecated: Use targets instead
from ._version import __version__
from .auth_registry import (
    AuthEntryReference,
    add_auth_entry,
    add_transient_auth_entry,
    ref_auth_entry,
)
from .flow import (  # DEPRECATED
    DataScope,
    DataSlice,
    EvaluateAndDumpOptions,
    Flow,
    FlowBuilder,
    FlowLiveUpdater,
    FlowLiveUpdaterOptions,
    FlowUpdaterStatusUpdates,
    GeneratedField,
    add_flow_def,
    drop_all_flows,
    flow_def,
    open_flow,
    remove_flow,
    setup_all_flows,
    transform_flow,
    update_all_flows_async,
)
from .index import (
    FtsIndexDef,
    HnswVectorIndexMethod,
    IndexOptions,
    IvfFlatVectorIndexMethod,
    VectorIndexDef,
    VectorSimilarityMetric,
)
from .lib import init, settings, start_server, stop
from .llm import LlmApiType, LlmSpec
from .query_handler import QueryHandlerResultFields, QueryInfo, QueryOutput
from .setting import (
    DatabaseConnectionSpec,
    GlobalExecutionOptions,
    ServerSettings,
    Settings,
    SurrealDBConnectionSpec,
    get_app_namespace,
)
from .typing import (
    Float32,
    Float64,
    Int64,
    Json,
    LocalDateTime,
    OffsetDateTime,
    Range,
    Vector,
)

_engine.init_pyo3_runtime()

__all__ = [
    "__version__",
    # Submodules
    "_engine",
    "functions",
    "llm",
    "sources",
    "targets",
    "storages",
    "cli",
    "op",
    "utils",
    # Auth registry
    "AuthEntryReference",
    "add_auth_entry",
    "add_transient_auth_entry",
    "ref_auth_entry",
    # Flow
    "FlowBuilder",
    "DataScope",
    "DataSlice",
    "Flow",
    "transform_flow",
    "flow_def",
    "EvaluateAndDumpOptions",
    "GeneratedField",
    "FlowLiveUpdater",
    "FlowLiveUpdaterOptions",
    "FlowUpdaterStatusUpdates",
    "open_flow",
    "add_flow_def",  # DEPRECATED
    "remove_flow",  # DEPRECATED
    "update_all_flows_async",
    "setup_all_flows",
    "drop_all_flows",
    # Lib
    "settings",
    "init",
    "start_server",
    "stop",
    # LLM
    "LlmSpec",
    "LlmApiType",
    # Index
    "VectorSimilarityMetric",
    "VectorIndexDef",
    "FtsIndexDef",
    "IndexOptions",
    "HnswVectorIndexMethod",
    "IvfFlatVectorIndexMethod",
    # Settings
    "DatabaseConnectionSpec",
    "SurrealDBConnectionSpec",
    "GlobalExecutionOptions",
    "Settings",
    "ServerSettings",
    "get_app_namespace",
    # Typing
    "Int64",
    "Float32",
    "Float64",
    "LocalDateTime",
    "OffsetDateTime",
    "Range",
    "Vector",
    "Json",
    # Query handler
    "QueryHandlerResultFields",
    "QueryInfo",
    "QueryOutput",
]
