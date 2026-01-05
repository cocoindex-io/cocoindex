"""
Cocoindex is a framework for building and running indexing pipelines.
"""

from ._version import __version__

from . import _version_check

from . import _engine  # type: ignore
from . import functions, sources, targets, cli, utils

from . import targets as storages  # Deprecated: Use targets instead

from .auth_registry import (
    AuthEntryReference,
    add_auth_entry,
    add_transient_auth_entry,
    ref_auth_entry,
)
from .flow import FlowBuilder, DataScope, DataSlice, Flow, transform_flow
from .flow import flow_def
from .flow import EvaluateAndDumpOptions, GeneratedField
from .flow import FlowLiveUpdater, FlowLiveUpdaterOptions, FlowUpdaterStatusUpdates
from .flow import open_flow
from .flow import add_flow_def, remove_flow  # DEPRECATED
from .flow import update_all_flows_async, setup_all_flows, drop_all_flows
from .lib import settings, init, start_server, stop
from .llm import LlmSpec, LlmApiType
from .index import (
    FtsIndexDef,
    VectorSimilarityMetric,
    VectorIndexDef,
    IndexOptions,
    HnswVectorIndexMethod,
    IvfFlatVectorIndexMethod,
)
from .setting import (
    DatabaseConnectionSpec,
    GlobalExecutionOptions,
    Settings,
    ServerSettings,
    get_app_namespace,
)
from .query_handler import QueryHandlerResultFields, QueryInfo, QueryOutput
from .typing import (
    Int64,
    Float32,
    Float64,
    LocalDateTime,
    OffsetDateTime,
    Range,
    Vector,
    Json,
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
