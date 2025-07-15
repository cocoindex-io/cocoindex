"""
Cocoindex is a framework for building and running indexing pipelines.
"""

from . import functions, sources, targets, cli, utils

from . import targets as storages  # Deprecated: Use targets instead

from .auth_registry import AuthEntryReference, add_auth_entry, ref_auth_entry
from .flow import FlowBuilder, DataScope, DataSlice, Flow, transform_flow
from .flow import flow_def
from .flow import EvaluateAndDumpOptions, GeneratedField
from .flow import FlowLiveUpdater, FlowLiveUpdaterOptions
from .flow import add_flow_def, remove_flow
from .flow import update_all_flows_async, setup_all_flows, drop_all_flows
from .lib import init, start_server, stop
from .llm import LlmSpec, LlmApiType
from .index import VectorSimilarityMetric, VectorIndexDef, IndexOptions
from .setting import DatabaseConnectionSpec, Settings, ServerSettings
from .setting import get_app_namespace
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

__all__ = [
    # Submodules
    "_engine",
    "functions",
    "llm",
    "sources",
    "targets",
    "storages",
    "cli",
    "utils",
    # Auth registry
    "AuthEntryReference",
    "add_auth_entry",
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
    "add_flow_def",
    "remove_flow",
    "update_all_flows_async",
    "setup_all_flows",
    "drop_all_flows",
    # Lib
    "init",
    "start_server",
    "stop",
    # LLM
    "LlmSpec",
    "LlmApiType",
    # Index
    "VectorSimilarityMetric",
    "VectorIndexDef",
    "IndexOptions",
    # Settings
    "DatabaseConnectionSpec",
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
]
