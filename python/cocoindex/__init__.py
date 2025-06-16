"""
Cocoindex is a framework for building and running indexing pipelines.
"""

from . import functions, sources, targets, cli, utils

from . import targets as storages  # Deprecated: Use targets instead

from .auth_registry import AuthEntryReference, add_auth_entry, ref_auth_entry
from .flow import FlowBuilder, DataScope, DataSlice, Flow, transform_flow
from .flow import flow_def
from .flow import EvaluateAndDumpOptions, GeneratedField
from .flow import update_all_flows_async, FlowLiveUpdater, FlowLiveUpdaterOptions
from .lib import init, start_server, stop, main_fn
from .llm import LlmSpec, LlmApiType
from .index import VectorSimilarityMetric, VectorIndexDef, IndexOptions
from .setting import DatabaseConnectionSpec, Settings, ServerSettings
from .setting import get_app_namespace
from .typing import Float32, Float64, LocalDateTime, OffsetDateTime, Range, Vector, Json

__all__ = [
    # Submodules
    "_engine",
    "functions",
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
    "update_all_flows_async",
    "FlowLiveUpdater",
    "FlowLiveUpdaterOptions",
    # Lib
    "init",
    "start_server",
    "stop",
    "main_fn",
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
    "Float32",
    "Float64",
    "LocalDateTime",
    "OffsetDateTime",
    "Range",
    "Vector",
    "Json",
]
