"""
Flow is the main interface for building and running flows.
"""

from __future__ import annotations

import re
import inspect
from typing import Any, Callable, Sequence, TypeVar
from threading import Lock

from . import _engine
from . import vector
from . import op
from .typing import dump_type

class _NameBuilder:
    _existing_names: set[str]
    _next_name_index: dict[str, int]

    def __init__(self):
        self._existing_names = set()
        self._next_name_index = {}

    def build_name(self, name: str | None, /, prefix: str) -> str:
        """
        Build a name. If the name is None, generate a name with the given prefix.
        """
        if name is not None:
            self._existing_names.add(name)
            return name

        next_idx = self._next_name_index.get(prefix, 0)
        while True:
            name = f"{prefix}{next_idx}"
            next_idx += 1
            self._next_name_index[prefix] = next_idx
            if name not in self._existing_names:
                self._existing_names.add(name)
                return name


_WORD_BOUNDARY_RE = re.compile('(?<!^)(?=[A-Z])')
def _to_snake_case(name: str) -> str:
    return _WORD_BOUNDARY_RE.sub('_', name).lower()

def _create_data_slice(
        flow_builder_state: _FlowBuilderState,
        creator: Callable[[_engine.DataScopeRef | None, str | None], _engine.DataSlice],
        name: str | None = None) -> DataSlice:
    if name is None:
        return DataSlice(_DataSliceState(
            flow_builder_state,
            lambda target:
                creator(target[0], target[1]) if target is not None else creator(None, None)))
    else:
        return DataSlice(_DataSliceState(flow_builder_state, creator(None, name)))


def _spec_kind(spec: Any) -> str:
    return spec.__class__.__name__

def _spec_dump(spec: Any) -> dict[str, Any]:
    return spec.__dict__

T = TypeVar('T')

class _DataSliceState:
    flow_builder_state: _FlowBuilderState

    _lazy_lock: Lock | None = None  # None means it's not lazy.
    _data_slice: _engine.DataSlice | None = None
    _data_slice_creator: Callable[[tuple[_engine.DataScopeRef, str] | None],
                                  _engine.DataSlice] | None = None

    def __init__(
            self, flow_builder_state: _FlowBuilderState,
            data_slice: _engine.DataSlice | Callable[[tuple[_engine.DataScopeRef, str] | None],
                                                     _engine.DataSlice]):
        self.flow_builder_state = flow_builder_state

        if isinstance(data_slice, _engine.DataSlice):
            self._data_slice = data_slice
        else:
            self._lazy_lock = Lock()
            self._data_slice_creator = data_slice

    @property
    def engine_data_slice(self) -> _engine.DataSlice:
        """
        Get the internal DataSlice.
        """
        if self._lazy_lock is None:
            if self._data_slice is None:
                raise ValueError("Data slice is not initialized")
            return self._data_slice
        else:
            if self._data_slice_creator is None:
                raise ValueError("Data slice creator is not initialized")
            with self._lazy_lock:
                if self._data_slice is None:
                    self._data_slice = self._data_slice_creator(None)
                return self._data_slice

    def attach_to_scope(self, scope: _engine.DataScopeRef, field_name: str) -> None:
        """
        Attach the current data slice (if not yet attached) to the given scope.
        """
        if self._lazy_lock is not None:
            with self._lazy_lock:
                if self._data_slice_creator is None:
                    raise ValueError("Data slice creator is not initialized")
                if self._data_slice is None:
                    self._data_slice = self._data_slice_creator((scope, field_name))
                    return
        # TODO: We'll support this by an identity transformer or "aliasing" in the future.
        raise ValueError("DataSlice is already attached to a field")

class DataSlice:
    """A data slice represents a slice of data in a flow. It's readonly."""

    _state: _DataSliceState

    def __init__(self, state: _DataSliceState):
        self._state = state

    def __str__(self):
        return str(self._state.engine_data_slice)

    def __repr__(self):
        return repr(self._state.engine_data_slice)

    def __getitem__(self, field_name: str) -> DataSlice:
        field_slice = self._state.engine_data_slice.field(field_name)
        if field_slice is None:
            raise KeyError(field_name)
        return DataSlice(_DataSliceState(self._state.flow_builder_state, field_slice))

    def row(self) -> DataScope:
        """
        Return a scope representing each entry of the collection.
        """
        row_scope = self._state.engine_data_slice.collection_entry_scope()
        return DataScope(self._state.flow_builder_state, row_scope)

    def for_each(self, f: Callable[[DataScope], None]) -> None:
        """
        Apply a function to each row of the collection.
        """
        with self.row() as scope:
            f(scope)

    def transform(self, fn_spec: op.FunctionSpec, /, name: str | None = None) -> DataSlice:
        """
        Apply a function to the data slice.
        """
        args = [(self._state.engine_data_slice, None)]
        flow_builder_state = self._state.flow_builder_state
        return _create_data_slice(
            flow_builder_state,
            lambda target_scope, name:
                flow_builder_state.engine_flow_builder.transform(
                    _spec_kind(fn_spec),
                    _spec_dump(fn_spec),
                    args,
                    target_scope,
                    flow_builder_state.field_name_builder.build_name(
                        name, prefix=_to_snake_case(_spec_kind(fn_spec))+'_'),
                ),
            name)

    def call(self, func: Callable[[DataSlice], T]) -> T:
        """
        Call a function with the data slice.
        """
        return func(self)

def _data_slice_state(data_slice: DataSlice) -> _DataSliceState:
    return data_slice._state  # pylint: disable=protected-access

class DataScope:
    """
    A data scope in a flow.
    It has multple fields and collectors, and allow users to add new fields and collectors.
    """
    _flow_builder_state: _FlowBuilderState
    _engine_data_scope: _engine.DataScopeRef

    def __init__(self, flow_builder_state: _FlowBuilderState, data_scope: _engine.DataScopeRef):
        self._flow_builder_state = flow_builder_state
        self._engine_data_scope = data_scope

    def __str__(self):
        return str(self._engine_data_scope)

    def __repr__(self):
        return repr(self._engine_data_scope)

    def __getitem__(self, field_name: str) -> DataSlice:
        return DataSlice(_DataSliceState(
            self._flow_builder_state,
            self._flow_builder_state.engine_flow_builder.scope_field(
                self._engine_data_scope, field_name)))

    def __setitem__(self, field_name: str, value: DataSlice):
        value._state.attach_to_scope(self._engine_data_scope, field_name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        del self._engine_data_scope

    def add_collector(self, name: str | None = None) -> DataCollector:
        """
        Add a collector to the flow.
        """
        return DataCollector(
            self._flow_builder_state,
            self._engine_data_scope.add_collector(
                self._flow_builder_state.field_name_builder.build_name(name, prefix="_collector_")
            )
        )

class DataCollector:
    """A data collector is used to collect data into a collector."""
    _flow_builder_state: _FlowBuilderState
    _engine_data_collector: _engine.DataCollector

    def __init__(self, flow_builder_state: _FlowBuilderState,
                 data_collector: _engine.DataCollector):
        self._flow_builder_state = flow_builder_state
        self._engine_data_collector = data_collector

    def collect(self, **kwargs: DataSlice):
        """
        Collect data into the collector.
        """
        self._flow_builder_state.engine_flow_builder.collect(
            self._engine_data_collector, [(k, _data_slice_state(v).engine_data_slice) for k, v in kwargs.items()])

    def export(self, name: str, target_spec: op.StorageSpec, /, *,
              primary_key_fields: Sequence[str] | None = None,
              vector_index: Sequence[tuple[str, vector.VectorSimilarityMetric]] = ()):
        """
        Export the collected data to the specified target.
        """
        index_options: dict[str, Any] = {}
        if primary_key_fields is not None:
            index_options["primary_key_fields"] = primary_key_fields
        index_options["vector_index_defs"] = [
            {"field_name": field_name, "metric": metric.value}
            for field_name, metric in vector_index]
        self._flow_builder_state.engine_flow_builder.export(
            name, _spec_kind(target_spec), _spec_dump(target_spec),
            index_options, self._engine_data_collector)


_flow_name_builder = _NameBuilder()

class _FlowBuilderState:
    """
    A flow builder is used to build a flow.
    """
    engine_flow_builder: _engine.FlowBuilder
    field_name_builder: _NameBuilder

    def __init__(self, /, name: str | None = None):
        flow_name = _flow_name_builder.build_name(name, prefix="_flow_")
        self.engine_flow_builder = _engine.FlowBuilder(flow_name)
        self.field_name_builder = _NameBuilder()

class FlowBuilder:
    """
    A flow builder is used to build a flow.
    """
    _state: _FlowBuilderState

    def __init__(self, state: _FlowBuilderState):
        self._state = state

    def __str__(self):
        return str(self._state.engine_flow_builder)

    def __repr__(self):
        return repr(self._state.engine_flow_builder)

    def add_source(self, spec: op.SourceSpec, /, name: str | None = None) -> DataSlice:
        """
        Add a source to the flow.
        """
        return _create_data_slice(
            self._state,
            lambda target_scope, name: self._state.engine_flow_builder.add_source(
                _spec_kind(spec),
                _spec_dump(spec),
                target_scope,
                self._state.field_name_builder.build_name(
                    name, prefix=_to_snake_case(_spec_kind(spec))+'_'),
            ),
            name
        )


class Flow:
    """
    A flow describes an indexing pipeline.
    """
    _lazy_engine_flow: Callable[[], _engine.Flow]

    def __init__(self, engine_flow_creator: Callable[[], _engine.Flow]):
        engine_flow = None
        lock = Lock()
        def _lazy_engine_flow() -> _engine.Flow:
            nonlocal engine_flow, lock
            if engine_flow is None:
                with lock:
                    if engine_flow is None:
                        engine_flow = engine_flow_creator()
            return engine_flow
        self._lazy_engine_flow = _lazy_engine_flow

    def __str__(self):
        return str(self._lazy_engine_flow())

    def __repr__(self):
        return repr(self._lazy_engine_flow())

    def update(self):
        """
        Update the index defined by the flow.
        Once the function returns, the indice is fresh up to the moment when the function is called.
        """
        return self._lazy_engine_flow().update()

    def internal_flow(self) -> _engine.Flow:
        """
        Get the engine flow.
        """
        return self._lazy_engine_flow()


def _create_lazy_flow(name: str | None, fl_def: Callable[[FlowBuilder, DataScope], None]) -> Flow:
    """
    Create a flow without really building it yet.
    The flow will be built the first time when it's really needed.
    """
    def _create_engine_flow() -> _engine.Flow:
        flow_builder_state = _FlowBuilderState(name=name)
        root_scope = DataScope(
            flow_builder_state, flow_builder_state.engine_flow_builder.root_scope())
        fl_def(FlowBuilder(flow_builder_state), root_scope)
        return flow_builder_state.engine_flow_builder.build_flow()

    return Flow(_create_engine_flow)


_flows_lock = Lock()
_flows: dict[str, Flow] = {}

def add_flow_def(name: str, fl_def: Callable[[FlowBuilder, DataScope], None]) -> Flow:
    """Add a flow definition to the cocoindex library."""
    with _flows_lock:
        if name in _flows:
            raise KeyError(f"Flow with name {name} already exists")
        fl = _flows[name] = _create_lazy_flow(name, fl_def)
    return fl

def flow_def(name = None) -> Callable[[Callable[[FlowBuilder, DataScope], None]], Flow]:
    """
    A decorator to wrap the flow definition.
    """
    return lambda fl_def: add_flow_def(name or fl_def.__name__, fl_def)

def flow_names() -> list[str]:
    """
    Get the names of all flows.
    """
    with _flows_lock:
        return list(_flows.keys())

def flow_by_name(name: str) -> Flow:
    """
    Get a flow by name.
    """
    with _flows_lock:
        return _flows[name]

def ensure_all_flows_built() -> None:
    """
    Ensure all flows are built.
    """
    with _flows_lock:
        for fl in _flows.values():
            fl.internal_flow()

_transient_flow_name_builder = _NameBuilder()
class TransientFlow:
    """
    A transient transformation flow that transforms in-memory data.
    """
    _engine_flow: _engine.TransientFlow

    def __init__(
            self, flow_fn: Callable[..., DataSlice],
            flow_arg_types: Sequence[Any], /, name: str | None = None):

        flow_builder_state = _FlowBuilderState(
            name=_transient_flow_name_builder.build_name(name, prefix="_transient_flow_"))
        sig = inspect.signature(flow_fn)
        if len(sig.parameters) != len(flow_arg_types):
            raise ValueError(
                f"Number of parameters in the flow function ({len(sig.parameters)}) "
                "does not match the number of argument types ({len(flow_arg_types)})")

        kwargs: dict[str, DataSlice] = {}
        for (param_name, param), param_type in zip(sig.parameters.items(), flow_arg_types):
            if param.kind not in (inspect.Parameter.POSITIONAL_OR_KEYWORD,
                                  inspect.Parameter.KEYWORD_ONLY):
                raise ValueError(f"Parameter {param_name} is not a parameter can be passed by name")
            engine_ds = flow_builder_state.engine_flow_builder.add_direct_input(
                param_name, dump_type(param_type))
            kwargs[param_name] = DataSlice(_DataSliceState(flow_builder_state, engine_ds))

        output = flow_fn(**kwargs)
        flow_builder_state.engine_flow_builder.set_direct_output(
            _data_slice_state(output).engine_data_slice)
        self._engine_flow = flow_builder_state.engine_flow_builder.build_transient_flow()

    def __str__(self):
        return str(self._engine_flow)

    def __repr__(self):
        return repr(self._engine_flow)

    def internal_flow(self) -> _engine.TransientFlow:
        """
        Get the internal flow.
        """
        return self._engine_flow
