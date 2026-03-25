use cocoindex_core::engine::profile::EngineProfile;

use crate::{
    component::PyComponentProcessor,
    prelude::*,
    target_state::{PyTargetActionSink, PyTargetHandler},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct PyEngineProfile;

impl EngineProfile for PyEngineProfile {
    type HostRuntimeCtx = crate::runtime::PyAsyncContext;
    type HostCtx = Py<PyAny>;

    type ComponentProc = PyComponentProcessor;
    type FunctionData = crate::value::PyStoredValue;

    type TargetHdl = PyTargetHandler;
    type TargetStateTrackingRecord = crate::value::PyStoredValue;
    type TargetAction = Py<PyAny>;
    type TargetActionSink = PyTargetActionSink;
    type TargetStateValue = Py<PyAny>;
}
