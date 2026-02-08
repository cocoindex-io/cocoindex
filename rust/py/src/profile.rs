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

    type ComponentProc = PyComponentProcessor;
    type FunctionData = crate::value::PyValue;

    type TargetHdl = PyTargetHandler;
    type TargetStateKey = cocoindex_core::state::stable_path::StableKey;
    type TargetStateTrackingRecord = crate::value::PyValue;
    type TargetAction = Py<PyAny>;
    type TargetActionSink = PyTargetActionSink;
    type TargetStateValue = Py<PyAny>;
}
