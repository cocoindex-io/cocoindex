use cocoindex_core::engine::profile::EngineProfile;

use crate::{
    component::PyComponentProcessor,
    prelude::*,
    target_state::{PyTargetActionSinkInner, PyTargetHandler},
    target_state_codec::{PyTargetAction, PyTargetStateValue},
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
    type TargetAction = PyTargetAction;
    type TargetActionSink = PyTargetActionSinkInner;
    type TargetStateValue = PyTargetStateValue;
}
