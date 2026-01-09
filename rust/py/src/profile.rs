use cocoindex_core::engine::profile::EngineProfile;

use crate::{
    component::PyComponentProcessor,
    effect::{PyEffectHandler, PyEffectSink},
    prelude::*,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct PyEngineProfile;

impl EngineProfile for PyEngineProfile {
    type HostRuntimeCtx = crate::runtime::PyAsyncContext;

    type ComponentProc = PyComponentProcessor;
    type FunctionData = crate::value::PyValue;

    type EffectHdl = PyEffectHandler;
    type EffectKey = Arc<crate::value::PyKey>;
    type EffectState = crate::value::PyValue;
    type EffectAction = Py<PyAny>;
    type EffectSink = PyEffectSink;
    type EffectValue = Py<PyAny>;
}
