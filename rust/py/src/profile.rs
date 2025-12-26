use cocoindex_core::engine::profile::EngineProfile;

use crate::{
    component::PyComponentProcessor,
    effect::{PyEffectHandler, PyEffectSink},
    prelude::*,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct PyEngineProfile;

impl EngineProfile for PyEngineProfile {
    type HostStateCtx = Arc<Py<PyAny>>;

    type ComponentProc = PyComponentProcessor;
    type ComponentProcRet = Py<PyAny>;

    type EffectHdl = PyEffectHandler;
    type EffectKey = crate::value::PyKey;
    type EffectState = crate::value::PyValue;
    type EffectAction = Py<PyAny>;
    type EffectSink = PyEffectSink;
    type EffectValue = Py<PyAny>;
}
