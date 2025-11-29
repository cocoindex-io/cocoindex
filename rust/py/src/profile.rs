use cocoindex_core::engine::profile::EngineProfile;

use crate::{
    component::PyComponentBuilder,
    effect::{PyEffectReconciler, PyEffectSink},
    prelude::*,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PyEngineProfile;

impl EngineProfile for PyEngineProfile {
    type HostStateCtx = Arc<Py<PyAny>>;

    type ComponentBld = PyComponentBuilder;
    type ComponentBuildRet = Py<PyAny>;
    type Error = PyErr;

    type EffectRcl = PyEffectReconciler;
    type EffectKey = crate::value::PyKey;
    type EffectState = Arc<Py<PyAny>>;
    type EffectAction = Py<PyAny>;
    type EffectSink = PyEffectSink;
    type EffectDecl = Py<PyAny>;
}
