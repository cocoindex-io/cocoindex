use cocoindex_core::engine::context::DataDepRecord;
use cocoindex_core::engine::profile::{DataDepsValidation, EngineProfile};
use cocoindex_core::state::stable_path::StablePath;

use crate::{
    component::PyComponentProcessor,
    function::{PyRefreshedDataDep, build_data_dep_records, data_deps_to_pylist},
    prelude::*,
    runtime::{PyAsyncContext, python_objects},
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

    /// Calls `cocoindex._internal.data_dep.validate_stored_data_deps`,
    /// registered at `init_runtime`: the same decode-then-validate phases a
    /// component's state handler runs for its own records, with no component
    /// context around them.
    fn validate_data_deps(
        host_runtime_ctx: &PyAsyncContext,
        path: &StablePath,
        records: Vec<DataDepRecord>,
    ) -> Result<impl Future<Output = Result<DataDepsValidation>> + Send + 'static> {
        let py_records: Py<PyAny> = Python::attach(|py| {
            data_deps_to_pylist(py, &records).map(|list| list.unbind().into_any())
        })
        .from_py_result()?;
        let fut = python_objects()
            .validate_data_deps_fn
            .call(host_runtime_ctx, (path.to_string(), py_records))?;
        Ok(async move {
            let result = fut.await?;
            Python::attach(|py| -> PyResult<DataDepsValidation> {
                // `(refreshed, can_reuse, states_changed)`; see
                // `validate_stored_data_deps` in data_dep.py.
                let tuple = result.bind(py).cast::<pyo3::types::PyTuple>()?;
                let refreshed: Vec<PyRefreshedDataDep> = tuple.get_item(0)?.extract()?;
                let can_reuse: bool = tuple.get_item(1)?.extract()?;
                let states_changed: bool = tuple.get_item(2)?.extract()?;
                Ok(DataDepsValidation {
                    records: build_data_dep_records(py, refreshed)?,
                    can_reuse,
                    states_changed,
                })
            })
            .from_py_result()
        })
    }
}
