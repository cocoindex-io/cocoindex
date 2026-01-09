use cocoindex_core::engine::function::{FnCallMemoGuard, PendingFnCallMemo};
use cocoindex_core::engine::runtime::get_runtime;
use pyo3_async_runtimes::tokio::future_into_py;

use crate::context::{PyComponentProcessorContext, PyFnCallContext};
use crate::fingerprint::PyFingerprint;
use crate::prelude::*;
use crate::value::PyValue;

#[pyclass(name = "PendingFnCallMemo")]
pub struct PyPendingFnCallMemo(Option<PendingFnCallMemo<PyEngineProfile>>);

#[pymethods]
impl PyPendingFnCallMemo {
    pub fn resolve(&mut self, fn_ctx: &PyFnCallContext, ret: Py<PyAny>) -> PyResult<bool> {
        let resolved = if let Some(pending_memo) = self.0.take() {
            pending_memo.resolve(&fn_ctx.0, || PyValue::new(ret))
        } else {
            false
        };
        Ok(resolved)
    }

    /// Release the underlying Rust lock without resolving the memo entry.
    ///
    /// Users should call this in a `finally` block if they don't end up calling `resolve(...)`.
    pub fn close(&mut self) {
        self.0 = None;
    }
}

async fn reserve_memoization_inner(
    comp_ctx: PyComponentProcessorContext,
    memo_fp: PyFingerprint,
) -> Result<Py<PyAny>> {
    let guard =
        cocoindex_core::engine::function::reserve_memoization(&comp_ctx.0, memo_fp.0).await?;
    Python::attach(|py| {
        let ret = match guard {
            FnCallMemoGuard::Ready(memo_guard) => match &*memo_guard {
                Some(memo) => memo.ret.value().clone_ref(py),
                None => Py::new(py, PyPendingFnCallMemo(None))?.into_any(),
            },
            FnCallMemoGuard::Pending(pending) => {
                Py::new(py, PyPendingFnCallMemo(Some(pending)))?.into_any()
            }
        };
        Ok(ret)
    })
}

#[pyfunction]
pub fn reserve_memoization(
    py: Python<'_>,
    comp_ctx: PyComponentProcessorContext,
    memo_fp: PyFingerprint,
) -> PyResult<Py<PyAny>> {
    py.detach(|| {
        get_runtime()
            .block_on(async move { reserve_memoization_inner(comp_ctx, memo_fp).await })
            .into_py_result()
    })
}

#[pyfunction]
pub fn reserve_memoization_async<'py>(
    py: Python<'py>,
    comp_ctx: PyComponentProcessorContext,
    memo_fp: PyFingerprint,
) -> PyResult<Bound<'py, PyAny>> {
    future_into_py(py, async move {
        reserve_memoization_inner(comp_ctx, memo_fp)
            .await
            .into_py_result()
    })
}
