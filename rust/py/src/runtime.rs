use std::{
    hash::{Hash, Hasher},
    sync::{Arc, OnceLock},
};

use crate::prelude::*;

use cocoindex_core::engine::runtime::{
    cancel_all, get_runtime, reset_global_cancellation, shutdown_runtime,
};
use cocoindex_py_utils::{cerror_to_pyerr, from_py_future};
use futures::FutureExt;
use pyo3::{call::PyCallArgs, exceptions::PyException};
use pyo3_async_runtimes::TaskLocals;
use tokio_util::task::AbortOnDropHandle;

pub struct PythonObjects {
    pub serialize_fn: Py<PyAny>,
    /// `ChildSlot(core_slot)`: wraps a `ChildTargetSlot` for a container sink.
    pub child_slot_wrapper_fn: Py<PyAny>,
    pub non_existence: Py<PyAny>,
    pub not_set: Py<PyAny>,
}

impl PythonObjects {
    pub fn serialize<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'py, PyAny>,
    ) -> Result<bytes::Bytes> {
        (|| -> PyResult<bytes::Bytes> {
            Ok(self
                .serialize_fn
                .call(py, (value,), None)?
                .extract::<bytes::Bytes>(py)?)
        })()
        .from_py_result()
    }
}

static PY_OBJECTS: OnceLock<std::mem::ManuallyDrop<PythonObjects>> = OnceLock::new();

#[pyfunction]
pub fn init_runtime(
    package_id: String,
    lang: String,
    serialize_fn: Py<PyAny>,
    child_slot_wrapper_fn: Py<PyAny>,
    non_existence: Py<PyAny>,
    not_set: Py<PyAny>,
) -> PyResult<()> {
    if let Err(_) = pyo3_async_runtimes::tokio::init_with_runtime(get_runtime()) {
        return Err(PyException::new_err(
            "Failed to initialize Tokio runtime: already initialized",
        ));
    }
    cocoindex_core::telemetry::init(package_id, lang);
    PY_OBJECTS
        .set(std::mem::ManuallyDrop::new(PythonObjects {
            serialize_fn,
            child_slot_wrapper_fn,
            non_existence,
            not_set,
        }))
        .map_err(|_| PyException::new_err("Failed to set Python objects: already initialized"))?;
    Ok(())
}

#[pyfunction]
pub fn shutdown_tokio_runtime() {
    shutdown_runtime();
}

/// Cancel the global cancellation token, causing all in-flight operations to
/// exit promptly.  Safe to call from signal handlers.
#[pyfunction]
#[pyo3(name = "cancel_all")]
pub fn py_cancel_all() {
    cancel_all();
}

/// Replace the cancelled global token with a fresh one so new operations can
/// proceed.  Called automatically at the start of each CLI command.
#[pyfunction]
#[pyo3(name = "reset_global_cancellation")]
pub fn py_reset_global_cancellation() {
    reset_global_cancellation();
}

pub fn python_objects() -> &'static PythonObjects {
    // ManuallyDrop<T> implements Deref<Target = T>, so &**x coerces to &T.
    &**PY_OBJECTS.get().expect("Python objects not initialized")
}

#[pyclass(name = "AsyncContext", from_py_object)]
#[derive(Clone)]
pub struct PyAsyncContext(pub Arc<TaskLocals>);

impl PartialEq for PyAsyncContext {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for PyAsyncContext {}

impl Hash for PyAsyncContext {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

#[pymethods]
impl PyAsyncContext {
    #[new]
    pub fn new(event_loop: Bound<PyAny>) -> Self {
        Self(Arc::new(pyo3_async_runtimes::TaskLocals::new(event_loop)))
    }
}

#[derive(Clone)]
pub enum PyCallback {
    Sync(Arc<Py<PyAny>>),
    Async(Arc<Py<PyAny>>),
}

impl PyCallback {
    /// The Python callable, however it is invoked.
    pub fn object(&self) -> &Py<PyAny> {
        match self {
            PyCallback::Sync(callback) | PyCallback::Async(callback) => callback,
        }
    }

    pub fn call<A>(
        &self,
        host_runtime_ctx: &PyAsyncContext,
        args: A,
    ) -> Result<impl Future<Output = Result<Py<PyAny>>> + Send + 'static>
    where
        A: for<'py> PyCallArgs<'py> + Send + 'static,
    {
        let boxed_fut = match self {
            PyCallback::Sync(sync_fn) => {
                let sync_fn = sync_fn.clone();
                let result_fut = AbortOnDropHandle::new(
                    get_runtime()
                        .spawn_blocking(move || Python::attach(|py| sync_fn.call(py, args, None))),
                );
                async move {
                    result_fut.await.map_err(|err| {
                        PyException::new_err(format!("Failed to call Python function: {err:?}"))
                    })?
                }
                .boxed()
            }
            PyCallback::Async(async_fn) => Python::attach(|py| {
                let result_coro = async_fn.call(py, args, None)?;
                from_py_future(py, &host_runtime_ctx.0, result_coro.into_bound(py))
            })?
            .boxed(),
        };
        Ok(boxed_fut.map(|r| r.from_py_result()))
    }
}

/// Wrap an optional Python async callback `(exc) -> Awaitable[None]`
/// as the Rust `OnError` closure expected by `Component::run_in_background`,
/// `Component::delete`, and the live-component controller.
///
/// Shared by `mount_async` (single-shot background mount), `update_full_async`,
/// `update_async`, and `delete_async` (live-component ops). The propagation
/// semantics are uniform across all of them:
///
/// - The engine error is converted with [`cerror_to_pyerr`], the same mapping
///   foreground paths (`use_mount`, `handle.ready()`) use: a Python-originated
///   failure reaches the callback as its original exception object (type +
///   traceback intact); engine-native failures map to the usual Python types.
/// - Coroutine returns normally (handler chain swallows) → `Ok(())` →
///   spawned task swallows.
/// - Coroutine raises (chain exhausted via raises) → `Err(...)` → spawned
///   task propagates via `handle.ready()`. The error is the `HostedPyErr`
///   produced by `PyCallback::call`, so the exception the handler raised
///   re-surfaces in Python with its type intact.
/// - Dispatch-level failures (couldn't schedule the coroutine) are logged
///   and converted to `Err` so they surface rather than disappearing.
pub fn build_on_error(
    host_runtime_ctx: PyAsyncContext,
    handler_callback: Option<Py<PyAny>>,
) -> Option<cocoindex_core::engine::component::OnError> {
    let handler_callback = handler_callback?;
    let cb = PyCallback::Async(Arc::new(handler_callback));
    Some(Arc::new(move |err: cocoindex_utils::prelude::Error| {
        let cb = cb.clone();
        let host_runtime_ctx = host_runtime_ctx.clone();
        Box::pin(async move {
            let exc = Python::attach(|py| cerror_to_pyerr(err).into_value(py));
            let fut = match cb.call(&host_runtime_ctx, (exc,)) {
                Ok(fut) => fut,
                Err(e) => {
                    error!("exception handler dispatch failed:\n{e:?}");
                    return Err(e.context("exception handler dispatch failed"));
                }
            };
            fut.await.map(|_| ())
        })
    }))
}
