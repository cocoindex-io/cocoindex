use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::BoxFuture;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_async_runtimes::TaskLocals;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::error;

struct CancelOnDropPy {
    inner: BoxFuture<'static, PyResult<Py<PyAny>>>,
    task_ref: Arc<Mutex<Option<Py<PyAny>>>>,
    event_loop: Py<PyAny>,
    ctx: Py<PyAny>,
    done: AtomicBool,
}

impl Future for CancelOnDropPy {
    type Output = PyResult<Py<PyAny>>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.inner).poll(cx) {
            Poll::Ready(out) => {
                self.done.store(true, Ordering::SeqCst);
                Poll::Ready(out)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for CancelOnDropPy {
    fn drop(&mut self) {
        if self.done.load(Ordering::SeqCst) {
            return;
        }
        let task = self.task_ref.lock().unwrap().take();
        if let Some(task) = task {
            Python::attach(|py| {
                let kwargs = PyDict::new(py);
                let result = || -> PyResult<()> {
                    // pass context so cancellation runs under the right contextvars
                    kwargs.set_item("context", self.ctx.bind(py))?;
                    self.event_loop.bind(py).call_method(
                        "call_soon_threadsafe",
                        (task.bind(py).getattr("cancel")?,),
                        Some(&kwargs),
                    )?;
                    Ok(())
                }();
                if let Err(e) = result {
                    error!("Error cancelling task: {e:?}");
                }
            });
        }
    }
}

/// Callback scheduled on the event loop thread via `call_soon_threadsafe`.
/// Creates an asyncio Task from the awaitable and sets up result forwarding.
#[pyclass]
struct CreateTaskAndBridge {
    awaitable: Option<Py<PyAny>>,
    result_tx: Option<oneshot::Sender<PyResult<Py<PyAny>>>>,
    task_ref: Arc<Mutex<Option<Py<PyAny>>>>,
}

#[pymethods]
impl CreateTaskAndBridge {
    fn __call__(&mut self) -> PyResult<()> {
        Python::attach(|py| {
            let awaitable = self.awaitable.take().unwrap();
            let asyncio = py.import(pyo3::intern!(py, "asyncio"))?;
            let task =
                asyncio.call_method1(pyo3::intern!(py, "ensure_future"), (awaitable.bind(py),))?;
            *self.task_ref.lock().unwrap() = Some(task.clone().unbind());
            task.call_method1(
                pyo3::intern!(py, "add_done_callback"),
                (TaskResultForwarder {
                    tx: self.result_tx.take(),
                },),
            )?;
            Ok(())
        })
    }
}

/// Done callback added to the asyncio Task. Forwards the task result
/// through the oneshot channel when the task completes.
#[pyclass]
struct TaskResultForwarder {
    tx: Option<oneshot::Sender<PyResult<Py<PyAny>>>>,
}

#[pymethods]
impl TaskResultForwarder {
    fn __call__(&mut self, task: Bound<PyAny>) -> PyResult<()> {
        if let Some(tx) = self.tx.take() {
            let result = task
                .call_method0(pyo3::intern!(task.py(), "result"))
                .map(|v| v.unbind());
            let _ = tx.send(result);
        }
        Ok(())
    }
}

pub fn from_py_future<'py, 'fut>(
    py: Python<'py>,
    locals: &TaskLocals,
    awaitable: Bound<'py, PyAny>,
) -> pyo3::PyResult<impl Future<Output = pyo3::PyResult<Py<PyAny>>> + Send + use<'fut>> {
    // 1) Capture loop + context from TaskLocals for thread-safe cancellation
    let event_loop: Bound<'py, PyAny> = locals.event_loop(py).into();
    let ctx: Bound<'py, PyAny> = locals.context(py);

    let (result_tx, result_rx) = oneshot::channel();
    let task_ref: Arc<Mutex<Option<Py<PyAny>>>> = Arc::new(Mutex::new(None));

    // 2) Schedule task creation on the event loop thread (thread-safe).
    //    This avoids calling event_loop.create_task() from a non-event-loop thread,
    //    which raises RuntimeError when PYTHONASYNCIODEBUG=1.
    let kwargs = PyDict::new(py);
    kwargs.set_item("context", &ctx)?;
    event_loop.call_method(
        pyo3::intern!(py, "call_soon_threadsafe"),
        (CreateTaskAndBridge {
            awaitable: Some(awaitable.unbind()),
            result_tx: Some(result_tx),
            task_ref: task_ref.clone(),
        },),
        Some(&kwargs),
    )?;

    // 3) Bridge the result channel to a Rust Future
    let fut = async move {
        match result_rx.await {
            Ok(result) => result,
            Err(_) => Python::attach(|py| {
                Err(PyErr::from_value(
                    py.import(pyo3::intern!(py, "asyncio"))?
                        .call_method0(pyo3::intern!(py, "CancelledError"))?,
                ))
            }),
        }
    }
    .boxed();

    Ok(CancelOnDropPy {
        inner: fut,
        task_ref,
        event_loop: event_loop.unbind(),
        ctx: ctx.unbind(),
        done: AtomicBool::new(false),
    })
}
