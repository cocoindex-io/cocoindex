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
    /// Shared with the loop callback that creates the task; see its field.
    dropped: Arc<AtomicBool>,
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
        if unsafe { pyo3::ffi::Py_IsInitialized() == 0 } {
            return;
        }
        // Set under the task lock: the loop callback checks it under the same
        // lock before it creates the task, so a drop before the callback ran
        // (the sync caller still held the GIL) leaves no task behind.
        let task = {
            // In `Drop`: a poisoned mutex must not panic here.
            let mut task_ref = self
                .task_ref
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.dropped.store(true, Ordering::SeqCst);
            task_ref.take()
        };
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
    /// Set by `CancelOnDropPy::drop`; a future dropped before this callback
    /// ran gets no task, and its coroutine is closed instead of run.
    dropped: Arc<AtomicBool>,
}

#[pymethods]
impl CreateTaskAndBridge {
    fn __call__(&mut self) -> PyResult<()> {
        Python::attach(|py| {
            let awaitable = self.awaitable.take().unwrap();
            let close_unrun = |awaitable: &Py<PyAny>| -> PyResult<()> {
                // Nobody awaits the result any more. A coroutine that never
                // ran is closed, so it neither runs nor warns.
                if awaitable.bind(py).hasattr(pyo3::intern!(py, "close"))? {
                    awaitable
                        .bind(py)
                        .call_method0(pyo3::intern!(py, "close"))?;
                }
                Ok(())
            };
            if self.dropped.load(Ordering::SeqCst) {
                return close_unrun(&awaitable);
            }
            // No Python call runs under the task mutex: `ensure_future` can
            // release the GIL, and a thread dropping the future meanwhile
            // takes that mutex while holding the GIL.
            let asyncio = py.import(pyo3::intern!(py, "asyncio"))?;
            let task = match asyncio
                .call_method1(pyo3::intern!(py, "ensure_future"), (awaitable.bind(py),))
            {
                Ok(task) => task,
                Err(e) => {
                    // The awaiter sees why, instead of a bare cancellation;
                    // reported once (not again as a callback error), and the
                    // never-started coroutine is closed.
                    if let Some(tx) = self.result_tx.take() {
                        let _ = tx.send(Err(e));
                    }
                    return close_unrun(&awaitable);
                }
            };
            let dropped_meanwhile = {
                let mut task_ref = self.task_ref.lock().unwrap();
                if self.dropped.load(Ordering::SeqCst) {
                    true
                } else {
                    *task_ref = Some(task.clone().unbind());
                    false
                }
            };
            if dropped_meanwhile {
                task.call_method0(pyo3::intern!(py, "cancel"))?;
                return Ok(());
            }
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
    let event_loop: Bound<'py, PyAny> = locals.event_loop(py);
    let ctx: Bound<'py, PyAny> = locals.context(py);

    let (result_tx, result_rx) = oneshot::channel();
    let task_ref: Arc<Mutex<Option<Py<PyAny>>>> = Arc::new(Mutex::new(None));
    let dropped = Arc::new(AtomicBool::new(false));

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
            dropped: dropped.clone(),
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
        dropped,
        event_loop: event_loop.unbind(),
        ctx: ctx.unbind(),
        done: AtomicBool::new(false),
    })
}
