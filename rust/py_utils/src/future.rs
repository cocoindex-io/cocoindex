use futures::FutureExt;
use futures::future::BoxFuture;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_async_runtimes::TaskLocals;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::error;

struct CancelOnDropPy {
    inner: BoxFuture<'static, PyResult<Py<PyAny>>>,
    task: Py<PyAny>,
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
        Python::attach(|py| {
            let kwargs = PyDict::new(py);
            let result = || -> PyResult<()> {
                // pass context so cancellation runs under the right contextvars
                kwargs.set_item("context", self.ctx.bind(py))?;
                self.event_loop.bind(py).call_method(
                    "call_soon_threadsafe",
                    (self.task.bind(py).getattr("cancel")?,),
                    Some(&kwargs),
                )?;
                // self.task.bind(py).call_method0("cancel")?;
                Ok(())
            }();
            if let Err(e) = result {
                error!("Error cancelling task: {e:?}");
            }
        });
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

    // 2) Create a Task so we own a handle we can cancel later
    let kwarg = PyDict::new(py);
    kwarg.set_item("context", &ctx)?;
    let task: Bound<'py, PyAny> = event_loop
        .call_method("create_task", (awaitable,), Some(&kwarg))?
        .into();

    // 3) Bridge it to a Rust Future as usual
    let fut = pyo3_async_runtimes::into_future_with_locals(locals, task.clone())?.boxed();

    Ok(CancelOnDropPy {
        inner: fut,
        task: task.unbind(),
        event_loop: event_loop.unbind(),
        ctx: ctx.unbind(),
        done: AtomicBool::new(false),
    })
}
