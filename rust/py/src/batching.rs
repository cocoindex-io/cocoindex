//! Python bindings for batching infrastructure.
//!
//! Exposes BatchQueue and Batcher to Python for implementing batched function execution.
//!
//! Design: Multiple batchers can share the same queue (e.g., for GPU serialization),
//! and each batcher has its own runner function. When a batcher creates a batch,
//! that batch carries the batcher's runner function.

use crate::prelude::*;
use crate::runtime::{PyAsyncContext, PyCallback};

use async_trait::async_trait;
use cocoindex_utils::batching::{BatchQueue, Batcher, BatchingOptions, Runner};
use pyo3_async_runtimes::tokio::future_into_py;

/// Options for batching behavior.
#[pyclass(name = "BatchingOptions")]
#[derive(Clone)]
pub struct PyBatchingOptions {
    #[pyo3(get, set)]
    pub max_batch_size: Option<usize>,
}

#[pymethods]
impl PyBatchingOptions {
    #[new]
    #[pyo3(signature = (max_batch_size=None))]
    pub fn new(max_batch_size: Option<usize>) -> Self {
        Self { max_batch_size }
    }
}

impl From<PyBatchingOptions> for BatchingOptions {
    fn from(opts: PyBatchingOptions) -> Self {
        BatchingOptions {
            max_batch_size: opts.max_batch_size,
        }
    }
}

/// A runner implementation that wraps a Python callback.
pub struct PyRunner {
    callback: PyCallback,
    async_ctx: PyAsyncContext,
}

#[async_trait]
impl Runner for PyRunner {
    type Input = Py<PyAny>;
    type Output = Py<PyAny>;

    async fn run(
        &self,
        inputs: Vec<Py<PyAny>>,
    ) -> cocoindex_utils::error::Result<impl ExactSizeIterator<Item = Py<PyAny>>> {
        // error!("PyRunner::run() call with input size: {}", inputs.len());
        // Convert inputs to a Python list (as Py<PyAny> which is Send)
        let py_list: Py<PyAny> = Python::attach(|py| {
            pyo3::types::PyList::new(py, inputs.iter().map(|p| p.bind(py)))
                .map(|list| list.into_any().unbind())
        })
        .from_py_result()?;

        // error!(
        //     "PyRunner::run() py_list created with input size: {}",
        //     inputs.len()
        // );
        // Call the callback
        let result_fut = self.callback.call(&self.async_ctx, (py_list,))?;
        // error!(
        //     "PyRunner::run() callback call created with input size: {}",
        //     inputs.len()
        // );
        let result = result_fut.await?;

        // error!(
        //     "PyRunner::run() call done with input size: {}",
        //     inputs.len()
        // );
        // Extract outputs from the returned list
        Python::attach(|py| {
            let outputs: Vec<Py<PyAny>> = result.extract(py).from_py_result()?;
            Ok(outputs.into_iter())
        })
    }
}

/// A shared queue that processes batches in FIFO order.
///
/// Multiple batchers can share the same queue. Each batcher has its own runner
/// function, and batches are processed using the runner from the batcher that
/// created them.
#[pyclass(name = "BatchQueue")]
pub struct PyBatchQueue {
    inner: Arc<BatchQueue<PyRunner>>,
}

#[pymethods]
impl PyBatchQueue {
    /// Create a new batch queue.
    ///
    /// The queue is shared among batchers. Each batcher provides its own runner
    /// function when created. Processing happens on-demand when items are added
    /// (no dedicated worker loop).
    #[new]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(BatchQueue::new()),
        }
    }
}

/// A batcher that collects inputs and submits them to a shared queue.
///
/// Each batcher maintains at most one non-full, non-sealed batch in the queue.
/// When inputs are submitted, they are added to the current batch or a new batch is created.
///
/// Multiple batchers can share the same queue with different runner functions.
/// Each batch uses the runner function from the batcher that created it.
#[pyclass(name = "Batcher")]
pub struct PyBatcher {
    inner: Arc<Batcher<PyRunner>>,
}

#[pymethods]
impl PyBatcher {
    /// Create a batcher with a sync runner function that uses the given shared queue.
    ///
    /// The runner function should take a list of inputs and return a list of outputs.
    /// This batcher's batches will use this runner function when processed.
    #[staticmethod]
    pub fn new_sync(
        queue: &PyBatchQueue,
        options: PyBatchingOptions,
        runner_fn: Py<PyAny>,
        async_ctx: PyAsyncContext,
    ) -> Self {
        let callback = PyCallback::Sync(Arc::new(runner_fn));
        let runner = PyRunner {
            callback,
            async_ctx,
        };

        Self {
            inner: Arc::new(Batcher::new(runner, queue.inner.clone(), options.into())),
        }
    }

    /// Create a batcher with an async runner function that uses the given shared queue.
    ///
    /// The runner function should take a list of inputs and return a list of outputs.
    /// This batcher's batches will use this runner function when processed.
    #[staticmethod]
    pub fn new_async(
        queue: &PyBatchQueue,
        options: PyBatchingOptions,
        runner_fn: Py<PyAny>,
        async_ctx: PyAsyncContext,
    ) -> Self {
        let callback = PyCallback::Async(Arc::new(runner_fn));
        let runner = PyRunner {
            callback,
            async_ctx,
        };

        Self {
            inner: Arc::new(Batcher::new(runner, queue.inner.clone(), options.into())),
        }
    }

    /// Submit an input and wait for the result asynchronously.
    ///
    /// Returns a Python awaitable that resolves to the output.
    /// Callers must always await the result.
    pub fn run<'py>(&self, py: Python<'py>, input: Py<PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let batcher = self.inner.clone();
        future_into_py(py, async move {
            // error!("PyBatcher::run() call");
            let result = batcher.run(input).await.into_py_result()?;
            Ok(result)
        })
    }
}
