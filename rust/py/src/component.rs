use crate::{prelude::*, runtime::PyAsyncContext};

use cocoindex_core::engine::{
    component::ComponentBuilder, context::ComponentBuilderContext, runtime::get_runtime,
};
use cocoindex_py_utils::from_py_future;
use tokio_util::task::AbortOnDropHandle;

use crate::context::PyComponentBuilderContext;

#[pyclass(name = "ComponentBuilder")]
#[derive(Clone)]
pub struct PyComponentBuilder {
    builder_fn: PyComponentBuilderFn,
}

#[derive(Clone)]
enum PyComponentBuilderFn {
    Sync(Arc<Py<PyAny>>),
    Async {
        builder_fn: Arc<Py<PyAny>>,
        async_context: PyAsyncContext,
    },
}

#[pymethods]
impl PyComponentBuilder {
    #[staticmethod]
    pub fn new_sync(builder_fn: Py<PyAny>) -> Self {
        Self {
            builder_fn: PyComponentBuilderFn::Sync(Arc::new(builder_fn)),
        }
    }

    #[staticmethod]
    pub fn new_async<'py>(builder_fn: Py<PyAny>, async_context: PyAsyncContext) -> Self {
        Self {
            builder_fn: PyComponentBuilderFn::Async {
                builder_fn: Arc::new(builder_fn),
                async_context,
            },
        }
    }
}

impl ComponentBuilder for PyComponentBuilder {
    type HostStateCtx = Arc<Py<PyAny>>;
    type BuildRet = Py<PyAny>;
    type BuildErr = PyErr;

    async fn build(
        &self,
        context: &Arc<ComponentBuilderContext>,
    ) -> Result<Result<Self::BuildRet, Self::BuildErr>> {
        let py_context = PyComponentBuilderContext(context.clone());
        let ret = match &self.builder_fn {
            PyComponentBuilderFn::Sync(builder_fn) => {
                let builder_fn = builder_fn.clone();
                let result_fut = AbortOnDropHandle::new(get_runtime().spawn_blocking(move || {
                    Python::with_gil(|py| builder_fn.call(py, (py_context,), None))
                }));
                result_fut.await?
            }
            PyComponentBuilderFn::Async {
                builder_fn,
                async_context,
            } => {
                let builder_fn = builder_fn.clone();
                let result_fut = Python::with_gil(|py| {
                    let result_coro = builder_fn.call(py, (py_context,), None)?;
                    from_py_future(py, &async_context.0, result_coro.into_bound(py))
                })?;
                result_fut.await
            }
        };
        Ok(ret)
    }
}
