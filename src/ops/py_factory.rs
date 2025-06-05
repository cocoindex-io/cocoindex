use std::sync::Arc;

use async_trait::async_trait;
use futures::{FutureExt, future::BoxFuture};
use pyo3::{
    IntoPyObjectExt, Py, PyAny, Python, pyclass, pymethods,
    types::{IntoPyDict, PyString, PyTuple},
};
use pythonize::pythonize;

use crate::{
    base::{schema, value},
    builder::plan,
    py::{self, FromPyResult},
};
use anyhow::{Result, anyhow};

use super::interface::{FlowInstanceContext, SimpleFunctionExecutor, SimpleFunctionFactory};

#[pyclass(name = "OpArgSchema")]
pub struct PyOpArgSchema {
    value_type: crate::py::Pythonized<schema::EnrichedValueType>,
    analyzed_value: crate::py::Pythonized<plan::AnalyzedValueMapping>,
}

#[pymethods]
impl PyOpArgSchema {
    #[getter]
    fn value_type(&self) -> &crate::py::Pythonized<schema::EnrichedValueType> {
        &self.value_type
    }

    #[getter]
    fn analyzed_value(&self) -> &crate::py::Pythonized<plan::AnalyzedValueMapping> {
        &self.analyzed_value
    }
}

struct PyFunctionExecutor {
    py_function_executor: Py<PyAny>,
    py_exec_ctx: Arc<crate::py::PythonExecutionContext>,

    num_positional_args: usize,
    kw_args_names: Vec<Py<PyString>>,
    result_type: schema::EnrichedValueType,

    enable_cache: bool,
    behavior_version: Option<u32>,
}

impl PyFunctionExecutor {
    fn call_py_fn<'py>(
        &self,
        py: Python<'py>,
        input: Vec<value::Value>,
    ) -> Result<pyo3::Bound<'py, pyo3::PyAny>> {
        let mut args = Vec::with_capacity(self.num_positional_args);
        for v in input[0..self.num_positional_args].iter() {
            args.push(py::value_to_py_object(py, v)?);
        }

        let kwargs = if self.kw_args_names.is_empty() {
            None
        } else {
            let mut kwargs = Vec::with_capacity(self.kw_args_names.len());
            for (name, v) in self
                .kw_args_names
                .iter()
                .zip(input[self.num_positional_args..].iter())
            {
                kwargs.push((name.bind(py), py::value_to_py_object(py, v)?));
            }
            Some(kwargs)
        };

        let result = self
            .py_function_executor
            .call(
                py,
                PyTuple::new(py, args.into_iter())?,
                kwargs
                    .map(|kwargs| -> Result<_> { Ok(kwargs.into_py_dict(py)?) })
                    .transpose()?
                    .as_ref(),
            )
            .from_py_result(py)?;
        Ok(result.into_bound(py))
    }
}

#[async_trait]
impl SimpleFunctionExecutor for Arc<PyFunctionExecutor> {
    async fn evaluate(&self, input: Vec<value::Value>) -> Result<value::Value> {
        let self = self.clone();
        let result_fut = Python::with_gil(|py| -> Result<_> {
            let result_coro = self.call_py_fn(py, input)?;
            let task_locals =
                pyo3_async_runtimes::TaskLocals::new(self.py_exec_ctx.event_loop.bind(py).clone());
            Ok(pyo3_async_runtimes::into_future_with_locals(
                &task_locals,
                result_coro,
            )?)
        })?;
        let result = result_fut.await;
        Python::with_gil(|py| -> Result<_> {
            let result = result.from_py_result(py)?;
            Ok(py::value_from_py_object(
                &self.result_type.typ,
                &result.into_bound(py),
            )?)
        })
    }

    fn enable_cache(&self) -> bool {
        self.enable_cache
    }

    fn behavior_version(&self) -> Option<u32> {
        self.behavior_version
    }
}

pub(crate) struct PyFunctionFactory {
    pub py_function_factory: Py<PyAny>,
}

impl SimpleFunctionFactory for PyFunctionFactory {
    fn build(
        self: Arc<Self>,
        spec: serde_json::Value,
        input_schema: Vec<schema::OpArgSchema>,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        schema::EnrichedValueType,
        BoxFuture<'static, Result<Box<dyn SimpleFunctionExecutor>>>,
    )> {
        let (result_type, executor, kw_args_names, num_positional_args) =
            Python::with_gil(|py| -> anyhow::Result<_> {
                let mut args = vec![pythonize(py, &spec)?];
                let mut kwargs = vec![];
                let mut num_positional_args = 0;
                for arg in input_schema.into_iter() {
                    let py_arg_schema = PyOpArgSchema {
                        value_type: crate::py::Pythonized(arg.value_type.clone()),
                        analyzed_value: crate::py::Pythonized(arg.analyzed_value.clone()),
                    };
                    match arg.name.0 {
                        Some(name) => {
                            kwargs.push((name.clone(), py_arg_schema));
                        }
                        None => {
                            args.push(py_arg_schema.into_bound_py_any(py)?);
                            num_positional_args += 1;
                        }
                    }
                }

                let kw_args_names = kwargs
                    .iter()
                    .map(|(name, _)| PyString::new(py, name).unbind())
                    .collect::<Vec<_>>();
                let result = self
                    .py_function_factory
                    .call(
                        py,
                        PyTuple::new(py, args.into_iter())?,
                        Some(&kwargs.into_py_dict(py)?),
                    )
                    .from_py_result(py)?;
                let (result_type, executor) = result
                    .extract::<(crate::py::Pythonized<schema::EnrichedValueType>, Py<PyAny>)>(py)?;
                Ok((
                    result_type.into_inner(),
                    executor,
                    kw_args_names,
                    num_positional_args,
                ))
            })?;

        let executor_fut = {
            let result_type = result_type.clone();
            async move {
                let py_exec_ctx = context
                    .py_exec_ctx
                    .as_ref()
                    .ok_or_else(|| anyhow!("Python execution context is missing"))?
                    .clone();
                let (prepare_fut, enable_cache, behavior_version) =
                    Python::with_gil(|py| -> anyhow::Result<_> {
                        let prepare_coro = executor
                            .call_method(py, "prepare", (), None)
                            .from_py_result(py)?;
                        let prepare_fut = pyo3_async_runtimes::into_future_with_locals(
                            &pyo3_async_runtimes::TaskLocals::new(
                                py_exec_ctx.event_loop.bind(py).clone(),
                            ),
                            prepare_coro.into_bound(py),
                        )?;
                        let enable_cache = executor
                            .call_method(py, "enable_cache", (), None)
                            .from_py_result(py)?
                            .extract::<bool>(py)?;
                        let behavior_version = executor
                            .call_method(py, "behavior_version", (), None)
                            .from_py_result(py)?
                            .extract::<Option<u32>>(py)?;
                        Ok((prepare_fut, enable_cache, behavior_version))
                    })?;
                prepare_fut.await?;
                Ok(Box::new(Arc::new(PyFunctionExecutor {
                    py_function_executor: executor,
                    py_exec_ctx,
                    num_positional_args,
                    kw_args_names,
                    result_type,
                    enable_cache,
                    behavior_version,
                })) as Box<dyn SimpleFunctionExecutor>)
            }
        };

        Ok((result_type, executor_fut.boxed()))
    }
}
