use std::sync::Arc;

use axum::async_trait;
use futures::FutureExt;
use pyo3::{
    pyclass, pymethods,
    types::{IntoPyDict, PyString, PyTuple},
    IntoPyObjectExt, Py, PyAny, Python,
};
use pythonize::pythonize;

use crate::{
    base::{schema, value},
    builder::plan,
    py,
};
use anyhow::Result;

use super::sdk::{
    ExecutorFuture, FlowInstanceContext, SimpleFunctionExecutor, SimpleFunctionFactory,
};

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
    num_positional_args: usize,
    kw_args_names: Vec<Py<PyString>>,
    result_type: schema::EnrichedValueType,

    enable_cache: bool,
    behavior_version: Option<u32>,
}

#[async_trait]
impl SimpleFunctionExecutor for Arc<PyFunctionExecutor> {
    async fn evaluate(&self, input: Vec<value::Value>) -> Result<value::Value> {
        let self = self.clone();
        let result = tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| -> Result<_> {
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

                let result = self.py_function_executor.call(
                    py,
                    PyTuple::new(py, args.into_iter())?,
                    kwargs
                        .map(|kwargs| -> Result<_> { Ok(kwargs.into_py_dict(py)?) })
                        .transpose()?
                        .as_ref(),
                )?;

                Ok(py::value_from_py_object(
                    &self.result_type.typ,
                    result.bind(py),
                )?)
            })
        })
        .await??;
        Ok(result)
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
        _context: Arc<FlowInstanceContext>,
    ) -> Result<(
        schema::EnrichedValueType,
        ExecutorFuture<'static, Box<dyn SimpleFunctionExecutor>>,
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
                let result = self.py_function_factory.call(
                    py,
                    PyTuple::new(py, args.into_iter())?,
                    Some(&kwargs.into_py_dict(py)?),
                )?;
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
                let executor = tokio::task::spawn_blocking(move || -> Result<_> {
                    let (enable_cache, behavior_version) =
                        Python::with_gil(|py| -> anyhow::Result<_> {
                            executor.call_method(py, "prepare", (), None)?;
                            let enable_cache = executor
                                .call_method(py, "enable_cache", (), None)?
                                .extract::<bool>(py)?;
                            let behavior_version = executor
                                .call_method(py, "behavior_version", (), None)?
                                .extract::<Option<u32>>(py)?;
                            Ok((enable_cache, behavior_version))
                        })?;
                    Ok(Box::new(Arc::new(PyFunctionExecutor {
                        py_function_executor: executor,
                        num_positional_args,
                        kw_args_names,
                        result_type,
                        enable_cache,
                        behavior_version,
                    })) as Box<dyn SimpleFunctionExecutor>)
                })
                .await??;
                Ok(executor)
            }
        };

        Ok((result_type, executor_fut.boxed()))
    }
}
