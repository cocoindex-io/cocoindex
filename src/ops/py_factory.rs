use std::sync::Arc;

use axum::async_trait;
use blocking::unblock;
use futures::FutureExt;
use pyo3::{
    exceptions::PyException,
    pyclass, pymethods,
    types::{IntoPyDict, PyAnyMethods, PyString, PyTuple},
    Bound, IntoPyObjectExt, Py, PyAny, PyResult, Python,
};

use crate::{
    base::{schema, value},
    builder::plan,
};
use anyhow::Result;

use super::sdk::{
    ExecutorFuture, FlowInstanceContext, SimpleFunctionExecutor, SimpleFunctionFactory,
};

fn basic_value_to_py_object<'py>(
    py: Python<'py>,
    v: &value::BasicValue,
) -> PyResult<Bound<'py, PyAny>> {
    let result = match v {
        value::BasicValue::Bytes(v) => v.into_bound_py_any(py)?,
        value::BasicValue::Str(v) => v.into_bound_py_any(py)?,
        value::BasicValue::Bool(v) => v.into_bound_py_any(py)?,
        value::BasicValue::Int64(v) => v.into_bound_py_any(py)?,
        value::BasicValue::Float32(v) => v.into_bound_py_any(py)?,
        value::BasicValue::Float64(v) => v.into_bound_py_any(py)?,
        value::BasicValue::Vector(v) => v
            .iter()
            .map(|v| basic_value_to_py_object(py, v))
            .collect::<PyResult<Vec<_>>>()?
            .into_bound_py_any(py)?,
        _ => {
            return Err(PyException::new_err(format!(
                "unsupported value type: {}",
                v.kind()
            )))
        }
    };
    Ok(result)
}

fn value_to_py_object<'py>(py: Python<'py>, v: &value::Value) -> PyResult<Bound<'py, PyAny>> {
    let result = match v {
        value::Value::Null => py.None().into_bound(py),
        value::Value::Basic(v) => basic_value_to_py_object(py, v)?,
        _ => {
            return Err(PyException::new_err(format!(
                "unsupported value type: {}",
                v.kind()
            )))
        }
    };
    Ok(result)
}

fn basic_value_from_py_object<'py>(
    typ: &schema::BasicValueType,
    v: &Bound<'py, PyAny>,
) -> PyResult<value::BasicValue> {
    let result = match typ {
        schema::BasicValueType::Bytes => {
            value::BasicValue::Bytes(Arc::from(v.extract::<Vec<u8>>()?))
        }
        schema::BasicValueType::Str => value::BasicValue::Str(Arc::from(v.extract::<String>()?)),
        schema::BasicValueType::Bool => value::BasicValue::Bool(v.extract::<bool>()?),
        schema::BasicValueType::Int64 => value::BasicValue::Int64(v.extract::<i64>()?),
        schema::BasicValueType::Float32 => value::BasicValue::Float32(v.extract::<f32>()?),
        schema::BasicValueType::Float64 => value::BasicValue::Float64(v.extract::<f64>()?),
        schema::BasicValueType::Vector(elem) => value::BasicValue::Vector(Arc::from(
            v.extract::<Vec<Bound<'py, PyAny>>>()?
                .into_iter()
                .map(|v| basic_value_from_py_object(&elem.element_type, &v))
                .collect::<PyResult<Vec<_>>>()?,
        )),
        _ => {
            return Err(PyException::new_err(format!(
                "unsupported value type: {}",
                typ
            )))
        }
    };
    Ok(result)
}

fn value_from_py_object<'py>(
    typ: &schema::ValueType,
    v: &Bound<'py, PyAny>,
) -> PyResult<value::Value> {
    let result = if v.is_none() {
        value::Value::Null
    } else {
        match typ {
            schema::ValueType::Basic(typ) => {
                value::Value::Basic(basic_value_from_py_object(typ, v)?)
            }
            _ => {
                return Err(PyException::new_err(format!(
                    "unsupported value type: {}",
                    typ
                )))
            }
        }
    };
    Ok(result)
}

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

    fn validate_arg(
        &self,
        name: &str,
        typ: crate::py::Pythonized<schema::EnrichedValueType>,
    ) -> PyResult<()> {
        if self.value_type.0.typ != typ.0.typ {
            return Err(PyException::new_err(format!(
                "argument `{}` type mismatch, input type: {}, argument type: {}",
                name, self.value_type.0.typ, typ.0.typ
            )));
        }
        Ok(())
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
        unblock(move || {
            Python::with_gil(|py| -> Result<_> {
                let mut args = Vec::with_capacity(self.num_positional_args);
                for v in input[0..self.num_positional_args].iter() {
                    args.push(value_to_py_object(py, v)?);
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
                        kwargs.push((name.bind(py), value_to_py_object(py, v)?));
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

                Ok(value_from_py_object(
                    &self.result_type.typ,
                    result.bind(py),
                )?)
            })
        })
        .await
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
                let mut args = vec![crate::py::Pythonized(spec).into_py_any(py)?];
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
                            args.push(py_arg_schema.into_py_any(py)?);
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
            unblock(move || {
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
        };

        Ok((result_type, executor_fut.boxed()))
    }
}
