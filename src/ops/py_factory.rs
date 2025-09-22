use crate::prelude::*;

use pyo3::{
    IntoPyObjectExt, Py, PyAny, Python, pyclass, pymethods,
    types::{IntoPyDict, PyList, PyString, PyTuple},
};
use pythonize::{depythonize, pythonize};

use crate::{
    base::{schema, value},
    builder::plan,
    ops::sdk::SetupStateCompatibility,
    py::{self, ToResultWithPyTrace},
};
use anyhow::{Result, anyhow};

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
            .to_result_with_py_trace(py)?;
        Ok(result.into_bound(py))
    }
}

#[async_trait]
impl interface::SimpleFunctionExecutor for Arc<PyFunctionExecutor> {
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
            let result = result.to_result_with_py_trace(py)?;
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

#[async_trait]
impl interface::SimpleFunctionFactory for PyFunctionFactory {
    async fn build(
        self: Arc<Self>,
        spec: serde_json::Value,
        input_schema: Vec<schema::OpArgSchema>,
        context: Arc<interface::FlowInstanceContext>,
    ) -> Result<(
        schema::EnrichedValueType,
        BoxFuture<'static, Result<Box<dyn interface::SimpleFunctionExecutor>>>,
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
                    .to_result_with_py_trace(py)?;
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
                            .to_result_with_py_trace(py)?;
                        let prepare_fut = pyo3_async_runtimes::into_future_with_locals(
                            &pyo3_async_runtimes::TaskLocals::new(
                                py_exec_ctx.event_loop.bind(py).clone(),
                            ),
                            prepare_coro.into_bound(py),
                        )?;
                        let enable_cache = executor
                            .call_method(py, "enable_cache", (), None)
                            .to_result_with_py_trace(py)?
                            .extract::<bool>(py)?;
                        let behavior_version = executor
                            .call_method(py, "behavior_version", (), None)
                            .to_result_with_py_trace(py)?
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
                }))
                    as Box<dyn interface::SimpleFunctionExecutor>)
            }
        };

        Ok((result_type, executor_fut.boxed()))
    }
}

pub(crate) struct PyExportTargetFactory {
    pub py_target_connector: Py<PyAny>,
}

struct PyTargetExecutorContext {
    py_export_ctx: Py<PyAny>,
    py_exec_ctx: Arc<crate::py::PythonExecutionContext>,
}

#[derive(Debug)]
struct PyTargetResourceSetupChange {
    stale_existing_states: IndexSet<Option<serde_json::Value>>,
    desired_state: Option<serde_json::Value>,
}

impl setup::ResourceSetupChange for PyTargetResourceSetupChange {
    fn describe_changes(&self) -> Vec<setup::ChangeDescription> {
        vec![]
    }

    fn change_type(&self) -> setup::SetupChangeType {
        if self.stale_existing_states.is_empty() {
            setup::SetupChangeType::NoChange
        } else if self.desired_state.is_some() {
            if self
                .stale_existing_states
                .iter()
                .any(|state| state.is_none())
            {
                setup::SetupChangeType::Create
            } else {
                setup::SetupChangeType::Update
            }
        } else {
            setup::SetupChangeType::Delete
        }
    }
}

#[async_trait]
impl interface::TargetFactory for PyExportTargetFactory {
    async fn build(
        self: Arc<Self>,
        data_collections: Vec<interface::ExportDataCollectionSpec>,
        declarations: Vec<serde_json::Value>,
        context: Arc<interface::FlowInstanceContext>,
    ) -> Result<(
        Vec<interface::ExportDataCollectionBuildOutput>,
        Vec<(serde_json::Value, serde_json::Value)>,
    )> {
        if declarations.len() != 0 {
            api_error!("Custom target connector doesn't support declarations yet");
        }

        let mut build_outputs = Vec::with_capacity(data_collections.len());
        let py_exec_ctx = context
            .py_exec_ctx
            .as_ref()
            .ok_or_else(|| anyhow!("Python execution context is missing"))?
            .clone();
        for data_collection in data_collections.into_iter() {
            let (py_export_ctx, persistent_key, setup_state) = Python::with_gil(|py| {
                // Deserialize the spec to Python object.
                let py_export_ctx = self
                    .py_target_connector
                    .call_method(
                        py,
                        "create_export_context",
                        (
                            &data_collection.name,
                            pythonize(py, &data_collection.spec)?,
                            pythonize(py, &data_collection.key_fields_schema)?,
                            pythonize(py, &data_collection.value_fields_schema)?,
                            pythonize(py, &data_collection.index_options)?,
                        ),
                        None,
                    )
                    .to_result_with_py_trace(py)?;

                // Call the `get_persistent_key` method to get the persistent key.
                let persistent_key = self
                    .py_target_connector
                    .call_method(py, "get_persistent_key", (&py_export_ctx,), None)
                    .to_result_with_py_trace(py)?;
                let persistent_key: serde_json::Value =
                    depythonize(&persistent_key.into_bound(py))?;

                let setup_state = self
                    .py_target_connector
                    .call_method(py, "get_setup_state", (&py_export_ctx,), None)
                    .to_result_with_py_trace(py)?;
                let setup_state: serde_json::Value = depythonize(&setup_state.into_bound(py))?;

                anyhow::Ok((py_export_ctx, persistent_key, setup_state))
            })?;

            let factory = self.clone();
            let py_exec_ctx = py_exec_ctx.clone();
            let build_output = interface::ExportDataCollectionBuildOutput {
                export_context: Box::pin(async move {
                    Python::with_gil(|py| {
                        let prepare_coro = factory
                            .py_target_connector
                            .call_method(py, "prepare_async", (&py_export_ctx,), None)
                            .to_result_with_py_trace(py)?;
                        let task_locals = pyo3_async_runtimes::TaskLocals::new(
                            py_exec_ctx.event_loop.bind(py).clone(),
                        );
                        anyhow::Ok(pyo3_async_runtimes::into_future_with_locals(
                            &task_locals,
                            prepare_coro.into_bound(py),
                        )?)
                    })?
                    .await?;
                    anyhow::Ok(Arc::new(PyTargetExecutorContext {
                        py_export_ctx,
                        py_exec_ctx,
                    }) as Arc<dyn Any + Send + Sync>)
                }),
                setup_key: persistent_key,
                desired_setup_state: setup_state,
            };
            build_outputs.push(build_output);
        }
        Ok((build_outputs, vec![]))
    }

    async fn diff_setup_states(
        &self,
        _key: &serde_json::Value,
        desired_state: Option<serde_json::Value>,
        existing_states: setup::CombinedState<serde_json::Value>,
        _context: Arc<interface::FlowInstanceContext>,
    ) -> Result<Box<dyn setup::ResourceSetupChange>> {
        // Collect all possible existing states that are not the desired state.
        let mut stale_existing_states = IndexSet::new();
        if !existing_states.always_exists() && desired_state.is_some() {
            stale_existing_states.insert(None);
        }
        for possible_state in existing_states.possible_versions() {
            if Some(possible_state) != desired_state.as_ref() {
                stale_existing_states.insert(Some(possible_state.clone()));
            }
        }

        Ok(Box::new(PyTargetResourceSetupChange {
            stale_existing_states,
            desired_state,
        }))
    }

    fn normalize_setup_key(&self, key: &serde_json::Value) -> Result<serde_json::Value> {
        Ok(key.clone())
    }

    fn check_state_compatibility(
        &self,
        desired_state: &serde_json::Value,
        existing_state: &serde_json::Value,
    ) -> Result<SetupStateCompatibility> {
        let compatibility = Python::with_gil(|py| -> Result<_> {
            let result = self
                .py_target_connector
                .call_method(
                    py,
                    "check_state_compatibility",
                    (
                        pythonize(py, desired_state)?,
                        pythonize(py, existing_state)?,
                    ),
                    None,
                )
                .to_result_with_py_trace(py)?;
            let compatibility: SetupStateCompatibility = depythonize(&result.into_bound(py))?;
            Ok(compatibility)
        })?;
        Ok(compatibility)
    }

    fn describe_resource(&self, key: &serde_json::Value) -> Result<String> {
        Python::with_gil(|py| -> Result<String> {
            let result = self
                .py_target_connector
                .call_method(py, "describe_resource", (pythonize(py, key)?,), None)
                .to_result_with_py_trace(py)?;
            let description = result.extract::<String>(py)?;
            Ok(description)
        })
    }

    fn extract_additional_key(
        &self,
        _key: &value::KeyValue,
        _value: &value::FieldValues,
        _export_context: &(dyn Any + Send + Sync),
    ) -> Result<serde_json::Value> {
        Ok(serde_json::Value::Null)
    }

    async fn apply_setup_changes(
        &self,
        setup_change: Vec<interface::ResourceSetupChangeItem<'async_trait>>,
        context: Arc<interface::FlowInstanceContext>,
    ) -> Result<()> {
        // Filter the setup changes that are not NoChange, and flatten to
        //   `list[tuple[key, list[stale_existing_states | None], desired_state | None]]` for Python.
        let mut setup_changes = Vec::new();
        for item in setup_change.into_iter() {
            let decoded_setup_change = (item.setup_change as &dyn Any)
                .downcast_ref::<PyTargetResourceSetupChange>()
                .ok_or_else(invariance_violation)?;
            if <dyn setup::ResourceSetupChange>::change_type(decoded_setup_change)
                != setup::SetupChangeType::NoChange
            {
                setup_changes.push((
                    item.key,
                    &decoded_setup_change.stale_existing_states,
                    &decoded_setup_change.desired_state,
                ));
            }
        }

        if setup_changes.is_empty() {
            return Ok(());
        }

        // Call the `apply_setup_changes_async()` method.
        let py_exec_ctx = context
            .py_exec_ctx
            .as_ref()
            .ok_or_else(|| anyhow!("Python execution context is missing"))?
            .clone();
        let py_result = Python::with_gil(move |py| -> Result<_> {
            let result_coro = self
                .py_target_connector
                .call_method(
                    py,
                    "apply_setup_changes_async",
                    (pythonize(py, &setup_changes)?,),
                    None,
                )
                .to_result_with_py_trace(py)?;
            let task_locals =
                pyo3_async_runtimes::TaskLocals::new(py_exec_ctx.event_loop.bind(py).clone());
            Ok(pyo3_async_runtimes::into_future_with_locals(
                &task_locals,
                result_coro.into_bound(py),
            )?)
        })?
        .await;
        Python::with_gil(move |py| py_result.to_result_with_py_trace(py))?;

        Ok(())
    }

    async fn apply_mutation(
        &self,
        mutations: Vec<
            interface::ExportTargetMutationWithContext<'async_trait, dyn Any + Send + Sync>,
        >,
    ) -> Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }

        let py_result = Python::with_gil(|py| -> Result<_> {
            // Create a `list[tuple[export_ctx, list[tuple[key, value | None]]]]` for Python, and collect `py_exec_ctx`.
            let mut py_args = Vec::with_capacity(mutations.len());
            let mut py_exec_ctx: Option<&Arc<crate::py::PythonExecutionContext>> = None;
            for mutation in mutations.into_iter() {
                // Downcast export_context to PyTargetExecutorContext.
                let export_context = (mutation.export_context as &dyn Any)
                    .downcast_ref::<PyTargetExecutorContext>()
                    .ok_or_else(invariance_violation)?;

                let mut flattened_mutations = Vec::with_capacity(
                    mutation.mutation.upserts.len() + mutation.mutation.deletes.len(),
                );
                for upsert in mutation.mutation.upserts.into_iter() {
                    flattened_mutations.push((
                        py::key_to_py_object(py, &upsert.key)?,
                        py::field_values_to_py_object(py, upsert.value.fields.iter())?,
                    ));
                }
                for delete in mutation.mutation.deletes.into_iter() {
                    flattened_mutations.push((
                        py::key_to_py_object(py, &delete.key)?,
                        py.None().into_bound(py),
                    ));
                }
                py_args.push((
                    &export_context.py_export_ctx,
                    PyList::new(py, flattened_mutations)?.into_any(),
                ));
                py_exec_ctx = py_exec_ctx.or(Some(&export_context.py_exec_ctx));
            }
            let py_exec_ctx = py_exec_ctx.ok_or_else(invariance_violation)?;

            let result_coro = self
                .py_target_connector
                .call_method(py, "mutate_async", (py_args,), None)
                .to_result_with_py_trace(py)?;
            let task_locals =
                pyo3_async_runtimes::TaskLocals::new(py_exec_ctx.event_loop.bind(py).clone());
            Ok(pyo3_async_runtimes::into_future_with_locals(
                &task_locals,
                result_coro.into_bound(py),
            )?)
        })?
        .await;

        Python::with_gil(move |py| py_result.to_result_with_py_trace(py))?;
        Ok(())
    }
}
