use crate::execution::evaluator::evaluate_transient_flow;
use crate::prelude::*;

use crate::base::schema::{FieldSchema, ValueType};
use crate::base::spec::{AuthEntryReference, NamedSpec, OutputMode, ReactiveOpSpec, SpecFormatter};
use crate::lib_context::{
    QueryHandlerContext, clear_lib_context, get_auth_registry, init_lib_context,
};
use crate::ops::py_factory::{PyExportTargetFactory, PyOpArgSchema, PySourceConnectorFactory};
use crate::ops::{interface::ExecutorFactory, py_factory::PyFunctionFactory, register_factory};
use crate::server::{self, ServerSettings};
use crate::service::query_handler::QueryHandlerSpec;
use crate::settings::Settings;
use crate::setup::{self};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

mod convert;
pub(crate) use convert::*;
pub(crate) use py_utils::*;

#[pyfunction]
fn set_settings_fn(get_settings_fn: Py<PyAny>) -> PyResult<()> {
    let get_settings_closure = move || {
        Python::attach(|py| {
            let obj = get_settings_fn.bind(py).call0().from_py_result()?;
            let py_settings = obj.extract::<Pythonized<Settings>>().from_py_result()?;
            Ok::<_, Error>(py_settings.into_inner())
        })
    };
    crate::lib_context::set_settings_fn(Box::new(get_settings_closure));
    Ok(())
}

#[pyfunction]
fn init_pyo3_runtime() {
    pyo3_async_runtimes::tokio::init_with_runtime(get_runtime()).unwrap();
}

#[pyfunction]
fn init(py: Python<'_>, settings: Pythonized<Option<Settings>>) -> PyResult<()> {
    py.detach(|| -> Result<()> {
        get_runtime().block_on(async move { init_lib_context(settings.into_inner()).await })
    })
    .into_py_result()
}

#[pyfunction]
fn start_server(py: Python<'_>, settings: Pythonized<ServerSettings>) -> PyResult<()> {
    py.detach(|| -> Result<()> {
        let server = get_runtime().block_on(async move {
            server::init_server(get_lib_context().await?, settings.into_inner()).await
        })?;
        get_runtime().spawn(server);
        Ok(())
    })
    .into_py_result()
}

#[pyfunction]
fn stop(py: Python<'_>) -> PyResult<()> {
    py.detach(|| get_runtime().block_on(clear_lib_context()));
    Ok(())
}

#[pyfunction]
fn register_source_connector(name: String, py_source_connector: Py<PyAny>) -> PyResult<()> {
    let factory = PySourceConnectorFactory {
        py_source_connector,
    };
    register_factory(name, ExecutorFactory::Source(Arc::new(factory))).into_py_result()
}

#[pyfunction]
fn register_function_factory(name: String, py_function_factory: Py<PyAny>) -> PyResult<()> {
    let factory = PyFunctionFactory {
        py_function_factory,
    };
    register_factory(name, ExecutorFactory::SimpleFunction(Arc::new(factory))).into_py_result()
}

#[pyfunction]
fn register_target_connector(name: String, py_target_connector: Py<PyAny>) -> PyResult<()> {
    let factory = PyExportTargetFactory {
        py_target_connector,
    };
    register_factory(name, ExecutorFactory::ExportTarget(Arc::new(factory))).into_py_result()
}

#[pyclass]
pub struct IndexUpdateInfo(pub execution::stats::IndexUpdateInfo);

#[pymethods]
impl IndexUpdateInfo {
    pub fn __str__(&self) -> String {
        format!("{}", self.0)
    }

    pub fn __repr__(&self) -> String {
        self.__str__()
    }
}

#[pyclass]
pub struct Flow(pub Arc<FlowContext>);

/// A single line in the rendered spec, with hierarchical children
#[pyclass(get_all, set_all)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenderedSpecLine {
    /// The formatted content of the line (e.g., "Import: name=documents, source=LocalFile")
    pub content: String,
    /// Child lines in the hierarchy
    pub children: Vec<RenderedSpecLine>,
}

/// A rendered specification, grouped by sections
#[pyclass(get_all, set_all)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenderedSpec {
    /// List of (section_name, lines) pairs
    pub sections: Vec<(String, Vec<RenderedSpecLine>)>,
}

#[pyclass]
pub struct FlowLiveUpdaterUpdates(execution::FlowLiveUpdaterUpdates);

#[pymethods]
impl FlowLiveUpdaterUpdates {
    #[getter]
    pub fn active_sources(&self) -> Vec<String> {
        self.0.active_sources.clone()
    }

    #[getter]
    pub fn updated_sources(&self) -> Vec<String> {
        self.0.updated_sources.clone()
    }
}

#[pyclass]
pub struct FlowLiveUpdater(pub Arc<execution::FlowLiveUpdater>);

#[pymethods]
impl FlowLiveUpdater {
    #[staticmethod]
    pub fn create<'py>(
        py: Python<'py>,
        flow: &Flow,
        options: Pythonized<execution::FlowLiveUpdaterOptions>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let flow = flow.0.clone();
        future_into_py(py, async move {
            let lib_context = get_lib_context().await.into_py_result()?;
            let live_updater = execution::FlowLiveUpdater::start(
                flow,
                lib_context.require_builtin_db_pool().into_py_result()?,
                &lib_context.multi_progress_bar,
                options.into_inner(),
            )
            .await
            .into_py_result()?;
            Ok(Self(Arc::new(live_updater)))
        })
    }

    pub fn wait_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let live_updater = self.0.clone();
        future_into_py(
            py,
            async move { live_updater.wait().await.into_py_result() },
        )
    }

    pub fn next_status_updates_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let live_updater = self.0.clone();
        future_into_py(py, async move {
            let updates = live_updater.next_status_updates().await.into_py_result()?;
            Ok(FlowLiveUpdaterUpdates(updates))
        })
    }

    pub fn abort(&self) {
        self.0.abort();
    }

    pub fn index_update_info(&self) -> IndexUpdateInfo {
        IndexUpdateInfo(self.0.index_update_info())
    }
}

#[pymethods]
impl Flow {
    pub fn __str__(&self) -> String {
        serde_json::to_string_pretty(&self.0.flow.flow_instance).unwrap()
    }

    pub fn __repr__(&self) -> String {
        self.__str__()
    }

    pub fn name(&self) -> &str {
        &self.0.flow.flow_instance.name
    }

    pub fn evaluate_and_dump(
        &self,
        py: Python<'_>,
        options: Pythonized<execution::dumper::EvaluateAndDumpOptions>,
    ) -> PyResult<()> {
        py.detach(|| {
            get_runtime()
                .block_on(async {
                    let exec_plan = self.0.flow.get_execution_plan().await?;
                    let lib_context = get_lib_context().await?;
                    let execution_ctx = self.0.use_execution_ctx().await?;
                    execution::dumper::evaluate_and_dump(
                        &exec_plan,
                        &execution_ctx.setup_execution_context,
                        &self.0.flow.data_schema,
                        options.into_inner(),
                        lib_context.require_builtin_db_pool()?,
                    )
                    .await
                })
                .into_py_result()?;
            Ok(())
        })
    }

    #[pyo3(signature = (output_mode=None))]
    pub fn get_spec(&self, output_mode: Option<Pythonized<OutputMode>>) -> PyResult<RenderedSpec> {
        let mode = output_mode.map_or(OutputMode::Concise, |m| m.into_inner());
        let spec = &self.0.flow.flow_instance;
        let mut sections: IndexMap<String, Vec<RenderedSpecLine>> = IndexMap::new();

        // Sources
        sections.insert(
            "Source".to_string(),
            spec.import_ops
                .iter()
                .map(|op| RenderedSpecLine {
                    content: format!("Import: name={}, {}", op.name, op.spec.format(mode)),
                    children: vec![],
                })
                .collect(),
        );

        // Processing
        fn walk(op: &NamedSpec<ReactiveOpSpec>, mode: OutputMode) -> RenderedSpecLine {
            let content = format!("{}: {}", op.name, op.spec.format(mode));

            let children = match &op.spec {
                ReactiveOpSpec::ForEach(fe) => fe
                    .op_scope
                    .ops
                    .iter()
                    .map(|nested| walk(nested, mode))
                    .collect(),
                _ => vec![],
            };

            RenderedSpecLine { content, children }
        }

        sections.insert(
            "Processing".to_string(),
            spec.reactive_ops.iter().map(|op| walk(op, mode)).collect(),
        );

        // Targets
        sections.insert(
            "Targets".to_string(),
            spec.export_ops
                .iter()
                .map(|op| RenderedSpecLine {
                    content: format!("Export: name={}, {}", op.name, op.spec.format(mode)),
                    children: vec![],
                })
                .collect(),
        );

        // Declarations
        sections.insert(
            "Declarations".to_string(),
            spec.declarations
                .iter()
                .map(|decl| RenderedSpecLine {
                    content: format!("Declaration: {}", decl.format(mode)),
                    children: vec![],
                })
                .collect(),
        );

        Ok(RenderedSpec {
            sections: sections.into_iter().collect(),
        })
    }

    pub fn get_schema(&self) -> Vec<(String, String, String)> {
        let schema = &self.0.flow.data_schema;
        let mut result = Vec::new();

        fn process_fields(
            fields: &[FieldSchema],
            prefix: &str,
            result: &mut Vec<(String, String, String)>,
        ) {
            for field in fields {
                let field_name = format!("{}{}", prefix, field.name);

                let mut field_type = match &field.value_type.typ {
                    ValueType::Basic(basic) => format!("{basic}"),
                    ValueType::Table(t) => format!("{}", t.kind),
                    ValueType::Struct(_) => "Struct".to_string(),
                };

                if field.value_type.nullable {
                    field_type.push('?');
                }

                let attr_str = if field.value_type.attrs.is_empty() {
                    String::new()
                } else {
                    field
                        .value_type
                        .attrs
                        .keys()
                        .map(|k| k.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                };

                result.push((field_name.clone(), field_type, attr_str));

                match &field.value_type.typ {
                    ValueType::Struct(s) => {
                        process_fields(&s.fields, &format!("{field_name}."), result);
                    }
                    ValueType::Table(t) => {
                        process_fields(&t.row.fields, &format!("{field_name}[]."), result);
                    }
                    ValueType::Basic(_) => {}
                }
            }
        }

        process_fields(&schema.schema.fields, "", &mut result);
        result
    }

    pub fn make_setup_action(&self) -> SetupChangeBundle {
        let bundle = setup::SetupChangeBundle {
            action: setup::FlowSetupChangeAction::Setup,
            flow_names: vec![self.name().to_string()],
        };
        SetupChangeBundle(Arc::new(bundle))
    }

    pub fn make_drop_action(&self) -> SetupChangeBundle {
        let bundle = setup::SetupChangeBundle {
            action: setup::FlowSetupChangeAction::Drop,
            flow_names: vec![self.name().to_string()],
        };
        SetupChangeBundle(Arc::new(bundle))
    }

    pub fn add_query_handler(
        &self,
        name: String,
        handler: Py<PyAny>,
        handler_info: Pythonized<Option<QueryHandlerSpec>>,
    ) -> PyResult<()> {
        struct PyQueryHandler {
            handler: Py<PyAny>,
        }

        #[async_trait]
        impl crate::service::query_handler::QueryHandler for PyQueryHandler {
            async fn query(
                &self,
                input: crate::service::query_handler::QueryInput,
                flow_ctx: &interface::FlowInstanceContext,
            ) -> Result<crate::service::query_handler::QueryOutput> {
                // Call the Python async function on the flow's event loop
                let result_fut = Python::attach(|py| -> Result<_> {
                    let handler = self.handler.clone_ref(py);
                    // Build args: pass a dict with the query input
                    let args = pyo3::types::PyTuple::new(py, [input.query]).from_py_result()?;
                    let result_coro = handler.call(py, args, None).from_py_result()?;

                    let py_exec_ctx = flow_ctx
                        .py_exec_ctx
                        .as_ref()
                        .ok_or_else(|| internal_error!("Python execution context is missing"))?;
                    let task_locals = pyo3_async_runtimes::TaskLocals::new(
                        py_exec_ctx.event_loop.bind(py).clone(),
                    );
                    Ok(
                        py_utils::from_py_future(py, &task_locals, result_coro.into_bound(py))
                            .from_py_result()?,
                    )
                })?;

                let py_obj = result_fut.await;
                // Convert Python result to Rust type with proper traceback handling
                let output = Python::attach(|py| -> Result<_> {
                    let output_any = py_obj.from_py_result()?;
                    let output: crate::py::Pythonized<crate::service::query_handler::QueryOutput> =
                        output_any.extract(py).from_py_result()?;
                    Ok(output.into_inner())
                })?;

                Ok(output)
            }
        }

        let mut handlers = self.0.query_handlers.write().unwrap();
        handlers.insert(
            name,
            QueryHandlerContext {
                info: Arc::new(handler_info.into_inner().unwrap_or_default()),
                handler: Arc::new(PyQueryHandler { handler }),
            },
        );
        Ok(())
    }
}

#[pyclass]
pub struct TransientFlow(pub Arc<builder::AnalyzedTransientFlow>);

#[pymethods]
impl TransientFlow {
    pub fn __str__(&self) -> String {
        serde_json::to_string_pretty(&self.0.transient_flow_instance).unwrap()
    }

    pub fn __repr__(&self) -> String {
        self.__str__()
    }

    pub fn evaluate_async<'py>(
        &self,
        py: Python<'py>,
        args: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let flow = self.0.clone();
        let input_values: Vec<value::Value> = std::iter::zip(
            self.0.transient_flow_instance.input_fields.iter(),
            args.into_iter(),
        )
        .map(|(input_schema, arg)| value_from_py_object(&input_schema.value_type.typ, &arg))
        .collect::<PyResult<_>>()?;

        future_into_py(py, async move {
            let result = evaluate_transient_flow(&flow, &input_values)
                .await
                .into_py_result()?;
            Python::attach(|py| value_to_py_object(py, &result)?.into_py_any(py))
        })
    }
}

#[pyclass]
pub struct SetupChangeBundle(Arc<setup::SetupChangeBundle>);

#[pymethods]
impl SetupChangeBundle {
    pub fn describe_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let bundle = self.0.clone();
        future_into_py(py, async move {
            let lib_context = get_lib_context().await.into_py_result()?;
            bundle.describe(&lib_context).await.into_py_result()
        })
    }

    pub fn apply_async<'py>(
        &self,
        py: Python<'py>,
        report_to_stdout: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bundle = self.0.clone();

        future_into_py(py, async move {
            let lib_context = get_lib_context().await.into_py_result()?;
            let mut stdout = None;
            let mut sink = None;
            bundle
                .apply(
                    &lib_context,
                    if report_to_stdout {
                        stdout.insert(std::io::stdout())
                    } else {
                        sink.insert(std::io::sink())
                    },
                )
                .await
                .into_py_result()
        })
    }
}

#[pyfunction]
fn flow_names_with_setup_async(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        let lib_context = get_lib_context().await.into_py_result()?;
        let setup_ctx = lib_context
            .require_persistence_ctx()
            .into_py_result()?
            .setup_ctx
            .read()
            .await;
        let flow_names: Vec<String> = setup_ctx.all_setup_states.flows.keys().cloned().collect();
        PyResult::Ok(flow_names)
    })
}

#[pyfunction]
fn make_setup_bundle(flow_names: Vec<String>) -> PyResult<SetupChangeBundle> {
    let bundle = setup::SetupChangeBundle {
        action: setup::FlowSetupChangeAction::Setup,
        flow_names,
    };
    Ok(SetupChangeBundle(Arc::new(bundle)))
}

#[pyfunction]
fn make_drop_bundle(flow_names: Vec<String>) -> PyResult<SetupChangeBundle> {
    let bundle = setup::SetupChangeBundle {
        action: setup::FlowSetupChangeAction::Drop,
        flow_names,
    };
    Ok(SetupChangeBundle(Arc::new(bundle)))
}

#[pyfunction]
fn remove_flow_context(py: Python<'_>, flow_name: String) -> PyResult<()> {
    py.detach(|| -> Result<()> {
        get_runtime().block_on(async move {
            let lib_context = get_lib_context().await?;
            lib_context.remove_flow_context(&flow_name);
            Ok(())
        })
    })
    .into_py_result()
}

#[pyfunction]
fn add_auth_entry(key: String, value: Pythonized<serde_json::Value>) -> PyResult<()> {
    get_auth_registry()
        .add(key, value.into_inner())
        .into_py_result()?;
    Ok(())
}

#[pyfunction]
fn add_transient_auth_entry(value: Pythonized<serde_json::Value>) -> PyResult<String> {
    get_auth_registry()
        .add_transient(value.into_inner())
        .into_py_result()
}

#[pyfunction]
fn get_auth_entry(key: String) -> PyResult<Pythonized<serde_json::Value>> {
    let auth_ref = AuthEntryReference::new(key);
    let json_value: serde_json::Value = get_auth_registry().get(&auth_ref).into_py_result()?;
    Ok(Pythonized(json_value))
}

#[pyfunction]
fn get_app_namespace(py: Python<'_>) -> PyResult<String> {
    let app_namespace = py
        .detach(|| -> Result<_> {
            get_runtime().block_on(async move {
                let lib_context = get_lib_context().await?;
                Ok(lib_context.app_namespace.clone())
            })
        })
        .into_py_result()?;
    Ok(app_namespace)
}

#[pyfunction]
fn serde_roundtrip<'py>(
    py: Python<'py>,
    value: Bound<'py, PyAny>,
    typ: Pythonized<ValueType>,
) -> PyResult<Bound<'py, PyAny>> {
    let typ = typ.into_inner();
    let value = value_from_py_object(&typ, &value)?;
    let value = value::test_util::serde_roundtrip(&value, &typ).into_py_result()?;
    value_to_py_object(py, &value)
}

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "_engine")]
fn cocoindex_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    m.add_function(wrap_pyfunction!(init_pyo3_runtime, m)?)?;
    m.add_function(wrap_pyfunction!(init, m)?)?;
    m.add_function(wrap_pyfunction!(set_settings_fn, m)?)?;
    m.add_function(wrap_pyfunction!(start_server, m)?)?;
    m.add_function(wrap_pyfunction!(stop, m)?)?;
    m.add_function(wrap_pyfunction!(register_source_connector, m)?)?;
    m.add_function(wrap_pyfunction!(register_function_factory, m)?)?;
    m.add_function(wrap_pyfunction!(register_target_connector, m)?)?;
    m.add_function(wrap_pyfunction!(flow_names_with_setup_async, m)?)?;
    m.add_function(wrap_pyfunction!(make_setup_bundle, m)?)?;
    m.add_function(wrap_pyfunction!(make_drop_bundle, m)?)?;
    m.add_function(wrap_pyfunction!(remove_flow_context, m)?)?;
    m.add_function(wrap_pyfunction!(add_auth_entry, m)?)?;
    m.add_function(wrap_pyfunction!(add_transient_auth_entry, m)?)?;
    m.add_function(wrap_pyfunction!(get_auth_entry, m)?)?;
    m.add_function(wrap_pyfunction!(get_app_namespace, m)?)?;

    m.add_class::<builder::flow_builder::FlowBuilder>()?;
    m.add_class::<builder::flow_builder::DataCollector>()?;
    m.add_class::<builder::flow_builder::DataSlice>()?;
    m.add_class::<builder::flow_builder::OpScopeRef>()?;
    m.add_class::<Flow>()?;
    m.add_class::<FlowLiveUpdater>()?;
    m.add_class::<TransientFlow>()?;
    m.add_class::<IndexUpdateInfo>()?;
    m.add_class::<SetupChangeBundle>()?;
    m.add_class::<PyOpArgSchema>()?;
    m.add_class::<RenderedSpec>()?;
    m.add_class::<RenderedSpecLine>()?;

    let testutil_module = PyModule::new(m.py(), "testutil")?;
    testutil_module.add_function(wrap_pyfunction!(serde_roundtrip, &testutil_module)?)?;
    m.add_submodule(&testutil_module)?;

    Ok(())
}
