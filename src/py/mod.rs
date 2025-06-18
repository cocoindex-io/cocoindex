use crate::execution::evaluator::evaluate_transient_flow;
use crate::prelude::*;

use crate::base::schema::{FieldSchema, ValueType};
use crate::base::spec::{NamedSpec, OutputMode, ReactiveOpSpec, SpecFormatter};
use crate::lib_context::{clear_lib_context, get_auth_registry, init_lib_context};
use crate::ops::py_factory::PyOpArgSchema;
use crate::ops::{interface::ExecutorFactory, py_factory::PyFunctionFactory, register_factory};
use crate::server::{self, ServerSettings};
use crate::settings::Settings;
use crate::setup;
use pyo3::IntoPyObjectExt;
use pyo3::{exceptions::PyException, prelude::*};
use pyo3_async_runtimes::tokio::future_into_py;
use std::fmt::Write;
use std::sync::Arc;

mod convert;
pub use convert::*;

pub struct PythonExecutionContext {
    pub event_loop: Py<PyAny>,
}

impl PythonExecutionContext {
    pub fn new(_py: Python<'_>, event_loop: Py<PyAny>) -> Self {
        Self { event_loop }
    }
}

pub trait FromPyResult<T> {
    fn from_py_result(self, py: Python<'_>) -> anyhow::Result<T>;
}

impl<T> FromPyResult<T> for Result<T, PyErr> {
    fn from_py_result(self, py: Python<'_>) -> anyhow::Result<T> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => {
                let mut err_str = format!("Error calling Python function: {}", err);
                if let Some(tb) = err.traceback(py) {
                    write!(&mut err_str, "\n{}", tb.format()?)?;
                }
                Err(anyhow::anyhow!(err_str))
            }
        }
    }
}
pub trait IntoPyResult<T> {
    fn into_py_result(self) -> PyResult<T>;
}

impl<T, E: std::fmt::Debug> IntoPyResult<T> for Result<T, E> {
    fn into_py_result(self) -> PyResult<T> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(PyException::new_err(format!("{:?}", err))),
        }
    }
}

#[pyfunction]
fn init(py: Python<'_>, settings: Pythonized<Settings>) -> PyResult<()> {
    py.allow_threads(|| -> anyhow::Result<()> {
        init_lib_context(settings.into_inner())?;
        Ok(())
    })
    .into_py_result()
}

#[pyfunction]
fn start_server(py: Python<'_>, settings: Pythonized<ServerSettings>) -> PyResult<()> {
    py.allow_threads(|| -> anyhow::Result<()> {
        let server = get_runtime().block_on(server::init_server(
            get_lib_context()?,
            settings.into_inner(),
        ))?;
        get_runtime().spawn(server);
        Ok(())
    })
    .into_py_result()
}

#[pyfunction]
fn stop(py: Python<'_>) -> PyResult<()> {
    py.allow_threads(clear_lib_context);
    Ok(())
}

#[pyfunction]
fn register_function_factory(name: String, py_function_factory: Py<PyAny>) -> PyResult<()> {
    let factory = PyFunctionFactory {
        py_function_factory,
    };
    register_factory(name, ExecutorFactory::SimpleFunction(Arc::new(factory))).into_py_result()
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
pub struct FlowLiveUpdater(pub Arc<tokio::sync::RwLock<execution::FlowLiveUpdater>>);

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
            let lib_context = get_lib_context().into_py_result()?;
            let live_updater = execution::FlowLiveUpdater::start(
                flow,
                lib_context.require_builtin_db_pool().into_py_result()?,
                options.into_inner(),
            )
            .await
            .into_py_result()?;
            Ok(Self(Arc::new(tokio::sync::RwLock::new(live_updater))))
        })
    }

    pub fn wait<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let live_updater = self.0.clone();
        future_into_py(py, async move {
            let mut live_updater = live_updater.write().await;
            live_updater.wait().await.into_py_result()
        })
    }

    pub fn abort(&self, py: Python<'_>) {
        py.allow_threads(|| {
            let mut live_updater = self.0.blocking_write();
            live_updater.abort();
        })
    }

    pub fn index_update_info(&self, py: Python<'_>) -> IndexUpdateInfo {
        py.allow_threads(|| {
            let live_updater = self.0.blocking_read();
            IndexUpdateInfo(live_updater.index_update_info())
        })
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
        py.allow_threads(|| {
            get_runtime()
                .block_on(async {
                    let exec_plan = self.0.flow.get_execution_plan().await?;
                    let lib_context = get_lib_context()?;
                    execution::dumper::evaluate_and_dump(
                        &exec_plan,
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
                    ValueType::Basic(basic) => format!("{}", basic),
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
                        process_fields(&s.fields, &format!("{}.", field_name), result);
                    }
                    ValueType::Table(t) => {
                        process_fields(&t.row.fields, &format!("{}[].", field_name), result);
                    }
                    ValueType::Basic(_) => {}
                }
            }
        }

        process_fields(&schema.schema.fields, "", &mut result);
        result
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
            Python::with_gil(|py| value_to_py_object(py, &result)?.into_py_any(py))
        })
    }
}

#[pyclass]
pub struct SetupStatus(setup::AllSetupStatus);

#[pymethods]
impl SetupStatus {
    pub fn __str__(&self) -> String {
        format!("{}", &self.0)
    }

    pub fn __repr__(&self) -> String {
        self.__str__()
    }

    pub fn is_up_to_date(&self) -> bool {
        self.0.is_up_to_date()
    }
}

#[pyfunction]
fn sync_setup(py: Python<'_>) -> PyResult<SetupStatus> {
    let lib_context = get_lib_context().into_py_result()?;
    let flows = lib_context.flows.lock().unwrap();
    let all_setup_states = lib_context
        .require_all_setup_states()
        .into_py_result()?
        .read()
        .unwrap();
    py.allow_threads(|| {
        get_runtime()
            .block_on(async {
                let setup_status = setup::sync_setup(&flows, &all_setup_states).await?;
                anyhow::Ok(SetupStatus(setup_status))
            })
            .into_py_result()
    })
}

#[pyfunction]
fn drop_setup(py: Python<'_>, flow_names: Vec<String>) -> PyResult<SetupStatus> {
    let lib_context = get_lib_context().into_py_result()?;
    let all_setup_states = lib_context
        .require_all_setup_states()
        .into_py_result()?
        .read()
        .unwrap();
    py.allow_threads(|| {
        get_runtime()
            .block_on(async {
                let setup_status = setup::drop_setup(flow_names, &all_setup_states).await?;
                anyhow::Ok(SetupStatus(setup_status))
            })
            .into_py_result()
    })
}

#[pyfunction]
fn flow_names_with_setup() -> PyResult<Vec<String>> {
    let lib_context = get_lib_context().into_py_result()?;
    let all_setup_states = lib_context
        .require_all_setup_states()
        .into_py_result()?
        .read()
        .unwrap();
    let flow_names = all_setup_states.flows.keys().cloned().collect();
    Ok(flow_names)
}

#[pyfunction]
fn apply_setup_changes(py: Python<'_>, setup_status: &SetupStatus) -> PyResult<()> {
    py.allow_threads(|| {
        get_runtime()
            .block_on(async {
                let lib_context = get_lib_context()?;
                setup::apply_changes(
                    &mut std::io::stdout(),
                    &setup_status.0,
                    lib_context.require_builtin_db_pool()?,
                )
                .await
            })
            .into_py_result()?;
        Ok(())
    })
}

#[pyfunction]
fn add_auth_entry(key: String, value: Pythonized<serde_json::Value>) -> PyResult<()> {
    get_auth_registry()
        .add(key, value.into_inner())
        .into_py_result()?;
    Ok(())
}

#[pyfunction]
fn seder_roundtrip<'py>(
    py: Python<'py>,
    value: Bound<'py, PyAny>,
    typ: Pythonized<ValueType>,
) -> PyResult<Bound<'py, PyAny>> {
    let typ = typ.into_inner();
    let value = value_from_py_object(&typ, &value)?;
    let value = value::test_util::seder_roundtrip(&value, &typ).into_py_result()?;
    Ok(value_to_py_object(py, &value)?)
}

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "_engine")]
fn cocoindex_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init, m)?)?;
    m.add_function(wrap_pyfunction!(start_server, m)?)?;
    m.add_function(wrap_pyfunction!(stop, m)?)?;
    m.add_function(wrap_pyfunction!(register_function_factory, m)?)?;
    m.add_function(wrap_pyfunction!(sync_setup, m)?)?;
    m.add_function(wrap_pyfunction!(drop_setup, m)?)?;
    m.add_function(wrap_pyfunction!(apply_setup_changes, m)?)?;
    m.add_function(wrap_pyfunction!(flow_names_with_setup, m)?)?;
    m.add_function(wrap_pyfunction!(add_auth_entry, m)?)?;

    m.add_class::<builder::flow_builder::FlowBuilder>()?;
    m.add_class::<builder::flow_builder::DataCollector>()?;
    m.add_class::<builder::flow_builder::DataSlice>()?;
    m.add_class::<builder::flow_builder::OpScopeRef>()?;
    m.add_class::<Flow>()?;
    m.add_class::<FlowLiveUpdater>()?;
    m.add_class::<TransientFlow>()?;
    m.add_class::<IndexUpdateInfo>()?;
    m.add_class::<SetupStatus>()?;
    m.add_class::<PyOpArgSchema>()?;
    m.add_class::<RenderedSpec>()?;
    m.add_class::<RenderedSpecLine>()?;

    let testutil_module = PyModule::new(m.py(), "testutil")?;
    testutil_module.add_function(wrap_pyfunction!(seder_roundtrip, &testutil_module)?)?;
    m.add_submodule(&testutil_module)?;

    Ok(())
}
