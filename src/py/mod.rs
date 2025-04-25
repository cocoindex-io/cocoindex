use crate::prelude::*;

use crate::base::spec::VectorSimilarityMetric;
use crate::execution::query;
use crate::lib_context::{clear_lib_context, get_auth_registry, init_lib_context};
use crate::ops::interface::{QueryResult, QueryResults};
use crate::ops::py_factory::PyOpArgSchema;
use crate::ops::{interface::ExecutorFactory, py_factory::PyFunctionFactory, register_factory};
use crate::server::{self, ServerSettings};
use crate::settings::Settings;
use crate::setup;
use pyo3::{exceptions::PyException, prelude::*};
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::btree_map;
use std::fmt::Write;

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
            let live_updater = execution::FlowLiveUpdater::start(
                flow,
                &get_lib_context().into_py_result()?.builtin_db_pool,
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
                    execution::dumper::evaluate_and_dump(
                        &exec_plan,
                        &self.0.flow.data_schema,
                        options.into_inner(),
                        &get_lib_context()?.builtin_db_pool,
                    )
                    .await
                })
                .into_py_result()?;
            Ok(())
        })
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
}

#[pyclass]
pub struct SimpleSemanticsQueryHandler(pub Arc<query::SimpleSemanticsQueryHandler>);

#[pymethods]
impl SimpleSemanticsQueryHandler {
    #[new]
    pub fn new(
        py: Python<'_>,
        flow: &Flow,
        target_name: &str,
        query_transform_flow: &TransientFlow,
        default_similarity_metric: Pythonized<VectorSimilarityMetric>,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            let handler = get_runtime()
                .block_on(query::SimpleSemanticsQueryHandler::new(
                    flow.0.flow.clone(),
                    target_name,
                    query_transform_flow.0.clone(),
                    default_similarity_metric.0,
                ))
                .into_py_result()?;
            Ok(Self(Arc::new(handler)))
        })
    }

    pub fn register_query_handler(&self, name: String) -> PyResult<()> {
        let flow_ctx = get_lib_context()
            .into_py_result()?
            .get_flow_context(&self.0.flow_name)
            .into_py_result()?;
        let mut query_handlers = flow_ctx.query_handlers.lock().unwrap();
        match query_handlers.entry(name) {
            btree_map::Entry::Occupied(entry) => {
                return Err(PyException::new_err(format!(
                    "query handler name already exists: {}",
                    entry.key()
                )));
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(self.0.clone());
            }
        }
        Ok(())
    }

    #[pyo3(signature = (query, limit, vector_field_name = None, similarity_metric = None))]
    pub fn search(
        &self,
        py: Python<'_>,
        query: String,
        limit: u32,
        vector_field_name: Option<String>,
        similarity_metric: Option<Pythonized<VectorSimilarityMetric>>,
    ) -> PyResult<(
        Pythonized<Vec<QueryResult<serde_json::Value>>>,
        Pythonized<query::SimpleSemanticsQueryInfo>,
    )> {
        py.allow_threads(|| {
            let (results, info) = get_runtime().block_on(async move {
                self.0
                    .search(
                        query,
                        limit,
                        vector_field_name,
                        similarity_metric.map(|m| m.0),
                    )
                    .await
            })?;
            let results = QueryResults::<serde_json::Value>::try_from(results)?;
            anyhow::Ok((Pythonized(results.results), Pythonized(info)))
        })
        .into_py_result()
    }
}

#[pyclass]
pub struct SetupStatusCheck(setup::AllSetupStatusCheck);

#[pymethods]
impl SetupStatusCheck {
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
fn sync_setup(py: Python<'_>) -> PyResult<SetupStatusCheck> {
    let lib_context = get_lib_context().into_py_result()?;
    let flows = lib_context.flows.lock().unwrap();
    let all_setup_states = lib_context.all_setup_states.read().unwrap();
    py.allow_threads(|| {
        get_runtime()
            .block_on(async {
                let setup_status = setup::sync_setup(&flows, &all_setup_states).await?;
                anyhow::Ok(SetupStatusCheck(setup_status))
            })
            .into_py_result()
    })
}

#[pyfunction]
fn drop_setup(py: Python<'_>, flow_names: Vec<String>) -> PyResult<SetupStatusCheck> {
    let lib_context = get_lib_context().into_py_result()?;
    let all_setup_states = lib_context.all_setup_states.read().unwrap();
    py.allow_threads(|| {
        get_runtime()
            .block_on(async {
                let setup_status = setup::drop_setup(flow_names, &all_setup_states).await?;
                anyhow::Ok(SetupStatusCheck(setup_status))
            })
            .into_py_result()
    })
}

#[pyfunction]
fn flow_names_with_setup() -> PyResult<Vec<String>> {
    let lib_context = get_lib_context().into_py_result()?;
    let all_setup_states = lib_context.all_setup_states.read().unwrap();
    let flow_names = all_setup_states.flows.keys().cloned().collect();
    Ok(flow_names)
}

#[pyfunction]
fn apply_setup_changes(py: Python<'_>, setup_status: &SetupStatusCheck) -> PyResult<()> {
    py.allow_threads(|| {
        get_runtime()
            .block_on(async {
                setup::apply_changes(
                    &mut std::io::stdout(),
                    &setup_status.0,
                    &get_lib_context()?.builtin_db_pool,
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
    m.add_class::<builder::flow_builder::DataScopeRef>()?;
    m.add_class::<Flow>()?;
    m.add_class::<FlowLiveUpdater>()?;
    m.add_class::<TransientFlow>()?;
    m.add_class::<IndexUpdateInfo>()?;
    m.add_class::<SimpleSemanticsQueryHandler>()?;
    m.add_class::<SetupStatusCheck>()?;
    m.add_class::<PyOpArgSchema>()?;

    Ok(())
}
