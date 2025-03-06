use anyhow::{anyhow, bail, Result};
use pyo3::{exceptions::PyException, prelude::*};
use std::{
    collections::{btree_map, hash_map::Entry, HashMap},
    ops::Deref,
    sync::{Arc, Mutex, Weak},
};

use super::analyzer::{
    build_flow_instance_context, AnalyzerContext, CollectorBuilder, DataScopeBuilder,
    ExecutionScope, ValueTypeBuilder,
};
use crate::{
    api_bail,
    base::{
        schema::{self, FieldSchema, StructSchema},
        spec::{self, FieldName, NamedSpec},
    },
    get_lib_context,
    lib_context::LibContext,
    ops::interface::FlowInstanceContext,
    py::IntoPyResult,
    setup,
    utils::immutable::RefList,
};
use crate::{lib_context::FlowContext, py};

#[derive(Debug)]
pub struct DataScopeRefInfo {
    scope_name: String,
    parent: Option<(DataScopeRef, spec::FieldPath)>,
    scope_builder: Arc<Mutex<DataScopeBuilder>>,
    children: Mutex<HashMap<spec::FieldPath, Weak<DataScopeRefInfo>>>,
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct DataScopeRef(Arc<DataScopeRefInfo>);

impl Deref for DataScopeRef {
    type Target = DataScopeRefInfo;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for DataScopeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some((scope, field_path)) = &self.parent {
            write!(f, "{} [{} AS {}]", scope, field_path, self.scope_name)?;
        } else {
            write!(f, "[{}]", self.scope_name)?;
        }
        Ok(())
    }
}

#[pymethods]
impl DataScopeRef {
    pub fn __str__(&self) -> String {
        format!("{}", self)
    }

    pub fn __repr__(&self) -> String {
        self.__str__()
    }

    pub fn add_collector(&mut self, name: String) -> PyResult<DataCollector> {
        let collector = DataCollector {
            name,
            scope: self.clone(),
            collector: Mutex::new(None),
        };
        Ok(collector)
    }
}

impl DataScopeRef {
    fn get_child_scope(&self, field_path: spec::FieldPath) -> Result<Self> {
        let mut children = self.children.lock().unwrap();
        let result = match children.entry(field_path) {
            Entry::Occupied(mut entry) => {
                let child = entry.get().upgrade();
                if let Some(child) = child {
                    DataScopeRef(child)
                } else {
                    let new_scope = self.make_child_scope(&entry.key())?;
                    entry.insert(Arc::downgrade(&new_scope.0));
                    new_scope
                }
            }
            Entry::Vacant(entry) => {
                let new_scope = self.make_child_scope(&entry.key())?;
                entry.insert(Arc::downgrade(&new_scope.0));
                new_scope
            }
        };
        Ok(result)
    }

    fn make_child_scope(&self, field_path: &spec::FieldPath) -> Result<Self> {
        let mut num_parent_layers = 0;
        let mut curr_scope = self;
        while let Some((parent, _)) = &curr_scope.parent {
            curr_scope = parent;
            num_parent_layers += 1;
        }

        let scope_data = &self.scope_builder.lock().unwrap().data;
        let mut field_typ = &scope_data
            .find_field(
                field_path
                    .first()
                    .ok_or_else(|| anyhow!("field path is empty"))?,
            )
            .ok_or_else(|| anyhow!("field {} not found", field_path.first().unwrap()))?
            .1
            .value_type
            .typ;
        for field in (&field_path[1..]).iter() {
            let struct_builder = match field_typ {
                ValueTypeBuilder::Struct(struct_type) => struct_type,
                _ => bail!("expect struct type"),
            };
            field_typ = &struct_builder
                .find_field(field)
                .ok_or_else(|| anyhow!("field {} not found", field))?
                .1
                .value_type
                .typ;
        }
        let scope_builder = match field_typ {
            ValueTypeBuilder::Collection(collection_type) => collection_type.sub_scope.clone(),
            _ => api_bail!("expect collection type"),
        };

        let new_scope = DataScopeRef(Arc::new(DataScopeRefInfo {
            scope_name: format!("_{}_{}", field_path.join("_"), num_parent_layers),
            parent: Some((self.clone(), field_path.clone())),
            scope_builder,
            children: Mutex::new(HashMap::new()),
        }));
        Ok(new_scope)
    }

    fn is_ds_scope_descendant(&self, other: &Self) -> bool {
        if Arc::ptr_eq(&self.0, &other.0) {
            return true;
        }
        match &self.parent {
            Some((parent, _)) => parent.is_ds_scope_descendant(other),
            None => false,
        }
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct DataType {
    typ: schema::EnrichedValueType,
}

impl From<schema::EnrichedValueType> for DataType {
    fn from(typ: schema::EnrichedValueType) -> Self {
        Self { typ }
    }
}

#[pymethods]
impl DataType {
    pub fn __str__(&self) -> String {
        format!("{}", self.typ)
    }

    pub fn __repr__(&self) -> String {
        self.__str__()
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct DataSlice {
    scope: DataScopeRef,
    value: Arc<spec::ValueMapping>,
    data_type: DataType,
}

#[pymethods]
impl DataSlice {
    pub fn data_type(&self) -> DataType {
        self.data_type.clone()
    }

    pub fn __str__(&self) -> String {
        format!("{}", self)
    }

    pub fn __repr__(&self) -> String {
        self.__str__()
    }

    pub fn field(&self, field_name: &str) -> PyResult<Option<DataSlice>> {
        let field_schema = match &self.data_type.typ.typ {
            schema::ValueType::Struct(struct_type) => {
                match struct_type.fields.iter().find(|f| f.name == field_name) {
                    Some(field) => field,
                    None => return Ok(None),
                }
            }
            _ => return Err(PyException::new_err("expect struct type")),
        };
        let value_mapping = match self.value.as_ref() {
            spec::ValueMapping::Field(spec::FieldMapping {
                scope,
                field_path: spec::FieldPath(field_path),
            }) => spec::ValueMapping::Field(spec::FieldMapping {
                scope: scope.clone(),
                field_path: spec::FieldPath(
                    field_path
                        .iter()
                        .cloned()
                        .chain([field_name.to_string()])
                        .collect(),
                ),
            }),

            spec::ValueMapping::Struct(v) => v
                .fields
                .iter()
                .find(|f| f.name == field_name)
                .map(|f| f.spec.clone())
                .ok_or_else(|| PyException::new_err(format!("field {} not found", field_name)))?,

            spec::ValueMapping::Literal { .. } => {
                return Err(PyException::new_err(
                    "field access not supported for literal",
                ))
            }
        };
        Ok(Some(DataSlice {
            scope: self.scope.clone(),
            value: Arc::new(value_mapping),
            data_type: field_schema.value_type.clone().into(),
        }))
    }

    pub fn collection_entry_scope(&self) -> PyResult<DataScopeRef> {
        let field_path = match self.value.as_ref() {
            spec::ValueMapping::Field(v) => &v.field_path,
            _ => return Err(PyException::new_err("expect field path")),
        };
        Ok(self
            .scope
            .get_child_scope(field_path.clone())
            .into_py_result()?)
    }
}

impl DataSlice {
    fn extract_value_mapping(&self) -> spec::ValueMapping {
        match self.value.as_ref() {
            spec::ValueMapping::Field(v) => spec::ValueMapping::Field(spec::FieldMapping {
                field_path: v.field_path.clone(),
                scope: v
                    .scope
                    .clone()
                    .or_else(|| Some(self.scope.scope_name.clone())),
            }),
            v => v.clone(),
        }
    }
}

impl std::fmt::Display for DataSlice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DataSlice({}; {} {}) ",
            self.data_type.typ, self.scope, self.value
        )?;
        Ok(())
    }
}

#[pyclass]
pub struct DataCollector {
    name: String,
    scope: DataScopeRef,
    collector: Mutex<Option<CollectorBuilder>>,
}

#[pymethods]
impl DataCollector {
    fn __str__(&self) -> String {
        format!("{}", self)
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}

impl std::fmt::Display for DataCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let collector = self.collector.lock().unwrap();
        write!(f, "DataCollector \"{}\" ({}", self.name, self.scope)?;
        if let Some(collector) = collector.as_ref() {
            write!(f, ": {}", collector.schema)?;
            if collector.is_used {
                write!(f, " (used)")?;
            }
        }
        write!(f, ")")?;
        Ok(())
    }
}

#[pyclass]
pub struct FlowBuilder {
    lib_context: Arc<LibContext>,
    flow_inst_context: Arc<FlowInstanceContext>,
    existing_flow_ss: Option<setup::FlowSetupState<setup::ExistingMode>>,

    root_data_scope: Arc<Mutex<DataScopeBuilder>>,
    root_data_scope_ref: DataScopeRef,

    flow_instance_name: String,
    reactive_ops: Vec<NamedSpec<spec::ReactiveOpSpec>>,

    direct_input_fields: Vec<FieldSchema>,
    direct_output_value: Option<spec::ValueMapping>,

    source_ops: Vec<NamedSpec<spec::OpSpec>>,
    export_ops: Vec<NamedSpec<spec::ExportOpSpec>>,

    next_generated_op_id: usize,
}

#[pymethods]
impl FlowBuilder {
    #[new]
    pub fn new(name: &str) -> PyResult<Self> {
        let lib_context = get_lib_context()
            .ok_or_else(|| PyException::new_err("cocoindex library not initialized"))?;
        let existing_flow_ss = lib_context
            .combined_setup_states
            .read()
            .unwrap()
            .flows
            .get(name)
            .cloned();
        let root_data_scope = Arc::new(Mutex::new(DataScopeBuilder::new()));
        let result = Self {
            lib_context,
            flow_inst_context: build_flow_instance_context(name),
            existing_flow_ss,

            root_data_scope_ref: DataScopeRef(Arc::new(DataScopeRefInfo {
                scope_name: spec::ROOT_SCOPE_NAME.to_string(),
                parent: None,
                scope_builder: root_data_scope.clone(),
                children: Mutex::new(HashMap::new()),
            })),
            root_data_scope,
            flow_instance_name: name.to_string(),

            reactive_ops: vec![],

            source_ops: vec![],
            export_ops: vec![],

            direct_input_fields: vec![],
            direct_output_value: None,

            next_generated_op_id: 0,
        };
        Ok(result)
    }

    pub fn root_scope(&self) -> DataScopeRef {
        self.root_data_scope_ref.clone()
    }

    #[pyo3(signature = (kind, op_spec, target_scope, name))]
    pub fn add_source(
        &mut self,
        kind: String,
        op_spec: py::Pythonized<serde_json::Map<String, serde_json::Value>>,
        target_scope: Option<DataScopeRef>,
        name: String,
    ) -> PyResult<DataSlice> {
        if let Some(target_scope) = target_scope {
            if !Arc::ptr_eq(&target_scope.0, &self.root_data_scope_ref.0) {
                return Err(PyException::new_err(
                    "source can only be added to the root scope",
                ));
            }
        }
        let source_op = spec::NamedSpec {
            name,
            spec: spec::OpSpec {
                kind,
                spec: op_spec.into_inner(),
            },
        };
        let analyzer_ctx = AnalyzerContext {
            registry: &crate::ops::executor_factory_registry(),
            flow_ctx: &self.flow_inst_context,
        };
        let mut root_data_scope = self.root_data_scope.lock().unwrap();
        let _ = analyzer_ctx
            .analyze_source_op(&mut root_data_scope, source_op.clone(), None, None)
            .into_py_result()?;

        let result =
            Self::last_field_to_data_slice(&root_data_scope, self.root_data_scope_ref.clone())
                .into_py_result()?;
        self.source_ops.push(source_op);
        Ok(result)
    }

    pub fn add_direct_input(
        &mut self,
        name: String,
        value_type: py::Pythonized<schema::EnrichedValueType>,
    ) -> PyResult<DataSlice> {
        let mut root_data_scope = self.root_data_scope.lock().unwrap();
        root_data_scope
            .add_field(name.clone(), &value_type)
            .into_py_result()?;
        let result =
            Self::last_field_to_data_slice(&root_data_scope, self.root_data_scope_ref.clone())
                .into_py_result()?;
        self.direct_input_fields.push(FieldSchema {
            name,
            value_type: value_type.into_inner(),
        });
        Ok(result)
    }

    pub fn set_direct_output(&mut self, data_slice: DataSlice) -> PyResult<()> {
        if !Arc::ptr_eq(&data_slice.scope.0, &self.root_data_scope_ref.0) {
            return Err(PyException::new_err(
                "direct output must be value in the root scope",
            ));
        }
        self.direct_output_value = Some(data_slice.extract_value_mapping());
        Ok(())
    }

    #[pyo3(signature = (kind, op_spec, args, target_scope, name))]
    pub fn transform(
        &mut self,
        kind: String,
        op_spec: py::Pythonized<serde_json::Map<String, serde_json::Value>>,
        args: Vec<(DataSlice, Option<String>)>,
        target_scope: Option<DataScopeRef>,
        name: String,
    ) -> PyResult<DataSlice> {
        let spec = spec::OpSpec {
            kind,
            spec: op_spec.into_inner(),
        };
        let common_scope =
            Self::minimum_common_scope(args.iter().map(|(ds, _)| &ds.scope), target_scope.as_ref())
                .into_py_result()?;
        self.do_in_scope(
            common_scope,
            |reactive_ops, scope, parent_scopes, analyzer_ctx| {
                let reactive_op = spec::NamedSpec {
                    name,
                    spec: spec::ReactiveOpSpec::Transform(spec::TransformOpSpec {
                        inputs: args
                            .iter()
                            .map(|(ds, arg_name)| spec::OpArgBinding {
                                arg_name: spec::OpArgName(arg_name.clone()),
                                value: ds.extract_value_mapping(),
                            })
                            .collect(),
                        op: spec,
                    }),
                };
                let _ = analyzer_ctx.analyze_reactive_op(scope, &reactive_op, parent_scopes)?;

                reactive_ops.push(reactive_op);
                let result = Self::last_field_to_data_slice(&scope.data, common_scope.clone())
                    .into_py_result()?;
                Ok(result)
            },
        )
        .into_py_result()
    }

    pub fn collect(
        &mut self,
        collector: &DataCollector,
        fields: Vec<(FieldName, DataSlice)>,
    ) -> PyResult<()> {
        let common_scope = Self::minimum_common_scope(fields.iter().map(|(_, ds)| &ds.scope), None)
            .into_py_result()?;
        let name = format!(".collect.{}", self.next_generated_op_id);
        self.next_generated_op_id += 1;
        self.do_in_scope(
            common_scope,
            |reactive_ops, scope, parent_scopes, analyzer_ctx| {
                let reactive_op = spec::NamedSpec {
                    name,
                    spec: spec::ReactiveOpSpec::Collect(spec::CollectOpSpec {
                        input: spec::StructMapping {
                            fields: fields
                                .iter()
                                .map(|(name, ds)| NamedSpec {
                                    name: name.clone(),
                                    spec: ds.extract_value_mapping(),
                                })
                                .collect(),
                        },
                        scope_name: collector.scope.scope_name.clone(),
                        collector_name: collector.name.clone(),
                    }),
                };
                let _ = analyzer_ctx.analyze_reactive_op(scope, &reactive_op, parent_scopes)?;
                reactive_ops.push(reactive_op);
                Ok(())
            },
        )
        .into_py_result()?;

        let struct_schema = StructSchema {
            fields: Arc::new(
                fields
                    .into_iter()
                    .map(|(name, ds)| FieldSchema {
                        name,
                        value_type: ds.data_type.typ,
                    })
                    .collect(),
            ),
        };

        {
            let mut collector = collector.collector.lock().unwrap();
            if let Some(collector) = collector.as_mut() {
                collector.merge_schema(&struct_schema).into_py_result()?;
            } else {
                *collector = Some(CollectorBuilder::new(struct_schema));
            }
        }

        Ok(())
    }

    pub fn export(
        &mut self,
        name: String,
        kind: String,
        op_spec: py::Pythonized<serde_json::Map<String, serde_json::Value>>,
        index_options: py::Pythonized<spec::IndexOptions>,
        input: &DataCollector,
    ) -> PyResult<()> {
        let spec = spec::OpSpec {
            kind,
            spec: op_spec.into_inner(),
        };

        if !Arc::ptr_eq(&input.scope.0, &self.root_data_scope_ref.0) {
            return Err(PyException::new_err(
                "Export can only work on collectors belonging to the root scope.",
            ));
        }
        self.export_ops.push(spec::NamedSpec {
            name,
            spec: spec::ExportOpSpec {
                collector_name: input.name.clone(),
                target: spec,
                index_options: index_options.into_inner(),
            },
        });
        Ok(())
    }

    pub fn scope_field(
        &self,
        scope: DataScopeRef,
        field_name: &str,
    ) -> PyResult<Option<DataSlice>> {
        let field_type = {
            let scope_builder = scope.scope_builder.lock().unwrap();
            let (_, field_schema) = scope_builder
                .data
                .find_field(field_name)
                .ok_or_else(|| PyException::new_err(format!("field {} not found", field_name)))?;
            schema::EnrichedValueType::from_alternative(&field_schema.value_type)
                .into_py_result()?
        };
        Ok(Some(DataSlice {
            scope,
            value: Arc::new(spec::ValueMapping::Field(spec::FieldMapping {
                scope: None,
                field_path: spec::FieldPath(vec![field_name.to_string()]),
            })),
            data_type: DataType { typ: field_type },
        }))
    }

    pub fn build_flow(&self, py: Python<'_>) -> PyResult<py::Flow> {
        let spec = spec::FlowInstanceSpec {
            name: self.flow_instance_name.clone(),
            source_ops: self.source_ops.clone(),
            reactive_ops: self.reactive_ops.clone(),
            export_ops: self.export_ops.clone(),
        };
        let analyzed_flow = py
            .allow_threads(|| {
                self.lib_context
                    .runtime
                    .block_on(super::AnalyzedFlow::from_flow_instance(
                        spec,
                        self.existing_flow_ss.as_ref(),
                        &crate::ops::executor_factory_registry(),
                    ))
            })
            .into_py_result()?;
        let analyzed_flow = Arc::new(analyzed_flow);

        let mut analyzed_flows = self.lib_context.flows.write().unwrap();
        match analyzed_flows.entry(self.flow_instance_name.clone()) {
            btree_map::Entry::Occupied(_) => {
                return Err(PyException::new_err(format!(
                    "flow instance name already exists: {}",
                    self.flow_instance_name
                )));
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(FlowContext::new(analyzed_flow.clone()));
            }
        }
        Ok(py::Flow(analyzed_flow))
    }

    pub fn build_transient_flow(&self, py: Python<'_>) -> PyResult<py::TransientFlow> {
        if self.direct_input_fields.is_empty() {
            return Err(PyException::new_err("expect at least one direct input"));
        }
        let direct_output_value = if let Some(direct_output_value) = &self.direct_output_value {
            direct_output_value
        } else {
            return Err(PyException::new_err("expect direct output"));
        };
        let spec = spec::TransientFlowSpec {
            name: self.flow_instance_name.clone(),
            input_fields: self.direct_input_fields.clone(),
            reactive_ops: self.reactive_ops.clone(),
            output_value: direct_output_value.clone(),
        };
        let analyzed_flow = py
            .allow_threads(|| {
                self.lib_context.runtime.block_on(
                    super::AnalyzedTransientFlow::from_transient_flow(
                        spec,
                        &crate::ops::executor_factory_registry(),
                    ),
                )
            })
            .into_py_result()?;
        Ok(py::TransientFlow(Arc::new(analyzed_flow)))
    }

    pub fn __str__(&self) -> String {
        format!("{}", self)
    }

    pub fn __repr__(&self) -> String {
        self.__str__()
    }
}

impl std::fmt::Display for FlowBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Flow instance name: {}\n\n", self.flow_instance_name)?;
        for op in self.source_ops.iter() {
            write!(
                f,
                "Source op {}\n{}\n",
                op.name,
                serde_json::to_string_pretty(&op.spec).unwrap_or_default()
            )?;
        }
        for field in self.direct_input_fields.iter() {
            write!(f, "Direct input {}: {}\n", field.name, field.value_type)?;
        }
        if !self.direct_input_fields.is_empty() {
            write!(f, "\n")?;
        }
        for op in self.reactive_ops.iter() {
            write!(
                f,
                "Reactive op {}\n{}\n",
                op.name,
                serde_json::to_string_pretty(&op.spec).unwrap_or_default()
            )?;
        }
        for op in self.export_ops.iter() {
            write!(
                f,
                "Export op {}\n{}\n",
                op.name,
                serde_json::to_string_pretty(&op.spec).unwrap_or_default()
            )?;
        }
        if let Some(output) = &self.direct_output_value {
            write!(f, "Direct output: {}\n\n", output)?;
        }
        Ok(())
    }
}

impl FlowBuilder {
    fn last_field_to_data_slice(
        data_builder: &DataScopeBuilder,
        scope: DataScopeRef,
    ) -> Result<DataSlice> {
        let last_field = data_builder.last_field().unwrap();
        let result = DataSlice {
            scope,
            value: Arc::new(spec::ValueMapping::Field(spec::FieldMapping {
                scope: None,
                field_path: spec::FieldPath(vec![last_field.name.clone()]),
            })),
            data_type: schema::EnrichedValueType::from_alternative(&last_field.value_type)?.into(),
        };
        Ok(result)
    }

    fn minimum_common_scope<'a>(
        scopes: impl Iterator<Item = &'a DataScopeRef>,
        target_scope: Option<&'a DataScopeRef>,
    ) -> Result<&'a DataScopeRef> {
        let mut scope_iter = scopes;
        let mut common_scope = scope_iter
            .next()
            .ok_or_else(|| PyException::new_err("expect at least one input"))?;
        for scope in scope_iter {
            if scope.is_ds_scope_descendant(common_scope) {
                common_scope = scope;
            } else if !common_scope.is_ds_scope_descendant(scope) {
                api_bail!(
                    "expect all arguments share the common scope, got {} and {} exclusive to each other",
                    common_scope, scope
                );
            }
        }
        if let Some(target_scope) = target_scope {
            if !target_scope.is_ds_scope_descendant(common_scope) {
                api_bail!(
                    "the field can only be attached to a scope or sub-scope of the input value. Target scope: {}, input scope: {}",
                    target_scope, common_scope
                );
            }
            common_scope = target_scope;
        }
        Ok(common_scope)
    }

    fn do_in_scope<T>(
        &mut self,
        data_slice_scope: &DataScopeRef,
        f: impl FnOnce(
            &mut Vec<spec::NamedSpec<spec::ReactiveOpSpec>>,
            &mut ExecutionScope<'_>,
            RefList<'_, &'_ ExecutionScope<'_>>,
            &AnalyzerContext<'_>,
        ) -> Result<T>,
    ) -> Result<T> {
        let mut data_slice_scopes = Vec::new();
        let mut next_ds_scope = data_slice_scope;
        while let Some((parent, _)) = &next_ds_scope.parent {
            data_slice_scopes.push(next_ds_scope);
            next_ds_scope = parent;
        }

        Self::do_in_sub_scope(
            &mut ExecutionScope {
                name: spec::ROOT_SCOPE_NAME,
                data: &mut self.root_data_scope.lock().unwrap(),
            },
            RefList::Nil,
            &data_slice_scopes,
            &mut self.reactive_ops,
            &mut self.next_generated_op_id,
            &AnalyzerContext {
                registry: &crate::ops::executor_factory_registry(),
                flow_ctx: &self.flow_inst_context,
            },
            f,
        )
    }

    fn do_in_sub_scope<T>(
        scope: &mut ExecutionScope<'_>,
        parent_scopes: RefList<'_, &'_ ExecutionScope<'_>>,
        data_slice_scopes: &[&DataScopeRef],
        reactive_ops: &mut Vec<spec::NamedSpec<spec::ReactiveOpSpec>>,
        next_generated_op_id: &mut usize,
        analyzer_ctx: &AnalyzerContext<'_>,
        f: impl FnOnce(
            &mut Vec<spec::NamedSpec<spec::ReactiveOpSpec>>,
            &mut ExecutionScope<'_>,
            RefList<'_, &'_ ExecutionScope<'_>>,
            &AnalyzerContext<'_>,
        ) -> Result<T>,
    ) -> Result<T> {
        let curr_ds_scope = if let Some(&ds_scope) = data_slice_scopes.last() {
            ds_scope
        } else {
            return f(reactive_ops, scope, parent_scopes, analyzer_ctx);
        };
        let field_path = if let Some((_, field_path)) = &curr_ds_scope.parent {
            field_path
        } else {
            bail!("expect sub scope, got root")
        };

        // Reuse the last foreach if matched, otherwise create a new one.
        let reactive_ops = match reactive_ops.last_mut() {
            Some(spec::NamedSpec {
                spec: spec::ReactiveOpSpec::ForEach(foreach_spec),
                ..
            }) if &foreach_spec.field_path == field_path
                && foreach_spec.op_scope.name == curr_ds_scope.scope_name =>
            {
                &mut foreach_spec.op_scope.ops
            }
            _ => {
                reactive_ops.push(spec::NamedSpec {
                    name: format!(".foreach.{}", next_generated_op_id),
                    spec: spec::ReactiveOpSpec::ForEach(spec::ForEachOpSpec {
                        field_path: field_path.clone(),
                        op_scope: spec::ReactiveOpScope {
                            name: curr_ds_scope.scope_name.clone(),
                            ops: vec![],
                        },
                    }),
                });
                *next_generated_op_id += 1;
                match &mut reactive_ops.last_mut().unwrap().spec {
                    spec::ReactiveOpSpec::ForEach(foreach_spec) => &mut foreach_spec.op_scope.ops,
                    _ => unreachable!(),
                }
            }
        };

        let (_, field_type) = scope.data.analyze_field_path(field_path)?;
        let sub_scope = match &field_type.typ {
            ValueTypeBuilder::Collection(collection_type) => &collection_type.sub_scope,
            t => api_bail!(
                "expect collection type, got {}",
                TryInto::<schema::ValueType>::try_into(t)?
            ),
        };
        let mut sub_scope = sub_scope.lock().unwrap();
        Self::do_in_sub_scope(
            &mut ExecutionScope {
                name: curr_ds_scope.scope_name.as_str(),
                data: &mut sub_scope,
            },
            parent_scopes.prepend(scope),
            &data_slice_scopes[0..data_slice_scopes.len() - 1],
            reactive_ops,
            next_generated_op_id,
            analyzer_ctx,
            f,
        )
    }
}
