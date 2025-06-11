use crate::prelude::*;

use super::plan::*;
use crate::execution::db_tracking_setup;
use crate::lib_context::get_auth_registry;
use crate::setup::{
    self, DesiredMode, FlowSetupMetadata, FlowSetupState, ResourceIdentifier, SourceSetupState,
    TargetSetupState, TargetSetupStateCommon,
};
use crate::utils::fingerprint::Fingerprinter;
use crate::{
    base::{schema::*, spec::*},
    ops::{interface::*, registry::*},
};
use futures::future::{BoxFuture, try_join3};
use futures::{FutureExt, future::try_join_all};

#[derive(Debug)]
pub(super) enum ValueTypeBuilder {
    Basic(BasicValueType),
    Struct(StructSchemaBuilder),
    Table(TableSchemaBuilder),
}

impl TryFrom<&ValueType> for ValueTypeBuilder {
    type Error = anyhow::Error;

    fn try_from(value_type: &ValueType) -> Result<Self> {
        match value_type {
            ValueType::Basic(basic_type) => Ok(ValueTypeBuilder::Basic(basic_type.clone())),
            ValueType::Struct(struct_type) => Ok(ValueTypeBuilder::Struct(struct_type.try_into()?)),
            ValueType::Table(table_type) => Ok(ValueTypeBuilder::Table(table_type.try_into()?)),
        }
    }
}

impl TryInto<ValueType> for &ValueTypeBuilder {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<ValueType> {
        match self {
            ValueTypeBuilder::Basic(basic_type) => Ok(ValueType::Basic(basic_type.clone())),
            ValueTypeBuilder::Struct(struct_type) => Ok(ValueType::Struct(struct_type.try_into()?)),
            ValueTypeBuilder::Table(table_type) => Ok(ValueType::Table(table_type.try_into()?)),
        }
    }
}

#[derive(Default, Debug)]
pub(super) struct StructSchemaBuilder {
    fields: Vec<FieldSchema<ValueTypeBuilder>>,
    field_name_idx: HashMap<FieldName, u32>,
    description: Option<Arc<str>>,
}

impl StructSchemaBuilder {
    fn add_field(&mut self, field: FieldSchema<ValueTypeBuilder>) -> Result<u32> {
        let field_idx = self.fields.len() as u32;
        match self.field_name_idx.entry(field.name.clone()) {
            std::collections::hash_map::Entry::Occupied(_) => {
                bail!("Field name already exists: {}", field.name);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(field_idx);
            }
        }
        self.fields.push(field);
        Ok(field_idx)
    }

    pub fn find_field(&self, field_name: &'_ str) -> Option<(u32, &FieldSchema<ValueTypeBuilder>)> {
        self.field_name_idx
            .get(field_name)
            .map(|&field_idx| (field_idx, &self.fields[field_idx as usize]))
    }
}

impl TryFrom<&StructSchema> for StructSchemaBuilder {
    type Error = anyhow::Error;

    fn try_from(schema: &StructSchema) -> Result<Self> {
        let mut result = StructSchemaBuilder {
            fields: Vec::with_capacity(schema.fields.len()),
            field_name_idx: HashMap::with_capacity(schema.fields.len()),
            description: schema.description.clone(),
        };
        for field in schema.fields.iter() {
            result.add_field(FieldSchema::<ValueTypeBuilder>::from_alternative(field)?)?;
        }
        Ok(result)
    }
}

impl TryInto<StructSchema> for &StructSchemaBuilder {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<StructSchema> {
        Ok(StructSchema {
            fields: Arc::new(
                self.fields
                    .iter()
                    .map(FieldSchema::<ValueType>::from_alternative)
                    .collect::<Result<Vec<_>>>()?,
            ),
            description: self.description.clone(),
        })
    }
}

#[derive(Debug)]
pub(super) struct TableSchemaBuilder {
    pub kind: TableKind,
    pub sub_scope: Arc<Mutex<DataScopeBuilder>>,
}

impl TryFrom<&TableSchema> for TableSchemaBuilder {
    type Error = anyhow::Error;

    fn try_from(schema: &TableSchema) -> Result<Self> {
        Ok(Self {
            kind: schema.kind,
            sub_scope: Arc::new(Mutex::new(DataScopeBuilder {
                data: (&schema.row).try_into()?,
            })),
        })
    }
}

impl TryInto<TableSchema> for &TableSchemaBuilder {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<TableSchema> {
        let sub_scope = self.sub_scope.lock().unwrap();
        let row = (&sub_scope.data).try_into()?;
        Ok(TableSchema {
            kind: self.kind,
            row,
        })
    }
}

fn try_make_common_value_type(
    value_type1: &EnrichedValueType,
    value_type2: &EnrichedValueType,
) -> Result<EnrichedValueType> {
    let typ = match (&value_type1.typ, &value_type2.typ) {
        (ValueType::Basic(basic_type1), ValueType::Basic(basic_type2)) => {
            if basic_type1 != basic_type2 {
                api_bail!("Value types are not compatible: {basic_type1} vs {basic_type2}");
            }
            ValueType::Basic(basic_type1.clone())
        }
        (ValueType::Struct(struct_type1), ValueType::Struct(struct_type2)) => {
            let common_schema = try_merge_struct_schemas(struct_type1, struct_type2)?;
            ValueType::Struct(common_schema)
        }
        (ValueType::Table(table_type1), ValueType::Table(table_type2)) => {
            if table_type1.kind != table_type2.kind {
                api_bail!(
                    "Collection types are not compatible: {} vs {}",
                    table_type1,
                    table_type2
                );
            }
            let row = try_merge_struct_schemas(&table_type1.row, &table_type2.row)?;
            ValueType::Table(TableSchema {
                kind: table_type1.kind,
                row,
            })
        }
        (t1 @ (ValueType::Basic(_) | ValueType::Struct(_) | ValueType::Table(_)), t2) => {
            api_bail!("Unmatched types:\n  {t1}\n  {t2}\n",)
        }
    };
    let common_attrs: Vec<_> = value_type1
        .attrs
        .iter()
        .filter_map(|(k, v)| {
            if value_type2.attrs.get(k) == Some(v) {
                Some((k, v))
            } else {
                None
            }
        })
        .collect();
    let attrs = if common_attrs.len() == value_type1.attrs.len() {
        value_type1.attrs.clone()
    } else {
        Arc::new(
            common_attrs
                .into_iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        )
    };

    Ok(EnrichedValueType {
        typ,
        nullable: value_type1.nullable || value_type2.nullable,
        attrs,
    })
}

fn try_merge_fields_schemas(
    schema1: &[FieldSchema],
    schema2: &[FieldSchema],
) -> Result<Vec<FieldSchema>> {
    if schema1.len() != schema2.len() {
        api_bail!(
            "Fields are not compatible as they have different fields count:\n  ({})\n  ({})\n",
            schema1
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            schema2
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let mut result_fields = Vec::with_capacity(schema1.len());
    for (field1, field2) in schema1.iter().zip(schema2.iter()) {
        if field1.name != field2.name {
            api_bail!(
                "Structs are not compatible as they have incompatible field names `{}` vs `{}`",
                field1.name,
                field2.name
            );
        }
        result_fields.push(FieldSchema {
            name: field1.name.clone(),
            value_type: try_make_common_value_type(&field1.value_type, &field2.value_type)?,
        });
    }
    Ok(result_fields)
}

fn try_merge_struct_schemas(
    schema1: &StructSchema,
    schema2: &StructSchema,
) -> Result<StructSchema> {
    let fields = try_merge_fields_schemas(&schema1.fields, &schema2.fields)?;
    Ok(StructSchema {
        fields: Arc::new(fields),
        description: schema1
            .description
            .clone()
            .or_else(|| schema2.description.clone()),
    })
}

fn try_merge_collector_schemas(
    schema1: &CollectorSchema,
    schema2: &CollectorSchema,
) -> Result<CollectorSchema> {
    let fields = try_merge_fields_schemas(&schema1.fields, &schema2.fields)?;
    Ok(CollectorSchema {
        fields,
        auto_uuid_field_idx: if schema1.auto_uuid_field_idx == schema2.auto_uuid_field_idx {
            schema1.auto_uuid_field_idx
        } else {
            None
        },
    })
}

#[derive(Debug)]
pub(super) struct CollectorBuilder {
    pub schema: Arc<CollectorSchema>,
    pub is_used: bool,
}

impl CollectorBuilder {
    pub fn new(schema: Arc<CollectorSchema>) -> Self {
        Self {
            schema,
            is_used: false,
        }
    }

    pub fn merge_schema(&mut self, schema: &CollectorSchema) -> Result<()> {
        if self.is_used {
            api_bail!("Collector is already used");
        }
        let existing_schema = Arc::make_mut(&mut self.schema);
        *existing_schema = try_merge_collector_schemas(existing_schema, schema)?;
        Ok(())
    }

    pub fn use_schema(&mut self) -> Arc<CollectorSchema> {
        self.is_used = true;
        self.schema.clone()
    }
}

#[derive(Debug)]
pub(super) struct DataScopeBuilder {
    pub data: StructSchemaBuilder,
}

impl DataScopeBuilder {
    pub fn new() -> Self {
        Self {
            data: Default::default(),
        }
    }

    pub fn last_field(&self) -> Option<&FieldSchema<ValueTypeBuilder>> {
        self.data.fields.last()
    }

    pub fn add_field(
        &mut self,
        name: FieldName,
        value_type: &EnrichedValueType,
    ) -> Result<AnalyzedOpOutput> {
        let field_index = self.data.add_field(FieldSchema {
            name,
            value_type: EnrichedValueType::from_alternative(value_type)?,
        })?;
        Ok(AnalyzedOpOutput {
            field_idx: field_index,
        })
    }

    pub fn analyze_field_path<'a>(
        &'a self,
        field_path: &'_ FieldPath,
    ) -> Result<(
        AnalyzedLocalFieldReference,
        &'a EnrichedValueType<ValueTypeBuilder>,
    )> {
        let mut indices = Vec::with_capacity(field_path.len());
        let mut struct_schema = &self.data;

        let mut i = 0;
        let value_type = loop {
            let field_name = &field_path[i];
            let (field_idx, field) = struct_schema.find_field(field_name).ok_or_else(|| {
                api_error!("Field {} not found", field_path[0..(i + 1)].join("."))
            })?;
            indices.push(field_idx);
            if i + 1 >= field_path.len() {
                break &field.value_type;
            }
            i += 1;

            struct_schema = match &field.value_type.typ {
                ValueTypeBuilder::Struct(struct_type) => struct_type,
                _ => {
                    api_bail!("Field {} is not a struct", field_path[0..(i + 1)].join("."));
                }
            };
        };
        Ok((
            AnalyzedLocalFieldReference {
                fields_idx: indices,
            },
            value_type,
        ))
    }
}

pub(super) struct AnalyzerContext<'a> {
    pub registry: &'a ExecutorFactoryRegistry,
    pub flow_ctx: &'a Arc<FlowInstanceContext>,
}

#[derive(Debug, Default)]
pub(super) struct OpScopeStates {
    pub op_output_types: HashMap<FieldName, EnrichedValueType>,
    pub collectors: IndexMap<FieldName, CollectorBuilder>,
    pub sub_scopes: HashMap<String, Arc<OpScopeSchema>>,
}

impl OpScopeStates {
    pub fn add_collector(
        &mut self,
        collector_name: FieldName,
        schema: CollectorSchema,
    ) -> Result<AnalyzedLocalCollectorReference> {
        let existing_len = self.collectors.len();
        let idx = match self.collectors.entry(collector_name) {
            indexmap::map::Entry::Occupied(mut entry) => {
                entry.get_mut().merge_schema(&schema)?;
                entry.index()
            }
            indexmap::map::Entry::Vacant(entry) => {
                entry.insert(CollectorBuilder::new(Arc::new(schema)));
                existing_len
            }
        };
        Ok(AnalyzedLocalCollectorReference {
            collector_idx: idx as u32,
        })
    }

    pub fn consume_collector(
        &mut self,
        collector_name: &FieldName,
    ) -> Result<(AnalyzedLocalCollectorReference, Arc<CollectorSchema>)> {
        let (collector_idx, _, collector) = self
            .collectors
            .get_full_mut(collector_name)
            .ok_or_else(|| api_error!("Collector not found: {}", collector_name))?;
        Ok((
            AnalyzedLocalCollectorReference {
                collector_idx: collector_idx as u32,
            },
            collector.use_schema(),
        ))
    }

    fn build_op_scope_schema(&self) -> OpScopeSchema {
        OpScopeSchema {
            op_output_types: self
                .op_output_types
                .iter()
                .map(|(name, value_type)| (name.clone(), value_type.without_attrs()))
                .collect(),
            collectors: self
                .collectors
                .iter()
                .map(|(name, schema)| NamedSpec {
                    name: name.clone(),
                    spec: schema.schema.clone(),
                })
                .collect(),
            op_scopes: self.sub_scopes.clone(),
        }
    }
}

#[derive(Debug)]
pub struct OpScope {
    pub name: String,
    pub parent: Option<(Arc<OpScope>, spec::FieldPath)>,
    pub(super) data: Arc<Mutex<DataScopeBuilder>>,
    pub(super) states: Mutex<OpScopeStates>,
}

struct Iter<'a>(Option<&'a OpScope>);

impl<'a> Iterator for Iter<'a> {
    type Item = &'a OpScope;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0 {
            Some(scope) => {
                self.0 = scope.parent.as_ref().map(|(parent, _)| parent.as_ref());
                Some(scope)
            }
            None => None,
        }
    }
}

impl OpScope {
    pub(super) fn new(
        name: String,
        parent: Option<(Arc<OpScope>, spec::FieldPath)>,
        data: Arc<Mutex<DataScopeBuilder>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            name,
            parent,
            data,
            states: Mutex::default(),
        })
    }

    fn add_op_output(
        &self,
        name: FieldName,
        value_type: EnrichedValueType,
    ) -> Result<AnalyzedOpOutput> {
        let op_output = self
            .data
            .lock()
            .unwrap()
            .add_field(name.clone(), &value_type)?;
        self.states
            .lock()
            .unwrap()
            .op_output_types
            .insert(name, value_type);
        Ok(op_output)
    }

    pub fn ancestors(&self) -> impl Iterator<Item = &OpScope> {
        Iter(Some(self))
    }

    pub fn is_op_scope_descendant(&self, other: &Self) -> bool {
        if self == other {
            return true;
        }
        match &self.parent {
            Some((parent, _)) => parent.is_op_scope_descendant(other),
            None => false,
        }
    }

    pub(super) fn new_foreach_op_scope(
        self: &Arc<Self>,
        scope_name: String,
        field_path: &FieldPath,
    ) -> Result<(AnalyzedLocalFieldReference, Arc<Self>)> {
        let (local_field_ref, sub_data_scope) = {
            let data_scope = self.data.lock().unwrap();
            let (local_field_ref, value_type) = data_scope.analyze_field_path(field_path)?;
            let sub_data_scope = match &value_type.typ {
                ValueTypeBuilder::Table(table_type) => table_type.sub_scope.clone(),
                _ => api_bail!("ForEach only works on collection, field {field_path} is not"),
            };
            (local_field_ref, sub_data_scope)
        };
        let sub_op_scope = OpScope::new(
            scope_name,
            Some((self.clone(), field_path.clone())),
            sub_data_scope,
        );
        Ok((local_field_ref, sub_op_scope))
    }
}

impl std::fmt::Display for OpScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some((scope, field_path)) = &self.parent {
            write!(f, "{} [{} AS {}]", scope, field_path, self.name)?;
        } else {
            write!(f, "[{}]", self.name)?;
        }
        Ok(())
    }
}

impl PartialEq for OpScope {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}
impl Eq for OpScope {}

fn find_scope<'a>(scope_name: &ScopeName, op_scope: &'a OpScope) -> Result<(u32, &'a OpScope)> {
    let (up_level, scope) = op_scope
        .ancestors()
        .enumerate()
        .find(|(_, s)| &s.name == scope_name)
        .ok_or_else(|| api_error!("Scope not found: {}", scope_name))?;
    Ok((up_level as u32, scope))
}

fn analyze_struct_mapping(
    mapping: &StructMapping,
    op_scope: &OpScope,
) -> Result<(AnalyzedStructMapping, Vec<FieldSchema>)> {
    let mut field_mappings = Vec::with_capacity(mapping.fields.len());
    let mut field_schemas = Vec::with_capacity(mapping.fields.len());
    for field in mapping.fields.iter() {
        let (field_mapping, value_type) = analyze_value_mapping(&field.spec, op_scope)?;
        field_mappings.push(field_mapping);
        field_schemas.push(FieldSchema {
            name: field.name.clone(),
            value_type,
        });
    }
    Ok((
        AnalyzedStructMapping {
            fields: field_mappings,
        },
        field_schemas,
    ))
}

fn analyze_value_mapping(
    value_mapping: &ValueMapping,
    op_scope: &OpScope,
) -> Result<(AnalyzedValueMapping, EnrichedValueType)> {
    let result = match value_mapping {
        ValueMapping::Constant(v) => {
            let value = value::Value::from_json(v.value.clone(), &v.schema.typ)?;
            (AnalyzedValueMapping::Constant { value }, v.schema.clone())
        }

        ValueMapping::Field(v) => {
            let (scope_up_level, op_scope) = match &v.scope {
                Some(scope_name) => find_scope(scope_name, op_scope)?,
                None => (0, op_scope),
            };
            let data_scope = op_scope.data.lock().unwrap();
            let (local_field_ref, value_type) = data_scope.analyze_field_path(&v.field_path)?;
            (
                AnalyzedValueMapping::Field(AnalyzedFieldReference {
                    local: local_field_ref,
                    scope_up_level,
                }),
                EnrichedValueType::from_alternative(value_type)?,
            )
        }

        ValueMapping::Struct(v) => {
            let (struct_mapping, field_schemas) = analyze_struct_mapping(v, op_scope)?;
            (
                AnalyzedValueMapping::Struct(struct_mapping),
                EnrichedValueType {
                    typ: ValueType::Struct(StructSchema {
                        fields: Arc::new(field_schemas),
                        description: None,
                    }),
                    nullable: false,
                    attrs: Default::default(),
                },
            )
        }
    };
    Ok(result)
}

fn analyze_input_fields(
    arg_bindings: &[OpArgBinding],
    op_scope: &OpScope,
) -> Result<Vec<OpArgSchema>> {
    let mut input_field_schemas = Vec::with_capacity(arg_bindings.len());
    for arg_binding in arg_bindings.iter() {
        let (analyzed_value, value_type) = analyze_value_mapping(&arg_binding.value, op_scope)?;
        input_field_schemas.push(OpArgSchema {
            name: arg_binding.arg_name.clone(),
            value_type,
            analyzed_value: analyzed_value.clone(),
        });
    }
    Ok(input_field_schemas)
}

fn add_collector(
    scope_name: &ScopeName,
    collector_name: FieldName,
    schema: CollectorSchema,
    op_scope: &OpScope,
) -> Result<AnalyzedCollectorReference> {
    let (scope_up_level, scope) = find_scope(scope_name, op_scope)?;
    let local_ref = scope
        .states
        .lock()
        .unwrap()
        .add_collector(collector_name, schema)?;
    Ok(AnalyzedCollectorReference {
        local: local_ref,
        scope_up_level,
    })
}

struct ExportDataFieldsInfo {
    local_collector_ref: AnalyzedLocalCollectorReference,
    primary_key_def: AnalyzedPrimaryKeyDef,
    primary_key_type: ValueType,
    value_fields_idx: Vec<u32>,
    value_stable: bool,
}

impl AnalyzerContext<'_> {
    pub(super) fn analyze_import_op(
        &self,
        op_scope: &Arc<OpScope>,
        import_op: NamedSpec<ImportOpSpec>,
        metadata: Option<&mut FlowSetupMetadata>,
        existing_source_states: Option<&Vec<&SourceSetupState>>,
    ) -> Result<impl Future<Output = Result<AnalyzedImportOp>> + Send + use<>> {
        let factory = self.registry.get(&import_op.spec.source.kind);
        let source_factory = match factory {
            Some(ExecutorFactory::Source(source_executor)) => source_executor.clone(),
            _ => {
                return Err(anyhow::anyhow!(
                    "Source executor not found for kind: {}",
                    import_op.spec.source.kind
                ));
            }
        };
        let (output_type, executor) = source_factory.build(
            serde_json::Value::Object(import_op.spec.source.spec),
            self.flow_ctx.clone(),
        )?;

        let key_schema_no_attrs = output_type
            .typ
            .key_type()
            .ok_or_else(|| api_error!("Source must produce a type with key"))?
            .typ
            .without_attrs();

        let source_id = metadata.map(|metadata| {
            let existing_source_ids = existing_source_states
                .iter()
                .flat_map(|v| v.iter())
                .filter_map(|state| {
                    if state.key_schema == key_schema_no_attrs {
                        Some(state.source_id)
                    } else {
                        None
                    }
                })
                .collect::<HashSet<_>>();
            let source_id = if existing_source_ids.len() == 1 {
                existing_source_ids.into_iter().next().unwrap()
            } else {
                if existing_source_ids.len() > 1 {
                    warn!("Multiple source states with the same key schema found");
                }
                metadata.last_source_id += 1;
                metadata.last_source_id
            };
            metadata.sources.insert(
                import_op.name.clone(),
                SourceSetupState {
                    source_id,
                    key_schema: key_schema_no_attrs,
                },
            );
            source_id
        });

        let op_name = import_op.name.clone();
        let primary_key_type = output_type
            .typ
            .key_type()
            .ok_or_else(|| api_error!("Source must produce a type with key: {op_name}"))?
            .typ
            .clone();
        let output = op_scope.add_op_output(import_op.name, output_type)?;
        let result_fut = async move {
            trace!("Start building executor for source op `{}`", op_name);
            let executor = executor.await?;
            trace!("Finished building executor for source op `{}`", op_name);
            Ok(AnalyzedImportOp {
                source_id: source_id.unwrap_or_default(),
                executor,
                output,
                primary_key_type,
                name: op_name,
                refresh_options: import_op.spec.refresh_options,
            })
        };
        Ok(result_fut)
    }

    pub(super) fn analyze_reactive_op(
        &self,
        op_scope: &Arc<OpScope>,
        reactive_op: &NamedSpec<ReactiveOpSpec>,
    ) -> Result<BoxFuture<'static, Result<AnalyzedReactiveOp>>> {
        let result_fut = match &reactive_op.spec {
            ReactiveOpSpec::Transform(op) => {
                let input_field_schemas =
                    analyze_input_fields(&op.inputs, op_scope).with_context(|| {
                        format!(
                            "Failed to analyze inputs for transform op: {}",
                            reactive_op.name
                        )
                    })?;
                let spec = serde_json::Value::Object(op.op.spec.clone());

                let factory = self.registry.get(&op.op.kind);
                match factory {
                    Some(ExecutorFactory::SimpleFunction(fn_executor)) => {
                        let input_value_mappings = input_field_schemas
                            .iter()
                            .map(|field| field.analyzed_value.clone())
                            .collect();
                        let (output_enriched_type, executor) = fn_executor.clone().build(
                            spec,
                            input_field_schemas,
                            self.flow_ctx.clone(),
                        )?;
                        let logic_fingerprinter = Fingerprinter::default()
                            .with(&op.op)?
                            .with(&output_enriched_type.without_attrs())?;
                        let output_type = output_enriched_type.typ.clone();
                        let output = op_scope
                            .add_op_output(reactive_op.name.clone(), output_enriched_type)?;
                        let op_name = reactive_op.name.clone();
                        async move {
                            trace!("Start building executor for transform op `{op_name}`");
                            let executor = executor.await.with_context(|| {
                                format!("Failed to build executor for transform op: {op_name}")
                            })?;
                            let enable_cache = executor.enable_cache();
                            let behavior_version = executor.behavior_version();
                            trace!("Finished building executor for transform op `{op_name}`, enable cache: {enable_cache}, behavior version: {behavior_version:?}");
                            let function_exec_info = AnalyzedFunctionExecInfo {
                                enable_cache,
                                behavior_version,
                                fingerprinter: logic_fingerprinter
                                    .with(&behavior_version)?,
                                output_type
                            };
                            if function_exec_info.enable_cache
                                && function_exec_info.behavior_version.is_none()
                            {
                                api_bail!(
                                    "When caching is enabled, behavior version must be specified for transform op: {op_name}"
                                );
                            }
                            Ok(AnalyzedReactiveOp::Transform(AnalyzedTransformOp {
                                name: op_name,
                                inputs: input_value_mappings,
                                function_exec_info,
                                executor,
                                output,
                            }))
                        }
                        .boxed()
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Transform op kind not found: {}",
                            op.op.kind
                        ));
                    }
                }
            }

            ReactiveOpSpec::ForEach(foreach_op) => {
                let (local_field_ref, sub_op_scope) = op_scope.new_foreach_op_scope(
                    foreach_op.op_scope.name.clone(),
                    &foreach_op.field_path,
                )?;
                let analyzed_op_scope_fut = {
                    let analyzed_op_scope_fut =
                        self.analyze_op_scope(&sub_op_scope, &foreach_op.op_scope.ops)?;
                    let sub_op_scope_schema =
                        sub_op_scope.states.lock().unwrap().build_op_scope_schema();
                    op_scope.states.lock().unwrap().sub_scopes.insert(
                        foreach_op.op_scope.name.clone(),
                        Arc::new(sub_op_scope_schema),
                    );
                    analyzed_op_scope_fut
                };
                let op_name = reactive_op.name.clone();
                async move {
                    Ok(AnalyzedReactiveOp::ForEach(AnalyzedForEachOp {
                        local_field_ref,
                        op_scope: analyzed_op_scope_fut
                            .await
                            .with_context(|| format!("Analyzing foreach op: {op_name}"))?,
                        name: op_name,
                    }))
                }
                .boxed()
            }

            ReactiveOpSpec::Collect(op) => {
                let (struct_mapping, fields_schema) = analyze_struct_mapping(&op.input, op_scope)?;
                let has_auto_uuid_field = op.auto_uuid_field.is_some();
                let fingerprinter = Fingerprinter::default().with(&fields_schema)?;
                let collect_op = AnalyzedReactiveOp::Collect(AnalyzedCollectOp {
                    name: reactive_op.name.clone(),
                    has_auto_uuid_field,
                    input: struct_mapping,
                    collector_ref: add_collector(
                        &op.scope_name,
                        op.collector_name.clone(),
                        CollectorSchema::from_fields(fields_schema, op.auto_uuid_field.clone()),
                        op_scope,
                    )?,
                    fingerprinter,
                });
                async move { Ok(collect_op) }.boxed()
            }
        };
        Ok(result_fut)
    }

    fn merge_export_op_states(
        &self,
        target_kind: String,
        setup_key: serde_json::Value,
        setup_state: serde_json::Value,
        setup_by_user: bool,
        export_factory: &dyn ExportTargetFactory,
        flow_setup_state: &mut FlowSetupState<DesiredMode>,
        existing_target_states: &HashMap<&ResourceIdentifier, Vec<&TargetSetupState>>,
    ) -> Result<i32> {
        let resource_id = ResourceIdentifier {
            key: setup_key,
            target_kind,
        };
        let existing_target_states = existing_target_states.get(&resource_id);
        let mut compatible_target_ids = HashSet::<Option<i32>>::new();
        let mut reusable_schema_version_ids = HashSet::<Option<i32>>::new();
        for existing_state in existing_target_states.iter().flat_map(|v| v.iter()) {
            let compatibility = if setup_by_user == existing_state.common.setup_by_user {
                export_factory.check_state_compatibility(&setup_state, &existing_state.state)?
            } else {
                SetupStateCompatibility::NotCompatible
            };
            let compatible_target_id = if compatibility != SetupStateCompatibility::NotCompatible {
                reusable_schema_version_ids.insert(
                    (compatibility == SetupStateCompatibility::Compatible)
                        .then_some(existing_state.common.schema_version_id),
                );
                Some(existing_state.common.target_id)
            } else {
                None
            };
            compatible_target_ids.insert(compatible_target_id);
        }

        let target_id = if compatible_target_ids.len() == 1 {
            compatible_target_ids.into_iter().next().flatten()
        } else {
            if compatible_target_ids.len() > 1 {
                warn!("Multiple target states with the same key schema found");
            }
            None
        };
        let target_id = target_id.unwrap_or_else(|| {
            flow_setup_state.metadata.last_target_id += 1;
            flow_setup_state.metadata.last_target_id
        });
        let max_schema_version_id = existing_target_states
            .iter()
            .flat_map(|v| v.iter())
            .map(|s| s.common.max_schema_version_id)
            .max()
            .unwrap_or(0);
        let schema_version_id = if reusable_schema_version_ids.len() == 1 {
            reusable_schema_version_ids
                .into_iter()
                .next()
                .unwrap()
                .unwrap_or(max_schema_version_id + 1)
        } else {
            max_schema_version_id + 1
        };
        match flow_setup_state.targets.entry(resource_id) {
            indexmap::map::Entry::Occupied(entry) => {
                api_bail!(
                    "Target resource already exists: kind = {}, key = {}",
                    entry.key().target_kind,
                    entry.key().key
                );
            }
            indexmap::map::Entry::Vacant(entry) => {
                entry.insert(TargetSetupState {
                    common: TargetSetupStateCommon {
                        target_id,
                        schema_version_id,
                        max_schema_version_id: max_schema_version_id.max(schema_version_id),
                        setup_by_user,
                    },
                    state: setup_state,
                });
            }
        }
        Ok(target_id)
    }

    fn analyze_export_op_group(
        &self,
        target_kind: String,
        op_scope: &Arc<OpScope>,
        flow_inst: &FlowInstanceSpec,
        export_op_group: &AnalyzedExportTargetOpGroup,
        declarations: Vec<serde_json::Value>,
        flow_setup_state: &mut FlowSetupState<DesiredMode>,
        existing_target_states: &HashMap<&ResourceIdentifier, Vec<&TargetSetupState>>,
    ) -> Result<Vec<impl Future<Output = Result<AnalyzedExportOp>> + Send + use<>>> {
        let mut collection_specs = Vec::<interface::ExportDataCollectionSpec>::new();
        let mut data_fields_infos = Vec::<ExportDataFieldsInfo>::new();
        for idx in export_op_group.op_idx.iter() {
            let export_op = &flow_inst.export_ops[*idx];
            let (local_collector_ref, collector_schema) = op_scope
                .states
                .lock()
                .unwrap()
                .consume_collector(&export_op.spec.collector_name)?;
            let (key_fields_schema, value_fields_schema, data_collection_info) =
                match &export_op.spec.index_options.primary_key_fields {
                    Some(fields) => {
                        let pk_fields_idx = fields
                            .iter()
                            .map(|f| {
                                collector_schema
                                    .fields
                                    .iter()
                                    .position(|field| &field.name == f)
                                    .ok_or_else(|| anyhow!("field not found: {}", f))
                            })
                            .collect::<Result<Vec<_>>>()?;

                        let key_fields_schema = pk_fields_idx
                            .iter()
                            .map(|idx| collector_schema.fields[*idx as usize].clone())
                            .collect::<Vec<_>>();
                        let primary_key_type = if pk_fields_idx.len() == 1 {
                            key_fields_schema[0].value_type.typ.clone()
                        } else {
                            ValueType::Struct(StructSchema {
                                fields: Arc::from(key_fields_schema.clone()),
                                description: None,
                            })
                        };
                        let mut value_fields_schema: Vec<FieldSchema> = vec![];
                        let mut value_fields_idx = vec![];
                        for (idx, field) in collector_schema.fields.iter().enumerate() {
                            if !pk_fields_idx.contains(&idx) {
                                value_fields_schema.push(field.clone());
                                value_fields_idx.push(idx as u32);
                            }
                        }
                        let value_stable = collector_schema
                            .auto_uuid_field_idx
                            .as_ref()
                            .map(|uuid_idx| pk_fields_idx.contains(uuid_idx))
                            .unwrap_or(false);
                        (
                            key_fields_schema,
                            value_fields_schema,
                            ExportDataFieldsInfo {
                                local_collector_ref,
                                primary_key_def: AnalyzedPrimaryKeyDef::Fields(pk_fields_idx),
                                primary_key_type,
                                value_fields_idx,
                                value_stable,
                            },
                        )
                    }
                    None => {
                        // TODO: Support auto-generate primary key
                        api_bail!("Primary key fields must be specified")
                    }
                };
            collection_specs.push(interface::ExportDataCollectionSpec {
                name: export_op.name.clone(),
                spec: serde_json::Value::Object(export_op.spec.target.spec.clone()),
                key_fields_schema,
                value_fields_schema,
                index_options: export_op.spec.index_options.clone(),
            });
            data_fields_infos.push(data_collection_info);
        }
        let (data_collections_output, declarations_output) = export_op_group
            .target_factory
            .clone()
            .build(collection_specs, declarations, self.flow_ctx.clone())?;
        let analyzed_export_ops = export_op_group
            .op_idx
            .iter()
            .zip(data_collections_output.into_iter())
            .zip(data_fields_infos.into_iter())
            .map(|((idx, data_coll_output), data_fields_info)| {
                let export_op = &flow_inst.export_ops[*idx];
                let target_id = self.merge_export_op_states(
                    export_op.spec.target.kind.clone(),
                    data_coll_output.setup_key,
                    data_coll_output.desired_setup_state,
                    export_op.spec.setup_by_user,
                    export_op_group.target_factory.as_ref(),
                    flow_setup_state,
                    existing_target_states,
                )?;
                let op_name = export_op.name.clone();
                let export_target_factory = export_op_group.target_factory.clone();
                Ok(async move {
                    trace!("Start building executor for export op `{op_name}`");
                    let export_context = data_coll_output
                        .export_context
                        .await
                        .with_context(|| format!("Analyzing export op: {op_name}"))?;
                    trace!("Finished building executor for export op `{op_name}`");
                    Ok(AnalyzedExportOp {
                        name: op_name,
                        target_id,
                        input: data_fields_info.local_collector_ref,
                        export_target_factory,
                        export_context,
                        primary_key_def: data_fields_info.primary_key_def,
                        primary_key_type: data_fields_info.primary_key_type,
                        value_fields: data_fields_info.value_fields_idx,
                        value_stable: data_fields_info.value_stable,
                    })
                })
            })
            .collect::<Result<Vec<_>>>()?;
        for (setup_key, setup_state) in declarations_output.into_iter() {
            self.merge_export_op_states(
                target_kind.clone(),
                setup_key,
                setup_state,
                /*setup_by_user=*/ false,
                export_op_group.target_factory.as_ref(),
                flow_setup_state,
                existing_target_states,
            )?;
        }

        Ok(analyzed_export_ops)
    }

    fn analyze_op_scope(
        &self,
        op_scope: &Arc<OpScope>,
        reactive_ops: &[NamedSpec<ReactiveOpSpec>],
    ) -> Result<impl Future<Output = Result<AnalyzedOpScope>> + Send + use<>> {
        let op_futs = reactive_ops
            .iter()
            .map(|reactive_op| self.analyze_reactive_op(op_scope, reactive_op))
            .collect::<Result<Vec<_>>>()?;
        let collector_len = op_scope.states.lock().unwrap().collectors.len();
        let result_fut = async move {
            Ok(AnalyzedOpScope {
                reactive_ops: try_join_all(op_futs).await?,
                collector_len,
            })
        };
        Ok(result_fut)
    }
}

pub fn build_flow_instance_context(
    flow_inst_name: &str,
    py_exec_ctx: Option<crate::py::PythonExecutionContext>,
) -> Arc<FlowInstanceContext> {
    Arc::new(FlowInstanceContext {
        flow_instance_name: flow_inst_name.to_string(),
        auth_registry: get_auth_registry().clone(),
        py_exec_ctx: py_exec_ctx.map(Arc::new),
    })
}

fn build_flow_schema(root_op_scope: &OpScope) -> Result<FlowSchema> {
    let schema = (&root_op_scope.data.lock().unwrap().data).try_into()?;
    let root_op_scope_schema = root_op_scope.states.lock().unwrap().build_op_scope_schema();
    Ok(FlowSchema {
        schema,
        root_op_scope: root_op_scope_schema,
    })
}

pub fn analyze_flow(
    flow_inst: &FlowInstanceSpec,
    flow_ctx: &Arc<FlowInstanceContext>,
    existing_flow_ss: Option<&setup::FlowSetupState<setup::ExistingMode>>,
    registry: &ExecutorFactoryRegistry,
) -> Result<(
    FlowSchema,
    impl Future<Output = Result<ExecutionPlan>> + Send + use<>,
    setup::FlowSetupState<setup::DesiredMode>,
)> {
    let existing_metadata_versions = || {
        existing_flow_ss
            .iter()
            .flat_map(|flow_ss| flow_ss.metadata.possible_versions())
    };

    let mut source_states_by_name = HashMap::<&str, Vec<&SourceSetupState>>::new();
    for metadata_version in existing_metadata_versions() {
        for (source_name, state) in metadata_version.sources.iter() {
            source_states_by_name
                .entry(source_name.as_str())
                .or_default()
                .push(state);
        }
    }

    let mut target_states_by_name_type =
        HashMap::<&ResourceIdentifier, Vec<&TargetSetupState>>::new();
    for metadata_version in existing_flow_ss.iter() {
        for (resource_id, target) in metadata_version.targets.iter() {
            target_states_by_name_type
                .entry(resource_id)
                .or_default()
                .extend(target.possible_versions());
        }
    }

    let mut setup_state = setup::FlowSetupState::<setup::DesiredMode> {
        seen_flow_metadata_version: existing_flow_ss
            .and_then(|flow_ss| flow_ss.seen_flow_metadata_version),
        metadata: FlowSetupMetadata {
            last_source_id: existing_metadata_versions()
                .map(|metadata| metadata.last_source_id)
                .max()
                .unwrap_or(0),
            last_target_id: existing_metadata_versions()
                .map(|metadata| metadata.last_target_id)
                .max()
                .unwrap_or(0),
            sources: BTreeMap::new(),
        },
        tracking_table: db_tracking_setup::TrackingTableSetupState {
            table_name: existing_flow_ss
                .and_then(|flow_ss| {
                    flow_ss
                        .tracking_table
                        .current
                        .as_ref()
                        .map(|v| v.table_name.clone())
                })
                .unwrap_or_else(|| db_tracking_setup::default_tracking_table_name(&flow_inst.name)),
            version_id: db_tracking_setup::CURRENT_TRACKING_TABLE_VERSION,
        },
        // TODO: Fill it with a meaningful value.
        targets: IndexMap::new(),
    };

    let analyzer_ctx = AnalyzerContext { registry, flow_ctx };
    let root_data_scope = Arc::new(Mutex::new(DataScopeBuilder::new()));
    let root_op_scope = OpScope::new(ROOT_SCOPE_NAME.to_string(), None, root_data_scope);
    let import_ops_futs = flow_inst
        .import_ops
        .iter()
        .map(|import_op| {
            let existing_source_states = source_states_by_name.get(import_op.name.as_str());
            analyzer_ctx.analyze_import_op(
                &root_op_scope,
                import_op.clone(),
                Some(&mut setup_state.metadata),
                existing_source_states,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let op_scope_fut = analyzer_ctx.analyze_op_scope(&root_op_scope, &flow_inst.reactive_ops)?;

    #[derive(Default)]
    struct TargetOpGroup {
        export_op_ids: Vec<usize>,
        declarations: Vec<serde_json::Value>,
    }
    let mut target_op_group = IndexMap::<String, TargetOpGroup>::new();
    for (idx, export_op) in flow_inst.export_ops.iter().enumerate() {
        target_op_group
            .entry(export_op.spec.target.kind.clone())
            .or_default()
            .export_op_ids
            .push(idx);
    }
    for declaration in flow_inst.declarations.iter() {
        target_op_group
            .entry(declaration.kind.clone())
            .or_default()
            .declarations
            .push(serde_json::Value::Object(declaration.spec.clone()));
    }

    let mut export_ops_futs = vec![];
    let mut analyzed_target_op_groups = vec![];
    for (target_kind, op_ids) in target_op_group.into_iter() {
        let export_factory = match registry.get(&target_kind) {
            Some(ExecutorFactory::ExportTarget(export_executor)) => export_executor,
            _ => {
                bail!("Export target kind not found: {target_kind}");
            }
        };
        let analyzed_target_op_group = AnalyzedExportTargetOpGroup {
            target_factory: export_factory.clone(),
            op_idx: op_ids.export_op_ids,
        };
        export_ops_futs.extend(analyzer_ctx.analyze_export_op_group(
            target_kind,
            &root_op_scope,
            flow_inst,
            &analyzed_target_op_group,
            op_ids.declarations,
            &mut setup_state,
            &target_states_by_name_type,
        )?);
        analyzed_target_op_groups.push(analyzed_target_op_group);
    }

    let tracking_table_setup = setup_state.tracking_table.clone();

    let flow_schema = build_flow_schema(&root_op_scope)?;
    let logic_fingerprint = Fingerprinter::default()
        .with(&flow_inst)?
        .with(&flow_schema.schema)?
        .into_fingerprint();
    let plan_fut = async move {
        let (import_ops, op_scope, export_ops) = try_join3(
            try_join_all(import_ops_futs),
            op_scope_fut,
            try_join_all(export_ops_futs),
        )
        .await?;

        Ok(ExecutionPlan {
            tracking_table_setup,
            logic_fingerprint,
            import_ops,
            op_scope,
            export_ops,
            export_op_groups: analyzed_target_op_groups,
        })
    };

    Ok((flow_schema, plan_fut, setup_state))
}

pub fn analyze_transient_flow<'a>(
    flow_inst: &TransientFlowSpec,
    flow_ctx: &'_ Arc<FlowInstanceContext>,
) -> Result<(
    EnrichedValueType,
    FlowSchema,
    impl Future<Output = Result<TransientExecutionPlan>> + Send + 'a,
)> {
    let mut root_data_scope = DataScopeBuilder::new();
    let registry = crate::ops::executor_factory_registry();
    let analyzer_ctx = AnalyzerContext {
        registry: &registry,
        flow_ctx,
    };
    let mut input_fields = vec![];
    for field in flow_inst.input_fields.iter() {
        let analyzed_field = root_data_scope.add_field(field.name.clone(), &field.value_type)?;
        input_fields.push(analyzed_field);
    }
    let root_op_scope = OpScope::new(
        ROOT_SCOPE_NAME.to_string(),
        None,
        Arc::new(Mutex::new(root_data_scope)),
    );
    let op_scope_fut = analyzer_ctx.analyze_op_scope(&root_op_scope, &flow_inst.reactive_ops)?;
    let (output_value, output_type) =
        analyze_value_mapping(&flow_inst.output_value, &root_op_scope)?;
    let data_schema = build_flow_schema(&root_op_scope)?;
    let plan_fut = async move {
        let op_scope = op_scope_fut.await?;
        Ok(TransientExecutionPlan {
            input_fields,
            op_scope,
            output_value,
        })
    };
    Ok((output_type, data_schema, plan_fut))
}
