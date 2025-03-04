use std::collections::{BTreeMap, HashSet};
use std::sync::Mutex;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, u32};

use super::plan::*;
use crate::execution::db_tracking_setup;
use crate::setup::{
    self, DesiredMode, FlowSetupMetadata, FlowSetupState, ResourceIdentifier, SourceSetupState,
    TargetSetupState, TargetSetupStateCommon,
};
use crate::utils::fingerprint::Fingerprinter;
use crate::{
    api_bail, api_error,
    base::{
        schema::*,
        spec::*,
        value::{self, *},
    },
    ops::{interface::*, registry::*},
    utils::immutable::RefList,
};
use anyhow::{anyhow, bail, Context, Result};
use futures::future::try_join3;
use futures::{future::try_join_all, FutureExt};
use indexmap::IndexMap;
use log::warn;

#[derive(Debug)]
pub(super) enum ValueTypeBuilder {
    Basic(BasicValueType),
    Struct(StructSchemaBuilder),
    Collection(CollectionSchemaBuilder),
}

impl TryFrom<&ValueType> for ValueTypeBuilder {
    type Error = anyhow::Error;

    fn try_from(value_type: &ValueType) -> Result<Self> {
        match value_type {
            ValueType::Basic(basic_type) => Ok(ValueTypeBuilder::Basic(basic_type.clone())),
            ValueType::Struct(struct_type) => Ok(ValueTypeBuilder::Struct(struct_type.try_into()?)),
            ValueType::Collection(collection_type) => {
                Ok(ValueTypeBuilder::Collection(collection_type.try_into()?))
            }
        }
    }
}

impl TryInto<ValueType> for &ValueTypeBuilder {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<ValueType> {
        match self {
            ValueTypeBuilder::Basic(basic_type) => Ok(ValueType::Basic(basic_type.clone())),
            ValueTypeBuilder::Struct(struct_type) => Ok(ValueType::Struct(struct_type.try_into()?)),
            ValueTypeBuilder::Collection(collection_type) => {
                Ok(ValueType::Collection(collection_type.try_into()?))
            }
        }
    }
}

#[derive(Default, Debug)]
pub(super) struct StructSchemaBuilder {
    fields: Vec<FieldSchema<ValueTypeBuilder>>,
    field_name_idx: HashMap<FieldName, u32>,
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
                    .map(|f| FieldSchema::<ValueType>::from_alternative(f))
                    .collect::<Result<Vec<_>>>()?,
            ),
        })
    }
}

#[derive(Debug)]
pub(super) struct CollectionSchemaBuilder {
    pub kind: CollectionKind,
    pub sub_scope: Arc<Mutex<DataScopeBuilder>>,
}

impl TryFrom<&CollectionSchema> for CollectionSchemaBuilder {
    type Error = anyhow::Error;

    fn try_from(schema: &CollectionSchema) -> Result<Self> {
        Ok(Self {
            kind: schema.kind,
            sub_scope: Arc::new(Mutex::new(DataScopeBuilder {
                data: (&schema.row).try_into()?,
                collectors: Mutex::new(
                    schema
                        .collectors
                        .iter()
                        .map(|c| (c.name.clone(), CollectorBuilder::new(c.spec.clone())))
                        .collect(),
                ),
            })),
        })
    }
}

impl TryInto<CollectionSchema> for &CollectionSchemaBuilder {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<CollectionSchema> {
        let sub_scope = self.sub_scope.lock().unwrap();
        let row = (&sub_scope.data).try_into()?;
        let collectors = sub_scope
            .collectors
            .lock()
            .unwrap()
            .iter()
            .map(|(name, schema)| NamedSpec {
                name: name.clone(),
                spec: schema.schema.clone(),
            })
            .collect();
        Ok(CollectionSchema {
            kind: self.kind,
            row,
            collectors,
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
            let common_schema = try_make_common_struct_schemas(struct_type1, struct_type2)?;
            ValueType::Struct(common_schema)
        }
        (ValueType::Collection(collection_type1), ValueType::Collection(collection_type2)) => {
            if collection_type1.kind != collection_type2.kind {
                api_bail!(
                    "Collection types are not compatible: {} vs {}",
                    collection_type1,
                    collection_type2
                );
            }
            let row = try_make_common_struct_schemas(&collection_type1.row, &collection_type2.row)?;

            if collection_type1.collectors.len() != collection_type2.collectors.len() {
                api_bail!(
                    "Collection types are not compatible as they have different collectors count: {} vs {}",
                    collection_type1,
                    collection_type2
                );
            }
            let collectors = collection_type1
                .collectors
                .iter()
                .zip(collection_type2.collectors.iter())
                .map(|(c1, c2)| -> Result<_> {
                    if c1.name != c2.name {
                        api_bail!(
                            "Collection types are not compatible as they have different collectors names: {} vs {}",
                            c1.name,
                            c2.name
                        );
                    }
                    let collector = NamedSpec {
                        name: c1.name.clone(),
                        spec: try_make_common_struct_schemas(&c1.spec, &c2.spec)?,
                    };
                    Ok(collector)
                })
                .collect::<Result<_>>()?;

            ValueType::Collection(CollectionSchema {
                kind: collection_type1.kind,
                row,
                collectors,
            })
        }
        (t1 @ (ValueType::Basic(_) | ValueType::Struct(_) | ValueType::Collection(_)), t2) => {
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

fn try_make_common_struct_schemas(
    schema1: &StructSchema,
    schema2: &StructSchema,
) -> Result<StructSchema> {
    if schema1.fields.len() != schema2.fields.len() {
        api_bail!(
            "Structs are not compatible as they have different fields count:\n  {}\n  {}\n",
            schema1,
            schema2
        );
    }
    let mut result_fields = Vec::with_capacity(schema1.fields.len());
    for (field1, field2) in schema1.fields.iter().zip(schema2.fields.iter()) {
        if field1.name != field2.name {
            api_bail!(
                "Structs are not compatible as they have incompatible field names `{}` vs `{}`:\n  {}\n  {}\n",
                field1.name,
                field2.name,
                schema1,
                schema2
            );
        }
        result_fields.push(FieldSchema {
            name: field1.name.clone(),
            value_type: try_make_common_value_type(&field1.value_type, &field2.value_type)?,
        });
    }
    Ok(StructSchema {
        fields: Arc::new(result_fields),
    })
}

#[derive(Debug)]
pub(super) struct CollectorBuilder {
    pub schema: StructSchema,
    pub is_used: bool,
}

impl CollectorBuilder {
    pub fn new(schema: StructSchema) -> Self {
        Self {
            schema,
            is_used: false,
        }
    }

    pub fn merge_schema(&mut self, schema: &StructSchema) -> Result<()> {
        if self.is_used {
            api_bail!("Collector is already used");
        }
        let common_schema =
            try_make_common_struct_schemas(&self.schema, &schema).with_context(|| {
                format!(
                    "Collectors are sent with entries in incompatible schemas:\n  {}\n  {}\n",
                    self.schema, schema
                )
            })?;
        self.schema = common_schema;
        Ok(())
    }

    pub fn use_schema(&mut self) -> StructSchema {
        self.is_used = true;
        self.schema.clone()
    }
}

#[derive(Debug)]
pub(super) struct DataScopeBuilder {
    pub data: StructSchemaBuilder,
    pub collectors: Mutex<IndexMap<FieldName, CollectorBuilder>>,
}

impl DataScopeBuilder {
    pub fn new() -> Self {
        Self {
            data: Default::default(),
            collectors: Default::default(),
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
                ValueTypeBuilder::Struct(struct_type) => &struct_type,
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

    pub fn consume_collector(
        &self,
        collector_name: &FieldName,
    ) -> Result<(AnalyzedLocalCollectorReference, StructSchema)> {
        let mut collectors = self.collectors.lock().unwrap();
        let (collector_idx, _, collector) = collectors
            .get_full_mut(collector_name)
            .ok_or_else(|| api_error!("Collector not found: {}", collector_name))?;
        Ok((
            AnalyzedLocalCollectorReference {
                collector_idx: collector_idx as u32,
            },
            collector.use_schema(),
        ))
    }

    pub fn add_collector(
        &self,
        collector_name: FieldName,
        schema: StructSchema,
    ) -> Result<AnalyzedLocalCollectorReference> {
        let mut collectors = self.collectors.lock().unwrap();
        let collector_idx = collectors.len() as u32;
        match collectors.entry(collector_name) {
            indexmap::map::Entry::Occupied(mut entry) => {
                entry.get_mut().merge_schema(&schema)?;
            }
            indexmap::map::Entry::Vacant(entry) => {
                entry.insert(CollectorBuilder::new(schema));
            }
        }
        Ok(AnalyzedLocalCollectorReference { collector_idx })
    }

    pub fn into_data_schema(self) -> Result<DataSchema> {
        Ok(DataSchema {
            schema: (&self.data).try_into()?,
            collectors: self
                .collectors
                .into_inner()
                .unwrap()
                .into_iter()
                .map(|(name, schema)| NamedSpec {
                    name,
                    spec: schema.schema,
                })
                .collect(),
        })
    }
}

pub(super) struct AnalyzerContext<'a> {
    pub registry: &'a ExecutorFactoryRegistry,
    pub flow_ctx: &'a Arc<FlowInstanceContext>,
}

pub(super) struct ExecutionScope<'a> {
    pub name: &'a str,
    pub data: &'a mut DataScopeBuilder,
}

fn find_scope<'a>(
    scope_name: &ScopeName,
    scopes: RefList<'a, &'a ExecutionScope<'a>>,
) -> Result<(u32, &'a ExecutionScope<'a>)> {
    let (up_level, scope) = scopes
        .iter()
        .enumerate()
        .find(|(_, s)| s.name == scope_name)
        .ok_or_else(|| api_error!("Scope not found: {}", scope_name))?;
    Ok((up_level as u32, scope))
}

fn analyze_struct_mapping(
    mapping: &StructMapping,
    scopes: RefList<'_, &'_ ExecutionScope<'_>>,
) -> Result<(AnalyzedStructMapping, StructSchema)> {
    let mut field_mappings = Vec::with_capacity(mapping.fields.len());
    let mut field_schemas = Vec::with_capacity(mapping.fields.len());
    for field in mapping.fields.iter() {
        let (field_mapping, value_type) = analyze_value_mapping(&field.spec, scopes)?;
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
        StructSchema {
            fields: Arc::new(field_schemas),
        },
    ))
}

fn analyze_value_mapping(
    value_mapping: &ValueMapping,
    scopes: RefList<'_, &'_ ExecutionScope<'_>>,
) -> Result<(AnalyzedValueMapping, EnrichedValueType)> {
    let result = match value_mapping {
        ValueMapping::Literal(v) => {
            let (value_type, basic_value) = match &v.value {
                serde_json::Value::String(s) => {
                    (BasicValueType::Str, BasicValue::Str(Arc::from(s.as_str())))
                }
                serde_json::Value::Number(n) => (
                    BasicValueType::Float64,
                    BasicValue::Float64(
                        n.as_f64().ok_or_else(|| anyhow!("Invalid number: {}", n))?,
                    ),
                ),
                serde_json::Value::Bool(b) => (BasicValueType::Bool, BasicValue::Bool(*b)),
                _ => bail!("Unsupported value type: {}", v.value),
            };
            (
                AnalyzedValueMapping::Literal {
                    value: value::Value::Basic(basic_value),
                },
                EnrichedValueType {
                    typ: ValueType::Basic(value_type),
                    nullable: false,
                    attrs: Default::default(),
                },
            )
        }

        ValueMapping::Field(v) => {
            let (scope_up_level, exec_scope) = match &v.scope {
                Some(scope) => find_scope(scope, scopes)?,
                None => (0, *scopes.head().ok_or_else(|| anyhow!("Scope not found"))?),
            };
            let (local_field_ref, value_type) =
                exec_scope.data.analyze_field_path(&v.field_path)?;
            (
                AnalyzedValueMapping::Field(AnalyzedFieldReference {
                    local: local_field_ref,
                    scope_up_level: scope_up_level as u32,
                }),
                EnrichedValueType::from_alternative(value_type)?,
            )
        }

        ValueMapping::Struct(v) => {
            let (struct_mapping, struct_schema) = analyze_struct_mapping(v, scopes)?;
            (
                AnalyzedValueMapping::Struct(struct_mapping),
                EnrichedValueType {
                    typ: ValueType::Struct(struct_schema),
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
    scopes: RefList<'_, &'_ ExecutionScope<'_>>,
) -> Result<Vec<OpArgSchema>> {
    let mut input_field_schemas = Vec::with_capacity(arg_bindings.len());
    for arg_binding in arg_bindings.iter() {
        let (analyzed_value, value_type) = analyze_value_mapping(&arg_binding.value, scopes)?;
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
    schema: StructSchema,
    scopes: RefList<'_, &'_ ExecutionScope<'_>>,
) -> Result<AnalyzedCollectorReference> {
    let (scope_up_level, scope) = find_scope(scope_name, scopes)?;
    let local_ref = scope.data.add_collector(collector_name, schema)?;
    Ok(AnalyzedCollectorReference {
        local: local_ref,
        scope_up_level,
    })
}

impl<'a> AnalyzerContext<'a> {
    pub(super) fn analyze_source_op(
        &self,
        scope: &mut DataScopeBuilder,
        source_op: NamedSpec<OpSpec>,
        metadata: Option<&mut FlowSetupMetadata>,
        existing_source_states: Option<&Vec<&SourceSetupState>>,
    ) -> Result<impl Future<Output = Result<AnalyzedSourceOp>> + Send> {
        let factory = self.registry.get(&source_op.spec.kind);
        let source_factory = match factory {
            Some(ExecutorFactory::Source(source_executor)) => source_executor.clone(),
            _ => {
                return Err(anyhow::anyhow!(
                    "Source executor not found for kind: {}",
                    source_op.spec.kind
                ))
            }
        };
        let (output_type, executor) = source_factory.build(
            serde_json::Value::Object(source_op.spec.spec),
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
                .map(|v| v.iter())
                .flatten()
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
                source_op.name.clone(),
                SourceSetupState {
                    source_id,
                    key_schema: key_schema_no_attrs,
                },
            );
            source_id
        });

        let op_name = source_op.name.clone();
        let output = scope.add_field(source_op.name, &output_type)?;
        let result_fut = async move {
            Ok(AnalyzedSourceOp {
                source_id: source_id.unwrap_or_default(),
                executor: executor.await?,
                output,
                primary_key_type: output_type
                    .typ
                    .key_type()
                    .ok_or_else(|| api_error!("Source must produce a type with key: {op_name}"))?
                    .typ
                    .clone(),
                name: op_name,
            })
        };
        Ok(result_fut)
    }

    pub(super) fn analyze_reactive_op(
        &self,
        scope: &mut ExecutionScope<'_>,
        reactive_op: &NamedSpec<ReactiveOpSpec>,
        parent_scopes: RefList<'_, &'_ ExecutionScope<'_>>,
    ) -> Result<Pin<Box<dyn Future<Output = Result<AnalyzedReactiveOp>> + Send>>> {
        let result_fut = match &reactive_op.spec {
            ReactiveOpSpec::Transform(op) => {
                let input_field_schemas =
                    analyze_input_fields(&op.inputs, parent_scopes.prepend(scope)).with_context(
                        || {
                            format!(
                                "Failed to analyze inputs for transform op: {}",
                                reactive_op.name
                            )
                        },
                    )?;
                let spec = serde_json::Value::Object(op.op.spec.clone());

                let factory = self.registry.get(&op.op.kind);
                match factory {
                    Some(ExecutorFactory::SimpleFunction(fn_executor)) => {
                        let input_value_mappings = input_field_schemas
                            .iter()
                            .map(|field| field.analyzed_value.clone())
                            .collect();
                        let (output_type, executor) = fn_executor.clone().build(
                            spec,
                            input_field_schemas,
                            self.flow_ctx.clone(),
                        )?;
                        let output = scope
                            .data
                            .add_field(reactive_op.name.clone(), &output_type)?;
                        let reactive_op = reactive_op.clone();
                        async move {
                            let executor = executor.await.with_context(|| {
                                format!("Failed to build executor for transform op: {}", reactive_op.name)
                            })?;
                            let behavior_version = executor.behavior_version();
                            let function_exec_info = AnalyzedFunctionExecInfo {
                                enable_cache: executor.enable_cache(),
                                behavior_version,
                                fingerprinter: Fingerprinter::default()
                                    .with(&reactive_op.name)?
                                    .with(&reactive_op.spec)?
                                    .with(&behavior_version)?
                                    .with(&output_type.without_attrs())?,
                                output_type: output_type.typ.clone(),
                            };
                            if function_exec_info.enable_cache
                                && function_exec_info.behavior_version.is_none()
                            {
                                api_bail!(
                                    "When caching is enabled, behavior version must be specified for transform op: {}",
                                    reactive_op.name
                                );
                            }
                            Ok(AnalyzedReactiveOp::Transform(AnalyzedTransformOp {
                                name: reactive_op.name,
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
                        ))
                    }
                }
            }
            ReactiveOpSpec::ForEach(op) => {
                let (local_field_ref, value_type) =
                    scope.data.analyze_field_path(&op.field_path)?;
                let sub_scope = match &value_type.typ {
                    ValueTypeBuilder::Collection(collection_type) => &collection_type.sub_scope,
                    _ => api_bail!(
                        "ForEach only works on collection, field {} is not",
                        op.field_path
                    ),
                };
                let op_scope_fut = {
                    let mut sub_scope = sub_scope.lock().unwrap();
                    let mut exec_scope = ExecutionScope {
                        name: &op.op_scope.name,
                        data: &mut sub_scope,
                    };
                    self.analyze_op_scope(
                        &mut exec_scope,
                        &op.op_scope.ops,
                        parent_scopes.prepend(&scope),
                    )?
                };
                let op_name = reactive_op.name.clone();
                async move {
                    Ok(AnalyzedReactiveOp::ForEach(AnalyzedForEachOp {
                        local_field_ref,
                        op_scope: op_scope_fut
                            .await
                            .with_context(|| format!("Analyzing foreach op: {op_name}"))?,
                        name: op_name,
                    }))
                }
                .boxed()
            }

            ReactiveOpSpec::Collect(op) => {
                let scopes = parent_scopes.prepend(scope);
                let (struct_mapping, struct_schema) = analyze_struct_mapping(&op.input, scopes)?;
                let collector_ref = add_collector(
                    &op.scope_name,
                    op.collector_name.clone(),
                    struct_schema,
                    scopes,
                )?;
                let op_name = reactive_op.name.clone();
                async move {
                    Ok(AnalyzedReactiveOp::Collect(AnalyzedCollectOp {
                        name: op_name,
                        input: struct_mapping,
                        collector_ref,
                    }))
                }
                .boxed()
            }
        };
        Ok(result_fut)
    }

    pub(super) fn analyze_export_op(
        &self,
        scope: &mut DataScopeBuilder,
        export_op: NamedSpec<ExportOpSpec>,
        setup_state: Option<&mut FlowSetupState<DesiredMode>>,
        existing_target_states: &HashMap<&ResourceIdentifier, Vec<&TargetSetupState>>,
    ) -> Result<impl Future<Output = Result<AnalyzedExportOp>> + Send> {
        let export_target = export_op.spec.target;
        let export_factory = match self.registry.get(&export_target.kind) {
            Some(ExecutorFactory::ExportTarget(export_executor)) => export_executor,
            _ => {
                return Err(anyhow::anyhow!(
                    "Export target kind not found: {}",
                    export_target.kind
                ))
            }
        };

        let spec = serde_json::Value::Object(export_target.spec.clone());
        let (local_collector_ref, collector_schema) =
            scope.consume_collector(&export_op.spec.collector_name)?;
        let (
            key_fields_schema,
            value_fields_schema,
            primary_key_def,
            primary_key_type,
            value_fields_idx,
        ) = match &export_op.spec.index_options.primary_key_fields {
            Some(fields) => {
                let pk_fields_idx = fields
                    .iter()
                    .map(|f| {
                        collector_schema
                            .fields
                            .iter()
                            .position(|field| &field.name == f)
                            .map(|idx| idx as u32)
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
                    })
                };
                let mut value_fields_schema: Vec<FieldSchema> = vec![];
                let mut value_fields_idx = vec![];
                for (idx, field) in collector_schema.fields.iter().enumerate() {
                    if !pk_fields_idx.contains(&(idx as u32)) {
                        value_fields_schema.push(field.clone());
                        value_fields_idx.push(idx as u32);
                    }
                }
                (
                    key_fields_schema,
                    value_fields_schema,
                    AnalyzedPrimaryKeyDef::Fields(pk_fields_idx),
                    primary_key_type,
                    value_fields_idx,
                )
            }
            None => {
                // TODO: Support auto-generate primary key
                api_bail!("Primary key fields must be specified")
            }
        };

        let target_id: i32 = 1; // TODO: Fill it with a meaningful value automatically
        let ((setup_key, desired_state), executor_futs) = export_factory.clone().build(
            export_op.name.clone(),
            target_id,
            spec,
            key_fields_schema,
            value_fields_schema,
            export_op.spec.index_options,
            self.flow_ctx.clone(),
        )?;
        let resource_id = ResourceIdentifier {
            key: setup_key.clone(),
            target_kind: export_target.kind.clone(),
        };
        let existing_target_states = existing_target_states.get(&resource_id);
        let target_id = setup_state
            .map(|setup_state| -> Result<i32> {
                let existing_target_ids = existing_target_states
                    .iter()
                    .map(|v| v.iter())
                    .flatten()
                    .map(|state| state.common.target_id)
                    .collect::<HashSet<_>>();
                let target_id = if existing_target_ids.len() == 1 {
                    existing_target_ids.into_iter().next().unwrap()
                } else {
                    if existing_target_ids.len() > 1 {
                        warn!("Multiple target states with the same key schema found");
                    }
                    setup_state.metadata.last_target_id += 1;
                    setup_state.metadata.last_target_id
                };
                let max_schema_version_id = existing_target_states
                    .iter()
                    .map(|v| v.iter())
                    .flatten()
                    .map(|s| s.common.max_schema_version_id)
                    .max()
                    .unwrap_or(0);
                let reusable_schema_version_ids = existing_target_states
                    .iter()
                    .map(|v| v.iter())
                    .flatten()
                    .map(|s| {
                        Ok({
                            if export_factory.will_keep_all_existing_data(
                                &export_op.name,
                                target_id,
                                &desired_state,
                                &s.state,
                            )? {
                                Some(s.common.schema_version_id)
                            } else {
                                None
                            }
                        })
                    })
                    .collect::<Result<HashSet<_>>>()?;
                let schema_version_id = if reusable_schema_version_ids.len() == 1 {
                    reusable_schema_version_ids
                        .into_iter()
                        .next()
                        .unwrap()
                        .unwrap_or(max_schema_version_id + 1)
                } else {
                    max_schema_version_id + 1
                };
                match setup_state.targets.entry(ResourceIdentifier {
                    key: setup_key,
                    target_kind: export_target.kind.clone(),
                }) {
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
                            },
                            state: desired_state,
                        });
                    }
                }
                Ok(target_id)
            })
            .transpose()?;

        Ok(async move {
            let (executor, query_target) = executor_futs
                .await
                .with_context(|| format!("Analyzing export op: {}", export_op.name))?;
            let name = export_op.name;
            Ok(AnalyzedExportOp {
                name,
                target_id: target_id.unwrap_or_default(),
                input: local_collector_ref,
                executor,
                query_target,
                primary_key_def,
                primary_key_type,
                value_fields: value_fields_idx,
            })
        })
    }

    fn analyze_op_scope(
        &self,
        scope: &mut ExecutionScope<'_>,
        reactive_ops: &[NamedSpec<ReactiveOpSpec>],
        parent_scopes: RefList<'_, &'_ ExecutionScope<'_>>,
    ) -> Result<impl Future<Output = Result<AnalyzedOpScope>> + Send> {
        let op_futs = reactive_ops
            .iter()
            .map(|reactive_op| self.analyze_reactive_op(scope, reactive_op, parent_scopes))
            .collect::<Result<Vec<_>>>()?;
        let result_fut = async move {
            Ok(AnalyzedOpScope {
                reactive_ops: try_join_all(op_futs).await?,
            })
        };
        Ok(result_fut)
    }
}

pub fn build_flow_instance_context(flow_inst_name: &str) -> Arc<FlowInstanceContext> {
    Arc::new(FlowInstanceContext {
        flow_instance_name: flow_inst_name.to_string(),
    })
}

pub fn analyze_flow(
    flow_inst: &FlowInstanceSpec,
    flow_ctx: &Arc<FlowInstanceContext>,
    existing_flow_ss: Option<&setup::FlowSetupState<setup::ExistingMode>>,
    registry: &ExecutorFactoryRegistry,
) -> Result<(
    DataSchema,
    impl Future<Output = Result<ExecutionPlan>> + Send,
    setup::FlowSetupState<setup::DesiredMode>,
)> {
    let mut root_data_scope = DataScopeBuilder::new();

    let existing_metadata_versions = || {
        existing_flow_ss
            .iter()
            .map(|flow_ss| flow_ss.metadata.possible_versions())
            .flatten()
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
            .map(|flow_ss| flow_ss.seen_flow_metadata_version)
            .flatten(),
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
                .map(|flow_ss| {
                    flow_ss
                        .tracking_table
                        .current
                        .as_ref()
                        .map(|v| v.table_name.clone())
                })
                .flatten()
                .unwrap_or_else(|| db_tracking_setup::default_tracking_table_name(&flow_inst.name)),
            version_id: db_tracking_setup::CURRENT_TRACKING_TABLE_VERSION,
        },
        // TODO: Fill it with a meaningful value.
        targets: IndexMap::new(),
    };
    let plan_fut = {
        let analyzer_ctx = AnalyzerContext { registry, flow_ctx };
        let mut root_exec_scope = ExecutionScope {
            name: ROOT_SCOPE_NAME,
            data: &mut root_data_scope,
        };
        let source_ops_futs = flow_inst
            .source_ops
            .iter()
            .map(|source_op| {
                let existing_source_states = source_states_by_name.get(source_op.name.as_str());
                analyzer_ctx.analyze_source_op(
                    &mut root_exec_scope.data,
                    source_op.clone(),
                    Some(&mut setup_state.metadata),
                    existing_source_states,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let op_scope_fut = analyzer_ctx.analyze_op_scope(
            &mut root_exec_scope,
            &flow_inst.reactive_ops,
            RefList::Nil,
        )?;
        let export_ops_futs = flow_inst
            .export_ops
            .iter()
            .map(|export_op| {
                analyzer_ctx.analyze_export_op(
                    &mut root_exec_scope.data,
                    export_op.clone(),
                    Some(&mut setup_state),
                    &target_states_by_name_type,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        let tracking_table_setup = setup_state.tracking_table.clone();
        async move {
            let (source_ops, op_scope, export_ops) = try_join3(
                try_join_all(source_ops_futs),
                op_scope_fut,
                try_join_all(export_ops_futs),
            )
            .await?;

            Ok(ExecutionPlan {
                tracking_table_setup,
                logic_fingerprint: vec![0; 8], // TODO: Fill it with a meaningful value automatically
                source_ops,
                op_scope,
                export_ops,
            })
        }
    };

    Ok((root_data_scope.into_data_schema()?, plan_fut, setup_state))
}

pub fn analyze_transient_flow<'a>(
    flow_inst: &TransientFlowSpec,
    flow_ctx: &'_ Arc<FlowInstanceContext>,
    registry: &'a ExecutorFactoryRegistry,
) -> Result<(
    EnrichedValueType,
    DataSchema,
    impl Future<Output = Result<TransientExecutionPlan>> + Send + 'a,
)> {
    let mut root_data_scope = DataScopeBuilder::new();
    let analyzer_ctx = AnalyzerContext { registry, flow_ctx };
    let mut input_fields = vec![];
    for field in flow_inst.input_fields.iter() {
        let analyzed_field = root_data_scope.add_field(field.name.clone(), &field.value_type)?;
        input_fields.push(analyzed_field);
    }
    let mut root_exec_scope = ExecutionScope {
        name: ROOT_SCOPE_NAME,
        data: &mut root_data_scope,
    };
    let op_scope_fut = analyzer_ctx.analyze_op_scope(
        &mut root_exec_scope,
        &flow_inst.reactive_ops,
        RefList::Nil,
    )?;
    let (output_value, output_type) = analyze_value_mapping(
        &flow_inst.output_value,
        RefList::Nil.prepend(&root_exec_scope),
    )?;
    let plan_fut = async move {
        let op_scope = op_scope_fut.await?;
        Ok(TransientExecutionPlan {
            input_fields,
            op_scope,
            output_value,
        })
    };
    Ok((output_type, root_data_scope.into_data_schema()?, plan_fut))
}
