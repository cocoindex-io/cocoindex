use crate::execution::indexing_status::SourceLogicFingerprint;
use crate::prelude::*;

use futures::future::try_join_all;
use tokio::time::Duration;

use crate::base::value::EstimatedByteSize;
use crate::base::{schema, value};
use crate::builder::{AnalyzedTransientFlow, plan::*};
use utils::immutable::RefList;

use super::memoization::{EvaluationMemory, EvaluationMemoryOptions, evaluate_with_cell};

const DEFAULT_TIMEOUT_THRESHOLD: Duration = Duration::from_secs(1800);
const MIN_WARNING_THRESHOLD: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub struct ScopeValueBuilder {
    // TODO: Share the same lock for values produced in the same execution scope, for stricter atomicity.
    pub fields: Vec<OnceLock<value::Value<ScopeValueBuilder>>>,
}

impl value::EstimatedByteSize for ScopeValueBuilder {
    fn estimated_detached_byte_size(&self) -> usize {
        self.fields
            .iter()
            .map(|f| f.get().map_or(0, |v| v.estimated_byte_size()))
            .sum()
    }
}

impl From<&ScopeValueBuilder> for value::ScopeValue {
    fn from(val: &ScopeValueBuilder) -> Self {
        value::ScopeValue(value::FieldValues {
            fields: val
                .fields
                .iter()
                .map(|f| value::Value::from_alternative_ref(f.get().unwrap()))
                .collect(),
        })
    }
}

impl From<ScopeValueBuilder> for value::ScopeValue {
    fn from(val: ScopeValueBuilder) -> Self {
        value::ScopeValue(value::FieldValues {
            fields: val
                .fields
                .into_iter()
                .map(|f| value::Value::from_alternative(f.into_inner().unwrap()))
                .collect(),
        })
    }
}

impl ScopeValueBuilder {
    fn new(num_fields: usize) -> Self {
        let mut fields = Vec::with_capacity(num_fields);
        fields.resize_with(num_fields, OnceLock::new);
        Self { fields }
    }

    fn augmented_from(source: &value::ScopeValue, schema: &schema::TableSchema) -> Result<Self> {
        let val_index_base = schema.key_schema().len();
        let len = schema.row.fields.len() - val_index_base;

        let mut builder = Self::new(len);

        let value::ScopeValue(source_fields) = source;
        for ((v, t), r) in source_fields
            .fields
            .iter()
            .zip(schema.row.fields[val_index_base..(val_index_base + len)].iter())
            .zip(&mut builder.fields)
        {
            r.set(augmented_value(v, &t.value_type.typ)?)
                .map_err(|_| internal_error!("Value of field `{}` is already set", t.name))?;
        }
        Ok(builder)
    }
}

fn augmented_value(
    val: &value::Value,
    val_type: &schema::ValueType,
) -> Result<value::Value<ScopeValueBuilder>> {
    let value = match (val, val_type) {
        (value::Value::Null, _) => value::Value::Null,
        (value::Value::Basic(v), _) => value::Value::Basic(v.clone()),
        (value::Value::Struct(v), schema::ValueType::Struct(t)) => {
            value::Value::Struct(value::FieldValues {
                fields: v
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(i, v)| augmented_value(v, &t.fields[i].value_type.typ))
                    .collect::<Result<Vec<_>>>()?,
            })
        }
        (value::Value::UTable(v), schema::ValueType::Table(t)) => value::Value::UTable(
            v.iter()
                .map(|v| ScopeValueBuilder::augmented_from(v, t))
                .collect::<Result<Vec<_>>>()?,
        ),
        (value::Value::KTable(v), schema::ValueType::Table(t)) => value::Value::KTable(
            v.iter()
                .map(|(k, v)| Ok((k.clone(), ScopeValueBuilder::augmented_from(v, t)?)))
                .collect::<Result<BTreeMap<_, _>>>()?,
        ),
        (value::Value::LTable(v), schema::ValueType::Table(t)) => value::Value::LTable(
            v.iter()
                .map(|v| ScopeValueBuilder::augmented_from(v, t))
                .collect::<Result<Vec<_>>>()?,
        ),
        (val, _) => internal_bail!("Value kind doesn't match the type {val_type}: {val:?}"),
    };
    Ok(value)
}

enum ScopeKey<'a> {
    /// For root struct and UTable.
    None,
    /// For KTable row.
    MapKey(&'a value::KeyValue),
    /// For LTable row.
    ListIndex(usize),
}

impl<'a> ScopeKey<'a> {
    pub fn key(&self) -> Option<Cow<'a, value::KeyValue>> {
        match self {
            ScopeKey::None => None,
            ScopeKey::MapKey(k) => Some(Cow::Borrowed(&k)),
            ScopeKey::ListIndex(i) => {
                Some(Cow::Owned(value::KeyValue::from_single_part(*i as i64)))
            }
        }
    }

    pub fn value_field_index_base(&self) -> usize {
        match *self {
            ScopeKey::None => 0,
            ScopeKey::MapKey(v) => v.len(),
            ScopeKey::ListIndex(_) => 0,
        }
    }
}

impl std::fmt::Display for ScopeKey<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScopeKey::None => write!(f, "()"),
            ScopeKey::MapKey(k) => write!(f, "{k}"),
            ScopeKey::ListIndex(i) => write!(f, "[{i}]"),
        }
    }
}

struct ScopeEntry<'a> {
    key: ScopeKey<'a>,
    value: &'a ScopeValueBuilder,
    schema: &'a schema::StructSchema,
    collected_values: Vec<Mutex<Vec<value::FieldValues>>>,
}

impl<'a> ScopeEntry<'a> {
    fn new(
        key: ScopeKey<'a>,
        value: &'a ScopeValueBuilder,
        schema: &'a schema::StructSchema,
        analyzed_op_scope: &AnalyzedOpScope,
    ) -> Self {
        let mut collected_values = Vec::with_capacity(analyzed_op_scope.collector_len);
        collected_values.resize_with(analyzed_op_scope.collector_len, Default::default);

        Self {
            key,
            value,
            schema,
            collected_values,
        }
    }

    fn get_local_field_schema<'b>(
        schema: &'b schema::StructSchema,
        indices: &[u32],
    ) -> Result<&'b schema::FieldSchema> {
        let field_idx = indices[0] as usize;
        let field_schema = &schema.fields[field_idx];
        let result = if indices.len() == 1 {
            field_schema
        } else {
            let struct_field_schema = match &field_schema.value_type.typ {
                schema::ValueType::Struct(s) => s,
                _ => internal_bail!("Expect struct field"),
            };
            Self::get_local_field_schema(struct_field_schema, &indices[1..])?
        };
        Ok(result)
    }

    fn get_local_key_field<'b>(
        key_val: &'b value::KeyPart,
        indices: &'_ [u32],
    ) -> Result<&'b value::KeyPart> {
        let result = if indices.is_empty() {
            key_val
        } else if let value::KeyPart::Struct(fields) = key_val {
            Self::get_local_key_field(&fields[indices[0] as usize], &indices[1..])?
        } else {
            internal_bail!("Only struct can be accessed by sub field");
        };
        Ok(result)
    }

    fn get_local_field<'b>(
        val: &'b value::Value<ScopeValueBuilder>,
        indices: &'_ [u32],
    ) -> Result<&'b value::Value<ScopeValueBuilder>> {
        let result = if indices.is_empty() {
            val
        } else if let value::Value::Null = val {
            val
        } else if let value::Value::Struct(fields) = val {
            Self::get_local_field(&fields.fields[indices[0] as usize], &indices[1..])?
        } else {
            internal_bail!("Only struct can be accessed by sub field");
        };
        Ok(result)
    }

    fn get_value_field_builder(
        &self,
        field_ref: &AnalyzedLocalFieldReference,
    ) -> Result<&value::Value<ScopeValueBuilder>> {
        let first_index = field_ref.fields_idx[0] as usize;
        let index_base = self.key.value_field_index_base();
        let val = self.value.fields[(first_index - index_base) as usize]
            .get()
            .ok_or_else(|| internal_error!("Field {} is not set", first_index))?;
        Self::get_local_field(val, &field_ref.fields_idx[1..])
    }

    fn get_field(&self, field_ref: &AnalyzedLocalFieldReference) -> Result<value::Value> {
        let first_index = field_ref.fields_idx[0] as usize;
        let index_base = self.key.value_field_index_base();
        let result = if first_index < index_base {
            let key_val = self
                .key
                .key()
                .ok_or_else(|| internal_error!("Key is not set"))?;
            let key_part =
                Self::get_local_key_field(&key_val[first_index], &field_ref.fields_idx[1..])?;
            key_part.clone().into()
        } else {
            let val = self.value.fields[(first_index - index_base) as usize]
                .get()
                .ok_or_else(|| internal_error!("Field {} is not set", first_index))?;
            let val_part = Self::get_local_field(val, &field_ref.fields_idx[1..])?;
            value::Value::from_alternative_ref(val_part)
        };
        Ok(result)
    }

    fn get_field_schema(
        &self,
        field_ref: &AnalyzedLocalFieldReference,
    ) -> Result<&schema::FieldSchema> {
        Ok(Self::get_local_field_schema(
            self.schema,
            &field_ref.fields_idx,
        )?)
    }

    fn define_field_w_builder(
        &self,
        output_field: &AnalyzedOpOutput,
        val: value::Value<ScopeValueBuilder>,
    ) -> Result<()> {
        let field_index = output_field.field_idx as usize;
        let index_base = self.key.value_field_index_base() as usize;
        self.value.fields[field_index - index_base].set(val).map_err(|_| {
            internal_error!("Field {field_index} for scope is already set, violating single-definition rule.")
        })?;
        Ok(())
    }

    fn define_field(&self, output_field: &AnalyzedOpOutput, val: &value::Value) -> Result<()> {
        let field_index = output_field.field_idx as usize;
        let field_schema = &self.schema.fields[field_index];
        let val = augmented_value(val, &field_schema.value_type.typ)?;
        self.define_field_w_builder(output_field, val)?;
        Ok(())
    }
}

fn assemble_value(
    value_mapping: &AnalyzedValueMapping,
    scoped_entries: RefList<'_, &ScopeEntry<'_>>,
) -> Result<value::Value> {
    let result = match value_mapping {
        AnalyzedValueMapping::Constant { value } => value.clone(),
        AnalyzedValueMapping::Field(field_ref) => scoped_entries
            .headn(field_ref.scope_up_level as usize)
            .ok_or_else(|| internal_error!("Invalid scope_up_level: {}", field_ref.scope_up_level))?
            .get_field(&field_ref.local)?,
        AnalyzedValueMapping::Struct(mapping) => {
            let fields = mapping
                .fields
                .iter()
                .map(|f| assemble_value(f, scoped_entries))
                .collect::<Result<Vec<_>>>()?;
            value::Value::Struct(value::FieldValues { fields })
        }
    };
    Ok(result)
}

fn assemble_input_values<'a>(
    value_mappings: &'a [AnalyzedValueMapping],
    scoped_entries: RefList<'a, &ScopeEntry<'a>>,
) -> impl Iterator<Item = Result<value::Value>> + 'a {
    value_mappings
        .iter()
        .map(move |value_mapping| assemble_value(value_mapping, scoped_entries))
}

async fn evaluate_child_op_scope(
    op_scope: &AnalyzedOpScope,
    scoped_entries: RefList<'_, &ScopeEntry<'_>>,
    child_scope_entry: ScopeEntry<'_>,
    concurrency_controller: &concur_control::ConcurrencyController,
    memory: &EvaluationMemory,
    operation_in_process_stats: Option<&execution::stats::OperationInProcessStats>,
) -> Result<()> {
    let _permit = concurrency_controller
        .acquire(Some(|| {
            child_scope_entry
                .value
                .fields
                .iter()
                .map(|f| f.get().map_or(0, |v| v.estimated_byte_size()))
                .sum()
        }))
        .await?;
    evaluate_op_scope(
        op_scope,
        scoped_entries.prepend(&child_scope_entry),
        memory,
        operation_in_process_stats,
    )
    .await
    .with_context(|| {
        format!(
            "Evaluating in scope with key {}",
            match child_scope_entry.key.key() {
                Some(k) => k.to_string(),
                None => "()".to_string(),
            }
        )
    })
}

async fn evaluate_with_timeout_and_warning<F, T>(
    eval_future: F,
    timeout_duration: Duration,
    warn_duration: Duration,
    op_kind: String,
    op_name: String,
) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    let mut eval_future = Box::pin(eval_future);
    let mut to_warn = warn_duration < timeout_duration;
    let timeout_future = tokio::time::sleep(timeout_duration);
    tokio::pin!(timeout_future);

    loop {
        tokio::select! {
            res = &mut eval_future => {
                return res;
            }
            _ = &mut timeout_future => {
                return Err(internal_error!(
                    "Function '{}' ({}) timed out after {} seconds",
                    op_kind, op_name, timeout_duration.as_secs()
                ));
            }
            _ = tokio::time::sleep(warn_duration), if to_warn => {
                warn!(
                    "Function '{}' ({}) is taking longer than {}s (will be timed out after {}s)",
                    op_kind, op_name, warn_duration.as_secs(), timeout_duration.as_secs()
                );
                to_warn = false;
            }
        }
    }
}

async fn evaluate_op_scope(
    op_scope: &AnalyzedOpScope,
    scoped_entries: RefList<'_, &ScopeEntry<'_>>,
    memory: &EvaluationMemory,
    operation_in_process_stats: Option<&execution::stats::OperationInProcessStats>,
) -> Result<()> {
    let head_scope = *scoped_entries.head().unwrap();
    for reactive_op in op_scope.reactive_ops.iter() {
        match reactive_op {
            AnalyzedReactiveOp::Transform(op) => {
                // Track transform operation start
                if let Some(ref op_stats) = operation_in_process_stats {
                    let transform_key =
                        format!("transform/{}{}", op_scope.scope_qualifier, op.name);
                    op_stats.start_processing(&transform_key, 1);
                }

                let mut input_values = Vec::with_capacity(op.inputs.len());
                for value in assemble_input_values(&op.inputs, scoped_entries) {
                    input_values.push(value?);
                }

                let timeout_duration = op
                    .function_exec_info
                    .timeout
                    .unwrap_or(DEFAULT_TIMEOUT_THRESHOLD);
                let warn_duration = std::cmp::max(timeout_duration / 2, MIN_WARNING_THRESHOLD);

                let op_name_for_warning = op.name.clone();
                let op_kind_for_warning = op.op_kind.clone();

                let result = if op.function_exec_info.enable_cache {
                    let output_value_cell = memory.get_cache_entry(
                        || -> Result<_> {
                            Ok(op
                                .function_exec_info
                                .fingerprinter
                                .clone()
                                .with(&input_values)
                                .map(|fp| fp.into_fingerprint())?)
                        },
                        &op.function_exec_info.output_type,
                        /*ttl=*/ None,
                    )?;

                    let eval_future = evaluate_with_cell(output_value_cell.as_ref(), move || {
                        op.executor.evaluate(input_values)
                    });
                    let v = evaluate_with_timeout_and_warning(
                        eval_future,
                        timeout_duration,
                        warn_duration,
                        op_kind_for_warning,
                        op_name_for_warning,
                    )
                    .await?;

                    head_scope.define_field(&op.output, &v)
                } else {
                    let eval_future = op.executor.evaluate(input_values);
                    let v = evaluate_with_timeout_and_warning(
                        eval_future,
                        timeout_duration,
                        warn_duration,
                        op_kind_for_warning,
                        op_name_for_warning,
                    )
                    .await?;

                    head_scope.define_field(&op.output, &v)
                };

                // Track transform operation completion
                if let Some(ref op_stats) = operation_in_process_stats {
                    let transform_key =
                        format!("transform/{}{}", op_scope.scope_qualifier, op.name);
                    op_stats.finish_processing(&transform_key, 1);
                }

                result.with_context(|| format!("Evaluating Transform op `{}`", op.name))?
            }

            AnalyzedReactiveOp::ForEach(op) => {
                let target_field_schema = head_scope.get_field_schema(&op.local_field_ref)?;
                let table_schema = match &target_field_schema.value_type.typ {
                    schema::ValueType::Table(cs) => cs,
                    _ => internal_bail!("Expect target field to be a table"),
                };

                let target_field = head_scope.get_value_field_builder(&op.local_field_ref)?;
                let task_futs = match target_field {
                    value::Value::Null => vec![],
                    value::Value::UTable(v) => v
                        .iter()
                        .map(|item| {
                            evaluate_child_op_scope(
                                &op.op_scope,
                                scoped_entries,
                                ScopeEntry::new(
                                    ScopeKey::None,
                                    item,
                                    &table_schema.row,
                                    &op.op_scope,
                                ),
                                &op.concurrency_controller,
                                memory,
                                operation_in_process_stats,
                            )
                        })
                        .collect::<Vec<_>>(),
                    value::Value::KTable(v) => v
                        .iter()
                        .map(|(k, v)| {
                            evaluate_child_op_scope(
                                &op.op_scope,
                                scoped_entries,
                                ScopeEntry::new(
                                    ScopeKey::MapKey(k),
                                    v,
                                    &table_schema.row,
                                    &op.op_scope,
                                ),
                                &op.concurrency_controller,
                                memory,
                                operation_in_process_stats,
                            )
                        })
                        .collect::<Vec<_>>(),
                    value::Value::LTable(v) => v
                        .iter()
                        .enumerate()
                        .map(|(i, item)| {
                            evaluate_child_op_scope(
                                &op.op_scope,
                                scoped_entries,
                                ScopeEntry::new(
                                    ScopeKey::ListIndex(i),
                                    item,
                                    &table_schema.row,
                                    &op.op_scope,
                                ),
                                &op.concurrency_controller,
                                memory,
                                operation_in_process_stats,
                            )
                        })
                        .collect::<Vec<_>>(),
                    _ => {
                        internal_bail!("Target field type is expected to be a table");
                    }
                };
                try_join_all(task_futs)
                    .await
                    .with_context(|| format!("Evaluating ForEach op `{}`", op.name,))?;
            }

            AnalyzedReactiveOp::Collect(op) => {
                let mut field_values = Vec::with_capacity(
                    op.input.fields.len() + if op.has_auto_uuid_field { 1 } else { 0 },
                );
                let field_values_iter = assemble_input_values(&op.input.fields, scoped_entries);
                if op.has_auto_uuid_field {
                    field_values.push(value::Value::Null);
                    for value in field_values_iter {
                        field_values.push(value?);
                    }
                    let uuid = memory.next_uuid(
                        op.fingerprinter
                            .clone()
                            .with(&field_values[1..])?
                            .into_fingerprint(),
                    )?;
                    field_values[0] = value::Value::Basic(value::BasicValue::Uuid(uuid));
                } else {
                    for value in field_values_iter {
                        field_values.push(value?);
                    }
                };
                let collector_entry = scoped_entries
                    .headn(op.collector_ref.scope_up_level as usize)
                    .ok_or_else(|| internal_error!("Collector level out of bound"))?;

                // Assemble input values
                let input_values: Vec<value::Value> =
                    assemble_input_values(&op.input.fields, scoped_entries)
                        .collect::<Result<Vec<_>>>()?;

                // Create field_values vector for all fields in the merged schema
                let mut field_values = op
                    .field_index_mapping
                    .iter()
                    .map(|idx| {
                        idx.map_or(value::Value::Null, |input_idx| {
                            input_values[input_idx].clone()
                        })
                    })
                    .collect::<Vec<_>>();

                // Handle auto_uuid_field (assumed to be at position 0 for efficiency)
                if op.has_auto_uuid_field {
                    if let Some(uuid_idx) = op.collector_schema.auto_uuid_field_idx {
                        let uuid = memory.next_uuid(
                            op.fingerprinter
                                .clone()
                                .with(
                                    &field_values
                                        .iter()
                                        .enumerate()
                                        .filter(|(i, _)| *i != uuid_idx)
                                        .map(|(_, v)| v)
                                        .collect::<Vec<_>>(),
                                )?
                                .into_fingerprint(),
                        )?;
                        field_values[uuid_idx] = value::Value::Basic(value::BasicValue::Uuid(uuid));
                    }
                }

                {
                    let mut collected_records = collector_entry.collected_values
                        [op.collector_ref.local.collector_idx as usize]
                        .lock()
                        .unwrap();
                    collected_records.push(value::FieldValues {
                        fields: field_values,
                    });
                }
            }
        }
    }
    Ok(())
}

pub struct SourceRowEvaluationContext<'a> {
    pub plan: &'a ExecutionPlan,
    pub import_op: &'a AnalyzedImportOp,
    pub schema: &'a schema::FlowSchema,
    pub key: &'a value::KeyValue,
    pub import_op_idx: usize,
    pub source_logic_fp: &'a SourceLogicFingerprint,
}

#[derive(Debug)]
pub struct EvaluateSourceEntryOutput {
    pub data_scope: ScopeValueBuilder,
    pub collected_values: Vec<Vec<value::FieldValues>>,
}

#[instrument(name = "evaluate_source_entry", skip_all, fields(source_name = %src_eval_ctx.import_op.name))]
pub async fn evaluate_source_entry(
    src_eval_ctx: &SourceRowEvaluationContext<'_>,
    source_value: value::FieldValues,
    memory: &EvaluationMemory,
    operation_in_process_stats: Option<&execution::stats::OperationInProcessStats>,
) -> Result<EvaluateSourceEntryOutput> {
    let _permit = src_eval_ctx
        .import_op
        .concurrency_controller
        .acquire_bytes_with_reservation(|| source_value.estimated_byte_size())
        .await?;
    let root_schema = &src_eval_ctx.schema.schema;
    let root_scope_value = ScopeValueBuilder::new(root_schema.fields.len());
    let root_scope_entry = ScopeEntry::new(
        ScopeKey::None,
        &root_scope_value,
        root_schema,
        &src_eval_ctx.plan.op_scope,
    );

    let table_schema = match &root_schema.fields[src_eval_ctx.import_op.output.field_idx as usize]
        .value_type
        .typ
    {
        schema::ValueType::Table(cs) => cs,
        _ => {
            internal_bail!("Expect source output to be a table")
        }
    };

    let scope_value =
        ScopeValueBuilder::augmented_from(&value::ScopeValue(source_value), table_schema)?;
    root_scope_entry.define_field_w_builder(
        &src_eval_ctx.import_op.output,
        value::Value::KTable(BTreeMap::from([(src_eval_ctx.key.clone(), scope_value)])),
    )?;

    // Fill other source fields with empty tables
    for import_op in src_eval_ctx.plan.import_ops.iter() {
        let field_idx = import_op.output.field_idx;
        if field_idx != src_eval_ctx.import_op.output.field_idx {
            root_scope_entry.define_field(
                &AnalyzedOpOutput { field_idx },
                &value::Value::KTable(BTreeMap::new()),
            )?;
        }
    }

    evaluate_op_scope(
        &src_eval_ctx.plan.op_scope,
        RefList::Nil.prepend(&root_scope_entry),
        memory,
        operation_in_process_stats,
    )
    .await?;
    let collected_values = root_scope_entry
        .collected_values
        .into_iter()
        .map(|v| v.into_inner().unwrap())
        .collect::<Vec<_>>();
    Ok(EvaluateSourceEntryOutput {
        data_scope: root_scope_value,
        collected_values,
    })
}

#[instrument(name = "evaluate_transient_flow", skip_all, fields(flow_name = %flow.transient_flow_instance.name))]
pub async fn evaluate_transient_flow(
    flow: &AnalyzedTransientFlow,
    input_values: &Vec<value::Value>,
) -> Result<value::Value> {
    let root_schema = &flow.data_schema.schema;
    let root_scope_value = ScopeValueBuilder::new(root_schema.fields.len());
    let root_scope_entry = ScopeEntry::new(
        ScopeKey::None,
        &root_scope_value,
        root_schema,
        &flow.execution_plan.op_scope,
    );

    if input_values.len() != flow.execution_plan.input_fields.len() {
        client_bail!(
            "Input values length mismatch: expect {}, got {}",
            flow.execution_plan.input_fields.len(),
            input_values.len()
        );
    }
    for (field, value) in flow.execution_plan.input_fields.iter().zip(input_values) {
        root_scope_entry.define_field(field, value)?;
    }
    let eval_memory = EvaluationMemory::new(
        chrono::Utc::now(),
        None,
        EvaluationMemoryOptions {
            enable_cache: false,
            evaluation_only: true,
        },
    );
    evaluate_op_scope(
        &flow.execution_plan.op_scope,
        RefList::Nil.prepend(&root_scope_entry),
        &eval_memory,
        None, // No operation stats for transient flows
    )
    .await?;
    let output_value = assemble_value(
        &flow.execution_plan.output_value,
        RefList::Nil.prepend(&root_scope_entry),
    )?;
    Ok(output_value)
}
