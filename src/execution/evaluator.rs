use std::sync::{Mutex, OnceLock};
use std::{borrow::Cow, collections::BTreeMap};

use anyhow::{bail, Context, Ok, Result};
use futures::future::try_join_all;

use crate::builder::{plan::*, AnalyzedTransientFlow};
use crate::py::IntoPyResult;
use crate::{
    base::{schema, value},
    utils::immutable::RefList,
};

use super::memoization::{evaluate_with_cell, EvaluationMemory, EvaluationMemoryOptions};

#[derive(Debug)]
pub struct ScopeValueBuilder {
    // TODO: Share the same lock for values produced in the same execution scope, for stricter atomicity.
    pub fields: Vec<OnceLock<value::Value<ScopeValueBuilder>>>,

    pub collected_values: Vec<Mutex<Vec<value::FieldValues>>>,
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
    fn new(num_fields: usize, num_collectors: usize) -> Self {
        let mut fields = Vec::with_capacity(num_fields);
        fields.resize_with(num_fields, OnceLock::new);

        let mut collected_values = Vec::with_capacity(num_collectors);
        collected_values.resize_with(num_collectors, Default::default);
        Self {
            fields,
            collected_values,
        }
    }

    fn augmented_from(
        source: &value::ScopeValue,
        schema: &schema::CollectionSchema,
    ) -> Result<Self> {
        let val_index_base = if schema.has_key() { 1 } else { 0 };
        let len = schema.row.fields.len() - val_index_base;

        let mut builder = Self::new(len, schema.collectors.len());

        let value::ScopeValue(source_fields) = source;
        for ((v, t), r) in source_fields
            .fields
            .iter()
            .zip(schema.row.fields[val_index_base..(val_index_base + len)].iter())
            .zip(&mut builder.fields)
        {
            r.set(augmented_value(v, &t.value_type.typ)?)
                .into_py_result()?;
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
        (value::Value::Collection(v), schema::ValueType::Collection(t)) => {
            value::Value::Collection(
                v.iter()
                    .map(|v| ScopeValueBuilder::augmented_from(v, t))
                    .collect::<Result<Vec<_>>>()?,
            )
        }
        (value::Value::Table(v), schema::ValueType::Collection(t)) => value::Value::Table(
            v.iter()
                .map(|(k, v)| Ok((k.clone(), ScopeValueBuilder::augmented_from(v, t)?)))
                .collect::<Result<BTreeMap<_, _>>>()?,
        ),
        (value::Value::List(v), schema::ValueType::Collection(t)) => value::Value::List(
            v.iter()
                .map(|v| ScopeValueBuilder::augmented_from(v, t))
                .collect::<Result<Vec<_>>>()?,
        ),
        (val, _) => bail!("Value kind doesn't match the type {val_type}: {val:?}"),
    };
    Ok(value)
}

enum ScopeKey<'a> {
    /// For root struct and generic collection.
    None,
    /// For table row.
    MapKey(&'a value::KeyValue),
    /// For list item.
    ListIndex(usize),
}

impl<'a> ScopeKey<'a> {
    pub fn key(&self) -> Option<Cow<'a, value::KeyValue>> {
        match self {
            ScopeKey::None => None,
            ScopeKey::MapKey(k) => Some(Cow::Borrowed(k)),
            ScopeKey::ListIndex(i) => Some(Cow::Owned(value::KeyValue::Int64(*i as i64))),
        }
    }

    pub fn value_field_index_base(&self) -> u32 {
        match *self {
            ScopeKey::None => 0,
            ScopeKey::MapKey(_) => 1,
            ScopeKey::ListIndex(_) => 1,
        }
    }
}

struct ScopeEntry<'a> {
    key: ScopeKey<'a>,
    value: &'a ScopeValueBuilder,
    schema: &'a schema::StructSchema,
}

impl ScopeEntry<'_> {
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
                _ => bail!("Expect struct field"),
            };
            Self::get_local_field_schema(struct_field_schema, &indices[1..])?
        };
        Ok(result)
    }

    fn get_local_key_field<'b>(
        key_val: &'b value::KeyValue,
        indices: &'_ [u32],
    ) -> &'b value::KeyValue {
        if indices.is_empty() {
            key_val
        } else if let value::KeyValue::Struct(ref fields) = key_val {
            Self::get_local_key_field(&fields[indices[0] as usize], &indices[1..])
        } else {
            panic!("Only struct can be accessed by sub field");
        }
    }

    fn get_local_field<'b>(
        val: &'b value::Value<ScopeValueBuilder>,
        indices: &'_ [u32],
    ) -> &'b value::Value<ScopeValueBuilder> {
        if indices.is_empty() {
            val
        } else if let value::Value::Struct(ref fields) = val {
            Self::get_local_field(&fields.fields[indices[0] as usize], &indices[1..])
        } else {
            panic!("Only struct can be accessed by sub field");
        }
    }

    fn get_value_field_builder(
        &self,
        field_ref: &AnalyzedLocalFieldReference,
    ) -> &value::Value<ScopeValueBuilder> {
        let first_index = field_ref.fields_idx[0];
        let index_base = self.key.value_field_index_base();
        let val = self.value.fields[(first_index - index_base) as usize]
            .get()
            .unwrap();
        Self::get_local_field(val, &field_ref.fields_idx[1..])
    }

    fn get_field(&self, field_ref: &AnalyzedLocalFieldReference) -> value::Value {
        let first_index = field_ref.fields_idx[0];
        let index_base = self.key.value_field_index_base();
        if first_index < index_base {
            let key_val = self.key.key().unwrap().into_owned();
            let key_part = Self::get_local_key_field(&key_val, &field_ref.fields_idx[1..]);
            key_part.clone().into()
        } else {
            let val = self.value.fields[(first_index - index_base) as usize]
                .get()
                .unwrap();
            let val_part = Self::get_local_field(val, &field_ref.fields_idx[1..]);
            value::Value::from_alternative_ref(val_part)
        }
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
    ) {
        let field_index = output_field.field_idx as usize;
        let index_base = self.key.value_field_index_base() as usize;
        self.value.fields[field_index - index_base]
            .set(val)
            .expect("Field is already set, violating single-definition rule");
    }

    fn define_field(&self, output_field: &AnalyzedOpOutput, val: &value::Value) -> Result<()> {
        let field_index = output_field.field_idx as usize;
        let field_schema = &self.schema.fields[field_index];
        let val = augmented_value(val, &field_schema.value_type.typ)?;
        self.define_field_w_builder(output_field, val);
        Ok(())
    }
}

fn assemble_value(
    value_mapping: &AnalyzedValueMapping,
    scoped_entries: RefList<'_, &ScopeEntry<'_>>,
) -> value::Value {
    match value_mapping {
        AnalyzedValueMapping::Constant { value } => value.clone(),
        AnalyzedValueMapping::Field(field_ref) => scoped_entries
            .headn(field_ref.scope_up_level as usize)
            .unwrap()
            .get_field(&field_ref.local),
        AnalyzedValueMapping::Struct(mapping) => {
            let fields = mapping
                .fields
                .iter()
                .map(|f| assemble_value(f, scoped_entries))
                .collect();
            value::Value::Struct(value::FieldValues { fields })
        }
    }
}

fn assemble_input_values<'a>(
    value_mappings: &'a [AnalyzedValueMapping],
    scoped_entries: RefList<'a, &ScopeEntry<'a>>,
) -> impl Iterator<Item = value::Value> + 'a {
    value_mappings
        .iter()
        .map(move |value_mapping| assemble_value(value_mapping, scoped_entries))
}

async fn evaluate_child_op_scope(
    op_scope: &AnalyzedOpScope,
    scoped_entries: RefList<'_, &ScopeEntry<'_>>,
    child_scope_entry: ScopeEntry<'_>,
    memory: &EvaluationMemory,
) -> Result<()> {
    evaluate_op_scope(op_scope, scoped_entries.prepend(&child_scope_entry), memory)
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

async fn evaluate_op_scope(
    op_scope: &AnalyzedOpScope,
    scoped_entries: RefList<'_, &ScopeEntry<'_>>,
    memory: &EvaluationMemory,
) -> Result<()> {
    let head_scope = *scoped_entries.head().unwrap();
    for reactive_op in op_scope.reactive_ops.iter() {
        match reactive_op {
            AnalyzedReactiveOp::Transform(op) => {
                let mut input_values = Vec::with_capacity(op.inputs.len());
                input_values
                    .extend(assemble_input_values(&op.inputs, scoped_entries).collect::<Vec<_>>());
                let output_value_cell = memory.get_cache_entry(
                    || {
                        Ok(op
                            .function_exec_info
                            .fingerprinter
                            .clone()
                            .with(&input_values)?
                            .into_fingerprint())
                    },
                    &op.function_exec_info.output_type,
                    /*ttl=*/ None,
                )?;
                let output_value = evaluate_with_cell(output_value_cell.as_ref(), move || {
                    op.executor.evaluate(input_values)
                })
                .await
                .with_context(|| format!("Evaluating Transform op `{}`", op.name,))?;
                head_scope.define_field(&op.output, &output_value)?;
            }

            AnalyzedReactiveOp::ForEach(op) => {
                let target_field_schema = head_scope.get_field_schema(&op.local_field_ref)?;
                let collection_schema = match &target_field_schema.value_type.typ {
                    schema::ValueType::Collection(cs) => cs,
                    _ => bail!("Expect target field to be a collection"),
                };

                let target_field = head_scope.get_value_field_builder(&op.local_field_ref);
                let task_futs = match target_field {
                    value::Value::Collection(v) => v
                        .iter()
                        .map(|item| {
                            evaluate_child_op_scope(
                                &op.op_scope,
                                scoped_entries,
                                ScopeEntry {
                                    key: ScopeKey::None,
                                    value: item,
                                    schema: &collection_schema.row,
                                },
                                memory,
                            )
                        })
                        .collect::<Vec<_>>(),
                    value::Value::Table(v) => v
                        .iter()
                        .map(|(k, v)| {
                            evaluate_child_op_scope(
                                &op.op_scope,
                                scoped_entries,
                                ScopeEntry {
                                    key: ScopeKey::MapKey(k),
                                    value: v,
                                    schema: &collection_schema.row,
                                },
                                memory,
                            )
                        })
                        .collect::<Vec<_>>(),
                    value::Value::List(v) => v
                        .iter()
                        .enumerate()
                        .map(|(i, item)| {
                            evaluate_child_op_scope(
                                &op.op_scope,
                                scoped_entries,
                                ScopeEntry {
                                    key: ScopeKey::ListIndex(i),
                                    value: item,
                                    schema: &collection_schema.row,
                                },
                                memory,
                            )
                        })
                        .collect::<Vec<_>>(),
                    _ => {
                        bail!("Target field type is expected to be a collection");
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
                    field_values.extend(field_values_iter);
                    let uuid = memory.next_uuid(
                        op.fingerprinter
                            .clone()
                            .with(&field_values[1..])?
                            .into_fingerprint(),
                    )?;
                    field_values[0] = value::Value::Basic(value::BasicValue::Uuid(uuid));
                } else {
                    field_values.extend(field_values_iter);
                };
                let collector_entry = scoped_entries
                    .headn(op.collector_ref.scope_up_level as usize)
                    .unwrap();
                {
                    let mut collected_records = collector_entry.value.collected_values
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

pub async fn evaluate_source_entry(
    plan: &ExecutionPlan,
    import_op: &AnalyzedImportOp,
    schema: &schema::DataSchema,
    key: &value::KeyValue,
    source_value: value::FieldValues,
    memory: &EvaluationMemory,
) -> Result<ScopeValueBuilder> {
    let root_schema = &schema.schema;
    let root_scope_value =
        ScopeValueBuilder::new(root_schema.fields.len(), schema.collectors.len());
    let root_scope_entry = ScopeEntry {
        key: ScopeKey::None,
        value: &root_scope_value,
        schema: root_schema,
    };

    let collection_schema = match &root_schema.fields[import_op.output.field_idx as usize]
        .value_type
        .typ
    {
        schema::ValueType::Collection(cs) => cs,
        _ => {
            bail!("Expect source output to be a table")
        }
    };

    let scope_value =
        ScopeValueBuilder::augmented_from(&value::ScopeValue(source_value), collection_schema)?;
    root_scope_entry.define_field_w_builder(
        &import_op.output,
        value::Value::Table(BTreeMap::from([(key.clone(), scope_value)])),
    );

    evaluate_op_scope(
        &plan.op_scope,
        RefList::Nil.prepend(&root_scope_entry),
        memory,
    )
    .await?;
    Ok(root_scope_value)
}

pub async fn evaluate_transient_flow(
    flow: &AnalyzedTransientFlow,
    input_values: &Vec<value::Value>,
) -> Result<value::Value> {
    let root_schema = &flow.data_schema.schema;
    let root_scope_value =
        ScopeValueBuilder::new(root_schema.fields.len(), flow.data_schema.collectors.len());
    let root_scope_entry = ScopeEntry {
        key: ScopeKey::None,
        value: &root_scope_value,
        schema: root_schema,
    };

    if input_values.len() != flow.execution_plan.input_fields.len() {
        bail!(
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
    )
    .await?;
    let output_value = assemble_value(
        &flow.execution_plan.output_value,
        RefList::Nil.prepend(&root_scope_entry),
    );
    Ok(output_value)
}
