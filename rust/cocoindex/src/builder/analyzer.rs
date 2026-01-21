use crate::builder::exec_ctx::AnalyzedSetupState;
use crate::ops::{
    get_attachment_factory, get_function_factory, get_source_factory, get_target_factory,
};
use crate::prelude::*;

use super::plan::*;
use crate::lib_context::get_auth_registry;
use crate::{
    base::{schema::*, spec::*},
    ops::interface::*,
};
use futures::future::{BoxFuture, try_join3};
use futures::{FutureExt, future::try_join_all};
use std::time::Duration;
use utils::fingerprint::Fingerprinter;

const TIMEOUT_THRESHOLD: Duration = Duration::from_secs(1800);

#[derive(Debug)]
pub(super) enum ValueTypeBuilder {
    Basic(BasicValueType),
    Struct(StructSchemaBuilder),
    Table(TableSchemaBuilder),
}

impl TryFrom<&ValueType> for ValueTypeBuilder {
    type Error = Error;

    fn try_from(value_type: &ValueType) -> std::result::Result<Self, Self::Error> {
        match value_type {
            ValueType::Basic(basic_type) => Ok(ValueTypeBuilder::Basic(basic_type.clone())),
            ValueType::Struct(struct_type) => Ok(ValueTypeBuilder::Struct(struct_type.try_into()?)),
            ValueType::Table(table_type) => Ok(ValueTypeBuilder::Table(table_type.try_into()?)),
        }
    }
}

impl TryInto<ValueType> for &ValueTypeBuilder {
    type Error = Error;

    fn try_into(self) -> std::result::Result<ValueType, Self::Error> {
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
                client_bail!("Field name already exists: {}", field.name);
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
    type Error = Error;

    fn try_from(schema: &StructSchema) -> std::result::Result<Self, Self::Error> {
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
    type Error = Error;

    fn try_into(self) -> std::result::Result<StructSchema, Self::Error> {
        Ok(StructSchema {
            fields: Arc::new(
                self.fields
                    .iter()
                    .map(FieldSchema::<ValueType>::from_alternative)
                    .collect::<std::result::Result<Vec<_>, _>>()?,
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
    type Error = Error;

    fn try_from(schema: &TableSchema) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            kind: schema.kind,
            sub_scope: Arc::new(Mutex::new(DataScopeBuilder {
                data: (&schema.row).try_into()?,
                added_fields_def_fp: Default::default(),
            })),
        })
    }
}

impl TryInto<TableSchema> for &TableSchemaBuilder {
    type Error = Error;

    fn try_into(self) -> std::result::Result<TableSchema, Self::Error> {
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
            description: None,
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
    let schema1_fields = &schema1.fields;
    let schema2_fields = &schema2.fields;

    // Create a map from field name to index in schema1
    let field_map: HashMap<FieldName, usize> = schema1_fields
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name.clone(), i))
        .collect();

    let mut output_fields = Vec::new();
    let mut next_field_id_1 = 0;
    let mut next_field_id_2 = 0;

    for (idx, field) in schema2_fields.iter().enumerate() {
        if let Some(&idx1) = field_map.get(&field.name) {
            if idx1 < next_field_id_1 {
                api_bail!(
                    "Common fields are expected to have consistent order across different `collect()` calls, but got different orders between fields '{}' and '{}'",
                    field.name,
                    schema1_fields[next_field_id_1 - 1].name
                );
            }
            // Add intervening fields from schema1
            for i in next_field_id_1..idx1 {
                output_fields.push(schema1_fields[i].clone());
            }
            // Add intervening fields from schema2
            for i in next_field_id_2..idx {
                output_fields.push(schema2_fields[i].clone());
            }
            // Merge the field
            let merged_type =
                try_make_common_value_type(&schema1_fields[idx1].value_type, &field.value_type)?;
            output_fields.push(FieldSchema {
                name: field.name.clone(),
                value_type: merged_type,
                description: None,
            });
            next_field_id_1 = idx1 + 1;
            next_field_id_2 = idx + 1;
            // Fields not in schema1 and not UUID are added at the end
        }
    }

    // Add remaining fields from schema1
    for i in next_field_id_1..schema1_fields.len() {
        output_fields.push(schema1_fields[i].clone());
    }

    // Add remaining fields from schema2
    for i in next_field_id_2..schema2_fields.len() {
        output_fields.push(schema2_fields[i].clone());
    }

    // Handle auto_uuid_field_idx
    let auto_uuid_field_idx = match (schema1.auto_uuid_field_idx, schema2.auto_uuid_field_idx) {
        (Some(idx1), Some(idx2)) => {
            let name1 = &schema1_fields[idx1].name;
            let name2 = &schema2_fields[idx2].name;
            if name1 == name2 {
                // Find the position of the auto_uuid field in the merged output
                output_fields.iter().position(|f| &f.name == name1)
            } else {
                api_bail!(
                    "Generated UUID fields must have the same name across different `collect()` calls, got different names: '{}' vs '{}'",
                    name1,
                    name2
                );
            }
        }
        (Some(_), None) | (None, Some(_)) => {
            api_bail!(
                "The generated UUID field, once present for one `collect()`, must be consistently present for other `collect()` calls for the same collector"
            );
        }
        (None, None) => None,
    };

    Ok(CollectorSchema {
        fields: output_fields,
        auto_uuid_field_idx,
    })
}

struct FieldDefFingerprintBuilder {
    source_op_names: HashSet<String>,
    fingerprinter: Fingerprinter,
}

impl FieldDefFingerprintBuilder {
    pub fn new() -> Self {
        Self {
            source_op_names: HashSet::new(),
            fingerprinter: Fingerprinter::default(),
        }
    }

    pub fn add(&mut self, key: Option<&str>, def_fp: FieldDefFingerprint) -> Result<()> {
        self.source_op_names.extend(def_fp.source_op_names);
        let mut fingerprinter = std::mem::take(&mut self.fingerprinter);
        if let Some(key) = key {
            fingerprinter = fingerprinter.with(key)?;
        }
        fingerprinter = fingerprinter.with(def_fp.fingerprint.as_slice())?;
        self.fingerprinter = fingerprinter;
        Ok(())
    }

    pub fn build(self) -> FieldDefFingerprint {
        FieldDefFingerprint {
            source_op_names: self.source_op_names,
            fingerprint: self.fingerprinter.into_fingerprint(),
        }
    }
}

#[derive(Debug)]
pub(super) struct CollectorBuilder {
    pub schema: Arc<CollectorSchema>,
    pub is_used: bool,
    pub def_fps: Vec<FieldDefFingerprint>,
}

impl CollectorBuilder {
    pub fn new(schema: Arc<CollectorSchema>, def_fp: FieldDefFingerprint) -> Self {
        Self {
            schema,
            is_used: false,
            def_fps: vec![def_fp],
        }
    }

    pub fn collect(&mut self, schema: &CollectorSchema, def_fp: FieldDefFingerprint) -> Result<()> {
        if self.is_used {
            api_bail!("Collector is already used");
        }
        let existing_schema = Arc::make_mut(&mut self.schema);
        *existing_schema = try_merge_collector_schemas(existing_schema, schema)?;
        self.def_fps.push(def_fp);
        Ok(())
    }

    pub fn use_collection(&mut self) -> Result<(Arc<CollectorSchema>, FieldDefFingerprint)> {
        self.is_used = true;

        self.def_fps
            .sort_by(|a, b| a.fingerprint.as_slice().cmp(b.fingerprint.as_slice()));
        let mut def_fp_builder = FieldDefFingerprintBuilder::new();
        for def_fp in self.def_fps.iter() {
            def_fp_builder.add(None, def_fp.clone())?;
        }
        Ok((self.schema.clone(), def_fp_builder.build()))
    }
}

#[derive(Debug)]
pub(super) struct DataScopeBuilder {
    pub data: StructSchemaBuilder,
    pub added_fields_def_fp: IndexMap<FieldName, FieldDefFingerprint>,
}

impl DataScopeBuilder {
    pub fn new() -> Self {
        Self {
            data: Default::default(),
            added_fields_def_fp: Default::default(),
        }
    }

    pub fn last_field(&self) -> Option<&FieldSchema<ValueTypeBuilder>> {
        self.data.fields.last()
    }

    pub fn add_field(
        &mut self,
        name: FieldName,
        value_type: &EnrichedValueType,
        def_fp: FieldDefFingerprint,
    ) -> Result<AnalyzedOpOutput> {
        let field_index = self.data.add_field(FieldSchema {
            name: name.clone(),
            value_type: EnrichedValueType::from_alternative(value_type)?,
            description: None,
        })?;
        self.added_fields_def_fp.insert(name, def_fp);
        Ok(AnalyzedOpOutput {
            field_idx: field_index,
        })
    }

    /// Must be called on an non-empty field path.
    pub fn analyze_field_path<'a>(
        &'a self,
        field_path: &'_ FieldPath,
        base_def_fp: FieldDefFingerprint,
    ) -> Result<(
        AnalyzedLocalFieldReference,
        &'a EnrichedValueType<ValueTypeBuilder>,
        FieldDefFingerprint,
    )> {
        let mut indices = Vec::with_capacity(field_path.len());
        let mut struct_schema = &self.data;
        let mut def_fp = base_def_fp;

        if field_path.is_empty() {
            client_bail!("Field path is empty");
        }

        let mut i = 0;
        let value_type = loop {
            let field_name = &field_path[i];
            let (field_idx, field) = struct_schema.find_field(field_name).ok_or_else(|| {
                api_error!("Field {} not found", field_path[0..(i + 1)].join("."))
            })?;
            if let Some(added_def_fp) = self.added_fields_def_fp.get(field_name) {
                def_fp = added_def_fp.clone();
            } else {
                def_fp.fingerprint = Fingerprinter::default()
                    .with(&("field", &def_fp.fingerprint, field_name))?
                    .into_fingerprint();
            };
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
            def_fp,
        ))
    }
}

pub(super) struct AnalyzerContext {
    pub lib_ctx: Arc<LibContext>,
    pub flow_ctx: Arc<FlowInstanceContext>,
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
        def_fp: FieldDefFingerprint,
    ) -> Result<AnalyzedLocalCollectorReference> {
        let existing_len = self.collectors.len();
        let idx = match self.collectors.entry(collector_name) {
            indexmap::map::Entry::Occupied(mut entry) => {
                entry.get_mut().collect(&schema, def_fp)?;
                entry.index()
            }
            indexmap::map::Entry::Vacant(entry) => {
                entry.insert(CollectorBuilder::new(Arc::new(schema), def_fp));
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
    ) -> Result<(
        AnalyzedLocalCollectorReference,
        Arc<CollectorSchema>,
        FieldDefFingerprint,
    )> {
        let (collector_idx, _, collector) = self
            .collectors
            .get_full_mut(collector_name)
            .ok_or_else(|| api_error!("Collector not found: {}", collector_name))?;
        let (schema, def_fp) = collector.use_collection()?;
        Ok((
            AnalyzedLocalCollectorReference {
                collector_idx: collector_idx as u32,
            },
            schema,
            def_fp,
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
    pub(super) base_value_def_fp: FieldDefFingerprint,
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
        base_value_def_fp: FieldDefFingerprint,
    ) -> Arc<Self> {
        Arc::new(Self {
            name,
            parent,
            data,
            states: Mutex::default(),
            base_value_def_fp,
        })
    }

    fn add_op_output(
        &self,
        name: FieldName,
        value_type: EnrichedValueType,
        def_fp: FieldDefFingerprint,
    ) -> Result<AnalyzedOpOutput> {
        let op_output = self
            .data
            .lock()
            .unwrap()
            .add_field(name.clone(), &value_type, def_fp)?;
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
        let (local_field_ref, sub_data_scope, def_fp) = {
            let data_scope = self.data.lock().unwrap();
            let (local_field_ref, value_type, def_fp) =
                data_scope.analyze_field_path(field_path, self.base_value_def_fp.clone())?;
            let sub_data_scope = match &value_type.typ {
                ValueTypeBuilder::Table(table_type) => table_type.sub_scope.clone(),
                _ => api_bail!("ForEach only works on collection, field {field_path} is not"),
            };
            (local_field_ref, sub_data_scope, def_fp)
        };
        let sub_op_scope = OpScope::new(
            scope_name,
            Some((self.clone(), field_path.clone())),
            sub_data_scope,
            def_fp,
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
) -> Result<(AnalyzedStructMapping, Vec<FieldSchema>, FieldDefFingerprint)> {
    let mut field_mappings = Vec::with_capacity(mapping.fields.len());
    let mut field_schemas = Vec::with_capacity(mapping.fields.len());

    let mut fields_def_fps = Vec::with_capacity(mapping.fields.len());
    for field in mapping.fields.iter() {
        let (field_mapping, value_type, field_def_fp) =
            analyze_value_mapping(&field.spec, op_scope)?;
        field_mappings.push(field_mapping);
        field_schemas.push(FieldSchema {
            name: field.name.clone(),
            value_type,
            description: None,
        });
        fields_def_fps.push((field.name.as_str(), field_def_fp));
    }
    fields_def_fps.sort_by_key(|(name, _)| *name);
    let mut def_fp_builder = FieldDefFingerprintBuilder::new();
    for (name, def_fp) in fields_def_fps {
        def_fp_builder.add(Some(name), def_fp)?;
    }
    Ok((
        AnalyzedStructMapping {
            fields: field_mappings,
        },
        field_schemas,
        def_fp_builder.build(),
    ))
}

fn analyze_value_mapping(
    value_mapping: &ValueMapping,
    op_scope: &OpScope,
) -> Result<(AnalyzedValueMapping, EnrichedValueType, FieldDefFingerprint)> {
    let result = match value_mapping {
        ValueMapping::Constant(v) => {
            let value = value::Value::from_json(v.value.clone(), &v.schema.typ)?;
            let value_mapping = AnalyzedValueMapping::Constant { value };
            let def_fp = FieldDefFingerprint {
                source_op_names: HashSet::new(),
                fingerprint: Fingerprinter::default()
                    .with(&("constant", &v.value, &v.schema.without_attrs()))?
                    .into_fingerprint(),
            };
            (value_mapping, v.schema.clone(), def_fp)
        }

        ValueMapping::Field(v) => {
            let (scope_up_level, op_scope) = match &v.scope {
                Some(scope_name) => find_scope(scope_name, op_scope)?,
                None => (0, op_scope),
            };
            let data_scope = op_scope.data.lock().unwrap();
            let (local_field_ref, value_type, def_fp) =
                data_scope.analyze_field_path(&v.field_path, op_scope.base_value_def_fp.clone())?;
            let schema = EnrichedValueType::from_alternative(value_type)?;
            let value_mapping = AnalyzedValueMapping::Field(AnalyzedFieldReference {
                local: local_field_ref,
                scope_up_level,
            });
            (value_mapping, schema, def_fp)
        }
    };
    Ok(result)
}

fn analyze_input_fields(
    arg_bindings: &[OpArgBinding],
    op_scope: &OpScope,
) -> Result<(Vec<OpArgSchema>, FieldDefFingerprint)> {
    let mut op_arg_schemas = Vec::with_capacity(arg_bindings.len());
    let mut def_fp_builder = FieldDefFingerprintBuilder::new();
    for arg_binding in arg_bindings.iter() {
        let (analyzed_value, value_type, def_fp) =
            analyze_value_mapping(&arg_binding.value, op_scope)?;
        let op_arg_schema = OpArgSchema {
            name: arg_binding.arg_name.clone(),
            value_type,
            analyzed_value: analyzed_value.clone(),
        };
        def_fp_builder.add(arg_binding.arg_name.0.as_ref().map(|n| n.as_str()), def_fp)?;
        op_arg_schemas.push(op_arg_schema);
    }
    Ok((op_arg_schemas, def_fp_builder.build()))
}

fn add_collector(
    scope_name: &ScopeName,
    collector_name: FieldName,
    schema: CollectorSchema,
    op_scope: &OpScope,
    def_fp: FieldDefFingerprint,
) -> Result<AnalyzedCollectorReference> {
    let (scope_up_level, scope) = find_scope(scope_name, op_scope)?;
    let local_ref = scope
        .states
        .lock()
        .unwrap()
        .add_collector(collector_name, schema, def_fp)?;
    Ok(AnalyzedCollectorReference {
        local: local_ref,
        scope_up_level,
    })
}

struct ExportDataFieldsInfo {
    local_collector_ref: AnalyzedLocalCollectorReference,
    primary_key_def: AnalyzedPrimaryKeyDef,
    primary_key_schema: Box<[FieldSchema]>,
    value_fields_idx: Vec<u32>,
    value_stable: bool,
    output_value_fingerprinter: Fingerprinter,
    def_fp: FieldDefFingerprint,
}

impl AnalyzerContext {
    pub(super) async fn analyze_import_op(
        &self,
        op_scope: &Arc<OpScope>,
        import_op: NamedSpec<ImportOpSpec>,
    ) -> Result<impl Future<Output = Result<AnalyzedImportOp>> + Send + use<>> {
        let source_factory = get_source_factory(&import_op.spec.source.kind)?;
        let (output_type, executor) = source_factory
            .build(
                &import_op.name,
                serde_json::Value::Object(import_op.spec.source.spec),
                self.flow_ctx.clone(),
            )
            .await?;

        let op_name = import_op.name;
        let primary_key_schema = Box::from(output_type.typ.key_schema());
        let def_fp = FieldDefFingerprint {
            source_op_names: HashSet::from([op_name.clone()]),
            fingerprint: Fingerprinter::default()
                .with(&("import", &op_name))?
                .into_fingerprint(),
        };
        let output = op_scope.add_op_output(op_name.clone(), output_type, def_fp)?;

        let concur_control_options = import_op
            .spec
            .execution_options
            .get_concur_control_options();
        let global_concurrency_controller = self.lib_ctx.global_concurrency_controller.clone();
        let result_fut = async move {
            trace!("Start building executor for source op `{op_name}`");
            let executor = executor
                .await
                .with_context(|| format!("Preparing for source op: {op_name}"))?;
            trace!("Finished building executor for source op `{op_name}`");
            Ok(AnalyzedImportOp {
                executor,
                output,
                primary_key_schema,
                name: op_name,
                refresh_options: import_op.spec.refresh_options,
                concurrency_controller: concur_control::CombinedConcurrencyController::new(
                    &concur_control_options,
                    global_concurrency_controller,
                ),
            })
        };
        Ok(result_fut)
    }

    pub(super) async fn analyze_reactive_op(
        &self,
        op_scope: &Arc<OpScope>,
        reactive_op: &NamedSpec<ReactiveOpSpec>,
    ) -> Result<BoxFuture<'static, Result<AnalyzedReactiveOp>>> {
        let reactive_op_clone = reactive_op.clone();
        let reactive_op_name = reactive_op.name.clone();
        let result_fut = match reactive_op_clone.spec {
            ReactiveOpSpec::Transform(op) => {
                let (input_field_schemas, input_def_fp) =
                    analyze_input_fields(&op.inputs, op_scope).with_context(|| {
                        format!("Preparing inputs for transform op: {}", reactive_op_name)
                    })?;
                let spec = serde_json::Value::Object(op.op.spec.clone());

                let fn_executor = get_function_factory(&op.op.kind)?;
                let input_value_mappings = input_field_schemas
                    .iter()
                    .map(|field| field.analyzed_value.clone())
                    .collect();
                let build_output = fn_executor
                    .build(spec, input_field_schemas, self.flow_ctx.clone())
                    .await?;
                let output_type = build_output.output_type.typ.clone();
                let logic_fingerprinter = Fingerprinter::default()
                    .with(&op.op)?
                    .with(&build_output.output_type.without_attrs())?
                    .with(&build_output.behavior_version)?;

                let def_fp = FieldDefFingerprint {
                    source_op_names: input_def_fp.source_op_names,
                    fingerprint: Fingerprinter::default()
                        .with(&(
                            "transform",
                            &op.op,
                            &input_def_fp.fingerprint,
                            &build_output.behavior_version,
                        ))?
                        .into_fingerprint(),
                };
                let output = op_scope.add_op_output(
                    reactive_op_name.clone(),
                    build_output.output_type,
                    def_fp,
                )?;
                let op_name = reactive_op_name.clone();
                let op_kind = op.op.kind.clone();

                let execution_options_timeout = op.execution_options.timeout;

                let behavior_version = build_output.behavior_version;
                async move {
                            trace!("Start building executor for transform op `{op_name}`");
                            let executor = build_output.executor.await.with_context(|| {
                                format!("Preparing for transform op: {op_name}")
                            })?;
                            let enable_cache = executor.enable_cache();
                            let timeout = executor.timeout()
                                .or(execution_options_timeout)
                                .or(Some(TIMEOUT_THRESHOLD));
                            trace!("Finished building executor for transform op `{op_name}`, enable cache: {enable_cache}, behavior version: {behavior_version:?}");
                            let function_exec_info = AnalyzedFunctionExecInfo {
                                enable_cache,
                                timeout,
                                behavior_version,
                                fingerprinter: logic_fingerprinter,
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
                                op_kind,
                                inputs: input_value_mappings,
                                function_exec_info,
                                executor,
                                output,
                            }))
                }
                .boxed()
            }

            ReactiveOpSpec::ForEach(foreach_op) => {
                let (local_field_ref, sub_op_scope) = op_scope.new_foreach_op_scope(
                    foreach_op.op_scope.name.clone(),
                    &foreach_op.field_path,
                )?;
                let analyzed_op_scope_fut = {
                    let analyzed_op_scope_fut = self
                        .analyze_op_scope(&sub_op_scope, &foreach_op.op_scope.ops)
                        .boxed_local()
                        .await?;
                    let sub_op_scope_schema =
                        sub_op_scope.states.lock().unwrap().build_op_scope_schema();
                    op_scope
                        .states
                        .lock()
                        .unwrap()
                        .sub_scopes
                        .insert(reactive_op_name.clone(), Arc::new(sub_op_scope_schema));
                    analyzed_op_scope_fut
                };
                let op_name = reactive_op_name.clone();

                let concur_control_options =
                    foreach_op.execution_options.get_concur_control_options();
                async move {
                    Ok(AnalyzedReactiveOp::ForEach(AnalyzedForEachOp {
                        local_field_ref,
                        op_scope: analyzed_op_scope_fut
                            .await
                            .with_context(|| format!("Preparing for foreach op: {op_name}"))?,
                        name: op_name,
                        concurrency_controller: concur_control::ConcurrencyController::new(
                            &concur_control_options,
                        ),
                    }))
                }
                .boxed()
            }

            ReactiveOpSpec::Collect(op) => {
                let (struct_mapping, fields_schema, mut def_fp) =
                    analyze_struct_mapping(&op.input, op_scope)?;
                let has_auto_uuid_field = op.auto_uuid_field.is_some();
                def_fp.fingerprint = Fingerprinter::default()
                    .with(&(
                        "collect",
                        &def_fp.fingerprint,
                        &fields_schema,
                        &has_auto_uuid_field,
                    ))?
                    .into_fingerprint();
                let fingerprinter = Fingerprinter::default().with(&fields_schema)?;

                let input_field_names: Vec<FieldName> =
                    fields_schema.iter().map(|f| f.name.clone()).collect();
                let collector_ref = add_collector(
                    &op.scope_name,
                    op.collector_name.clone(),
                    CollectorSchema::from_fields(fields_schema, op.auto_uuid_field.clone()),
                    op_scope,
                    def_fp,
                )?;
                let op_scope = op_scope.clone();
                async move {
                    // Get the merged collector schema after adding
                    let collector_schema: Arc<CollectorSchema> = {
                        let scope = find_scope(&op.scope_name, &op_scope)?.1;
                        let states = scope.states.lock().unwrap();
                        let collector = states.collectors.get(&op.collector_name).unwrap();
                        collector.schema.clone()
                    };

                    // Pre-compute field index mappings for efficient evaluation
                    let field_name_to_index: HashMap<&FieldName, usize> = input_field_names
                        .iter()
                        .enumerate()
                        .map(|(i, n)| (n, i))
                        .collect();
                    let field_index_mapping = collector_schema
                        .fields
                        .iter()
                        .map(|field| field_name_to_index.get(&field.name).copied())
                        .collect::<Vec<Option<usize>>>();

                    let collect_op = AnalyzedReactiveOp::Collect(AnalyzedCollectOp {
                        name: reactive_op_name,
                        has_auto_uuid_field,
                        input: struct_mapping,
                        input_field_names,
                        collector_schema,
                        collector_ref,
                        field_index_mapping,
                        fingerprinter,
                    });
                    Ok(collect_op)
                }
                .boxed()
            }
        };
        Ok(result_fut)
    }

    #[allow(clippy::too_many_arguments)]
    async fn analyze_export_op_group(
        &self,
        target_kind: &str,
        op_scope: &Arc<OpScope>,
        flow_inst: &FlowInstanceSpec,
        export_op_group: &AnalyzedExportTargetOpGroup,
        declarations: Vec<serde_json::Value>,
        targets_analyzed_ss: &mut [Option<exec_ctx::AnalyzedTargetSetupState>],
        declarations_analyzed_ss: &mut Vec<exec_ctx::AnalyzedTargetSetupState>,
    ) -> Result<Vec<impl Future<Output = Result<AnalyzedExportOp>> + Send + use<>>> {
        let mut collection_specs = Vec::<interface::ExportDataCollectionSpec>::new();
        let mut data_fields_infos = Vec::<ExportDataFieldsInfo>::new();
        for idx in export_op_group.op_idx.iter() {
            let export_op = &flow_inst.export_ops[*idx];
            let (local_collector_ref, collector_schema, def_fp) =
                op_scope
                    .states
                    .lock()
                    .unwrap()
                    .consume_collector(&export_op.spec.collector_name)?;
            let (value_fields_schema, data_collection_info) =
                match &export_op.spec.index_options.primary_key_fields {
                    Some(fields) => {
                        let pk_fields_idx = fields
                            .iter()
                            .map(|f| {
                                collector_schema
                                    .fields
                                    .iter()
                                    .position(|field| &field.name == f)
                                    .ok_or_else(|| client_error!("field not found: {}", f))
                            })
                            .collect::<Result<Vec<_>>>()?;

                        let primary_key_schema = pk_fields_idx
                            .iter()
                            .map(|idx| collector_schema.fields[*idx].without_attrs())
                            .collect::<Box<[_]>>();
                        let mut value_fields_schema: Vec<FieldSchema> = vec![];
                        let mut value_fields_idx = vec![];
                        for (idx, field) in collector_schema.fields.iter().enumerate() {
                            if !pk_fields_idx.contains(&idx) {
                                value_fields_schema.push(field.without_attrs());
                                value_fields_idx.push(idx as u32);
                            }
                        }
                        let value_stable = collector_schema
                            .auto_uuid_field_idx
                            .as_ref()
                            .map(|uuid_idx| pk_fields_idx.contains(uuid_idx))
                            .unwrap_or(false);
                        let output_value_fingerprinter =
                            Fingerprinter::default().with(&value_fields_schema)?;
                        (
                            value_fields_schema,
                            ExportDataFieldsInfo {
                                local_collector_ref,
                                primary_key_def: AnalyzedPrimaryKeyDef::Fields(pk_fields_idx),
                                primary_key_schema,
                                value_fields_idx,
                                value_stable,
                                output_value_fingerprinter,
                                def_fp,
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
                key_fields_schema: data_collection_info.primary_key_schema.clone(),
                value_fields_schema,
                index_options: export_op.spec.index_options.clone(),
            });
            data_fields_infos.push(data_collection_info);
        }
        let (data_collections_output, declarations_output) = export_op_group
            .target_factory
            .clone()
            .build(collection_specs, declarations, self.flow_ctx.clone())
            .await?;
        let analyzed_export_ops = export_op_group
            .op_idx
            .iter()
            .zip(data_collections_output.into_iter())
            .zip(data_fields_infos.into_iter())
            .map(|((idx, data_coll_output), data_fields_info)| {
                let export_op = &flow_inst.export_ops[*idx];
                let op_name = export_op.name.clone();
                let export_target_factory = export_op_group.target_factory.clone();

                let attachments = export_op
                    .spec
                    .attachments
                    .iter()
                    .map(|attachment| {
                        let attachment_factory = get_attachment_factory(&attachment.kind)?;
                        let attachment_state = attachment_factory.get_state(
                            &op_name,
                            &export_op.spec.target.spec,
                            serde_json::Value::Object(attachment.spec.clone()),
                        )?;
                        Ok((
                            interface::AttachmentSetupKey(
                                attachment.kind.clone(),
                                attachment_state.setup_key,
                            ),
                            attachment_state.setup_state,
                        ))
                    })
                    .collect::<Result<IndexMap<_, _>>>()?;

                let export_op_ss = exec_ctx::AnalyzedTargetSetupState {
                    target_kind: target_kind.to_string(),
                    setup_key: data_coll_output.setup_key,
                    desired_setup_state: data_coll_output.desired_setup_state,
                    setup_by_user: export_op.spec.setup_by_user,
                    key_type: Some(
                        data_fields_info
                            .primary_key_schema
                            .iter()
                            .map(|field| field.value_type.typ.clone())
                            .collect::<Box<[_]>>(),
                    ),
                    attachments,
                };
                targets_analyzed_ss[*idx] = Some(export_op_ss);

                let def_fp = FieldDefFingerprint {
                    source_op_names: data_fields_info.def_fp.source_op_names,
                    fingerprint: Fingerprinter::default()
                        .with("export")?
                        .with(&data_fields_info.def_fp.fingerprint)?
                        .with(&export_op.spec.target)?
                        .into_fingerprint(),
                };
                Ok(async move {
                    trace!("Start building executor for export op `{op_name}`");
                    let export_context = data_coll_output
                        .export_context
                        .await
                        .with_context(|| format!("Preparing for export op: {op_name}"))?;
                    trace!("Finished building executor for export op `{op_name}`");
                    Ok(AnalyzedExportOp {
                        name: op_name,
                        input: data_fields_info.local_collector_ref,
                        export_target_factory,
                        export_context,
                        primary_key_def: data_fields_info.primary_key_def,
                        primary_key_schema: data_fields_info.primary_key_schema,
                        value_fields: data_fields_info.value_fields_idx,
                        value_stable: data_fields_info.value_stable,
                        output_value_fingerprinter: data_fields_info.output_value_fingerprinter,
                        def_fp,
                    })
                })
            })
            .collect::<Result<Vec<_>>>()?;
        for (setup_key, desired_setup_state) in declarations_output {
            let decl_ss = exec_ctx::AnalyzedTargetSetupState {
                target_kind: target_kind.to_string(),
                setup_key,
                desired_setup_state,
                setup_by_user: false,
                key_type: None,
                attachments: IndexMap::new(),
            };
            declarations_analyzed_ss.push(decl_ss);
        }
        Ok(analyzed_export_ops)
    }

    async fn analyze_op_scope(
        &self,
        op_scope: &Arc<OpScope>,
        reactive_ops: &[NamedSpec<ReactiveOpSpec>],
    ) -> Result<impl Future<Output = Result<AnalyzedOpScope>> + Send + use<>> {
        let mut op_futs = Vec::with_capacity(reactive_ops.len());
        for reactive_op in reactive_ops.iter() {
            op_futs.push(self.analyze_reactive_op(op_scope, reactive_op).await?);
        }
        let collector_len = op_scope.states.lock().unwrap().collectors.len();
        let scope_qualifier = self.build_scope_qualifier(op_scope);
        let result_fut = async move {
            Ok(AnalyzedOpScope {
                reactive_ops: try_join_all(op_futs).await?,
                collector_len,
                scope_qualifier,
            })
        };
        Ok(result_fut)
    }

    fn build_scope_qualifier(&self, op_scope: &Arc<OpScope>) -> String {
        let mut scope_names = Vec::new();
        let mut current_scope = op_scope.as_ref();

        // Walk up the parent chain to collect scope names
        while let Some((parent, _)) = &current_scope.parent {
            scope_names.push(current_scope.name.as_str());
            current_scope = parent.as_ref();
        }

        // Reverse to get the correct order (root to leaf)
        scope_names.reverse();

        // Build the qualifier string
        let mut result = String::new();
        for name in scope_names {
            result.push_str(&name);
            result.push('.');
        }
        result
    }
}

pub fn build_flow_instance_context(
    flow_inst_name: &str,
    py_exec_ctx: Option<Arc<crate::py::PythonExecutionContext>>,
) -> Arc<FlowInstanceContext> {
    Arc::new(FlowInstanceContext {
        flow_instance_name: flow_inst_name.to_string(),
        auth_registry: get_auth_registry().clone(),
        py_exec_ctx: py_exec_ctx,
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

pub async fn analyze_flow(
    flow_inst: &FlowInstanceSpec,
    flow_ctx: Arc<FlowInstanceContext>,
) -> Result<(
    FlowSchema,
    AnalyzedSetupState,
    impl Future<Output = Result<ExecutionPlan>> + Send + use<>,
)> {
    let analyzer_ctx = AnalyzerContext {
        lib_ctx: get_lib_context().await?,
        flow_ctx,
    };
    let root_data_scope = Arc::new(Mutex::new(DataScopeBuilder::new()));
    let root_op_scope = OpScope::new(
        ROOT_SCOPE_NAME.to_string(),
        None,
        root_data_scope,
        FieldDefFingerprint::default(),
    );
    let mut import_ops_futs = Vec::with_capacity(flow_inst.import_ops.len());
    for import_op in flow_inst.import_ops.iter() {
        import_ops_futs.push(
            analyzer_ctx
                .analyze_import_op(&root_op_scope, import_op.clone())
                .await
                .with_context(|| format!("Preparing for import op: {}", import_op.name))?,
        );
    }
    let op_scope_fut = analyzer_ctx
        .analyze_op_scope(&root_op_scope, &flow_inst.reactive_ops)
        .await?;

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

    let mut targets_analyzed_ss = Vec::with_capacity(flow_inst.export_ops.len());
    targets_analyzed_ss.resize_with(flow_inst.export_ops.len(), || None);

    let mut declarations_analyzed_ss = Vec::with_capacity(flow_inst.declarations.len());

    for (target_kind, op_ids) in target_op_group.into_iter() {
        let target_factory = get_target_factory(&target_kind)?;
        let analyzed_target_op_group = AnalyzedExportTargetOpGroup {
            target_factory,
            target_kind: target_kind.clone(),
            op_idx: op_ids.export_op_ids,
        };
        export_ops_futs.extend(
            analyzer_ctx
                .analyze_export_op_group(
                    target_kind.as_str(),
                    &root_op_scope,
                    flow_inst,
                    &analyzed_target_op_group,
                    op_ids.declarations,
                    &mut targets_analyzed_ss,
                    &mut declarations_analyzed_ss,
                )
                .await
                .with_context(|| format!("Analyzing export ops for target `{target_kind}`"))?,
        );
        analyzed_target_op_groups.push(analyzed_target_op_group);
    }

    let flow_schema = build_flow_schema(&root_op_scope)?;
    let analyzed_ss = exec_ctx::AnalyzedSetupState {
        targets: targets_analyzed_ss
            .into_iter()
            .enumerate()
            .map(|(idx, v)| v.ok_or_else(|| internal_error!("target op `{}` not found", idx)))
            .collect::<Result<Vec<_>>>()?,
        declarations: declarations_analyzed_ss,
    };

    let legacy_fingerprint_v1 = Fingerprinter::default()
        .with(&flow_inst)?
        .with(&flow_schema.schema)?
        .into_fingerprint();

    fn append_reactive_op_scope(
        mut fingerprinter: Fingerprinter,
        reactive_ops: &[NamedSpec<ReactiveOpSpec>],
    ) -> Result<Fingerprinter> {
        fingerprinter = fingerprinter.with(&reactive_ops.len())?;
        for reactive_op in reactive_ops.iter() {
            fingerprinter = fingerprinter.with(&reactive_op.name)?;
            match &reactive_op.spec {
                ReactiveOpSpec::Transform(_) => {}
                ReactiveOpSpec::ForEach(foreach_op) => {
                    fingerprinter = fingerprinter.with(&foreach_op.field_path)?;
                    fingerprinter =
                        append_reactive_op_scope(fingerprinter, &foreach_op.op_scope.ops)?;
                }
                ReactiveOpSpec::Collect(collect_op) => {
                    fingerprinter = fingerprinter.with(collect_op)?;
                }
            }
        }
        Ok(fingerprinter)
    }
    let current_fingerprinter =
        append_reactive_op_scope(Fingerprinter::default(), &flow_inst.reactive_ops)?
            .with(&flow_inst.export_ops)?
            .with(&flow_inst.declarations)?
            .with(&flow_schema.schema)?;
    let plan_fut = async move {
        let (import_ops, op_scope, export_ops) = try_join3(
            try_join_all(import_ops_futs),
            op_scope_fut,
            try_join_all(export_ops_futs),
        )
        .await?;

        fn append_function_behavior(
            mut fingerprinter: Fingerprinter,
            reactive_ops: &[AnalyzedReactiveOp],
        ) -> Result<Fingerprinter> {
            for reactive_op in reactive_ops.iter() {
                match reactive_op {
                    AnalyzedReactiveOp::Transform(transform_op) => {
                        fingerprinter = fingerprinter.with(&transform_op.name)?.with(
                            &transform_op
                                .function_exec_info
                                .fingerprinter
                                .clone()
                                .into_fingerprint(),
                        )?;
                    }
                    AnalyzedReactiveOp::ForEach(foreach_op) => {
                        fingerprinter = append_function_behavior(
                            fingerprinter,
                            &foreach_op.op_scope.reactive_ops,
                        )?;
                    }
                    _ => {}
                }
            }
            Ok(fingerprinter)
        }
        let legacy_fingerprint_v2 =
            append_function_behavior(current_fingerprinter, &op_scope.reactive_ops)?
                .into_fingerprint();
        Ok(ExecutionPlan {
            legacy_fingerprint: vec![legacy_fingerprint_v1, legacy_fingerprint_v2],
            import_ops,
            op_scope,
            export_ops,
            export_op_groups: analyzed_target_op_groups,
        })
    };

    Ok((flow_schema, analyzed_ss, plan_fut))
}

pub async fn analyze_transient_flow<'a>(
    flow_inst: &TransientFlowSpec,
    flow_ctx: Arc<FlowInstanceContext>,
) -> Result<(
    EnrichedValueType,
    FlowSchema,
    impl Future<Output = Result<TransientExecutionPlan>> + Send + 'a,
)> {
    let mut root_data_scope = DataScopeBuilder::new();
    let analyzer_ctx = AnalyzerContext {
        lib_ctx: get_lib_context().await?,
        flow_ctx,
    };
    let mut input_fields = vec![];
    for field in flow_inst.input_fields.iter() {
        let analyzed_field = root_data_scope.add_field(
            field.name.clone(),
            &field.value_type,
            FieldDefFingerprint::default(),
        )?;
        input_fields.push(analyzed_field);
    }
    let root_op_scope = OpScope::new(
        ROOT_SCOPE_NAME.to_string(),
        None,
        Arc::new(Mutex::new(root_data_scope)),
        FieldDefFingerprint::default(),
    );
    let op_scope_fut = analyzer_ctx
        .analyze_op_scope(&root_op_scope, &flow_inst.reactive_ops)
        .await?;
    let (output_value, output_type, _) =
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
