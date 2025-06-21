pub(crate) use crate::prelude::*;

use crate::builder::plan::AnalyzedFieldReference;
use crate::builder::plan::AnalyzedLocalFieldReference;
use std::collections::BTreeMap;

pub use super::factory_bases::*;
pub use super::interface::*;
pub use crate::base::schema::*;
pub use crate::base::spec::*;
pub use crate::base::value::*;

// Disambiguate the ExportTargetBuildOutput type.
pub use super::factory_bases::TypedExportDataCollectionBuildOutput;
pub use super::registry::ExecutorFactoryRegistry;
/// Defined for all types convertible to ValueType, to ease creation for ValueType in various operation factories.
pub trait TypeCore {
    fn into_type(self) -> ValueType;
}

impl TypeCore for BasicValueType {
    fn into_type(self) -> ValueType {
        ValueType::Basic(self)
    }
}

impl TypeCore for StructSchema {
    fn into_type(self) -> ValueType {
        ValueType::Struct(self)
    }
}

impl TypeCore for TableSchema {
    fn into_type(self) -> ValueType {
        ValueType::Table(self)
    }
}

pub fn make_output_type<Type: TypeCore>(value_type: Type) -> EnrichedValueType {
    EnrichedValueType {
        typ: value_type.into_type(),
        attrs: Default::default(),
        nullable: false,
    }
}

#[derive(Debug, Deserialize)]
pub struct EmptySpec {}

#[macro_export]
macro_rules! fields_value {
    ($($field:expr), +) => {
        $crate::base::value::FieldValues { fields: std::vec![ $(($field).into()),+ ] }
    };
}

pub struct SchemaBuilderFieldRef(AnalyzedLocalFieldReference);

impl SchemaBuilderFieldRef {
    pub fn to_field_ref(&self) -> AnalyzedFieldReference {
        AnalyzedFieldReference {
            local: self.0.clone(),
            scope_up_level: 0,
        }
    }
}
pub struct StructSchemaBuilder<'a> {
    base_fields_idx: Vec<u32>,
    target: &'a mut StructSchema,
}

impl<'a> StructSchemaBuilder<'a> {
    pub fn new(target: &'a mut StructSchema) -> Self {
        Self {
            base_fields_idx: Vec::new(),
            target,
        }
    }

    pub fn _set_description(&mut self, description: impl Into<Arc<str>>) {
        self.target.description = Some(description.into());
    }

    pub fn add_field(&mut self, field_schema: FieldSchema) -> SchemaBuilderFieldRef {
        let current_idx = self.target.fields.len() as u32;
        Arc::make_mut(&mut self.target.fields).push(field_schema);
        let mut fields_idx = self.base_fields_idx.clone();
        fields_idx.push(current_idx);
        SchemaBuilderFieldRef(AnalyzedLocalFieldReference { fields_idx })
    }

    pub fn _add_struct_field(
        &mut self,
        name: impl Into<FieldName>,
        nullable: bool,
        attrs: Arc<BTreeMap<String, serde_json::Value>>,
    ) -> (StructSchemaBuilder<'_>, SchemaBuilderFieldRef) {
        let field_schema = FieldSchema::new(
            name.into(),
            EnrichedValueType {
                typ: ValueType::Struct(StructSchema {
                    fields: Arc::new(Vec::new()),
                    description: None,
                }),
                nullable,
                attrs,
            },
        );
        let local_ref = self.add_field(field_schema);
        let struct_schema = match &mut Arc::make_mut(&mut self.target.fields)
            .last_mut()
            .unwrap()
            .value_type
            .typ
        {
            ValueType::Struct(s) => s,
            _ => unreachable!(),
        };
        (
            StructSchemaBuilder {
                base_fields_idx: local_ref.0.fields_idx.clone(),
                target: struct_schema,
            },
            local_ref,
        )
    }
}
