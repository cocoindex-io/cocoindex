use super::schema;
use schemars::schema::{
    ArrayValidation, InstanceType, Metadata, ObjectValidation, Schema, SchemaObject, SingleOrVec,
};

pub struct ToJsonSchemaOptions {
    /// If true, mark all fields as required.
    /// Use union type (with `null`) for optional fields instead.
    /// Models like OpenAI will reject the schema if a field is not required.
    pub fields_always_required: bool,
}

pub trait ToJsonSchema {
    fn to_json_schema(&self, options: &ToJsonSchemaOptions) -> SchemaObject;
}

impl ToJsonSchema for schema::BasicValueType {
    fn to_json_schema(&self, options: &ToJsonSchemaOptions) -> SchemaObject {
        let mut schema = SchemaObject::default();
        match self {
            schema::BasicValueType::Str => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::String)));
            }
            schema::BasicValueType::Bytes => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::String)));
            }
            schema::BasicValueType::Bool => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::Boolean)));
            }
            schema::BasicValueType::Int64 => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::Integer)));
            }
            schema::BasicValueType::Float32 | schema::BasicValueType::Float64 => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::Number)));
            }
            schema::BasicValueType::Range => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::Array)));
                schema.array = Some(Box::new(ArrayValidation {
                    items: Some(SingleOrVec::Single(Box::new(
                        SchemaObject {
                            instance_type: Some(SingleOrVec::Single(Box::new(
                                InstanceType::Integer,
                            ))),
                            ..Default::default()
                        }
                        .into(),
                    ))),
                    min_items: Some(2),
                    max_items: Some(2),
                    ..Default::default()
                }));
                schema
                    .metadata
                    .get_or_insert_with(Default::default)
                    .description =
                    Some("A range, start pos (inclusive), end pos (exclusive).".to_string());
            }
            schema::BasicValueType::Uuid => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::String)));
                schema.format = Some("uuid".to_string());
            }
            schema::BasicValueType::Json => {
                // Can be any value. No type constraint.
            }
            schema::BasicValueType::Vector(s) => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::Array)));
                schema.array = Some(Box::new(ArrayValidation {
                    items: Some(SingleOrVec::Single(Box::new(
                        s.element_type.to_json_schema(options).into(),
                    ))),
                    min_items: s.dimension.and_then(|d| u32::try_from(d).ok()),
                    max_items: s.dimension.and_then(|d| u32::try_from(d).ok()),
                    ..Default::default()
                }));
            }
        }
        schema
    }
}

impl ToJsonSchema for schema::StructSchema {
    fn to_json_schema(&self, options: &ToJsonSchemaOptions) -> SchemaObject {
        SchemaObject {
            metadata: Some(Box::new(Metadata {
                description: self.description.as_ref().map(|s| s.to_string()),
                ..Default::default()
            })),
            instance_type: Some(SingleOrVec::Single(Box::new(InstanceType::Object))),
            object: Some(Box::new(ObjectValidation {
                properties: self
                    .fields
                    .iter()
                    .map(|f| {
                        let mut schema = f.value_type.to_json_schema(options);
                        if options.fields_always_required && f.value_type.nullable {
                            if let Some(instance_type) = &mut schema.instance_type {
                                let mut types = match instance_type {
                                    SingleOrVec::Single(t) => vec![**t],
                                    SingleOrVec::Vec(t) => std::mem::take(t),
                                };
                                types.push(InstanceType::Null);
                                *instance_type = SingleOrVec::Vec(types);
                            }
                        }
                        (f.name.to_string(), schema.into())
                    })
                    .collect(),
                required: self
                    .fields
                    .iter()
                    .filter(|&f| (options.fields_always_required || !f.value_type.nullable))
                    .map(|f| f.name.to_string())
                    .collect(),
                additional_properties: Some(Schema::Bool(false).into()),
                ..Default::default()
            })),
            ..Default::default()
        }
    }
}

impl ToJsonSchema for schema::ValueType {
    fn to_json_schema(&self, options: &ToJsonSchemaOptions) -> SchemaObject {
        match self {
            schema::ValueType::Basic(b) => b.to_json_schema(options),
            schema::ValueType::Struct(s) => s.to_json_schema(options),
            schema::ValueType::Collection(c) => SchemaObject {
                instance_type: Some(SingleOrVec::Single(Box::new(InstanceType::Array))),
                array: Some(Box::new(ArrayValidation {
                    items: Some(SingleOrVec::Single(Box::new(
                        c.row.to_json_schema(options).into(),
                    ))),
                    ..Default::default()
                })),
                ..Default::default()
            },
        }
    }
}

impl ToJsonSchema for schema::EnrichedValueType {
    fn to_json_schema(&self, options: &ToJsonSchemaOptions) -> SchemaObject {
        self.typ.to_json_schema(options)
    }
}
