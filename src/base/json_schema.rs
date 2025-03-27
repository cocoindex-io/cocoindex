use super::schema;
use schemars::schema::{
    ArrayValidation, InstanceType, Metadata, ObjectValidation, Schema, SchemaObject, SingleOrVec,
};

pub struct ToJsonSchemaOptions {
    /// If true, mark all fields as required.
    /// Use union type (with `null`) for optional fields instead.
    /// Models like OpenAI will reject the schema if a field is not required.
    pub fields_always_required: bool,

    /// If true, the JSON schema supports the `format` keyword.
    pub supports_format: bool,
}

pub struct JsonSchemaBuilder {
    options: ToJsonSchemaOptions,
}

impl JsonSchemaBuilder {
    pub fn new(options: ToJsonSchemaOptions) -> Self {
        Self { options }
    }

    fn for_basic_value_type(&mut self, basic_type: &schema::BasicValueType) -> SchemaObject {
        let mut schema = SchemaObject::default();
        match basic_type {
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
                schema.metadata.get_or_insert_default().description =
                    Some("A range, start pos (inclusive), end pos (exclusive).".to_string());
            }
            schema::BasicValueType::Uuid => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::String)));
                if self.options.supports_format {
                    schema.format = Some("uuid".to_string());
                }
                schema.metadata.get_or_insert_default().description =
                    Some("A UUID, e.g. 123e4567-e89b-12d3-a456-426614174000".to_string());
            }
            schema::BasicValueType::Date => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::String)));
                if self.options.supports_format {
                    schema.format = Some("date".to_string());
                }
                schema.metadata.get_or_insert_default().title =
                    Some("A date in YYYY-MM-DD format, e.g. 2025-03-27".to_string());
            }
            schema::BasicValueType::Time => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::String)));
                if self.options.supports_format {
                    schema.format = Some("time".to_string());
                }
                schema.metadata.get_or_insert_default().description =
                    Some("A time in HH:MM:SS format, e.g. 13:32:12".to_string());
            }
            schema::BasicValueType::LocalDateTime => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::String)));
                if self.options.supports_format {
                    schema.format = Some("date-time".to_string());
                }
                schema.metadata.get_or_insert_default().description =
                    Some("Date time without timezone offset in YYYY-MM-DDTHH:MM:SS format, e.g. 2025-03-27T13:32:12".to_string());
            }
            schema::BasicValueType::OffsetDateTime => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::String)));
                if self.options.supports_format {
                    schema.format = Some("date-time".to_string());
                }
                schema.metadata.get_or_insert_default().description =
                    Some("Date time with timezone offset in RFC3339, e.g. 2025-03-27T13:32:12Z, 2025-03-27T07:32:12.313-06:00".to_string());
            }
            schema::BasicValueType::Json => {
                // Can be any value. No type constraint.
            }
            schema::BasicValueType::Vector(s) => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::Array)));
                schema.array = Some(Box::new(ArrayValidation {
                    items: Some(SingleOrVec::Single(Box::new(
                        self.for_basic_value_type(&s.element_type).into(),
                    ))),
                    min_items: s.dimension.and_then(|d| u32::try_from(d).ok()),
                    max_items: s.dimension.and_then(|d| u32::try_from(d).ok()),
                    ..Default::default()
                }));
            }
        }
        schema
    }

    fn for_struct_schema(&mut self, struct_schema: &schema::StructSchema) -> SchemaObject {
        SchemaObject {
            metadata: Some(Box::new(Metadata {
                description: struct_schema.description.as_ref().map(|s| s.to_string()),
                ..Default::default()
            })),
            instance_type: Some(SingleOrVec::Single(Box::new(InstanceType::Object))),
            object: Some(Box::new(ObjectValidation {
                properties: struct_schema
                    .fields
                    .iter()
                    .map(|f| {
                        let mut schema = self.for_enriched_value_type(&f.value_type);
                        if self.options.fields_always_required && f.value_type.nullable {
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
                required: struct_schema
                    .fields
                    .iter()
                    .filter(|&f| (self.options.fields_always_required || !f.value_type.nullable))
                    .map(|f| f.name.to_string())
                    .collect(),
                additional_properties: Some(Schema::Bool(false).into()),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    fn for_value_type(&mut self, value_type: &schema::ValueType) -> SchemaObject {
        match value_type {
            schema::ValueType::Basic(b) => self.for_basic_value_type(b),
            schema::ValueType::Struct(s) => self.for_struct_schema(s),
            schema::ValueType::Collection(c) => SchemaObject {
                instance_type: Some(SingleOrVec::Single(Box::new(InstanceType::Array))),
                array: Some(Box::new(ArrayValidation {
                    items: Some(SingleOrVec::Single(Box::new(
                        self.for_struct_schema(&c.row).into(),
                    ))),
                    ..Default::default()
                })),
                ..Default::default()
            },
        }
    }

    pub fn for_enriched_value_type(
        &mut self,
        enriched_value_type: &schema::EnrichedValueType,
    ) -> SchemaObject {
        self.for_value_type(&enriched_value_type.typ)
    }
}
