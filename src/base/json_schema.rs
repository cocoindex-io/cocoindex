use crate::prelude::*;

use crate::utils::immutable::RefList;
use schemars::schema::{
    ArrayValidation, InstanceType, ObjectValidation, Schema, SchemaObject, SingleOrVec,
    SubschemaValidation,
};
use std::fmt::Write;

pub struct ToJsonSchemaOptions {
    /// If true, mark all fields as required.
    /// Use union type (with `null`) for optional fields instead.
    /// Models like OpenAI will reject the schema if a field is not required.
    pub fields_always_required: bool,

    /// If true, the JSON schema supports the `format` keyword.
    pub supports_format: bool,

    /// If true, extract descriptions to a separate extra instruction.
    pub extract_descriptions: bool,

    /// If true, the top level must be a JSON object.
    pub top_level_must_be_object: bool,
}

struct JsonSchemaBuilder {
    options: ToJsonSchemaOptions,
    extra_instructions_per_field: IndexMap<String, String>,
}

impl JsonSchemaBuilder {
    fn new(options: ToJsonSchemaOptions) -> Self {
        Self {
            options,
            extra_instructions_per_field: IndexMap::new(),
        }
    }

    fn set_description(
        &mut self,
        schema: &mut SchemaObject,
        description: impl ToString,
        field_path: RefList<'_, &'_ spec::FieldName>,
    ) {
        if self.options.extract_descriptions {
            let mut fields: Vec<_> = field_path.iter().map(|f| f.as_str()).collect();
            fields.reverse();
            self.extra_instructions_per_field
                .insert(fields.join("."), description.to_string());
        } else {
            schema.metadata.get_or_insert_default().description = Some(description.to_string());
        }
    }

    fn for_basic_value_type(
        &mut self,
        basic_type: &schema::BasicValueType,
        field_path: RefList<'_, &'_ spec::FieldName>,
    ) -> SchemaObject {
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
                self.set_description(
                    &mut schema,
                    "A range represented by a list of two positions, start pos (inclusive), end pos (exclusive).",
                    field_path,
                );
            }
            schema::BasicValueType::Uuid => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::String)));
                if self.options.supports_format {
                    schema.format = Some("uuid".to_string());
                }
                self.set_description(
                    &mut schema,
                    "A UUID, e.g. 123e4567-e89b-12d3-a456-426614174000",
                    field_path,
                );
            }
            schema::BasicValueType::Date => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::String)));
                if self.options.supports_format {
                    schema.format = Some("date".to_string());
                }
                self.set_description(
                    &mut schema,
                    "A date in YYYY-MM-DD format, e.g. 2025-03-27",
                    field_path,
                );
            }
            schema::BasicValueType::Time => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::String)));
                if self.options.supports_format {
                    schema.format = Some("time".to_string());
                }
                self.set_description(
                    &mut schema,
                    "A time in HH:MM:SS format, e.g. 13:32:12",
                    field_path,
                );
            }
            schema::BasicValueType::LocalDateTime => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::String)));
                if self.options.supports_format {
                    schema.format = Some("date-time".to_string());
                }
                self.set_description(
                    &mut schema,
                    "Date time without timezone offset in YYYY-MM-DDTHH:MM:SS format, e.g. 2025-03-27T13:32:12",
                    field_path,
                );
            }
            schema::BasicValueType::OffsetDateTime => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::String)));
                if self.options.supports_format {
                    schema.format = Some("date-time".to_string());
                }
                self.set_description(
                    &mut schema,
                    "Date time with timezone offset in RFC3339, e.g. 2025-03-27T13:32:12Z, 2025-03-27T07:32:12.313-06:00",
                    field_path,
                );
            }
            &schema::BasicValueType::TimeDelta => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::String)));
                if self.options.supports_format {
                    schema.format = Some("duration".to_string());
                }
                self.set_description(
                    &mut schema,
                    "A duration, e.g. 'PT1H2M3S' (ISO 8601) or '1 day 2 hours 3 seconds'",
                    field_path,
                );
            }
            schema::BasicValueType::Json => {
                // Can be any value. No type constraint.
            }
            schema::BasicValueType::Vector(s) => {
                schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::Array)));
                schema.array = Some(Box::new(ArrayValidation {
                    items: Some(SingleOrVec::Single(Box::new(
                        self.for_basic_value_type(&s.element_type, field_path)
                            .into(),
                    ))),
                    min_items: s.dimension.and_then(|d| u32::try_from(d).ok()),
                    max_items: s.dimension.and_then(|d| u32::try_from(d).ok()),
                    ..Default::default()
                }));
            }
            schema::BasicValueType::Union(s) => {
                schema.subschemas = Some(Box::new(SubschemaValidation {
                    one_of: Some(
                        s.types
                            .iter()
                            .map(|t| Schema::Object(self.for_basic_value_type(t, field_path)))
                            .collect(),
                    ),
                    ..Default::default()
                }));
            }
        }
        schema
    }

    fn for_struct_schema(
        &mut self,
        struct_schema: &schema::StructSchema,
        field_path: RefList<'_, &'_ spec::FieldName>,
    ) -> SchemaObject {
        let mut schema = SchemaObject::default();
        if let Some(description) = &struct_schema.description {
            self.set_description(&mut schema, description, field_path);
        }
        schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::Object)));
        schema.object = Some(Box::new(ObjectValidation {
            properties: struct_schema
                .fields
                .iter()
                .map(|f| {
                    let mut schema =
                        self.for_enriched_value_type(&f.value_type, field_path.prepend(&f.name));
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
        }));
        schema
    }

    fn for_value_type(
        &mut self,
        value_type: &schema::ValueType,
        field_path: RefList<'_, &'_ spec::FieldName>,
    ) -> SchemaObject {
        match value_type {
            schema::ValueType::Basic(b) => self.for_basic_value_type(b, field_path),
            schema::ValueType::Struct(s) => self.for_struct_schema(s, field_path),
            schema::ValueType::Table(c) => SchemaObject {
                instance_type: Some(SingleOrVec::Single(Box::new(InstanceType::Array))),
                array: Some(Box::new(ArrayValidation {
                    items: Some(SingleOrVec::Single(Box::new(
                        self.for_struct_schema(&c.row, field_path).into(),
                    ))),
                    ..Default::default()
                })),
                ..Default::default()
            },
        }
    }

    fn for_enriched_value_type(
        &mut self,
        enriched_value_type: &schema::EnrichedValueType,
        field_path: RefList<'_, &'_ spec::FieldName>,
    ) -> SchemaObject {
        self.for_value_type(&enriched_value_type.typ, field_path)
    }

    fn build_extra_instructions(&self) -> Result<Option<String>> {
        if self.extra_instructions_per_field.is_empty() {
            return Ok(None);
        }

        let mut instructions = String::new();
        write!(&mut instructions, "Instructions for specific fields:\n\n")?;
        for (field_path, instruction) in self.extra_instructions_per_field.iter() {
            write!(
                &mut instructions,
                "- {}: {}\n\n",
                if field_path.is_empty() {
                    "(root object)"
                } else {
                    field_path.as_str()
                },
                instruction
            )?;
        }
        Ok(Some(instructions))
    }
}

pub struct ValueExtractor {
    value_type: schema::ValueType,
    object_wrapper_field_name: Option<String>,
}

impl ValueExtractor {
    pub fn extract_value(&self, json_value: serde_json::Value) -> Result<value::Value> {
        let unwrapped_json_value =
            if let Some(object_wrapper_field_name) = &self.object_wrapper_field_name {
                match json_value {
                    serde_json::Value::Object(mut o) => o
                        .remove(object_wrapper_field_name)
                        .unwrap_or(serde_json::Value::Null),
                    _ => {
                        bail!("Field `{}` not found", object_wrapper_field_name)
                    }
                }
            } else {
                json_value
            };
        let result = value::Value::from_json(unwrapped_json_value, &self.value_type)?;
        Ok(result)
    }
}

pub struct BuildJsonSchemaOutput {
    pub schema: SchemaObject,
    pub extra_instructions: Option<String>,
    pub value_extractor: ValueExtractor,
}

pub fn build_json_schema(
    value_type: schema::EnrichedValueType,
    options: ToJsonSchemaOptions,
) -> Result<BuildJsonSchemaOutput> {
    let mut builder = JsonSchemaBuilder::new(options);
    let (schema, object_wrapper_field_name) = if builder.options.top_level_must_be_object
        && !matches!(value_type.typ, schema::ValueType::Struct(_))
    {
        let object_wrapper_field_name = "value".to_string();
        let wrapper_struct = schema::StructSchema {
            fields: Arc::new(vec![schema::FieldSchema {
                name: object_wrapper_field_name.clone(),
                value_type: value_type.clone(),
            }]),
            description: None,
        };
        (
            builder.for_struct_schema(&wrapper_struct, RefList::Nil),
            Some(object_wrapper_field_name),
        )
    } else {
        (
            builder.for_enriched_value_type(&value_type, RefList::Nil),
            None,
        )
    };
    Ok(BuildJsonSchemaOutput {
        schema,
        extra_instructions: builder.build_extra_instructions()?,
        value_extractor: ValueExtractor {
            value_type: value_type.typ,
            object_wrapper_field_name,
        },
    })
}
