//! Split utilities - re-exports and schema helpers.

use crate::{
    base::field_attrs,
    fields_value,
    ops::sdk::value,
    ops::sdk::{
        BasicValueType, EnrichedValueType, FieldSchema, KTableInfo, OpArgsResolver, StructSchema,
        StructSchemaBuilder, TableKind, TableSchema, make_output_type, schema,
    },
    prelude::*,
};

// Re-export core types from extra_text
pub use cocoindex_extra_text::split::{
    // Recursive chunker
    CustomLanguageConfig,
    // Separator splitter
    KeepSeparator,
    OutputPosition,
    RecursiveChunkConfig,
    RecursiveChunker,
    RecursiveSplitConfig,
    SeparatorSplitConfig,
    SeparatorSplitter,
};

/// Convert an OutputPosition to cocoindex Value format.
pub fn output_position_to_value(pos: OutputPosition) -> value::Value {
    value::Value::Struct(fields_value!(
        pos.char_offset as i64,
        pos.line as i64,
        pos.column as i64
    ))
}

/// Build the common chunk output schema used by splitters.
/// Fields: `location: Range`, `text: Str`, `start: {offset,line,column}`, `end: {offset,line,column}`.
pub fn make_common_chunk_schema<'a>(
    args_resolver: &OpArgsResolver<'a>,
    text_arg: &crate::ops::sdk::ResolvedOpArg,
) -> Result<EnrichedValueType> {
    let pos_struct = schema::ValueType::Struct(schema::StructSchema {
        fields: std::sync::Arc::new(vec![
            schema::FieldSchema::new("offset", make_output_type(BasicValueType::Int64)),
            schema::FieldSchema::new("line", make_output_type(BasicValueType::Int64)),
            schema::FieldSchema::new("column", make_output_type(BasicValueType::Int64)),
        ]),
        description: None,
    });

    let mut struct_schema = StructSchema::default();
    let mut sb = StructSchemaBuilder::new(&mut struct_schema);
    sb.add_field(FieldSchema::new(
        "location",
        make_output_type(BasicValueType::Range),
    ));
    sb.add_field(FieldSchema::new(
        "text",
        make_output_type(BasicValueType::Str),
    ));
    sb.add_field(FieldSchema::new(
        "start",
        schema::EnrichedValueType {
            typ: pos_struct.clone(),
            nullable: false,
            attrs: Default::default(),
        },
    ));
    sb.add_field(FieldSchema::new(
        "end",
        schema::EnrichedValueType {
            typ: pos_struct,
            nullable: false,
            attrs: Default::default(),
        },
    ));
    let output_schema = make_output_type(TableSchema::new(
        TableKind::KTable(KTableInfo { num_key_parts: 1 }),
        struct_schema,
    ))
    .with_attr(
        field_attrs::CHUNK_BASE_TEXT,
        serde_json::to_value(args_resolver.get_analyze_value(text_arg))?,
    );
    Ok(output_schema)
}
