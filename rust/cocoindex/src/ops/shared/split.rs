use crate::{
    base::field_attrs,
    fields_value,
    ops::sdk::value,
    ops::sdk::{
        BasicValueType, EnrichedValueType, FieldSchema, KTableInfo, OpArgsResolver, StructSchema,
        StructSchemaBuilder, TableKind, TableSchema, make_output_type, schema,
    },
};
use anyhow::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputPosition {
    pub char_offset: usize,
    pub line: u32,
    pub column: u32,
}

impl OutputPosition {
    pub fn into_output(self) -> value::Value {
        value::Value::Struct(fields_value!(
            self.char_offset as i64,
            self.line as i64,
            self.column as i64
        ))
    }
}

pub struct Position {
    pub byte_offset: usize,
    pub output: Option<OutputPosition>,
}

impl Position {
    pub fn new(byte_offset: usize) -> Self {
        Self {
            byte_offset,
            output: None,
        }
    }
}

/// Fill OutputPosition for the requested byte offsets.
pub fn set_output_positions<'a>(text: &str, positions: impl Iterator<Item = &'a mut Position>) {
    let mut positions = positions.collect::<Vec<_>>();
    positions.sort_by_key(|o| o.byte_offset);

    let mut positions_iter = positions.iter_mut();
    let Some(mut next_position) = positions_iter.next() else {
        return;
    };

    let mut char_offset = 0;
    let mut line = 1;
    let mut column = 1;
    for (byte_offset, ch) in text.char_indices() {
        while next_position.byte_offset == byte_offset {
            next_position.output = Some(OutputPosition {
                char_offset,
                line,
                column,
            });
            if let Some(p) = positions_iter.next() {
                next_position = p
            } else {
                return;
            }
        }
        char_offset += 1;
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }

    loop {
        next_position.output = Some(OutputPosition {
            char_offset,
            line,
            column,
        });
        if let Some(p) = positions_iter.next() {
            next_position = p
        } else {
            return;
        }
    }
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
