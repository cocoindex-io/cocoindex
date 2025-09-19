use crate::{fields_value, ops::sdk::value};

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
