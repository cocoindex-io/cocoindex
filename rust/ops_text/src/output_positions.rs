//! Internal module for computing output positions from byte offsets.

/// A text range specified by byte offsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextRange {
    /// Start byte offset (inclusive).
    pub start: usize,
    /// End byte offset (exclusive).
    pub end: usize,
}

impl TextRange {
    /// Create a new text range.
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    /// Get the length of the range in bytes.
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Check if the range is empty.
    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }
}

/// Output position information with character offset and line/column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputPosition {
    /// Character (not byte) offset from the start of the text.
    pub char_offset: usize,
    /// 1-based line number.
    pub line: u32,
    /// 1-based column number.
    pub column: u32,
}

/// Position tracking helper that converts byte offsets to character positions.
pub(crate) struct Position {
    /// The byte offset in the text.
    pub byte_offset: usize,
    /// Computed output position (populated by `set_output_positions`).
    pub output: Option<OutputPosition>,
}

impl Position {
    /// Create a new position with the given byte offset.
    pub fn new(byte_offset: usize) -> Self {
        Self {
            byte_offset,
            output: None,
        }
    }
}

/// A reusable per-file index from byte offset to [`OutputPosition`]. Built once
/// in a single O(file) pass, then each lookup is a binary search over line starts
/// plus a short char count within the target line. A long-lived parse (e.g. a
/// `CodeAst` matched against many patterns) builds this once and reuses it, rather
/// than re-scanning the whole file per query (a one-shot batch is O(file) *every*
/// call, so N queries cost O(file·N)).
pub struct LineIndex {
    /// Byte offset of each line start (`[0]` is byte 0), ascending.
    line_start_byte: Vec<usize>,
    /// Char offset of each line start, parallel to `line_start_byte`.
    line_start_char: Vec<usize>,
}

impl LineIndex {
    /// Build the index from `text` in a single pass.
    pub fn build(text: &str) -> Self {
        let mut line_start_byte = vec![0usize];
        let mut line_start_char = vec![0usize];
        let mut char_count = 0usize;
        for (b, ch) in text.char_indices() {
            char_count += 1;
            if ch == '\n' {
                line_start_byte.push(b + 1); // the next line starts after the '\n'
                line_start_char.push(char_count);
            }
        }
        Self {
            line_start_byte,
            line_start_char,
        }
    }

    /// Resolve `byte_offset` to its char offset + 1-based line/column. The offset
    /// must lie on a char boundary of `text`; the end of the text is allowed.
    /// `text` must be the same string the index was built from.
    pub fn position(&self, text: &str, byte_offset: usize) -> OutputPosition {
        // `line_start_byte[0]` is 0 ≤ byte_offset, so `partition_point` ≥ 1.
        let idx = self.line_start_byte.partition_point(|&s| s <= byte_offset) - 1;
        let chars_in_line = text[self.line_start_byte[idx]..byte_offset].chars().count();
        OutputPosition {
            char_offset: self.line_start_char[idx] + chars_in_line,
            line: (idx + 1) as u32,
            column: (chars_in_line + 1) as u32,
        }
    }
}

/// Fill OutputPosition for the requested byte offsets.
///
/// This function efficiently computes character offsets, line numbers, and column
/// numbers for a set of byte positions in a single pass through the text.
pub(crate) fn set_output_positions<'a>(
    text: &str,
    positions: impl Iterator<Item = &'a mut Position>,
) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_output_positions_simple() {
        let text = "abc";
        let mut start = Position::new(0);
        let mut end = Position::new(3);

        set_output_positions(text, vec![&mut start, &mut end].into_iter());

        assert_eq!(
            start.output,
            Some(OutputPosition {
                char_offset: 0,
                line: 1,
                column: 1,
            })
        );
        assert_eq!(
            end.output,
            Some(OutputPosition {
                char_offset: 3,
                line: 1,
                column: 4,
            })
        );
    }

    #[test]
    fn test_set_output_positions_with_newlines() {
        let text = "ab\ncd\nef";
        let mut pos1 = Position::new(0);
        let mut pos2 = Position::new(3); // 'c'
        let mut pos3 = Position::new(6); // 'e'
        let mut pos4 = Position::new(8); // end

        set_output_positions(
            text,
            vec![&mut pos1, &mut pos2, &mut pos3, &mut pos4].into_iter(),
        );

        assert_eq!(
            pos1.output,
            Some(OutputPosition {
                char_offset: 0,
                line: 1,
                column: 1,
            })
        );
        assert_eq!(
            pos2.output,
            Some(OutputPosition {
                char_offset: 3,
                line: 2,
                column: 1,
            })
        );
        assert_eq!(
            pos3.output,
            Some(OutputPosition {
                char_offset: 6,
                line: 3,
                column: 1,
            })
        );
        assert_eq!(
            pos4.output,
            Some(OutputPosition {
                char_offset: 8,
                line: 3,
                column: 3,
            })
        );
    }

    #[test]
    fn test_set_output_positions_multibyte() {
        // Test with emoji (4-byte UTF-8 character)
        let text = "abc\u{1F604}def"; // abc + emoji (4 bytes) + def
        let mut start = Position::new(0);
        let mut before_emoji = Position::new(3);
        let mut after_emoji = Position::new(7); // byte position after emoji
        let mut end = Position::new(10);

        set_output_positions(
            text,
            vec![&mut start, &mut before_emoji, &mut after_emoji, &mut end].into_iter(),
        );

        assert_eq!(
            start.output,
            Some(OutputPosition {
                char_offset: 0,
                line: 1,
                column: 1,
            })
        );
        assert_eq!(
            before_emoji.output,
            Some(OutputPosition {
                char_offset: 3,
                line: 1,
                column: 4,
            })
        );
        assert_eq!(
            after_emoji.output,
            Some(OutputPosition {
                char_offset: 4, // 3 chars + 1 emoji
                line: 1,
                column: 5,
            })
        );
        assert_eq!(
            end.output,
            Some(OutputPosition {
                char_offset: 7, // 3 + 1 + 3
                line: 1,
                column: 8,
            })
        );
    }

    #[test]
    fn test_translate_bytes_to_chars_detailed() {
        // Comprehensive test moved from cocoindex
        let text = "abc\u{1F604}def";
        let mut start1 = Position::new(0);
        let mut end1 = Position::new(3);
        let mut start2 = Position::new(3);
        let mut end2 = Position::new(7);
        let mut start3 = Position::new(7);
        let mut end3 = Position::new(10);
        let mut end_full = Position::new(text.len());

        let offsets = vec![
            &mut start1,
            &mut end1,
            &mut start2,
            &mut end2,
            &mut start3,
            &mut end3,
            &mut end_full,
        ];

        set_output_positions(text, offsets.into_iter());

        assert_eq!(
            start1.output,
            Some(OutputPosition {
                char_offset: 0,
                line: 1,
                column: 1,
            })
        );
        assert_eq!(
            end1.output,
            Some(OutputPosition {
                char_offset: 3,
                line: 1,
                column: 4,
            })
        );
        assert_eq!(
            start2.output,
            Some(OutputPosition {
                char_offset: 3,
                line: 1,
                column: 4,
            })
        );
        assert_eq!(
            end2.output,
            Some(OutputPosition {
                char_offset: 4,
                line: 1,
                column: 5,
            })
        );
        assert_eq!(
            end3.output,
            Some(OutputPosition {
                char_offset: 7,
                line: 1,
                column: 8,
            })
        );
        assert_eq!(
            end_full.output,
            Some(OutputPosition {
                char_offset: 7,
                line: 1,
                column: 8,
            })
        );
    }

    #[test]
    fn line_index_matches_single_pass() {
        // LineIndex.position must agree with set_output_positions for the same
        // offsets, including newlines and multibyte chars.
        let text = "ab\ncd\nef";
        let li = LineIndex::build(text);
        for &b in &[0usize, 3, 6, 8] {
            let mut p = Position::new(b);
            set_output_positions(text, std::iter::once(&mut p));
            assert_eq!(Some(li.position(text, b)), p.output, "byte {b}");
        }

        let emoji = "abc\u{1F604}def"; // emoji is 4 bytes at byte 3..7
        let eli = LineIndex::build(emoji);
        for &b in &[0usize, 3, 7, 10] {
            let mut p = Position::new(b);
            set_output_positions(emoji, std::iter::once(&mut p));
            assert_eq!(Some(eli.position(emoji, b)), p.output, "emoji byte {b}");
        }
    }

    #[test]
    fn line_index_end_of_text_and_line_starts() {
        let text = "x\n\ny"; // line starts at bytes 0, 2, 3
        let li = LineIndex::build(text);
        // start of line 3 (the 'y')
        assert_eq!(
            li.position(text, 3),
            OutputPosition {
                char_offset: 3,
                line: 3,
                column: 1,
            }
        );
        // end of text
        assert_eq!(
            li.position(text, text.len()),
            OutputPosition {
                char_offset: 4,
                line: 3,
                column: 2,
            }
        );
    }

    #[test]
    fn test_text_range() {
        let range = TextRange::new(0, 10);
        assert_eq!(range.len(), 10);
        assert!(!range.is_empty());

        let empty = TextRange::new(5, 5);
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());
    }
}
