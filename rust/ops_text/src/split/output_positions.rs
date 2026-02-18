//! Internal module for computing output positions from byte offsets.

use super::OutputPosition;

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
}
