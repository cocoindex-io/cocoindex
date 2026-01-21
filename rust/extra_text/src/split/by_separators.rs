//! Split text by regex separators.

use regex::Regex;

use super::output_positions::{Position, set_output_positions};
use super::{Chunk, TextRange};

/// How to handle separators when splitting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeepSeparator {
    /// Include separator at the end of the preceding chunk.
    Left,
    /// Include separator at the start of the following chunk.
    Right,
}

/// Configuration for separator-based text splitting.
#[derive(Debug, Clone)]
pub struct SeparatorSplitConfig {
    /// Regex patterns for separators. They are OR-joined into a single pattern.
    pub separators_regex: Vec<String>,
    /// How to handle separators (None means discard them).
    pub keep_separator: Option<KeepSeparator>,
    /// Whether to include empty chunks in the output.
    pub include_empty: bool,
    /// Whether to trim whitespace from chunks.
    pub trim: bool,
}

impl Default for SeparatorSplitConfig {
    fn default() -> Self {
        Self {
            separators_regex: vec![],
            keep_separator: None,
            include_empty: false,
            trim: true,
        }
    }
}

/// A text splitter that splits by regex separators.
pub struct SeparatorSplitter {
    config: SeparatorSplitConfig,
    regex: Option<Regex>,
}

impl SeparatorSplitter {
    /// Create a new separator splitter with the given configuration.
    ///
    /// Returns an error if the regex patterns are invalid.
    pub fn new(config: SeparatorSplitConfig) -> Result<Self, regex::Error> {
        let regex = if config.separators_regex.is_empty() {
            None
        } else {
            // OR-join all separators with multiline mode
            let pattern = format!(
                "(?m){}",
                config
                    .separators_regex
                    .iter()
                    .map(|s| format!("(?:{s})"))
                    .collect::<Vec<_>>()
                    .join("|")
            );
            Some(Regex::new(&pattern)?)
        };
        Ok(Self { config, regex })
    }

    /// Split the text and return chunks with position information.
    pub fn split(&self, text: &str) -> Vec<Chunk> {
        let bytes = text.as_bytes();

        // Collect raw chunks (byte ranges)
        struct RawChunk {
            start: usize,
            end: usize,
        }

        let mut raw_chunks: Vec<RawChunk> = Vec::new();

        let mut add_range = |mut s: usize, mut e: usize| {
            if self.config.trim {
                while s < e && bytes[s].is_ascii_whitespace() {
                    s += 1;
                }
                while e > s && bytes[e - 1].is_ascii_whitespace() {
                    e -= 1;
                }
            }
            if self.config.include_empty || e > s {
                raw_chunks.push(RawChunk { start: s, end: e });
            }
        };

        if let Some(re) = &self.regex {
            let mut start = 0usize;
            for m in re.find_iter(text) {
                let end = match self.config.keep_separator {
                    Some(KeepSeparator::Left) => m.end(),
                    Some(KeepSeparator::Right) | None => m.start(),
                };
                add_range(start, end);
                start = match self.config.keep_separator {
                    Some(KeepSeparator::Right) => m.start(),
                    _ => m.end(),
                };
            }
            add_range(start, text.len());
        } else {
            // No separators: emit whole text
            add_range(0, text.len());
        }

        // Compute positions for all chunks
        let mut positions: Vec<Position> = raw_chunks
            .iter()
            .flat_map(|c| vec![Position::new(c.start), Position::new(c.end)])
            .collect();

        set_output_positions(text, positions.iter_mut());

        // Build final chunks
        raw_chunks
            .into_iter()
            .enumerate()
            .map(|(i, raw)| {
                let start_pos = positions[i * 2].output.unwrap();
                let end_pos = positions[i * 2 + 1].output.unwrap();
                Chunk {
                    range: TextRange::new(raw.start, raw.end),
                    start: start_pos,
                    end: end_pos,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_by_paragraphs() {
        let config = SeparatorSplitConfig {
            separators_regex: vec![r"\n\n+".to_string()],
            keep_separator: None,
            include_empty: false,
            trim: true,
        };
        let splitter = SeparatorSplitter::new(config).unwrap();
        let text = "Para1\n\nPara2\n\n\nPara3";
        let chunks = splitter.split(text);

        assert_eq!(chunks.len(), 3);
        assert_eq!(&text[chunks[0].range.start..chunks[0].range.end], "Para1");
        assert_eq!(&text[chunks[1].range.start..chunks[1].range.end], "Para2");
        assert_eq!(&text[chunks[2].range.start..chunks[2].range.end], "Para3");
    }

    #[test]
    fn test_split_keep_separator_left() {
        let config = SeparatorSplitConfig {
            separators_regex: vec![r"\.".to_string()],
            keep_separator: Some(KeepSeparator::Left),
            include_empty: false,
            trim: true,
        };
        let splitter = SeparatorSplitter::new(config).unwrap();
        let text = "A. B. C.";
        let chunks = splitter.split(text);

        assert_eq!(chunks.len(), 3);
        assert_eq!(&text[chunks[0].range.start..chunks[0].range.end], "A.");
        assert_eq!(&text[chunks[1].range.start..chunks[1].range.end], "B.");
        assert_eq!(&text[chunks[2].range.start..chunks[2].range.end], "C.");
    }

    #[test]
    fn test_split_keep_separator_right() {
        let config = SeparatorSplitConfig {
            separators_regex: vec![r"\.".to_string()],
            keep_separator: Some(KeepSeparator::Right),
            include_empty: false,
            trim: true,
        };
        let splitter = SeparatorSplitter::new(config).unwrap();
        let text = "A. B. C";
        let chunks = splitter.split(text);

        assert_eq!(chunks.len(), 3);
        assert_eq!(&text[chunks[0].range.start..chunks[0].range.end], "A");
        assert_eq!(&text[chunks[1].range.start..chunks[1].range.end], ". B");
        assert_eq!(&text[chunks[2].range.start..chunks[2].range.end], ". C");
    }

    #[test]
    fn test_split_no_separators() {
        let config = SeparatorSplitConfig {
            separators_regex: vec![],
            keep_separator: None,
            include_empty: false,
            trim: true,
        };
        let splitter = SeparatorSplitter::new(config).unwrap();
        let text = "Hello World";
        let chunks = splitter.split(text);

        assert_eq!(chunks.len(), 1);
        assert_eq!(
            &text[chunks[0].range.start..chunks[0].range.end],
            "Hello World"
        );
    }

    #[test]
    fn test_split_with_trim() {
        let config = SeparatorSplitConfig {
            separators_regex: vec![r"\|".to_string()],
            keep_separator: None,
            include_empty: false,
            trim: true,
        };
        let splitter = SeparatorSplitter::new(config).unwrap();
        let text = "  A  |  B  |  C  ";
        let chunks = splitter.split(text);

        assert_eq!(chunks.len(), 3);
        assert_eq!(&text[chunks[0].range.start..chunks[0].range.end], "A");
        assert_eq!(&text[chunks[1].range.start..chunks[1].range.end], "B");
        assert_eq!(&text[chunks[2].range.start..chunks[2].range.end], "C");
    }

    #[test]
    fn test_split_include_empty() {
        let config = SeparatorSplitConfig {
            separators_regex: vec![r"\|".to_string()],
            keep_separator: None,
            include_empty: true,
            trim: true,
        };
        let splitter = SeparatorSplitter::new(config).unwrap();
        let text = "A||B";
        let chunks = splitter.split(text);

        assert_eq!(chunks.len(), 3);
        assert_eq!(&text[chunks[0].range.start..chunks[0].range.end], "A");
        assert_eq!(&text[chunks[1].range.start..chunks[1].range.end], "");
        assert_eq!(&text[chunks[2].range.start..chunks[2].range.end], "B");
    }

    #[test]
    fn test_split_positions() {
        let config = SeparatorSplitConfig {
            separators_regex: vec![r"\n".to_string()],
            keep_separator: None,
            include_empty: false,
            trim: false,
        };
        let splitter = SeparatorSplitter::new(config).unwrap();
        let text = "Line1\nLine2\nLine3";
        let chunks = splitter.split(text);

        assert_eq!(chunks.len(), 3);

        // Check positions
        assert_eq!(chunks[0].start.line, 1);
        assert_eq!(chunks[0].start.column, 1);
        assert_eq!(chunks[0].end.line, 1);
        assert_eq!(chunks[0].end.column, 6);

        assert_eq!(chunks[1].start.line, 2);
        assert_eq!(chunks[1].start.column, 1);

        assert_eq!(chunks[2].start.line, 3);
        assert_eq!(chunks[2].start.column, 1);
    }
}
