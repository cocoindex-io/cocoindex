use crate::ops::sdk::*;
use globset::{Glob, GlobSet, GlobSetBuilder};

/// Builds a GlobSet from a vector of pattern strings
fn build_glob_set(patterns: Vec<String>) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern.as_str())?);
    }
    Ok(builder.build()?)
}

/// Pattern matcher that handles include and exclude patterns for files
#[derive(Debug)]
pub struct PatternMatcher {
    /// Patterns matching full path of files to be included.
    included_glob_set: Option<GlobSet>,
    /// Patterns matching full path of files and directories to be excluded.
    /// If a directory is excluded, all files and subdirectories within it are also excluded.
    excluded_glob_set: Option<GlobSet>,
}

impl PatternMatcher {
    /// Create a new PatternMatcher from optional include and exclude pattern vectors
    pub fn new(
        included_patterns: Option<Vec<String>>,
        excluded_patterns: Option<Vec<String>>,
    ) -> Result<Self> {
        let included_glob_set = included_patterns.map(build_glob_set).transpose()?;
        let excluded_glob_set = excluded_patterns.map(build_glob_set).transpose()?;

        Ok(Self {
            included_glob_set,
            excluded_glob_set,
        })
    }

    /// Check if a file or directory is excluded by the exclude patterns
    /// Can be called on directories to prune traversal on excluded directories.
    pub fn is_excluded(&self, path: &str) -> bool {
        self.excluded_glob_set
            .as_ref()
            .is_some_and(|glob_set| glob_set.is_match(path))
    }

    /// Check if a file should be included based on both include and exclude patterns
    /// Should be called for each file.
    pub fn is_file_included(&self, path: &str) -> bool {
        self.included_glob_set
            .as_ref()
            .is_none_or(|glob_set| glob_set.is_match(path))
            && !self.is_excluded(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_matcher_no_patterns() {
        let matcher = PatternMatcher::new(None, None).unwrap();
        assert!(matcher.is_file_included("test.txt"));
        assert!(matcher.is_file_included("path/to/file.rs"));
        assert!(!matcher.is_excluded("anything"));
    }

    #[test]
    fn test_pattern_matcher_include_only() {
        let matcher =
            PatternMatcher::new(Some(vec!["*.txt".to_string(), "*.rs".to_string()]), None).unwrap();

        assert!(matcher.is_file_included("test.txt"));
        assert!(matcher.is_file_included("main.rs"));
        assert!(!matcher.is_file_included("image.png"));
    }

    #[test]
    fn test_pattern_matcher_exclude_only() {
        let matcher =
            PatternMatcher::new(None, Some(vec!["*.tmp".to_string(), "*.log".to_string()]))
                .unwrap();

        assert!(matcher.is_file_included("test.txt"));
        assert!(!matcher.is_file_included("temp.tmp"));
        assert!(!matcher.is_file_included("debug.log"));
    }

    #[test]
    fn test_pattern_matcher_both_patterns() {
        let matcher = PatternMatcher::new(
            Some(vec!["*.txt".to_string()]),
            Some(vec!["*temp*".to_string()]),
        )
        .unwrap();

        assert!(matcher.is_file_included("test.txt"));
        assert!(!matcher.is_file_included("temp.txt")); // excluded despite matching include
        assert!(!matcher.is_file_included("main.rs")); // doesn't match include
    }
}
