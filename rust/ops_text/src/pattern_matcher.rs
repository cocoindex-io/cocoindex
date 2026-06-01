use anyhow::Result;
use globset::{Glob, GlobMatcher, GlobSet, GlobSetBuilder};

/// Builds a GlobSet from a vector of pattern strings
fn build_glob_set(patterns: Vec<String>) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern.as_str())?);
    }
    Ok(builder.build()?)
}

#[derive(Debug)]
struct ExcludeRule {
    matcher: GlobMatcher,
    excludes: bool,
    literal_components: Vec<String>,
}

fn literal_components(pattern: &str) -> Vec<String> {
    pattern
        .split('/')
        .filter(|component| {
            !component.is_empty()
                && *component != "**"
                && !component
                    .chars()
                    .any(|ch| matches!(ch, '*' | '?' | '[' | ']' | '{' | '}'))
        })
        .map(str::to_string)
        .collect()
}

fn path_components(path: &str) -> Vec<&str> {
    path.split('/')
        .filter(|component| !component.is_empty())
        .collect()
}

impl ExcludeRule {
    fn new(pattern: String) -> Result<Self> {
        let (excludes, glob_pattern) = if let Some(negated) = pattern.strip_prefix('!') {
            (false, negated)
        } else {
            (true, pattern.as_str())
        };
        let matcher = Glob::new(glob_pattern)?.compile_matcher();
        Ok(Self {
            matcher,
            excludes,
            literal_components: literal_components(glob_pattern),
        })
    }

    fn could_match_descendant(&self, path: &str) -> bool {
        if self.excludes {
            return false;
        }
        // Keep an excluded parent traversable when a later negated pattern can
        // re-include one of its descendants, e.g. **/.* then !**/.github/**.
        if self.matcher.is_match(path) || self.matcher.is_match(format!("{path}/")) {
            return true;
        }
        if self.literal_components.is_empty() {
            return true;
        }

        let components = path_components(path);
        let max_overlap = components.len().min(self.literal_components.len());
        (1..=max_overlap).any(|overlap| {
            components[components.len() - overlap..]
                .iter()
                .zip(&self.literal_components[..overlap])
                .all(|(path_component, pattern_component)| path_component == pattern_component)
        })
    }
}

fn build_exclude_rules(patterns: Vec<String>) -> Result<Vec<ExcludeRule>> {
    patterns.into_iter().map(ExcludeRule::new).collect()
}

/// Pattern matcher that handles include and exclude patterns for files
#[derive(Debug)]
pub struct PatternMatcher {
    /// Patterns matching full path of files to be included.
    included_glob_set: Option<GlobSet>,
    /// Patterns matching full path of files and directories to be excluded.
    /// If a directory is excluded, all files and subdirectories within it are also excluded.
    excluded_rules: Option<Vec<ExcludeRule>>,
}

impl PatternMatcher {
    /// Create a new PatternMatcher from optional include and exclude pattern vectors
    pub fn new(
        included_patterns: Option<Vec<String>>,
        excluded_patterns: Option<Vec<String>>,
    ) -> Result<Self> {
        let included_glob_set = included_patterns.map(build_glob_set).transpose()?;
        let excluded_rules = excluded_patterns.map(build_exclude_rules).transpose()?;

        Ok(Self {
            included_glob_set,
            excluded_rules,
        })
    }

    /// Check if a file or directory is excluded by the exclude patterns
    /// Can be called on directories to prune traversal on excluded directories.
    pub fn is_excluded(&self, path: &str) -> bool {
        self.excluded_rules.as_ref().is_some_and(|rules| {
            let mut excluded = false;
            for rule in rules {
                if rule.matcher.is_match(path) {
                    excluded = rule.excludes;
                }
            }
            excluded
        })
    }

    /// Check if a directory should be included (traversed) based on the exclude patterns.
    /// Directories are included unless they match an exclude pattern.
    pub fn is_dir_included(&self, path: &str) -> bool {
        if !self.is_excluded(path) {
            return true;
        }
        self.excluded_rules
            .as_ref()
            .is_some_and(|rules| rules.iter().any(|rule| rule.could_match_descendant(path)))
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

    #[test]
    fn test_pattern_matcher_exclude_negation_reincludes_file() {
        let matcher = PatternMatcher::new(
            Some(vec!["**/*.tmp".to_string()]),
            Some(vec!["**/*.tmp".to_string(), "!keep.tmp".to_string()]),
        )
        .unwrap();

        assert!(matcher.is_file_included("keep.tmp"));
        assert!(!matcher.is_file_included("scratch.tmp"));
    }

    #[test]
    fn test_is_dir_included_no_patterns() {
        let matcher = PatternMatcher::new(None, None).unwrap();
        assert!(matcher.is_dir_included("any_dir"));
        assert!(matcher.is_dir_included(".git"));
    }

    #[test]
    fn test_is_dir_included_with_excluded() {
        let matcher = PatternMatcher::new(None, Some(vec!["**/.*".to_string()])).unwrap();
        assert!(!matcher.is_dir_included(".git"));
        assert!(!matcher.is_dir_included("src/.hidden"));
        assert!(matcher.is_dir_included("src"));
        assert!(matcher.is_dir_included("node_modules"));
    }

    #[test]
    fn test_is_dir_included_for_negated_descendant_pattern() {
        let matcher = PatternMatcher::new(
            None,
            Some(vec!["**/.*".to_string(), "!**/.github/**".to_string()]),
        )
        .unwrap();

        assert!(matcher.is_dir_included(".github"));
        assert!(matcher.is_dir_included(".github/workflows"));
        assert!(!matcher.is_dir_included(".git"));
    }

    #[test]
    fn test_recursive_include_patterns() {
        let matcher = PatternMatcher::new(Some(vec!["**/*.py".to_string()]), None).unwrap();
        assert!(matcher.is_file_included("main.py"));
        assert!(matcher.is_file_included("src/main.py"));
        assert!(matcher.is_file_included("a/b/c/main.py"));
        assert!(!matcher.is_file_included("main.rs"));
    }
}
