use anyhow::Result;
use globset::{Glob, GlobSet, GlobSetBuilder};

/// Builds a GlobSet from a vector of pattern strings
fn build_glob_set(patterns: Vec<String>) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern.as_str())?);
    }
    Ok(builder.build()?)
}

/// Splits a list of patterns into regular exclusion patterns and negation (``!``-prefixed)
/// patterns, building a GlobSet for each group.
///
/// A pattern like ``!**/.github/**`` means "do **not** exclude paths that match
/// ``**/.github/**``", allowing fine-grained exceptions to broad exclusion rules.
fn split_excluded_patterns(
    patterns: Option<Vec<String>>,
) -> Result<(Option<GlobSet>, Option<GlobSet>)> {
    let Some(pats) = patterns else {
        return Ok((None, None));
    };

    let mut regular: Vec<String> = Vec::new();
    let mut negation: Vec<String> = Vec::new();

    for p in pats {
        if let Some(stripped) = p.strip_prefix('!') {
            negation.push(stripped.to_string());
        } else {
            regular.push(p);
        }
    }

    let regular_set = if regular.is_empty() {
        None
    } else {
        Some(build_glob_set(regular)?)
    };
    let negation_set = if negation.is_empty() {
        None
    } else {
        Some(build_glob_set(negation)?)
    };

    Ok((regular_set, negation_set))
}

/// Pattern matcher that handles include and exclude patterns for files.
///
/// Supports gitignore-style ``!``-prefixed negation in ``excluded_patterns``: a pattern
/// beginning with ``!`` un-excludes paths that would otherwise be excluded.  For example,
/// combining ``"**/.*"`` with ``"!**/.github/**"`` excludes all dot-entries *except*
/// anything inside ``.github/``.
#[derive(Debug)]
pub struct PatternMatcher {
    /// Patterns matching full path of files to be included.
    included_glob_set: Option<GlobSet>,
    /// Regular (non-negated) exclusion patterns.
    excluded_glob_set: Option<GlobSet>,
    /// Negation patterns (``!``-prefixed in the original list, stored without the ``!``).
    /// A path that matches one of these is *not* excluded even if it matches the regular
    /// exclusion patterns.
    negation_glob_set: Option<GlobSet>,
}

impl PatternMatcher {
    /// Create a new PatternMatcher from optional include and exclude pattern vectors.
    ///
    /// Patterns in `excluded_patterns` that start with ``!`` are treated as negations:
    /// they un-exclude any path that would otherwise be excluded by the preceding patterns.
    pub fn new(
        included_patterns: Option<Vec<String>>,
        excluded_patterns: Option<Vec<String>>,
    ) -> Result<Self> {
        let included_glob_set = included_patterns.map(build_glob_set).transpose()?;
        let (excluded_glob_set, negation_glob_set) = split_excluded_patterns(excluded_patterns)?;

        Ok(Self {
            included_glob_set,
            excluded_glob_set,
            negation_glob_set,
        })
    }

    /// Check if a path is excluded after applying both exclusion and negation patterns.
    pub fn is_excluded(&self, path: &str) -> bool {
        if !self
            .excluded_glob_set
            .as_ref()
            .is_some_and(|gs| gs.is_match(path))
        {
            return false;
        }
        // The path matches an exclusion pattern; a negation pattern can un-exclude it.
        !self
            .negation_glob_set
            .as_ref()
            .is_some_and(|gs| gs.is_match(path))
    }

    /// Check if a directory should be traversed based on the exclude/negation patterns.
    ///
    /// A directory is included unless it matches an exclusion pattern *and* no negation
    /// pattern applies to it or to any file that could live inside it.  The latter check
    /// uses a probe path (``<dir>/__probe__``) so that patterns like ``**/.github/**``
    /// correctly un-prune the ``.github`` directory.
    pub fn is_dir_included(&self, path: &str) -> bool {
        if !self
            .excluded_glob_set
            .as_ref()
            .is_some_and(|gs| gs.is_match(path))
        {
            return true;
        }
        // Directory matches an exclusion. Check whether negation patterns could apply to
        // the directory itself or to a file one level inside it.
        if let Some(neg_gs) = &self.negation_glob_set {
            if neg_gs.is_match(path) {
                return true;
            }
            // Probe one level inside: catches patterns like `**/.github/**`.
            let probe = format!("{}/__probe__", path);
            if neg_gs.is_match(probe.as_str()) {
                return true;
            }
        }
        false
    }

    /// Check if a file should be included based on both include and exclude patterns.
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
    fn test_recursive_include_patterns() {
        let matcher = PatternMatcher::new(Some(vec!["**/*.py".to_string()]), None).unwrap();
        assert!(matcher.is_file_included("main.py"));
        assert!(matcher.is_file_included("src/main.py"));
        assert!(matcher.is_file_included("a/b/c/main.py"));
        assert!(!matcher.is_file_included("main.rs"));
    }

    // --- Negation (!) pattern tests ---

    #[test]
    fn test_negation_un_excludes_file() {
        // Exclude all dot-files, but un-exclude .env.example
        let matcher = PatternMatcher::new(
            None,
            Some(vec!["**/.env*".to_string(), "!**/.env.example".to_string()]),
        )
        .unwrap();
        assert!(!matcher.is_file_included(".env"));
        assert!(!matcher.is_file_included("config/.env.local"));
        assert!(matcher.is_file_included(".env.example"));
        assert!(matcher.is_file_included("config/.env.example"));
    }

    #[test]
    fn test_negation_un_excludes_dotdir_files() {
        // Exclude all dot-directories, but allow .github workflow files through.
        let matcher = PatternMatcher::new(
            None,
            Some(vec!["**/.*".to_string(), "!**/.github/**".to_string()]),
        )
        .unwrap();
        // Dot files/dirs that are NOT in .github remain excluded.
        assert!(!matcher.is_file_included(".git/config"));
        assert!(!matcher.is_file_included(".vscode/settings.json"));
        // Files under .github are un-excluded.
        assert!(matcher.is_file_included(".github/workflows/ci.yml"));
        assert!(matcher.is_file_included("repo/.github/dependabot.yml"));
        // Regular files are unaffected.
        assert!(matcher.is_file_included("src/main.rs"));
    }

    #[test]
    fn test_negation_dir_traversal() {
        // Exclude all dot-directories, but un-exclude .github subtree.
        let matcher = PatternMatcher::new(
            None,
            Some(vec!["**/.*".to_string(), "!**/.github/**".to_string()]),
        )
        .unwrap();
        // .git should be pruned.
        assert!(!matcher.is_dir_included(".git"));
        assert!(!matcher.is_dir_included("src/.vscode"));
        // .github must NOT be pruned so its contents can be reached.
        assert!(matcher.is_dir_included(".github"));
        assert!(matcher.is_dir_included("repo/.github"));
        // Normal directories are always included.
        assert!(matcher.is_dir_included("src"));
    }

    #[test]
    fn test_negation_only_no_regular_exclusion() {
        // A negation without a corresponding exclusion is a no-op — nothing is excluded.
        let matcher =
            PatternMatcher::new(None, Some(vec!["!**/.github/**".to_string()])).unwrap();
        assert!(matcher.is_file_included(".git/config"));
        assert!(matcher.is_file_included(".github/workflows/ci.yml"));
        assert!(matcher.is_file_included("src/main.rs"));
    }

    #[test]
    fn test_negation_with_include_patterns() {
        // Include only YAML files, exclude all dot-directories, but un-exclude .github.
        let matcher = PatternMatcher::new(
            Some(vec!["**/*.yml".to_string(), "**/*.yaml".to_string()]),
            Some(vec!["**/.*".to_string(), "!**/.github/**".to_string()]),
        )
        .unwrap();
        assert!(matcher.is_file_included(".github/workflows/ci.yml"));
        assert!(!matcher.is_file_included(".github/workflows/ci.sh")); // not in included
        assert!(!matcher.is_file_included(".git/config")); // excluded, not negated
        assert!(matcher.is_file_included("src/config.yaml"));
    }
}
