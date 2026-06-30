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

/// Expands brace alternations (``{a,b}``) in a glob pattern into the full set of
/// concrete alternatives, e.g. ``a/{b/c,d}/e`` → ``["a/b/c/e", "a/d/e"]``.
///
/// Multiple groups expand combinatorially (``a/{b,c}/{d,e}`` → four forms) and
/// nested groups are handled recursively (``a/{b,{c,d}}/e`` → three forms).
/// Braces and commas inside a character class (``[...]``) are treated as
/// literals, and wildcards (``*``, ``**``, ``?``) are preserved untouched.
/// A pattern with no expandable braces — including a malformed, unbalanced one —
/// returns a single-element vector holding the original string, so that any
/// genuine syntax error still surfaces when the glob is later compiled.
///
/// This is the *only* piece of glob grammar the matcher reimplements: once the
/// alternations are flattened, every remaining matching concern (wildcards,
/// character classes, ``**`` spans) is delegated back to ``globset``.
fn brace_expand(pattern: &str) -> Vec<String> {
    let bytes = pattern.as_bytes();
    let mut in_class = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'[' => in_class = true,
            b']' => in_class = false,
            b'{' if !in_class => {
                // First top-level group found. Scan to its matching close,
                // collecting the depth-1 comma-separated alternatives.
                let open = i;
                let mut depth = 1;
                let mut inner_class = false;
                let mut seg_start = open + 1;
                let mut alternatives: Vec<&str> = Vec::new();
                let mut j = open + 1;
                while j < bytes.len() && depth > 0 {
                    match bytes[j] {
                        b'[' => inner_class = true,
                        b']' => inner_class = false,
                        b'{' if !inner_class => depth += 1,
                        b'}' if !inner_class => {
                            depth -= 1;
                            if depth == 0 {
                                alternatives.push(&pattern[seg_start..j]);
                            }
                        }
                        b',' if !inner_class && depth == 1 => {
                            alternatives.push(&pattern[seg_start..j]);
                            seg_start = j + 1;
                        }
                        _ => {}
                    }
                    j += 1;
                }
                // Unbalanced braces: leave the pattern untouched so globset can
                // report the error rather than us silently swallowing the brace.
                if depth != 0 {
                    return vec![pattern.to_string()];
                }
                let prefix = &pattern[..open];
                let suffix = &pattern[j..]; // j is one past the closing '}'
                let mut out = Vec::new();
                for alt in alternatives {
                    // Recurse on the recombined string: this expands any nested
                    // group inside `alt` and any further group in `suffix`.
                    let combined = format!("{prefix}{alt}{suffix}");
                    out.extend(brace_expand(&combined));
                }
                return out;
            }
            _ => {}
        }
        i += 1;
    }
    vec![pattern.to_string()]
}

/// Splits a list of patterns into regular exclusion patterns and negation (``!``-prefixed)
/// patterns.  Returns the compiled GlobSets together with the raw negation strings so that
/// callers can do prefix-based path ancestry checks without unpacking compiled globs.
fn split_excluded_patterns(
    patterns: Option<Vec<String>>,
) -> Result<(Option<GlobSet>, Option<GlobSet>, Vec<String>)> {
    let Some(pats) = patterns else {
        return Ok((None, None, Vec::new()));
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
    // Brace-expand the raw negation strings into their concrete alternation
    // forms (e.g. `a/{b/c,d}/e` → [`a/b/c/e`, `a/d/e`]).  `is_dir_included`
    // does prefix-based ancestry checks on these raw strings; without expansion
    // an intermediate directory like `a/b` would fail `starts_with("a/{b/c,d}…")`
    // and be pruned, never reaching the negation-exempt file.  The compiled
    // `negation_set` is left built from the original patterns because `globset`
    // already expands braces when matching, so file-level negation is unaffected.
    let negation_raw: Vec<String> = negation.iter().flat_map(|p| brace_expand(p)).collect();
    let negation_set = if negation.is_empty() {
        None
    } else {
        Some(build_glob_set(negation)?)
    };

    Ok((regular_set, negation_set, negation_raw))
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
    /// Negation patterns compiled into a GlobSet (``!``-prefixed in the original list,
    /// stored without the ``!``).  A path that matches one of these is *not* excluded
    /// even if it matches the regular exclusion patterns.
    negation_excluded_glob_set: Option<GlobSet>,
    /// Raw (uncompiled) negation pattern strings, **brace-expanded** into their concrete
    /// alternation forms, kept so that ``is_dir_included`` can detect directories that lie
    /// on the path to a negation-exempt file even when the directory itself would otherwise
    /// be pruned (e.g. ``!dir1/dir2/dir3/a.yml`` combined with ``dir1/**``).  Brace expansion
    /// means ``!a/{b/c,d}/e`` is stored as ``["a/b/c/e", "a/d/e"]`` so the prefix-ancestry
    /// check below sees each branch (``a/b``, ``a/d`` …) rather than the literal ``a/{b/c,d}…``.
    negation_patterns_raw: Vec<String>,
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
        let (excluded_glob_set, negation_excluded_glob_set, negation_patterns_raw) =
            split_excluded_patterns(excluded_patterns)?;

        Ok(Self {
            included_glob_set,
            excluded_glob_set,
            negation_excluded_glob_set,
            negation_patterns_raw,
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
            .negation_excluded_glob_set
            .as_ref()
            .is_some_and(|gs| gs.is_match(path))
    }

    /// Check if a directory should be traversed based on the exclude/negation patterns.
    ///
    /// A directory is included unless it matches an exclusion pattern *and* no negation
    /// pattern applies to it or to any file that could live inside it.
    ///
    /// Two complementary checks are used so that both glob-style and exact-path negations
    /// work correctly:
    ///
    /// 1. **GlobSet probe** — matches ``<dir>/__probe__`` against the compiled negation
    ///    GlobSet.  Catches wildcard negations such as ``!**/.github/**`` that use ``**``
    ///    to span multiple directory levels.
    ///
    /// 2. **Raw-prefix check** — scans the brace-expanded raw negation strings and returns
    ///    ``true`` if any of them starts with ``<dir>/``.  Catches exact-path negations
    ///    such as ``!dir1/dir2/dir3/a.yml`` where the probe alone would not help because
    ///    the pattern contains no wildcards relative to the directory.  Brace expansion
    ///    happens up front (see ``negation_patterns_raw``) so that comma-alternations such
    ///    as ``!a/{b/c,d}/e`` contribute every branch to this check.
    pub fn is_dir_included(&self, path: &str) -> bool {
        if !self
            .excluded_glob_set
            .as_ref()
            .is_some_and(|gs| gs.is_match(path))
        {
            return true;
        }
        // Directory matches an exclusion pattern.  Check whether a negation pattern
        // could apply to the directory itself or to any descendant.
        if let Some(neg_gs) = &self.negation_excluded_glob_set {
            if neg_gs.is_match(path) {
                return true;
            }
            // Probe one level inside: catches glob negations like `**/.github/**`.
            let probe = format!("{}/__probe__", path);
            if neg_gs.is_match(probe.as_str()) {
                return true;
            }
        }
        // Raw-prefix check: catches exact-path negations like `!dir1/dir2/dir3/a.yml`
        // where the parent directories would otherwise be pruned by the exclusion but
        // need to be traversed to reach the negation-exempt file.
        let dir_prefix = format!("{}/", path);
        if self
            .negation_patterns_raw
            .iter()
            .any(|p| p.starts_with(&dir_prefix))
        {
            return true;
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
    fn test_negation_exact_path_deep_dir_traversal() {
        // Exclude all of dir1, but un-exclude a specific deep file with an exact path.
        // All ancestor directories of dir1/dir2/dir3/a.yml must be traversable.
        let matcher = PatternMatcher::new(
            None,
            Some(vec![
                "dir1/**".to_string(),
                "!dir1/dir2/dir3/a.yml".to_string(),
            ]),
        )
        .unwrap();
        // The negation-exempt file itself must be included.
        assert!(matcher.is_file_included("dir1/dir2/dir3/a.yml"));
        // Other files under dir1 remain excluded.
        assert!(!matcher.is_file_included("dir1/other.txt"));
        assert!(!matcher.is_file_included("dir1/dir2/other.txt"));
        // Ancestor directories must be traversable to reach the exempt file.
        assert!(matcher.is_dir_included("dir1"));
        assert!(matcher.is_dir_included("dir1/dir2"));
        assert!(matcher.is_dir_included("dir1/dir2/dir3"));
        // Sibling directories that contain no negation-exempt files are still pruned.
        assert!(!matcher.is_dir_included("dir1/other"));
    }

    #[test]
    fn test_negation_only_no_regular_exclusion() {
        // A negation without a corresponding exclusion is a no-op — nothing is excluded.
        let matcher = PatternMatcher::new(None, Some(vec!["!**/.github/**".to_string()])).unwrap();
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

    // --- Brace-expansion tests ---

    #[test]
    fn test_brace_expand_helper() {
        // No braces: identity.
        assert_eq!(brace_expand("a/b/c"), vec!["a/b/c".to_string()]);
        // Single group spanning a separator.
        assert_eq!(
            brace_expand("a/{b/c,d}/e"),
            vec!["a/b/c/e".to_string(), "a/d/e".to_string()]
        );
        // Multiple groups expand combinatorially.
        assert_eq!(
            brace_expand("a/{b,c}/{d,e}"),
            vec![
                "a/b/d".to_string(),
                "a/b/e".to_string(),
                "a/c/d".to_string(),
                "a/c/e".to_string(),
            ]
        );
        // Nested groups.
        assert_eq!(
            brace_expand("a/{b,{c,d}}/e"),
            vec![
                "a/b/e".to_string(),
                "a/c/e".to_string(),
                "a/d/e".to_string()
            ]
        );
        // Wildcards inside an alternative are preserved untouched.
        assert_eq!(
            brace_expand("a/{b*,d}/e"),
            vec!["a/b*/e".to_string(), "a/d/e".to_string()]
        );
        // A comma inside a character class is a literal, not an alternation.
        assert_eq!(brace_expand("a/[b,c]/d"), vec!["a/[b,c]/d".to_string()]);
        // Unbalanced braces are left untouched (globset will report the error).
        assert_eq!(brace_expand("a/{b,c"), vec!["a/{b,c".to_string()]);
    }

    #[test]
    fn test_negation_brace_expansion_dir_traversal() {
        // George's case: exclude all of `a`, but un-exclude two specific deep
        // files expressed via a single brace-alternation negation. Every
        // ancestor directory of `a/b/c/e` and `a/d/e` must stay traversable.
        let matcher = PatternMatcher::new(
            None,
            Some(vec!["a/**".to_string(), "!a/{b/c,d}/e".to_string()]),
        )
        .unwrap();
        // Both negation-exempt files are included.
        assert!(matcher.is_file_included("a/b/c/e"));
        assert!(matcher.is_file_included("a/d/e"));
        // Other files under `a` remain excluded.
        assert!(!matcher.is_file_included("a/b/c/x"));
        assert!(!matcher.is_file_included("a/d/x"));
        assert!(!matcher.is_file_included("a/other.txt"));
        // Ancestors on both branches must be traversable.
        assert!(matcher.is_dir_included("a"));
        assert!(matcher.is_dir_included("a/b"));
        assert!(matcher.is_dir_included("a/b/c"));
        assert!(matcher.is_dir_included("a/d"));
        // Directories that lie on neither branch are still pruned.
        assert!(!matcher.is_dir_included("a/x"));
        assert!(!matcher.is_dir_included("a/b/x"));
    }

    #[test]
    fn test_negation_brace_expansion_multiple_groups() {
        // Two brace groups -> four exempt files, all ancestors traversable.
        let matcher = PatternMatcher::new(
            None,
            Some(vec![
                "build/**".to_string(),
                "!build/{lib,bin}/{keep,save}.txt".to_string(),
            ]),
        )
        .unwrap();
        assert!(matcher.is_file_included("build/lib/keep.txt"));
        assert!(matcher.is_file_included("build/bin/save.txt"));
        assert!(!matcher.is_file_included("build/lib/drop.txt"));
        assert!(matcher.is_dir_included("build"));
        assert!(matcher.is_dir_included("build/lib"));
        assert!(matcher.is_dir_included("build/bin"));
        assert!(!matcher.is_dir_included("build/etc"));
    }
}
