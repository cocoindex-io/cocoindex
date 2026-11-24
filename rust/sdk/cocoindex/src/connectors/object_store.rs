//! Key handling the object store sources (Amazon S3, Azure Blob, OCI) share.

use std::path::Path;

use crate::resources::file::FilePathMatcher;

/// Whether `matcher` includes the object with item key `key`, a path relative
/// to the listed folder. The key is matched as a file, and each folder above it
/// as a directory, so an excluded directory excludes every object under it, as
/// in a directory walk.
pub(crate) fn is_key_included(matcher: &dyn FilePathMatcher, key: &str) -> bool {
    matcher.is_file_included(Path::new(key))
        && key
            .match_indices('/')
            .map(|(end, _)| &key[..end])
            // An empty segment (`a//b`, `/b`) names no further folder.
            .filter(|dir| !dir.is_empty() && !dir.ends_with('/'))
            .all(|dir| matcher.is_dir_included(Path::new(dir)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::file::PatternFilePathMatcher;

    #[test]
    fn key_is_included_only_when_no_folder_above_it_is_excluded() {
        let m = PatternFilePathMatcher::new(std::iter::empty::<&str>(), ["ignored", "a/ignored"])
            .unwrap();
        // The file matches no pattern: its folder is what excludes it.
        assert!(m.is_file_included(Path::new("ignored/deep/hidden.txt")));
        assert!(!is_key_included(&m, "ignored/deep/hidden.txt"));
        assert!(!is_key_included(&m, "a/ignored/hidden.txt"));
        assert!(is_key_included(&m, "a/kept.txt"));
        assert!(is_key_included(&m, "b/ignored.txt"));
        // An empty segment names no folder.
        assert!(is_key_included(&m, "/visible.txt"));
        assert!(!is_key_included(&m, "ignored//hidden.txt"));

        // A negation keeps its folders traversable, so it keeps the key.
        let negated = PatternFilePathMatcher::new(
            std::iter::empty::<&str>(),
            ["dir1/**", "!dir1/dir2/a.yml"],
        )
        .unwrap();
        assert!(is_key_included(&negated, "dir1/dir2/a.yml"));
        assert!(!is_key_included(&negated, "dir1/dir2/b.yml"));

        // Include patterns apply to the key alone, never to its folders.
        let include_only = PatternFilePathMatcher::include(["**/*.md"]).unwrap();
        assert!(is_key_included(&include_only, "a/b/c.md"));
        assert!(!is_key_included(&include_only, "a/b/c.txt"));
    }
}
