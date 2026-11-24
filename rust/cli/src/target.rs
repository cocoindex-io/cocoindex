//! The TARGET argument: `[path][:app_name]`.
//!
//! `path` defaults to `.`. A directory must hold a `Cargo.toml`; a
//! `Cargo.toml` is used as the manifest; any other existing file is an
//! already-built executable, run without building.

use std::path::{Path, PathBuf};

use crate::error::{CliError, CliResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetPath {
    /// Build this manifest, then run its binary.
    Manifest(PathBuf),
    /// Run this binary as is.
    Executable(PathBuf),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub path: TargetPath,
    pub app: Option<String>,
}

/// Split `[path][:app_name]`. A Windows drive letter (`C:\`, `C:/`) is part of
/// the path, not an app separator.
pub fn split_target(raw: &str) -> CliResult<(String, Option<String>)> {
    let bytes = raw.as_bytes();
    let has_drive = bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && (bytes[2] == b'\\' || bytes[2] == b'/');
    let search_from = if has_drive { 2 } else { 0 };
    match raw[search_from..].find(':') {
        None => Ok((raw.to_string(), None)),
        Some(offset) => {
            let at = search_from + offset;
            let (path, app) = (&raw[..at], &raw[at + 1..]);
            if app.is_empty() {
                return Err(CliError::usage(format!(
                    "TARGET '{raw}': the app name after ':' is empty"
                )));
            }
            Ok((path.to_string(), Some(app.to_string())))
        }
    }
}

/// Parse and classify a TARGET (`None` means `.`).
pub fn parse_target(raw: Option<&str>) -> CliResult<Target> {
    let (path, app) = split_target(raw.unwrap_or("."))?;
    let path = if path.is_empty() {
        ".".to_string()
    } else {
        path
    };
    Ok(Target {
        path: classify(Path::new(&path))?,
        app,
    })
}

fn classify(path: &Path) -> CliResult<TargetPath> {
    if path.is_dir() {
        let manifest = path.join("Cargo.toml");
        if manifest.is_file() {
            return Ok(TargetPath::Manifest(manifest));
        }
        return Err(CliError::usage(format!(
            "no Cargo.toml in '{}'; TARGET is a crate directory, a Cargo.toml, or a built binary",
            path.display()
        )));
    }
    if path.is_file() {
        if path.file_name().is_some_and(|name| name == "Cargo.toml") {
            return Ok(TargetPath::Manifest(path.to_path_buf()));
        }
        return Ok(TargetPath::Executable(path.to_path_buf()));
    }
    Err(CliError::usage(format!(
        "TARGET '{}' does not exist",
        path.display()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_path_and_app() {
        assert_eq!(split_target(".").unwrap(), (".".into(), None));
        assert_eq!(
            split_target("crate:MyApp").unwrap(),
            ("crate".into(), Some("MyApp".into()))
        );
        assert_eq!(
            split_target(":MyApp").unwrap(),
            ("".into(), Some("MyApp".into()))
        );
        assert_eq!(
            split_target(r"C:\work\crate:App").unwrap(),
            (r"C:\work\crate".into(), Some("App".into()))
        );
        assert_eq!(split_target("C:/work").unwrap(), ("C:/work".into(), None));
        assert!(split_target("crate:").is_err());
    }

    #[test]
    fn classifies_paths() {
        let dir = tempfile::tempdir().unwrap();
        let err = parse_target(Some(dir.path().to_str().unwrap())).unwrap_err();
        assert_eq!(err.code, 2);
        assert!(err.message.contains("no Cargo.toml"));

        std::fs::write(dir.path().join("Cargo.toml"), "").unwrap();
        let target = parse_target(Some(&format!("{}:App", dir.path().display()))).unwrap();
        assert_eq!(
            target.path,
            TargetPath::Manifest(dir.path().join("Cargo.toml"))
        );
        assert_eq!(target.app.as_deref(), Some("App"));

        let manifest = dir.path().join("Cargo.toml");
        let target = parse_target(Some(manifest.to_str().unwrap())).unwrap();
        assert_eq!(target.path, TargetPath::Manifest(manifest));

        let exe = dir.path().join("app-bin");
        std::fs::write(&exe, "").unwrap();
        let target = parse_target(Some(exe.to_str().unwrap())).unwrap();
        assert_eq!(target.path, TargetPath::Executable(exe));

        let missing = dir.path().join("missing");
        let err = parse_target(Some(missing.to_str().unwrap())).unwrap_err();
        assert!(err.message.contains("does not exist"));
    }
}
