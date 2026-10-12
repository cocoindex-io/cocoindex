//! Build the target crate with cargo and find the binary to run.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use serde::Deserialize;

use crate::error::{CliError, CliResult, fail};

#[derive(Debug, Clone, Default)]
pub struct BuildOpts {
    pub release: bool,
    pub bin: Option<String>,
    pub quiet: bool,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "reason", rename_all = "kebab-case")]
enum CargoMessage {
    CompilerArtifact(Artifact),
    BuildFinished {
        success: bool,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Deserialize)]
struct Artifact {
    manifest_path: PathBuf,
    target: ArtifactTarget,
    executable: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
struct ArtifactTarget {
    name: String,
    kind: Vec<String>,
}

/// The cargo to run: the one driving us (`CARGO`, set by `cargo run` /
/// `cargo test`), else `cargo` on `PATH`.
fn cargo_program() -> std::ffi::OsString {
    std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into())
}

/// Whether `name` is a variable cargo sets for the crate it is running
/// (`cargo run` / `cargo test` of this tool). Passing those on makes the nested
/// build look different from a plain one (e.g. `ring`'s build script tracks
/// `CARGO_MANIFEST_DIR`), so shared dependencies rebuild back and forth.
fn is_outer_crate_env(name: &str) -> bool {
    matches!(
        name,
        "CARGO_MANIFEST_DIR"
            | "CARGO_MANIFEST_PATH"
            | "CARGO_CRATE_NAME"
            | "CARGO_BIN_NAME"
            | "CARGO_PRIMARY_PACKAGE"
            | "CARGO_TARGET_TMPDIR"
            | "CARGO_RUSTC_CURRENT_DIR"
            | "OUT_DIR"
    ) || name.starts_with("CARGO_PKG_")
        || name.starts_with("CARGO_BIN_EXE_")
}

/// `cargo build` the manifest and return the path of its binary. Cargo's own
/// output (progress, rendered diagnostics) goes to stderr as usual.
pub async fn build(manifest: &Path, opts: &BuildOpts) -> CliResult<PathBuf> {
    let manifest = &std::path::absolute(manifest).map_err(fail("resolving the manifest path"))?;
    let mut cmd = tokio::process::Command::new(cargo_program());
    cmd.arg("build")
        .arg("--message-format=json-render-diagnostics")
        .arg("--manifest-path")
        .arg(manifest);
    if opts.release {
        cmd.arg("--release");
    }
    if let Some(bin) = &opts.bin {
        cmd.args(["--bin", bin]);
    }
    if opts.quiet {
        cmd.arg("--quiet");
    }
    // Run cargo from the crate's directory so its `.cargo/config.toml` (and
    // toolchain file) apply, exactly as for `cargo run` inside the crate.
    if let Some(dir) = manifest.parent().filter(|d| !d.as_os_str().is_empty()) {
        cmd.current_dir(dir);
    }
    for (name, _) in std::env::vars_os() {
        if name.to_str().is_some_and(is_outer_crate_env) {
            cmd.env_remove(name);
        }
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let output = cmd.output().await.map_err(fail("could not run cargo"))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let (artifacts, finished_ok) = parse_messages(&stdout);
    if !output.status.success() || finished_ok == Some(false) {
        return Err(CliError::failure(format!(
            "building {} failed",
            manifest.display()
        )));
    }
    pick_executable(&artifacts, manifest, opts.bin.as_deref())
}

fn parse_messages(stdout: &str) -> (Vec<Artifact>, Option<bool>) {
    let mut artifacts = Vec::new();
    let mut finished = None;
    for line in stdout.lines() {
        match serde_json::from_str::<CargoMessage>(line) {
            Ok(CargoMessage::CompilerArtifact(artifact)) => artifacts.push(artifact),
            Ok(CargoMessage::BuildFinished { success }) => finished = Some(success),
            Ok(CargoMessage::Other) | Err(_) => {}
        }
    }
    (artifacts, finished)
}

/// The built binary: the one bin of the manifest's package (or of the whole
/// build when the manifest is a virtual workspace); `--bin` picks among many.
fn pick_executable(
    artifacts: &[Artifact],
    manifest: &Path,
    bin: Option<&str>,
) -> CliResult<PathBuf> {
    let bins: Vec<&Artifact> = artifacts
        .iter()
        .filter(|a| a.executable.is_some() && a.target.kind.iter().any(|k| k == "bin"))
        .filter(|a| bin.is_none_or(|b| a.target.name == b))
        .collect();
    let manifest = manifest
        .canonicalize()
        .unwrap_or_else(|_| manifest.to_path_buf());
    let own: Vec<&Artifact> = bins
        .iter()
        .copied()
        .filter(|a| {
            a.manifest_path
                .canonicalize()
                .unwrap_or_else(|_| a.manifest_path.clone())
                == manifest
        })
        .collect();
    let candidates = if own.is_empty() { bins } else { own };
    match candidates.as_slice() {
        [one] => Ok(one.executable.clone().expect("filtered on executable")),
        [] => Err(CliError::usage(match bin {
            Some(b) => format!("{} builds no binary named `{b}`", manifest.display()),
            None => format!("{} builds no binary", manifest.display()),
        })),
        many => {
            let names: Vec<&str> = many.iter().map(|a| a.target.name.as_str()).collect();
            Err(CliError::usage(format!(
                "{} builds several binaries ({}); pass --bin NAME",
                manifest.display(),
                names.join(", ")
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact(manifest: &str, name: &str, kind: &str, exe: Option<&str>) -> String {
        serde_json::json!({
            "reason": "compiler-artifact",
            "package_id": "x",
            "manifest_path": manifest,
            "target": {"name": name, "kind": [kind], "crate_types": [kind], "src_path": "x"},
            "profile": {},
            "features": [],
            "filenames": [],
            "executable": exe,
            "fresh": true
        })
        .to_string()
    }

    #[test]
    fn parses_cargo_messages_and_picks_the_bin() {
        let stdout = [
            artifact("/dep/Cargo.toml", "dep", "lib", None),
            r#"{"reason":"build-script-executed","package_id":"x"}"#.to_string(),
            artifact("/app/Cargo.toml", "app", "bin", Some("/t/debug/app")),
            r#"{"reason":"build-finished","success":true}"#.to_string(),
            "not json".to_string(),
        ]
        .join("\n");
        let (artifacts, finished) = parse_messages(&stdout);
        assert_eq!(artifacts.len(), 2);
        assert_eq!(finished, Some(true));
        let exe = pick_executable(&artifacts, Path::new("/app/Cargo.toml"), None).unwrap();
        assert_eq!(exe, PathBuf::from("/t/debug/app"));
    }

    #[test]
    fn several_bins_need_a_name() {
        let stdout = [
            artifact("/ws/a/Cargo.toml", "a", "bin", Some("/t/a")),
            artifact("/ws/b/Cargo.toml", "b", "bin", Some("/t/b")),
        ]
        .join("\n");
        let (artifacts, _) = parse_messages(&stdout);
        let err = pick_executable(&artifacts, Path::new("/ws/Cargo.toml"), None).unwrap_err();
        assert_eq!(err.code, 2);
        assert!(
            err.message.contains("several binaries (a, b)"),
            "{}",
            err.message
        );
        let exe = pick_executable(&artifacts, Path::new("/ws/Cargo.toml"), Some("b")).unwrap();
        assert_eq!(exe, PathBuf::from("/t/b"));
        let err = pick_executable(&artifacts, Path::new("/ws/Cargo.toml"), Some("c")).unwrap_err();
        assert!(err.message.contains("no binary named `c`"));
    }

    #[test]
    fn strips_only_the_outer_crate_env() {
        for name in [
            "CARGO_MANIFEST_DIR",
            "CARGO_PKG_NAME",
            "CARGO_BIN_EXE_cocoindex",
            "OUT_DIR",
        ] {
            assert!(is_outer_crate_env(name), "{name}");
        }
        for name in [
            "CARGO",
            "CARGO_HOME",
            "CARGO_TARGET_DIR",
            "CARGO_BUILD_JOBS",
            "PATH",
        ] {
            assert!(!is_outer_crate_env(name), "{name}");
        }
    }

    #[test]
    fn failed_build_is_reported() {
        let (_, finished) = parse_messages(r#"{"reason":"build-finished","success":false}"#);
        assert_eq!(finished, Some(false));
    }
}
