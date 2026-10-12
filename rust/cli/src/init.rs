//! `init`: scaffold a Rust CocoIndex project.

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{CliError, CliResult, fail};

const CARGO_TOML: &str = include_str!("../templates/Cargo.toml.tmpl");
const MAIN_RS: &str = include_str!("../templates/main.rs.tmpl");
const README_MD: &str = include_str!("../templates/README.md.tmpl");
const ENV_EXAMPLE: &str = include_str!("../templates/env.example.tmpl");
const GITIGNORE: &str = include_str!("../templates/gitignore.tmpl");
const SAMPLE_INPUT: &str =
    "# Hello\n\nEdit or add markdown files here, then run `cocoindex update`.\n";

/// The SDK dependency line: a git dependency (the crate is not on crates.io
/// yet), or a path dependency with `--sdk-path`.
fn sdk_dependency(sdk_path: Option<&Path>) -> CliResult<String> {
    match sdk_path {
        None => {
            Ok("cocoindex = { git = \"https://github.com/cocoindex-io/cocoindex\" }".to_string())
        }
        Some(path) => {
            let path = path
                .canonicalize()
                .map_err(fail(&format!("--sdk-path {}", path.display())))?;
            if !path.join("Cargo.toml").is_file() {
                return Err(CliError::usage(format!(
                    "--sdk-path {} has no Cargo.toml",
                    path.display()
                )));
            }
            Ok(format!(
                "cocoindex = {{ path = {} }}",
                toml_string(&path.display().to_string())
            ))
        }
    }
}

/// A TOML basic string.
fn toml_string(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        && !name.starts_with(|c: char| c.is_ascii_digit())
}

fn confirm(prompt: &str) -> bool {
    print!("{prompt} [y/N]: ");
    let _ = std::io::stdout().flush();
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer).is_ok()
        && matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes")
}

pub fn init(
    project_name: Option<String>,
    dir: Option<PathBuf>,
    sdk_path: Option<PathBuf>,
) -> CliResult<()> {
    let (project_dir, project_name) = match (dir, project_name) {
        (Some(dir), name) => {
            let name = name.unwrap_or_else(|| dir_name(&dir));
            (dir, name)
        }
        (None, Some(name)) => (PathBuf::from(&name), name),
        (None, None) => (PathBuf::from("."), dir_name(Path::new("."))),
    };
    if !valid_name(&project_name) {
        return Err(CliError::usage(format!(
            "Invalid project name '{project_name}'. Project name must contain only alphanumeric \
             characters, hyphens, and underscores, and must not start with a digit."
        )));
    }
    let non_empty = project_dir
        .read_dir()
        .map(|mut entries| entries.next().is_some())
        .unwrap_or(false);
    if non_empty
        && !confirm(&format!(
            "Directory '{}' already exists and is not empty. Continue and overwrite existing files?",
            project_dir.display()
        ))
    {
        println!("Init cancelled.");
        return Ok(());
    }

    let sdk = sdk_dependency(sdk_path.as_deref())?;
    let render = |template: &str| {
        template
            .replace("{{name}}", &project_name)
            .replace("{{sdk_dependency}}", &sdk)
    };
    let files: [(&str, String); 6] = [
        ("Cargo.toml", render(CARGO_TOML)),
        ("src/main.rs", render(MAIN_RS)),
        ("README.md", render(README_MD)),
        (".env.example", render(ENV_EXAMPLE)),
        (".gitignore", render(GITIGNORE)),
        ("data/hello.md", SAMPLE_INPUT.to_string()),
    ];
    for (rel, content) in &files {
        let path = project_dir.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(fail("Failed to create project"))?;
        }
        std::fs::write(&path, content).map_err(fail("Failed to create project"))?;
    }

    let shown = project_dir.display().to_string();
    println!("Created CocoIndex project '{project_name}' in '{shown}'");
    println!("\nNext steps:");
    if shown != "." {
        println!("  1. cd {shown}");
        println!("  2. cocoindex update");
    } else {
        println!("  1. cocoindex update");
    }
    Ok(())
}

/// The last component of the directory's absolute path (it may not exist yet).
fn dir_name(dir: &Path) -> String {
    dir.canonicalize()
        .or_else(|_| std::path::absolute(dir))
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .unwrap_or_else(|| dir.display().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_names() {
        assert!(valid_name("my_app-2"));
        assert!(!valid_name(""));
        assert!(!valid_name("2app"));
        assert!(!valid_name("my app"));
        assert!(!valid_name("app!"));
    }

    #[test]
    fn writes_the_project() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("demo");
        let sdk = Path::new(env!("CARGO_MANIFEST_DIR")).join("../sdk/cocoindex");
        init(None, Some(dir.clone()), Some(sdk)).unwrap();
        let cargo = std::fs::read_to_string(dir.join("Cargo.toml")).unwrap();
        assert!(cargo.contains("name = \"demo\""));
        assert!(cargo.contains("cocoindex = { path = \""), "{cargo}");
        let main = std::fs::read_to_string(dir.join("src/main.rs")).unwrap();
        assert!(main.contains("#[cocoindex::app(name = \"demo\")]"));
        assert!(main.contains("#[cocoindex::lifespan]"));
        assert!(main.contains("#[cocoindex::main]"));
        assert!(!main.contains("{{"), "every placeholder is filled");
        for rel in ["README.md", ".env.example", ".gitignore", "data/hello.md"] {
            assert!(dir.join(rel).is_file(), "{rel}");
        }
        let err = init(Some("bad name".into()), Some(tmp.path().join("x")), None).unwrap_err();
        assert_eq!(err.code, 2);
    }

    #[test]
    fn git_dependency_by_default() {
        assert!(
            sdk_dependency(None)
                .unwrap()
                .contains("git = \"https://github.com/cocoindex-io/cocoindex\"")
        );
        assert_eq!(toml_string(r#"C:\a "b""#), r#""C:\\a \"b\"""#);
    }
}
