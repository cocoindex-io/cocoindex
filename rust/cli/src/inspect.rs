//! Read a state db without user code (`ls --db`, `show`), and render what
//! `show` prints in the Python CLI's formats.
//!
//! No provider registry is available here, so target-state segments that were
//! never persisted with a readable name render as `#hex` fingerprints.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;

use cocoindex_core::inspect::db_inspect::{
    StablePathDetail, StablePathNodeType, TargetStateEntry, iter_stable_paths_from_store,
    query_stable_path_details_from_store, spawn_stable_path_detail_iter, spawn_target_state_iter,
};
use cocoindex_core::state::stable_path::{StableKey, StablePath};
use cocoindex_core::state_store::{AppStore, Storage, StorageSettings};
use futures::{Stream, StreamExt};

use crate::error::{CliError, CliResult, fail};

use cocoindex::app::{
    DEFAULT_LMDB_MAP_SIZE, DEFAULT_LMDB_MAX_DBS, LMDB_MAP_SIZE_ENV, LMDB_MAX_DBS_ENV,
};

fn env_setting<T: std::str::FromStr>(name: &str, default: T) -> CliResult<T> {
    match std::env::var(name) {
        Ok(raw) => raw
            .trim()
            .parse()
            .map_err(|_| CliError::usage(format!("{name} must be an integer, got {raw:?}"))),
        Err(_) => Ok(default),
    }
}

/// Open an existing state db. Checks existence first: opening creates
/// missing directories.
pub async fn open_db(path: &Path) -> CliResult<Storage> {
    if !path.exists() {
        return Err(CliError::failure(format!(
            "Database path does not exist: {}",
            path.display()
        )));
    }
    let settings = StorageSettings {
        db_path: path.to_path_buf(),
        lmdb_max_dbs: env_setting(LMDB_MAX_DBS_ENV, DEFAULT_LMDB_MAX_DBS)?,
        lmdb_map_size: env_setting(LMDB_MAP_SIZE_ENV, DEFAULT_LMDB_MAP_SIZE)?,
    };
    Storage::new(&settings)
        .await
        .map_err(fail("Failed to open database"))
}

pub async fn list_apps(storage: &Storage) -> CliResult<Vec<String>> {
    let mut names = storage
        .list_app_names()
        .await
        .map_err(fail("Failed to list apps"))?;
    names.sort();
    Ok(names)
}

/// Show display options.
#[derive(Debug, Clone, Copy, Default)]
pub struct ShowMode {
    pub tree: bool,
    pub long: bool,
    pub recursive: bool,
    pub parents: bool,
    pub fingerprints: bool,
    pub target_states: bool,
}

/// Run `show` against one app in an open db; returns the text to print.
pub async fn show(
    storage: &Storage,
    app_name: &str,
    stable_path: Option<&str>,
    mode: ShowMode,
) -> CliResult<String> {
    let store = storage
        .open_app_store_by_name(app_name)
        .await
        .map_err(fail("Failed to open app"))?;
    let no_keys = HashMap::new;
    if mode.target_states {
        let entries = match store {
            Some(store) => collect(spawn_target_state_iter(store, no_keys()).await).await?,
            None => Vec::new(),
        };
        return Ok(render_target_states(&entries, mode.tree, mode.fingerprints));
    }
    if let Some(path) = stable_path {
        let details = match &store {
            Some(store) => query_stable_path_details_from_store(
                store,
                no_keys(),
                &parse_stable_path(path),
                mode.recursive,
                mode.recursive,
                mode.parents,
            )
            .await
            .map_err(fail("Failed to read the stable path"))?,
            None => Vec::new(),
        };
        return Ok(render_details(&details, mode.fingerprints));
    }
    if mode.long {
        let details = match store {
            Some(store) => collect(spawn_stable_path_detail_iter(store, no_keys()).await).await?,
            None => Vec::new(),
        };
        return Ok(render_details(&details, mode.fingerprints));
    }
    let paths = match store {
        Some(store) => stable_paths(store).await?,
        None => Vec::new(),
    };
    Ok(if mode.tree {
        render_tree(&paths)
    } else {
        render_paths(&paths)
    })
}

async fn stable_paths(store: AppStore) -> CliResult<Vec<(StablePath, StablePathNodeType)>> {
    let items = collect(iter_stable_paths_from_store(store).await).await?;
    Ok(items.into_iter().map(|i| (i.path, i.node_type)).collect())
}

async fn collect<T, E: std::fmt::Display>(
    stream: impl Stream<Item = Result<T, E>>,
) -> CliResult<Vec<T>> {
    let mut stream = std::pin::pin!(stream);
    let mut items = Vec::new();
    while let Some(item) = stream.next().await {
        items.push(item.map_err(fail("Failed to read the database"))?);
    }
    Ok(items)
}

/// `/"files"/"a.txt"` or `/files/a.txt` → a stable path of string keys.
///
/// A quoted segment is read up to its closing quote, so it may contain `/`
/// (file keys such as `"docs/a.md"`) and the escapes `show` prints (`\"`,
/// `\\`, `\n`, `\t`, `\r`, `\'`, `\u{..}`).
pub fn parse_stable_path(raw: &str) -> StablePath {
    let mut keys: Vec<StableKey> = Vec::new();
    let mut chars = raw.trim_start_matches('/').chars().peekable();
    while chars.peek().is_some() {
        let mut key = String::new();
        if chars.peek() == Some(&'"') {
            chars.next();
            let mut closed = false;
            while let Some(c) = chars.next() {
                match c {
                    '"' => {
                        closed = true;
                        break;
                    }
                    '\\' => unescape_into(&mut chars, &mut key),
                    c => key.push(c),
                }
            }
            if !closed {
                // An unterminated quote is taken literally.
                key.insert(0, '"');
            }
        }
        // The rest of the segment, up to the next `/`.
        for c in chars.by_ref() {
            if c == '/' {
                break;
            }
            key.push(c);
        }
        keys.push(StableKey::Str(Arc::from(key)));
        while chars.peek() == Some(&'/') {
            chars.next();
        }
    }
    StablePath(keys.into())
}

/// Undo one `str::escape_default` escape (the backslash is already read).
fn unescape_into(chars: &mut std::iter::Peekable<std::str::Chars<'_>>, out: &mut String) {
    match chars.next() {
        Some('n') => out.push('\n'),
        Some('t') => out.push('\t'),
        Some('r') => out.push('\r'),
        Some('u') if chars.peek() == Some(&'{') => {
            chars.next();
            let hex: String = chars.by_ref().take_while(|&c| c != '}').collect();
            match u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32) {
                Some(c) => out.push(c),
                None => {
                    out.push_str("\\u{");
                    out.push_str(&hex);
                    out.push('}');
                }
            }
        }
        Some(c) => out.push(c),
        None => out.push('\\'),
    }
}

/// A key as a tree label: strings without quotes, like Python's `str()`.
fn key_label(key: &StableKey) -> String {
    match key {
        StableKey::Str(s) => s.to_string(),
        other => other.to_string(),
    }
}

fn is_component(node_type: &StablePathNodeType) -> bool {
    matches!(node_type, StablePathNodeType::Component)
}

pub fn render_paths(paths: &[(StablePath, StablePathNodeType)]) -> String {
    let mut out = String::from("Stable paths:\n");
    for (path, _) in paths {
        let _ = writeln!(out, "  {path}");
    }
    out
}

pub fn render_tree(paths: &[(StablePath, StablePathNodeType)]) -> String {
    let mut out = String::from("Stable paths:\n");
    for (path, node_type) in paths {
        let mut line = match path.last() {
            None => "- /".to_string(),
            Some(last) => format!("{}- {}", "  ".repeat(path.len() - 1), key_label(last)),
        };
        if is_component(node_type) {
            line.push_str(" [component]");
        }
        let _ = writeln!(out, "{line}");
    }
    if paths.is_empty() {
        out.push_str("(none)\n");
    }
    out
}

pub fn render_details(details: &[StablePathDetail], fingerprints: bool) -> String {
    let mut out = String::from("Stable paths:\n");
    if details.is_empty() {
        out.push_str("  (none)\n");
    }
    for detail in details {
        let node_type = if is_component(&detail.node_type) {
            "component"
        } else {
            "directory"
        };
        let processor = if detail.processor_name.is_empty() {
            "-"
        } else {
            &detail.processor_name
        };
        let _ = writeln!(out, "  {}", detail.path);
        let _ = writeln!(
            out,
            "    type:{node_type} version:{} processor:{processor}",
            detail.version
        );
        let _ = writeln!(
            out,
            "    has_memoization:{} target_state_count:{}",
            detail.has_memoization, detail.target_state_count
        );
        if !detail.target_state_items.is_empty() {
            out.push_str("    Target states:\n");
            for item in &detail.target_state_items {
                let generation = item
                    .provider_generation
                    .as_ref()
                    .map(|g| format!("{}.{}", g.provider_id, g.provider_schema_version))
                    .unwrap_or_else(|| "None".to_string());
                let states: Vec<String> = item
                    .states
                    .iter()
                    .map(|s| format!("{}:{}", s.version, s.state))
                    .collect();
                let states = if states.is_empty() {
                    "-".to_string()
                } else {
                    states.join(", ")
                };
                let path = if fingerprints {
                    &item.fingerprint_path
                } else {
                    &item.target_state_path
                };
                let _ = writeln!(out, "      - path:{path}");
                let _ = writeln!(
                    out,
                    "        states:{states} schema_version:{} generation:{generation}",
                    item.provider_schema_version
                );
            }
        }
        out.push('\n');
    }
    out
}

pub fn render_target_states(
    entries: &[TargetStateEntry],
    tree: bool,
    fingerprints: bool,
) -> String {
    let mut out = String::from("Target states:\n");
    if tree {
        // Stored order lists a parent before its descendants, so comparing with
        // the previous entry's fingerprint segments places each line.
        let mut prev: Vec<&str> = Vec::new();
        for entry in entries {
            let segments: Vec<&str> = entry
                .fingerprint_path
                .trim_start_matches('/')
                .split('/')
                .collect();
            let labels: Vec<&str> = if fingerprints {
                segments.clone()
            } else {
                entry.readable_segments.iter().map(String::as_str).collect()
            };
            let last = segments.len() - 1;
            let mut common = 0;
            while common < prev.len() && common < last && prev[common] == segments[common] {
                common += 1;
            }
            for (depth, segment) in segments.iter().enumerate().take(last).skip(common) {
                let label = labels.get(depth).copied().unwrap_or(segment);
                let _ = writeln!(out, "{}- {label}", "  ".repeat(depth));
            }
            let marker = if entry.dangling { " [dangling]" } else { "" };
            let label = labels.get(last).copied().unwrap_or(segments[last]);
            let _ = writeln!(
                out,
                "{}- {label}{marker} owner:{}",
                "  ".repeat(last),
                entry.owner_component_path
            );
            prev = segments;
        }
    } else {
        for entry in entries {
            let path = if fingerprints {
                &entry.fingerprint_path
            } else {
                &entry.readable_path
            };
            let marker = if entry.dangling { " [dangling]" } else { "" };
            let _ = writeln!(out, "  {path}{marker}");
            let _ = writeln!(out, "    owner:{}", entry.owner_component_path);
        }
    }
    if entries.is_empty() {
        out.push_str("  (none)\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use cocoindex_core::inspect::db_inspect::{
        ProviderGeneration, TargetStateInfoItemSummary, TargetStateVersion,
    };

    fn path(parts: &[&str]) -> StablePath {
        StablePath(
            parts
                .iter()
                .map(|p| StableKey::Str(Arc::from(*p)))
                .collect::<Vec<_>>()
                .into(),
        )
    }

    #[test]
    fn parses_stable_paths() {
        assert_eq!(parse_stable_path("/").to_string(), "/");
        assert_eq!(parse_stable_path("").to_string(), "/");
        assert_eq!(
            parse_stable_path("/files/a.txt").to_string(),
            r#"/"files"/"a.txt""#
        );
        assert_eq!(
            parse_stable_path(r#"/"files"/"a.txt"/"#).to_string(),
            r#"/"files"/"a.txt""#
        );
        // A quoted key may hold `/` and the escapes `show` prints.
        let nested = parse_stable_path(r#"/"files"/"docs/a \"b\".md""#);
        assert_eq!(nested.len(), 2);
        assert_eq!(nested.to_string(), r#"/"files"/"docs/a \"b\".md""#);
    }

    #[test]
    fn renders_flat_and_tree_paths() {
        let paths = vec![
            (path(&[]), StablePathNodeType::Component),
            (path(&["files"]), StablePathNodeType::Directory),
            (path(&["files", "a.txt"]), StablePathNodeType::Component),
        ];
        assert_eq!(
            render_paths(&paths),
            "Stable paths:\n  /\n  /\"files\"\n  /\"files\"/\"a.txt\"\n"
        );
        assert_eq!(
            render_tree(&paths),
            "Stable paths:\n- / [component]\n- files\n  - a.txt [component]\n"
        );
        assert_eq!(render_tree(&[]), "Stable paths:\n(none)\n");
    }

    #[test]
    fn renders_details() {
        let detail = StablePathDetail {
            path: path(&["files", "a.txt"]),
            node_type: StablePathNodeType::Component,
            version: 3,
            processor_name: String::new(),
            target_state_count: 1,
            has_memoization: true,
            target_state_items: vec![TargetStateInfoItemSummary {
                target_state_path: "/@dir/\"A.TXT\"".into(),
                fingerprint_path: "/#ab/#cd".into(),
                key: StableKey::Str(Arc::from("A.TXT")),
                states: vec![TargetStateVersion {
                    version: 3,
                    state: "Existing".into(),
                }],
                provider_schema_version: 0,
                provider_generation: Some(ProviderGeneration {
                    provider_id: 1,
                    provider_schema_version: 0,
                }),
            }],
        };
        let text = render_details(std::slice::from_ref(&detail), false);
        assert_eq!(
            text,
            "Stable paths:\n  /\"files\"/\"a.txt\"\n    type:component version:3 processor:-\n    \
             has_memoization:true target_state_count:1\n    Target states:\n      - path:/@dir/\"A.TXT\"\n        \
             states:3:Existing schema_version:0 generation:1.0\n\n"
        );
        assert!(render_details(&[detail], true).contains("- path:/#ab/#cd"));
        assert_eq!(render_details(&[], false), "Stable paths:\n  (none)\n");
    }

    #[test]
    fn renders_target_states() {
        let entries = vec![
            TargetStateEntry {
                fingerprint_path: "/#aa/#bb".into(),
                readable_path: "/@dir/\"a\"".into(),
                readable_segments: vec!["@dir".into(), "\"a\"".into()],
                owner_component_path: path(&["files", "a"]),
                dangling: false,
            },
            TargetStateEntry {
                fingerprint_path: "/#aa/#cc".into(),
                readable_path: "/@dir/\"b\"".into(),
                readable_segments: vec!["@dir".into(), "\"b\"".into()],
                owner_component_path: path(&[]),
                dangling: true,
            },
        ];
        assert_eq!(
            render_target_states(&entries, false, false),
            "Target states:\n  /@dir/\"a\"\n    owner:/\"files\"/\"a\"\n  /@dir/\"b\" [dangling]\n    owner:/\n"
        );
        assert_eq!(
            render_target_states(&entries, true, false),
            "Target states:\n- @dir\n  - \"a\" owner:/\"files\"/\"a\"\n  - \"b\" [dangling] owner:/\n"
        );
        assert!(render_target_states(&entries, true, true).contains("- #aa\n  - #bb"));
        assert_eq!(
            render_target_states(&[], false, false),
            "Target states:\n  (none)\n"
        );
    }
}
