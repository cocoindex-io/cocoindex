//! Read a state db without user code (`ls --db`, `show`), and render what
//! `show` prints in the Python CLI's formats.
//!
//! No provider registry is available here, so target-state segments that were
//! never persisted with a readable name render as `#hex` fingerprints.

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Write as _;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use cocoindex_core::inspect::db_inspect::{
    StablePathDetail, StablePathInfo, StablePathNodeType, TargetStateEntry,
    iter_stable_paths_from_store, query_stable_path_details_from_store,
    spawn_stable_path_detail_iter, spawn_target_state_iter,
};
use cocoindex_core::state::stable_path::{StableKey, StablePath};
use cocoindex_core::state_store::{Storage, StorageSettings};
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

/// Run `show` against one app in an open db, writing to `out` as the rows
/// stream in (nothing is buffered, so a large db prints at once).
pub async fn show(
    storage: &Storage,
    app_name: &str,
    stable_path: Option<&str>,
    mode: ShowMode,
    out: &mut impl Write,
) -> CliResult<()> {
    let store = storage
        .open_app_store_by_name(app_name)
        .await
        .map_err(fail("Failed to open app"))?;
    if mode.target_states {
        let renderer = TargetStates {
            tree: mode.tree,
            fingerprints: mode.fingerprints,
            prev: Vec::new(),
        };
        return match store {
            Some(store) => {
                write_stream(
                    spawn_target_state_iter(store, HashMap::new()).await,
                    renderer,
                    out,
                )
                .await
            }
            None => write_stream(none::<TargetStateEntry>(), renderer, out).await,
        };
    }
    let details = Details {
        fingerprints: mode.fingerprints,
    };
    if let Some(path) = stable_path {
        let found = match &store {
            Some(store) => query_stable_path_details_from_store(
                store,
                HashMap::new(),
                &parse_stable_path(path),
                mode.recursive,
                mode.recursive,
                mode.parents,
            )
            .await
            .map_err(fail("Failed to read the stable path"))?,
            None => Vec::new(),
        };
        let found = futures::stream::iter(found.into_iter().map(Ok::<_, Infallible>));
        return write_stream(found, details, out).await;
    }
    match (store, mode.long, mode.tree) {
        (Some(store), true, _) => {
            write_stream(
                spawn_stable_path_detail_iter(store, HashMap::new()).await,
                details,
                out,
            )
            .await
        }
        (None, true, _) => write_stream(none::<StablePathDetail>(), details, out).await,
        (Some(store), false, true) => {
            write_stream(iter_stable_paths_from_store(store).await, TreePaths, out).await
        }
        (None, false, true) => write_stream(none::<StablePathInfo>(), TreePaths, out).await,
        (Some(store), false, false) => {
            write_stream(iter_stable_paths_from_store(store).await, FlatPaths, out).await
        }
        (None, false, false) => write_stream(none::<StablePathInfo>(), FlatPaths, out).await,
    }
}

/// The stream of an app that has no state yet.
fn none<T>() -> futures::stream::Empty<Result<T, Infallible>> {
    futures::stream::empty()
}

/// Renders one kind of `show` row, one at a time.
trait Render {
    type Item;
    /// Printed first.
    const HEADER: &'static str;
    /// Printed when there are no rows.
    const EMPTY: Option<&'static str>;
    fn item(&mut self, item: &Self::Item, out: &mut String);
}

/// Write the header, each row as it arrives, then the empty marker if no row
/// came. A closed pipe (`cocoindex show | head`) ends the output quietly.
async fn write_stream<R: Render, E: std::fmt::Display>(
    stream: impl Stream<Item = Result<R::Item, E>>,
    mut renderer: R,
    out: &mut impl Write,
) -> CliResult<()> {
    let written = async {
        out.write_all(R::HEADER.as_bytes())?;
        let mut stream = std::pin::pin!(stream);
        let mut rows = 0usize;
        let mut buf = String::new();
        while let Some(item) = stream.next().await {
            let item = item
                .map_err(|e| std::io::Error::other(format!("Failed to read the database: {e}")))?;
            buf.clear();
            renderer.item(&item, &mut buf);
            out.write_all(buf.as_bytes())?;
            rows += 1;
        }
        if rows == 0
            && let Some(empty) = R::EMPTY
        {
            out.write_all(empty.as_bytes())?;
        }
        out.flush()
    }
    .await;
    match written {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        Err(err) => Err(CliError::failure(err.to_string())),
    }
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

/// `show`: one line per stable path.
struct FlatPaths;

impl Render for FlatPaths {
    type Item = StablePathInfo;
    const HEADER: &'static str = "Stable paths:\n";
    const EMPTY: Option<&'static str> = None;

    fn item(&mut self, item: &StablePathInfo, out: &mut String) {
        let _ = writeln!(out, "  {}", item.path);
    }
}

/// `show --tree`: stable paths as an indented list.
struct TreePaths;

impl Render for TreePaths {
    type Item = StablePathInfo;
    const HEADER: &'static str = "Stable paths:\n";
    const EMPTY: Option<&'static str> = Some("(none)\n");

    fn item(&mut self, item: &StablePathInfo, out: &mut String) {
        match item.path.last() {
            None => out.push_str("- /"),
            Some(last) => {
                let _ = write!(
                    out,
                    "{}- {}",
                    "  ".repeat(item.path.len() - 1),
                    key_label(last)
                );
            }
        }
        if is_component(&item.node_type) {
            out.push_str(" [component]");
        }
        out.push('\n');
    }
}

/// `show -l` and `show STABLE_PATH`: a block per stable path.
struct Details {
    fingerprints: bool,
}

impl Render for Details {
    type Item = StablePathDetail;
    const HEADER: &'static str = "Stable paths:\n";
    const EMPTY: Option<&'static str> = Some("  (none)\n");

    fn item(&mut self, detail: &StablePathDetail, out: &mut String) {
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
                let path = if self.fingerprints {
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
}

/// `show --target-states [--tree]`.
struct TargetStates {
    tree: bool,
    fingerprints: bool,
    /// The previous entry's fingerprint segments (tree mode).
    prev: Vec<String>,
}

impl Render for TargetStates {
    type Item = TargetStateEntry;
    const HEADER: &'static str = "Target states:\n";
    const EMPTY: Option<&'static str> = Some("  (none)\n");

    fn item(&mut self, entry: &TargetStateEntry, out: &mut String) {
        let marker = if entry.dangling { " [dangling]" } else { "" };
        if !self.tree {
            let path = if self.fingerprints {
                &entry.fingerprint_path
            } else {
                &entry.readable_path
            };
            let _ = writeln!(out, "  {path}{marker}");
            let _ = writeln!(out, "    owner:{}", entry.owner_component_path);
            return;
        }
        // Stored order lists a parent before its descendants, so comparing with
        // the previous entry's fingerprint segments places each line.
        let segments: Vec<&str> = entry
            .fingerprint_path
            .trim_start_matches('/')
            .split('/')
            .collect();
        let label = |depth: usize| -> &str {
            if self.fingerprints {
                segments[depth]
            } else {
                entry
                    .readable_segments
                    .get(depth)
                    .map_or(segments[depth], String::as_str)
            }
        };
        let last = segments.len() - 1;
        let common = self
            .prev
            .iter()
            .zip(&segments[..last])
            .take_while(|(prev, seg)| prev.as_str() == **seg)
            .count();
        for depth in common..last {
            let _ = writeln!(out, "{}- {}", "  ".repeat(depth), label(depth));
        }
        let _ = writeln!(
            out,
            "{}- {}{marker} owner:{}",
            "  ".repeat(last),
            label(last),
            entry.owner_component_path
        );
        self.prev = segments.iter().map(|s| s.to_string()).collect();
    }
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

    /// Drive a renderer through the real streaming writer.
    fn render<R: Render>(renderer: R, items: Vec<R::Item>) -> String {
        let mut out = Vec::new();
        let stream = futures::stream::iter(items.into_iter().map(Ok::<_, Infallible>));
        futures::executor::block_on(write_stream(stream, renderer, &mut out)).unwrap();
        String::from_utf8(out).unwrap()
    }

    fn info(parts: &[&str], node_type: StablePathNodeType) -> StablePathInfo {
        StablePathInfo {
            path: path(parts),
            node_type,
        }
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
        // Quoted keys may hold `/` and escapes, exactly as `show` prints them.
        let printed = path(&["files", r#"docs/a "b".md"#]).to_string();
        assert_eq!(parse_stable_path(&printed).to_string(), printed);
    }

    #[test]
    fn renders_flat_and_tree_paths() {
        let paths = || {
            vec![
                info(&[], StablePathNodeType::Component),
                info(&["files"], StablePathNodeType::Directory),
                info(&["files", "a.txt"], StablePathNodeType::Component),
            ]
        };
        assert_eq!(
            render(FlatPaths, paths()),
            "Stable paths:\n  /\n  /\"files\"\n  /\"files\"/\"a.txt\"\n"
        );
        assert_eq!(
            render(TreePaths, paths()),
            "Stable paths:\n- / [component]\n- files\n  - a.txt [component]\n"
        );
        assert_eq!(render(TreePaths, vec![]), "Stable paths:\n(none)\n");
        assert_eq!(render(FlatPaths, vec![]), "Stable paths:\n");
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
        assert_eq!(
            render(
                Details {
                    fingerprints: false
                },
                vec![detail.clone()]
            ),
            "Stable paths:\n  /\"files\"/\"a.txt\"\n    type:component version:3 processor:-\n    \
             has_memoization:true target_state_count:1\n    Target states:\n      - path:/@dir/\"A.TXT\"\n        \
             states:3:Existing schema_version:0 generation:1.0\n\n"
        );
        assert!(render(Details { fingerprints: true }, vec![detail]).contains("- path:/#ab/#cd"));
        assert_eq!(
            render(
                Details {
                    fingerprints: false
                },
                vec![]
            ),
            "Stable paths:\n  (none)\n"
        );
    }

    #[test]
    fn renders_target_states() {
        let entries = || {
            vec![
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
            ]
        };
        let renderer = |tree, fingerprints| TargetStates {
            tree,
            fingerprints,
            prev: Vec::new(),
        };
        assert_eq!(
            render(renderer(false, false), entries()),
            "Target states:\n  /@dir/\"a\"\n    owner:/\"files\"/\"a\"\n  /@dir/\"b\" [dangling]\n    owner:/\n"
        );
        assert_eq!(
            render(renderer(true, false), entries()),
            "Target states:\n- @dir\n  - \"a\" owner:/\"files\"/\"a\"\n  - \"b\" [dangling] owner:/\n"
        );
        assert!(render(renderer(true, true), entries()).contains("- #aa\n  - #bb"));
        assert_eq!(
            render(renderer(false, false), vec![]),
            "Target states:\n  (none)\n"
        );
    }

    #[test]
    fn a_closed_pipe_ends_output_quietly() {
        struct Closed;
        impl Write for Closed {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let stream = futures::stream::iter(vec![Ok::<_, Infallible>(info(
            &[],
            StablePathNodeType::Component,
        ))]);
        futures::executor::block_on(write_stream(stream, FlatPaths, &mut Closed)).unwrap();
    }
}
