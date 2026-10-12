//! The `__coco` stdio protocol between the `cocoindex` CLI and a user binary.
//!
//! The CLI builds the user's crate and runs the binary as
//! `<bin> __coco <verb> --protocol 1 [flags]`. The binary answers with records
//! on stdout: one per line, an ASCII record separator (`U+001E`) followed by a
//! JSON object with an `event` tag. Text on stdout without the separator is the
//! user's own output and is forwarded by the CLI verbatim. Stderr is inherited.
//!
//! The first record is always [`Event::Hello`]; the CLI refuses a protocol
//! number it does not know. Terminal records are [`Event::Done`] and
//! [`Event::Error`]. See `dev/plans/rust-cli.md` for the full contract.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::stats::{ComponentStats, RunStats, UpdateStats, UpdateStatus};

/// The protocol version this SDK speaks.
pub const PROTOCOL_VERSION: u32 = 1;

/// Record separator that starts every protocol line.
pub const RS: char = '\u{1e}';

/// The argv marker that routes a user binary into protocol mode.
pub const MARKER: &str = "__coco";

/// One protocol record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    /// Always the first record: the protocol number and SDK version.
    Hello { protocol: u32, sdk: String },
    /// Answer to `ls`: registered and persisted apps and the state db path.
    Apps { apps: Vec<AppInfo>, db_path: String },
    /// An `update` or `drop` is starting on this app.
    Start { app: String, db_path: String },
    /// Per-component counters at engine version `version`.
    Progress {
        version: u64,
        ready: bool,
        by_component: BTreeMap<String, ComponentStatsWire>,
    },
    /// One planned target action from an `update --preview`.
    Preview {
        kind: PreviewKind,
        value: serde_json::Value,
    },
    /// Terminal: the verb finished. `stats` is set for a completed update,
    /// `dropped` for a drop (`false` when there was nothing to drop).
    Done {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stats: Option<RunStatsWire>,
        #[serde(default)]
        cancelled: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dropped: Option<bool>,
    },
    /// Terminal: the verb failed.
    Error { message: String },
}

/// One app as reported by `ls`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppInfo {
    pub name: String,
    /// Whether the state db already holds data for this app.
    pub persisted: bool,
    /// Whether a `#[cocoindex::app]` with this name is linked into the binary.
    pub registered: bool,
}

/// Wire form of [`ComponentStats`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentStatsWire {
    pub num_execution_starts: u64,
    pub num_unchanged: u64,
    pub num_adds: u64,
    pub num_deletes: u64,
    pub num_reprocesses: u64,
    pub num_errors: u64,
}

impl From<&ComponentStats> for ComponentStatsWire {
    fn from(s: &ComponentStats) -> Self {
        Self {
            num_execution_starts: s.num_execution_starts,
            num_unchanged: s.num_unchanged,
            num_adds: s.num_adds,
            num_deletes: s.num_deletes,
            num_reprocesses: s.num_reprocesses,
            num_errors: s.num_errors,
        }
    }
}

impl ComponentStatsWire {
    /// Successfully processed items: `unchanged + adds + deletes + reprocesses`.
    pub fn num_processed(&self) -> u64 {
        self.num_unchanged + self.num_adds + self.num_deletes + self.num_reprocesses
    }

    /// Executions started but not yet finished.
    pub fn num_in_progress(&self) -> u64 {
        self.num_execution_starts
            .saturating_sub(self.num_processed() + self.num_errors)
    }
}

/// Wire form of [`RunStats`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RunStatsWire {
    pub processed: u64,
    pub skipped: u64,
    pub written: u64,
    pub deleted: u64,
    pub elapsed_secs: f64,
}

impl From<&RunStats> for RunStatsWire {
    fn from(s: &RunStats) -> Self {
        Self {
            processed: s.processed,
            skipped: s.skipped,
            written: s.written,
            deleted: s.deleted,
            elapsed_secs: s.elapsed.as_secs_f64(),
        }
    }
}

impl std::fmt::Display for RunStatsWire {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "processed {}, wrote {}, skipped {}, deleted {} in {:.1}s",
            self.processed, self.written, self.skipped, self.deleted, self.elapsed_secs
        )
    }
}

/// The kind of a previewed target action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreviewKind {
    Create,
    Update,
    Delete,
}

impl std::fmt::Display for PreviewKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            PreviewKind::Create => "Create",
            PreviewKind::Update => "Update",
            PreviewKind::Delete => "Delete",
        })
    }
}

impl Event {
    /// A progress record from a detailed stats snapshot.
    pub fn progress(version: u64, stats: &UpdateStats) -> Self {
        Event::Progress {
            version,
            ready: stats.status == UpdateStatus::Ready,
            by_component: stats
                .by_component
                .iter()
                .map(|(k, v)| (k.clone(), ComponentStatsWire::from(v)))
                .collect(),
        }
    }
}

/// Encode a record as one protocol line (separator, JSON, newline).
pub fn encode(event: &Event) -> String {
    let json = serde_json::to_string(event).expect("protocol events always serialize");
    format!("{RS}{json}\n")
}

/// One line of a protocol child's stdout, classified.
#[derive(Debug, Clone, PartialEq)]
pub enum Line<'a> {
    /// A protocol record. `text` is any user output that preceded the
    /// separator on the same line (a `print!` without a newline).
    Record { text: &'a str, event: Event },
    /// User output: no separator on the line.
    Text(&'a str),
    /// A separator followed by something that is not a record.
    Malformed { text: &'a str, raw: &'a str },
}

/// Classify one stdout line (without its trailing newline).
pub fn decode_line(line: &str) -> Line<'_> {
    match line.find(RS) {
        None => Line::Text(line),
        Some(i) => {
            let text = &line[..i];
            let raw = &line[i + RS.len_utf8()..];
            match serde_json::from_str::<Event>(raw) {
                Ok(event) => Line::Record { text, event },
                Err(_) => Line::Malformed { text, raw },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_every_variant() {
        let mut by_component = BTreeMap::new();
        by_component.insert(
            "app:x".to_string(),
            ComponentStatsWire {
                num_execution_starts: 3,
                num_adds: 2,
                ..Default::default()
            },
        );
        let events = vec![
            Event::Hello {
                protocol: 1,
                sdk: "0.1.0".into(),
            },
            Event::Apps {
                apps: vec![AppInfo {
                    name: "A".into(),
                    persisted: true,
                    registered: false,
                }],
                db_path: "./db".into(),
            },
            Event::Start {
                app: "A".into(),
                db_path: "./db".into(),
            },
            Event::Progress {
                version: 7,
                ready: false,
                by_component,
            },
            Event::Preview {
                kind: PreviewKind::Delete,
                value: serde_json::json!({"id": 1}),
            },
            Event::Done {
                stats: Some(RunStatsWire {
                    processed: 1,
                    elapsed_secs: 0.5,
                    ..Default::default()
                }),
                cancelled: false,
                dropped: None,
            },
            Event::Done {
                stats: None,
                cancelled: true,
                dropped: Some(false),
            },
            Event::Error {
                message: "boom".into(),
            },
        ];
        for event in events {
            let line = encode(&event);
            assert!(line.starts_with(RS), "line starts with RS");
            assert!(line.ends_with('\n'));
            match decode_line(line.trim_end_matches('\n')) {
                Line::Record { text, event: back } => {
                    assert_eq!(text, "");
                    assert_eq!(back, event);
                }
                other => panic!("expected a record, got {other:?}"),
            }
        }
    }

    #[test]
    fn event_tag_is_snake_case() {
        let line = encode(&Event::Hello {
            protocol: 1,
            sdk: "x".into(),
        });
        assert!(line.contains(r#""event":"hello""#));
        let line = encode(&Event::Done {
            stats: None,
            cancelled: false,
            dropped: None,
        });
        assert_eq!(
            line,
            format!("{RS}{{\"event\":\"done\",\"cancelled\":false}}\n")
        );
    }

    #[test]
    fn plain_text_passes_through() {
        assert_eq!(
            decode_line("indexed 3 files"),
            Line::Text("indexed 3 files")
        );
        assert_eq!(decode_line(""), Line::Text(""));
    }

    #[test]
    fn text_before_separator_is_kept() {
        let line = format!("partial{RS}{{\"event\":\"error\",\"message\":\"m\"}}");
        assert_eq!(
            decode_line(&line),
            Line::Record {
                text: "partial",
                event: Event::Error {
                    message: "m".into()
                }
            }
        );
    }

    #[test]
    fn bad_json_after_separator_is_malformed() {
        let line = format!("{RS}not json");
        assert_eq!(
            decode_line(&line),
            Line::Malformed {
                text: "",
                raw: "not json"
            }
        );
    }
}
