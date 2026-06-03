//! Data models for the Conversation-to-Knowledge pipeline.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Entity types
// ---------------------------------------------------------------------------

pub const PERSON: &str = "person";
pub const TECH: &str = "tech";
pub const ORG: &str = "org";
pub const ENTITY_TYPES: &[&str] = &[PERSON, TECH, ORG];

/// LLM-facing description + examples for an entity type (injected into prompts).
pub fn entity_guidance(kind: &str) -> (&'static str, &'static str) {
    match kind {
        PERSON => (
            "Real people, using Wikipedia-style full names. Only include people you can \
             confidently identify with their full name — omit anyone you cannot identify.",
            "Lex Fridman, Sam Altman, Franklin D. Roosevelt",
        ),
        TECH => (
            "Technologies, tools, frameworks, and concepts.",
            "Python (programming language), Large language model, ChatGPT",
        ),
        ORG => (
            "Organizations and companies, using their canonical names.",
            "OpenAI, Google DeepMind, US Department of Education",
        ),
        _ => ("Named entities.", ""),
    }
}

// ---------------------------------------------------------------------------
// Transcript / source
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Utterance {
    /// Diarization label (e.g. "A", "B") or a resolved speaker name.
    pub speaker: String,
    pub text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionTranscript {
    pub utterances: Vec<Utterance>,
    pub yt_channel: String,
    pub yt_title: String,
    pub yt_description: Option<String>,
    pub yt_upload_date: Option<String>,
}

/// A unit of input: either a YouTube video to fetch+transcribe, or a
/// pre-transcribed local session (cheap, audio-free — for testing/demo).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SessionSource {
    YouTube {
        youtube_id: String,
    },
    Local {
        key: String,
        transcript: SessionTranscript,
    },
}

impl SessionSource {
    pub fn key(&self) -> String {
        match self {
            SessionSource::YouTube { youtube_id } => youtube_id.clone(),
            SessionSource::Local { key, .. } => key.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// LLM response models
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpeakerId {
    pub label: String,
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionMetadata {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub date: Option<String>,
    #[serde(default)]
    pub speakers: Vec<SpeakerId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawStatement {
    pub statement: String,
    #[serde(default)]
    pub speakers: Vec<String>,
    #[serde(default)]
    pub mentioned_person: Vec<String>,
    #[serde(default)]
    pub mentioned_tech: Vec<String>,
    #[serde(default)]
    pub mentioned_org: Vec<String>,
}

impl RawStatement {
    pub fn mentioned(&self, kind: &str) -> &[String] {
        match kind {
            PERSON => &self.mentioned_person,
            TECH => &self.mentioned_tech,
            ORG => &self.mentioned_org,
            _ => &[],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatementExtraction {
    #[serde(default)]
    pub statements: Vec<RawStatement>,
}

// ---------------------------------------------------------------------------
// Pipeline transfer types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IdentifiedStatement {
    pub id: i64,
    pub raw: RawStatement,
}

/// Everything phase 3 needs to build the graph for one session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProcessedSession {
    pub session_id: i64,
    pub youtube_id: String,
    pub name: String,
    pub description: Option<String>,
    pub transcript: String,
    pub date: Option<String>,
    /// Identified (named) speakers of the session.
    pub identified_persons: Vec<String>,
    pub statements: Vec<IdentifiedStatement>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Chase dedup chains to the canonical name. `dedup[name] == None` means `name`
/// is itself canonical; `Some(parent)` points one step upstream.
pub fn resolve_canonical(name: &str, dedup: &HashMap<String, Option<String>>) -> String {
    let mut cur = name.to_string();
    while let Some(Some(parent)) = dedup.get(&cur) {
        if parent == &cur {
            break;
        }
        cur = parent.clone();
    }
    cur
}

/// True if `name` is a leftover diarization label like "Speaker A" / "(Speaker B)".
pub fn is_speaker_label(name: &str) -> bool {
    let n = name
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim();
    let lower = n.to_ascii_lowercase();
    if let Some(rest) = lower.strip_prefix("speaker ") {
        let rest = rest.trim();
        return rest.len() <= 2 && rest.chars().all(|c| c.is_ascii_alphanumeric());
    }
    false
}

/// A plausible person name has at least two words (filters "null", "unknown", etc.).
pub fn is_plausible_person_name(name: &str) -> bool {
    name.split_whitespace().count() >= 2
}
