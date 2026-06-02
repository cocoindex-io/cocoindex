//! Pipeline stages: fetch/transcribe, LLM extraction, entity resolution, graph build.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use cocoindex::entity_resolution;
use cocoindex::prelude::*;
use cocoindex::surrealdb;
use serde::Deserialize;

use crate::clients::{EMBEDDER, Embedder, GRAPH, LLM, LlmClient, RESOLVER_LLM};
use crate::models::*;

// ---------------------------------------------------------------------------
// Phase 1: per-session processing
// ---------------------------------------------------------------------------

/// Fetch a transcript for a source. Local sources are returned directly;
/// YouTube sources are downloaded (yt-dlp) and transcribed (AssemblyAI).
/// Memoized so the expensive download/transcription happens once per video.
#[cocoindex::function(memo)]
pub async fn fetch_transcript(_ctx: &Ctx, source: &SessionSource) -> Result<SessionTranscript> {
    match source {
        SessionSource::Local { transcript, .. } => Ok(transcript.clone()),
        SessionSource::YouTube { youtube_id } => fetch_youtube(&youtube_id).await,
    }
}

/// Step 1: identify speakers + session metadata.
#[cocoindex::function(memo)]
pub async fn extract_metadata(
    ctx: &Ctx,
    reformatted_transcript: &String,
    transcript: &SessionTranscript,
) -> Result<SessionMetadata> {
    let llm = ctx.get_key(&LLM)?;
    let system = "You are an expert knowledge extractor analyzing a podcast/interview transcript. \
        Given the transcript (speakers labeled like \"(Speaker A)\") and YouTube metadata, return \
        a JSON object with fields: name (string, a clear descriptive episode name), description \
        (string or null, 1-2 sentence summary), date (string or null, ISO YYYY-MM-DD), and \
        speakers (array of {label, name}). For speakers, map each diarization label (A, B, ...) \
        to the speaker's full canonical Wikipedia-style name (e.g. \"Lex Fridman\"). Only include \
        speakers you can confidently identify with a full name; omit the rest. Do not guess.";
    let user = format!(
        "YouTube channel: {}\nVideo title: {}\nDescription: {}\nUpload date: {}\n\nTranscript:\n{}",
        transcript.yt_channel,
        transcript.yt_title,
        transcript.yt_description.as_deref().unwrap_or("N/A"),
        transcript.yt_upload_date.as_deref().unwrap_or("unknown"),
        reformatted_transcript,
    );
    let mut metadata: SessionMetadata = llm.json(system, &user).await?;
    metadata
        .speakers
        .retain(|s| is_plausible_person_name(&s.name));
    Ok(metadata)
}

/// Step 2: extract thematic statements + the entities they involve.
#[cocoindex::function(memo)]
pub async fn extract_statements(
    ctx: &Ctx,
    reformatted_transcript: &String,
) -> Result<StatementExtraction> {
    let llm = ctx.get_key(&LLM)?;
    let mut entity_lines = String::new();
    for kind in ENTITY_TYPES {
        let (desc, examples) = entity_guidance(kind);
        entity_lines.push_str(&format!("  - {kind}: {desc} Examples: {examples}.\n"));
    }
    let system = format!(
        "You are an expert knowledge extractor. Given a transcript where speakers are identified \
         by name, extract substantive thematic claims. Return a JSON object {{\"statements\": \
         [{{\"statement\": string, \"speakers\": [string], \"mentioned_person\": [string], \
         \"mentioned_tech\": [string], \"mentioned_org\": [string]}}]}}.\n\
         Rules:\n\
         - Write each statement as a clear standalone claim WITHOUT speaker attribution \
           (no \"X says...\").\n\
         - speakers: full names of who made it (empty for unidentified \"(Speaker X)\").\n\
         - mentioned_*: entities the statement is ABOUT; do not include the speakers in \
           mentioned_person unless the statement is about them.\n\
         - All names must be self-contained canonical Wikipedia-style names (no pronouns, \
           no \"the host\", no speaker labels):\n{entity_lines}",
    );
    let user = format!("Transcript:\n{reformatted_transcript}");
    let mut extraction: StatementExtraction = llm.json(&system, &user).await?;
    for st in &mut extraction.statements {
        st.speakers.retain(|s| !is_speaker_label(s));
        st.mentioned_person.retain(|p| !is_speaker_label(p));
    }
    Ok(extraction)
}

/// Format utterances into text, substituting resolved speaker names when known.
pub fn format_transcript(
    utterances: &[Utterance],
    speaker_map: &HashMap<String, String>,
) -> String {
    utterances
        .iter()
        .map(|u| match speaker_map.get(&u.speaker) {
            Some(name) => format!("{name}: {}", u.text),
            None => format!("(Speaker {}): {}", u.speaker, u.text),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Process one session end-to-end (fetch -> 2-pass extract -> assign ids).
/// Memoized by source: unchanged sessions skip all fetch/LLM work. Graph writes
/// happen later in `create_knowledge_base` through target-state declarations.
#[cocoindex::function(memo)]
pub async fn process_session(ctx: &Ctx, source: &SessionSource) -> Result<ProcessedSession> {
    let transcript = fetch_transcript(&ctx, &source).await?;

    // Step 1: format with no known names, extract metadata + speaker map.
    let step1 = format_transcript(&transcript.utterances, &HashMap::new());
    let metadata = extract_metadata(&ctx, &step1, &transcript).await?;

    // Step 2: format with real names, extract statements.
    let speaker_map: HashMap<String, String> = metadata
        .speakers
        .iter()
        .map(|s| (s.label.clone(), s.name.clone()))
        .collect();
    let step2 = format_transcript(&transcript.utterances, &speaker_map);
    let extraction = extract_statements(&ctx, &step2).await?;

    let key = source.key();
    let mut id_gen = IdGenerator::with_deps(&key)?;
    let session_id = id_gen.next_id_default(&ctx).await? as i64;

    let mut statements = Vec::with_capacity(extraction.statements.len());
    for raw in extraction.statements {
        let id = id_gen.next_id(&ctx, &raw.statement).await? as i64;
        statements.push(IdentifiedStatement { id, raw });
    }

    let youtube_id = match source {
        SessionSource::YouTube { youtube_id } => youtube_id.clone(),
        SessionSource::Local { key, .. } => key.clone(),
    };

    Ok(ProcessedSession {
        session_id,
        youtube_id,
        name: if metadata.name.is_empty() {
            transcript.yt_title.clone()
        } else {
            metadata.name
        },
        description: metadata.description,
        transcript: step2,
        date: metadata.date.or(transcript.yt_upload_date),
        identified_persons: metadata.speakers.iter().map(|s| s.name.clone()).collect(),
        statements,
    })
}

// ---------------------------------------------------------------------------
// Phase 2: entity resolution
// ---------------------------------------------------------------------------

/// Resolve near-duplicate entity names of one type. Returns a map
/// name -> Some(canonical) / None (canonical). Memoized by (kind, names).
#[cocoindex::function(memo)]
pub async fn resolve_entities(
    ctx: &Ctx,
    kind: &String,
    names: &Vec<String>,
) -> Result<HashMap<String, Option<String>>> {
    let embedder = ctx.get_key(&EMBEDDER)?;
    let llm = ctx.get_key(&RESOLVER_LLM)?;
    let resolver = LlmEntityResolver {
        llm: llm.clone(),
        kind: kind.clone(),
    };
    let resolved = entity_resolution::resolve_entities(
        names,
        &EntityNameEmbedder {
            embedder: embedder.clone(),
        },
        &resolver,
        None,
        entity_resolution::ResolveOptions::default(),
    )
    .await?;
    Ok(resolved.to_map().into_iter().collect())
}

#[derive(Deserialize)]
struct PairResolution {
    matched: Option<String>,
    #[serde(default)]
    canonical: Option<String>,
}

struct EntityNameEmbedder {
    embedder: Embedder,
}

#[async_trait]
impl entity_resolution::EntityEmbedder for EntityNameEmbedder {
    async fn embed_entity(&self, entity: &str) -> Result<Vec<f32>> {
        let mut embeddings = self.embedder.embed(vec![entity.to_string()]).await?;
        embeddings
            .pop()
            .ok_or_else(|| Error::engine("embedder returned no vector"))
    }
}

struct LlmEntityResolver {
    llm: LlmClient,
    kind: String,
}

#[async_trait]
impl entity_resolution::PairResolver for LlmEntityResolver {
    async fn resolve_pair(
        &self,
        entity: &str,
        candidates: &[String],
    ) -> Result<entity_resolution::PairDecision> {
        let candidate_lines = candidates
            .iter()
            .enumerate()
            .map(|(idx, candidate)| format!("{idx}. {candidate}"))
            .collect::<Vec<_>>()
            .join("\n");
        let system = format!(
            "You are resolving duplicate {kind} entities. Given one new entity and a candidate \
             list of canonical entities, decide whether the new entity refers to exactly one of \
             the candidates. Return JSON {{\"matched\": string|null, \"canonical\": \"matched\"|\"new\"}}. \
             Use matched=null when none are the same real-world {kind}. Be conservative.",
            kind = self.kind
        );
        let user = format!("Entity: {entity}\n\nCandidates:\n{candidate_lines}");
        let response: PairResolution = self.llm.json(&system, &user).await?;
        let canonical = match response.canonical.as_deref() {
            Some("new") => entity_resolution::CanonicalSide::New,
            _ => entity_resolution::CanonicalSide::Matched,
        };
        Ok(entity_resolution::PairDecision {
            matched: response.matched,
            canonical,
        })
    }
}

fn session_schema() -> Result<surrealdb::TableSchema> {
    surrealdb::TableSchema::new([
        ("youtube_id", surrealdb::ColumnDef::new("string")),
        ("name", surrealdb::ColumnDef::new("string")),
        (
            "description",
            surrealdb::ColumnDef::new("string").nullable(),
        ),
        ("transcript", surrealdb::ColumnDef::new("string")),
        ("date", surrealdb::ColumnDef::new("string").nullable()),
    ])
}

fn statement_schema() -> Result<surrealdb::TableSchema> {
    surrealdb::TableSchema::new([("statement", surrealdb::ColumnDef::new("string"))])
}

fn entity_schema() -> Result<surrealdb::TableSchema> {
    surrealdb::TableSchema::new([("name", surrealdb::ColumnDef::new("string"))])
}

// ---------------------------------------------------------------------------
// Phase 3: declare the desired knowledge graph target state
// ---------------------------------------------------------------------------

pub async fn create_knowledge_base(
    ctx: &Ctx,
    processed: &[ProcessedSession],
    dedups: &HashMap<String, HashMap<String, Option<String>>>,
) -> Result<()> {
    let graph = ctx.get_key(&GRAPH)?;

    let session_target =
        surrealdb::mount_table_target_with_schema(ctx, graph, "session", Some(session_schema()?))
            .await?;
    let statement_target = surrealdb::mount_table_target_with_schema(
        ctx,
        graph,
        "statement",
        Some(statement_schema()?),
    )
    .await?;
    let person_target =
        surrealdb::mount_table_target_with_schema(ctx, graph, PERSON, Some(entity_schema()?))
            .await?;
    let tech_target =
        surrealdb::mount_table_target_with_schema(ctx, graph, TECH, Some(entity_schema()?)).await?;
    let org_target =
        surrealdb::mount_table_target_with_schema(ctx, graph, ORG, Some(entity_schema()?)).await?;
    let session_statement_target = surrealdb::mount_relation_target(
        ctx,
        graph,
        "session_statement",
        &session_target,
        &statement_target,
    )
    .await?;
    let person_session_target = surrealdb::mount_relation_target(
        ctx,
        graph,
        "person_session",
        &person_target,
        &session_target,
    )
    .await?;
    let person_statement_target = surrealdb::mount_relation_target(
        ctx,
        graph,
        "person_statement",
        &person_target,
        &statement_target,
    )
    .await?;
    let statement_mentions_target = surrealdb::mount_relation_target_many(
        ctx,
        graph,
        "statement_mentions",
        &[&statement_target],
        &[&person_target, &tech_target, &org_target],
        None,
    )
    .await?;

    let entity_target = |kind: &str| match kind {
        PERSON => &person_target,
        TECH => &tech_target,
        ORG => &org_target,
        _ => unreachable!("unknown entity type"),
    };

    // Sessions + statements + session_statement edges.
    for ps in processed {
        session_target.declare_record(
            ctx,
            ps.session_id,
            &serde_json::json!({
                "youtube_id": &ps.youtube_id,
                "name": &ps.name,
                "description": &ps.description,
                "transcript": &ps.transcript,
                "date": &ps.date,
            }),
        )?;
        for st in &ps.statements {
            statement_target.declare_record(
                ctx,
                st.id,
                &serde_json::json!({ "statement": &st.raw.statement }),
            )?;
            session_statement_target.declare_relation(ctx, ps.session_id, st.id)?;
        }
    }

    // Canonical entity nodes (those whose dedup value is None).
    for kind in ENTITY_TYPES {
        let dedup = dedups.get(*kind).cloned().unwrap_or_default();
        for (name, upstream) in &dedup {
            if upstream.is_none() {
                entity_target(kind).declare_record(
                    ctx,
                    name,
                    &serde_json::json!({ "name": name }),
                )?;
            }
        }
    }

    let empty = HashMap::new();
    let person_dedup = dedups.get(PERSON).unwrap_or(&empty);

    // Relations.
    for ps in processed {
        for person in &ps.identified_persons {
            let canon = resolve_canonical(person, person_dedup);
            person_session_target.declare_relation(ctx, canon, ps.session_id)?;
        }
        for st in &ps.statements {
            let mut seen: HashSet<String> = HashSet::new();
            for speaker in &st.raw.speakers {
                let canon = resolve_canonical(speaker, person_dedup);
                if seen.insert(canon.clone()) {
                    person_statement_target.declare_relation(ctx, canon, st.id)?;
                }
            }
            for kind in ENTITY_TYPES {
                let dedup = dedups.get(*kind).unwrap_or(&empty);
                let canons: HashSet<String> = st
                    .raw
                    .mentioned(kind)
                    .iter()
                    .map(|e| resolve_canonical(e, dedup))
                    .collect();
                for canon in canons {
                    statement_mentions_target.declare_relation_between(
                        ctx,
                        "statement",
                        st.id,
                        kind,
                        canon,
                    )?;
                }
            }
        }
    }
    Ok(())
}

/// Collect all raw entity names of one type across sessions (resolution input).
pub fn collect_raw(processed: &[ProcessedSession], kind: &str) -> Vec<String> {
    let mut set: HashSet<String> = HashSet::new();
    for ps in processed {
        if kind == PERSON {
            set.extend(ps.identified_persons.iter().cloned());
        }
        for st in &ps.statements {
            if kind == PERSON {
                set.extend(st.raw.speakers.iter().cloned());
            }
            set.extend(st.raw.mentioned(kind).iter().cloned());
        }
    }
    let mut names: Vec<String> = set.into_iter().collect();
    names.sort();
    names
}

// ---------------------------------------------------------------------------
// YouTube fetch (yt-dlp + AssemblyAI) — real path; compile-checked here.
// ---------------------------------------------------------------------------

async fn fetch_youtube(youtube_id: &str) -> Result<SessionTranscript> {
    let url = format!("https://www.youtube.com/watch?v={youtube_id}");
    let dir = tempdir()?;
    let out_tmpl = dir.path().join("audio.%(ext)s");
    let out_tmpl = out_tmpl.to_string_lossy().to_string();

    // 1. Download audio + metadata JSON via yt-dlp (blocking subprocess).
    let url_c = url.clone();
    let info_json = tokio::task::spawn_blocking(move || -> Result<String> {
        let output = std::process::Command::new("yt-dlp")
            .args([
                "-x",
                "--audio-format",
                "mp3",
                "--audio-quality",
                "64K",
                "-o",
                &out_tmpl,
                "--no-playlist",
                "--quiet",
                "--print-json",
                &url_c,
            ])
            .output()
            .map_err(|e| Error::engine(format!("yt-dlp failed to launch (installed?): {e}")))?;
        if !output.status.success() {
            return Err(Error::engine(format!(
                "yt-dlp error: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    })
    .await
    .map_err(|e| Error::engine(format!("yt-dlp task panicked: {e}")))??;

    let info: serde_json::Value = serde_json::from_str(info_json.lines().next().unwrap_or("{}"))
        .unwrap_or(serde_json::Value::Null);
    let yt_channel = info["channel"]
        .as_str()
        .or_else(|| info["uploader"].as_str())
        .unwrap_or("")
        .to_string();
    let yt_title = info["title"].as_str().unwrap_or(youtube_id).to_string();
    let yt_description = info["description"].as_str().map(str::to_string);
    let yt_upload_date = info["upload_date"].as_str().map(|d| {
        if d.len() == 8 {
            format!("{}-{}-{}", &d[0..4], &d[4..6], &d[6..8])
        } else {
            d.to_string()
        }
    });

    let mp3 = dir.path().join("audio.mp3");
    let audio = std::fs::read(&mp3)
        .map_err(|e| Error::engine(format!("missing yt-dlp mp3 output {mp3:?}: {e}")))?;

    // 2. Transcribe with AssemblyAI (speaker diarization).
    let utterances = assemblyai_transcribe(audio).await?;

    Ok(SessionTranscript {
        utterances,
        yt_channel,
        yt_title,
        yt_description,
        yt_upload_date,
    })
}

async fn assemblyai_transcribe(audio: Vec<u8>) -> Result<Vec<Utterance>> {
    let key =
        std::env::var("ASSEMBLYAI_API_KEY").map_err(|_| Error::engine("set ASSEMBLYAI_API_KEY"))?;
    let http = reqwest::Client::new();
    let base = "https://api.assemblyai.com/v2";

    let upload: serde_json::Value = http
        .post(format!("{base}/upload"))
        .header("authorization", &key)
        .body(audio)
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| Error::engine(format!("AssemblyAI upload failed: {e}")))?
        .json()
        .await
        .map_err(|e| Error::engine(format!("AssemblyAI upload response: {e}")))?;
    let audio_url = upload["upload_url"]
        .as_str()
        .ok_or_else(|| Error::engine("AssemblyAI upload: no upload_url"))?;

    let created: serde_json::Value = http
        .post(format!("{base}/transcript"))
        .header("authorization", &key)
        .json(&serde_json::json!({
            "audio_url": audio_url,
            "speaker_labels": true,
            "speech_models": ["universal-3-pro"],
        }))
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| Error::engine(format!("AssemblyAI transcript create failed: {e}")))?
        .json()
        .await
        .map_err(|e| Error::engine(format!("AssemblyAI create response: {e}")))?;
    let id = created["id"]
        .as_str()
        .ok_or_else(|| Error::engine("AssemblyAI: no transcript id"))?
        .to_string();

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let poll: serde_json::Value = http
            .get(format!("{base}/transcript/{id}"))
            .header("authorization", &key)
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| Error::engine(format!("AssemblyAI poll failed: {e}")))?
            .json()
            .await
            .map_err(|e| Error::engine(format!("AssemblyAI poll response: {e}")))?;
        match poll["status"].as_str() {
            Some("completed") => {
                let mut out = Vec::new();
                if let Some(utts) = poll["utterances"].as_array() {
                    for u in utts {
                        out.push(Utterance {
                            speaker: u["speaker"].as_str().unwrap_or("A").to_string(),
                            text: u["text"].as_str().unwrap_or("").to_string(),
                        });
                    }
                }
                if out.is_empty() {
                    out.push(Utterance {
                        speaker: "A".to_string(),
                        text: poll["text"].as_str().unwrap_or("").to_string(),
                    });
                }
                return Ok(out);
            }
            Some("error") => {
                return Err(Error::engine(format!(
                    "AssemblyAI transcription error: {}",
                    poll["error"].as_str().unwrap_or("unknown")
                )));
            }
            _ => continue,
        }
    }
}

fn tempdir() -> Result<tempfile::TempDir> {
    tempfile::Builder::new()
        .prefix("conv2k_")
        .tempdir()
        .map_err(Error::Io)
}
