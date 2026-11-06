//! Meeting Notes Graph (SurrealDB) — Rust port of the Python
//! `meeting_notes_graph_surrealdb` example.
//!
//! Ingest Markdown meeting notes from a local folder, split each note into
//! per-meeting sections at heading boundaries, extract structured information
//! with an LLM, deduplicate person names with embedding-based entity
//! resolution, and build a knowledge graph in SurrealDB:
//!
//!   meeting records — one per meeting section
//!   person  records — canonical organizers, participants, and task assignees
//!   task    records — tasks decided in meetings
//!
//!   attended     person -> meeting (with is_organizer flag)
//!   decided      meeting -> task
//!   assigned_to  person -> task
//!
//! The pipeline runs in three phases:
//!   1. Per-file extraction declares meeting and task records plus decided edges,
//!      and publishes raw (un-resolved) person names into an in-memory LiveMap.
//!   2. Person entity resolution maps raw names to canonical names.
//!   3. Phases 2+3 run together in a periodically-refreshed component that reads
//!      the LiveMap snapshot and declares canonical person records and the
//!      person-touching edges (attended, assigned_to) using resolved names.
//!
//!   cargo run                    # one-shot catch-up over ./notes
//!   cargo run -- --live          # keep watching ./notes; edits stream into the graph
//!   cargo run -- [NOTES_DIR] [--live]
//!
//! Parallels the Python example:
//!   - LLM extraction     : `ApiChatClient::chat_json` + serde (cf. instructor + LiteLLM)
//!   - entity resolution  : `cocoindex::resolve_entities` + `LlmPairResolver`
//!   - per-file compute   : `ctx.mount_each_live` over `walk_dir(..).live()`
//!   - cross-file phase   : `ctx.auto_refresh` reading a `LiveMap` snapshot
//!   - target             : `surrealdb::TableTarget` / `RelationTarget`

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use chrono::NaiveDate;
use cocoindex::connectors::surrealdb::{self, Graph, TableSchema};
use cocoindex::ops::api::{ApiChatClient, ChatMessage, LlmPairResolver};
use cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder;
use cocoindex::prelude::*;

const APP_NAME: &str = "MeetingNotesGraphSurrealDB";
const EMBED_MODEL: &str = "Snowflake/snowflake-arctic-embed-xs";
const REFRESH_INTERVAL: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Context keys
// ---------------------------------------------------------------------------

cocoindex::context_key!(static KG_DB: Graph);
// The model ids are memo dependencies (cf. Python's `detect_change=True`):
// swapping a model re-extracts / re-resolves against it.
cocoindex::context_key!(static LLM_MODEL: String, detect_change);
cocoindex::context_key!(static RESOLUTION_LLM_MODEL: String, detect_change);
cocoindex::context_key!(
    static EMBEDDER: SentenceTransformerEmbedder,
    detect_change
);

// ---------------------------------------------------------------------------
// SurrealDB row schemas. The `id` field becomes the SurrealDB record id.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, SchemaFields)]
struct Meeting {
    id: i64, // Generated via IdGenerator over the meeting date
    note_file: String,
    // Stored as an ISO string: the connector's datetime mapping requires
    // SurrealDB datetime literals, which JSON transport doesn't produce.
    time: String,
    note: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, SchemaFields)]
struct Person {
    id: String, // canonical name — traversals read as person:`Alice Chen`
}

#[derive(Clone, Debug, Serialize, Deserialize, SchemaFields)]
struct Task {
    id: String, // task description
}

/// `attended` edge payload. The relation record id is derived from
/// (from_id=person, to_id=meeting_id) by the SurrealDB connector — the relation
/// is mounted without a schema, giving exactly one edge per (person, meeting).
#[derive(Clone, Debug, Serialize)]
struct AttendedRel {
    is_organizer: bool,
}

// `decided` and `assigned_to` carry no payload — declared without schema or
// record, with the connector deriving record ids from (from_id, to_id).

// ---------------------------------------------------------------------------
// LLM extraction schema (serde, for `ApiChatClient::chat_json`)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExtractedPerson {
    name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExtractedTask {
    description: String,
    #[serde(default)]
    assigned_to: Vec<ExtractedPerson>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExtractedMeeting {
    time: NaiveDate,
    note: String,
    organizer: ExtractedPerson,
    #[serde(default)]
    participants: Vec<ExtractedPerson>,
    #[serde(default)]
    tasks: Vec<ExtractedTask>,
}

const EXTRACT_PROMPT: &str = "\
You are an expert at reading meeting notes and extracting structured information.

Given a single meeting section (Markdown), extract:
- The meeting date (look for a date in the heading or body; required).
- A brief note summarizing what the meeting was about.
- The organizer (the person who ran the meeting). If unclear, pick the person
  who appears most central to the meeting.
- Participants other than the organizer.
- Tasks or action items decided, including who they are assigned to.

Return only what is supported by the text. Use full names where available.

Respond with a JSON object of this exact shape:
{
  \"time\": \"YYYY-MM-DD\",
  \"note\": \"a brief summary or notes from the meeting section\",
  \"organizer\": {\"name\": \"full name of the person who organized or led the meeting\"},
  \"participants\": [{\"name\": \"full name, as written in the note\"}],
  \"tasks\": [
    {
      \"description\": \"concise, standalone description of the task or action item\",
      \"assigned_to\": [{\"name\": \"full name\"}]
    }
  ]
}
Do not include the organizer in `participants`.
";

/// The chat client for `model`: an OpenAI-compatible endpoint (`OPENAI_API_KEY`;
/// `LLM_BASE_URL` for another provider).
fn chat_client(model: &str) -> ApiChatClient {
    let client = ApiChatClient::new(model);
    match std::env::var("LLM_BASE_URL") {
        Ok(url) => client.with_base_url(url),
        Err(_) => client,
    }
}

/// Extract a structured meeting from a Markdown section. Memoized by section
/// text (and the model id, through `LLM_MODEL`), so an unchanged section costs
/// no LLM call on a re-run.
#[cocoindex::function(memo)]
async fn extract_meeting(ctx: &Ctx, section_text: &String) -> Result<ExtractedMeeting> {
    let client = chat_client(ctx.get_key(&LLM_MODEL)?);
    let content = client
        .chat_json(&[
            ChatMessage::system(EXTRACT_PROMPT),
            ChatMessage::user(section_text.clone()),
        ])
        .await?;
    serde_json::from_str(&content).map_err(|e| {
        Error::engine(format!(
            "meeting extraction is not the expected JSON: {e}: {content}"
        ))
    })
}

// ---------------------------------------------------------------------------
// Splitting — match Python's `\n\n##?\s+` heading regex
// ---------------------------------------------------------------------------

/// Split a note into per-meeting sections at `#`/`##` headings that follow a
/// blank line (or open the note). Like the Python regex split, the heading
/// marker and the whitespace after it are dropped from the section.
fn split_meetings(text: &str) -> Vec<String> {
    let padded = format!("\n\n{text}");
    let bytes = padded.as_bytes();
    // (start of the separator, start of the section text after it)
    let mut cuts: Vec<(usize, usize)> = Vec::new();
    let mut i = 0;
    while let Some(pos) = padded[i..].find("\n\n#") {
        let at = i + pos;
        let mut after = at + 3;
        if bytes.get(after) == Some(&b'#') {
            after += 1;
        }
        let body = padded[after..].trim_start();
        if body.len() < padded.len() - after {
            cuts.push((at, padded.len() - body.len()));
        }
        i = at + 2;
    }
    let mut sections = Vec::new();
    let mut last = 0;
    for (separator, body) in cuts {
        sections.push(padded[last..separator].trim().to_string());
        last = body;
    }
    sections.push(padded[last..].trim().to_string());
    sections.into_iter().filter(|s| !s.is_empty()).collect()
}

// ---------------------------------------------------------------------------
// Internal transfer type (Phase 1 -> Phase 3)
// ---------------------------------------------------------------------------

/// Raw per-meeting data carried forward to entity resolution + relation
/// declaration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct MeetingExtraction {
    meeting_id: i64,
    organizer: String,                          // raw name
    participants: Vec<String>,                  // raw names
    task_assignees: Vec<(String, Vec<String>)>, // (task_description, [raw assignee names])
}

// ---------------------------------------------------------------------------
// Phase 1: per-meeting and per-file processing
// ---------------------------------------------------------------------------

/// Not memoized: LiveMap producers must re-run each session to refill the
/// in-memory map. The expensive part — `extract_meeting` — is memoized per
/// section.
#[cocoindex::function]
async fn process_file(
    ctx: &Ctx,
    file: FileEntry,
    meeting_table: surrealdb::TableTarget,
    task_table: surrealdb::TableTarget,
    decided_rel: surrealdb::RelationTarget,
    extractions: LiveMap<Vec<MeetingExtraction>>,
) -> Result<()> {
    let text = file.content_str()?;
    let note_file = file.key();
    let mut id_generator = IdGenerator::new();
    let mut file_extractions = Vec::new();
    for section in split_meetings(&text) {
        let extracted = extract_meeting(ctx, &section).await?;
        let meeting_id = id_generator.next_id(ctx, &extracted.time).await? as i64;

        meeting_table.declare_row(
            ctx,
            &Meeting {
                id: meeting_id,
                note_file: note_file.clone(),
                time: extracted.time.to_string(),
                note: extracted.note.clone(),
            },
        )?;

        for task in &extracted.tasks {
            task_table.declare_row(
                ctx,
                &Task {
                    id: task.description.clone(),
                },
            )?;
            decided_rel.declare_relation(ctx, meeting_id, task.description.as_str())?;
        }

        file_extractions.push(MeetingExtraction {
            meeting_id,
            organizer: extracted.organizer.name,
            participants: extracted.participants.into_iter().map(|p| p.name).collect(),
            task_assignees: extracted
                .tasks
                .into_iter()
                .map(|t| {
                    (
                        t.description,
                        t.assigned_to.into_iter().map(|a| a.name).collect(),
                    )
                })
                .collect(),
        });
    }
    extractions.declare_entry(ctx, note_file, file_extractions)
}

// ---------------------------------------------------------------------------
// Phase 2: Person entity resolution
// ---------------------------------------------------------------------------

/// Memoized by the set of raw names (and the resolution model id). The SDK
/// embedder and `LlmPairResolver` also memoize each name and each pair
/// decision, so a new name costs one embedding and its own LLM calls only.
#[cocoindex::function(memo)]
async fn resolve_persons(ctx: &Ctx, raw_persons: &Vec<String>) -> Result<ResolvedEntities> {
    let resolver = LlmPairResolver::with_client(chat_client(ctx.get_key(&RESOLUTION_LLM_MODEL)?));
    resolve_entities(
        ctx,
        raw_persons,
        ctx.get_key(&EMBEDDER)?,
        &resolver,
        None,
        ResolveOptions::default(),
    )
    .await
}

// ---------------------------------------------------------------------------
// Phase 3: declare canonical person records + person-touching relations.
// Runs as an auto-refreshed component: each cycle reads the LiveMap snapshot,
// so edits streamed in by phase 1 reach the graph within one interval.
// ---------------------------------------------------------------------------

async fn sync_person_graph(
    ctx: &Ctx,
    extractions: &LiveMap<Vec<MeetingExtraction>>,
    person_table: &surrealdb::TableTarget,
    attended_rel: &surrealdb::RelationTarget,
    assigned_rel: &surrealdb::RelationTarget,
) -> Result<()> {
    let meetings: Vec<MeetingExtraction> = extractions
        .scan()
        .await?
        .into_iter()
        .flat_map(|(_note_file, file_extractions)| file_extractions)
        .collect();

    let mut raw_persons = BTreeSet::new();
    for m in &meetings {
        raw_persons.insert(m.organizer.clone());
        raw_persons.extend(m.participants.iter().cloned());
        for (_task_desc, assignees) in &m.task_assignees {
            raw_persons.extend(assignees.iter().cloned());
        }
    }

    let persons = resolve_persons(ctx, &raw_persons.into_iter().collect()).await?;

    // Declare canonical person records.
    for canonical_name in persons.canonicals() {
        person_table.declare_row(
            ctx,
            &Person {
                id: canonical_name.to_string(),
            },
        )?;
    }

    for m in &meetings {
        // attended — aggregate organizer + participants. Organizer flag wins
        // on collision so a person listed as both gets a single edge with
        // is_organizer=true. Resolution happens before aggregation so two
        // raw names that resolve to the same person also collapse.
        let mut attendees: BTreeMap<String, bool> =
            BTreeMap::from([(persons.canonical_of(&m.organizer)?, true)]);
        for p in &m.participants {
            attendees.entry(persons.canonical_of(p)?).or_insert(false);
        }
        for (canonical, is_organizer) in attendees {
            attended_rel.declare_relation_record(
                ctx,
                canonical.as_str(),
                m.meeting_id,
                &AttendedRel { is_organizer },
            )?;
        }

        // assigned_to — dedup per (canonical person, task description).
        for (task_desc, assignees) in &m.task_assignees {
            let mut seen = BTreeSet::new();
            for raw in assignees {
                let canonical = persons.canonical_of(raw)?;
                if seen.insert(canonical.clone()) {
                    assigned_rel.declare_relation(ctx, canonical.as_str(), task_desc.as_str())?;
                }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// App main
// ---------------------------------------------------------------------------

async fn app_main(ctx: Ctx, notesdir: PathBuf) -> Result<()> {
    // --- Mount node tables ---
    let meeting_table = surrealdb::mount_table_target_with_schema(
        &ctx,
        &KG_DB,
        "meeting",
        Some(TableSchema::from_row::<Meeting>()?),
    )
    .await?;
    let person_table = surrealdb::mount_table_target_with_schema(
        &ctx,
        &KG_DB,
        "person",
        Some(TableSchema::from_row::<Person>()?),
    )
    .await?;
    let task_table = surrealdb::mount_table_target_with_schema(
        &ctx,
        &KG_DB,
        "task",
        Some(TableSchema::from_row::<Task>()?),
    )
    .await?;

    // --- Mount relation targets ---
    // attended carries is_organizer; mounted without a schema so the connector
    // derives the relation record id from (from_id, to_id).
    let attended_rel =
        surrealdb::mount_relation_target(&ctx, &KG_DB, "attended", &person_table, &meeting_table)
            .await?;
    let decided_rel =
        surrealdb::mount_relation_target(&ctx, &KG_DB, "decided", &meeting_table, &task_table)
            .await?;
    let assigned_rel =
        surrealdb::mount_relation_target(&ctx, &KG_DB, "assigned_to", &person_table, &task_table)
            .await?;

    // --- Phase 1: per-file extraction, live-watching the notes folder ---
    let extractions: LiveMap<Vec<MeetingExtraction>> = LiveMap::create(&ctx).await?;
    let source = walk_dir(notesdir)
        .recursive(true)
        .path_matcher(PatternFilePathMatcher::include(["**/*.md"])?)
        .live();
    {
        let (meeting_table, task_table, decided_rel, extractions) = (
            meeting_table.clone(),
            task_table.clone(),
            decided_rel.clone(),
            extractions.clone(),
        );
        // Returns once the initial scan is ready, so the first aggregation
        // below sees every note.
        ctx.mount_each_live(&"files", source, move |ctx, file: FileEntry| {
            let (meeting_table, task_table, decided_rel, extractions) = (
                meeting_table.clone(),
                task_table.clone(),
                decided_rel.clone(),
                extractions.clone(),
            );
            async move {
                process_file(
                    &ctx,
                    file,
                    meeting_table,
                    task_table,
                    decided_rel,
                    extractions,
                )
                .await
            }
        })
        .await?;
    }

    // --- Phases 2+3: entity resolution + person graph, refreshed periodically ---
    ctx.auto_refresh(&"sync_person_graph", REFRESH_INTERVAL, move |ctx| {
        let (extractions, person_table, attended_rel, assigned_rel) = (
            extractions.clone(),
            person_table.clone(),
            attended_rel.clone(),
            assigned_rel.clone(),
        );
        async move {
            sync_person_graph(
                &ctx,
                &extractions,
                &person_table,
                &attended_rel,
                &assigned_rel,
            )
            .await
        }
    })
    .await
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let live = args.iter().any(|a| a == "--live" || a == "-L");
    let notesdir = args
        .iter()
        .find(|a| !a.starts_with('-'))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("notes"));

    let graph = Graph::connect(
        &env_or("SURREALDB_URL", "localhost:8000"),
        &env_or("SURREALDB_NS", "cocoindex"),
        &env_or("SURREALDB_DB", "meeting_notes"),
        &env_or("SURREALDB_USER", "root"),
        &env_or("SURREALDB_PASS", "root"),
    )
    .await?;
    let embedder = SentenceTransformerEmbedder::load(EMBED_MODEL).await?;

    let app = Environment::builder()
        .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
        .provide_key(&KG_DB, graph)
        .provide_key(&LLM_MODEL, env_or("LLM_MODEL", "gpt-5-mini"))
        .provide_key(
            &RESOLUTION_LLM_MODEL,
            env_or("RESOLUTION_LLM_MODEL", "gpt-5-mini"),
        )
        .provide_key(&EMBEDDER, embedder)
        .build()
        .await?
        .app(APP_NAME)
        .await?;

    let body = move |ctx: Ctx| app_main(ctx, notesdir);
    if live {
        println!("watching the notes folder (Ctrl-C to stop)…");
        let handle = app.start_update_with_options(
            UpdateOptions {
                live: true,
                ..UpdateOptions::default()
            },
            body,
        )?;
        handle.result().await?;
    } else {
        let stats = app.run(body).await?;
        println!("{stats}");
    }
    Ok(())
}
