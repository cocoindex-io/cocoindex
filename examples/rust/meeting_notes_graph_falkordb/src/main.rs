use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use chrono::NaiveDate;
use cocoindex::falkordb::{self, ColumnDef, TableSchema};
use cocoindex::fs;
use cocoindex::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Meeting {
    id: i64,
    note_file: String,
    time: String,
    note: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Person {
    name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Task {
    description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AttendedRel {
    is_organizer: bool,
}

#[derive(Clone, Debug)]
struct ExtractedMeeting {
    time: NaiveDate,
    note: String,
    organizer: String,
    participants: Vec<String>,
    tasks: Vec<ExtractedTask>,
}

#[derive(Clone, Debug)]
struct ExtractedTask {
    description: String,
    assigned_to: Vec<String>,
}

#[derive(Clone, Debug)]
struct MeetingExtraction {
    meeting_id: i64,
    organizer: String,
    participants: Vec<String>,
    task_assignees: Vec<(String, Vec<String>)>,
}

fn schema(columns: &[(&str, &str)], primary_key: &str) -> Result<TableSchema> {
    TableSchema::new(
        columns
            .iter()
            .map(|(name, ty)| ((*name).to_string(), ColumnDef::new(*ty))),
        primary_key,
    )
}

fn split_meetings(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = Vec::new();
    for line in text.lines() {
        if line.starts_with("# ") || line.starts_with("## ") {
            if !current.is_empty() {
                out.push(current.join("\n").trim().to_string());
                current.clear();
            }
        }
        current.push(line.to_string());
    }
    if !current.is_empty() {
        out.push(current.join("\n").trim().to_string());
    }
    out.into_iter().filter(|s| !s.is_empty()).collect()
}

fn extract_meeting(section: &str) -> Result<ExtractedMeeting> {
    let mut lines = section.lines();
    let heading = lines
        .next()
        .ok_or_else(|| Error::engine("meeting section is empty"))?
        .trim_start_matches('#')
        .trim();
    let (_, date) = heading
        .rsplit_once(" — ")
        .or_else(|| heading.rsplit_once(" - "))
        .ok_or_else(|| Error::engine(format!("meeting heading lacks date: {heading}")))?;
    let time = NaiveDate::parse_from_str(date.trim(), "%Y-%m-%d")
        .map_err(|e| Error::engine(format!("invalid meeting date {date:?}: {e}")))?;

    let mut organizer = String::new();
    let mut participants = Vec::new();
    let mut note = String::new();
    let mut tasks = Vec::new();
    let mut in_tasks = false;

    for line in lines {
        let line = line.trim();
        if let Some(value) = line.strip_prefix("Organizer:") {
            organizer = value.trim().to_string();
            in_tasks = false;
        } else if let Some(value) = line.strip_prefix("Participants:") {
            participants = value
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToString::to_string)
                .collect();
            in_tasks = false;
        } else if let Some(value) = line.strip_prefix("Notes:") {
            note = value.trim().to_string();
            in_tasks = false;
        } else if line == "Tasks:" {
            in_tasks = true;
        } else if in_tasks && line.starts_with("- ") {
            let task = &line[2..];
            let (assignee, description) = task
                .split_once(':')
                .ok_or_else(|| Error::engine(format!("task lacks assignee: {task}")))?;
            tasks.push(ExtractedTask {
                description: description.trim().to_string(),
                assigned_to: vec![assignee.trim().to_string()],
            });
        }
    }

    if organizer.is_empty() {
        return Err(Error::engine(format!(
            "meeting section lacks organizer: {heading}"
        )));
    }

    Ok(ExtractedMeeting {
        time,
        note,
        organizer,
        participants,
        tasks,
    })
}

fn canonical_person(name: &str) -> String {
    match name.trim() {
        "Alice C." => "Alice Chen".to_string(),
        "Bob Lee" => "Robert Lee".to_string(),
        other => other.to_string(),
    }
}

async fn build_graph(ctx: &Ctx, graph: &falkordb::Graph, input_dir: &str) -> Result<()> {
    let meeting_table = falkordb::mount_table_target(
        ctx,
        graph,
        "Meeting",
        schema(
            &[
                ("id", "integer"),
                ("note_file", "string"),
                ("time", "string"),
                ("note", "string"),
            ],
            "id",
        )?,
    )
    .await?;
    let person_table =
        falkordb::mount_table_target(ctx, graph, "Person", schema(&[("name", "string")], "name")?)
            .await?;
    let task_table = falkordb::mount_table_target(
        ctx,
        graph,
        "Task",
        schema(&[("description", "string")], "description")?,
    )
    .await?;
    let attended_rel =
        falkordb::mount_relation_target(ctx, graph, "ATTENDED", &person_table, &meeting_table)
            .await?;
    let decided_rel =
        falkordb::mount_relation_target(ctx, graph, "DECIDED", &meeting_table, &task_table).await?;
    let assigned_rel =
        falkordb::mount_relation_target(ctx, graph, "ASSIGNED_TO", &person_table, &task_table)
            .await?;

    let files = fs::walk(input_dir, &["**/*.md"])?;
    let mut meetings = Vec::new();
    let mut canonical_people = BTreeSet::new();
    let mut id_gen = IdGenerator::new();

    for file in files {
        let note_file = file.relative_path().to_string_lossy().to_string();
        let text = file.content_str()?;
        for section in split_meetings(&text) {
            let extracted = extract_meeting(&section)?;
            let meeting_id = id_gen
                .next_id(ctx, &(note_file.clone(), extracted.time.to_string()))
                .await? as i64;
            meeting_table.declare_record(
                ctx,
                meeting_id,
                &Meeting {
                    id: meeting_id,
                    note_file: note_file.clone(),
                    time: extracted.time.to_string(),
                    note: extracted.note.clone(),
                },
            )?;

            for task in &extracted.tasks {
                task_table.declare_record(
                    ctx,
                    task.description.as_str(),
                    &Task {
                        description: task.description.clone(),
                    },
                )?;
                decided_rel.declare_relation(ctx, meeting_id, task.description.as_str())?;
            }

            for person in std::iter::once(&extracted.organizer).chain(extracted.participants.iter())
            {
                canonical_people.insert(canonical_person(person));
            }
            for task in &extracted.tasks {
                for assignee in &task.assigned_to {
                    canonical_people.insert(canonical_person(assignee));
                }
            }

            meetings.push(MeetingExtraction {
                meeting_id,
                organizer: extracted.organizer,
                participants: extracted.participants,
                task_assignees: extracted
                    .tasks
                    .into_iter()
                    .map(|t| (t.description, t.assigned_to))
                    .collect(),
            });
        }
    }

    for name in &canonical_people {
        person_table.declare_record(ctx, name.as_str(), &Person { name: name.clone() })?;
    }

    for meeting in meetings {
        let mut attendees: BTreeMap<String, bool> =
            BTreeMap::from([(canonical_person(&meeting.organizer), true)]);
        for participant in &meeting.participants {
            attendees
                .entry(canonical_person(participant))
                .or_insert(false);
        }
        for (person, is_organizer) in attendees {
            attended_rel.declare_relation_record(
                ctx,
                person.as_str(),
                meeting.meeting_id,
                &AttendedRel { is_organizer },
            )?;
        }
        for (task, assignees) in meeting.task_assignees {
            let mut seen = BTreeSet::new();
            for assignee in assignees {
                let person = canonical_person(&assignee);
                if seen.insert(person.clone()) {
                    assigned_rel.declare_relation(ctx, person.as_str(), task.as_str())?;
                }
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let input_dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| format!("{}/input", env!("CARGO_MANIFEST_DIR")));
    let uri =
        std::env::var("FALKORDB_URI").unwrap_or_else(|_| "falkor://localhost:6379".to_string());
    let graph_name =
        std::env::var("FALKORDB_GRAPH").unwrap_or_else(|_| "meeting_notes".to_string());
    let graph = falkordb::Graph::connect(&uri, &graph_name).await?;
    let app = App::builder("meeting_notes_graph_falkordb")
        .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
        .build()
        .await?;
    app.update(move |ctx| {
        let graph = graph.clone();
        let input_dir = input_dir.clone();
        async move { build_graph(&ctx, &graph, &input_dir).await }
    })
    .await?;
    Ok(())
}
