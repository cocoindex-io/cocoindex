//! Audio to Text — Rust port of the Python `audio_to_text` example.
//!
//! Pipeline: walk local audio files -> transcribe each with OpenAI Whisper ->
//! store one row per file in Postgres, keyed by filename. Re-running
//! incrementally processes added/changed/removed files: transcription is
//! memoized per file (size+mtime fingerprint), and rows whose source audio
//! disappeared are deleted by target-state reconciliation.
//!
//!   cargo run -- [AUDIO_DIR]    # default AUDIO_DIR = ./audio_files
//!
//! Parallels the Python example:
//!   - source        : `cocoindex::fs::walk` (cf. `localfs.walk_dir`)
//!   - per-file work : `#[cocoindex::function(memo)]` (cf. `@coco.fn(memo=True)`)
//!   - transcription : OpenAI `/v1/audio/transcriptions` (cf. `LiteLLMTranscriber("whisper-1")`)
//!   - target        : `postgres::TableTarget` (cf. `postgres.mount_table_target`)

use std::path::PathBuf;
use std::sync::LazyLock;

use cocoindex::postgres;
use cocoindex::prelude::*;
use cocoindex::walk;

const TABLE: &str = "audio_transcriptions";
const PG_SCHEMA: &str = "coco_examples";
const TRANSCRIBE_MODEL: &str = "whisper-1";

/// Common audio extensions, matching the Python example's pattern list.
const AUDIO_PATTERNS: &[&str] = &[
    "**/*.aac",
    "**/*.aiff",
    "**/*.flac",
    "**/*.m4a",
    "**/*.mp3",
    "**/*.ogg",
    "**/*.wav",
    "**/*.webm",
];

static DB: LazyLock<ContextKey<postgres::Database>> = LazyLock::new(|| {
    ContextKey::new_with_state("audio_to_text_db", |db: &postgres::Database| {
        db.state_id().to_string()
    })
});

#[derive(Clone, Serialize, Deserialize)]
struct AudioTranscription {
    filename: String,
    text: String,
}

fn transcription_schema() -> Result<postgres::TableSchema> {
    postgres::TableSchema::new(
        [
            ("filename", postgres::ColumnDef::new("text")),
            ("text", postgres::ColumnDef::new("text")),
        ],
        ["filename"],
    )
}

/// Transcribe one audio file with OpenAI Whisper. Memoized so the expensive
/// API call only runs when the file's content changes (or is first seen).
#[cocoindex::function(memo)]
async fn transcribe(_ctx: &Ctx, file: &FileEntry) -> Result<String> {
    let bytes = file.content()?;
    // Whisper sniffs the container from the upload filename's extension, so
    // pass a name that preserves it.
    let name = file
        .relative_path()
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("audio")
        .to_string();
    transcribe_audio(bytes, name).await
}

async fn transcribe_audio(bytes: Vec<u8>, filename: String) -> Result<String> {
    let api_key =
        std::env::var("OPENAI_API_KEY").map_err(|_| Error::engine("set OPENAI_API_KEY"))?;
    let base_url =
        std::env::var("OPENAI_BASE_URL").unwrap_or_else(|_| "https://api.openai.com/v1".into());

    let part = reqwest::multipart::Part::bytes(bytes).file_name(filename);
    let form = reqwest::multipart::Form::new()
        .text("model", TRANSCRIBE_MODEL)
        .part("file", part);

    let resp = reqwest::Client::new()
        .post(format!("{base_url}/audio/transcriptions"))
        .bearer_auth(&api_key)
        .multipart(form)
        .send()
        .await
        .map_err(|e| Error::engine(format!("transcription request failed: {e}")))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| Error::engine(format!("transcription response read failed: {e}")))?;
    if !status.is_success() {
        return Err(Error::engine(format!("Whisper HTTP {status}: {text}")));
    }
    let envelope: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| Error::engine(format!("transcription response not JSON: {e}: {text}")))?;
    envelope["text"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| Error::engine(format!("transcription response missing `text`: {text}")))
}

async fn app_main(ctx: Ctx, sourcedir: PathBuf) -> Result<()> {
    let db = ctx.get_key(&DB)?;
    let table =
        postgres::mount_table_target(&ctx, db, TABLE, transcription_schema()?, Some(PG_SCHEMA))
            .await?;

    let files: Vec<FileEntry> = walk(&sourcedir, AUDIO_PATTERNS)?;
    println!(
        "transcribing {} audio file(s) from {}",
        files.len(),
        sourcedir.display()
    );

    ctx.mount_each(files, |f| f.key(), {
        let table = table.clone();
        move |child, file| {
            let table = table.clone();
            async move {
                let text = transcribe(&child, &file).await?;
                table.declare_row(
                    &child,
                    &AudioTranscription {
                        filename: file.key(),
                        text,
                    },
                )?;
                Ok(())
            }
        }
    })
    .await?;

    Ok(())
}

fn database_url() -> String {
    std::env::var("POSTGRES_URL")
        .unwrap_or_else(|_| "postgres://cocoindex:cocoindex@localhost/cocoindex".to_string())
}

fn default_sourcedir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("audio_files")
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let dir = match args.first().map(String::as_str) {
        Some("index") => args.get(1).map(PathBuf::from),
        Some(other) => Some(PathBuf::from(other)),
        None => None,
    }
    .unwrap_or_else(default_sourcedir);

    let db = postgres::Database::connect(&database_url()).await?;
    let app = App::builder("AudioToText")
        .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
        .provide_key(&DB, db)
        .build()
        .await?;

    let stats = app.run(move |ctx| app_main(ctx, dir)).await?;
    println!("{stats}");
    Ok(())
}
