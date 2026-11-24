//! Audio to Text — Rust port of the Python `audio_to_text` example.
//!
//! Pipeline: walk local audio files -> transcribe each with OpenAI Whisper ->
//! store one row per file in Postgres, keyed by filename. Re-running
//! incrementally processes added/changed/removed files: `ApiTranscriber`
//! memoizes the transcription per file (mtime + content fingerprint), and rows
//! whose source audio disappeared are deleted by target-state reconciliation.
//!
//!   cargo run -- [AUDIO_DIR]    # default AUDIO_DIR = ./audio_files
//!
//! Parallels the Python example:
//!   - source        : `cocoindex::resources::fs::walk` (cf. `localfs.walk_dir`)
//!   - transcription : `cocoindex::ops::api::ApiTranscriber::transcribe`, memoized
//!                     (cf. `LiteLLMTranscriber("whisper-1").transcribe`)
//!   - target        : `postgres::TableTarget` (cf. `postgres.mount_table_target`)

use std::path::PathBuf;

use cocoindex::connectors::postgres;
use cocoindex::ops::api::ApiTranscriber;
use cocoindex::prelude::*;

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

cocoindex::context_key!(static DB: postgres::Database);
cocoindex::context_key!(static TRANSCRIBER: ApiTranscriber);

#[derive(Clone, Serialize, Deserialize, SchemaFields)]
struct AudioTranscription {
    filename: String,
    text: String,
}

fn transcription_schema() -> Result<postgres::TableSchema> {
    postgres::TableSchema::from_row::<AudioTranscription>(["filename"])
}

/// Process one audio file: transcribe it and declare the output row. Mounted as
/// a per-file processing component — the component-memo fast-path skips files
/// whose content and this logic are unchanged, and `transcribe` itself is
/// memoized by the file's content, so the Whisper call is not repeated.
#[cocoindex::function]
async fn process_audio(ctx: &Ctx, file: FileEntry, table: postgres::TableTarget) -> Result<()> {
    let text = ctx.get_key(&TRANSCRIBER)?.transcribe(ctx, &file).await?;
    table.declare_row(
        ctx,
        &AudioTranscription {
            filename: file.key(),
            text,
        },
    )?;
    Ok(())
}

async fn app_main(ctx: Ctx, sourcedir: PathBuf) -> Result<()> {
    let table =
        postgres::mount_table_target(&ctx, &DB, TABLE, transcription_schema()?, Some(PG_SCHEMA))
            .await?;

    let files = walk_items(&sourcedir, AUDIO_PATTERNS)?;
    println!(
        "transcribing {} audio file(s) from {}",
        files.len(),
        sourcedir.display()
    );

    mount_each!(files, |file| process_audio(ctx, file, table)).await?;

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
    // `ApiTranscriber::new` defaults to the OpenAI base URL + `OPENAI_API_KEY`;
    // honor `OPENAI_BASE_URL` for OpenAI-compatible endpoints.
    let mut transcriber = ApiTranscriber::new(TRANSCRIBE_MODEL);
    if let Ok(base_url) = std::env::var("OPENAI_BASE_URL") {
        transcriber = transcriber.with_base_url(base_url);
    }
    let app = Environment::builder()
        .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
        .provide_key(&DB, db)
        .provide_key(&TRANSCRIBER, transcriber)
        .build()
        .await?
        .app("AudioToText")
        .await?;

    let stats = app.run(move |ctx| app_main(ctx, dir)).await?;
    println!("{stats}");
    Ok(())
}
