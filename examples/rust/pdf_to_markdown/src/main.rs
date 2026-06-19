//! PDF to Markdown — Rust port of the Python `pdf_to_markdown` example.
//!
//! Walk local PDF files, extract their text, and write one `.md` output per
//! PDF through a declarative `DirTarget`: files are written/updated, unchanged
//! files are skipped (memoized conversion), and outputs whose source PDF was
//! deleted are removed automatically.
//!
//!   cargo run -- [PDF_DIR] [OUT_DIR]   # defaults: ./pdf_files -> ./out
//!
//! Parity note: the Python example converts PDFs to Markdown with `docling`
//! (a heavy ML pipeline). There is no Rust equivalent, so this port extracts
//! plain text with `lopdf` — the same Rust-native PDF approach used by the
//! `paper_metadata`/`pdf_embedding` examples. Output naming (`<stem>.md`) and
//! the declarative directory target mirror Python.

use std::path::PathBuf;

use cocoindex::prelude::*;
use lopdf::Document;

/// Extract all text from a PDF (Rust-native stand-in for `docling` markdown).
fn pdf_to_text(content: &[u8]) -> Result<String> {
    let doc = Document::load_mem(content)
        .map_err(|e| Error::engine(format!("failed to parse PDF: {e}")))?;
    let pages: Vec<u32> = doc.get_pages().keys().copied().collect();
    if pages.is_empty() {
        return Ok(String::new());
    }
    doc.extract_text(&pages)
        .map_err(|e| Error::engine(format!("failed to extract PDF text: {e}")))
}

#[cocoindex::function]
async fn convert_pdf(_ctx: &Ctx, file: &FileEntry) -> Result<String> {
    let content = file.content()?;
    tokio::task::spawn_blocking(move || pdf_to_text(&content))
        .await
        .map_err(|e| Error::engine(format!("PDF parse task panicked: {e}")))?
}

fn parse_args() -> (PathBuf, PathBuf) {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let source_dir = args
        .first()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("pdf_files"));
    let output_dir = args
        .get(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("out"));
    (source_dir, output_dir)
}

/// Convert one PDF and declare its Markdown output. Mounted as a per-file
/// processing component — the component-memo fast-path skips unchanged PDFs.
#[cocoindex::function]
async fn process_pdf(ctx: &Ctx, file: FileEntry, target: DirTarget) -> Result<()> {
    let markdown = convert_pdf(ctx, &file).await?;
    let outname = format!("{}.md", file.stem());
    target.declare_file(ctx, &outname, markdown.as_bytes())?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let (source_dir, output_dir) = parse_args();

    let app = App::builder("PdfToMarkdown")
        .db_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cocoindex_db"))
        .build()
        .await?;
    let stats = app
        .run(move |ctx| {
            let source_dir = source_dir.clone();
            let output_dir = output_dir.clone();
            async move {
                let target = DirTarget::mount(&ctx, &output_dir)?;
                let files = cocoindex::fs::walk_items(&source_dir, &["**/*.pdf"])?;
                println!(
                    "converting {} PDF(s) from {}",
                    files.len(),
                    source_dir.display()
                );

                mount_each!(files, |file| process_pdf(ctx, file, target)).await?;
                Ok(())
            }
        })
        .await?;

    println!("{stats}");
    Ok(())
}
