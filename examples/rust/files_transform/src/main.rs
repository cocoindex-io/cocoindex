//! Files Transform — Rust equivalent of the Python localfs markdown example.
//!
//! Walk markdown files in a source directory, memoize markdown-to-HTML
//! conversion per file, and sync HTML outputs via a declarative `DirTarget`:
//! files are written/updated, unchanged files are skipped, and outputs whose
//! source markdown was deleted are removed automatically (like Python's
//! `localfs` directory target).

use cocoindex::prelude::*;
use pulldown_cmark::{Options, Parser, html};
use std::path::PathBuf;

#[cocoindex::function]
async fn render_markdown(_ctx: &Ctx, file: &FileEntry) -> Result<String> {
    let markdown = file.content_str()?;
    // GFM-leaning options, to track the Python example's MarkdownIt("gfm-like").
    // Note: exact HTML parity isn't a goal — pulldown-cmark and markdown-it-py
    // are different engines. The one notable gfm-like feature pulldown-cmark
    // does NOT support is bare-URL "linkify" (autolinking `https://...`); only
    // angle-bracket `<https://...>` autolinks are rendered.
    let mut options = Options::empty();
    options.insert(Options::ENABLE_GFM);
    options.insert(Options::ENABLE_TABLES);
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_TASKLISTS);

    let parser = Parser::new_ext(&markdown, options);
    let mut html_out = String::new();
    html::push_html(&mut html_out, parser);
    Ok(html_out)
}

/// Process one markdown file: render it and declare the HTML output.
///
/// Mounted as a processing component per file via `mount_each!`. As a component
/// entry it carries the component-memo fast-path — when this logic and the
/// file's content (and the target) are unchanged, the engine skips the whole
/// component and replays the previous output, so unchanged files aren't
/// re-rendered or re-written. `render_markdown` is logic-tracked, so editing it
/// invalidates the cache.
#[cocoindex::function]
async fn process_file(ctx: &Ctx, file: FileEntry, target: DirTarget) -> Result<()> {
    let html = render_markdown(ctx, &file).await?;
    target.declare_file(ctx, &output_name_for(&file), html.as_bytes())?;
    Ok(())
}

fn output_name_for(file: &FileEntry) -> String {
    let mut name = file
        .relative_path()
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("__");
    name.push_str(".html");
    name
}

fn parse_args() -> (PathBuf, PathBuf) {
    let args: Vec<String> = std::env::args().collect();
    let source_dir = PathBuf::from(
        args.get(1)
            .map(String::as_str)
            .unwrap_or("../../../../examples/files_transform/data"),
    );
    let output_dir = PathBuf::from(args.get(2).map(String::as_str).unwrap_or("./output_html"));
    (source_dir, output_dir)
}

#[tokio::main]
async fn main() -> Result<()> {
    let (source_dir, output_dir) = parse_args();

    let app = cocoindex::App::open("files_transform", ".cocoindex_db").await?;
    let stats = app
        .run(move |ctx| {
            let source_dir = source_dir.clone();
            let output_dir = output_dir.clone();
            async move {
                // Declarative output: the engine reconciles these files against
                // the previous run and deletes outputs whose source disappeared.
                let target = DirTarget::mount(&ctx, &output_dir)?;
                // Recursive `**/*.md`, matching the Python example's
                // `PatternFilePathMatcher(included_patterns=["**/*.md"])`, as
                // `(key, file)` pairs for `mount_each!`. (Output names join the
                // relative-path components with `__`, so nested files don't
                // collide — see `output_name_for`.)
                let files = cocoindex::resources::fs::walk_items(&source_dir, &["**/*.md"])?;

                // One processing component per file. `ctx` is the parent; the
                // macro substitutes each child scope's `&Ctx`, and fingerprints
                // `(file, target)` for the per-file component-memo fast-path.
                mount_each!(files, |file| process_file(ctx, file, target)).await?;

                Ok(())
            }
        })
        .await?;

    println!("{stats}");
    Ok(())
}
