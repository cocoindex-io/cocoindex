//! Files Transform — Rust equivalent of the Python localfs markdown example.
//!
//! Walk markdown files in a source directory, memoize markdown-to-HTML
//! conversion per file, and write HTML outputs.

use cocoindex::prelude::*;
use pulldown_cmark::{Options, Parser, html};
use std::path::{Path, PathBuf};

#[cocoindex::function(memo)]
async fn render_markdown(_ctx: &Ctx, file: &FileEntry) -> Result<String> {
    let markdown = file.content_str()?;
    let mut options = Options::empty();
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_TABLES);
    options.insert(Options::ENABLE_TASKLISTS);

    let parser = Parser::new_ext(&markdown, options);
    let mut html_out = String::new();
    html::push_html(&mut html_out, parser);
    Ok(html_out)
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

fn ensure_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path).map_err(cocoindex::Error::Io)
}

#[tokio::main]
async fn main() -> Result<()> {
    let (source_dir, output_dir) = parse_args();
    ensure_dir(&output_dir)?;

    let app = cocoindex::App::open("files_transform", ".cocoindex_db")?;
    let stats = app
        .run(move |ctx| {
            let source_dir = source_dir.clone();
            let output_dir = output_dir.clone();
            async move {
                let files = cocoindex::fs::walk(&source_dir, &["*.md", "**/*.md"])?;

                ctx.mount_each(files, |file| file.key(), {
                    let output_dir = output_dir.clone();
                    move |file_ctx, file| {
                        let output_dir = output_dir.clone();
                        async move {
                            let html = render_markdown(&file_ctx, &file).await?;
                            let output_path = output_dir.join(output_name_for(&file));
                            file_ctx.write_file(output_path, html.as_bytes())?;
                            Ok(())
                        }
                    }
                })
                .await?;

                Ok(())
            }
        })
        .await?;

    println!("{stats}");
    Ok(())
}
