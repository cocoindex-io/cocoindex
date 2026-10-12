//! Fixture for `rust/cli/tests/e2e.rs`: two registered apps, a lifespan with a
//! shutdown hook and no db path (the test sets `COCOINDEX_DB`), and a `main`
//! body of its own. Inputs and outputs come from `FIXTURE_SRC` / `FIXTURE_OUT`.

use std::path::PathBuf;

use cocoindex::prelude::*;

fn env_path(name: &str) -> Result<PathBuf> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .ok_or_else(|| Error::engine(format!("{name} is not set")))
}

#[cocoindex::lifespan]
async fn lifespan(builder: EnvironmentBuilder) -> Result<EnvironmentBuilder> {
    Ok(builder.on_shutdown(|| async { eprintln!("fixture: shutdown") }))
}

#[cocoindex::function]
async fn upper(_ctx: &Ctx, file: FileEntry) -> Result<(String, String)> {
    Ok((file.key(), file.content_str()?.to_uppercase()))
}

/// Writes an upper-cased copy of every `*.txt` in `FIXTURE_SRC` to `FIXTURE_OUT`.
#[cocoindex::app(name = "FixtureApp")]
async fn fixture_app(ctx: Ctx) -> Result<()> {
    let target = mount_dir_target(&ctx, env_path("FIXTURE_OUT")?)?;
    let files = walk_items(env_path("FIXTURE_SRC")?, &["*.txt"])?;
    let count = files.len();
    let outputs = mount_each!(files, |file| upper(ctx, file)).await?;
    for (name, text) in &outputs {
        target.declare_file(&ctx, name, text.as_bytes())?;
    }
    println!("fixture: processed {count}");
    Ok(())
}

#[cocoindex::app]
async fn never_run(_ctx: Ctx) -> Result<()> {
    Ok(())
}

#[cocoindex::main]
async fn main() {
    println!("fixture: own main");
}
