//! A child that the update's component selector leaves out runs no user code:
//! `mount_each!` and `mount!` compute no component-memo key for it (the key is
//! `MemoInput::write_memo_key` over its arguments).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use cocoindex::memo::{MemoInput, MemoKeyWriter};
use cocoindex::prelude::*;
use cocoindex::{ComponentSelector, StablePath};

static KEYS: AtomicUsize = AtomicUsize::new(0);
static RUNS: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
struct Counted(String);

#[cocoindex::async_trait]
impl MemoInput for Counted {
    fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
        KEYS.fetch_add(1, Ordering::SeqCst);
        writer.write(&self.0)
    }

    fn has_memo_state(&self) -> bool {
        false
    }
}

#[cocoindex::function]
async fn leaf(_ctx: &Ctx, item: Counted) -> Result<()> {
    let _ = item;
    RUNS.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

async fn run(app: &App, opts: UpdateOptions) {
    app.update_with_options(opts, move |ctx| async move {
        let ctx = &ctx;
        let items: Vec<(String, Counted)> = (0..100)
            .map(|i| (format!("k{i}"), Counted(format!("v{i}"))))
            .collect();
        cocoindex::mount_each!("items", items, |item| leaf(ctx, item)).await?;
        for i in 0..20 {
            let key = format!("single{i}");
            cocoindex::mount!(key, leaf(ctx, Counted(format!("s{i}")))).await?;
        }
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn unselected_children_compute_no_memo_key() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder("selector_memo_key")
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap();
    run(&app, UpdateOptions::default()).await;
    KEYS.store(0, Ordering::SeqCst);
    RUNS.store(0, Ordering::SeqCst);

    let selected = StablePath::root()
        .concat_part(StableKey::Str(Arc::from("items")))
        .concat_part(StableKey::Str(Arc::from("k7")));
    run(
        &app,
        UpdateOptions {
            component_selector: Some(ComponentSelector::new([selected])),
            full_reprocess: true,
            ..UpdateOptions::default()
        },
    )
    .await;
    let runs = RUNS.load(Ordering::SeqCst);
    let keys = KEYS.load(Ordering::SeqCst);
    assert_eq!(runs, 1, "only the selected child runs");
    // Only the selected child computes its key.
    assert_eq!(
        keys, 1,
        "memo keys computed for {keys} children, though 119 are unselected"
    );
}
