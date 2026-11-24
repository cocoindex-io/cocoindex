//! `#[cocoindex::function(memo)]` borrows its arguments for the memo probe
//! and moves them into the body only on a miss, and `MemoInput::has_memo_state`
//! lets a plain-data argument skip the state call.
//!
//!   cargo test -p cocoindex --test memo_clone

use std::sync::atomic::{AtomicUsize, Ordering};

use cocoindex::memo::{MemoInput, MemoKeyWriter, MemoStateDecision, MemoStateValue};
use cocoindex::prelude::*;
use cocoindex::resources::fs::FileEntry;

async fn temp_app(name: &str) -> (App, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder(name)
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap();
    (app, dir)
}

static PAYLOAD_CLONES: AtomicUsize = AtomicUsize::new(0);
static BY_VALUE_CALLS: AtomicUsize = AtomicUsize::new(0);
static BY_REF_CALLS: AtomicUsize = AtomicUsize::new(0);

/// A memo input whose clones are counted.
#[derive(cocoindex::MemoInput)]
struct Payload {
    text: String,
}

impl Clone for Payload {
    fn clone(&self) -> Self {
        PAYLOAD_CLONES.fetch_add(1, Ordering::SeqCst);
        Self {
            text: self.text.clone(),
        }
    }
}

/// A memo input that cannot be cloned at all.
#[derive(cocoindex::MemoInput)]
struct NotClone {
    n: u32,
}

#[cocoindex::function(memo)]
async fn by_value(_ctx: &Ctx, payload: Payload, extra: NotClone) -> Result<usize> {
    BY_VALUE_CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(payload.text.len() + extra.n as usize)
}

#[cocoindex::function(memo)]
async fn by_ref(_ctx: &Ctx, payload: &Payload, name: &str, items: &[u32]) -> Result<usize> {
    BY_REF_CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(payload.text.len() + name.len() + items.len())
}

/// Neither a miss nor a hit clones an argument: the key and the state probe
/// borrow it, and on a miss the function's own argument moves into the body.
#[tokio::test]
async fn memo_function_never_clones_its_arguments() {
    let (app, _dir) = temp_app("memo_no_clone").await;
    for expected_calls in [1, 1] {
        let (value, by_ref_value) = app
            .update(|ctx| async move {
                let value = by_value(
                    &ctx,
                    Payload {
                        text: "hello".to_string(),
                    },
                    NotClone { n: 2 },
                )
                .await?;
                let payload = Payload {
                    text: "hi".to_string(),
                };
                let by_ref_value = by_ref(&ctx, &payload, "name", &[1, 2, 3]).await?;
                Ok((value, by_ref_value))
            })
            .await
            .unwrap();
        assert_eq!(value, 7);
        assert_eq!(by_ref_value, 9);
        assert_eq!(BY_VALUE_CALLS.load(Ordering::SeqCst), expected_calls);
        assert_eq!(BY_REF_CALLS.load(Ordering::SeqCst), expected_calls);
        assert_eq!(PAYLOAD_CLONES.load(Ordering::SeqCst), 0);
    }
}

static STATE_CALLS: AtomicUsize = AtomicUsize::new(0);

/// A manual `MemoInput` that declares it has no state: its `memo_state` must
/// never be called.
struct Stateless;

#[cocoindex::async_trait]
impl MemoInput for Stateless {
    fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
        writer.write(&"stateless")
    }

    fn has_memo_state(&self) -> bool {
        false
    }

    async fn memo_state(
        &self,
        _previous: Option<&MemoStateValue>,
    ) -> Result<Option<MemoStateDecision>> {
        STATE_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
}

#[cocoindex::function(memo)]
async fn with_stateless(_ctx: &Ctx, _arg: Stateless, _plain: Vec<Payload>) -> Result<u32> {
    Ok(1)
}

#[derive(cocoindex::MemoInput)]
struct WithFile {
    label: String,
    file: FileEntry,
}

#[derive(cocoindex::MemoInput)]
enum Source {
    Plain(u32),
    File(WithFile),
}

#[tokio::test]
async fn stateless_inputs_skip_the_state_probe() {
    let (app, _dir) = temp_app("memo_stateless").await;
    for _ in 0..2 {
        app.update(|ctx| async move {
            with_stateless(
                &ctx,
                Stateless,
                vec![Payload {
                    text: "x".to_string(),
                }],
            )
            .await
        })
        .await
        .unwrap();
    }
    assert_eq!(STATE_CALLS.load(Ordering::SeqCst), 0);
}

/// Derived and composite inputs have state exactly when an element does:
/// plain data does not, a file (validated by mtime and content) does.
#[test]
fn has_memo_state_follows_the_elements() {
    assert!(!Payload { text: "a".into() }.has_memo_state());
    assert!(!(1u32, "s".to_string()).has_memo_state());
    assert!(!vec![NotClone { n: 1 }].has_memo_state());
    assert!(!Source::Plain(1).has_memo_state());
    assert!(!Option::<Payload>::None.has_memo_state());

    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.txt"), "a").unwrap();
    let file = walk(dir.path(), &["*.txt"])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert!(file.has_memo_state());
    assert!(
        Source::File(WithFile {
            label: "l".into(),
            file: file.clone(),
        })
        .has_memo_state()
    );
    assert!(vec![file].has_memo_state());
}

// ---------------------------------------------------------------------------
// A target handle as a memoized argument
// ---------------------------------------------------------------------------

static RENDER_CALLS: AtomicUsize = AtomicUsize::new(0);

/// A target handle is `Serialize` (its stable identity) but not `MemoInput`:
/// a memoized function keys it by its serialized value, as Python memoizes a
/// target on its key.
#[cocoindex::function(memo)]
async fn render(ctx: &Ctx, name: &str, target: &DirTarget) -> Result<()> {
    RENDER_CALLS.fetch_add(1, Ordering::SeqCst);
    target.declare_file(ctx, &format!("{name}.txt"), name.as_bytes())
}

#[tokio::test]
async fn memo_function_takes_a_target_handle() {
    let (app, dir) = temp_app("memo_target_arg").await;
    let run = |out: std::path::PathBuf| {
        let app = app.clone();
        async move {
            app.update(move |ctx| async move {
                let target = DirTarget::mount(&ctx, &out)?;
                render(&ctx, "a", &target).await
            })
            .await
            .unwrap()
        }
    };
    let out = dir.path().join("out");
    run(out.clone()).await;
    run(out.clone()).await;
    assert_eq!(RENDER_CALLS.load(Ordering::SeqCst), 1, "same target: a hit");
    assert_eq!(std::fs::read(out.join("a.txt")).unwrap(), b"a");

    // A different target is a different key.
    run(dir.path().join("other")).await;
    assert_eq!(RENDER_CALLS.load(Ordering::SeqCst), 2);
}
