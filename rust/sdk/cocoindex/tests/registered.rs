//! `#[cocoindex::lifespan]` + `#[cocoindex::app]` registration end to end:
//! `Environment::from_registered` runs the lifespan, apps are found by name,
//! `on_shutdown` hooks run LIFO, and the `__coco` protocol child answers `ls`,
//! `update` (plain and `--preview`) and `drop` against the same binary.
//!
//! Known gap (pre-existing, not specific to the CLI): `App::drop_state` clears
//! the app's state but does not revert target states declared through root
//! providers registered during the run (`mount_dir_target`, connector tables).
//! The engine deletes a root target only through the environment's provider
//! registry (`execution.rs`, "skip deleting target states ... provider not
//! found"), and the Rust SDK registers root providers per run instead.
//! `drop_reverts_root_provider_targets` pins the intended behaviour.
//!
//! One test function: the registry is per binary and the lifespan reads its
//! db path from an environment variable, so scenarios run in sequence.

use std::process::ExitCode;
use std::sync::Mutex;

use cocoindex::cli::protocol::{Event, Line, Outcome, decode_line};
use cocoindex::prelude::*;

const DB_ENV: &str = "COCO_TEST_REGISTERED_DB";
const OUT_ENV: &str = "COCO_TEST_REGISTERED_OUT";

static SHUTDOWN_LOG: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

#[derive(Clone)]
struct Greeting(String);

#[cocoindex::lifespan]
async fn lifespan(builder: EnvironmentBuilder) -> Result<EnvironmentBuilder> {
    let db = std::env::var(DB_ENV).expect("the test sets the db path");
    Ok(builder
        .db_path(db)
        .provide(Greeting("hello".to_string()))
        .on_shutdown(|| async { SHUTDOWN_LOG.lock().unwrap().push("first") })
        .on_shutdown(|| async { SHUTDOWN_LOG.lock().unwrap().push("second") }))
}

/// Writes one file per greeting character into the directory named by
/// `COCO_TEST_REGISTERED_OUT`; returns the number of files.
#[cocoindex::app(name = "Named")]
async fn app_one(ctx: Ctx) -> Result<usize> {
    let greeting = ctx.get_or_err::<Greeting>()?.0.clone();
    let out = std::env::var(OUT_ENV).expect("the test sets the output dir");
    let target = mount_dir_target(&ctx, out)?;
    for (i, ch) in greeting.chars().enumerate() {
        target.declare_file(&ctx, &format!("{i}.txt"), ch.to_string().as_bytes())?;
    }
    Ok(greeting.len())
}

#[cocoindex::app]
async fn app_two(_ctx: Ctx) -> Result<()> {
    Ok(())
}

fn records(out: &[u8]) -> (Vec<Event>, Vec<String>) {
    let text = String::from_utf8(out.to_vec()).unwrap();
    let mut events = Vec::new();
    let mut passthrough = Vec::new();
    for line in text.lines() {
        match decode_line(line) {
            Line::Record { text, event } => {
                if !text.is_empty() {
                    passthrough.push(text.to_string());
                }
                events.push(event);
            }
            Line::Text(t) => passthrough.push(t.to_string()),
            Line::Malformed { raw, .. } => panic!("malformed record: {raw}"),
        }
    }
    (events, passthrough)
}

async fn child(args: &[&str]) -> (ExitCode, Vec<Event>, Vec<String>) {
    let mut out: Vec<u8> = Vec::new();
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    let code = cocoindex::cli::dispatch_to(args, &mut out).await;
    let (events, passthrough) = records(&out);
    assert!(
        matches!(events.first(), Some(Event::Hello { protocol: 1, .. })),
        "first record is hello: {events:?}"
    );
    (code, events, passthrough)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_lifespan_apps_and_protocol() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let out = dir.path().join("out");
    // SAFETY: this is the only test in the binary and it sets the variables
    // before any thread reads them.
    unsafe {
        std::env::set_var(DB_ENV, &db);
        std::env::set_var(OUT_ENV, &out);
        std::env::remove_var("COCOINDEX_DB");
    }

    // --- registry contents -------------------------------------------------
    let names: Vec<&str> = cocoindex::registry::apps().iter().map(|a| a.name).collect();
    assert_eq!(names.len(), 2);
    assert!(
        names.contains(&"Named") && names.contains(&"app_two"),
        "{names:?}"
    );
    assert_eq!(cocoindex::registry::lifespans().len(), 1);
    assert_eq!(cocoindex::registry::lifespans()[0].name, "lifespan");
    cocoindex::registry::check_unique_app_names().unwrap();

    // --- from_registered runs the lifespan ---------------------------------
    let env = Environment::from_registered().await.unwrap();
    assert_eq!(env.db_path(), db.as_path());
    let app = env.app("Named").await.unwrap();
    let n: usize = app.update(app_one).await.unwrap();
    assert_eq!(n, 5, "the provided Greeting is visible to the app");
    assert_eq!(std::fs::read_dir(&out).unwrap().count(), 5);
    // The registered shim runs the same function.
    let entry = cocoindex::registry::find_app("Named").unwrap();
    app.update(move |ctx| (entry.run)(ctx)).await.unwrap();
    env.shutdown().await;
    assert_eq!(*SHUTDOWN_LOG.lock().unwrap(), vec!["second", "first"]);
    env.shutdown().await;
    assert_eq!(SHUTDOWN_LOG.lock().unwrap().len(), 2, "hooks run once");

    // --- protocol: ls ------------------------------------------------------
    let (code, events, _) = child(&["ls", "--protocol", "1"]).await;
    assert_eq!(code, ExitCode::SUCCESS);
    let Some(Event::Apps { apps, db_path }) = events.get(1) else {
        panic!("expected apps record: {events:?}");
    };
    assert_eq!(db_path, &db.display().to_string());
    let summary: Vec<(&str, bool, bool)> = apps
        .iter()
        .map(|a| (a.name.as_str(), a.persisted, a.registered))
        .collect();
    assert_eq!(
        summary,
        vec![("Named", true, true), ("app_two", false, true)]
    );

    // --- protocol: update --preview (no changes after the run above) --------
    std::fs::remove_dir_all(&out).unwrap();
    let (code, events, passthrough) =
        child(&["update", "--protocol", "1", "--app", "Named", "--preview"]).await;
    assert_eq!(code, ExitCode::SUCCESS);
    assert!(matches!(events.get(1), Some(Event::Start { app, .. }) if app == "Named"));
    assert!(events.iter().any(|e| matches!(e, Event::Progress { .. })));
    assert!(matches!(
        events.last(),
        Some(Event::Done {
            outcome: Outcome::Completed,
            stats: None,
        })
    ));
    // The app's own `println!` goes to the process stdout here (in-process
    // child); passthrough over a real pipe is covered by rust/cli's e2e test.
    assert!(passthrough.is_empty(), "{passthrough:?}");
    // `mount_dir_target` creates the directory eagerly; preview writes no file.
    assert_eq!(
        std::fs::read_dir(&out).unwrap().count(),
        0,
        "preview applies nothing"
    );

    // --- protocol: update --full-reprocess writes the files again -----------
    let (code, events, _) = child(&[
        "update",
        "--protocol",
        "1",
        "--app",
        "Named",
        "--full-reprocess",
    ])
    .await;
    assert_eq!(code, ExitCode::SUCCESS);
    match events.last() {
        Some(Event::Done {
            outcome: Outcome::Completed,
            stats: Some(stats),
        }) => {
            assert!(stats.processed >= 1, "{stats:?}");
        }
        other => panic!("expected done with stats: {other:?}"),
    }
    assert_eq!(std::fs::read_dir(&out).unwrap().count(), 5);

    // --- protocol: errors are usage errors ----------------------------------
    let (code, events, _) = child(&["update", "--protocol", "1", "--app", "Nope"]).await;
    assert_eq!(code, ExitCode::from(2));
    assert!(matches!(events.last(), Some(Event::Error { message }) if message.contains("Nope")));
    let (code, _, _) = child(&["update", "--protocol", "1"]).await;
    assert_eq!(code, ExitCode::from(2), "two apps: the name is required");

    // --- protocol: drop ------------------------------------------------------
    let (code, events, _) = child(&["drop", "--protocol", "1", "--app", "app_two"]).await;
    assert_eq!(code, ExitCode::SUCCESS);
    assert!(
        matches!(
            events.last(),
            Some(Event::Done {
                outcome: Outcome::NothingToDrop,
                ..
            })
        ),
        "{events:?}"
    );
    let (code, events, _) = child(&["drop", "--protocol", "1", "--app", "Named"]).await;
    assert_eq!(code, ExitCode::SUCCESS);
    assert!(
        matches!(
            events.last(),
            Some(Event::Done {
                outcome: Outcome::Completed,
                ..
            })
        ),
        "{events:?}"
    );
    // Target files are not reverted yet: see `drop_reverts_root_provider_targets`.
    let (_, events, _) = child(&["ls", "--protocol", "1"]).await;
    let Some(Event::Apps { apps, .. }) = events.get(1) else {
        panic!()
    };
    assert!(apps.iter().all(|a| !a.persisted));
}

/// Intended behaviour of `drop`: target states are reverted. Ignored until the
/// Rust SDK makes root target-state providers reachable at drop time (see the
/// module docs).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Rust SDK gap: drop_state does not revert root-provider target states"]
async fn drop_reverts_root_provider_targets() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let app = App::builder("DropReverts")
        .db_path(dir.path().join("db"))
        .build()
        .await
        .unwrap();
    let target_dir = out.clone();
    app.update(move |ctx| async move {
        let target = mount_dir_target(&ctx, target_dir)?;
        target.declare_file(&ctx, "a.txt", b"a")?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(std::fs::read_dir(&out).unwrap().count(), 1);
    app.drop_state().await.unwrap();
    assert_eq!(
        std::fs::read_dir(&out).unwrap().count(),
        0,
        "drop reverts the file"
    );
}
