//! Preview mode + stdout progress reporting (P1 parity with Python's
//! `App.update(preview=True)` / `update_blocking(report_to_stdout=True)`).
//!
//!   cargo test -p cocoindex --test preview

use std::sync::{Arc, Mutex};

use cocoindex::{
    App, PreviewAction, Result, StableKey, TargetAction, TargetActionSink, TargetHandler,
    TargetReconcileOutput, UpdateOptions, declare_target_state,
    register_root_target_states_provider,
};
use serde::{Deserialize, Serialize};

type Log = Arc<Mutex<Vec<String>>>;

async fn temp_app(name: &str) -> (App, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder(name)
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap();
    (app, dir)
}

fn key_str(key: &StableKey) -> String {
    match key {
        StableKey::Str(s) | StableKey::Symbol(s) => s.to_string(),
        StableKey::Int(i) => i.to_string(),
        other => format!("{other:?}"),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct RowAction {
    key: String,
    value: Option<String>,
}

/// A sink that records each applied action — used to prove that preview mode
/// never invokes the sink.
fn recording_sink(log: Log) -> TargetActionSink<RowAction> {
    TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<RowAction>>| {
        let log = log.clone();
        async move {
            let mut log = log.lock().unwrap();
            for action in actions {
                let (verb, row) = match action {
                    TargetAction::Create(r) => ("create", r),
                    TargetAction::Update(r) => ("update", r),
                    TargetAction::Delete(r) => ("delete", r),
                };
                log.push(match row.value {
                    Some(v) => format!("{verb} {}={}", row.key, v),
                    None => format!("{verb} {}", row.key),
                });
            }
            Ok(())
        }
    })
}

#[derive(Clone)]
struct RowHandler {
    sink: TargetActionSink<RowAction>,
}

impl TargetHandler<String> for RowHandler {
    type TrackingRecord = String;
    type Action = RowAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<String>,
        prev: Vec<String>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<RowAction, String>>> {
        let k = key_str(&key);
        match desired {
            Some(value) => {
                let unchanged =
                    !prev_may_be_missing && !prev.is_empty() && prev.iter().all(|p| *p == value);
                if unchanged {
                    return Ok(None);
                }
                let row = RowAction {
                    key: k,
                    value: Some(value.clone()),
                };
                let action = if prev.is_empty() {
                    TargetAction::Create(row)
                } else {
                    TargetAction::Update(row)
                };
                Ok(Some(TargetReconcileOutput {
                    action,
                    sink: self.sink.clone(),
                    tracking_record: Some(value),
                    child_invalidation: None,
                }))
            }
            None => {
                if prev.is_empty() && !prev_may_be_missing {
                    return Ok(None);
                }
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Delete(RowAction {
                        key: k,
                        value: None,
                    }),
                    sink: self.sink.clone(),
                    tracking_record: None,
                    child_invalidation: None,
                }))
            }
        }
    }
}

/// The app body: declare a flat set of rows.
async fn run_rows(
    app: &App,
    options: UpdateOptions,
    log: Log,
    rows: Vec<(&'static str, &'static str)>,
) -> Result<()> {
    app.update_with_options(options, move |ctx| {
        let log = log.clone();
        let rows = rows.clone();
        async move {
            let provider = register_root_target_states_provider(
                &ctx,
                "test/preview",
                RowHandler {
                    sink: recording_sink(log),
                },
            )?;
            for (k, v) in rows {
                declare_target_state(&ctx, provider.target_state(k, v.to_string()))?;
            }
            Ok(())
        }
    })
    .await
}

#[tokio::test]
async fn preview_collects_actions_without_applying() {
    let (app, _dir) = temp_app("preview_no_apply").await;
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    // Preview: compute the actions, but the sink must NOT run.
    let log_for_preview = log.clone();
    let actions = app
        .preview(move |ctx| {
            let log = log_for_preview.clone();
            async move {
                let provider = register_root_target_states_provider(
                    &ctx,
                    "test/preview",
                    RowHandler {
                        sink: recording_sink(log),
                    },
                )?;
                declare_target_state(&ctx, provider.target_state("a", "v1".to_string()))?;
                declare_target_state(&ctx, provider.target_state("b", "v1".to_string()))?;
                Ok::<(), cocoindex::Error>(())
            }
        })
        .await
        .unwrap();

    // Two planned Create actions, decodable to the concrete row type.
    assert_eq!(actions.len(), 2, "expected two planned actions");
    let mut decoded: Vec<String> = actions
        .iter()
        .map(|a| match a {
            PreviewAction::Create(v) => {
                let row: RowAction = v.decode().unwrap();
                format!("create {}={}", row.key, row.value.unwrap())
            }
            other => panic!("unexpected action: {other:?}"),
        })
        .collect();
    decoded.sort();
    assert_eq!(decoded, vec!["create a=v1", "create b=v1"]);

    // The sink never ran in preview mode.
    assert!(
        log.lock().unwrap().is_empty(),
        "preview must not apply actions, but sink recorded: {:?}",
        log.lock().unwrap()
    );

    // A subsequent real update DOES apply (proving preview didn't persist state).
    run_rows(
        &app,
        UpdateOptions::default(),
        log.clone(),
        vec![("a", "v1"), ("b", "v1")],
    )
    .await
    .unwrap();
    let mut applied = std::mem::take(&mut *log.lock().unwrap());
    applied.sort();
    assert_eq!(applied, vec!["create a=v1", "create b=v1"]);
}

#[tokio::test]
async fn report_to_stdout_runs_to_completion() {
    let (app, _dir) = temp_app("preview_report_stdout").await;
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    let options = UpdateOptions {
        report_to_stdout: true,
        ..UpdateOptions::default()
    };
    run_rows(&app, options, log.clone(), vec![("x", "1")])
        .await
        .unwrap();

    assert_eq!(*log.lock().unwrap(), vec!["create x=1".to_string()]);
}
