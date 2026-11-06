//! `cocoindex::inspect` reads back what a run stored: the stable paths, the
//! details of each one and the tracked target states, from an `App` and from
//! an `Environment` plus the app's name.

use std::path::Path;

use cocoindex::inspect::{
    self, StablePath, StablePathDetail, StablePathNodeType, StablePathQuery, TargetStateEntry,
};
use cocoindex::prelude::*;
use futures::TryStreamExt;

const APP_NAME: &str = "InspectApp";

#[cocoindex::function]
async fn upper(_ctx: &Ctx, file: FileEntry) -> Result<(String, String)> {
    Ok((file.key(), file.content_str()?.to_uppercase()))
}

/// Run an app that mounts one component per `*.txt` file under `root/src` and
/// declares an upper-cased copy of each in `root/out`, owned by the root.
async fn run_app(root: &Path) -> (Environment, App) {
    let src = root.join("src");
    let out = root.join("out");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(src.join("a.txt"), "alpha").unwrap();
    std::fs::write(src.join("b.txt"), "beta").unwrap();
    let env = Environment::builder()
        .db_path(root.join("db"))
        .build()
        .await
        .unwrap();
    let app = env.app(APP_NAME).await.unwrap();
    app.update(move |ctx| async move {
        let target = mount_dir_target(&ctx, out)?;
        let files = walk_items(src, &["*.txt"])?;
        let outputs = mount_each!(files, |file| upper(ctx, file)).await?;
        for (name, text) in &outputs {
            target.declare_file(&ctx, name, text.as_bytes())?;
        }
        Ok(())
    })
    .await
    .unwrap();
    (env, app)
}

fn child(key: &str) -> StablePath {
    StablePath::root().concat_part(key.into_stable_key())
}

fn paths_of(details: &[StablePathDetail]) -> Vec<String> {
    details.iter().map(|d| d.path.to_string()).collect()
}

/// What `Debug` prints is every field, so equal strings mean equal records.
fn debug_all<T: std::fmt::Debug>(items: &[T]) -> Vec<String> {
    items.iter().map(|item| format!("{item:?}")).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stable_paths_of_a_run() {
    let dir = tempfile::tempdir().unwrap();
    let (env, app) = run_app(dir.path()).await;

    let paths: Vec<String> = inspect::list_stable_paths(&app)
        .await
        .unwrap()
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(paths, ["/", "/\"upper/a.txt\"", "/\"upper/b.txt\""]);

    let infos: Vec<_> = inspect::iter_stable_paths(&app)
        .await
        .try_collect()
        .await
        .unwrap();
    assert_eq!(infos.len(), 3);
    assert!(
        infos
            .iter()
            .all(|info| info.node_type == StablePathNodeType::Component)
    );

    let by_name: Vec<_> = inspect::iter_stable_paths_by_name(&env, APP_NAME)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(debug_all(&by_name), debug_all(&infos));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn details_of_stable_paths() {
    let dir = tempfile::tempdir().unwrap();
    let (env, app) = run_app(dir.path()).await;

    let details: Vec<StablePathDetail> = inspect::iter_stable_path_details(&app)
        .await
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        paths_of(&details),
        ["/", "/\"upper/a.txt\"", "/\"upper/b.txt\""]
    );

    // The root component owns both files of the directory target.
    let root = &details[0];
    assert_eq!(root.node_type, StablePathNodeType::Component);
    assert_eq!(root.processor_name, format!("app:{APP_NAME}"));
    assert_eq!(root.version, 1);
    assert!(!root.has_memoization);
    assert_eq!(root.target_state_count, 2);
    let mut files: Vec<&str> = root
        .target_state_items
        .iter()
        .map(|item| {
            assert!(
                item.target_state_path
                    .starts_with("/@cocoindex/localfs/file/"),
                "{}",
                item.target_state_path
            );
            assert!(item.fingerprint_path.starts_with("/#"));
            assert_eq!(item.states.len(), 1);
            match &item.key {
                StableKey::Array(parts) => match &parts[1] {
                    StableKey::Str(name) => &**name,
                    other => panic!("unexpected file key {other:?}"),
                },
                other => panic!("unexpected target state key {other:?}"),
            }
        })
        .collect();
    files.sort();
    assert_eq!(files, ["a.txt", "b.txt"]);

    // A mounted component: memoized, no target states of its own.
    let a = &details[1];
    assert_eq!(a.processor_name, "mount:upper");
    assert!(a.has_memoization);
    assert_eq!(a.target_state_count, 0);
    assert!(a.target_state_items.is_empty());

    // One path at a time reads the same records as the stream.
    let a_path = child("upper/a.txt");
    let one = inspect::get_stable_path_detail(&app, &a_path)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(format!("{one:?}"), format!("{a:?}"));
    let one_by_name = inspect::get_stable_path_detail_by_name(&env, APP_NAME, &a_path)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(format!("{one_by_name:?}"), format!("{a:?}"));

    let streamed_by_name: Vec<StablePathDetail> =
        inspect::iter_stable_path_details_by_name(&env, APP_NAME)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
    assert_eq!(debug_all(&streamed_by_name), debug_all(&details));

    // A path the app never stored reads as an empty directory node.
    let unknown = inspect::get_stable_path_detail(&app, &child("unknown"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unknown.node_type, StablePathNodeType::Directory);
    assert_eq!(unknown.version, 0);
    assert!(unknown.processor_name.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_adds_children_and_parents() {
    let dir = tempfile::tempdir().unwrap();
    let (env, app) = run_app(dir.path()).await;
    let root = StablePath::root();
    let a_path = child("upper/a.txt");

    let alone = inspect::query_stable_path_details(&app, &root, StablePathQuery::default())
        .await
        .unwrap();
    assert_eq!(paths_of(&alone), ["/"]);

    let with_children = StablePathQuery {
        include_children: true,
        ..Default::default()
    };
    let children = inspect::query_stable_path_details(&app, &root, with_children)
        .await
        .unwrap();
    assert_eq!(
        paths_of(&children),
        ["/", "/\"upper/a.txt\"", "/\"upper/b.txt\""]
    );
    // A leaf has no children to add.
    let leaf = inspect::query_stable_path_details(&app, &a_path, with_children)
        .await
        .unwrap();
    assert_eq!(paths_of(&leaf), ["/\"upper/a.txt\""]);

    let with_parents = StablePathQuery {
        include_parents: true,
        ..Default::default()
    };
    let parents = inspect::query_stable_path_details(&app, &a_path, with_parents)
        .await
        .unwrap();
    assert_eq!(paths_of(&parents), ["/", "/\"upper/a.txt\""]);
    assert_eq!(parents[0].target_state_count, 2);

    let by_name = inspect::query_stable_path_details_by_name(&env, APP_NAME, &a_path, with_parents)
        .await
        .unwrap();
    assert_eq!(debug_all(&by_name), debug_all(&parents));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn target_states_with_their_owner() {
    let dir = tempfile::tempdir().unwrap();
    let (env, app) = run_app(dir.path()).await;
    let out = dir.path().join("out");

    let entries: Vec<TargetStateEntry> = inspect::iter_target_states(&app)
        .await
        .try_collect()
        .await
        .unwrap();
    let mut readable: Vec<&str> = entries
        .iter()
        .map(|entry| {
            assert_eq!(entry.owner_component_path, StablePath::root());
            assert!(!entry.dangling);
            assert_eq!(entry.readable_segments.len(), 2);
            assert_eq!(entry.readable_segments[0], "@cocoindex/localfs/file");
            assert!(entry.fingerprint_path.starts_with("/#"));
            entry.readable_path.as_str()
        })
        .collect();
    readable.sort();
    // A readable path escapes its string keys (the `\` of a Windows path).
    let out_key = out.display().to_string().escape_default().to_string();
    let expected = |name: &str| format!("/@cocoindex/localfs/file/[\"{out_key}\",\"{name}\"]");
    assert_eq!(readable, [expected("a.txt"), expected("b.txt")]);

    let by_name: Vec<TargetStateEntry> = inspect::iter_target_states_by_name(&env, APP_NAME)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(debug_all(&by_name), debug_all(&entries));

    // A file the next run no longer declares leaves the tracked target states.
    std::fs::remove_file(dir.path().join("src/b.txt")).unwrap();
    let src = dir.path().join("src");
    let out_dir = out.clone();
    app.update(move |ctx| async move {
        let target = mount_dir_target(&ctx, out_dir)?;
        let outputs = mount_each!(walk_items(src, &["*.txt"])?, |file| upper(ctx, file)).await?;
        for (name, text) in &outputs {
            target.declare_file(&ctx, name, text.as_bytes())?;
        }
        Ok(())
    })
    .await
    .unwrap();
    let after: Vec<TargetStateEntry> = inspect::iter_target_states(&app)
        .await
        .try_collect()
        .await
        .unwrap();
    let readable: Vec<&str> = after.iter().map(|e| e.readable_path.as_str()).collect();
    assert_eq!(readable, [expected("a.txt")]);
    assert_eq!(
        inspect::list_stable_paths(&app).await.unwrap().len(),
        2,
        "the component of the removed file is gone too"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_app_without_stored_state_reads_as_empty() {
    let dir = tempfile::tempdir().unwrap();
    let (env, _app) = run_app(dir.path()).await;
    let name = "NeverRun";

    let paths: Vec<_> = inspect::iter_stable_paths_by_name(&env, name)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(paths.is_empty());
    let details: Vec<_> = inspect::iter_stable_path_details_by_name(&env, name)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(details.is_empty());
    let target_states: Vec<_> = inspect::iter_target_states_by_name(&env, name)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(target_states.is_empty());
    let root = StablePath::root();
    assert!(
        inspect::get_stable_path_detail_by_name(&env, name, &root)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        inspect::query_stable_path_details_by_name(&env, name, &root, StablePathQuery::default())
            .await
            .unwrap()
            .is_empty()
    );
}
