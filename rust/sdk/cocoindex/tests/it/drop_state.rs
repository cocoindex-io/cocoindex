//! Target states declared through the SDK's built-in targets stay reachable
//! after the run that declared them: `App::drop_state` reverts them, and so
//! does a later run that no longer declares the target.

use cocoindex::prelude::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unmounted_dir_target_files_are_removed() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a");
    let b = dir.path().join("b");
    let app = App::builder("Unmounted")
        .db_path(dir.path().join("db"))
        .build()
        .await
        .unwrap();
    for target_dir in [a.clone(), b.clone()] {
        app.update(move |ctx| async move {
            let target = mount_dir_target(&ctx, target_dir)?;
            target.declare_file(&ctx, "x.txt", b"x")?;
            Ok(())
        })
        .await
        .unwrap();
    }
    assert_eq!(
        std::fs::read_dir(&a).unwrap().count(),
        0,
        "the second run no longer mounts `a`, so its file is removed"
    );
    assert!(b.join("x.txt").is_file());
}
