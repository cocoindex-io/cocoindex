//! The local-files target beyond a flat directory: nested directory targets
//! (removed with their contents when no longer declared), a standalone file
//! target, `create_parent_dirs`, and a root under a `ContextKey` base directory
//! that can move between runs. Mirrors the Python `localfs` target tests.

use std::path::PathBuf;

use cocoindex::resources::fs::{self, DirTarget, FileOptions, FilePath};
use cocoindex::{App, Environment};

#[tokio::test]
async fn nested_dir_targets_are_created_and_removed_with_their_contents() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lmdb");
    let out = dir.path().join("out");

    // Run 1: a nested directory target with a file, and a plain file whose
    // parent needs creating.
    let app = App::builder("fs_nested")
        .db_path(&db)
        .build()
        .await
        .unwrap();
    let out1 = out.clone();
    app.update(move |ctx| async move {
        let t = DirTarget::mount(&ctx, &out1)?;
        let sub = t.declare_dir_target(&ctx, "sub", false)?;
        sub.declare_file(&ctx, "b.txt", b"BBB")?;
        let deep = sub.declare_dir_target(&ctx, "deep/er", true)?;
        deep.declare_file(&ctx, "c.txt", b"CCC")?;
        t.declare_file_with_options(
            &ctx,
            "made/a.txt",
            b"AAA",
            FileOptions {
                create_parent_dirs: true,
            },
        )?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(std::fs::read(out.join("sub/b.txt")).unwrap(), b"BBB");
    assert_eq!(
        std::fs::read(out.join("sub/deep/er/c.txt")).unwrap(),
        b"CCC"
    );
    assert_eq!(std::fs::read(out.join("made/a.txt")).unwrap(), b"AAA");
    // Something the user put there, inside a declared directory.
    std::fs::write(out.join("sub/user.txt"), b"mine").unwrap();
    drop(app);

    // Run 2: the nested target is gone → the directory and everything under
    // it is removed; the plain file stays.
    let app = App::builder("fs_nested")
        .db_path(&db)
        .build()
        .await
        .unwrap();
    let out2 = out.clone();
    app.update(move |ctx| async move {
        let t = DirTarget::mount(&ctx, &out2)?;
        t.declare_file(&ctx, "made/a.txt", b"AAA")?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(
        !out.join("sub").exists(),
        "an undeclared directory target is removed"
    );
    assert_eq!(std::fs::read(out.join("made/a.txt")).unwrap(), b"AAA");
    drop(app);

    // A file whose parent does not exist is an error without create_parent_dirs.
    let app = App::builder("fs_nested")
        .db_path(&db)
        .build()
        .await
        .unwrap();
    let out3 = out.clone();
    let result = app
        .update(move |ctx| async move {
            let t = DirTarget::mount(&ctx, &out3)?;
            t.declare_file(&ctx, "made/a.txt", b"AAA")?;
            t.declare_file(&ctx, "missing/x.txt", b"X")?;
            Ok(())
        })
        .await;
    assert!(result.is_err(), "a missing parent is an error: {result:?}");
    assert!(!out.join("missing/x.txt").exists());

    // Names must stay inside the directory.
    let out4 = out.clone();
    let result = app
        .update(move |ctx| async move {
            let t = DirTarget::mount(&ctx, &out4)?;
            t.declare_file(&ctx, "../escape.txt", b"X")?;
            Ok(())
        })
        .await;
    assert!(result.is_err());
    assert!(!dir.path().join("escape.txt").exists());
}

#[tokio::test]
async fn a_standalone_file_target_is_written_and_removed() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lmdb");
    let file = dir.path().join("out/hello.txt");

    let app = App::builder("fs_file").db_path(&db).build().await.unwrap();
    let path = file.clone();
    app.update(move |ctx| async move {
        fs::declare_file_with_options(
            &ctx,
            path,
            b"hello",
            FileOptions {
                create_parent_dirs: true,
            },
        )
    })
    .await
    .unwrap();
    assert_eq!(std::fs::read(&file).unwrap(), b"hello");
    drop(app);

    let app = App::builder("fs_file").db_path(&db).build().await.unwrap();
    app.update(move |_ctx| async move { Ok(()) }).await.unwrap();
    assert!(
        !file.exists(),
        "a file target no longer declared is removed"
    );
}

cocoindex::context_key!(static OUT_DIR: PathBuf);

#[tokio::test]
async fn a_root_under_a_context_dir_follows_the_directory_between_runs() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lmdb");
    let first = dir.path().join("first");
    let second = dir.path().join("second");

    let run = |base: PathBuf, names: Vec<&'static str>| {
        let db = db.clone();
        async move {
            let app = Environment::builder()
                .db_path(&db)
                .provide_key(&OUT_DIR, base)
                .build()
                .await
                .unwrap()
                .app("fs_context_dir")
                .await
                .unwrap();
            app.update(move |ctx| async move {
                let root = FilePath::under_context_dir(&ctx, &OUT_DIR, "out")?;
                let t = DirTarget::mount(&ctx, root)?;
                for name in names {
                    t.declare_file(&ctx, name, name.as_bytes())?;
                }
                Ok(())
            })
            .await
            .unwrap();
        }
    };

    run(first.clone(), vec!["a.txt", "b.txt"]).await;
    assert!(first.join("out/a.txt").exists());

    // The tree moves: the same key names the new base, so the orphaned b.txt is
    // removed there, where the tree now is, and a.txt is written there too.
    std::fs::rename(&first, &second).unwrap();
    run(second.clone(), vec!["a.txt"]).await;
    assert!(second.join("out/a.txt").exists());
    assert!(
        !second.join("out/b.txt").exists(),
        "an orphan is removed under the moved base directory"
    );
    assert!(!first.exists());
}
