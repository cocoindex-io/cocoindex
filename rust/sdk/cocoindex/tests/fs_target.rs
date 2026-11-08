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

/// One `update` of the app `name` over `db`.
async fn update_once<F, Fut>(db: &std::path::Path, name: &str, f: F) -> cocoindex::Result<()>
where
    F: FnOnce(cocoindex::Ctx) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = cocoindex::Result<()>> + Send + 'static,
{
    let app = App::builder(name).db_path(db).build().await?;
    app.update(f).await
}

/// An entry declared as a directory where a previous run declared a file (and
/// the other way round) replaces it, along with what was under the directory.
#[tokio::test]
async fn a_file_and_a_directory_replace_each_other_at_one_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lmdb");
    let out = dir.path().join("out");

    let o = out.clone();
    update_once(&db, "fs_kind", move |ctx| async move {
        DirTarget::mount(&ctx, &o)?.declare_file(&ctx, "x", b"file")
    })
    .await
    .unwrap();
    assert!(out.join("x").is_file());

    let o = out.clone();
    update_once(&db, "fs_kind", move |ctx| async move {
        let t = DirTarget::mount(&ctx, &o)?;
        t.declare_dir_target(&ctx, "x", false)?
            .declare_file(&ctx, "inner.txt", b"inner")
    })
    .await
    .unwrap();
    assert_eq!(std::fs::read(out.join("x/inner.txt")).unwrap(), b"inner");

    let o = out.clone();
    update_once(&db, "fs_kind", move |ctx| async move {
        DirTarget::mount(&ctx, &o)?.declare_file(&ctx, "x", b"file again")
    })
    .await
    .unwrap();
    assert_eq!(std::fs::read(out.join("x")).unwrap(), b"file again");

    // Something CocoIndex didn't create is never replaced: a directory the
    // user made where a new file is declared fails the run and stays.
    std::fs::create_dir(out.join("y")).unwrap();
    std::fs::write(out.join("y/user.txt"), b"mine").unwrap();
    let o = out.clone();
    let result = update_once(&db, "fs_kind", move |ctx| async move {
        let t = DirTarget::mount(&ctx, &o)?;
        t.declare_file(&ctx, "x", b"file again")?;
        t.declare_file(&ctx, "y", b"clobber?")
    })
    .await;
    assert!(result.is_err());
    assert_eq!(std::fs::read(out.join("y/user.txt")).unwrap(), b"mine");
}

/// When a declared file moves from one name to another that names the same
/// file (here through a symlinked directory; on a case-insensitive file system,
/// a change of case), the removal of the old name doesn't remove the file just
/// written under the new one.
#[cfg(unix)]
#[tokio::test]
async fn a_file_moved_to_another_name_for_the_same_entry_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lmdb");
    let out = dir.path().join("out");
    std::fs::create_dir_all(out.join("real")).unwrap();
    std::os::unix::fs::symlink("real", out.join("link")).unwrap();

    let o = out.clone();
    update_once(&db, "fs_alias", move |ctx| async move {
        DirTarget::mount(&ctx, &o)?.declare_file(&ctx, "real/a.txt", b"v1")
    })
    .await
    .unwrap();
    let o = out.clone();
    update_once(&db, "fs_alias", move |ctx| async move {
        DirTarget::mount(&ctx, &o)?.declare_file(&ctx, "link/a.txt", b"v2")
    })
    .await
    .unwrap();
    assert_eq!(std::fs::read(out.join("real/a.txt")).unwrap(), b"v2");
}

/// Names that leave the directory, or name nothing in it, are rejected when
/// declared; nothing is written outside the directory.
#[tokio::test]
async fn names_that_leave_the_directory_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lmdb");
    let out = dir.path().join("out");
    for name in [
        "..",
        "../x",
        "a/../../x",
        "a/../b",
        "/etc/passwd",
        "",
        ".",
        "./",
        "sub/..",
    ] {
        let o = out.clone();
        let result = update_once(&db, "fs_names", move |ctx| async move {
            DirTarget::mount(&ctx, &o)?.declare_file(&ctx, name, b"x")
        })
        .await;
        assert!(result.is_err(), "{name:?} must be rejected");
        let o = out.clone();
        let result = update_once(&db, "fs_names", move |ctx| async move {
            DirTarget::mount(&ctx, &o)?.declare_dir_target(&ctx, name, true)?;
            Ok(())
        })
        .await;
        assert!(result.is_err(), "{name:?} must be rejected as a directory");
    }
    assert!(!dir.path().join("x").exists());
    let entries: Vec<_> = std::fs::read_dir(&out).unwrap().collect();
    assert!(entries.is_empty(), "{entries:?}");
}

/// Two names for one entry in one run (`a` and `./a`) are one key: the second
/// declaration fails the run instead of racing the first.
#[tokio::test]
async fn one_entry_declared_twice_fails_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let o = out.clone();
    let result = update_once(&dir.path().join("lmdb"), "fs_dup", move |ctx| async move {
        let t = DirTarget::mount(&ctx, &o)?;
        t.declare_file(&ctx, "a", b"one")?;
        t.declare_file(&ctx, "./a", b"two")
    })
    .await;
    let err = result.unwrap_err().to_string();
    assert!(err.contains("already declared"), "{err}");
}

/// Unicode names and names that differ only in case are distinct entries on a
/// case-sensitive file system; a name over the file system's limit fails the
/// run with the OS error.
#[tokio::test]
async fn unicode_case_and_overlong_names() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lmdb");
    let out = dir.path().join("out");
    let o = out.clone();
    update_once(&db, "fs_unicode", move |ctx| async move {
        let t = DirTarget::mount(&ctx, &o)?;
        t.declare_file(&ctx, "ünïcödé 🎉.txt", b"u")?;
        t.declare_file(&ctx, "Case.txt", b"upper")?;
        t.declare_file(&ctx, "with space & $ymbols;.txt", b"s")
    })
    .await
    .unwrap();
    assert_eq!(std::fs::read(out.join("ünïcödé 🎉.txt")).unwrap(), b"u");
    assert_eq!(std::fs::read(out.join("Case.txt")).unwrap(), b"upper");

    let o = out.clone();
    let long = "n".repeat(300);
    let result = update_once(&db, "fs_unicode", move |ctx| async move {
        DirTarget::mount(&ctx, &o)?.declare_file(&ctx, &long, b"x")
    })
    .await;
    assert!(result.is_err());
}

/// A file that can't be written (a read-only directory) fails the run; once
/// the directory is writable again, the next run writes it.
#[cfg(unix)]
#[tokio::test]
async fn a_read_only_directory_fails_the_run_and_the_next_run_repairs() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lmdb");
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::set_permissions(&out, std::fs::Permissions::from_mode(0o555)).unwrap();
    if std::fs::write(out.join("probe"), b"").is_ok() {
        eprintln!("skipping: running with permission to write a read-only directory (root?)");
        return;
    }
    let run = || {
        let o = out.clone();
        let db = db.clone();
        async move {
            update_once(&db, "fs_readonly", move |ctx| async move {
                DirTarget::mount(&ctx, &o)?.declare_file(&ctx, "a.txt", b"A")
            })
            .await
        }
    };
    let err = run().await.unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("permission"),
        "{err}"
    );
    std::fs::set_permissions(&out, std::fs::Permissions::from_mode(0o755)).unwrap();
    run().await.unwrap();
    assert_eq!(std::fs::read(out.join("a.txt")).unwrap(), b"A");
}

/// `drop_state` removes exactly what the app declared: its files and declared
/// directories (with what is under them, as in Python), not the user's files
/// next to them.
#[tokio::test]
async fn drop_state_removes_exactly_the_declared_entries() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lmdb");
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join("user.txt"), b"mine").unwrap();
    let app = App::builder("fs_drop").db_path(&db).build().await.unwrap();
    let o = out.clone();
    app.update(move |ctx| async move {
        let t = DirTarget::mount(&ctx, &o)?;
        t.declare_file(&ctx, "a.txt", b"A")?;
        t.declare_dir_target(&ctx, "sub", false)?
            .declare_file(&ctx, "b.txt", b"B")
    })
    .await
    .unwrap();
    app.drop_state().await.unwrap();
    let mut left: Vec<String> = std::fs::read_dir(&out)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    left.sort();
    assert_eq!(left, ["user.txt"]);
}

/// A file someone else changed or removed between runs is rewritten only when
/// its declared content changes (the target tracks what it wrote, not what is
/// on disk), as in Python.
#[tokio::test]
async fn a_file_changed_by_someone_else_is_rewritten_when_its_content_changes() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lmdb");
    let out = dir.path().join("out");
    let run = |content: &'static [u8]| {
        let o = out.clone();
        let db = db.clone();
        async move {
            update_once(&db, "fs_external", move |ctx| async move {
                DirTarget::mount(&ctx, &o)?.declare_file(&ctx, "a.txt", content)
            })
            .await
            .unwrap()
        }
    };
    run(b"v1").await;
    std::fs::remove_file(out.join("a.txt")).unwrap();
    run(b"v1").await;
    assert!(
        !out.join("a.txt").exists(),
        "unchanged content: not rewritten"
    );
    run(b"v2").await;
    assert_eq!(std::fs::read(out.join("a.txt")).unwrap(), b"v2");
}
