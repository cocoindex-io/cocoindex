//! Cross-process lock on updating (or dropping) one app of one database.
//!
//! Two processes that open the same database can each run an update of the
//! same app. LMDB keeps the store consistent, but both processes do all the
//! work and race on the targets. So an update or a drop holds an exclusive
//! advisory file lock (a POSIX record lock on Unix, `LockFileEx` on Windows)
//! on a file per app under `<db_path>/app_locks/`, and an update or drop of
//! the same app in another process fails at once with an error that names the
//! app. It does not wait: a live update holds the lock for its whole run, so
//! waiting could hang for good. A filesystem without file locks fails every
//! update and drop with an error that says so: running unlocked would drop the
//! guarantee silently.
//!
//! Within one process, every concurrent operation of the app shares the lock
//! (a catch-up update may run beside a live one, see
//! `specs/live_component/design.md`). Each operation holds one [`AppOpLock`]
//! for as long as any of its work runs (see
//! [`crate::engine::context::OperationWork`]); the lock is released when the
//! last of them ends. A preview writes nothing and takes no lock. Reads (`show`,
//! `ls`) take no lock either. The OS releases the lock when the process exits,
//! so a crashed process leaves no stale lock behind, and a child the process
//! forks (Python's `multiprocessing` on Linux) does not inherit it, as it
//! would an `flock` lock through the shared file description.
//!
//! A POSIX record lock belongs to the process, and closing any file
//! description of the file releases it, so a process opens an app's lock file
//! once: [`AppOpLockSlot::for_app`] hands out one slot per lock file from a
//! process-wide registry, whatever environment opened the database. (An
//! update started while a drop of the same app runs in the same process shares
//! its lock; refusing it is a separate question, see `AppContext::begin_update`.)
//!
//! A finished drop removes the app's lock file (on Unix; on Windows the file
//! stays). It removes it while it still holds the lock, and a process that
//! takes the lock checks afterwards that the file it locked is still the one
//! at the path, and opens the path again if not, so two processes never hold
//! two different files of one app.

use crate::prelude::*;

use cocoindex_utils::fingerprint::Fingerprint;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Weak;

/// One operation's hold on the app's lock; dropping it releases the hold, and
/// the last hold in the process releases the lock.
pub struct AppOpLock {
    slot: Arc<AppOpLockSlot>,
}

impl Drop for AppOpLock {
    fn drop(&mut self) {
        self.slot.release();
    }
}

/// The app's lock file and this process's holds on it.
pub struct AppOpLockSlot {
    app_name: String,
    db_path: PathBuf,
    path: PathBuf,
    /// The locked lock file, while this process holds the lock. Taking,
    /// sharing and releasing the lock all happen under this mutex, so an
    /// acquire never sees a hold that is released but whose file is still
    /// open.
    held: parking_lot::Mutex<Option<HeldLock>>,
}

struct HeldLock {
    file: File,
    /// The operations of this process that hold the lock.
    holders: usize,
    /// The app was dropped: the last release removes the lock file.
    remove_on_release: bool,
}

const LOCK_DIR: &str = "app_locks";

/// The lock file of `app_name` under `db_path`. App names are free-form, so the
/// file is named by a fingerprint of the name, after a sanitized prefix that
/// keeps it recognizable.
fn lock_file_path(db_path: &Path, app_name: &str) -> PathBuf {
    let prefix: String = app_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .take(64)
        .collect();
    let hash = Fingerprint::from_bytes(app_name.as_bytes()).to_string();
    let hex = hash.trim_start_matches('#');
    db_path.join(LOCK_DIR).join(format!("{prefix}.{hex}.lock"))
}

/// The process's slots, one per lock file, held weakly: a slot no app holds any
/// more goes, and a later `for_app` makes a new one.
static SLOTS: parking_lot::Mutex<Option<HashMap<PathBuf, Weak<AppOpLockSlot>>>> =
    parking_lot::Mutex::new(None);

impl AppOpLockSlot {
    /// The slot of the app `app_name` of the database at `db_path`: the one
    /// slot this process has for that lock file, however many environments or
    /// apps reach it.
    pub fn for_app(db_path: &Path, app_name: &str) -> Arc<Self> {
        let path = lock_file_path(db_path, app_name);
        let mut slots = SLOTS.lock();
        let slots = slots.get_or_insert_with(HashMap::new);
        if let Some(slot) = slots.get(&path).and_then(Weak::upgrade) {
            return slot;
        }
        slots.retain(|_, slot| slot.strong_count() > 0);
        let slot = Arc::new(Self::new(db_path, app_name));
        slots.insert(path, Arc::downgrade(&slot));
        slot
    }

    /// A slot of its own, not registered: for the tests' child process, which
    /// stands for another process.
    fn new(db_path: &Path, app_name: &str) -> Self {
        Self {
            app_name: app_name.to_owned(),
            db_path: db_path.to_owned(),
            path: lock_file_path(db_path, app_name),
            held: parking_lot::Mutex::new(None),
        }
    }

    /// Take the lock, or share this process's hold on it. Fails at once if
    /// another process holds it.
    pub fn acquire(self: &Arc<Self>) -> Result<AppOpLock> {
        let mut held = self.held.lock();
        match &mut *held {
            Some(held) => {
                held.holders += 1;
                // An operation of the app runs again: its lock file stays.
                held.remove_on_release = false;
            }
            None => {
                *held = Some(HeldLock {
                    file: self.lock_file()?,
                    holders: 1,
                    remove_on_release: false,
                })
            }
        }
        Ok(AppOpLock { slot: self.clone() })
    }

    /// Remove the lock file when this process's last hold ends, unless an
    /// operation of the app takes the lock again before that. For a finished
    /// drop: the app has nothing left to guard.
    pub fn remove_on_last_release(&self) {
        if let Some(held) = &mut *self.held.lock() {
            held.remove_on_release = true;
        }
    }

    fn release(&self) {
        let mut state = self.held.lock();
        let Some(held) = &mut *state else {
            error!(
                "app lock of '{}' released more often than taken",
                self.app_name
            );
            return;
        };
        held.holders -= 1;
        if held.holders > 0 {
            return;
        }
        let Some(HeldLock {
            file,
            remove_on_release,
            ..
        }) = state.take()
        else {
            return;
        };
        if remove_on_release {
            remove_lock_file(&self.path);
        }
        // Closing the file releases the lock, still under the mutex.
        drop(file);
    }

    /// Open the lock file and lock it.
    fn lock_file(&self) -> Result<File> {
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        loop {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&self.path)?;
            match try_lock_whole_file(&file) {
                Ok(true) => {}
                Ok(false) => {
                    // Best effort: Windows refuses the read while the holder's
                    // exclusive lock is on, so the pid is missing there.
                    let holder = read_holder_pid(&mut file)
                        .map(|pid| format!(" (pid {pid})"))
                        .unwrap_or_default();
                    client_bail!(
                        "app '{}' is being updated or dropped by another process{holder} \
                         using the database at '{}'; wait for that process to finish, \
                         or stop it, then try again",
                        self.app_name,
                        self.db_path.display()
                    );
                }
                Err(e) => return Err(lock_failed_error(&self.path, e)),
            }
            // A process that finished a drop removed the file after this open
            // and before this lock: no other process opens the file locked
            // here, so it guards nothing. Lock the file at the path instead.
            if !is_file_at(&file, &self.path)? {
                continue;
            }
            // Record the holder for the error message above; best effort.
            let _ = file
                .set_len(0)
                .and_then(|()| file.seek(SeekFrom::Start(0)))
                .and_then(|_| write!(file, "{}", std::process::id()))
                .and_then(|()| file.flush());
            return Ok(file);
        }
    }
}

/// Try to take the exclusive lock on the whole of `file` without waiting:
/// `Ok(true)` holds it, `Ok(false)` means another process does.
#[cfg(unix)]
fn try_lock_whole_file(file: &File) -> std::io::Result<bool> {
    use nix::fcntl::{FcntlArg, fcntl};
    use std::os::fd::AsFd;
    // A POSIX record lock (`fcntl`): a child this process forks does not
    // inherit it. An `flock` lock stays with the shared file description for
    // as long as the child keeps the file open, so a `multiprocessing` worker
    // forked during an update would hold the app's lock after the update.
    let mut lock: nix::libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = nix::libc::F_WRLCK as _;
    lock.l_whence = nix::libc::SEEK_SET as _;
    match fcntl(file.as_fd(), FcntlArg::F_SETLK(&lock)) {
        Ok(_) => Ok(true),
        Err(nix::errno::Errno::EAGAIN | nix::errno::Errno::EACCES) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

#[cfg(not(unix))]
fn try_lock_whole_file(file: &File) -> std::io::Result<bool> {
    use std::fs::TryLockError;
    match file.try_lock() {
        Ok(()) => Ok(true),
        Err(TryLockError::WouldBlock) => Ok(false),
        Err(TryLockError::Error(e)) => Err(e),
    }
}

/// The error when the lock file cannot be locked for a reason other than
/// another holder: usually a filesystem without file locks (some network and
/// FUSE filesystems). Every update and drop of the app would fail alike, and
/// only the user can move the database, so it is a client error.
fn lock_failed_error(path: &Path, err: std::io::Error) -> Error {
    client_error!(
        "cannot lock '{}' ({err}), which guards updates of the app against other \
         processes; the filesystem that holds the database may not support file \
         locks: put the database on a local filesystem",
        path.display()
    )
}

/// Whether `file` is the file at `path` now.
#[cfg(unix)]
fn is_file_at(file: &File, path: &Path) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let opened = file.metadata()?;
    match std::fs::metadata(path) {
        Ok(at_path) => Ok(at_path.dev() == opened.dev() && at_path.ino() == opened.ino()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Lock files are never removed off Unix, so the file opened is the one at
/// the path.
#[cfg(not(unix))]
fn is_file_at(_file: &File, _path: &Path) -> Result<bool> {
    Ok(true)
}

/// Remove a lock file this process holds the lock on; best effort.
#[cfg(unix)]
fn remove_lock_file(path: &Path) {
    if let Err(e) = std::fs::remove_file(path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        warn!(
            "could not remove the app lock file '{}': {e}",
            path.display()
        );
    }
}

/// Windows can't remove a file another process holds open, so the file stays.
#[cfg(not(unix))]
fn remove_lock_file(_path: &Path) {}

fn read_holder_pid(file: &mut File) -> Option<u32> {
    let mut content = String::new();
    file.seek(SeekFrom::Start(0)).ok()?;
    file.read_to_string(&mut content).ok()?;
    content.trim().parse().ok()
}

/// Another process's view of an app's lock, for tests: a child process (this
/// test binary, running [`other_process::lock_holder_child`]) tries to take
/// the lock, and holds it until told to release.
#[cfg(test)]
pub(crate) mod other_process {
    use super::*;
    use std::io::{BufRead, BufReader, Lines};
    use std::process::{Child, ChildStdout, Command, Stdio};

    const PATH_ENV: &str = "COCOINDEX_TEST_LOCK_HOLDER_DB_PATH";
    const APP_ENV: &str = "COCOINDEX_TEST_LOCK_HOLDER_APP";

    /// A hold on the lock from another process; dropping it releases the
    /// hold and ends the process.
    pub(crate) struct Hold {
        child: Child,
        _stdout: Lines<BufReader<ChildStdout>>,
    }

    impl Drop for Hold {
        fn drop(&mut self) {
            // Closing its stdin tells the child to release and exit.
            drop(self.child.stdin.take());
            let _ = self.child.wait();
        }
    }

    /// Try to take the app's lock from another process. `Err` is the refusal's
    /// message.
    pub(crate) fn try_acquire(db_path: &Path, app_name: &str) -> std::result::Result<Hold, String> {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "engine::app_lock::other_process::lock_holder_child",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(PATH_ENV, db_path)
            .env(APP_ENV, app_name)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut stdout = BufReader::new(child.stdout.take().unwrap()).lines();
        // The test harness prints lines of its own, and starts the test's
        // line ("test ... "), so the child's answer is marked, not at the
        // line's start. Reading until the answer, never to EOF: the child
        // holds until its stdin closes.
        const MARK: &str = "lock-holder: ";
        loop {
            let line = stdout
                .next()
                .expect("the lock holder child answers")
                .unwrap();
            let Some(at) = line.find(MARK) else {
                continue;
            };
            let answer = &line[at + MARK.len()..];
            if answer == "locked" {
                return Ok(Hold {
                    child,
                    _stdout: stdout,
                });
            }
            let message = answer
                .strip_prefix("refused: ")
                .unwrap_or(answer)
                .to_owned();
            drop(child.stdin.take());
            let _ = child.wait();
            return Err(message);
        }
    }

    /// The child's body: a no-op unless [`try_acquire`] started this process.
    #[test]
    #[ignore = "the body of the lock tests' child process"]
    fn lock_holder_child() {
        let (Ok(db_path), Ok(app_name)) = (std::env::var(PATH_ENV), std::env::var(APP_ENV)) else {
            return;
        };
        let slot = Arc::new(AppOpLockSlot::new(Path::new(&db_path), &app_name));
        match slot.acquire() {
            Ok(hold) => {
                println!("lock-holder: locked");
                // Hold until the parent closes this process's stdin.
                let mut rest = String::new();
                let _ = std::io::stdin().read_to_string(&mut rest);
                drop(hold);
            }
            Err(err) => println!("lock-holder: refused: {err}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(dir: &Path, app_name: &str) -> Arc<AppOpLockSlot> {
        Arc::new(AppOpLockSlot::new(dir, app_name))
    }

    #[test]
    fn lock_file_names_are_distinct_and_safe() {
        let dir = Path::new("/db");
        let a = lock_file_path(dir, "my/app");
        let b = lock_file_path(dir, "my_app");
        assert_ne!(a, b);
        assert_eq!(a.parent().unwrap(), dir.join(LOCK_DIR));
        let name = a.file_name().unwrap().to_str().unwrap();
        assert!(name.starts_with("my_app."), "{name}");
        let long = lock_file_path(dir, &"x".repeat(10_000));
        assert!(long.file_name().unwrap().len() < 128);
    }

    #[test]
    fn a_held_lock_is_shared_in_process_and_refused_to_another_process() {
        let dir = tempfile::tempdir().unwrap();
        let slot = slot(dir.path(), "app");
        let first = slot.acquire().unwrap();
        // A second operation in this process shares the hold.
        let second = slot.acquire().unwrap();

        // Another process is refused, and the error names the app and the
        // holder.
        let err = other_process::try_acquire(dir.path(), "app").err().unwrap();
        assert!(err.contains("app 'app'"), "{err}");
        #[cfg(unix)]
        assert!(
            err.contains(&format!("pid {}", std::process::id())),
            "{err}"
        );
        // A different app is not affected.
        drop(other_process::try_acquire(dir.path(), "other_app").unwrap());

        // Released when the last hold in the process ends.
        drop(first);
        assert!(other_process::try_acquire(dir.path(), "app").is_err());
        drop(second);
        let held_elsewhere = other_process::try_acquire(dir.path(), "app").unwrap();
        // And the other way round: a holder elsewhere refuses this process.
        let err = slot.acquire().err().unwrap();
        assert!(err.client_kind().is_some(), "{err}");
        drop(held_elsewhere);
        slot.acquire().unwrap();
    }

    /// Operations of one process that start and end at the same time never
    /// see their own process as another holder: a hold ends (and its file
    /// closes) under the same mutex an acquire takes.
    #[test]
    fn concurrent_holders_in_one_process_are_never_refused() {
        let dir = tempfile::tempdir().unwrap();
        let slot = slot(dir.path(), "app");
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let slot = slot.clone();
                std::thread::spawn(move || {
                    for _ in 0..1_000 {
                        if let Err(err) = slot.acquire() {
                            return Err(err.to_string());
                        }
                    }
                    Ok(())
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap().unwrap();
        }
    }

    #[test]
    fn a_filesystem_without_locks_is_a_client_error_naming_the_lock_file() {
        let path = Path::new("/db/app_locks/app.lock");
        let err = lock_failed_error(path, std::io::Error::from(std::io::ErrorKind::Unsupported));
        assert!(err.client_kind().is_some(), "{err}");
        let message = err.to_string();
        assert!(message.contains("/db/app_locks/app.lock"), "{message}");
        assert!(message.contains("not support file locks"), "{message}");
    }

    /// A finished drop removes the lock file once the last hold ends, unless
    /// an operation took the lock again before that.
    #[cfg(unix)]
    #[test]
    fn the_last_release_after_a_drop_removes_the_lock_file() {
        let dir = tempfile::tempdir().unwrap();
        let slot = slot(dir.path(), "app");
        let drop_hold = slot.acquire().unwrap();
        let other_hold = slot.acquire().unwrap();
        slot.remove_on_last_release();
        drop(drop_hold);
        assert!(slot.path.exists(), "another hold remains");
        drop(other_hold);
        assert!(!slot.path.exists());

        // An operation that starts again keeps the file.
        let drop_hold = slot.acquire().unwrap();
        slot.remove_on_last_release();
        let update_hold = slot.acquire().unwrap();
        drop(drop_hold);
        drop(update_hold);
        assert!(slot.path.exists());
    }

    /// A process that opened the lock file before a drop removed it locks a
    /// file nobody else opens any more: it notices and locks the file at the
    /// path, so it never holds the lock beside a third process.
    #[cfg(unix)]
    #[test]
    fn a_lock_on_a_removed_file_is_not_taken_for_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        let dropping = slot(dir.path(), "app");
        let hold = dropping.acquire().unwrap();
        // Opened for writing: a write lock needs that.
        let opened_early = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&dropping.path)
            .unwrap();
        dropping.remove_on_last_release();
        drop(hold);
        // The early opener's lock succeeds, on the removed file, which is not
        // the file at the path any more.
        assert!(try_lock_whole_file(&opened_early).unwrap());
        assert!(!is_file_at(&opened_early, &dropping.path).unwrap());

        // A process that takes the lock creates and locks the file at the
        // path, so a later one is refused.
        let _hold = dropping.acquire().unwrap();
        assert!(is_file_at(&dropping.held.lock().as_ref().unwrap().file, &dropping.path).unwrap());
        assert!(other_process::try_acquire(dir.path(), "app").is_err());
    }
}
