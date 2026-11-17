//! Cross-process lock on updating (or dropping) one app of one database.
//!
//! Two processes that open the same database can each run an update of the
//! same app. LMDB keeps the store consistent, but both processes do all the
//! work and race on the targets. So an update or a drop holds an exclusive
//! advisory file lock (`flock` on Unix, `LockFileEx` on Windows) on a file per
//! app under `<db_path>/app_locks/`, and an update or drop of the same app in
//! another process fails at once with an error that names the app. It does not
//! wait: a live update holds the lock for its whole run, so waiting could hang
//! for good.
//!
//! Within one process, every concurrent operation of the app shares the lock
//! (a catch-up update may run beside a live one, see
//! `specs/live_component/design.md`); it is released when the last of them
//! ends. A preview writes nothing and takes no lock. Reads (`show`, `ls`) take
//! no lock either. The OS releases the lock when the process exits, so a
//! crashed process leaves no stale lock behind.

use crate::prelude::*;

use cocoindex_utils::fingerprint::Fingerprint;
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Weak;

/// Held while an update or drop of the app runs in this process; dropping the
/// last clone releases the lock.
pub(crate) struct AppUpdateLock {
    _file: File,
}

/// The app's lock file and this process's hold on it, if any.
pub(crate) struct AppUpdateLockSlot {
    app_name: String,
    db_path: PathBuf,
    path: PathBuf,
    held: Mutex<Weak<AppUpdateLock>>,
}

const LOCK_DIR: &str = "app_locks";

/// The lock file of `app_name` under `db_path`. App names are free-form, so the
/// file is named by a fingerprint of the name, after a sanitized prefix that
/// keeps it recognizable.
pub(crate) fn lock_file_path(db_path: &Path, app_name: &str) -> PathBuf {
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
    let hash = Fingerprint::from_bytes(app_name.as_bytes());
    let hex: String = hash.0.iter().map(|b| format!("{b:02x}")).collect();
    db_path.join(LOCK_DIR).join(format!("{prefix}.{hex}.lock"))
}

impl AppUpdateLockSlot {
    pub(crate) fn new(db_path: &Path, app_name: &str) -> Self {
        Self {
            app_name: app_name.to_owned(),
            db_path: db_path.to_owned(),
            path: lock_file_path(db_path, app_name),
            held: Mutex::new(Weak::new()),
        }
    }

    /// Take the lock, or share this process's hold on it. Fails at once if
    /// another process holds it.
    pub(crate) fn acquire(&self) -> Result<Arc<AppUpdateLock>> {
        let mut held = self.held.lock().unwrap();
        if let Some(lock) = held.upgrade() {
            return Ok(lock);
        }
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.path)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                let holder = read_holder_pid(&mut file)
                    .map(|pid| format!(" (pid {pid})"))
                    .unwrap_or_default();
                internal_bail!(
                    "app '{}' is being updated or dropped by another process{holder} \
                     using the database at '{}'; wait for that process to finish, \
                     or stop it, then try again",
                    self.app_name,
                    self.db_path.display()
                );
            }
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
        // Record the holder for the error message above; best effort.
        let _ = file
            .set_len(0)
            .and_then(|()| file.seek(SeekFrom::Start(0)))
            .and_then(|_| write!(file, "{}", std::process::id()))
            .and_then(|()| file.flush());
        let lock = Arc::new(AppUpdateLock { _file: file });
        *held = Arc::downgrade(&lock);
        Ok(lock)
    }
}

fn read_holder_pid(file: &mut File) -> Option<u32> {
    let mut content = String::new();
    file.seek(SeekFrom::Start(0)).ok()?;
    file.read_to_string(&mut content).ok()?;
    content.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn a_held_lock_is_shared_in_process_and_refused_to_another_holder() {
        let dir = tempfile::tempdir().unwrap();
        let slot = AppUpdateLockSlot::new(dir.path(), "app");
        let first = slot.acquire().unwrap();
        // A second operation in this process shares the hold.
        let second = slot.acquire().unwrap();
        assert!(Arc::ptr_eq(&first, &second));

        // Another holder (here, a second slot with its own file description,
        // which `flock` treats like another process) is refused, and the
        // error names the app and the holder.
        let other = AppUpdateLockSlot::new(dir.path(), "app");
        let err = other.acquire().err().unwrap().to_string();
        assert!(err.contains("app 'app'"), "{err}");
        assert!(
            err.contains(&format!("pid {}", std::process::id())),
            "{err}"
        );
        // A different app is not affected.
        AppUpdateLockSlot::new(dir.path(), "other_app")
            .acquire()
            .unwrap();

        // Released when the last hold in the process ends.
        drop(first);
        assert!(other.acquire().is_err());
        drop(second);
        other.acquire().unwrap();
    }
}
