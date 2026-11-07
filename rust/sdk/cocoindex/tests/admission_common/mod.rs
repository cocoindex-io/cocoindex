//! Shared by `admission.rs` and `admission_env.rs`.

use std::sync::Arc;
use std::time::Duration;

use cocoindex::App;
use tokio::sync::{Semaphore, watch};

/// How far the children of [`assert_admits`] have got.
#[derive(Clone, Copy, Debug, Default)]
struct Progress {
    /// Children the parent has taken from its source. `mount_each` takes the
    /// next one only once the previous one is admitted.
    taken: usize,
    /// Children whose body has started.
    started: usize,
    /// Children whose body has passed the gate.
    finished: usize,
    /// The most children in their body at any one time
    /// (`started - finished`).
    peak_running: usize,
}

/// A bound on the wait for `limit` children to run at once: it only turns a
/// limit that is too small from a hang into a failure, so it is generous.
const LIMIT_WAIT: Duration = Duration::from_secs(120);

/// Mount `children` background components, more than `limit`, that block
/// until released, and assert that exactly `limit` of them run at once.
///
/// The children are released once `limit` of them run and the parent has
/// taken one more, whose mount then waits for a token. Both are events, so a
/// stalled machine delays them but cannot make the test see a lower limit.
/// While no child is released, a correct pool admits none past the limit:
/// one more shows as a child taken or started too many at that point, and
/// later as a peak above `limit`.
pub async fn assert_admits(app: &App, limit: usize, children: usize) {
    assert!(children > limit, "the children must outnumber the limit");
    let (progress, mut observed) = watch::channel(Progress::default());
    let gate = Arc::new(Semaphore::new(0));

    let observer = tokio::spawn({
        let gate = gate.clone();
        async move {
            let at_limit = tokio::time::timeout(LIMIT_WAIT, async {
                observed
                    .wait_for(|p| p.started >= limit && p.taken > limit)
                    .await
                    .map(|p| *p)
            })
            .await;
            let at_limit = match at_limit {
                Ok(at_limit) => Ok(at_limit.unwrap()),
                Err(_) => Err(*observed.borrow()),
            };
            gate.add_permits(children);
            at_limit
        }
    });

    let (take, run) = (progress.clone(), progress.clone());
    app.update(move |ctx| async move {
        let handle = ctx
            .mount_each(
                0..children,
                move |i| {
                    take.send_modify(|p| p.taken += 1);
                    *i as i64
                },
                move |_child, _i| {
                    let (progress, gate) = (run.clone(), gate.clone());
                    async move {
                        progress.send_modify(|p| {
                            p.started += 1;
                            p.peak_running = p.peak_running.max(p.started - p.finished);
                        });
                        let _permit = gate.acquire().await.unwrap();
                        progress.send_modify(|p| p.finished += 1);
                        Ok(())
                    }
                },
            )
            .await?;
        handle.ready().await?;
        Ok(())
    })
    .await
    .unwrap();

    let at_limit = match observer.await.unwrap() {
        Ok(at_limit) => at_limit,
        Err(stuck) => panic!("{limit} children never ran at once: {stuck:?}"),
    };
    assert_eq!(
        (at_limit.started, at_limit.taken),
        (limit, limit + 1),
        "with {limit} children running and none released, the next one waits \
         for a token: {at_limit:?}"
    );
    let end = *progress.borrow();
    assert_eq!(end.started, children, "{end:?}");
    assert_eq!(end.peak_running, limit, "{end:?}");
}
