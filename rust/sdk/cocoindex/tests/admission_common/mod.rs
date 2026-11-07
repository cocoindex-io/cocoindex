//! Shared by `admission.rs` and `admission_env.rs`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use cocoindex::App;
use tokio::sync::Semaphore;

/// Mount `children` background components that block until released and
/// return how many of them were running once the count stopped growing:
/// the app's limit, when `children` exceeds it.
pub async fn count_admitted(app: &App, children: usize) -> usize {
    let started = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let admitted = Arc::new(AtomicUsize::new(0));

    let observer = tokio::spawn({
        let (started, gate, admitted) = (started.clone(), gate.clone(), admitted.clone());
        async move {
            let mut last = usize::MAX;
            let mut stable_since = Instant::now();
            loop {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let now = started.load(Ordering::SeqCst);
                if now != last {
                    last = now;
                    stable_since = Instant::now();
                } else if now > 0 && stable_since.elapsed() > Duration::from_millis(500) {
                    break;
                }
            }
            admitted.store(started.load(Ordering::SeqCst), Ordering::SeqCst);
            gate.add_permits(children);
        }
    });

    let (started_outer, gate_outer) = (started.clone(), gate.clone());
    app.update(move |ctx| async move {
        let handle = ctx
            .mount_each(0..children, |i| *i as i64, {
                move |_child, _i| {
                    let (started, gate) = (started_outer.clone(), gate_outer.clone());
                    async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        let _permit = gate.acquire().await.unwrap();
                        Ok(())
                    }
                }
            })
            .await?;
        handle.ready().await?;
        Ok(())
    })
    .await
    .unwrap();
    observer.await.unwrap();
    assert_eq!(started.load(Ordering::SeqCst), children);
    admitted.load(Ordering::SeqCst)
}
