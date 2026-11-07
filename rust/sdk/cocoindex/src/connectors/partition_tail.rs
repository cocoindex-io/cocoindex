//! The tail loop the partitioned stream sources (Kafka, Iggy) share: every
//! partition of a [`LiveStream`](crate::live_component::LiveStream) is polled
//! at the same time, each polled batch is delivered in order and acknowledged
//! once processed, and readiness is signalled once every partition has caught
//! up to the end it had when the watch started.

use std::sync::Mutex;

use crate::error::Result;
use crate::live_component::LiveStreamSubscriber;

/// What one partition tail reads and commits. Abstracted from the broker so
/// the loop can be tested without one.
#[crate::async_trait]
pub(crate) trait PartitionSource<M>: Send + Sync {
    /// The records from `offset` on, each with its offset, in order; empty when
    /// none arrived within the broker's wait.
    async fn fetch(&self, offset: i64) -> Result<Vec<(i64, M)>>;

    /// Record that every record before `next` has been processed, where the
    /// source keeps its position (a consumer group's cursor).
    async fn commit(&self, next: i64) -> Result<()>;
}

/// A partition a watch tails: where to start, and the end it had when the
/// watch started (the offset the next record gets).
pub(crate) struct PartitionTail<S> {
    pub(crate) source: S,
    pub(crate) start: i64,
    pub(crate) high_watermark: i64,
}

/// Readiness across the partitions of one watch: each partition's committed
/// next offset against its initial high watermark.
struct Readiness {
    /// `(committed, high_watermark)` per partition.
    partitions: Mutex<Vec<(i64, i64)>>,
    signaled: std::sync::atomic::AtomicBool,
}

impl Readiness {
    fn new<S>(tails: &[PartitionTail<S>]) -> Self {
        Self {
            partitions: Mutex::new(tails.iter().map(|t| (t.start, t.high_watermark)).collect()),
            signaled: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Record a partition's commit; `true` the one time every partition has
    /// caught up, so the caller signals readiness.
    fn committed(&self, index: usize, next: i64) -> bool {
        let mut partitions = self.partitions.lock().unwrap_or_else(|e| e.into_inner());
        partitions[index].0 = next;
        let caught_up = partitions.iter().all(|(committed, hwm)| committed >= hwm);
        caught_up
            && !self
                .signaled
                .swap(true, std::sync::atomic::Ordering::AcqRel)
    }
}

/// Tail every partition at the same time: each partition's fetches and
/// deliveries overlap the others'. Per partition, a fetched batch is delivered
/// in order, the batch's handles are awaited together, and the partition's next
/// offset is then committed and counted toward readiness; so at most one batch
/// per partition is in flight, and a committed offset never precedes an
/// unprocessed record. Returns only on an error.
pub(crate) async fn watch_partitions<M, S>(
    tails: Vec<PartitionTail<S>>,
    subscriber: &dyn LiveStreamSubscriber<M>,
) -> Result<()>
where
    M: Send + 'static,
    S: PartitionSource<M>,
{
    let readiness = Readiness::new(&tails);
    // An empty topic, or a cursor already at the end, is ready at once.
    if tails.iter().all(|t| t.start >= t.high_watermark)
        && !readiness
            .signaled
            .swap(true, std::sync::atomic::Ordering::AcqRel)
    {
        subscriber.mark_ready().await;
    }
    let readiness = &readiness;
    futures::future::try_join_all(
        tails
            .iter()
            .enumerate()
            .map(|(index, tail)| tail_partition(index, tail, subscriber, readiness)),
    )
    .await?;
    Ok(())
}

/// One partition's tail loop (see [`watch_partitions`]); ends only on an error.
async fn tail_partition<M, S>(
    index: usize,
    tail: &PartitionTail<S>,
    subscriber: &dyn LiveStreamSubscriber<M>,
    readiness: &Readiness,
) -> Result<()>
where
    M: Send + 'static,
    S: PartitionSource<M>,
{
    let mut offset = tail.start;
    loop {
        let messages = tail.source.fetch(offset).await?;
        if messages.is_empty() {
            continue;
        }
        let mut handles = Vec::new();
        for (message_offset, message) in messages {
            offset = message_offset + 1;
            let handle = subscriber.send(message).await?;
            if !handle.is_immediate() {
                handles.push(handle.ready());
            }
        }
        futures::future::try_join_all(handles).await?;
        tail.source.commit(offset).await?;
        if readiness.committed(index, offset) {
            subscriber.mark_ready().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live_component::ReadyHandle;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// A fake partition: a fixed log served one record per fetch, counting
    /// how many fetches are in flight at once across the fakes sharing the
    /// counter, and recording what it committed and what had been processed by
    /// then.
    struct FakePartition {
        name: &'static str,
        log: Vec<&'static str>,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
        processed: Arc<Mutex<BTreeSet<(&'static str, i64)>>>,
        /// `(next, the records of this partition processed by then)`.
        commits: Mutex<Vec<(i64, Vec<i64>)>>,
    }

    #[crate::async_trait]
    impl PartitionSource<(&'static str, i64)> for FakePartition {
        async fn fetch(&self, offset: i64) -> Result<Vec<(i64, (&'static str, i64))>> {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(if (offset as usize) < self.log.len() {
                vec![(offset, (self.name, offset))]
            } else {
                Vec::new()
            })
        }

        async fn commit(&self, next: i64) -> Result<()> {
            let processed = self
                .processed
                .lock()
                .unwrap()
                .iter()
                .filter(|(name, _)| *name == self.name)
                .map(|(_, offset)| *offset)
                .collect();
            self.commits.lock().unwrap().push((next, processed));
            Ok(())
        }
    }

    /// Records what a watch delivered and signalled; every message is
    /// processed by a task that finishes a little later, so a commit that did
    /// not wait for processing would see it missing.
    struct Recorder {
        delivered: Mutex<Vec<(&'static str, i64)>>,
        processed: Arc<Mutex<BTreeSet<(&'static str, i64)>>>,
        ready: AtomicUsize,
    }

    #[crate::async_trait]
    impl LiveStreamSubscriber<(&'static str, i64)> for Recorder {
        async fn send(&self, message: (&'static str, i64)) -> Result<ReadyHandle> {
            self.delivered.lock().unwrap().push(message);
            let processed = self.processed.clone();
            Ok(ReadyHandle::from_future(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                processed.lock().unwrap().insert(message);
                Ok(())
            }))
        }

        async fn mark_ready(&self) {
            self.ready.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Fakes {
        tails: Vec<PartitionTail<Arc<FakePartition>>>,
        max_in_flight: Arc<AtomicUsize>,
        recorder: Recorder,
    }

    fn fakes(logs: Vec<(&'static str, Vec<&'static str>)>) -> Fakes {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let processed = Arc::new(Mutex::new(BTreeSet::new()));
        let tails = logs
            .into_iter()
            .map(|(name, log)| PartitionTail {
                start: 0,
                high_watermark: log.len() as i64,
                source: Arc::new(FakePartition {
                    name,
                    log,
                    in_flight: in_flight.clone(),
                    max_in_flight: max_in_flight.clone(),
                    processed: processed.clone(),
                    commits: Mutex::new(Vec::new()),
                }),
            })
            .collect();
        Fakes {
            tails,
            max_in_flight,
            recorder: Recorder {
                delivered: Mutex::new(Vec::new()),
                processed,
                ready: AtomicUsize::new(0),
            },
        }
    }

    #[crate::async_trait]
    impl<M: Send + 'static, S: PartitionSource<M>> PartitionSource<M> for Arc<S> {
        async fn fetch(&self, offset: i64) -> Result<Vec<(i64, M)>> {
            S::fetch(self, offset).await
        }
        async fn commit(&self, next: i64) -> Result<()> {
            S::commit(self, next).await
        }
    }

    /// Run `watch` until `done` holds, within five seconds.
    async fn watch_until(
        tails: Vec<PartitionTail<Arc<FakePartition>>>,
        recorder: &Recorder,
        done: impl Fn() -> bool,
    ) {
        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                r = watch_partitions(tails, recorder) => r,
                _ = async {
                    while !done() {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                } => Ok(()),
            }
        })
        .await;
        assert!(outcome.is_ok(), "the watch did not finish in time");
        outcome.unwrap().unwrap();
    }

    // Partitions are polled at the same time (two fetches in flight at once),
    // every record is delivered in partition order, a partition's offset is
    // committed only after its records are processed, and readiness is
    // signalled exactly once, when every partition reached its initial end.
    #[tokio::test]
    async fn partitions_are_polled_concurrently_and_committed_after_processing() {
        let Fakes {
            tails,
            max_in_flight,
            recorder,
        } = fakes(vec![
            ("a", vec!["x", "y", "z"]),
            ("b", vec!["x", "y"]),
            ("c", vec![]),
        ]);
        let sources: Vec<Arc<FakePartition>> = tails.iter().map(|t| t.source.clone()).collect();
        watch_until(tails, &recorder, || {
            recorder.processed.lock().unwrap().len() == 5
                && recorder.ready.load(Ordering::SeqCst) > 0
        })
        .await;

        assert!(
            max_in_flight.load(Ordering::SeqCst) >= 2,
            "partitions were polled one after another"
        );
        let delivered = recorder.delivered.lock().unwrap().clone();
        let per_partition = |name: &str| -> Vec<i64> {
            delivered
                .iter()
                .filter(|(n, _)| *n == name)
                .map(|(_, o)| *o)
                .collect()
        };
        assert_eq!(per_partition("a"), vec![0, 1, 2]);
        assert_eq!(per_partition("b"), vec![0, 1]);
        for source in &sources {
            let commits = source.commits.lock().unwrap().clone();
            for (next, processed_by_then) in &commits {
                assert_eq!(
                    processed_by_then,
                    &(0..*next).collect::<Vec<i64>>(),
                    "partition {} committed {next} before processing its records",
                    source.name
                );
            }
            assert_eq!(
                commits.last().map(|(next, _)| *next),
                if source.log.is_empty() {
                    None
                } else {
                    Some(source.log.len() as i64)
                }
            );
        }
        assert_eq!(recorder.ready.load(Ordering::SeqCst), 1);
    }

    // A watch that starts at the end of every partition is ready at once,
    // before any record arrives.
    #[tokio::test]
    async fn a_watch_at_the_end_is_ready_at_once() {
        let Fakes {
            mut tails,
            recorder,
            ..
        } = fakes(vec![("a", vec!["x"])]);
        tails[0].start = 1;
        let source = tails[0].source.clone();
        watch_until(tails, &recorder, || {
            recorder.ready.load(Ordering::SeqCst) > 0
        })
        .await;
        assert_eq!(recorder.ready.load(Ordering::SeqCst), 1);
        assert!(source.commits.lock().unwrap().is_empty());
    }
}
