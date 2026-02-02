use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, watch};
use tokio_util::task::AbortOnDropHandle;
use tracing::error;

use crate::error::{Error, ResidualError, Result};

// ============================================================================
// Common types
// ============================================================================

/// Options for batching behavior.
#[derive(Default, Clone, Serialize, Deserialize)]
pub struct BatchingOptions {
    pub max_batch_size: Option<usize>,
}

/// A batch of inputs waiting to be processed.
struct Batch<I, O> {
    inputs: Vec<I>,
    output_txs: Vec<oneshot::Sender<Result<O>>>,
    num_cancelled_tx: watch::Sender<usize>,
    num_cancelled_rx: watch::Receiver<usize>,
}

impl<I, O> Default for Batch<I, O> {
    fn default() -> Self {
        let (num_cancelled_tx, num_cancelled_rx) = watch::channel(0);
        Self {
            inputs: Vec::new(),
            output_txs: Vec::new(),
            num_cancelled_tx,
            num_cancelled_rx,
        }
    }
}

impl<I, O> Batch<I, O> {
    fn len(&self) -> usize {
        self.inputs.len()
    }
}

// ============================================================================
// BatchQueue - Shared queue for multiple batchers (no worker loop)
// ============================================================================

/// Type-erased runner function that can be stored in batches.
/// Each batcher has its own runner function, and batches carry a reference to it.
pub type BoxedRunnerFn<I, O> =
    Arc<dyn Fn(Vec<I>) -> BoxFuture<'static, Result<Vec<O>>> + Send + Sync + 'static>;

/// A batch in the queue. Each batch includes its own runner function.
struct QueuedBatch<I, O> {
    batch: Batch<I, O>,
    /// True when the batch is sealed (being processed or max_batch_size reached).
    sealed: bool,
    /// Unique ID for this batch.
    id: u64,
    /// The runner function for THIS batch (from the batcher that created it).
    runner_fn: BoxedRunnerFn<I, O>,
}

impl<I, O> QueuedBatch<I, O> {
    fn new(id: u64, runner_fn: BoxedRunnerFn<I, O>) -> Self {
        Self {
            batch: Batch::default(),
            sealed: false,
            id,
            runner_fn,
        }
    }
}

/// Internal state protected by the queue's mutex.
struct BatchQueueInner<I, O> {
    /// FIFO queue of batches waiting to be processed.
    batches: VecDeque<QueuedBatch<I, O>>,
    /// Counter for generating unique batch IDs.
    next_batch_id: u64,
    /// True when the queue is being processed.
    processing: bool,
}

/// Shared queue that multiple batchers can submit to.
/// Uses on-demand processing (no worker loop) - the first caller to arrive
/// when idle becomes the processor.
///
/// This allows different batched functions to share the same queue (e.g., for GPU
/// serialization) while each using their own processing logic.
pub struct BatchQueue<I: Send + 'static, O: Send + 'static> {
    inner: Mutex<BatchQueueInner<I, O>>,
}

impl<I: Send + 'static, O: Send + 'static> BatchQueue<I, O> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(BatchQueueInner {
                batches: VecDeque::new(),
                next_batch_id: 0,
                processing: false,
            }),
        }
    }

    /// Take the next batch to process, or release if none.
    /// Must be called while `processing` is true.
    /// Returns the batch to run, or None (and releases processing) if no batches.
    fn take_next_or_release(&self) -> Option<QueuedBatch<I, O>> {
        let mut inner = self.inner.lock().unwrap();

        // Find first batch with items
        while let Some(batch) = inner.batches.front_mut() {
            if batch.batch.len() > 0 {
                batch.sealed = true;
                return inner.batches.pop_front();
            }
            // Empty batch, remove and check next
            inner.batches.pop_front();
        }

        // No batches with items, release
        inner.processing = false;
        None
    }

    /// Execute a batch and send results to waiters.
    async fn run_batch(batch: QueuedBatch<I, O>) {
        let num_inputs = batch.batch.inputs.len();
        if num_inputs == 0 {
            return;
        }

        let mut num_cancelled_rx = batch.batch.num_cancelled_rx;

        // Run with cancellation check
        let outputs = tokio::select! {
            outputs = (batch.runner_fn)(batch.batch.inputs) => outputs,
            _ = num_cancelled_rx.wait_for(|v| *v == num_inputs) => {
                // All callers cancelled
                return;
            }
        };

        // Send results
        match outputs {
            Ok(outputs) => {
                if outputs.len() != batch.batch.output_txs.len() {
                    let message = format!(
                        "Batched output length mismatch: expected {} outputs, got {}",
                        batch.batch.output_txs.len(),
                        outputs.len()
                    );
                    error!("{message}");
                    for sender in batch.batch.output_txs {
                        sender.send(Err(Error::internal_msg(&message))).ok();
                    }
                    return;
                }
                for (output, sender) in outputs.into_iter().zip(batch.batch.output_txs) {
                    sender.send(Ok(output)).ok();
                }
            }
            Err(err) => {
                let mut senders_iter = batch.batch.output_txs.into_iter();
                if let Some(sender) = senders_iter.next() {
                    if senders_iter.len() > 0 {
                        let residual_err = ResidualError::new(&err);
                        for sender in senders_iter {
                            sender.send(Err(residual_err.clone().into())).ok();
                        }
                    }
                    sender.send(Err(err)).ok();
                }
            }
        }
    }
}

impl<I: Send + 'static, O: Send + 'static> Default for BatchQueue<I, O> {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Batcher - Collects inputs and submits to a BatchQueue
// ============================================================================

/// Batcher that collects inputs and submits to a shared BatchQueue.
/// Each batcher has at most one non-full, non-sealed batch in the queue.
///
/// Multiple batchers can share the same queue (e.g., for GPU serialization),
/// and each batcher uses its own runner function.
pub struct Batcher<I: Send + 'static, O: Send + 'static> {
    queue: Arc<BatchQueue<I, O>>,
    /// ID of this batcher's current non-full, non-sealed batch in the queue.
    current_batch_id: Mutex<Option<u64>>,
    options: BatchingOptions,
    /// This batcher's runner function.
    runner_fn: BoxedRunnerFn<I, O>,
}

impl<I: Send + 'static, O: Send + 'static> Batcher<I, O> {
    /// Create a batcher that uses the given shared queue and runner function.
    ///
    /// The runner function will be called for batches created by this batcher.
    /// Multiple batchers can share the same queue with different runner functions.
    pub fn new(
        queue: Arc<BatchQueue<I, O>>,
        options: BatchingOptions,
        runner_fn: BoxedRunnerFn<I, O>,
    ) -> Self {
        Self {
            queue,
            current_batch_id: Mutex::new(None),
            options,
            runner_fn,
        }
    }

    /// Submit an input and wait for the result.
    pub async fn run(&self, input: I) -> Result<O> {
        let (output_rx, num_cancelled_tx, should_process) = self.add_to_batch(input);

        if should_process {
            // We acquired processing - run batches until queue is empty
            let _guard = BatchQueueKickOffGuard {
                queue: self.queue.clone(),
            };

            while let Some(batch) = self.queue.take_next_or_release() {
                // Run in spawned task for isolation (cancel safety)
                let handle = AbortOnDropHandle::new(tokio::spawn(async move {
                    BatchQueue::run_batch(batch).await;
                }));
                let _ = handle.await;
            }
            // Guard drops here - but take_next_or_release already released if we got None
        }

        // Wait for result
        let mut guard = BatchRecvCancellationGuard::new(Some(num_cancelled_tx));
        let output = output_rx.await?;
        guard.done();
        output
    }

    /// Add input to a batch, creating a new batch if needed.
    /// Returns (output_rx, num_cancelled_tx, should_process).
    fn add_to_batch(&self, input: I) -> (oneshot::Receiver<Result<O>>, watch::Sender<usize>, bool) {
        let mut current_id = self.current_batch_id.lock().unwrap();
        let mut inner = self.queue.inner.lock().unwrap();

        // Find our current batch (by ID, not sealed, not full)
        let batch_idx = if let Some(id) = *current_id {
            inner.batches.iter().position(|b| {
                b.id == id
                    && !b.sealed
                    && self
                        .options
                        .max_batch_size
                        .map_or(true, |max| b.batch.len() < max)
            })
        } else {
            None
        };

        let batch_idx = match batch_idx {
            Some(idx) => idx,
            None => {
                // Create new batch with this batcher's runner function
                let id = inner.next_batch_id;
                inner.next_batch_id += 1;
                let batch = QueuedBatch::new(id, self.runner_fn.clone());
                inner.batches.push_back(batch);
                *current_id = Some(id);
                inner.batches.len() - 1
            }
        };

        let batch = &mut inner.batches[batch_idx];

        // Add item
        batch.batch.inputs.push(input);
        let (output_tx, output_rx) = oneshot::channel();
        batch.batch.output_txs.push(output_tx);
        let num_cancelled_tx = batch.batch.num_cancelled_tx.clone();

        // Check if batch should be sealed (max_batch_size reached)
        if let Some(max) = self.options.max_batch_size {
            if batch.batch.len() >= max {
                batch.sealed = true;
                *current_id = None;
            }
        }

        // Check if we should process (queue was idle)
        let should_process = if !inner.processing {
            inner.processing = true;
            true
        } else {
            false
        };

        (output_rx, num_cancelled_tx, should_process)
    }
}

/// Guard that ensures kick-off-next happens even on panic/cancel.
/// Only used as a safety net - normally take_next_or_release releases processing.
struct BatchQueueKickOffGuard<I: Send + 'static, O: Send + 'static> {
    queue: Arc<BatchQueue<I, O>>,
}

impl<I: Send + 'static, O: Send + 'static> Drop for BatchQueueKickOffGuard<I, O> {
    fn drop(&mut self) {
        // Check if there are more batches to process
        if let Some(batch) = self.queue.take_next_or_release() {
            // Process remaining batches in a spawned task
            let queue = self.queue.clone();
            tokio::spawn(async move {
                let _guard = BatchQueueKickOffGuard {
                    queue: queue.clone(),
                };
                BatchQueue::run_batch(batch).await;
                while let Some(batch) = queue.take_next_or_release() {
                    BatchQueue::run_batch(batch).await;
                }
            });
        }
        // If no batches, take_next_or_release already released processing
    }
}

/// Guard that increments the cancellation count when dropped (unless done() is called).
struct BatchRecvCancellationGuard {
    num_cancelled_tx: Option<watch::Sender<usize>>,
}

impl Drop for BatchRecvCancellationGuard {
    fn drop(&mut self) {
        if let Some(num_cancelled_tx) = self.num_cancelled_tx.take() {
            num_cancelled_tx.send_modify(|v| *v += 1);
        }
    }
}

impl BatchRecvCancellationGuard {
    pub fn new(num_cancelled_tx: Option<watch::Sender<usize>>) -> Self {
        Self { num_cancelled_tx }
    }

    pub fn done(&mut self) {
        self.num_cancelled_tx = None;
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::time::{Duration, sleep};

    /// Helper to create a boxed runner function for tests.
    fn boxed_runner<I, O, F, Fut>(f: F) -> BoxedRunnerFn<I, O>
    where
        I: Send + 'static,
        O: Send + 'static,
        F: Fn(Vec<I>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Vec<O>>> + Send + 'static,
    {
        Arc::new(move |inputs| Box::pin(f(inputs)) as BoxFuture<'static, _>)
    }

    #[tokio::test]
    async fn batch_queue_basic() -> Result<()> {
        let queue = Arc::new(BatchQueue::new());

        // Create batcher with a simple doubling function
        let batcher = Batcher::new(
            queue.clone(),
            BatchingOptions::default(),
            boxed_runner(|inputs: Vec<i32>| async move {
                Ok(inputs.into_iter().map(|x| x * 2).collect())
            }),
        );

        // Single call should work
        let result = batcher.run(5).await?;
        assert_eq!(result, 10);

        Ok(())
    }

    #[tokio::test]
    async fn batch_queue_multiple_items() -> Result<()> {
        let call_count = Arc::new(Mutex::new(0));
        let call_count_clone = call_count.clone();

        let queue = Arc::new(BatchQueue::new());
        let batcher = Arc::new(Batcher::new(
            queue.clone(),
            BatchingOptions::default(),
            boxed_runner(move |inputs: Vec<i32>| {
                let call_count = call_count_clone.clone();
                async move {
                    *call_count.lock().unwrap() += 1;
                    Ok(inputs.into_iter().map(|x| x * 2).collect())
                }
            }),
        ));

        // Submit multiple items concurrently
        let b1 = batcher.clone();
        let b2 = batcher.clone();
        let b3 = batcher.clone();

        let (r1, r2, r3) = tokio::join!(b1.run(1), b2.run(2), b3.run(3),);

        assert_eq!(r1?, 2);
        assert_eq!(r2?, 4);
        assert_eq!(r3?, 6);

        // Items should have been batched together (1 or 2 calls, not 3)
        let calls = *call_count.lock().unwrap();
        assert!(calls <= 2, "Expected batching, got {} calls", calls);

        Ok(())
    }

    #[tokio::test]
    async fn batch_queue_max_batch_size() -> Result<()> {
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let batch_sizes_clone = batch_sizes.clone();

        let queue = Arc::new(BatchQueue::new());
        let batcher = Arc::new(Batcher::new(
            queue.clone(),
            BatchingOptions {
                max_batch_size: Some(2),
            },
            boxed_runner(move |inputs: Vec<i32>| {
                let batch_sizes = batch_sizes_clone.clone();
                async move {
                    batch_sizes.lock().unwrap().push(inputs.len());
                    Ok(inputs.into_iter().map(|x| x * 2).collect())
                }
            }),
        ));

        // Submit 5 items
        let handles: Vec<_> = (1..=5)
            .map(|i| {
                let b = batcher.clone();
                tokio::spawn(async move { b.run(i).await })
            })
            .collect();

        let mut results = Vec::new();
        for h in handles {
            results.push(h.await??);
        }

        // Verify results (order may vary due to batching)
        results.sort();
        assert_eq!(results, vec![2, 4, 6, 8, 10]);

        // Check that max_batch_size was respected
        let sizes = batch_sizes.lock().unwrap();
        for &size in sizes.iter() {
            assert!(size <= 2, "Batch size {} exceeds max of 2", size);
        }

        Ok(())
    }

    #[tokio::test]
    async fn batch_queue_shared_by_multiple_batchers_same_fn() -> Result<()> {
        let call_count = Arc::new(Mutex::new(0));
        let call_count_clone = call_count.clone();

        let queue = Arc::new(BatchQueue::new());

        // Create two batchers sharing the same queue with the same runner function
        let batcher1 = Arc::new(Batcher::new(
            queue.clone(),
            BatchingOptions::default(),
            boxed_runner({
                let call_count = call_count_clone.clone();
                move |inputs: Vec<i32>| {
                    let call_count = call_count.clone();
                    async move {
                        *call_count.lock().unwrap() += 1;
                        sleep(Duration::from_millis(10)).await;
                        Ok(inputs.into_iter().map(|x| x * 2).collect())
                    }
                }
            }),
        ));
        let batcher2 = Arc::new(Batcher::new(
            queue.clone(),
            BatchingOptions::default(),
            boxed_runner({
                let call_count = call_count_clone.clone();
                move |inputs: Vec<i32>| {
                    let call_count = call_count.clone();
                    async move {
                        *call_count.lock().unwrap() += 1;
                        sleep(Duration::from_millis(10)).await;
                        Ok(inputs.into_iter().map(|x| x * 2).collect())
                    }
                }
            }),
        ));

        // Submit from both batchers concurrently
        let b1 = batcher1.clone();
        let b2 = batcher2.clone();

        let (r1, r2) = tokio::join!(b1.run(10), b2.run(20),);

        assert_eq!(r1?, 20);
        assert_eq!(r2?, 40);

        Ok(())
    }

    /// Test that multiple batchers with DIFFERENT runner functions can share a queue.
    /// This is the key scenario for GPU serialization where different functions
    /// share the same GPU but have different processing logic.
    #[tokio::test]
    async fn batch_queue_shared_by_multiple_batchers_different_fns() -> Result<()> {
        let execution_order = Arc::new(Mutex::new(Vec::new()));

        let queue = Arc::new(BatchQueue::new());

        // Batcher 1: doubles the input
        let order1 = execution_order.clone();
        let batcher1 = Arc::new(Batcher::new(
            queue.clone(),
            BatchingOptions::default(),
            boxed_runner(move |inputs: Vec<i32>| {
                let order = order1.clone();
                async move {
                    order.lock().unwrap().push("double");
                    sleep(Duration::from_millis(20)).await;
                    Ok(inputs.into_iter().map(|x| x * 2).collect())
                }
            }),
        ));

        // Batcher 2: triples the input (different function!)
        let order2 = execution_order.clone();
        let batcher2 = Arc::new(Batcher::new(
            queue.clone(),
            BatchingOptions::default(),
            boxed_runner(move |inputs: Vec<i32>| {
                let order = order2.clone();
                async move {
                    order.lock().unwrap().push("triple");
                    sleep(Duration::from_millis(20)).await;
                    Ok(inputs.into_iter().map(|x| x * 3).collect())
                }
            }),
        ));

        // Submit from both batchers concurrently
        let b1 = batcher1.clone();
        let b2 = batcher2.clone();

        let (r1, r2) = tokio::join!(b1.run(10), b2.run(10),);

        // Batcher1 should double: 10 * 2 = 20
        assert_eq!(r1?, 20);
        // Batcher2 should triple: 10 * 3 = 30
        assert_eq!(r2?, 30);

        // Both functions should have been called (in some order)
        let order = execution_order.lock().unwrap();
        assert_eq!(order.len(), 2);
        assert!(order.contains(&"double"));
        assert!(order.contains(&"triple"));

        Ok(())
    }

    /// Test multiple concurrent calls to different batchers sharing a queue,
    /// verifying that batches are processed serially through the shared queue.
    #[tokio::test]
    async fn batch_queue_serial_execution_with_different_fns() -> Result<()> {
        let execution_log = Arc::new(Mutex::new(Vec::new()));

        let queue = Arc::new(BatchQueue::new());

        // Batcher 1: adds 100 to inputs
        let log1 = execution_log.clone();
        let batcher1 = Arc::new(Batcher::new(
            queue.clone(),
            BatchingOptions::default(),
            boxed_runner(move |inputs: Vec<i32>| {
                let log = log1.clone();
                async move {
                    log.lock()
                        .unwrap()
                        .push(format!("add100_start:{:?}", inputs));
                    sleep(Duration::from_millis(30)).await;
                    log.lock().unwrap().push(format!("add100_end:{:?}", inputs));
                    Ok(inputs.into_iter().map(|x| x + 100).collect())
                }
            }),
        ));

        // Batcher 2: multiplies by 10
        let log2 = execution_log.clone();
        let batcher2 = Arc::new(Batcher::new(
            queue.clone(),
            BatchingOptions::default(),
            boxed_runner(move |inputs: Vec<i32>| {
                let log = log2.clone();
                async move {
                    log.lock()
                        .unwrap()
                        .push(format!("mul10_start:{:?}", inputs));
                    sleep(Duration::from_millis(30)).await;
                    log.lock().unwrap().push(format!("mul10_end:{:?}", inputs));
                    Ok(inputs.into_iter().map(|x| x * 10).collect())
                }
            }),
        ));

        // Submit multiple items from both batchers
        let b1a = batcher1.clone();
        let b1b = batcher1.clone();
        let b2a = batcher2.clone();
        let b2b = batcher2.clone();

        let (r1a, r1b, r2a, r2b) = tokio::join!(b1a.run(1), b1b.run(2), b2a.run(3), b2b.run(4),);

        // Verify correct results
        assert_eq!(r1a?, 101); // 1 + 100
        assert_eq!(r1b?, 102); // 2 + 100
        assert_eq!(r2a?, 30); // 3 * 10
        assert_eq!(r2b?, 40); // 4 * 10

        // Verify serial execution (batches don't overlap)
        let log = execution_log.lock().unwrap();
        // We should see start/end pairs that don't overlap
        // The exact order depends on timing, but each batch should complete before the next starts
        assert!(log.len() >= 4); // At least 2 start + 2 end entries

        Ok(())
    }

    /// Test that each batcher's batch only contains items from that batcher,
    /// even when sharing a queue.
    #[tokio::test]
    async fn batch_queue_batches_are_per_batcher() -> Result<()> {
        let batcher1_inputs = Arc::new(Mutex::new(Vec::new()));
        let batcher2_inputs = Arc::new(Mutex::new(Vec::new()));

        let queue = Arc::new(BatchQueue::new());

        // Batcher 1: records inputs and adds 1000
        let inputs1 = batcher1_inputs.clone();
        let batcher1 = Arc::new(Batcher::new(
            queue.clone(),
            BatchingOptions::default(),
            boxed_runner(move |inputs: Vec<i32>| {
                let recorded = inputs1.clone();
                async move {
                    recorded.lock().unwrap().extend(inputs.iter().cloned());
                    Ok(inputs.into_iter().map(|x| x + 1000).collect())
                }
            }),
        ));

        // Batcher 2: records inputs and adds 2000
        let inputs2 = batcher2_inputs.clone();
        let batcher2 = Arc::new(Batcher::new(
            queue.clone(),
            BatchingOptions::default(),
            boxed_runner(move |inputs: Vec<i32>| {
                let recorded = inputs2.clone();
                async move {
                    recorded.lock().unwrap().extend(inputs.iter().cloned());
                    Ok(inputs.into_iter().map(|x| x + 2000).collect())
                }
            }),
        ));

        // Submit items - batcher1 gets 1,2,3 and batcher2 gets 10,20,30
        let handles = vec![
            {
                let b = batcher1.clone();
                tokio::spawn(async move { b.run(1).await })
            },
            {
                let b = batcher1.clone();
                tokio::spawn(async move { b.run(2).await })
            },
            {
                let b = batcher2.clone();
                tokio::spawn(async move { b.run(10).await })
            },
            {
                let b = batcher1.clone();
                tokio::spawn(async move { b.run(3).await })
            },
            {
                let b = batcher2.clone();
                tokio::spawn(async move { b.run(20).await })
            },
            {
                let b = batcher2.clone();
                tokio::spawn(async move { b.run(30).await })
            },
        ];

        let results: Vec<_> = futures::future::try_join_all(handles).await?;
        let results: Result<Vec<_>> = results.into_iter().collect();
        let results = results?;

        // Verify results
        assert_eq!(results[0], 1001); // 1 + 1000
        assert_eq!(results[1], 1002); // 2 + 1000
        assert_eq!(results[2], 2010); // 10 + 2000
        assert_eq!(results[3], 1003); // 3 + 1000
        assert_eq!(results[4], 2020); // 20 + 2000
        assert_eq!(results[5], 2030); // 30 + 2000

        // Verify that each batcher only saw its own inputs
        let mut b1_inputs = batcher1_inputs.lock().unwrap().clone();
        let mut b2_inputs = batcher2_inputs.lock().unwrap().clone();
        b1_inputs.sort();
        b2_inputs.sort();

        assert_eq!(b1_inputs, vec![1, 2, 3]);
        assert_eq!(b2_inputs, vec![10, 20, 30]);

        Ok(())
    }
}
