use async_trait::async_trait;
use hashlink::LinkedHashMap;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{Notify, oneshot, watch};
use tokio_util::task::AbortOnDropHandle;
use tracing::{Instrument, Span, error};

use crate::error::{Error, Result};
use crate::internal_bail;
use crate::ratelimit::RateLimiter;

#[async_trait]
pub trait Runner: Send + Sync {
    type Input: Send;
    type Output: Send;

    async fn run(
        &self,
        inputs: Vec<Self::Input>,
    ) -> Result<impl ExactSizeIterator<Item = Self::Output>>;
}

/// What a gated batcher waits on before it runs a batch: one `acquire` per
/// batch. See [`BatchingOptions::gate`].
#[async_trait]
pub trait BatchGate: Send + Sync {
    async fn acquire(&self) -> Result<()>;
}

/// One token per batch, so a token pays for one request rather than one call.
#[async_trait]
impl BatchGate for RateLimiter {
    async fn acquire(&self) -> Result<()> {
        self.until_ready_n(1).await
    }
}

/// Entry for a pending batch in the queue.
struct PendingBatchEntry<R: Runner + 'static> {
    /// Weak reference to the batcher that owns this batch.
    /// Allows graceful handling if batcher is dropped while batch is pending.
    batcher_data: Weak<BatcherData<R>>,
    /// The actual batch of inputs waiting to be processed.
    batch: Batch<R::Input, R::Output>,
}

/// Shared queue state protected by a Mutex.
struct BatchQueueState<R: Runner + 'static> {
    /// Batches that reached `max_batch_size` while the queue was at its
    /// `max_in_flight` bound, in the order they filled (FIFO across batchers).
    /// Empty unless a batcher sets `max_in_flight`.
    sealed_batches: VecDeque<PendingBatchEntry<R>>,
    /// Per-batcher open (partial) batch, keyed by batcher pointer address.
    /// LinkedHashMap preserves insertion order for FIFO semantics.
    open_batches: LinkedHashMap<usize, PendingBatchEntry<R>>,
    /// Count of batches currently executing across all batchers.
    ongoing_count: usize,
}

impl<R: Runner + 'static> BatchQueueState<R> {
    /// Whether a batch that just filled may start now rather than wait sealed.
    fn has_slot(&self, max_in_flight: Option<NonZeroUsize>) -> bool {
        max_in_flight.is_none_or(|max| self.ongoing_count < max.get())
    }

    /// Whether a pending batch may start after another batch finished: under
    /// a `max_in_flight` bound whenever a slot is free, otherwise only once the
    /// queue is idle.
    fn may_start_pending(&self, max_in_flight: Option<NonZeroUsize>) -> bool {
        match max_in_flight {
            Some(max) => self.ongoing_count < max.get(),
            None => self.ongoing_count == 0,
        }
    }

    /// The next batch to start: sealed batches first (they filled before any
    /// open batch of their batcher), then the oldest open batch.
    fn pop_next_pending(&mut self) -> Option<PendingBatchEntry<R>> {
        self.sealed_batches
            .pop_front()
            .or_else(|| self.open_batches.pop_front().map(|(_, entry)| entry))
    }

    /// Starts pending batches for as long as `may_start_pending` holds.
    fn kick_off_pending(&mut self, max_in_flight: Option<NonZeroUsize>) {
        while self.may_start_pending(max_in_flight) {
            let Some(entry) = self.pop_next_pending() else {
                break;
            };
            if let Some(batcher_data) = entry.batcher_data.upgrade() {
                // Batcher still alive - execute this batch
                self.ongoing_count += 1;
                let slot = InFlightSlot {
                    batcher: batcher_data.clone(),
                };
                tokio::spawn(async move {
                    batcher_data.run_batch(entry.batch, slot).await;
                });
            }
            // Batcher was dropped - batch will be cancelled automatically
            // when output_txs are dropped. Continue to next pending batch.
        }
    }
}

/// A shared queue that processes batches in FIFO order.
///
/// Multiple batchers can share the same queue. Each batcher has its own runner
/// function, and batches are processed using the runner from the batcher that
/// created them.
pub struct BatchQueue<R: Runner + 'static> {
    state: Mutex<BatchQueueState<R>>,
    /// Signalled once per finished batch; a gated pump waits on it for a slot.
    slot_freed: Notify,
}

impl<R: Runner + 'static> BatchQueue<R> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(BatchQueueState {
                sealed_batches: VecDeque::new(),
                open_batches: LinkedHashMap::new(),
                ongoing_count: 0,
            }),
            slot_freed: Notify::new(),
        }
    }
}

impl<R: Runner + 'static> Default for BatchQueue<R> {
    fn default() -> Self {
        Self::new()
    }
}

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
    /// Nobody is waiting for this batch: it is empty, or every caller has
    /// cancelled. Running it would deliver to no one.
    fn is_abandoned(&self) -> bool {
        *self.num_cancelled_rx.borrow() >= self.inputs.len()
    }
}

/// Hands `err` to the first recipient and an `Error::replica` — a faithful
/// copy that preserves a clonable structural error (e.g. a Python `PyErr`
/// with its type + traceback) and flattens only what can't be cloned — to
/// every other one.
fn fail_all<O>(senders: Vec<oneshot::Sender<Result<O>>>, err: Error) {
    let mut senders_iter = senders.into_iter();
    if let Some(sender) = senders_iter.next() {
        for sender in senders_iter {
            sender.send(Err(err.replica())).ok();
        }
        sender.send(Err(err)).ok();
    }
}

/// A gated batcher's inputs waiting for its pump (see `BatchingOptions::gate`).
struct GatedState<I, O> {
    /// Batches that reached `max_batch_size`, oldest first.
    sealed: VecDeque<Batch<I, O>>,
    /// The partial batch new inputs join.
    open: Batch<I, O>,
    /// Whether a pump task is alive. Set and cleared only under this lock, so
    /// a call that finds it set is served by that pump.
    pump_running: bool,
}

struct Gated<R: Runner + 'static> {
    gate: Arc<dyn BatchGate>,
    state: Mutex<GatedState<R::Input, R::Output>>,
}

/// What the gated pump holds towards its next batch.
enum Held<R: Runner + 'static> {
    Nothing,
    /// An in-flight slot, the gate still to pass.
    Slot(InFlightSlot<R>),
    /// A slot and a token: the next live batch runs at once.
    Ready(InFlightSlot<R>),
}

/// One decision of the gated pump, taken under the gated state's lock.
enum PumpStep<R: Runner + 'static> {
    /// Nothing live is pending; the pump has handed itself back.
    Exit,
    /// Inputs are pending but a slot or the gate is still to be waited for.
    Wait,
    /// Run this batch on the slot and token in hand.
    Run(Batch<R::Input, R::Output>, InFlightSlot<R>),
}

struct BatcherData<R: Runner + 'static> {
    runner: R,
    queue: Arc<BatchQueue<R>>,
    max_batch_size: Option<usize>,
    max_in_flight: Option<NonZeroUsize>,
    /// Present when the batcher has a gate.
    gated: Option<Gated<R>>,
}

impl<R: Runner + 'static> BatcherData<R> {
    /// Waits until `max_in_flight` has room for one more running batch and
    /// takes that place.
    async fn reserve_slot(self: &Arc<Self>) -> InFlightSlot<R> {
        loop {
            let slot_freed = self.queue.slot_freed.notified();
            {
                let mut state = self.queue.state.lock().unwrap();
                if state.has_slot(self.max_in_flight) {
                    state.ongoing_count += 1;
                    return InFlightSlot {
                        batcher: self.clone(),
                    };
                }
            }
            slot_freed.await;
        }
    }

    /// Runs one batch on `_slot`, which is released when this returns.
    async fn run_batch(
        self: &Arc<Self>,
        batch: Batch<R::Input, R::Output>,
        _slot: InFlightSlot<R>,
    ) {
        let num_inputs = batch.inputs.len();

        let mut num_cancelled_rx = batch.num_cancelled_rx;
        let outputs = tokio::select! {
            outputs = self.runner.run(batch.inputs) => {
                outputs
            }
            _ = num_cancelled_rx.wait_for(|v| *v == num_inputs) => {
                return;
            }
        };

        match outputs {
            Ok(outputs) => {
                if outputs.len() != batch.output_txs.len() {
                    let message = format!(
                        "Batched output length mismatch: expected {} outputs, got {}",
                        batch.output_txs.len(),
                        outputs.len()
                    );
                    error!("{message}");
                    for sender in batch.output_txs {
                        sender.send(Err(Error::internal_msg(&message))).ok();
                    }
                    return;
                }
                for (output, sender) in outputs.zip(batch.output_txs) {
                    sender.send(Ok(output)).ok();
                }
            }
            Err(err) => fail_all(batch.output_txs, err),
        }
    }

    /// The gated batcher's pump. While inputs are pending: wait for an
    /// in-flight slot, then for the gate, then run the oldest pending batch.
    /// A slot and a token in hand survive a batch that was abandoned
    /// meanwhile, so they go to the next live batch; the slot is given back
    /// only when nothing is pending.
    async fn run_pump(self: Arc<Self>) {
        let gated = self
            .gated
            .as_ref()
            .expect("the pump runs only for a gated batcher");
        let mut held = Held::Nothing;
        loop {
            let step = {
                let mut state = gated.state.lock().unwrap();
                state.sealed.retain(|batch| !batch.is_abandoned());
                if state.sealed.is_empty() && state.open.is_abandoned() {
                    // Nothing live is pending: hand the pump back under the
                    // lock, so a call that finds `pump_running` set is served.
                    state.open = Batch::default();
                    state.pump_running = false;
                    PumpStep::Exit
                } else {
                    match std::mem::replace(&mut held, Held::Nothing) {
                        Held::Ready(slot) => {
                            let batch = state
                                .sealed
                                .pop_front()
                                .unwrap_or_else(|| std::mem::take(&mut state.open));
                            PumpStep::Run(batch, slot)
                        }
                        not_ready => {
                            held = not_ready;
                            PumpStep::Wait
                        }
                    }
                }
            };
            match step {
                PumpStep::Exit => return,
                PumpStep::Wait => {
                    held = match std::mem::replace(&mut held, Held::Nothing) {
                        Held::Nothing => Held::Slot(self.reserve_slot().await),
                        Held::Slot(slot) => match gated.gate.acquire().await {
                            Ok(()) => Held::Ready(slot),
                            Err(err) => {
                                // No batch can be sent: fail every pending caller.
                                let senders: Vec<_> = {
                                    let mut state = gated.state.lock().unwrap();
                                    state.pump_running = false;
                                    let open = std::mem::take(&mut state.open);
                                    state
                                        .sealed
                                        .drain(..)
                                        .chain(std::iter::once(open))
                                        .flat_map(|batch| batch.output_txs)
                                        .collect()
                                };
                                fail_all(senders, err);
                                return;
                            }
                        },
                        ready => ready,
                    };
                }
                // Abandoned while the pump waited: skip it, keep the slot and
                // the token for the next batch.
                PumpStep::Run(batch, slot) if batch.is_abandoned() => held = Held::Ready(slot),
                PumpStep::Run(batch, slot) => {
                    let data = self.clone();
                    tokio::spawn(async move {
                        data.run_batch(batch, slot).await;
                    });
                }
            }
        }
    }
}

pub struct Batcher<R: Runner + 'static> {
    data: Arc<BatcherData<R>>,
}

enum BatchExecutionAction<R: Runner + 'static> {
    Inline {
        input: R::Input,
    },
    Batched {
        output_rx: oneshot::Receiver<Result<R::Output>>,
        num_cancelled_tx: watch::Sender<usize>,
    },
}

/// Options of one batcher.
///
/// Batchers sharing a [`BatchQueue`] should carry the same `max_in_flight`
/// and the same gate: the queue applies the bound of the batcher whose batch
/// is being submitted or has just finished, so it is a queue-wide bound only
/// when they agree.
#[derive(Default, Clone, Serialize, Deserialize)]
pub struct BatchingOptions {
    /// Seal and submit a batch once it holds this many inputs.
    pub max_batch_size: Option<usize>,
    /// Upper bound on the queue's concurrently running batches; the inline
    /// first call counts as one.
    ///
    /// - `None`: a full batch starts the moment it fills, and the next partial
    ///   batch starts only once every running batch has finished.
    /// - `Some(n)`: a batch that fills while `n` batches are running stays
    ///   sealed in FIFO order, and whenever a batch finishes the next pending
    ///   batch (sealed or partial) starts as long as fewer than `n` are
    ///   running.
    ///
    /// Without a gate, either way the first call on an idle queue runs inline
    /// at once.
    pub max_in_flight: Option<NonZeroUsize>,
    /// What every batch passes before it runs — typically a [`RateLimiter`],
    /// whose token then pays for the batch's request.
    ///
    /// A gate replaces the idle-queue rule: no call runs inline. Calls join
    /// the batcher's open batch, a full batch is sealed and waits in order,
    /// and a pump takes the batches one at a time: it waits for an in-flight
    /// slot, then for the gate, then runs the oldest pending batch, and loops
    /// while inputs are pending. The batch boundary is thus the moment a
    /// request can be sent: under a scarce quota every call that arrives
    /// during the wait rides the next token, and with tokens to spare a
    /// batch goes out at once and the next one fills during its round trip
    /// (bounded by `max_in_flight`, when set). A batch abandoned by all its
    /// callers while the pump waits is skipped, and a slot or token already
    /// in hand goes to the next live batch; a token taken for a batch that
    /// is abandoned with nothing else pending is spent.
    #[serde(skip)]
    pub gate: Option<Arc<dyn BatchGate>>,
}
impl<R: Runner + 'static> Batcher<R> {
    pub fn new(runner: R, queue: Arc<BatchQueue<R>>, options: BatchingOptions) -> Self {
        let gated = options.gate.map(|gate| Gated {
            gate,
            state: Mutex::new(GatedState {
                sealed: VecDeque::new(),
                open: Batch::default(),
                pump_running: false,
            }),
        });
        Self {
            data: Arc::new(BatcherData {
                runner,
                queue,
                max_batch_size: options.max_batch_size,
                max_in_flight: options.max_in_flight,
                gated,
            }),
        }
    }

    pub async fn run(&self, input: R::Input) -> Result<R::Output> {
        if let Some(gated) = &self.data.gated {
            return self.run_gated(gated, input).await;
        }

        let batcher_key = Arc::as_ptr(&self.data) as usize;

        let batch_exec_action: BatchExecutionAction<R> = {
            let mut queue_state = self.data.queue.state.lock().unwrap();

            if queue_state.ongoing_count == 0 {
                // Queue is idle - execute inline
                queue_state.ongoing_count = 1;
                BatchExecutionAction::Inline { input }
            } else {
                // Queue is busy - add to the open batch for this batcher
                let entry = queue_state
                    .open_batches
                    .entry(batcher_key)
                    .or_insert_with(|| PendingBatchEntry {
                        batcher_data: Arc::downgrade(&self.data),
                        batch: Batch::default(),
                    });

                entry.batch.inputs.push(input);
                let (output_tx, output_rx) = oneshot::channel();
                entry.batch.output_txs.push(output_tx);
                let num_cancelled_tx = entry.batch.num_cancelled_tx.clone();

                // Check if we need to flush due to max_batch_size
                let should_flush = self
                    .data
                    .max_batch_size
                    .is_some_and(|max_size| entry.batch.inputs.len() >= max_size);

                if should_flush {
                    let entry = queue_state.open_batches.remove(&batcher_key).unwrap();
                    if queue_state.has_slot(self.data.max_in_flight) {
                        // Execute immediately
                        queue_state.ongoing_count += 1;
                        let data = self.data.clone();
                        let slot = InFlightSlot {
                            batcher: self.data.clone(),
                        };
                        tokio::spawn(async move {
                            data.run_batch(entry.batch, slot).await;
                        });
                    } else {
                        // At the bound - wait sealed for a finishing batch
                        queue_state.sealed_batches.push_back(entry);
                    }
                }

                BatchExecutionAction::Batched {
                    output_rx,
                    num_cancelled_tx,
                }
            }
        };

        match batch_exec_action {
            BatchExecutionAction::Inline { input } => {
                let _slot = InFlightSlot {
                    batcher: self.data.clone(),
                };

                let data = self.data.clone();
                let handle = AbortOnDropHandle::new(tokio::spawn(
                    async move {
                        let mut outputs = data.runner.run(vec![input]).await?;
                        if outputs.len() != 1 {
                            internal_bail!("Expected 1 output, got {}", outputs.len());
                        }
                        Ok(outputs.next().unwrap())
                    }
                    .instrument(Span::current()),
                ));
                Ok(handle.await??)
            }
            BatchExecutionAction::Batched {
                output_rx,
                num_cancelled_tx,
            } => await_batched(output_rx, num_cancelled_tx).await,
        }
    }

    /// Gated mode: the input joins the batcher's open batch and the pump
    /// (started here if none is alive) runs it once a slot and the gate allow.
    async fn run_gated(&self, gated: &Gated<R>, input: R::Input) -> Result<R::Output> {
        let (output_rx, num_cancelled_tx) = {
            let mut state = gated.state.lock().unwrap();
            let (output_tx, output_rx) = oneshot::channel();
            state.open.inputs.push(input);
            state.open.output_txs.push(output_tx);
            let num_cancelled_tx = state.open.num_cancelled_tx.clone();
            if self
                .data
                .max_batch_size
                .is_some_and(|max_size| state.open.inputs.len() >= max_size)
            {
                let full = std::mem::take(&mut state.open);
                state.sealed.push_back(full);
            }
            if !state.pump_running {
                state.pump_running = true;
                tokio::spawn(self.data.clone().run_pump());
            }
            (output_rx, num_cancelled_tx)
        };
        await_batched(output_rx, num_cancelled_tx).await
    }
}

/// Waits for a batched call's output, counting the caller as cancelled on the
/// batch if this future is dropped first.
async fn await_batched<O>(
    output_rx: oneshot::Receiver<Result<O>>,
    num_cancelled_tx: watch::Sender<usize>,
) -> Result<O> {
    let mut guard = BatchRecvCancellationGuard::new(Some(num_cancelled_tx));
    let output = output_rx.await?;
    guard.done();
    output
}

/// One counted place among the queue's running batches. Dropping it
/// releases the place and starts what the place allows, under the owning
/// batcher's `max_in_flight`.
struct InFlightSlot<R: Runner + 'static> {
    batcher: Arc<BatcherData<R>>,
}

impl<R: Runner + 'static> Drop for InFlightSlot<R> {
    fn drop(&mut self) {
        let mut queue_state = self
            .batcher
            .queue
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        queue_state.ongoing_count -= 1;
        queue_state.kick_off_pending(self.batcher.max_in_flight);
        self.batcher.queue.slot_freed.notify_one();
    }
}

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
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::sync::oneshot;
    use tokio::time::{Duration, sleep};

    struct TestRunner {
        // Records each call's input values as a vector, in call order
        recorded_calls: Arc<Mutex<Vec<Vec<i64>>>>,
    }

    #[async_trait]
    impl Runner for TestRunner {
        type Input = (i64, oneshot::Receiver<()>);
        type Output = i64;

        async fn run(
            &self,
            inputs: Vec<Self::Input>,
        ) -> Result<impl ExactSizeIterator<Item = Self::Output>> {
            // Record the values for this invocation (order-agnostic)
            let mut values: Vec<i64> = inputs.iter().map(|(v, _)| *v).collect();
            values.sort();
            self.recorded_calls.lock().unwrap().push(values);

            // Split into values and receivers so we can await by value (send-before-wait safe)
            let (vals, rxs): (Vec<i64>, Vec<oneshot::Receiver<()>>) = inputs.into_iter().unzip();

            // Block until every input's signal is fired
            for rx in rxs.into_iter() {
                let _ = rx.await;
            }

            // Return outputs mapping v -> v * 2
            let outputs: Vec<i64> = vals.into_iter().map(|v| v * 2).collect();
            Ok(outputs.into_iter())
        }
    }

    async fn wait_until_len(recorded: &Arc<Mutex<Vec<Vec<i64>>>>, expected_len: usize) {
        for _ in 0..200 {
            // up to ~2s
            if recorded.lock().unwrap().len() == expected_len {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for recorded_calls length {expected_len}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn batches_after_first_inline_call() -> Result<()> {
        let recorded_calls = Arc::new(Mutex::new(Vec::<Vec<i64>>::new()));
        let runner = TestRunner {
            recorded_calls: recorded_calls.clone(),
        };
        let queue = Arc::new(BatchQueue::<TestRunner>::new());
        let batcher = Arc::new(Batcher::new(runner, queue, BatchingOptions::default()));

        let (n1_tx, n1_rx) = oneshot::channel::<()>();
        let (n2_tx, n2_rx) = oneshot::channel::<()>();
        let (n3_tx, n3_rx) = oneshot::channel::<()>();

        // Submit first call; it should execute inline and block on n1
        let b1 = batcher.clone();
        let f1 = tokio::spawn(async move { b1.run((1_i64, n1_rx)).await });

        // Wait until the runner has recorded the first inline call
        wait_until_len(&recorded_calls, 1).await;

        // Submit the next two calls; they should be batched together and not run yet
        let b2 = batcher.clone();
        let f2 = tokio::spawn(async move { b2.run((2_i64, n2_rx)).await });

        let b3 = batcher.clone();
        let f3 = tokio::spawn(async move { b3.run((3_i64, n3_rx)).await });

        // Ensure no new batch has started yet
        {
            let len_now = recorded_calls.lock().unwrap().len();
            assert_eq!(
                len_now, 1,
                "second invocation should not have started before unblocking first"
            );
        }

        // Unblock the first call; this should trigger the next batch of [2,3]
        let _ = n1_tx.send(());

        // Wait for the batch call to be recorded
        wait_until_len(&recorded_calls, 2).await;

        // First result should now be available
        let v1 = f1.await??;
        assert_eq!(v1, 2);

        // The batched call is waiting on n2 and n3; now unblock both and collect results
        let _ = n2_tx.send(());
        let _ = n3_tx.send(());

        let v2 = f2.await??;
        let v3 = f3.await??;
        assert_eq!(v2, 4);
        assert_eq!(v3, 6);

        // Validate the call recording: first [1], then [2, 3]
        let calls = recorded_calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], vec![1]);
        assert_eq!(calls[1], vec![2, 3]);

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn respects_max_batch_size() -> Result<()> {
        let recorded_calls = Arc::new(Mutex::new(Vec::<Vec<i64>>::new()));
        let runner = TestRunner {
            recorded_calls: recorded_calls.clone(),
        };
        let queue = Arc::new(BatchQueue::<TestRunner>::new());
        let batcher = Arc::new(Batcher::new(
            runner,
            queue,
            BatchingOptions {
                max_batch_size: Some(2),
                max_in_flight: None,
                gate: None,
            },
        ));

        let (n1_tx, n1_rx) = oneshot::channel::<()>();
        let (n2_tx, n2_rx) = oneshot::channel::<()>();
        let (n3_tx, n3_rx) = oneshot::channel::<()>();
        let (n4_tx, n4_rx) = oneshot::channel::<()>();

        // Submit first call; it should execute inline and block on n1
        let b1 = batcher.clone();
        let f1 = tokio::spawn(async move { b1.run((1_i64, n1_rx)).await });

        // Wait until the runner has recorded the first inline call
        wait_until_len(&recorded_calls, 1).await;

        // Submit second call; it should be batched
        let b2 = batcher.clone();
        let f2 = tokio::spawn(async move { b2.run((2_i64, n2_rx)).await });

        // Submit third call; this should trigger a flush because max_batch_size=2
        // The batch [2, 3] should be executed immediately
        let b3 = batcher.clone();
        let f3 = tokio::spawn(async move { b3.run((3_i64, n3_rx)).await });

        // Wait for the second batch to be recorded
        wait_until_len(&recorded_calls, 2).await;

        // Verify that the second batch was triggered by max_batch_size
        {
            let calls = recorded_calls.lock().unwrap();
            assert_eq!(calls.len(), 2, "second batch should have started");
            assert_eq!(calls[1], vec![2, 3], "second batch should contain [2, 3]");
        }

        // Submit fourth call; it should wait because there are still ongoing batches
        let b4 = batcher.clone();
        let f4 = tokio::spawn(async move { b4.run((4_i64, n4_rx)).await });

        // Give it a moment to ensure no new batch starts
        sleep(Duration::from_millis(50)).await;
        {
            let len_now = recorded_calls.lock().unwrap().len();
            assert_eq!(
                len_now, 2,
                "third batch should not start until all ongoing batches complete"
            );
        }

        // Unblock the first inline call
        let _ = n1_tx.send(());

        // Wait for first result
        let v1 = f1.await??;
        assert_eq!(v1, 2);

        // Batch [2,3] is still running, so batch [4] shouldn't start yet
        sleep(Duration::from_millis(50)).await;
        {
            let len_now = recorded_calls.lock().unwrap().len();
            assert_eq!(
                len_now, 2,
                "third batch should not start until all ongoing batches complete"
            );
        }

        // Unblock batch [2,3] - this should trigger batch [4] to start
        let _ = n2_tx.send(());
        let _ = n3_tx.send(());

        let v2 = f2.await??;
        let v3 = f3.await??;
        assert_eq!(v2, 4);
        assert_eq!(v3, 6);

        // Now batch [4] should start since all previous batches are done
        wait_until_len(&recorded_calls, 3).await;

        // Unblock batch [4]
        let _ = n4_tx.send(());
        let v4 = f4.await??;
        assert_eq!(v4, 8);

        // Validate the call recording: [1], [2, 3] (flushed by max_batch_size), [4]
        let calls = recorded_calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0], vec![1]);
        assert_eq!(calls[1], vec![2, 3]);
        assert_eq!(calls[2], vec![4]);

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tracks_multiple_concurrent_batches() -> Result<()> {
        let recorded_calls = Arc::new(Mutex::new(Vec::<Vec<i64>>::new()));
        let runner = TestRunner {
            recorded_calls: recorded_calls.clone(),
        };
        let queue = Arc::new(BatchQueue::<TestRunner>::new());
        let batcher = Arc::new(Batcher::new(
            runner,
            queue,
            BatchingOptions {
                max_batch_size: Some(2),
                max_in_flight: None,
                gate: None,
            },
        ));

        let (n1_tx, n1_rx) = oneshot::channel::<()>();
        let (n2_tx, n2_rx) = oneshot::channel::<()>();
        let (n3_tx, n3_rx) = oneshot::channel::<()>();
        let (n4_tx, n4_rx) = oneshot::channel::<()>();
        let (n5_tx, n5_rx) = oneshot::channel::<()>();
        let (n6_tx, n6_rx) = oneshot::channel::<()>();

        // Submit first call - executes inline
        let b1 = batcher.clone();
        let f1 = tokio::spawn(async move { b1.run((1_i64, n1_rx)).await });
        wait_until_len(&recorded_calls, 1).await;

        // Submit calls 2-3 - should batch and flush at max_batch_size
        let b2 = batcher.clone();
        let f2 = tokio::spawn(async move { b2.run((2_i64, n2_rx)).await });
        let b3 = batcher.clone();
        let f3 = tokio::spawn(async move { b3.run((3_i64, n3_rx)).await });
        wait_until_len(&recorded_calls, 2).await;

        // Submit calls 4-5 - should batch and flush at max_batch_size
        let b4 = batcher.clone();
        let f4 = tokio::spawn(async move { b4.run((4_i64, n4_rx)).await });
        let b5 = batcher.clone();
        let f5 = tokio::spawn(async move { b5.run((5_i64, n5_rx)).await });
        wait_until_len(&recorded_calls, 3).await;

        // Submit call 6 - should be batched but not flushed yet
        let b6 = batcher.clone();
        let f6 = tokio::spawn(async move { b6.run((6_i64, n6_rx)).await });

        // Give it a moment to ensure no new batch starts
        sleep(Duration::from_millis(50)).await;
        {
            let len_now = recorded_calls.lock().unwrap().len();
            assert_eq!(
                len_now, 3,
                "fourth batch should not start with ongoing batches"
            );
        }

        // Unblock batch [2, 3] - should not cause [6] to execute yet (batch 1 still ongoing)
        let _ = n2_tx.send(());
        let _ = n3_tx.send(());
        let v2 = f2.await??;
        let v3 = f3.await??;
        assert_eq!(v2, 4);
        assert_eq!(v3, 6);

        sleep(Duration::from_millis(50)).await;
        {
            let len_now = recorded_calls.lock().unwrap().len();
            assert_eq!(
                len_now, 3,
                "batch [6] should still not start (batch 1 and batch [4,5] still ongoing)"
            );
        }

        // Unblock batch [4, 5] - should not cause [6] to execute yet (batch 1 still ongoing)
        let _ = n4_tx.send(());
        let _ = n5_tx.send(());
        let v4 = f4.await??;
        let v5 = f5.await??;
        assert_eq!(v4, 8);
        assert_eq!(v5, 10);

        sleep(Duration::from_millis(50)).await;
        {
            let len_now = recorded_calls.lock().unwrap().len();
            assert_eq!(
                len_now, 3,
                "batch [6] should still not start (batch 1 still ongoing)"
            );
        }

        // Unblock batch 1 - NOW batch [6] should start
        let _ = n1_tx.send(());
        let v1 = f1.await??;
        assert_eq!(v1, 2);

        wait_until_len(&recorded_calls, 4).await;

        // Unblock batch [6]
        let _ = n6_tx.send(());
        let v6 = f6.await??;
        assert_eq!(v6, 12);

        // Validate the call recording
        let calls = recorded_calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 4);
        assert_eq!(calls[0], vec![1]);
        assert_eq!(calls[1], vec![2, 3]);
        assert_eq!(calls[2], vec![4, 5]);
        assert_eq!(calls[3], vec![6]);

        Ok(())
    }

    /// Spawns one `batcher.run` task per value, in order; returns each task's
    /// handle beside the gate that lets its runner call return.
    fn spawn_calls(
        batcher: &Arc<Batcher<TestRunner>>,
        values: impl IntoIterator<Item = i64>,
    ) -> (
        Vec<tokio::task::JoinHandle<Result<i64>>>,
        Vec<oneshot::Sender<()>>,
    ) {
        values
            .into_iter()
            .map(|v| {
                let (tx, rx) = oneshot::channel::<()>();
                let b = batcher.clone();
                (tokio::spawn(async move { b.run((v, rx)).await }), tx)
            })
            .unzip()
    }

    fn release(gates: &mut [Option<oneshot::Sender<()>>], range: std::ops::Range<usize>) {
        for gate in &mut gates[range] {
            let _ = gate.take().expect("gate released twice").send(());
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn max_in_flight_bounds_concurrent_batches() -> Result<()> {
        let recorded_calls = Arc::new(Mutex::new(Vec::<Vec<i64>>::new()));
        let runner = TestRunner {
            recorded_calls: recorded_calls.clone(),
        };
        let queue = Arc::new(BatchQueue::<TestRunner>::new());
        let batcher = Arc::new(Batcher::new(
            runner,
            queue,
            BatchingOptions {
                max_batch_size: Some(2),
                max_in_flight: NonZeroUsize::new(2),
                gate: None,
            },
        ));

        // A burst of 1 + 2 * max_batch_size + 1: inline, two full batches, a remainder.
        let (mut handles, gates) = spawn_calls(&batcher, [1_i64]);
        let mut gates: Vec<_> = gates.into_iter().map(Some).collect();
        wait_until_len(&recorded_calls, 1).await;

        let (more_handles, more_gates) = spawn_calls(&batcher, 2..=6);
        handles.extend(more_handles);
        gates.extend(more_gates.into_iter().map(Some));

        // The first full batch [2, 3] starts beside the inline call; the second
        // full batch [4, 5] fills at the bound and stays sealed; [6] stays open.
        wait_until_len(&recorded_calls, 2).await;
        sleep(Duration::from_millis(50)).await;
        {
            let calls = recorded_calls.lock().unwrap();
            assert_eq!(calls.len(), 2, "a third batch must not start at the bound");
            assert_eq!(calls[1], vec![2, 3]);
        }

        // The inline call finishes: the sealed [4, 5] takes its slot; [6] still waits.
        release(&mut gates, 0..1);
        assert_eq!((&mut handles[0]).await??, 2);
        wait_until_len(&recorded_calls, 3).await;
        sleep(Duration::from_millis(50)).await;
        {
            let calls = recorded_calls.lock().unwrap();
            assert_eq!(calls.len(), 3, "the remainder must wait for a free slot");
            assert_eq!(calls[2], vec![4, 5]);
        }

        // [2, 3] finishes: the remainder [6] starts.
        release(&mut gates, 1..3);
        wait_until_len(&recorded_calls, 4).await;
        assert_eq!(recorded_calls.lock().unwrap()[3], vec![6]);

        release(&mut gates, 3..6);
        for (handle, v) in handles.into_iter().zip(1_i64..).skip(1) {
            assert_eq!(handle.await??, v * 2);
        }
        Ok(())
    }

    /// A burst of 250 at `max_batch_size = 32`: the inline call, seven full
    /// batches and a remainder of 25. Returns the recorded calls and the
    /// handles + gates, with the first eight calls already running.
    async fn start_burst_of_250(
        max_in_flight: Option<NonZeroUsize>,
    ) -> (
        Arc<Mutex<Vec<Vec<i64>>>>,
        Vec<tokio::task::JoinHandle<Result<i64>>>,
        Vec<Option<oneshot::Sender<()>>>,
    ) {
        let recorded_calls = Arc::new(Mutex::new(Vec::<Vec<i64>>::new()));
        let runner = TestRunner {
            recorded_calls: recorded_calls.clone(),
        };
        let queue = Arc::new(BatchQueue::<TestRunner>::new());
        let batcher = Arc::new(Batcher::new(
            runner,
            queue,
            BatchingOptions {
                max_batch_size: Some(32),
                max_in_flight,
                gate: None,
            },
        ));

        let (mut handles, gates) = spawn_calls(&batcher, [1_i64]);
        let mut gates: Vec<_> = gates.into_iter().map(Some).collect();
        wait_until_len(&recorded_calls, 1).await;

        let (more_handles, more_gates) = spawn_calls(&batcher, 2..=250);
        handles.extend(more_handles);
        gates.extend(more_gates.into_iter().map(Some));

        // Seven full batches start beside the inline call (the bound, if any,
        // is 8); the remainder of 25 stays open.
        wait_until_len(&recorded_calls, 8).await;
        sleep(Duration::from_millis(50)).await;
        {
            let calls = recorded_calls.lock().unwrap();
            assert_eq!(calls.len(), 8, "the remainder must not start on its own");
            assert_eq!(calls[0], vec![1]);
            let mut full: Vec<i64> = calls[1..].iter().flatten().copied().collect();
            full.sort();
            assert_eq!(full, (2..=225).collect::<Vec<i64>>());
            assert!(calls[1..].iter().all(|c| c.len() == 32));
        }
        (recorded_calls, handles, gates)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn max_in_flight_starts_remainder_when_any_batch_finishes() -> Result<()> {
        let (recorded_calls, handles, mut gates) = start_burst_of_250(NonZeroUsize::new(8)).await;

        // The inline call finishes while the seven full batches are still
        // blocked on their gates: the remainder takes the freed slot.
        release(&mut gates, 0..1);
        wait_until_len(&recorded_calls, 9).await;
        assert_eq!(
            recorded_calls.lock().unwrap()[8],
            (226..=250).collect::<Vec<i64>>()
        );
        assert!(
            handles[1..225].iter().all(|h| !h.is_finished()),
            "the full batches are still blocked"
        );

        release(&mut gates, 1..250);
        for (handle, v) in handles.into_iter().zip(1_i64..) {
            assert_eq!(handle.await??, v * 2);
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn without_max_in_flight_remainder_waits_for_idle_queue() -> Result<()> {
        let (recorded_calls, handles, mut gates) = start_burst_of_250(None).await;

        // The inline call finishes; seven full batches are still running, so
        // the remainder keeps waiting.
        release(&mut gates, 0..1);
        sleep(Duration::from_millis(50)).await;
        assert_eq!(recorded_calls.lock().unwrap().len(), 8);

        // Six of the seven full batches finish; still not idle.
        release(&mut gates, 1..193);
        sleep(Duration::from_millis(50)).await;
        assert_eq!(recorded_calls.lock().unwrap().len(), 8);

        // The last full batch finishes: the queue is idle, the remainder starts.
        release(&mut gates, 193..225);
        wait_until_len(&recorded_calls, 9).await;
        assert_eq!(
            recorded_calls.lock().unwrap()[8],
            (226..=250).collect::<Vec<i64>>()
        );

        release(&mut gates, 225..250);
        for (handle, v) in handles.into_iter().zip(1_i64..) {
            assert_eq!(handle.await??, v * 2);
        }
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Gated mode
    // ------------------------------------------------------------------------

    /// A gate the test opens one token at a time.
    struct ManualGate {
        tokens: tokio::sync::Semaphore,
    }

    impl ManualGate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                tokens: tokio::sync::Semaphore::new(0),
            })
        }

        fn grant(&self, n: usize) {
            self.tokens.add_permits(n);
        }

        /// Tokens granted and not yet taken by a batch.
        fn spare(&self) -> usize {
            self.tokens.available_permits()
        }
    }

    #[async_trait]
    impl BatchGate for ManualGate {
        async fn acquire(&self) -> Result<()> {
            self.tokens
                .acquire()
                .await
                .map_err(|e| Error::internal_msg(e.to_string()))?
                .forget();
            Ok(())
        }
    }

    struct FailingGate;

    #[async_trait]
    impl BatchGate for FailingGate {
        async fn acquire(&self) -> Result<()> {
            Err(Error::internal_msg("gate closed"))
        }
    }

    struct GatedFixture {
        recorded_calls: Arc<Mutex<Vec<Vec<i64>>>>,
        batcher: Arc<Batcher<TestRunner>>,
        gate: Arc<ManualGate>,
    }

    fn gated_batcher(
        max_batch_size: Option<usize>,
        max_in_flight: Option<NonZeroUsize>,
    ) -> GatedFixture {
        let recorded_calls = Arc::new(Mutex::new(Vec::<Vec<i64>>::new()));
        let gate = ManualGate::new();
        let batcher = Arc::new(Batcher::new(
            TestRunner {
                recorded_calls: recorded_calls.clone(),
            },
            Arc::new(BatchQueue::<TestRunner>::new()),
            BatchingOptions {
                max_batch_size,
                max_in_flight,
                gate: Some(gate.clone()),
            },
        ));
        GatedFixture {
            recorded_calls,
            batcher,
            gate,
        }
    }

    fn recorded(recorded_calls: &Arc<Mutex<Vec<Vec<i64>>>>) -> Vec<Vec<i64>> {
        recorded_calls.lock().unwrap().clone()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gated_first_call_waits_for_the_gate_and_takes_everything_pending() -> Result<()> {
        let GatedFixture {
            recorded_calls,
            batcher,
            gate,
        } = gated_batcher(None, None);

        // No inline call: three calls and no token leave the runner idle.
        let (handles, gates) = spawn_calls(&batcher, 1..=3);
        let mut gates: Vec<_> = gates.into_iter().map(Some).collect();
        sleep(Duration::from_millis(50)).await;
        assert!(
            recorded(&recorded_calls).is_empty(),
            "no batch runs before a token"
        );

        // One token: one batch with every pending call.
        gate.grant(1);
        wait_until_len(&recorded_calls, 1).await;
        assert_eq!(recorded(&recorded_calls), vec![vec![1, 2, 3]]);
        assert_eq!(gate.spare(), 0);

        release(&mut gates, 0..3);
        for (handle, v) in handles.into_iter().zip(1_i64..) {
            assert_eq!(handle.await??, v * 2);
        }
        Ok(())
    }

    /// Readers that read, process, and read again: each round is one batch
    /// with every reader in it, one token each — no size-1 request while
    /// others are pending.
    #[tokio::test(flavor = "current_thread")]
    async fn gated_readers_in_rounds_cost_one_token_per_round() -> Result<()> {
        const READERS: i64 = 4;
        const ROUNDS: i64 = 3;
        let GatedFixture {
            recorded_calls,
            batcher,
            gate,
        } = gated_batcher(None, None);
        let submitted = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // Reader r reads values r*10 + round, one after another.
        let mut release_gates: HashMap<i64, oneshot::Sender<()>> = HashMap::new();
        let mut handles = Vec::new();
        for r in 0..READERS {
            let mut rxs = Vec::new();
            for round in 0..ROUNDS {
                let (tx, rx) = oneshot::channel::<()>();
                release_gates.insert(r * 10 + round, tx);
                rxs.push(rx);
            }
            let b = batcher.clone();
            let submitted = submitted.clone();
            handles.push(tokio::spawn(async move {
                let mut outputs = Vec::new();
                for (round, rx) in rxs.into_iter().enumerate() {
                    submitted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    outputs.push(b.run((r * 10 + round as i64, rx)).await?);
                }
                Ok::<_, Error>(outputs)
            }));
        }

        for round in 0..ROUNDS {
            // Every reader has submitted this round's read before the token.
            while submitted.load(std::sync::atomic::Ordering::SeqCst)
                < ((round + 1) * READERS) as usize
            {
                sleep(Duration::from_millis(5)).await;
            }
            sleep(Duration::from_millis(20)).await;
            assert_eq!(recorded(&recorded_calls).len(), round as usize);
            gate.grant(1);
            wait_until_len(&recorded_calls, round as usize + 1).await;
            let calls = recorded(&recorded_calls);
            let expected: Vec<i64> = (0..READERS).map(|r| r * 10 + round).collect();
            assert_eq!(
                calls[round as usize], expected,
                "round {round} is one full batch"
            );
            assert_eq!(gate.spare(), 0, "round {round} cost exactly one token");
            for v in expected {
                release_gates.remove(&v).unwrap().send(()).unwrap();
            }
        }

        for (r, handle) in handles.into_iter().enumerate() {
            let outputs = handle.await??;
            let expected: Vec<i64> = (0..ROUNDS)
                .map(|round| (r as i64 * 10 + round) * 2)
                .collect();
            assert_eq!(outputs, expected);
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gated_full_batches_wait_in_order_one_token_each() -> Result<()> {
        let GatedFixture {
            recorded_calls,
            batcher,
            gate,
        } = gated_batcher(Some(2), None);
        let (handles, gates) = spawn_calls(&batcher, 1..=5);
        let mut gates: Vec<_> = gates.into_iter().map(Some).collect();
        sleep(Duration::from_millis(50)).await;
        assert!(
            recorded(&recorded_calls).is_empty(),
            "a full batch waits for its token too"
        );

        for (tokens, expected) in [(1, vec![vec![1, 2]]), (2, vec![vec![1, 2], vec![3, 4]])] {
            gate.grant(1);
            wait_until_len(&recorded_calls, tokens).await;
            sleep(Duration::from_millis(20)).await;
            assert_eq!(recorded(&recorded_calls), expected);
        }
        gate.grant(1);
        wait_until_len(&recorded_calls, 3).await;
        assert_eq!(recorded(&recorded_calls)[2], vec![5]);

        release(&mut gates, 0..5);
        for (handle, v) in handles.into_iter().zip(1_i64..) {
            assert_eq!(handle.await??, v * 2);
        }
        Ok(())
    }

    /// Callers that cancel while the pump waits leave an abandoned batch; the
    /// token the pump then gets goes to the next live batch.
    #[tokio::test(flavor = "current_thread")]
    async fn gated_abandoned_batch_is_skipped_and_its_token_reused() -> Result<()> {
        let GatedFixture {
            recorded_calls,
            batcher,
            gate,
        } = gated_batcher(Some(2), None);
        let (handles, gates) = spawn_calls(&batcher, 1..=3);
        let mut gates: Vec<_> = gates.into_iter().map(Some).collect();
        // [1, 2] is sealed, [3] is open, the pump waits at the gate.
        sleep(Duration::from_millis(20)).await;
        handles[0].abort();
        handles[1].abort();
        sleep(Duration::from_millis(20)).await;

        gate.grant(1);
        wait_until_len(&recorded_calls, 1).await;
        assert_eq!(
            recorded(&recorded_calls),
            vec![vec![3]],
            "[1, 2] never runs"
        );
        assert_eq!(gate.spare(), 0, "one token served the live batch");

        release(&mut gates, 2..3);
        let mut handles = handles.into_iter();
        assert!(handles.next().unwrap().await.is_err());
        assert!(handles.next().unwrap().await.is_err());
        assert_eq!(handles.next().unwrap().await??, 6);
        Ok(())
    }

    /// With tokens to spare, `max_in_flight` is what holds the next batch
    /// back: calls arriving during a round trip fill one batch for its end.
    #[tokio::test(flavor = "current_thread")]
    async fn gated_max_in_flight_holds_the_next_batch_until_a_batch_finishes() -> Result<()> {
        let GatedFixture {
            recorded_calls,
            batcher,
            gate,
        } = gated_batcher(None, NonZeroUsize::new(1));
        gate.grant(10);

        let (mut handles, gates) = spawn_calls(&batcher, [1_i64]);
        let mut gates: Vec<_> = gates.into_iter().map(Some).collect();
        wait_until_len(&recorded_calls, 1).await;
        assert_eq!(gate.spare(), 9);

        let (more_handles, more_gates) = spawn_calls(&batcher, 2..=3);
        handles.extend(more_handles);
        gates.extend(more_gates.into_iter().map(Some));
        sleep(Duration::from_millis(50)).await;
        assert_eq!(recorded(&recorded_calls).len(), 1, "the slot is taken");
        assert_eq!(gate.spare(), 9, "no token is taken before a slot");

        release(&mut gates, 0..1);
        wait_until_len(&recorded_calls, 2).await;
        assert_eq!(recorded(&recorded_calls)[1], vec![2, 3]);
        assert_eq!(gate.spare(), 8);

        release(&mut gates, 1..3);
        for (handle, v) in handles.into_iter().zip(1_i64..) {
            assert_eq!(handle.await??, v * 2);
        }
        Ok(())
    }

    /// The pump holds a slot while it waits at the gate; if every caller
    /// cancels meanwhile, the slot is released when the pump exits, so the
    /// next call can run under `max_in_flight = 1`.
    #[tokio::test(flavor = "current_thread")]
    async fn gated_pump_releases_its_slot_when_every_caller_cancels() -> Result<()> {
        let GatedFixture {
            recorded_calls,
            batcher,
            gate,
        } = gated_batcher(None, NonZeroUsize::new(1));
        let (handles, _gates) = spawn_calls(&batcher, [1_i64]);
        sleep(Duration::from_millis(20)).await;
        handles[0].abort();
        sleep(Duration::from_millis(20)).await;

        // The token lets the pump see the abandoned batch and exit.
        gate.grant(1);
        sleep(Duration::from_millis(20)).await;
        assert!(recorded(&recorded_calls).is_empty());

        let (handles, gates) = spawn_calls(&batcher, [2_i64]);
        let mut gates: Vec<_> = gates.into_iter().map(Some).collect();
        gate.grant(1);
        wait_until_len(&recorded_calls, 1).await;
        assert_eq!(recorded(&recorded_calls), vec![vec![2]]);
        release(&mut gates, 0..1);
        assert_eq!(handles.into_iter().next().unwrap().await??, 4);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gated_pump_restarts_after_going_idle() -> Result<()> {
        let GatedFixture {
            recorded_calls,
            batcher,
            gate,
        } = gated_batcher(None, None);
        for (round, v) in [1_i64, 2].into_iter().enumerate() {
            let (handles, gates) = spawn_calls(&batcher, [v]);
            let mut gates: Vec<_> = gates.into_iter().map(Some).collect();
            gate.grant(1);
            wait_until_len(&recorded_calls, round + 1).await;
            release(&mut gates, 0..1);
            assert_eq!(handles.into_iter().next().unwrap().await??, v * 2);
        }
        assert_eq!(recorded(&recorded_calls), vec![vec![1], vec![2]]);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gate_error_fails_every_pending_caller() -> Result<()> {
        let recorded_calls = Arc::new(Mutex::new(Vec::<Vec<i64>>::new()));
        let batcher = Arc::new(Batcher::new(
            TestRunner {
                recorded_calls: recorded_calls.clone(),
            },
            Arc::new(BatchQueue::<TestRunner>::new()),
            BatchingOptions {
                max_batch_size: Some(2),
                max_in_flight: None,
                gate: Some(Arc::new(FailingGate)),
            },
        ));
        let (handles, _gates) = spawn_calls(&batcher, 1..=3);
        for handle in handles {
            let err = handle
                .await?
                .expect_err("the gate's error reaches the caller");
            assert!(err.to_string().contains("gate closed"), "{err}");
        }
        assert!(recorded(&recorded_calls).is_empty());
        Ok(())
    }
}
