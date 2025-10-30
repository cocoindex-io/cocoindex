use crate::{prelude::*, service::error::ResidualError};
use tokio::sync::{oneshot, watch};
use tokio_util::task::AbortOnDropHandle;

#[async_trait]
pub trait Runner: Send + Sync {
    type Input: Send;
    type Output: Send;

    async fn run(
        &self,
        inputs: Vec<Self::Input>,
    ) -> Result<impl ExactSizeIterator<Item = Self::Output>>;
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

#[derive(Default)]
enum BatcherState<I, O> {
    #[default]
    Idle,
    Busy(Option<Batch<I, O>>),
}

struct BatcherData<R: Runner + 'static> {
    runner: R,
    state: Mutex<BatcherState<R::Input, R::Output>>,
}

impl<R: Runner + 'static> BatcherData<R> {
    async fn run_batch(self: &Arc<Self>, batch: Batch<R::Input, R::Output>) {
        let _kick_off_next = BatchKickOffNext { batcher_data: self };
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
                        sender.send(Err(anyhow!("{message}"))).ok();
                    }
                    return;
                }
                for (output, sender) in outputs.zip(batch.output_txs) {
                    sender.send(Ok(output)).ok();
                }
            }
            Err(err) => {
                let mut senders_iter = batch.output_txs.into_iter();
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
impl<R: Runner + 'static> Batcher<R> {
    pub fn new(runner: R) -> Self {
        Self {
            data: Arc::new(BatcherData {
                runner,
                state: Mutex::new(BatcherState::Idle),
            }),
        }
    }
    pub async fn run(&self, input: R::Input) -> Result<R::Output> {
        let batch_exec_action: BatchExecutionAction<R> = {
            let mut state = self.data.state.lock().unwrap();
            match &mut *state {
                state @ BatcherState::Idle => {
                    *state = BatcherState::Busy(None);
                    BatchExecutionAction::Inline { input }
                }
                BatcherState::Busy(batch) => {
                    let batch = batch.get_or_insert_default();
                    batch.inputs.push(input);

                    let (output_tx, output_rx) = oneshot::channel();
                    batch.output_txs.push(output_tx);

                    BatchExecutionAction::Batched {
                        output_rx,
                        num_cancelled_tx: batch.num_cancelled_tx.clone(),
                    }
                }
            }
        };
        match batch_exec_action {
            BatchExecutionAction::Inline { input } => {
                let _kick_off_next = BatchKickOffNext {
                    batcher_data: &self.data,
                };

                let data = self.data.clone();
                let handle = AbortOnDropHandle::new(tokio::spawn(async move {
                    let mut outputs = data.runner.run(vec![input]).await?;
                    if outputs.len() != 1 {
                        bail!("Expected 1 output, got {}", outputs.len());
                    }
                    Ok(outputs.next().unwrap())
                }));
                Ok(handle.await??)
            }
            BatchExecutionAction::Batched {
                output_rx,
                num_cancelled_tx,
            } => {
                let mut guard = BatchRecvCancellationGuard::new(Some(num_cancelled_tx));
                let output = output_rx.await?;
                guard.done();
                output
            }
        }
    }
}

struct BatchKickOffNext<'a, R: Runner + 'static> {
    batcher_data: &'a Arc<BatcherData<R>>,
}

impl<'a, R: Runner + 'static> Drop for BatchKickOffNext<'a, R> {
    fn drop(&mut self) {
        let mut state = self.batcher_data.state.lock().unwrap();
        let existing_state = std::mem::take(&mut *state);
        let BatcherState::Busy(Some(batch)) = existing_state else {
            return;
        };
        *state = BatcherState::Busy(None);
        let data = self.batcher_data.clone();
        tokio::spawn(async move { data.run_batch(batch).await });
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

#[cfg(test)]
mod tests {
    use super::*;
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
            let (vals, rxs): (Vec<i64>, Vec<oneshot::Receiver<()>>) =
                inputs.into_iter().map(|(v, rx)| (v, rx)).unzip();

            // Block until every input's signal is fired
            for (_i, rx) in rxs.into_iter().enumerate() {
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
        let batcher = Arc::new(Batcher::new(runner));

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
}
