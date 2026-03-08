use std::any::Any;
use std::sync::Arc;

use cocoindex_utils::batching::{BatchQueue, Batcher, BatchingOptions, Runner};

use crate::prelude::*;

/// Type-erased body for a write transaction.
/// Runs inside a shared `RwTxn`, returns a boxed output value.
type TxnBody =
    Box<dyn for<'txn> FnOnce(&mut heed::RwTxn<'txn>) -> Result<Box<dyn Any + Send>> + Send>;

struct TxnRunner {
    db_env: heed::Env,
}

#[async_trait]
impl Runner for TxnRunner {
    type Input = TxnBody;
    type Output = Box<dyn Any + Send>;

    async fn run(
        &self,
        inputs: Vec<TxnBody>,
    ) -> Result<impl ExactSizeIterator<Item = Box<dyn Any + Send>>> {
        let mut outputs = Vec::with_capacity(inputs.len());
        let mut wtxn = self.db_env.write_txn()?;
        for body in inputs {
            outputs.push(body(&mut wtxn)?);
        }
        wtxn.commit()?;
        Ok(outputs.into_iter())
    }
}

/// Batches LMDB write transactions: multiple callers' closures run sequentially
/// inside a single `write_txn()` → `commit()` cycle.
///
/// Leverages [`Batcher`] for FIFO scheduling: the first caller executes
/// immediately (inline), while concurrent callers queue up and are flushed
/// together once the current batch commits.
///
/// If any closure in a batch returns `Err`, the whole batch is rolled back
/// (the `RwTxn` is dropped without committing), and every caller in the batch
/// receives an error.
pub struct TxnBatcher {
    inner: Batcher<TxnRunner>,
}

impl TxnBatcher {
    pub fn new(db_env: heed::Env) -> Self {
        let queue = Arc::new(BatchQueue::new());
        Self {
            inner: Batcher::new(TxnRunner { db_env }, queue, BatchingOptions::default()),
        }
    }

    /// Run `body` inside a batched write transaction.
    ///
    /// The body receives an exclusive `&mut RwTxn` shared with other concurrent
    /// callers. If the body returns `Ok(value)`, `value` is returned once the
    /// transaction commits. If it returns `Err`, the transaction is aborted.
    pub async fn run<T: Send + 'static>(
        &self,
        body: impl for<'txn> FnOnce(&mut heed::RwTxn<'txn>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let output = self
            .inner
            .run(Box::new(move |wtxn| {
                Ok(Box::new(body(wtxn)?) as Box<dyn Any + Send>)
            }))
            .await?;
        output
            .downcast::<T>()
            .map(|b| *b)
            .map_err(|_| internal_error!("TxnBatcher: output type mismatch"))
    }
}
