use crate::prelude::*;

use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};

struct WeightedSemaphore {
    downscale_factor: u8,
    downscaled_quota: u32,
    sem: Arc<Semaphore>,
}

impl WeightedSemaphore {
    pub fn new(quota: usize) -> Self {
        let mut downscale_factor = 0;
        let mut downscaled_quota = quota;
        while downscaled_quota > u32::MAX as usize {
            downscaled_quota >>= 1;
            downscale_factor += 1;
        }
        let sem = Arc::new(Semaphore::new(downscaled_quota));
        Self {
            downscaled_quota: downscaled_quota as u32,
            downscale_factor,
            sem,
        }
    }

    async fn acquire_reservation(&self) -> Result<OwnedSemaphorePermit, AcquireError> {
        self.sem.clone().acquire_owned().await
    }

    async fn acquire(
        &self,
        weight: usize,
        reserved: bool,
    ) -> Result<Option<OwnedSemaphorePermit>, AcquireError> {
        let downscaled_weight = (weight >> self.downscale_factor) as u32;
        let capped_weight = downscaled_weight.min(self.downscaled_quota);
        let reserved_weight = if reserved { 1 } else { 0 };
        if reserved_weight >= capped_weight {
            return Ok(None);
        }
        Ok(Some(
            self.sem
                .clone()
                .acquire_many_owned(capped_weight - reserved_weight)
                .await?,
        ))
    }
}

pub struct Options {
    pub max_inflight_rows: Option<usize>,
    pub max_inflight_bytes: Option<usize>,
}

pub struct ConcurrencyControllerPermit {
    _inflight_count_permit: Option<OwnedSemaphorePermit>,
    _inflight_bytes_permit: Option<OwnedSemaphorePermit>,
}

pub struct ConcurrencyController {
    inflight_count_sem: Option<Arc<Semaphore>>,
    inflight_bytes_sem: Option<WeightedSemaphore>,
}

pub static BYTES_UNKNOWN_YET: Option<fn() -> usize> = None;

impl ConcurrencyController {
    pub fn new(exec_options: &Options) -> Self {
        Self {
            inflight_count_sem: exec_options
                .max_inflight_rows
                .map(|max| Arc::new(Semaphore::new(max))),
            inflight_bytes_sem: exec_options.max_inflight_bytes.map(WeightedSemaphore::new),
        }
    }

    /// If `bytes_fn` is `None`, it means the number of bytes is not known yet.
    /// The controller will reserve a minimum number of bytes.
    /// The caller should call `acquire_bytes_with_reservation` with the actual number of bytes later.
    pub async fn acquire(
        &self,
        bytes_fn: Option<impl FnOnce() -> usize>,
    ) -> Result<ConcurrencyControllerPermit, AcquireError> {
        let inflight_count_permit = if let Some(sem) = &self.inflight_count_sem {
            Some(sem.clone().acquire_owned().await?)
        } else {
            None
        };
        let inflight_bytes_permit = if let Some(sem) = &self.inflight_bytes_sem {
            if let Some(bytes_fn) = bytes_fn {
                sem.acquire(bytes_fn(), false).await?
            } else {
                Some(sem.acquire_reservation().await?)
            }
        } else {
            None
        };
        Ok(ConcurrencyControllerPermit {
            _inflight_count_permit: inflight_count_permit,
            _inflight_bytes_permit: inflight_bytes_permit,
        })
    }

    pub async fn acquire_bytes_with_reservation(
        &self,
        bytes_fn: impl FnOnce() -> usize,
    ) -> Result<Option<OwnedSemaphorePermit>, AcquireError> {
        if let Some(sem) = &self.inflight_bytes_sem {
            sem.acquire(bytes_fn(), true).await
        } else {
            Ok(None)
        }
    }
}

pub struct CombinedConcurrencyControllerPermit {
    _permit: ConcurrencyControllerPermit,
    _global_permit: ConcurrencyControllerPermit,
}

pub struct CombinedConcurrencyController {
    controller: ConcurrencyController,
    global_controller: Arc<ConcurrencyController>,
    needs_num_bytes: bool,
}

impl CombinedConcurrencyController {
    pub fn new(exec_options: &Options, global_controller: Arc<ConcurrencyController>) -> Self {
        Self {
            controller: ConcurrencyController::new(exec_options),
            needs_num_bytes: exec_options.max_inflight_bytes.is_some()
                || global_controller.inflight_bytes_sem.is_some(),
            global_controller,
        }
    }

    pub async fn acquire(
        &self,
        bytes_fn: Option<impl FnOnce() -> usize>,
    ) -> Result<CombinedConcurrencyControllerPermit, AcquireError> {
        let num_bytes_fn = if let Some(bytes_fn) = bytes_fn
            && self.needs_num_bytes
        {
            let num_bytes = bytes_fn();
            Some(move || num_bytes)
        } else {
            None
        };

        let permit = self.controller.acquire(num_bytes_fn).await?;
        let global_permit = self.global_controller.acquire(num_bytes_fn).await?;
        Ok(CombinedConcurrencyControllerPermit {
            _permit: permit,
            _global_permit: global_permit,
        })
    }

    pub async fn acquire_bytes_with_reservation(
        &self,
        bytes_fn: impl FnOnce() -> usize,
    ) -> Result<(Option<OwnedSemaphorePermit>, Option<OwnedSemaphorePermit>), AcquireError> {
        let num_bytes = bytes_fn();
        let permit = self
            .controller
            .acquire_bytes_with_reservation(move || num_bytes)
            .await?;
        let global_permit = self
            .global_controller
            .acquire_bytes_with_reservation(move || num_bytes)
            .await?;
        Ok((permit, global_permit))
    }
}
