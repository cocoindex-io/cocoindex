use crate::prelude::*;

use tokio::sync::{Semaphore, SemaphorePermit};

pub struct ConcurrencyController {
    inflight_count_sem: Option<Semaphore>,
}

pub struct ConcurrencyControllerPermit<'a> {
    _inflight_count_permit: Option<SemaphorePermit<'a>>,
}

impl ConcurrencyController {
    pub fn new(max_inflight_count: Option<u32>) -> Self {
        Self {
            inflight_count_sem: max_inflight_count.map(|max| Semaphore::new(max as usize)),
        }
    }

    pub async fn acquire<'a>(&'a self) -> Result<ConcurrencyControllerPermit<'a>> {
        let inflight_count_permit = if let Some(sem) = &self.inflight_count_sem {
            Some(sem.acquire().await?)
        } else {
            None
        };
        Ok(ConcurrencyControllerPermit {
            _inflight_count_permit: inflight_count_permit,
        })
    }
}
