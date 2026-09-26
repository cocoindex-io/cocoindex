use crate::error::{ContextExt, Result};
use crate::{client_bail, internal_error};
use container::SortedVec;
use futures::stream::{FuturesUnordered, StreamExt};
use gpu_capacity::GPUCapacity;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Mutex;
use tokio::sync::oneshot;

/// Tracks fractional GPU capacity across multiple GPUs.
///
/// Each GPU starts with capacity 1.0. ``acquire(fraction)`` blocks until a
/// GPU with enough remaining capacity is available, then returns its id.
/// ``release(gpu_id, fraction)`` restores capacity and wakes waiters.
///
/// The ``detected`` pool size is auto-detected from ``COCOINDEX_NUM_GPUS``,
/// ``CUDA_VISIBLE_DEVICES``, or ``nvidia-smi`` (falling back to 1).
/// Construct with ``GPUPool::new(N)`` to override programmatically.
pub struct GPUPool {
    num_gpus: usize,
    state: Mutex<PoolState>,
}

struct PoolState {
    capacities: SortedVec<GPUCapacity>,
    acquisition_queue: VecDeque<Acquisition>,
}

struct Acquisition {
    demand: GPUCapacity,
    notifier: oneshot::Sender<usize>,
}

struct AcquisitionGuard<'a> {
    pool: &'a GPUPool,
    acquired_gpus: Vec<usize>,
    requested_capacity: GPUCapacity,
    receivers: FuturesUnordered<oneshot::Receiver<usize>>,
}

impl<'a> AcquisitionGuard<'a> {
    pub async fn acquire(&mut self) -> Result<Vec<usize>> {
        while let Some(gpu_id) = self.receivers.next().await {
            self.acquired_gpus.push(gpu_id.map_err(|err| {
                internal_error!("GPUPool reservation cancelled while waiting: {err}")
            })?);
        }
        self.receivers.clear();
        Ok(std::mem::take(&mut self.acquired_gpus))
    }
}

impl<'a> Drop for AcquisitionGuard<'a> {
    fn drop(&mut self) {
        if self.acquired_gpus.is_empty() && self.receivers.is_empty() {
            return;
        }
        let mut state = self.pool.state.lock().unwrap_or_else(|e| e.into_inner());
        for &gpu_id in &self.acquired_gpus {
            let updated = state.capacities[gpu_id]
                .checked_add(&self.requested_capacity)
                .unwrap_or(GPUCapacity::MAX);
            state.capacities.update(gpu_id, updated);
        }
        for mut receiver in std::mem::take(&mut self.receivers) {
            if let Ok(gpu_id) = receiver.try_recv() {
                let updated = state.capacities[gpu_id]
                    .checked_add(&self.requested_capacity)
                    .unwrap_or(GPUCapacity::MAX);
                state.capacities.update(gpu_id, updated);
            }
        }
        GPUPool::process_acquisition_queue(&mut state);
    }
}

impl GPUPool {
    pub fn new(num_gpus: NonZeroUsize) -> Self {
        let num_gpus = num_gpus.get();
        let capacities = std::iter::repeat_n(GPUCapacity::MAX, num_gpus).collect();
        let state = PoolState {
            capacities,
            acquisition_queue: VecDeque::new(),
        };
        GPUPool {
            num_gpus,
            state: Mutex::new(state),
        }
    }

    pub fn num_gpus(&self) -> usize {
        self.num_gpus
    }

    /// acquire a fraction of a GPU, if not available the acquisition is thrown into the queue.
    ///
    /// The function would first attempt to find a GPU with just enough capacity for the demanded
    /// fraction, and the GPU cannot be the top n, in the following rules:
    ///
    /// 1. The `n` is the size of the queue.
    /// 2. The top n are defined by capacities.
    /// 3. The top n are reserved for acquisitions already in the queue.
    ///
    /// Reservation is for an abstract concept,
    /// e.g. "the GPU with the most available capacity" is reserved for the head acquisition in the queue.
    ///
    /// The function would try to host the acquisition using the remaining GPUs,
    /// or it will send it to the queue which will reserve a GPU now or later.
    ///
    /// When the acquisition is cancelled, the GPU capacity is released and the queue is processed.
    pub async fn acquire(&self, fraction: GPUCapacity) -> Result<usize> {
        if fraction == GPUCapacity::ZERO {
            client_bail!("Acquired fraction must be between 0.0 and 1.0, got 0");
        }
        let mut guard = {
            let mut pool = self.state.lock().expect("lock poisoned");
            pool.acquisition_queue
                .retain(|acq| !acq.notifier.is_closed());
            if pool.acquisition_queue.len() < self.num_gpus
                && let Some(gpu_id) = pool
                    .capacities
                    .find_excluding_top_n(&fraction, pool.acquisition_queue.len())
            {
                // excluding top_n, because the acquisitions in the queue have already reserved the top n GPUs.
                let updated_capacity = pool.capacities[gpu_id] - fraction;
                pool.capacities.update(gpu_id, updated_capacity);
                return Ok(gpu_id);
            }
            let receiver = Self::send_acquisition_to_queue(&mut pool.acquisition_queue, fraction);
            AcquisitionGuard {
                pool: self,
                acquired_gpus: vec![],
                requested_capacity: fraction,
                receivers: FuturesUnordered::from_iter(std::iter::once(receiver)),
            }
        };
        let acquired_gpus = guard.acquire().await?;
        debug_assert_eq!(
            acquired_gpus.len(),
            1,
            "Expected one GPU for fraction {}, but got: {:?}",
            fraction,
            &acquired_gpus
        );
        Ok(acquired_gpus[0])
    }

    fn send_acquisition_to_queue(
        acquisition_queue: &mut VecDeque<Acquisition>,
        demand: GPUCapacity,
    ) -> oneshot::Receiver<usize> {
        let (notifier, receiver) = oneshot::channel();
        acquisition_queue.push_back(Acquisition { demand, notifier });
        receiver
    }

    /// Acquires a given integer number of fully available GPUs (capacity == 1.0) from the GPU pool.
    ///
    /// # Error:
    /// * When the given gpu_count is larger than the total gpus, it returns an error.
    ///
    /// # Warning
    /// * When unable to acquire all GPUs, the system will be acquired the ones that can be acquired first.
    ///   For instance, if user attempts to acquire 5 GPUs,
    ///   the function will partially acquire 4 and wait for the last GPU.
    pub async fn acquire_full(&self, gpu_count: NonZeroUsize) -> Result<Vec<usize>> {
        let gpu_count = gpu_count.get();
        if gpu_count > self.num_gpus() {
            client_bail!(
                "Attempted to acquire {} GPUs but only has {}.",
                gpu_count,
                self.num_gpus
            );
        }
        let mut guard = {
            let mut pool = self.state.lock().expect("lock poisoned");
            pool.acquisition_queue
                .retain(|acq| !acq.notifier.is_closed());
            let mut acquired_gpus = Vec::with_capacity(gpu_count);
            if pool.acquisition_queue.len() < self.num_gpus {
                let taken_gpus = pool.capacities.find_many_excluding_top_n(
                    &GPUCapacity::MAX,
                    gpu_count,
                    pool.acquisition_queue.len(),
                );
                for gpu_id in taken_gpus {
                    acquired_gpus.push(gpu_id);
                    pool.capacities.update(gpu_id, GPUCapacity::ZERO);
                }
            }
            if acquired_gpus.len() == gpu_count {
                return Ok(acquired_gpus);
            }
            let gpus_to_be_acquired = gpu_count - acquired_gpus.len();
            let receivers = std::iter::repeat_with(|| {
                Self::send_acquisition_to_queue(&mut pool.acquisition_queue, GPUCapacity::MAX)
            })
            .take(gpus_to_be_acquired)
            .collect::<FuturesUnordered<_>>();
            AcquisitionGuard {
                pool: self,
                acquired_gpus,
                requested_capacity: GPUCapacity::MAX,
                receivers,
            }
        };
        guard.acquire().await
    }

    /// release adds back capacities to GPUs, and processes pending acquisitions afterward.
    ///
    /// # Example
    /// Initially:
    /// ```text
    /// GPUs: G1(capacity=0), G2(capacity=0), G3(capacity=0)
    /// Queue: T1(req=0.7, reserved=[G1]), T2(req=0.5, reserved=[G2])
    /// ```
    /// After releasing 0.5 capacity to G1:
    /// ```text
    /// GPUs: G1(capacity=0.5), G2(capacity=0), G3(capacity=0)
    /// Queue: T1(req=0.7, reserved=[G1]), T2(req=0.5, reserved=[G2])
    /// ```
    /// After releasing 0.6 capacity to G2, T2 will be hosted by G1, then get popped:
    /// ```text
    /// GPUs: G1(capacity=0), G2(capacity=0.6), G3(capacity=0)
    /// Queue: T1(req=0.7, reserved=[G2])
    /// ```
    /// After releasing 0.1 capacity to G2, T1 will be hosted by G2, then get popped:
    /// ```text
    /// GPUs: G1(capacity=0), G2(capacity=0), G3(capacity=0)
    /// Queue: (empty)
    /// ```
    pub fn release(&self, gpu_id: usize, fraction: GPUCapacity) -> Result<()> {
        if gpu_id >= self.num_gpus() {
            client_bail!("Releasing to a gpu_id that does not exist: {gpu_id}",);
        }
        if fraction == GPUCapacity::ZERO {
            client_bail!("Cannot release a zero fraction");
        }
        let mut state = self.state.lock().expect("lock poisoned");
        let updated_capacity = state.capacities[gpu_id].checked_add(&fraction)?;
        state.capacities.update(gpu_id, updated_capacity);
        Self::process_acquisition_queue(&mut state);
        Ok(())
    }

    /// processes pending acquisition queue following the rules:
    ///
    /// 1. The first task always reserves the GPU with the most availability at this moment
    /// 2. Processing does not change the order of pending acquisitions
    ///
    fn process_acquisition_queue(pool: &mut PoolState) {
        pool.acquisition_queue
            .retain(|acq| !acq.notifier.is_closed());
        let length = pool.capacities.len();
        let mut pending_acquisitions = Vec::with_capacity(length);
        while pending_acquisitions.len() < length
            && let Some(acquisition) = pool.acquisition_queue.pop_front()
        {
            if let Some(gpu_id) = pool
                .capacities
                .find_excluding_top_n(&acquisition.demand, pending_acquisitions.len())
            {
                if acquisition.notifier.send(gpu_id).is_ok() {
                    let updated_capacity = pool.capacities[gpu_id] - acquisition.demand;
                    pool.capacities.update(gpu_id, updated_capacity);
                }
            } else {
                pending_acquisitions.push(acquisition);
            }
        }
        while let Some(acquisition) = pending_acquisitions.pop() {
            pool.acquisition_queue.push_front(acquisition);
        }
    }

    /// detect the number of GPUs available for the default pool.
    ///
    /// # Returns:
    /// * number of GPUs
    ///
    /// # Errors:
    /// * failed to find environment variables
    /// * failed to read environment variable values
    /// * failed to parse an environment variable value to a number
    /// * failed to find given commands
    ///
    /// # Detection order:
    ///
    /// 1. ``COCOINDEX_NUM_GPUS`` environment variable (explicit override).
    /// 2. ``CUDA_VISIBLE_DEVICES`` environment variable (count of entries).
    /// 3. ``nvidia-smi`` command output (if available).
    /// 4. Default to ``1``.
    ///
    fn detect_num_gpus() -> Result<usize> {
        if let Some(env_num) = std::env::var("COCOINDEX_NUM_GPUS").ok() {
            return Ok(env_num
                .trim()
                .parse::<usize>()
                .with_context(|| format!("Failed to parse COCOINDEX_NUM_GPUS={env_num}"))?
                .max(1));
        }
        if let Ok(cuda_visible) = std::env::var("CUDA_VISIBLE_DEVICES") {
            let count = cuda_visible
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .count();
            return Ok(std::cmp::max(1, count));
        }
        #[cfg(not(test))]
        let output = Self::call_nvidia_smi(std::time::Duration::from_secs(5));
        #[cfg(test)]
        let output = {
            if std::env::var("MOCK_NVIDIA_SMI_NOT_FOUND").is_ok() {
                Err(crate::error::Error::internal(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "nvidia-smi not found",
                )))
            } else {
                let mock_gpu_count = std::env::var("MOCK_NVIDIA_SMI_STDOUT").unwrap_or_default();
                let mock_exit_code = std::env::var("MOCK_NVIDIA_SMI_EXIT_CODE")
                    .ok()
                    .and_then(|s| s.parse::<i32>().ok())
                    .unwrap_or(0);
                std::process::Command::new("sh")
                    .arg("-c")
                    .arg(format!("echo \"{mock_gpu_count}\"; exit {mock_exit_code}"))
                    .output()
                    .map_err(|e| internal_error!("{e}"))
            }
        };
        let Ok(output) = output else { return Ok(1) };

        if !output.status.success() {
            return Ok(1);
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let count = stdout
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .parse::<usize>()
            .with_context(|| format!("Failed to parse nvidia-smi output: {stdout}"))?;
        Ok(std::cmp::max(1, count))
    }

    #[cfg(not(test))]
    fn call_nvidia_smi(timeout: std::time::Duration) -> Result<std::process::Output> {
        use std::io::Read;
        use std::time::{Duration, Instant};

        let mut child = std::process::Command::new("nvidia-smi")
            .arg("--query-gpu=count")
            .arg("--format=csv,noheader")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;
        let start = Instant::now();
        loop {
            if let Some(status) = child.try_wait()? {
                let mut stdout = Vec::new();
                let mut stderr = Vec::new();
                let _ = child
                    .stdout
                    .take()
                    .and_then(|mut out| out.read_to_end(&mut stdout).ok());
                let _ = child
                    .stderr
                    .take()
                    .and_then(|mut err| err.read_to_end(&mut stderr).ok());
                return Ok(std::process::Output {
                    status,
                    stdout,
                    stderr,
                });
            }
            if start.elapsed() >= timeout {
                let _ = child.kill();
                let reap_deadline = Instant::now() + Duration::from_millis(100);
                while Instant::now() < reap_deadline {
                    if let Ok(Some(_)) = child.try_wait() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                crate::internal_bail!("Timeout waiting for nvidia-smi");
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Detect how many GPUs there are, and create a GPUPool based on the number.
    pub fn detected() -> Result<Self> {
        Ok(Self::new(
            NonZeroUsize::new(Self::detect_num_gpus()?).unwrap(),
        ))
    }
}

pub mod gpu_capacity {
    use crate::client_bail;
    use crate::error::{Error, Result};
    use std::ops::{Add, AddAssign, Sub, SubAssign};

    #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
    pub struct GPUCapacity(u32);

    impl GPUCapacity {
        const SCALE: f32 = 1_000_000.0;
        pub const ZERO: Self = Self(0);
        pub const MAX: Self = Self(Self::SCALE as u32);

        #[cfg(test)]
        pub(crate) fn unchecked(value: f32) -> Self {
            GPUCapacity::try_from(value).expect("Unchecked value initialization should not fail")
        }

        pub fn checked_add(&self, other: &Self) -> Result<Self> {
            if self.0 + other.0 > GPUCapacity::MAX.0 {
                client_bail!(
                    "The sum of {self} and {other} is greater than the max value {}",
                    Self::MAX
                );
            } else {
                Ok(GPUCapacity(self.0 + other.0))
            }
        }
    }

    impl TryFrom<f32> for GPUCapacity {
        type Error = Error;

        fn try_from(value: f32) -> Result<Self, Self::Error> {
            if !(0.0..=1.0).contains(&value) {
                client_bail!("Fraction must be between 0.0 and 1.0, got {value}",);
            }
            Ok(Self((value * Self::SCALE) as u32))
        }
    }

    impl Add for GPUCapacity {
        type Output = Self;

        fn add(self, other: Self) -> Self {
            Self(self.0 + other.0)
        }
    }

    impl AddAssign for GPUCapacity {
        fn add_assign(&mut self, other: Self) {
            self.0 += other.0;
        }
    }

    impl Sub for GPUCapacity {
        type Output = Self;

        fn sub(self, other: Self) -> Self {
            Self(self.0 - other.0)
        }
    }

    impl SubAssign for GPUCapacity {
        fn sub_assign(&mut self, other: Self) {
            self.0 -= other.0;
        }
    }

    impl std::fmt::Display for GPUCapacity {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0 as f32 / Self::SCALE)
        }
    }
}

mod container {
    use std::collections::BTreeSet;

    #[derive(Debug, Default, Clone)]
    pub struct SortedVec<T> {
        values: Vec<T>,
        sorted: BTreeSet<(T, usize)>,
    }

    impl<T> SortedVec<T> {
        pub fn len(&self) -> usize {
            self.values.len()
        }
    }

    impl<T: Clone + Ord> FromIterator<T> for SortedVec<T> {
        fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
            let values = iter.into_iter().collect::<Vec<_>>();
            let sorted = BTreeSet::from_iter(values.iter().cloned().zip(0..));
            Self { values, sorted }
        }
    }

    impl<T: Clone + Ord> SortedVec<T> {
        /// find_excluding_top_n should return the first index which points to minimal value that
        /// is greater or equal to `target`.
        ///
        /// When the target value is greater than all values, return None.
        pub fn find_excluding_top_n(&self, target: &T, top_n: usize) -> Option<usize> {
            self.find_excluding_top_n_iter(target, top_n).next()
        }

        /// find_many_excluding_top_n should return the `count` number of indices
        /// which points to minimal value that is greater or equal to `target`.
        ///
        /// When the target value is greater than all values, return empty vec.
        pub fn find_many_excluding_top_n(
            &self,
            target: &T,
            count: usize,
            top_n: usize,
        ) -> Vec<usize> {
            self.find_excluding_top_n_iter(target, top_n)
                .take(count)
                .collect()
        }

        fn find_excluding_top_n_iter(
            &self,
            target: &T,
            top_n: usize,
        ) -> impl Iterator<Item = usize> {
            let upper_bound = if top_n >= self.sorted.len() {
                None
            } else if top_n > self.sorted.len() / 2 {
                self.sorted.iter().nth(self.sorted.len() - 1 - top_n)
            } else {
                self.sorted.iter().rev().nth(top_n)
            };
            upper_bound
                .into_iter()
                .filter(move |(upper_bound_value, _)| target <= upper_bound_value)
                .flat_map(|upper_bound| self.sorted.range(&(target.clone(), 0)..=upper_bound))
                .map(|(_, index)| *index)
        }

        pub fn update(&mut self, index: usize, value: T) {
            let Some(old_value) = self.values.get_mut(index) else {
                return;
            };
            self.sorted.remove(&(old_value.clone(), index));
            *old_value = value.clone();
            self.sorted.insert((value, index));
        }
    }

    impl<T> std::ops::Index<usize> for SortedVec<T> {
        type Output = T;

        fn index(&self, index: usize) -> &Self::Output {
            &self.values[index]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gpu_pool::container::SortedVec;
    use itertools::Itertools;
    use rand::Rng;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_acquire_returns_gpu_id() -> Result<()> {
        let pool = GPUPool::new(NonZeroUsize::new(2).unwrap());
        let gpu = pool.acquire(GPUCapacity::MAX).await?;
        assert!(gpu < 2);
        pool.release(gpu, GPUCapacity::MAX)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_acquire_different_gpus() -> Result<()> {
        let pool = GPUPool::new(NonZeroUsize::new(2).unwrap());
        let gpu0 = pool.acquire(GPUCapacity::MAX).await?;
        let gpu1 = pool.acquire(GPUCapacity::MAX).await?;
        assert_ne!(gpu0, gpu1);
        pool.release(gpu0, GPUCapacity::MAX)?;
        pool.release(gpu1, GPUCapacity::MAX)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_acquire_blocks_when_capacity_full() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(1).unwrap()));
        let gpu = pool.acquire(GPUCapacity::MAX).await?;

        let cloned_pool = pool.clone();
        let task = tokio::spawn(async move { cloned_pool.acquire(GPUCapacity::MAX).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!task.is_finished());

        pool.release(gpu, GPUCapacity::MAX)?;
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("task finished")?;
        assert!(matches!(result, Ok(0)));
        pool.release(result.unwrap(), GPUCapacity::MAX)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_fractional_shares_same_gpu() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(1).unwrap()));
        let half_fraction = GPUCapacity::try_from(0.5).expect("0.5 is a valid fraction");
        let gpu0 = pool.acquire(half_fraction).await?;
        let gpu1 = pool.acquire(half_fraction).await?;
        assert_eq!(gpu0, gpu1);

        let cloned_pool = pool.clone();
        let task = tokio::spawn(async move { cloned_pool.acquire(half_fraction).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!task.is_finished());

        pool.release(gpu0, half_fraction)?;
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("task finished")?;
        assert!(matches!(result, Ok(0)));
        pool.release(gpu1, half_fraction)?;
        pool.release(result.unwrap(), half_fraction)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_multi_gpu_all_parallel() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(3).unwrap()));
        let mut tasks = Vec::with_capacity(3);
        for _ in 0..3 {
            let pool = pool.clone();
            tasks.push(tokio::spawn(
                async move { pool.acquire(GPUCapacity::MAX).await },
            ));
        }
        let results = futures::future::try_join_all(tasks).await?;
        let gpus = results.into_iter().collect::<Result<Vec<usize>, _>>()?;
        assert_eq!(gpus.len(), 3);
        for g in gpus {
            pool.release(g, GPUCapacity::MAX)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_acquire_fractions_equals_to_zero() {
        let pool = GPUPool::new(NonZeroUsize::new(1).unwrap());
        let result = pool.acquire(GPUCapacity::ZERO).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Acquired fraction must be between 0.0 and 1.0, got 0")
        );
    }

    #[tokio::test]
    async fn test_acquire_fractions_not_enough_with_release_not_enough() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(3).unwrap()));
        let occupied_gpu_1 = pool.acquire(GPUCapacity::unchecked(0.6)).await?;
        assert_eq!(occupied_gpu_1, 0);
        let occupied_gpu_2 = pool.acquire(GPUCapacity::unchecked(0.6)).await?;
        assert_eq!(occupied_gpu_2, 1);
        let cloned_pool = pool.clone();
        let not_enough_task = tokio::spawn(async move {
            cloned_pool
                .acquire_full(NonZeroUsize::new(3).unwrap())
                .await
        });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!not_enough_task.is_finished());
        pool.release(occupied_gpu_2, GPUCapacity::unchecked(0.2))?;
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!not_enough_task.is_finished());
        pool.release(occupied_gpu_2, GPUCapacity::unchecked(0.4))?;
        pool.release(occupied_gpu_1, GPUCapacity::unchecked(0.6))?;
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(not_enough_task.is_finished());
        let gpus = tokio::time::timeout(std::time::Duration::from_secs(1), not_enough_task)
            .await
            .expect("task finished")
            .expect("no timeout")?;
        assert_eq!(gpus.len(), 3);
        for gpu in gpus {
            pool.release(gpu, GPUCapacity::MAX)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_acquire_cancelled_by_caller() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(1).unwrap()));
        let _busy = pool.acquire(GPUCapacity::MAX).await?; // GPU0 fully busy
        let cloned = pool.clone();
        let task = tokio::spawn(async move { cloned.acquire(GPUCapacity::unchecked(0.5)).await });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        task.abort(); // drops future while waiting in queue
        let _ = task.await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let state = pool.state.lock().unwrap();
        assert_eq!(state.acquisition_queue.len(), 0);
        drop(state);
        pool.release(0, GPUCapacity::MAX)?;
        let state = pool.state.lock().unwrap();
        assert_eq!(state.capacities[0], GPUCapacity::MAX);
        assert_eq!(state.acquisition_queue.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_acquire_cancelled_after_assigned() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(1).unwrap()));
        let busy = pool.acquire(GPUCapacity::MAX).await?; // GPU0 fully busy
        let cloned = pool.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _ = tx.send(());
            cloned.acquire(GPUCapacity::unchecked(0.5)).await
        });
        rx.await.unwrap(); // main thread waiting for the `acquire` call
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        // Release capacity so that process_acquisition_queue assigns GPU 0 to the task
        pool.release(busy, GPUCapacity::MAX)?;
        // Immediately abort the task: the assigned GPU fraction must not be leaked!
        task.abort();
        let _ = task.await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let state = pool.state.lock().unwrap();
        assert_eq!(state.capacities[0], GPUCapacity::MAX);
        assert_eq!(state.acquisition_queue.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_acquire_full_gpus_enough() -> Result<()> {
        let pool = GPUPool::new(NonZeroUsize::new(2).unwrap());
        let gpus = pool
            .acquire_full(NonZeroUsize::new(2).expect("2 is not zero"))
            .await?;
        assert_eq!(gpus, vec![0, 1]);
        for g in gpus {
            pool.release(g, GPUCapacity::MAX)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_acquire_full_gpus_not_enough() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(3).unwrap()));
        let partially_used_gpu = pool.acquire(GPUCapacity::unchecked(0.6)).await?;
        assert_eq!(partially_used_gpu, 0);
        let cloned_pool = pool.clone();
        let task = tokio::spawn(async move {
            cloned_pool
                .acquire_full(NonZeroUsize::new(3).expect("3 is not zero"))
                .await
        });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!task.is_finished());
        pool.release(partially_used_gpu, GPUCapacity::unchecked(0.6))?;
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("task finished")
            .expect("no timeout")?;
        assert_eq!(&result, &[1, 2, 0]);
        for gpu in result {
            pool.release(gpu, GPUCapacity::MAX)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_acquire_full_gpus_with_partial_acquiring() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(3).unwrap()));
        let partially_used_gpu = pool.acquire(GPUCapacity::unchecked(0.6)).await?;
        assert_eq!(partially_used_gpu, 0);
        let cloned_pool = pool.clone();
        let task = tokio::spawn(async move {
            cloned_pool
                .acquire_full(NonZeroUsize::new(3).expect("3 is not zero"))
                .await
        });
        let cloned_pool = pool.clone();
        let second_acquired_gpu =
            tokio::spawn(async move { cloned_pool.acquire(GPUCapacity::unchecked(0.2)).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!task.is_finished());
        assert!(!second_acquired_gpu.is_finished());
        pool.release(partially_used_gpu, GPUCapacity::unchecked(0.6))?;
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("task finished")
            .expect("no timeout")?;
        // initial 0.6 occupied index 0, then GPU 1 and 2 are reserved, until 0 is added.
        assert_eq!(&result, &[1, 2, 0]);
        for gpu in result {
            pool.release(gpu, GPUCapacity::MAX)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_acquire_full_cancelled_releases_acquired_gpus() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(3).unwrap()));
        let partially_used_gpu = pool.acquire(GPUCapacity::unchecked(0.6)).await?;
        assert_eq!(partially_used_gpu, 0);

        let cloned_pool = pool.clone();
        let task = tokio::spawn(async move {
            cloned_pool
                .acquire_full(NonZeroUsize::new(3).expect("3 is not zero"))
                .await
        });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!task.is_finished());

        // Verify GPU 1 and 2 were acquired and 1 request is waiting in queue.
        // then cancel
        {
            let mut state = pool.state.lock().expect("lock poisoned");
            assert_eq!(state.capacities[1], GPUCapacity::ZERO);
            assert_eq!(state.capacities[2], GPUCapacity::ZERO);
            assert_eq!(state.acquisition_queue.len(), 1);
            // Cancel the acquisition by dropping the receiver.
            state.acquisition_queue.clear();
        }

        let result = tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("task finished")
            .expect("task did not panic");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("GPUPool reservation cancelled while waiting")
        );

        // Verify all partially acquired GPUs (1 and 2) are released back to full capacity.
        {
            let state = pool.state.lock().expect("lock poisoned");
            assert_eq!(state.capacities[1], GPUCapacity::MAX);
            assert_eq!(state.capacities[2], GPUCapacity::MAX);
            assert_eq!(state.capacities[0], GPUCapacity::unchecked(0.4));
        }

        pool.release(partially_used_gpu, GPUCapacity::unchecked(0.6))?;
        Ok(())
    }

    #[tokio::test]
    async fn test_acquire_full_cancelled_by_caller() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(3).unwrap()));
        let _busy = pool.acquire(GPUCapacity::unchecked(0.6)).await?; // GPU0 partially busy
        let cloned = pool.clone();
        let task =
            tokio::spawn(async move { cloned.acquire_full(NonZeroUsize::new(3).unwrap()).await });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        task.abort(); // == caller-side cancellation: drops the future mid-await
        let _ = task.await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let state = pool.state.lock().unwrap();
        assert_eq!(state.capacities[1], GPUCapacity::MAX);
        assert_eq!(state.capacities[2], GPUCapacity::MAX);
        Ok(())
    }

    #[tokio::test]
    async fn test_acquire_full_cancelled_after_assigned() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(2).unwrap()));
        let g0 = pool.acquire(GPUCapacity::MAX).await?;
        let g1 = pool.acquire(GPUCapacity::MAX).await?;
        // Both GPUs busy.
        let cloned = pool.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _ = tx.send(());
            cloned.acquire_full(NonZeroUsize::new(2).unwrap()).await
        });
        rx.await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        pool.release(g0, GPUCapacity::MAX)?;
        pool.release(g1, GPUCapacity::MAX)?;
        task.abort();
        let _ = task.await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let state = pool.state.lock().unwrap();
        assert_eq!(state.capacities[0], GPUCapacity::MAX);
        assert_eq!(state.capacities[1], GPUCapacity::MAX);
        assert_eq!(state.acquisition_queue.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_acquire_full_cancelled_after_partial_queued_assigned() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(3).unwrap()));
        let g0 = pool.acquire(GPUCapacity::MAX).await?;
        let g1 = pool.acquire(GPUCapacity::MAX).await?;
        // GPU 0 and GPU 1 are busy. GPU 2 is free.
        let cloned = pool.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _ = tx.send(());
            // One acquired, two receivers waiting.
            cloned.acquire_full(NonZeroUsize::new(3).unwrap()).await
        });
        rx.await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        pool.release(g0, GPUCapacity::MAX)?;

        // Only one receiver is waiting at this moment.
        task.abort();
        let _ = task.await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let state = pool.state.lock().unwrap();
        assert_eq!(state.capacities[0], GPUCapacity::MAX);
        assert_eq!(state.capacities[1], GPUCapacity::ZERO);
        assert_eq!(state.capacities[2], GPUCapacity::MAX);
        assert_eq!(state.acquisition_queue.len(), 0);
        drop(state);

        pool.release(g1, GPUCapacity::MAX)?;
        let state = pool.state.lock().unwrap();
        assert_eq!(state.capacities[1], GPUCapacity::MAX);
        Ok(())
    }

    #[tokio::test]
    async fn test_acquire_full_cancellation_unblocks_other_waiters() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(2).unwrap()));
        let g0 = pool.acquire(GPUCapacity::MAX).await?;
        let g1 = pool.acquire(GPUCapacity::MAX).await?;
        // Both GPUs busy.
        let cloned = pool.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let task_full = tokio::spawn(async move {
            let _ = tx.send(());
            cloned.acquire_full(NonZeroUsize::new(2).unwrap()).await
        });
        rx.await.unwrap();

        let cloned = pool.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let task_single = tokio::spawn(async move {
            let _ = tx.send(());
            cloned.acquire(GPUCapacity::MAX).await
        });
        rx.await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!task_single.is_finished());

        // Cancel the task waiting for 2 GPUs
        task_full.abort();
        let _ = task_full.await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Release 1 GPU: task_single should receive it without being blocked by task_full
        pool.release(g0, GPUCapacity::MAX)?;
        let acquired_gpu = tokio::time::timeout(std::time::Duration::from_millis(500), task_single)
            .await
            .expect("did not timeout")
            .unwrap()?;
        assert_eq!(acquired_gpu, 0);

        pool.release(acquired_gpu, GPUCapacity::MAX)?;
        pool.release(g1, GPUCapacity::MAX)?;
        let state = pool.state.lock().unwrap();
        assert_eq!(state.capacities[0], GPUCapacity::MAX);
        assert_eq!(state.capacities[1], GPUCapacity::MAX);
        assert_eq!(state.acquisition_queue.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_acquire_more_gpus_than_allowed() {
        let pool = GPUPool::new(NonZeroUsize::new(2).unwrap());
        let result = pool
            .acquire_full(NonZeroUsize::new(3).expect("3 is not zero"))
            .await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Attempted to acquire 3 GPUs but only has 2.")
        );
    }

    #[tokio::test]
    async fn test_reserve_gpus_then_release() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(2).unwrap()));
        let gpu_0 = pool.acquire(GPUCapacity::unchecked(0.5)).await?;
        assert_eq!(gpu_0, 0);
        let gpu_1 = pool.acquire(GPUCapacity::unchecked(0.6)).await?;
        assert_eq!(gpu_1, 1);
        let cloned_pool = pool.clone();
        let reserving_task_1 =
            tokio::spawn(async move { cloned_pool.acquire(GPUCapacity::unchecked(0.6)).await });
        let cloned_pool = pool.clone();
        let reserving_task_2 =
            tokio::spawn(async move { cloned_pool.acquire(GPUCapacity::unchecked(0.7)).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!reserving_task_1.is_finished());
        assert!(!reserving_task_2.is_finished());

        pool.release(gpu_0, GPUCapacity::unchecked(0.1))?;
        let reserving_task_1_acquired_gpu =
            tokio::time::timeout(std::time::Duration::from_secs(1), reserving_task_1)
                .await
                .expect("task finished")
                .expect("no timeout")?;
        assert_eq!(reserving_task_1_acquired_gpu, gpu_0);
        assert!(!reserving_task_2.is_finished());

        pool.release(gpu_1, GPUCapacity::unchecked(0.3))?;
        let reserving_task_2_acquired_gpu =
            tokio::time::timeout(std::time::Duration::from_secs(1), reserving_task_2)
                .await
                .expect("task finished")
                .expect("no timeout")?;
        assert_eq!(reserving_task_2_acquired_gpu, gpu_1);

        pool.release(gpu_0, GPUCapacity::MAX)?;
        pool.release(gpu_1, GPUCapacity::MAX)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_reserve_gpus_without_affecting_unreserved() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(2).unwrap()));
        let gpu_0 = pool.acquire(GPUCapacity::unchecked(0.5)).await?;
        assert_eq!(gpu_0, 0);
        let gpu_1 = pool.acquire(GPUCapacity::unchecked(0.6)).await?;
        assert_eq!(gpu_1, 1);
        let cloned_pool = pool.clone();
        let reserving_task =
            tokio::spawn(async move { cloned_pool.acquire(GPUCapacity::unchecked(0.6)).await });
        let cloned_pool = pool.clone();
        let task_not_blocked =
            tokio::spawn(async move { cloned_pool.acquire(GPUCapacity::unchecked(0.2)).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!reserving_task.is_finished());
        assert!(task_not_blocked.is_finished());
        pool.release(gpu_0, GPUCapacity::unchecked(0.1))?;

        pool.release(gpu_1, GPUCapacity::unchecked(0.8))?;
        let reserving_task_acquired_gpu =
            tokio::time::timeout(std::time::Duration::from_secs(1), reserving_task)
                .await
                .expect("task finished")
                .expect("no timeout")?;
        assert_eq!(reserving_task_acquired_gpu, gpu_0);

        pool.release(gpu_0, GPUCapacity::MAX)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_reserve_the_same_gpu_in_a_queue() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(1).unwrap()));
        let gpu_0 = pool.acquire(GPUCapacity::unchecked(0.5)).await?;
        assert_eq!(gpu_0, 0);
        let cloned_pool = pool.clone();
        let reserving_task_1 =
            tokio::spawn(async move { cloned_pool.acquire(GPUCapacity::unchecked(0.6)).await });
        let cloned_pool = pool.clone();
        let reserving_task_2 =
            tokio::spawn(async move { cloned_pool.acquire(GPUCapacity::unchecked(0.7)).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!reserving_task_1.is_finished());
        assert!(!reserving_task_2.is_finished());

        pool.release(gpu_0, GPUCapacity::unchecked(0.1))?;
        let reserving_task_1_acquired_gpu =
            tokio::time::timeout(std::time::Duration::from_secs(1), reserving_task_1)
                .await
                .expect("task finished")
                .expect("no timeout")?;
        assert_eq!(reserving_task_1_acquired_gpu, gpu_0);
        assert!(!reserving_task_2.is_finished());

        pool.release(gpu_0, GPUCapacity::unchecked(0.7))?;
        let reserving_task_2_acquired_gpu =
            tokio::time::timeout(std::time::Duration::from_secs(1), reserving_task_2)
                .await
                .expect("task finished")
                .expect("no timeout")?;
        assert_eq!(reserving_task_2_acquired_gpu, gpu_0);

        pool.release(gpu_0, GPUCapacity::MAX)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_reserve_front_queue_not_block_later_items() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(2).unwrap()));
        let gpu_1 = pool.acquire(GPUCapacity::unchecked(0.5)).await?;
        let gpu_2 = pool.acquire(GPUCapacity::unchecked(0.8)).await?;
        let cloned_pool = pool.clone();
        let task_1 =
            tokio::spawn(async move { cloned_pool.acquire(GPUCapacity::unchecked(0.6)).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!task_1.is_finished());
        let cloned_pool = pool.clone();
        let task_2 =
            tokio::spawn(async move { cloned_pool.acquire(GPUCapacity::unchecked(0.4)).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!task_2.is_finished());

        pool.release(gpu_2, GPUCapacity::unchecked(0.2))?;
        let task_2_acquired_gpu = tokio::time::timeout(std::time::Duration::from_secs(1), task_2)
            .await
            .expect("task finished")
            .expect("no timeout")?;
        assert_eq!(task_2_acquired_gpu, gpu_2);
        assert!(!task_1.is_finished());

        pool.release(gpu_1, GPUCapacity::unchecked(0.5))?;
        let task_1_acquired_gpu = tokio::time::timeout(std::time::Duration::from_secs(1), task_1)
            .await
            .expect("task finished")
            .expect("no timeout")?;
        assert_eq!(task_1_acquired_gpu, gpu_1);

        pool.release(gpu_2, GPUCapacity::MAX)
    }

    #[tokio::test]
    async fn test_reserve_queue_assigned_task_not_blocking() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(2).unwrap()));
        let gpu_1 = pool.acquire(GPUCapacity::unchecked(0.3)).await?;
        let gpu_2 = pool.acquire(GPUCapacity::unchecked(0.8)).await?;
        let cloned_pool = pool.clone();
        let task_1 = tokio::spawn(async move { cloned_pool.acquire(GPUCapacity::MAX).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!task_1.is_finished());
        let cloned_pool = pool.clone();
        let task_2 =
            tokio::spawn(async move { cloned_pool.acquire(GPUCapacity::unchecked(0.4)).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!task_2.is_finished());
        let cloned_pool = pool.clone();
        let task_3 =
            tokio::spawn(async move { cloned_pool.acquire(GPUCapacity::unchecked(0.2)).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!task_3.is_finished());

        pool.release(gpu_2, GPUCapacity::unchecked(0.4))?;
        assert!(!task_1.is_finished());
        let task_2_acquired_gpu = tokio::time::timeout(std::time::Duration::from_secs(1), task_2)
            .await
            .expect("task finished")
            .expect("no timeout")?;
        assert_eq!(task_2_acquired_gpu, gpu_2);
        let task_3_acquired_gpu = tokio::time::timeout(std::time::Duration::from_secs(1), task_3)
            .await
            .expect("task finished")
            .expect("no timeout")?;
        assert_eq!(task_3_acquired_gpu, gpu_2);

        pool.release(gpu_1, GPUCapacity::unchecked(0.3))?;
        let task_1_acquired_gpu = tokio::time::timeout(std::time::Duration::from_secs(1), task_1)
            .await
            .expect("task finished")
            .expect("no timeout")?;
        assert_eq!(task_1_acquired_gpu, gpu_1);

        pool.release(gpu_2, GPUCapacity::MAX)
    }

    #[tokio::test]
    async fn test_release_gpus() -> Result<()> {
        let pool = GPUPool::new(NonZeroUsize::new(1).unwrap());
        let gpu_0 = pool.acquire(GPUCapacity::unchecked(0.5)).await?;
        assert_eq!(gpu_0, 0);
        pool.release(gpu_0, GPUCapacity::unchecked(0.5))?;
        Ok(())
    }

    #[tokio::test]
    async fn test_release_to_wrong_gpu_id() {
        let pool = GPUPool::new(NonZeroUsize::new(1).unwrap());
        let release_result = pool.release(1, GPUCapacity::unchecked(0.5));
        assert!(release_result.is_err());
        assert!(
            release_result
                .unwrap_err()
                .to_string()
                .contains("Releasing to a gpu_id that does not exist: 1")
        );
    }

    #[tokio::test]
    async fn test_release_zero_fraction() {
        let pool = GPUPool::new(NonZeroUsize::new(1).unwrap());
        let release_result = pool.release(0, GPUCapacity::ZERO);
        assert!(release_result.is_err());
        assert!(
            release_result
                .unwrap_err()
                .to_string()
                .contains("Cannot release a zero fraction")
        );
    }

    #[tokio::test]
    async fn test_release_overflown_gpus() -> Result<()> {
        let pool = GPUPool::new(NonZeroUsize::new(1).unwrap());
        let gpu_0 = pool.acquire(GPUCapacity::unchecked(0.5)).await?;
        assert_eq!(gpu_0, 0);
        let release_result = pool.release(gpu_0, GPUCapacity::unchecked(0.6));
        assert!(release_result.is_err());
        assert!(
            release_result
                .unwrap_err()
                .to_string()
                .contains("The sum of 0.5 and 0.6 is greater than the max value 1")
        );
        Ok(())
    }

    #[test]
    fn test_detect_num_gpus_explicit_env() {
        temp_env::with_vars(
            [
                ("COCOINDEX_NUM_GPUS", Some("4")),
                ("CUDA_VISIBLE_DEVICES", None),
            ],
            || {
                let pool = GPUPool::detected().unwrap();
                assert_eq!(pool.num_gpus(), 4);
            },
        );
    }

    #[test]
    fn test_detect_num_gpus_cuda_visible_devices() {
        temp_env::with_vars(
            [
                ("CUDA_VISIBLE_DEVICES", Some("0,2,3")),
                ("COCOINDEX_NUM_GPUS", None),
            ],
            || {
                let pool = GPUPool::detected().unwrap();
                assert_eq!(pool.num_gpus(), 3);
            },
        );
    }

    #[test]
    fn test_detect_num_gpus_cuda_visible_empty() {
        temp_env::with_vars(
            [
                ("CUDA_VISIBLE_DEVICES", Some("")),
                ("COCOINDEX_NUM_GPUS", None),
            ],
            || {
                let pool = GPUPool::detected().unwrap();
                assert_eq!(pool.num_gpus(), 1);
            },
        );
    }

    #[test]
    fn test_detect_num_gpus_explicit_env_zero() {
        temp_env::with_vars(
            [
                ("CUDA_VISIBLE_DEVICES", None),
                ("COCOINDEX_NUM_GPUS", Some("0")),
            ],
            || {
                let pool = GPUPool::detected().unwrap();
                assert_eq!(pool.num_gpus(), 1);
            },
        );
    }

    #[test]
    fn test_detect_num_gpus_parse_error() {
        temp_env::with_vars([("COCOINDEX_NUM_GPUS", Some("test"))], || {
            let pool_result = GPUPool::detected();
            assert!(pool_result.is_err());
            let error_msg = pool_result.err().unwrap().to_string();
            assert!(error_msg.contains("Failed to parse COCOINDEX_NUM_GPUS=test"));
            assert!(error_msg.contains("invalid digit found in string"));
        });
    }

    #[test]
    fn test_detect_num_gpus_explicit_env_overrides_cuda_visible() {
        temp_env::with_vars(
            [
                ("CUDA_VISIBLE_DEVICES", Some("0,1,2,3")),
                ("COCOINDEX_NUM_GPUS", Some("2")),
            ],
            || {
                let pool = GPUPool::detected().unwrap();
                assert_eq!(pool.num_gpus(), 2);
            },
        );
    }

    #[test]
    fn test_detect_num_gpus_cuda_visible_single_device() {
        temp_env::with_vars(
            [
                ("CUDA_VISIBLE_DEVICES", Some("0")),
                ("COCOINDEX_NUM_GPUS", None),
            ],
            || {
                let pool = GPUPool::detected().unwrap();
                assert_eq!(pool.num_gpus(), 1);
            },
        );
    }

    #[test]
    fn test_detect_num_gpus_cuda_visible_with_whitespace() {
        temp_env::with_vars(
            [
                ("CUDA_VISIBLE_DEVICES", Some("0, 1 , 2")),
                ("COCOINDEX_NUM_GPUS", None),
            ],
            || {
                let pool = GPUPool::detected().unwrap();
                assert_eq!(pool.num_gpus(), 3);
            },
        );
    }

    #[test]
    fn test_detect_num_gpus_nvidia_smi_returns_count() {
        temp_env::with_vars(
            [
                ("MOCK_NVIDIA_SMI_STDOUT", Some("8")),
                ("CUDA_VISIBLE_DEVICES", None),
                ("COCOINDEX_NUM_GPUS", None),
            ],
            || {
                let pool = GPUPool::detected().unwrap();
                assert_eq!(pool.num_gpus(), 8);
            },
        );
    }

    #[test]
    fn test_detect_num_gpus_nvidia_smi_empty_output() {
        temp_env::with_vars_unset(["CUDA_VISIBLE_DEVICES", "COCOINDEX_NUM_GPUS"], || {
            let detect_result = GPUPool::detected();
            assert!(detect_result.is_err());
            let error_msg = detect_result.err().unwrap().to_string();
            assert!(error_msg.contains("Failed to parse nvidia-smi output: "));
        })
    }

    #[test]
    fn test_detect_num_gpus_nvidia_smi_nonzero_exit() {
        temp_env::with_vars(
            [
                ("MOCK_NVIDIA_SMI_STDOUT", Some("8")),
                ("MOCK_NVIDIA_SMI_EXIT_CODE", Some("1")),
                ("CUDA_VISIBLE_DEVICES", None),
                ("COCOINDEX_NUM_GPUS", None),
            ],
            || {
                let pool = GPUPool::detected().unwrap();
                assert_eq!(pool.num_gpus(), 1);
            },
        );
    }

    #[test]
    fn test_detect_num_gpus_nvidia_smi_not_found() {
        temp_env::with_vars(
            [
                ("MOCK_NVIDIA_SMI_NOT_FOUND", Some("1")),
                ("MOCK_NVIDIA_SMI_STDOUT", Some("8")),
                ("CUDA_VISIBLE_DEVICES", None),
                ("COCOINDEX_NUM_GPUS", None),
            ],
            || {
                let pool = GPUPool::detected().unwrap();
                assert_eq!(pool.num_gpus(), 1);
            },
        );
    }

    #[test]
    fn test_detect_num_gpus_all_missing_fallback() {
        temp_env::with_vars(
            [
                ("MOCK_NVIDIA_SMI_EXIT_CODE", Some("1")),
                ("MOCK_NVIDIA_SMI_STDOUT", None),
                ("CUDA_VISIBLE_DEVICES", None),
                ("COCOINDEX_NUM_GPUS", None),
            ],
            || {
                let pool = GPUPool::detected().unwrap();
                assert_eq!(pool.num_gpus(), 1);
            },
        );
    }

    #[test]
    fn test_gpu_capacity_larger_than_one() {
        let result = GPUCapacity::try_from(1.1);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Fraction must be between 0.0 and 1.0, got 1.1")
        );
    }

    #[test]
    fn test_gpu_capacity_less_than_zero() {
        let result = GPUCapacity::try_from(-1.1);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Fraction must be between 0.0 and 1.0, got -1.1")
        );
    }

    #[test]
    fn test_gpu_capacity_zero() -> Result<()> {
        let half = GPUCapacity::try_from(0.5)?;
        let result = GPUCapacity::MAX - half + half - GPUCapacity::ZERO;
        assert_eq!(result, GPUCapacity::MAX);
        Ok(())
    }

    #[test]
    fn test_gpu_capacity_repeat_acquire_then_release() -> Result<()> {
        let mut full = GPUCapacity::MAX;
        let mut rng = rand::rng();
        for _ in 0..100_000 {
            let random_portion: f32 = rng.random_range(0.0..=1.0);
            full -= GPUCapacity::try_from(random_portion)?;
            full += GPUCapacity::try_from(random_portion)?;
        }
        assert_eq!(full, GPUCapacity::MAX);
        Ok(())
    }

    #[test]
    fn test_gpu_capacity_repeat_acquire_then_release_later() -> Result<()> {
        let mut full = GPUCapacity::MAX;
        let mut rng = rand::rng();
        let mut random_capacities = vec![];
        for _ in 0..100_000 {
            let random_portion: f32 = rng.random_range(0.0..=1.0);
            let capacity = GPUCapacity::try_from(random_portion)?;
            if full <= capacity {
                for cap in &random_capacities {
                    full += *cap;
                }
                assert_eq!(
                    full,
                    GPUCapacity::MAX,
                    "full ({full}) + sum({random_capacities:?}) != 1.0 (should be 1.0)",
                );
                random_capacities.clear();
            }
            random_capacities.push(capacity);
            full -= capacity;
        }
        for cap in &random_capacities {
            full += *cap;
        }
        assert_eq!(
            full,
            GPUCapacity::MAX,
            "full ({full}) + sum({random_capacities:?}) != 1.0 (should be 1.0) (final)"
        );
        Ok(())
    }

    #[test]
    fn test_sorted_vec_find_lowest_index() {
        let original = [1; 10];
        let capacity = SortedVec::from_iter(original);
        let index = capacity.find_excluding_top_n(&1, 0);
        assert_eq!(index, Some(0));
    }

    #[test]
    fn test_sorted_vec_find_missing() {
        let original = [4, 3, 0];
        let capacity = SortedVec::from_iter(original); // [0, 3, 4]
        let index = capacity.find_excluding_top_n(&1, 0);
        let expected = original.iter().position(|x| *x == 3);
        assert_eq!(index, expected);
    }

    #[test]
    fn test_sorted_vec_find_exact() {
        let original = [4, 3, 0];
        let capacity = SortedVec::from_iter(original);
        let index = capacity.find_excluding_top_n(&3, 0);
        let expected = original.iter().position(|x| *x == 3);
        assert_eq!(index, expected);
    }

    #[test]
    fn test_sorted_vec_find_over_max() {
        let capacity = SortedVec::from_iter([0, 3, 4].into_iter().rev());
        let index = capacity.find_excluding_top_n(&i32::MAX, 0);
        assert_eq!(index, None);
    }

    #[test]
    fn test_sorted_vec_find_empty() {
        let capacity = SortedVec::<usize>::from_iter([]);
        let index = capacity.find_excluding_top_n(&3, 0);
        assert_eq!(index, None);
    }

    #[test]
    fn test_sorted_vec_first_excluding_top_n_found() {
        let original = (0..10).rev().collect::<Vec<_>>();
        let capacity = SortedVec::from_iter(original.clone());
        let index = capacity.find_excluding_top_n(&5, 3);
        let expected = original.iter().position(|x| *x == 5);
        assert_eq!(index, expected);
    }

    #[test]
    fn test_sorted_vec_first_excluding_top_n_found_repeated() {
        let capacity = SortedVec::from_iter([1; 10]);
        let index = capacity.find_excluding_top_n(&1, 3);
        assert_eq!(index, Some(0));
    }

    #[test]
    fn test_sorted_vec_first_excluding_top_n_excluded() {
        let capacity = SortedVec::from_iter((0..10).rev());
        let index = capacity.find_excluding_top_n(&5, 6);
        assert_eq!(index, None);
    }

    #[test]
    fn test_sorted_vec_first_excluding_top_n_missing_excluded() {
        let capacity = SortedVec::from_iter([0, 4, 3, 5]); // [0, 3, 4, 5]
        let index = capacity.find_excluding_top_n(&2, 3);
        assert_eq!(index, None);
    }

    #[test]
    fn test_sorted_vec_first_excluding_top_n_missing_found() {
        let original = [0, 19, 15, 9, 20];
        let capacity = SortedVec::from_iter(original); // [0, 9, 15, 19, 20]
        let index = capacity.find_excluding_top_n(&9, 2);
        let expected = original.iter().position(|x| *x == 9);
        assert_eq!(index, expected);
    }

    #[test]
    fn test_sorted_vec_take_excluding_top_n_success() {
        let original = [20, 19, 15, 9, 20, 20, 11, 20];
        let capacity = SortedVec::from_iter(original);
        let indices = capacity.find_many_excluding_top_n(&20, usize::MAX, 0);
        let expected = original.iter().positions(|x| *x == 20).collect::<Vec<_>>();
        assert_eq!(indices, expected);
    }

    #[test]
    fn test_sorted_vec_take_excluding_top_n_excluded() {
        let original = [20, 19, 15, 9, 20, 20, 11, 20];
        let capacity = SortedVec::from_iter(original);
        let indices = capacity.find_many_excluding_top_n(&20, usize::MAX, 1);
        let mut expected = original.iter().positions(|x| *x == 20).collect::<Vec<_>>();
        expected.pop();
        assert_eq!(indices, expected);
    }

    #[test]
    fn test_sorted_vec_take_excluding_top_n_take_few() {
        let original = [20, 19, 15, 9, 20, 20, 11, 20];
        let capacity = SortedVec::from_iter(original);
        let indices = capacity.find_many_excluding_top_n(&20, 2, 0);
        let expected = original.iter().positions(|x| *x == 20).collect::<Vec<_>>();
        assert_eq!(indices, expected[..2]);
    }

    #[test]
    fn test_sorted_vec_take_excluding_top_n_take_close_values() {
        let original = [20, 19, 15, 9, 20, 20, 11, 20];
        let capacity = SortedVec::from_iter(original);
        let indices = capacity.find_many_excluding_top_n(&19, usize::MAX, 0);
        let expected = original
            .iter()
            .positions(|x| *x == 19)
            .chain(original.iter().positions(|x| *x == 20))
            .collect::<Vec<_>>();
        assert_eq!(indices, expected);
    }
}
