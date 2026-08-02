use crate::error::Result;
use crate::{client_bail, internal_error};
use container::SortedVec;
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
/// The default pool size is auto-detected from ``COCOINDEX_NUM_GPUS``,
/// ``CUDA_VISIBLE_DEVICES``, or ``nvidia-smi`` (falling back to 1).
/// Call ``configure_gpu_pool(N)`` to override programmatically.
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
    pub async fn acquire(&self, fraction: GPUCapacity) -> Result<usize> {
        if fraction == GPUCapacity::ZERO {
            client_bail!("Acquired fraction must be between 0.0 and 1.0, got 0");
        }
        let receiver = {
            let mut pool = self.state.lock().expect("lock poisoned");
            if pool.acquisition_queue.len() < self.num_gpus
                && let Some(gpu_id) = pool
                    .capacities
                    .excluding_top_n(pool.acquisition_queue.len())
                    .find(&fraction)
            {
                // excluding top_n, because the acquisitions in the queue have already reserved the top n GPUs.
                let updated_capacity = pool.capacities[gpu_id] - fraction;
                pool.capacities.update(gpu_id, updated_capacity);
                return Ok(gpu_id);
            }
            Self::send_acquisition_to_queue(&mut pool.acquisition_queue, fraction)
        };
        receiver
            .await
            .map_err(|err| internal_error!("GPUPool dropped while waiting: {err}"))
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
        let (mut acquired_gpus, receivers) = {
            let mut pool = self.state.lock().expect("lock poisoned");
            let mut acquired_gpus = Vec::with_capacity(gpu_count);
            if pool.acquisition_queue.len() < self.num_gpus {
                let found_gpus = pool
                    .capacities
                    .excluding_top_n(pool.acquisition_queue.len())
                    .find_n(&GPUCapacity::MAX, gpu_count);
                for gpu_id in found_gpus {
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
            .collect::<Vec<_>>();
            (acquired_gpus, receivers)
        };
        let reserve_gpus = receivers
            .into_iter()
            .map(|receiver| async move { receiver.await });
        let gpu_ids = futures::future::try_join_all(reserve_gpus).await?;
        acquired_gpus.extend(gpu_ids);
        Ok(acquired_gpus)
    }

    /// release adds back capacities to GPUs, and processes pending acquisitions afterward.
    ///
    /// # Example
    /// Initially:
    /// ```text
    /// GPUs: G1(capacity=0), G2(capacity=0), G3(capacity=0)
    /// Queue: T1(req=0.7, reserved=[G1]) T2(req=0.5, reserved=[G2])
    /// ```
    /// After releasing 0.5 capacity to G1:
    /// ```text
    /// GPUs: G1(capacity=0.5), G2(capacity=0), G3(capacity=0)
    /// Queue: T1(req=0.7, reserved=[G1]), T2(req=0.5, reserved=[G2])
    /// ```
    /// After releasing 0.6 capacity to G2:
    /// ```text
    /// GPUs: G1(capacity=0.5), G2(capacity=0.6), G3(capacity=0)
    /// Queue: T1(req=0.7, reserved=[G2]), T2(req=0.5, reserved=[G1])
    /// ```
    /// After releasing 0.1 capacity to G2, T1 will be hosted by G2, then get popped:
    /// ```text
    /// GPUs: G1(capacity=0.5), G2(capacity=0), G3(capacity=0)
    /// Queue: T2(req=0.5, reserved=[G1])
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
    /// 1. strict FIFO - when acquisition A comes in earlier than acquisition B, acquisition A always gets processed earlier.
    /// 2. only pop if available - the acquisition remains in the queue until a GPU has enough availability to process it.
    ///
    fn process_acquisition_queue(pool: &mut PoolState) {
        while let Some(pending_acquisition) = pool.acquisition_queue.front()
            && let Some(gpu_id) = pool.capacities.find(&pending_acquisition.demand)
        {
            let pending_acquisition = pool.acquisition_queue.pop_front().unwrap();
            if pending_acquisition.notifier.send(gpu_id).is_ok() {
                let updated_capacity = pool.capacities[gpu_id] - pending_acquisition.demand;
                pool.capacities.update(gpu_id, updated_capacity);
            }
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
        if let Some(env_num) = std::env::var("COCOINDEX_NUM_GPUS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            return Ok(std::cmp::max(1, env_num));
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
        let output = std::process::Command::new("nvidia-smi")
            .arg("--query-gpu=count")
            .arg("--format=csv,noheader")
            .output()?;
        #[cfg(test)]
        let output = {
            if std::env::var("MOCK_NVIDIA_SMI_NOT_FOUND").is_ok() {
                return Err(crate::error::Error::internal(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "nvidia-smi not found",
                )));
            }
            let mock_gpu_count = std::env::var("MOCK_NVIDIA_SMI_STDOUT").unwrap_or_default();
            let mock_exit_code = std::env::var("MOCK_NVIDIA_SMI_EXIT_CODE")
                .ok()
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0);
            std::process::Command::new("sh")
                .arg("-c")
                .arg(format!("echo \"{mock_gpu_count}\"; exit {mock_exit_code}"))
                .output()
        }?;

        if !output.status.success() {
            return Ok(1);
        }
        let count = String::from_utf8_lossy(&output.stdout)
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .parse::<usize>()?;
        Ok(std::cmp::max(1, count))
    }
}

impl Default for GPUPool {
    fn default() -> Self {
        Self::new(NonZeroUsize::new(Self::detect_num_gpus().unwrap_or(1)).unwrap())
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

    impl<T: Clone + Ord> FromIterator<T> for SortedVec<T> {
        fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
            let values = iter.into_iter().collect::<Vec<_>>();
            let sorted = BTreeSet::from_iter(values.iter().cloned().zip(0..));
            Self { values, sorted }
        }
    }

    impl<T: Clone + Ord> SortedVec<T> {
        /// find should return the index where the index points to minimal value that
        /// is greater or equal to `target`.
        ///
        /// When the target value is greater than all values, return None.
        pub fn find(&self, target: &T) -> Option<usize> {
            self.sorted
                .range(&(target.clone(), 0)..)
                .next()
                .map(|(_, index)| *index)
        }

        pub fn excluding_top_n<'a>(&'a self, top_n: usize) -> SplitSortedVec<'a, T> {
            let upper_bound = if top_n >= self.sorted.len() {
                None
            } else if top_n > self.sorted.len() / 2 {
                self.sorted.iter().nth(self.sorted.len() - 1 - top_n)
            } else {
                self.sorted.iter().rev().nth(top_n)
            };
            SplitSortedVec {
                source: &self.sorted,
                upper_bound: upper_bound,
            }
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

    pub(crate) struct SplitSortedVec<'a, T> {
        source: &'a BTreeSet<(T, usize)>,
        upper_bound: Option<&'a (T, usize)>,
    }

    impl<'a, T: Ord + Clone> SplitSortedVec<'a, T> {
        pub fn find(&self, target: &T) -> Option<usize> {
            self.find_n_iter(target, 1).next()
        }

        pub fn find_n(&self, target: &T, count: usize) -> Vec<usize> {
            self.find_n_iter(target, count).collect()
        }

        fn find_n_iter(&self, target: &T, count: usize) -> impl Iterator<Item = usize> {
            (self.upper_bound.is_some() && target <= &self.upper_bound.unwrap().0)
                .then(|| {
                    self.source
                        .range(&(target.clone(), 0_usize)..=self.upper_bound.unwrap())
                })
                .into_iter()
                .flatten()
                .take(count)
                .map(|(_, index)| *index)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gpu_pool::container::SortedVec;
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
                let pool = GPUPool::default();
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
                let pool = GPUPool::default();
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
                let pool = GPUPool::default();
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
                let pool = GPUPool::default();
                assert_eq!(pool.num_gpus(), 1);
            },
        );
    }

    #[test]
    fn test_detect_num_gpus_explicit_env_overrides_cuda_visible() {
        temp_env::with_vars(
            [
                ("CUDA_VISIBLE_DEVICES", Some("0,1,2,3")),
                ("COCOINDEX_NUM_GPUS", Some("2")),
            ],
            || {
                let pool = GPUPool::default();
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
                let pool = GPUPool::default();
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
                let pool = GPUPool::default();
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
                let pool = GPUPool::default();
                assert_eq!(pool.num_gpus(), 8);
            },
        );
    }

    #[test]
    fn test_detect_num_gpus_nvidia_smi_empty_output() {
        temp_env::with_vars_unset(["CUDA_VISIBLE_DEVICES", "COCOINDEX_NUM_GPUS"], || {
            let pool = GPUPool::default();
            assert_eq!(pool.num_gpus(), 1);
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
                let pool = GPUPool::default();
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
                let pool = GPUPool::default();
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
                let pool = GPUPool::default();
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
        let index = capacity.find(&1);
        assert_eq!(index, Some(0));
    }

    #[test]
    fn test_sorted_vec_find_missing() {
        let original = [4, 3, 0];
        let capacity = SortedVec::from_iter(original); // [0, 3, 4]
        let index = capacity.find(&1);
        let expected = original.iter().position(|x| *x == 3);
        assert_eq!(index, expected);
    }

    #[test]
    fn test_sorted_vec_find_exact() {
        let original = [4, 3, 0];
        let capacity = SortedVec::from_iter(original);
        let index = capacity.find(&3);
        let expected = original.iter().position(|x| *x == 3);
        assert_eq!(index, expected);
    }

    #[test]
    fn test_sorted_vec_find_over_max() {
        let capacity = SortedVec::from_iter([0, 3, 4].into_iter().rev());
        let index = capacity.find(&i32::MAX);
        assert_eq!(index, None);
    }

    #[test]
    fn test_sorted_vec_find_empty() {
        let capacity = SortedVec::<usize>::from_iter([]);
        let index = capacity.find(&3);
        assert_eq!(index, None);
    }

    #[test]
    fn test_sorted_vec_find_excluding_top_n_found() {
        let original = (0..10).rev().collect::<Vec<_>>();
        let capacity = SortedVec::from_iter(original.clone());
        let index = capacity.excluding_top_n(3).find(&5);
        let expected = original.iter().position(|x| *x == 5);
        assert_eq!(index, expected);
    }

    #[test]
    fn test_sorted_vec_find_excluding_top_n_found_repeated() {
        let capacity = SortedVec::from_iter([1; 10]);
        let index = capacity.excluding_top_n(3).find(&1);
        assert_eq!(index, Some(0));
    }

    #[test]
    fn test_sorted_vec_find_excluding_top_n_excluded() {
        let capacity = SortedVec::from_iter((0..10).rev());
        let index = capacity.excluding_top_n(6).find(&5);
        assert_eq!(index, None);
    }

    #[test]
    fn test_sorted_vec_find_excluding_top_n_missing_excluded() {
        let capacity = SortedVec::from_iter([0, 4, 3, 5]); // [0, 3, 4, 5]
        let index = capacity.excluding_top_n(3).find(&2);
        assert_eq!(index, None);
    }

    #[test]
    fn test_sorted_vec_find_excluding_top_n_missing_found() {
        let original = [0, 19, 15, 9, 20];
        let capacity = SortedVec::from_iter(original); // [0, 9, 15, 19, 20]
        let index = capacity.excluding_top_n(2).find(&9);
        let expected = original.iter().position(|x| *x == 9);
        assert_eq!(index, expected);
    }
}
