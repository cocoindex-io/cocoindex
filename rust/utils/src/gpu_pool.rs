use anyhow::{Error, Result};
use gpu_fraction::GPUCapacity;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use tokio::sync::{oneshot, Mutex};

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
    gpus: Mutex<Vec<GPUState>>,
    task_queue: Mutex<VecDeque<PendingTask>>,
}

type PendingTask = (GPUCapacity, oneshot::Sender<usize>);

struct GPUState {
    capacity: GPUCapacity,
    reservation: Option<(GPUCapacity, oneshot::Sender<usize>)>,
}

impl GPUState {
    fn is_reservable(&self) -> bool {
        self.reservation.is_none()
    }
}

impl GPUPool {
    pub fn new(num_gpus: NonZeroUsize) -> Self {
        let num_gpus = num_gpus.get();
        let gpus = std::iter::repeat_with(|| GPUState {
            capacity: GPUCapacity::ONE,
            reservation: None,
        })
        .take(num_gpus)
        .collect();
        GPUPool {
            num_gpus,
            gpus: Mutex::new(gpus),
            task_queue: Mutex::new(VecDeque::new()),
        }
    }

    pub fn num_gpus(&self) -> usize {
        self.num_gpus
    }

    pub async fn acquire(&self, fraction: GPUCapacity) -> Result<usize> {
        if fraction == GPUCapacity::ZERO {
            return Err(anyhow::anyhow!(
                "Acquired fraction must be between 0.0 and 1.0, got 0"
            ));
        }
        let receiver = {
            let mut task_queue = self.task_queue.lock().await;
            if task_queue.is_empty() {
                let mut gpus = self.gpus.lock().await;
                if let Some(gpu) = Self::find_available(Self::capacities(gpus.iter()), fraction) {
                    gpus[gpu].capacity -= fraction;
                    return Ok(gpu);
                } else if let Some(gpu) = Self::find_reservable_gpu(gpus.iter()) {
                    Self::reserve_gpu(&mut gpus, gpu, fraction)
                } else {
                    Self::send_task_to_queue(&mut task_queue, fraction)
                }
            } else {
                Self::send_task_to_queue(&mut task_queue, fraction)
            }
        };
        match receiver.await {
            Ok(gpu_label) => Ok(gpu_label),
            Err(err) => panic!("GPUPool dropped while waiting: {err}"),
        }
    }

    fn capacities<'a>(
        gpus: impl IntoIterator<Item = &'a GPUState>,
    ) -> impl Iterator<Item = GPUCapacity> {
        gpus.into_iter().map(|gpu| {
            if gpu.is_reservable() {
                gpu.capacity
            } else {
                GPUCapacity::ZERO
            }
        })
    }

    fn find_available<N: Ord + Copy>(
        capacity: impl IntoIterator<Item = N>,
        fraction: N,
    ) -> Option<usize> {
        capacity
            .into_iter()
            .enumerate()
            .filter(|(_, cap)| *cap >= fraction)
            .min_by_key(|(_, cap)| *cap)
            .map(|(gpu_label, _)| gpu_label)
    }

    fn reserve_gpu(
        gpus: &mut Vec<GPUState>,
        gpu_id: usize,
        fraction: GPUCapacity,
    ) -> oneshot::Receiver<usize> {
        let (notifier, receiver) = oneshot::channel();
        gpus[gpu_id].reservation = Some((fraction, notifier));
        receiver
    }

    fn send_task_to_queue(
        task_queue: &mut VecDeque<PendingTask>,
        desired_capacity: GPUCapacity,
    ) -> oneshot::Receiver<usize> {
        let (notifier, receiver) = oneshot::channel();
        task_queue.push_back((desired_capacity, notifier));
        receiver
    }

    /// Acquires a given integer number of fully available GPUs (capacity == 1.0) from the GPU pool.
    ///
    /// # Error:
    /// * When the given gpu_count is larger than the total gpus, it returns an error.
    ///
    /// # Warning
    /// * All GPUs will be acquired at simultaneously.
    ///   For instance, if user attempts to acquire 5 GPUs,
    ///   the function will not partially acquire 4 and waiting for the last GPU.
    pub async fn acquire_full(&self, gpu_count: NonZeroUsize) -> Result<Vec<usize>> {
        let gpu_count = gpu_count.get();
        if gpu_count > self.num_gpus() {
            return Err(anyhow::format_err!(
                "Attempted to acquire {} GPUs but only has {}.",
                gpu_count,
                self.num_gpus
            ));
        }
        let (acquired_gpus, receivers) = {
            let mut task_queue = self.task_queue.lock().await;
            let (acquired_gpus, mut receivers) = if task_queue.is_empty() {
                let mut gpus = self.gpus.lock().await;
                let acquired_gpus = Self::find_fully_available(
                    gpus.iter()
                        .filter(|gpu| gpu.is_reservable())
                        .map(|gpu| gpu.capacity),
                    GPUCapacity::ONE,
                    gpu_count,
                );
                let acquired_gpu_count = acquired_gpus.len();
                for gpu_id in &acquired_gpus {
                    gpus[*gpu_id].capacity = GPUCapacity::ZERO;
                }
                if acquired_gpu_count == gpu_count {
                    return Ok(acquired_gpus);
                }
                let reserved_gpus_count = gpu_count - acquired_gpu_count;
                let mut receivers = Vec::with_capacity(reserved_gpus_count);
                for _ in 0..reserved_gpus_count {
                    if let Some(gpu_id) = Self::find_reservable_gpu(gpus.iter()) {
                        receivers.push(Self::reserve_gpu(&mut gpus, gpu_id, GPUCapacity::ONE));
                    } else {
                        break;
                    }
                }
                (acquired_gpus, receivers)
            } else {
                (vec![], vec![])
            };
            let reserved_gpus_count = gpu_count - acquired_gpus.len() - receivers.len();
            receivers.extend(
                std::iter::repeat_with(|| {
                    Self::send_task_to_queue(&mut task_queue, GPUCapacity::ONE)
                })
                .take(reserved_gpus_count),
            );
            (acquired_gpus, receivers)
        };
        let reserved_gpu_count = receivers.len();
        let reserve_gpu_tasks = receivers.into_iter().map(async |receiver| receiver.await);
        let gpu_labels = futures::future::try_join_all(reserve_gpu_tasks).await?;
        debug_assert_eq!(
            gpu_labels.len(),
            reserved_gpu_count,
            "reserved {} GPUs but received {} GPUs. May be a bug in `release` function",
            reserved_gpu_count,
            gpu_labels.len()
        );
        let mut acquired_gpus = acquired_gpus;
        acquired_gpus.extend(gpu_labels);
        Ok(acquired_gpus)
    }

    fn find_fully_available<N: PartialEq>(
        capacity: impl IntoIterator<Item = N>,
        target: N,
        count: usize,
    ) -> Vec<usize> {
        debug_assert!(count >= 1, "count must be >= 1, got {count}");
        capacity
            .into_iter()
            .enumerate()
            .filter(|(_, cap)| cap == &target)
            .map(|(gpu_id, _)| gpu_id)
            .take(count)
            .collect()
    }

    pub async fn release(&self, gpu_id: usize, fraction: GPUCapacity) -> Result<()> {
        if gpu_id >= self.num_gpus() {
            return Err(anyhow::format_err!(
                "Releasing to a gpu_id that does not exist: {}",
                gpu_id
            ));
        }
        if fraction == GPUCapacity::ZERO {
            return Err(anyhow::format_err!("Cannot release a zero fraction"));
        }
        let mut gpus = self.gpus.lock().await;
        if gpus[gpu_id].capacity + fraction > GPUCapacity::ONE {
            return Err(anyhow::format_err!(
                "Capacity after releasing cannot be greater than 1.0, got {}",
                gpus[gpu_id].capacity + fraction
            ));
        }
        gpus[gpu_id].capacity += fraction;
        Self::fulfill_reserved_task(gpu_id, &mut gpus[gpu_id]);
        let mut task_queue = self.task_queue.lock().await;
        Self::process_task_queue(&mut task_queue, &mut gpus);
        Ok(())
    }

    fn fulfill_reserved_task(gpu_id: usize, gpu: &mut GPUState) {
        if gpu
            .reservation
            .as_ref()
            .map(|(desired_capacity, _)| desired_capacity <= &gpu.capacity)
            .unwrap_or_default()
        {
            let (desired_capacity, notifier) = gpu.reservation.take().unwrap();
            // if the task is no longer waiting, so we can ignore the error.
            if notifier.send(gpu_id).is_ok() {
                gpu.capacity -= desired_capacity;
            }
        }
    }

    /// Process a queue of pending tasks.
    ///
    /// 1. if we can schedule a task, then go ahead and schedule it
    /// 2. if not capable for scheduling,
    ///    and this task is not the last task for a batch (explained later),
    ///    then we try to find a GPU and reserve the GPU.
    ///
    /// The reason why we want to check if it's the last task of a batch is to do a small optimization.
    /// We want to wait for a GPU with enough capacity to host it, instead of scheduling it for a GPU,
    /// while another GPU may be freed up very quickly.
    fn process_task_queue(task_queue: &mut VecDeque<PendingTask>, gpus: &mut [GPUState]) {
        while let Some((desired_capacity, _)) = task_queue.front() {
            if let Some(gpu_id) =
                Self::find_available(Self::capacities(gpus.iter()), *desired_capacity)
            {
                let (desired_capacity, notifier) = task_queue.pop_front().unwrap();
                if notifier.send(gpu_id).is_ok() {
                    gpus[gpu_id].capacity -= desired_capacity;
                }
            } else if let Some(gpu_id) = Self::find_reservable_gpu(gpus.iter()) {
                let (desired_capacity, notifier) = task_queue.pop_front().unwrap();
                gpus[gpu_id].reservation = Some((desired_capacity, notifier));
            } else {
                break;
            }
        }
    }
    fn find_reservable_gpu<'a>(gpus: impl IntoIterator<Item = &'a GPUState>) -> Option<usize> {
        gpus.into_iter()
            .enumerate()
            .filter(|(_, gpu)| gpu.is_reservable())
            .max_by_key(|(_, gpu)| gpu.capacity)
            .map(|(gpu_id, _)| gpu_id)
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
        if let Ok(env_num) = std::env::var("COCOINDEX_NUM_GPUS")
            .map_err(Error::from)
            .and_then(|s| s.parse::<usize>().map_err(Error::from))
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
                return Err(Error::from(std::io::Error::new(
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

pub mod gpu_fraction {
    use anyhow::{Error, Result};
    use std::ops::{Add, AddAssign, Sub, SubAssign};

    #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
    pub struct GPUCapacity(u32);

    impl GPUCapacity {
        const SCALE: f32 = 1_000_000.0;
        pub const ZERO: Self = Self(0);
        pub const ONE: Self = Self(Self::SCALE as u32);

        #[cfg(test)]
        pub(crate) fn unchecked(value: f32) -> Self {
            GPUCapacity::try_from(value).expect("Unchecked value initialization should not fail")
        }
    }

    impl TryFrom<f32> for GPUCapacity {
        type Error = Error;

        fn try_from(value: f32) -> Result<Self, Self::Error> {
            if !(0.0..=1.0).contains(&value) {
                return Err(anyhow::format_err!(
                    "Fraction must be between 0.0 and 1.0, got {}",
                    value
                ));
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

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_acquire_returns_gpu_id() -> Result<()> {
        let pool = GPUPool::new(NonZeroUsize::new(2).unwrap());
        let gpu = pool.acquire(GPUCapacity::ONE).await?;
        assert!(gpu < 2);
        pool.release(gpu, GPUCapacity::ONE).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_acquire_different_gpus() -> Result<()> {
        let pool = GPUPool::new(NonZeroUsize::new(2).unwrap());
        let gpu0 = pool.acquire(GPUCapacity::ONE).await?;
        let gpu1 = pool.acquire(GPUCapacity::ONE).await?;
        assert_ne!(gpu0, gpu1);
        pool.release(gpu0, GPUCapacity::ONE).await?;
        pool.release(gpu1, GPUCapacity::ONE).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_acquire_blocks_when_capacity_full() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(1).unwrap()));
        let gpu = pool.acquire(GPUCapacity::ONE).await?;

        let cloned_pool = pool.clone();
        let task = tokio::spawn(async move { cloned_pool.acquire(GPUCapacity::ONE).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!task.is_finished());

        pool.release(gpu, GPUCapacity::ONE).await?;
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("task finished")?;
        assert!(matches!(result, Ok(0)));
        pool.release(result.unwrap(), GPUCapacity::ONE).await?;
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

        pool.release(gpu0, half_fraction).await?;
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("task finished")?;
        assert!(matches!(result, Ok(0)));
        pool.release(gpu1, half_fraction).await?;
        pool.release(result.unwrap(), half_fraction).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_multi_gpu_all_parallel() -> Result<()> {
        let pool = Arc::new(GPUPool::new(NonZeroUsize::new(3).unwrap()));
        let mut tasks = Vec::with_capacity(3);
        for _ in 0..3 {
            let pool = pool.clone();
            tasks.push(tokio::spawn(
                async move { pool.acquire(GPUCapacity::ONE).await },
            ));
        }
        let results = futures::future::try_join_all(tasks).await?;
        let gpus = results.into_iter().collect::<Result<Vec<usize>, _>>()?;
        assert_eq!(gpus.len(), 3);
        for g in gpus {
            pool.release(g, GPUCapacity::ONE).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_acquire_fractions_equals_to_zero() {
        let pool = GPUPool::new(NonZeroUsize::new(1).unwrap());
        let result = pool.acquire(GPUCapacity::ZERO).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Acquired fraction must be between 0.0 and 1.0, got 0"
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
        pool.release(occupied_gpu_2, GPUCapacity::unchecked(0.2))
            .await?;
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!not_enough_task.is_finished());
        pool.release(occupied_gpu_2, GPUCapacity::unchecked(0.4))
            .await?;
        pool.release(occupied_gpu_1, GPUCapacity::unchecked(0.6))
            .await?;
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(not_enough_task.is_finished());
        let gpus = tokio::time::timeout(std::time::Duration::from_secs(1), not_enough_task)
            .await
            .expect("task finished")
            .expect("no timeout")?;
        assert_eq!(gpus.len(), 3);
        for gpu in gpus {
            pool.release(gpu, GPUCapacity::ONE).await?;
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
            pool.release(g, GPUCapacity::ONE).await?;
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
        pool.release(partially_used_gpu, GPUCapacity::unchecked(0.6))
            .await?;
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("task finished")
            .expect("no timeout")?;
        assert_eq!(&result, &[1, 2, 0]);
        for gpu in result {
            pool.release(gpu, GPUCapacity::ONE).await?;
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
        pool.release(partially_used_gpu, GPUCapacity::unchecked(0.6))
            .await?;
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("task finished")
            .expect("no timeout")?;
        // initial 0.6 occupied index 0, then GPU 1 and 2 are reserved, until 0 is added.
        assert_eq!(&result, &[1, 2, 0]);
        for gpu in result {
            pool.release(gpu, GPUCapacity::ONE).await?;
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
        assert_eq!(
            result.unwrap_err().to_string(),
            "Attempted to acquire 3 GPUs but only has 2."
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

        pool.release(gpu_0, GPUCapacity::unchecked(0.1)).await?;
        let reserving_task_1_acquired_gpu =
            tokio::time::timeout(std::time::Duration::from_secs(1), reserving_task_1)
                .await
                .expect("task finished")
                .expect("no timeout")?;
        assert_eq!(reserving_task_1_acquired_gpu, gpu_0);
        assert!(!reserving_task_2.is_finished());

        pool.release(gpu_1, GPUCapacity::unchecked(0.3)).await?;
        let reserving_task_2_acquired_gpu =
            tokio::time::timeout(std::time::Duration::from_secs(1), reserving_task_2)
                .await
                .expect("task finished")
                .expect("no timeout")?;
        assert_eq!(reserving_task_2_acquired_gpu, gpu_1);

        pool.release(gpu_0, GPUCapacity::ONE).await?;
        pool.release(gpu_1, GPUCapacity::ONE).await?;
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
        pool.release(gpu_0, GPUCapacity::unchecked(0.1)).await?;

        pool.release(gpu_1, GPUCapacity::unchecked(0.8)).await?;
        let reserving_task_acquired_gpu =
            tokio::time::timeout(std::time::Duration::from_secs(1), reserving_task)
                .await
                .expect("task finished")
                .expect("no timeout")?;
        assert_eq!(reserving_task_acquired_gpu, gpu_0);

        pool.release(gpu_0, GPUCapacity::ONE).await?;
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

        pool.release(gpu_0, GPUCapacity::unchecked(0.1)).await?;
        let reserving_task_1_acquired_gpu =
            tokio::time::timeout(std::time::Duration::from_secs(1), reserving_task_1)
                .await
                .expect("task finished")
                .expect("no timeout")?;
        assert_eq!(reserving_task_1_acquired_gpu, gpu_0);
        assert!(!reserving_task_2.is_finished());

        pool.release(gpu_0, GPUCapacity::unchecked(0.7)).await?;
        let reserving_task_2_acquired_gpu =
            tokio::time::timeout(std::time::Duration::from_secs(1), reserving_task_2)
                .await
                .expect("task finished")
                .expect("no timeout")?;
        assert_eq!(reserving_task_2_acquired_gpu, gpu_0);

        pool.release(gpu_0, GPUCapacity::ONE).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_release_gpus() -> Result<()> {
        let pool = GPUPool::new(NonZeroUsize::new(1).unwrap());
        let gpu_0 = pool.acquire(GPUCapacity::unchecked(0.5)).await?;
        assert_eq!(gpu_0, 0);
        pool.release(gpu_0, GPUCapacity::unchecked(0.5)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_release_to_wrong_gpu_id() {
        let pool = GPUPool::new(NonZeroUsize::new(1).unwrap());
        let release_result = pool.release(1, GPUCapacity::unchecked(0.5)).await;
        assert!(release_result.is_err());
        assert_eq!(
            release_result.unwrap_err().to_string(),
            "Releasing to a gpu_id that does not exist: 1"
        );
    }

    #[tokio::test]
    async fn test_release_zero_fraction() {
        let pool = GPUPool::new(NonZeroUsize::new(1).unwrap());
        let release_result = pool.release(0, GPUCapacity::ZERO).await;
        assert!(release_result.is_err());
        assert_eq!(
            release_result.unwrap_err().to_string(),
            "Cannot release a zero fraction"
        );
    }

    #[tokio::test]
    async fn test_release_overflown_gpus() -> Result<()> {
        let pool = GPUPool::new(NonZeroUsize::new(1).unwrap());
        let gpu_0 = pool.acquire(GPUCapacity::unchecked(0.5)).await?;
        assert_eq!(gpu_0, 0);
        let release_result = pool.release(gpu_0, GPUCapacity::unchecked(0.6)).await;
        assert!(release_result.is_err());
        assert_eq!(
            release_result.unwrap_err().to_string(),
            "Capacity after releasing cannot be greater than 1.0, got 1.1"
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
        assert_eq!(
            result.unwrap_err().to_string(),
            "Fraction must be between 0.0 and 1.0, got 1.1"
        );
    }

    #[test]
    fn test_gpu_capacity_less_than_zero() {
        let result = GPUCapacity::try_from(-1.1);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Fraction must be between 0.0 and 1.0, got -1.1"
        );
    }

    #[test]
    fn test_gpu_capacity_zero() -> Result<()> {
        let half = GPUCapacity::try_from(0.5)?;
        let result = GPUCapacity::ONE - half + half - GPUCapacity::ZERO;
        assert_eq!(result, GPUCapacity::ONE);
        Ok(())
    }

    #[test]
    fn test_gpu_capacity_repeat_acquire_then_release() -> Result<()> {
        let mut full = GPUCapacity::ONE;
        let mut rng = rand::rng();
        for _ in 0..100_000 {
            let random_portion: f32 = rng.random_range(0.0..=1.0);
            full -= GPUCapacity::try_from(random_portion)?;
            full += GPUCapacity::try_from(random_portion)?;
        }
        assert_eq!(full, GPUCapacity::ONE);
        Ok(())
    }

    #[test]
    fn test_gpu_capacity_repeat_acquire_then_release_later() -> Result<()> {
        let mut full = GPUCapacity::ONE;
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
                    GPUCapacity::ONE,
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
            GPUCapacity::ONE,
            "full ({full}) + sum({random_capacities:?}) != 1.0 (should be 1.0) (final)"
        );
        Ok(())
    }
}
