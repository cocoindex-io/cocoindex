use anyhow::{Error, Result};
use std::collections::HashSet;
use std::{collections::VecDeque, num::NonZeroUsize};
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
    state: Mutex<GPUPoolState>,
}

struct GPUPoolState {
    capacity: Vec<f32>,
    reserved: Vec<VecDeque<(f32, oneshot::Sender<()>)>>,
}

impl GPUPool {
    pub fn new(num_gpus: usize) -> Self {
        assert!(num_gpus >= 1, "num_gpus must be >= 1, got {num_gpus}");
        let state = GPUPoolState {
            capacity: vec![1.0; num_gpus],
            reserved: std::iter::repeat_with(VecDeque::new)
                .take(num_gpus)
                .collect(),
        };
        GPUPool {
            num_gpus,
            state: Mutex::new(state),
        }
    }

    pub fn num_gpus(&self) -> usize {
        self.num_gpus
    }

    pub async fn acquire(&self, fraction: f32) -> usize {
        let mut state = self.state.lock().await;
        if let Some(gpu) = Self::find_available(&state.capacity, fraction, &state.reserved) {
            state.capacity[gpu] -= fraction;
            return gpu;
        }
        let (reserved_gpu, recv) = Self::reserve_gpu(fraction, &mut state);
        drop(state);
        match recv.await {
            Ok(()) => reserved_gpu,
            Err(err) => panic!("GPUPool dropped while waiting: {err}"),
        }
    }

    fn find_available<T>(
        capacity: &[f32],
        fraction: f32,
        exclude: &[VecDeque<T>],
    ) -> Option<usize> {
        let max_position = capacity
            .iter()
            .enumerate()
            .filter(|(gpu_id, _)| exclude[*gpu_id].is_empty())
            .max_by(|(_, cap_a), (_, cap_b)| cap_a.partial_cmp(cap_b).unwrap())
            .map(|(gpu_id, _)| gpu_id)?;
        (capacity[max_position] >= fraction).then_some(max_position)
    }

    fn reserve_gpu(fraction: f32, state: &mut GPUPoolState) -> (usize, oneshot::Receiver<()>) {
        Self::reserve_gpu_with_exclusion(fraction, state, &HashSet::new())
    }

    fn reserve_gpu_with_exclusion(
        fraction: f32,
        state: &mut GPUPoolState,
        exclude_gpus: &HashSet<usize>,
    ) -> (usize, oneshot::Receiver<()>) {
        let (sender, recv) = oneshot::channel();
        let reserved_gpu = Self::find_shortest_queue(&state.reserved, exclude_gpus);
        state.reserved[reserved_gpu].push_back((fraction, sender));
        (reserved_gpu, recv)
    }

    fn find_shortest_queue<T>(queues: &[VecDeque<T>], exclude: &HashSet<usize>) -> usize {
        queues
            .iter()
            .enumerate()
            .filter(|(gpu_id, _)| !exclude.contains(gpu_id))
            .min_by(|(_, queue_a), (_, queue_b)| queue_a.len().cmp(&queue_b.len()))
            .map(|(gpu_id, _)| gpu_id)
            .unwrap_or_default()
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
        let mut state = self.state.lock().await;
        let gpu_ids = Self::find_fully_available(&state.capacity, gpu_count, &state.reserved);
        let acquired_gpu_count = gpu_ids.len();
        for gpu_id in &gpu_ids {
            state.capacity[*gpu_id] = 0_f32;
        }
        if acquired_gpu_count == gpu_count {
            return Ok(gpu_ids);
        }
        let reserved_gpu_count = gpu_count - acquired_gpu_count;
        let mut acquired_gpus = gpu_ids;
        let exclude_gpus: HashSet<_> = acquired_gpus.clone().into_iter().collect();
        let mut pending_gpus = Vec::with_capacity(reserved_gpu_count);
        let mut pending_tasks = Vec::with_capacity(reserved_gpu_count);
        for _ in 0..reserved_gpu_count {
            let (gpu_id, task) = Self::reserve_gpu_with_exclusion(1.0, &mut state, &exclude_gpus);
            pending_gpus.push(gpu_id);
            pending_tasks.push(task);
        }
        drop(state);
        futures::future::join_all(pending_tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        acquired_gpus.extend(pending_gpus);
        Ok(acquired_gpus)
    }

    fn find_fully_available<T>(
        capacity: &[f32],
        count: usize,
        exclude: &[VecDeque<T>],
    ) -> Vec<usize> {
        debug_assert!(count >= 1, "count must be >= 1, got {count}");
        capacity
            .iter()
            .enumerate()
            .filter(|(gpu_id, _)| exclude[*gpu_id].is_empty())
            .filter(|(_, cap)| (*cap - 1.0).abs() <= f32::EPSILON)
            .map(|(gpu_id, _)| gpu_id)
            .take(count)
            .collect()
    }

    pub async fn release(&self, gpu_id: usize, fraction: f32) {
        debug_assert!(gpu_id < self.num_gpus(), "gpu_id out of bounds");
        debug_assert!(
            fraction > 0.0,
            "fraction must be positive, got {}",
            fraction
        );
        let mut state = self.state.lock().await;
        state.capacity[gpu_id] += fraction;
        debug_assert!(
            (state.capacity[gpu_id]) <= 1.0 + f32::EPSILON,
            "capacity overflown"
        );
        while state.reserved[gpu_id]
            .front()
            .map(|(fraction, _)| f32::EPSILON >= state.capacity[gpu_id] - *fraction)
            .unwrap_or(false)
        {
            let Some((fraction, sender)) = state.reserved[gpu_id].pop_front() else {
                break;
            };
            if sender.send(()).is_ok() {
                state.capacity[gpu_id] -= fraction;
                break;
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
    /// * failed to parse a environment variable value to a number
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
        Self::new(Self::detect_num_gpus().unwrap_or(1))
    }
}

#[cfg(test)]
mod tests {
    use crate::gpu_pool::GPUPool;
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_acquire_returns_gpu_id() {
        let pool = GPUPool::new(2);
        let gpu = pool.acquire(1.0).await;
        assert!(gpu < 2);
        pool.release(gpu, 1.0).await;
    }

    #[tokio::test]
    async fn test_acquire_different_gpus() {
        let pool = GPUPool::new(2);
        let gpu0 = pool.acquire(1.0).await;
        let gpu1 = pool.acquire(1.0).await;
        assert_ne!(gpu0, gpu1);
        pool.release(gpu0, 1.0).await;
        pool.release(gpu1, 1.0).await;
    }

    #[tokio::test]
    async fn test_acquire_blocks_when_capacity_full() {
        let pool = Arc::new(GPUPool::new(1));
        let gpu = pool.acquire(1.0).await;

        let cloned_pool = pool.clone();
        let task = tokio::spawn(async move { cloned_pool.acquire(1.0).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!task.is_finished());

        pool.release(gpu, 1.0).await;
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("task finished");
        assert!(matches!(result, Ok(0)));
        pool.release(result.unwrap(), 1.0).await;
    }

    #[tokio::test]
    async fn test_fractional_shares_same_gpu() {
        let pool = Arc::new(GPUPool::new(1));
        let gpu0 = pool.acquire(0.5).await;
        let gpu1 = pool.acquire(0.5).await;
        assert_eq!(gpu0, gpu1);

        let cloned_pool = pool.clone();
        let task = tokio::spawn(async move { cloned_pool.acquire(0.5).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!task.is_finished());

        pool.release(gpu0, 0.5).await;
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("task finished");
        assert!(matches!(result, Ok(0)));
        pool.release(gpu1, 0.5).await;
        pool.release(result.unwrap(), 0.5).await;
    }

    #[tokio::test]
    async fn test_multi_gpu_all_parallel() {
        let pool = Arc::new(GPUPool::new(3));
        let mut tasks = Vec::with_capacity(3);
        for _ in 0..3 {
            let pool = pool.clone();
            tasks.push(tokio::spawn(async move { pool.acquire(1.0).await }));
        }
        let results = futures::future::join_all(tasks).await;
        let gpus = results
            .into_iter()
            .collect::<Result<Vec<usize>, _>>()
            .expect("tasks finished");
        assert_eq!(gpus.len(), 3);
        for g in gpus {
            pool.release(g, 1.0).await;
        }
    }

    #[tokio::test]
    async fn test_acquire_full_gpus_enough() {
        let pool = GPUPool::new(2);
        let gpus = pool
            .acquire_full(NonZeroUsize::new(2).expect("2 is not zero"))
            .await;
        assert_eq!(gpus.ok(), Some(vec![0, 1]));
    }

    #[tokio::test]
    async fn test_acquire_full_gpus_not_enough() {
        let pool = Arc::new(GPUPool::new(3));
        let partially_used_gpu = pool.acquire(0.6).await;
        assert_eq!(partially_used_gpu, 2);
        let cloned_pool = pool.clone();
        let task = tokio::spawn(async move {
            cloned_pool
                .acquire_full(NonZeroUsize::new(3).expect("3 is not zero"))
                .await
        });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!task.is_finished());
        pool.release(partially_used_gpu, 0.6).await;
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("task finished")
            .expect("no timeout");
        assert_eq!(result.as_ref().ok(), Some(&vec![0, 1, 2]));
        for gpu in result.unwrap() {
            pool.release(gpu, 1.0).await;
        }
    }

    #[tokio::test]
    async fn test_acquire_full_gpus_no_partial_acquiring() {
        let pool = Arc::new(GPUPool::new(3));
        let partially_used_gpu = pool.acquire(0.6).await;
        assert_eq!(partially_used_gpu, 2);
        let cloned_pool = pool.clone();
        let task = tokio::spawn(async move {
            cloned_pool
                .acquire_full(NonZeroUsize::new(3).expect("3 is not zero"))
                .await
        });
        let cloned_pool = pool.clone();
        let second_acquired_gpu = tokio::spawn(async move { cloned_pool.acquire(0.2).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!task.is_finished());
        assert!(!second_acquired_gpu.is_finished());
        pool.release(partially_used_gpu, 0.6).await;
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("task finished")
            .expect("no timeout");
        assert_eq!(result.as_ref().ok(), Some(&vec![0, 1, 2]));
        for gpu in result.unwrap() {
            pool.release(gpu, 1.0).await;
        }
    }

    #[tokio::test]
    async fn test_acquire_more_gpus_than_allowed() {
        let pool = GPUPool::new(2);
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
    async fn test_reserve_gpus_then_release() {
        let pool = Arc::new(GPUPool::new(2));
        let gpu_0 = pool.acquire(0.5).await;
        assert_eq!(gpu_0, 1);
        let gpu_1 = pool.acquire(0.6).await;
        assert_eq!(gpu_1, 0);
        let cloned_pool = pool.clone();
        let reserving_task_1 = tokio::spawn(async move { cloned_pool.acquire(0.6).await });
        let cloned_pool = pool.clone();
        let reserving_task_2 = tokio::spawn(async move { cloned_pool.acquire(0.7).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!reserving_task_1.is_finished());
        assert!(!reserving_task_2.is_finished());

        pool.release(gpu_1, 0.2).await;
        let reserving_task_1_acquired_gpu =
            tokio::time::timeout(std::time::Duration::from_secs(1), reserving_task_1)
                .await
                .expect("task finished")
                .expect("no timeout");
        assert_eq!(reserving_task_1_acquired_gpu, gpu_1);
        assert!(!reserving_task_2.is_finished());

        pool.release(gpu_0, 0.2).await;
        let reserving_task_2_acquired_gpu =
            tokio::time::timeout(std::time::Duration::from_secs(1), reserving_task_2)
                .await
                .expect("task finished")
                .expect("no timeout");
        assert_eq!(reserving_task_2_acquired_gpu, gpu_0);

        pool.release(gpu_0, 1.0).await;
        pool.release(gpu_1, 1.0).await;
    }

    #[tokio::test]
    async fn test_reserve_gpus_without_affecting_unreserved() {
        let pool = Arc::new(GPUPool::new(2));
        let gpu_0 = pool.acquire(0.5).await;
        assert_eq!(gpu_0, 1);
        let gpu_1 = pool.acquire(0.6).await;
        assert_eq!(gpu_1, 0);
        let cloned_pool = pool.clone();
        let reserving_task = tokio::spawn(async move { cloned_pool.acquire(0.6).await });
        let cloned_pool = pool.clone();
        let task_not_blocked = tokio::spawn(async move { cloned_pool.acquire(0.2).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!reserving_task.is_finished());
        assert!(task_not_blocked.is_finished());
        pool.release(gpu_0, 0.7).await;

        pool.release(gpu_1, 0.2).await;
        let reserving_task_acquired_gpu =
            tokio::time::timeout(std::time::Duration::from_secs(1), reserving_task)
                .await
                .expect("task finished")
                .expect("no timeout");
        assert_eq!(reserving_task_acquired_gpu, gpu_1);

        pool.release(gpu_1, 1.0).await;
    }

    #[tokio::test]
    async fn test_reserve_the_same_gpu_in_a_queue() {
        let pool = Arc::new(GPUPool::new(1));
        let gpu_0 = pool.acquire(0.5).await;
        assert_eq!(gpu_0, 0);
        let cloned_pool = pool.clone();
        let reserving_task_1 = tokio::spawn(async move { cloned_pool.acquire(0.6).await });
        let cloned_pool = pool.clone();
        let reserving_task_2 = tokio::spawn(async move { cloned_pool.acquire(0.7).await });
        tokio::time::sleep(std::time::Duration::from_secs_f32(0.02)).await;
        assert!(!reserving_task_1.is_finished());
        assert!(!reserving_task_2.is_finished());

        pool.release(gpu_0, 0.1).await;
        let reserving_task_1_acquired_gpu =
            tokio::time::timeout(std::time::Duration::from_secs(1), reserving_task_1)
                .await
                .expect("task finished")
                .expect("no timeout");
        assert_eq!(reserving_task_1_acquired_gpu, gpu_0);
        assert!(!reserving_task_2.is_finished());

        pool.release(gpu_0, 0.7).await;
        let reserving_task_2_acquired_gpu =
            tokio::time::timeout(std::time::Duration::from_secs(1), reserving_task_2)
                .await
                .expect("task finished")
                .expect("no timeout");
        assert_eq!(reserving_task_2_acquired_gpu, gpu_0);

        pool.release(gpu_0, 1.0).await;
    }

    #[test]
    #[should_panic]
    fn test_invalid_num_gpus_raises() {
        let _ = GPUPool::new(0);
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
}
