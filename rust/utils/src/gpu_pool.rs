use anyhow::{Error, Result};
use itertools::Itertools;
use tokio::sync::{Mutex, Notify};

pub struct GPUPool {
    num_gpus: usize,
    capacity: Mutex<Vec<f32>>,
    release: Notify,
}

impl GPUPool {
    pub fn new(num_gpus: usize) -> Self {
        assert!(num_gpus >= 1, "num_gpus must be >= 1, got {num_gpus}");
        GPUPool {
            num_gpus,
            capacity: Mutex::new(vec![1.0; num_gpus]),
            release: Notify::new(),
        }
    }

    pub fn num_gpus(&self) -> usize {
        self.num_gpus
    }

    pub async fn acquire(&self, fraction: f32) -> usize {
        loop {
            let notified = self.release.notified();
            {
                let mut cap = self.capacity.lock().await;
                if let Some(index) = Self::find_available(&cap, fraction) {
                    cap[index] -= fraction;
                    return index;
                }
            }
            notified.await;
        }
    }

    fn find_available(capacity: &[f32], fraction: f32) -> Option<usize> {
        let max_position = capacity
            .iter()
            .position_max_by(|a, b| a.partial_cmp(b).unwrap())?;
        if capacity[max_position] >= fraction {
            Some(max_position)
        } else {
            None
        }
    }

    pub async fn release(&self, gpu_id: usize, fraction: f32) {
        {
            let mut cap = self.capacity.lock().await;
            cap[gpu_id] += fraction;
        }
        self.release.notify_waiters();
    }

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
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "nvidia-smi not found",
                ))
                .map_err(Error::from);
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
