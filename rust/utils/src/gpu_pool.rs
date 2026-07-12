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
        let output = std::process::Command::new("nvidia-smi")
            .arg("--query-gpu=count")
            .arg("--format=csv,noheader")
            .output()?;
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
