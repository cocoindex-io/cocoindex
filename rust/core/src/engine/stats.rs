use crate::prelude::*;

#[derive(Default)]
pub struct ProcessingStatsGroup {
    pub num_starts: u64,
    pub num_ends: u64,

    pub num_adds: u64,
    pub num_deletes: u64,
    pub num_updates: u64,
    pub num_errors: u64,
}

#[derive(Default, Clone)]
pub struct ProcessingStats {
    pub stats: Arc<Mutex<HashMap<String, ProcessingStatsGroup>>>,
}

impl ProcessingStats {
    pub fn update(&self, operation_name: &str, mutator: impl FnOnce(&mut ProcessingStatsGroup)) {
        let mut stats = self.stats.lock().unwrap();
        if let Some(group) = stats.get_mut(operation_name) {
            mutator(group);
        } else {
            let mut group = ProcessingStatsGroup::default();
            mutator(&mut group);
            stats.insert(operation_name.to_string(), group);
        }
    }
}
