use crate::prelude::*;

pub const SOURCE_STATE_TABLE: &str = "source_state_table";

pub fn default_features() -> BTreeSet<String> {
    vec![SOURCE_STATE_TABLE.to_string()].into_iter().collect()
}
