use crate::prelude::*;

pub const SOURCE_STATE_TABLE: &str = "source_state_table";
pub const FAST_FINGERPRINT: &str = "fast_fingerprint";

pub fn default_features() -> BTreeSet<String> {
    BTreeSet::from_iter([FAST_FINGERPRINT.to_string()])
}
